use std::collections::BTreeMap;
use std::env;
use std::fs;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use tonic::transport::{Channel, Endpoint};

pub mod classify {
    tonic::include_proto!("classify");
}

use classify::classify_client::ClassifyClient;
use classify::{ClassificationStatus, ClassifyRequest, ClassifyResponse};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CacheMode {
    Hit,
    Miss,
    Mixed,
}

impl CacheMode {
    fn parse(raw: &str) -> Self {
        match raw {
            "hit" => Self::Hit,
            "miss" => Self::Miss,
            "mixed" => Self::Mixed,
            _ => panic!("--cache-mode must be hit, miss, or mixed"),
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Hit => "hit",
            Self::Miss => "miss",
            Self::Mixed => "mixed",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ArrivalPattern {
    Constant,
    Burst,
    Sawtooth,
    Spike,
}

impl ArrivalPattern {
    fn parse(raw: &str) -> Self {
        match raw {
            "constant" => Self::Constant,
            "burst" => Self::Burst,
            "sawtooth" => Self::Sawtooth,
            "spike" => Self::Spike,
            _ => panic!("--pattern must be constant, burst, sawtooth, or spike"),
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Constant => "constant",
            Self::Burst => "burst",
            Self::Sawtooth => "sawtooth",
            Self::Spike => "spike",
        }
    }
}

#[derive(Clone, Debug)]
struct Config {
    targets: Vec<String>,
    warm_targets: Vec<String>,
    run_id: String,
    topology: String,
    cache_mode: CacheMode,
    hit_ratio_bps: u32,
    context_bytes: usize,
    signals: Vec<String>,
    connections_per_target: usize,
    concurrency: usize,
    requests: Option<u64>,
    duration: Option<Duration>,
    base_rps: f64,
    peak_rps: f64,
    pattern: ArrivalPattern,
    period: Duration,
    duty_cycle_bps: u32,
    max_in_flight: usize,
    rpc_timeout: Duration,
    scheduled_start_epoch_ms: Option<u64>,
}

fn all_args(name: &str) -> Vec<String> {
    let args = env::args().collect::<Vec<_>>();
    args.iter()
        .enumerate()
        .filter(|(_, value)| value.as_str() == name)
        .map(|(index, _)| {
            args.get(index + 1)
                .unwrap_or_else(|| panic!("{name} requires a value"))
                .clone()
        })
        .collect()
}

fn arg(name: &str, default: Option<&str>) -> String {
    all_args(name)
        .into_iter()
        .last()
        .or_else(|| default.map(str::to_string))
        .unwrap_or_else(|| panic!("missing required argument {name}"))
}

impl Config {
    fn from_args() -> Self {
        let targets = all_args("--target");
        assert!(!targets.is_empty(), "at least one --target is required");
        let cache_mode = CacheMode::parse(&arg("--cache-mode", Some("hit")));
        let hit_ratio_bps = arg("--hit-ratio-bps", Some("5000"))
            .parse::<u32>()
            .expect("--hit-ratio-bps must be an integer");
        assert!(
            hit_ratio_bps <= 10_000,
            "--hit-ratio-bps cannot exceed 10000"
        );
        let context_bytes = arg("--context-bytes", Some("256"))
            .parse::<usize>()
            .expect("--context-bytes must be an integer");
        assert!(context_bytes >= 32, "--context-bytes must be at least 32");
        let connections_per_target = arg("--connections-per-target", Some("1"))
            .parse::<usize>()
            .expect("--connections-per-target must be an integer");
        let concurrency = arg("--concurrency", Some("64"))
            .parse::<usize>()
            .expect("--concurrency must be an integer");
        assert!(connections_per_target > 0 && concurrency > 0);

        let offered = all_args("--offered-rps").into_iter().last();
        let duration = all_args("--duration-seconds")
            .into_iter()
            .last()
            .map(|raw| {
                Duration::from_secs(raw.parse::<u64>().expect("invalid --duration-seconds"))
            });
        let requests = all_args("--requests")
            .into_iter()
            .last()
            .map(|raw| raw.parse::<u64>().expect("invalid --requests"));
        assert!(
            (offered.is_some() && duration.is_some() && requests.is_none())
                || (offered.is_none() && duration.is_none() && requests.is_some()),
            "choose either --requests or both --offered-rps and --duration-seconds"
        );
        let base_rps = offered
            .as_deref()
            .unwrap_or("0")
            .parse::<f64>()
            .expect("invalid --offered-rps");
        let peak_rps = arg("--peak-rps", Some(offered.as_deref().unwrap_or("0")))
            .parse::<f64>()
            .expect("invalid --peak-rps");
        if offered.is_some() {
            assert!(base_rps > 0.0 && peak_rps >= base_rps);
        }
        let period_ms = arg("--period-ms", Some("10000"))
            .parse::<u64>()
            .expect("invalid --period-ms");
        let duty_cycle_bps = arg("--duty-cycle-bps", Some("2000"))
            .parse::<u32>()
            .expect("invalid --duty-cycle-bps");
        assert!(period_ms > 0 && duty_cycle_bps <= 10_000);
        let max_in_flight = arg("--max-in-flight", Some(&concurrency.to_string()))
            .parse::<usize>()
            .expect("invalid --max-in-flight");
        let rpc_timeout_ms = arg("--rpc-timeout-ms", Some("30000"))
            .parse::<u64>()
            .expect("invalid --rpc-timeout-ms");
        assert!(max_in_flight > 0 && rpc_timeout_ms > 0);

        let run_id = arg(
            "--run-id",
            Some(&format!("signal-emulator-{}", std::process::id())),
        );
        let mut warm_targets = all_args("--warm-target");
        if warm_targets.is_empty() && cache_mode != CacheMode::Miss {
            warm_targets = targets.clone();
        }
        Self {
            targets,
            warm_targets,
            run_id,
            topology: arg("--topology", Some("unspecified")),
            cache_mode,
            hit_ratio_bps,
            context_bytes,
            signals: all_args("--signal"),
            connections_per_target,
            concurrency,
            requests,
            duration,
            base_rps,
            peak_rps,
            pattern: ArrivalPattern::parse(&arg("--pattern", Some("constant"))),
            period: Duration::from_millis(period_ms),
            duty_cycle_bps,
            max_in_flight,
            rpc_timeout: Duration::from_millis(rpc_timeout_ms),
            scheduled_start_epoch_ms: all_args("--scheduled-start-epoch-ms")
                .into_iter()
                .last()
                .map(|raw| {
                    raw.parse::<u64>()
                        .expect("invalid --scheduled-start-epoch-ms")
                }),
        }
    }

    fn rate_at(&self, elapsed: Duration, total: Duration) -> f64 {
        let period = self.period.as_secs_f64();
        let phase = (elapsed.as_secs_f64() % period) / period;
        match self.pattern {
            ArrivalPattern::Constant => self.base_rps,
            ArrivalPattern::Burst => {
                if phase < self.duty_cycle_bps as f64 / 10_000.0 {
                    self.peak_rps
                } else {
                    self.base_rps
                }
            }
            ArrivalPattern::Sawtooth => self.base_rps + (self.peak_rps - self.base_rps) * phase,
            ArrivalPattern::Spike => {
                let midpoint = total.as_secs_f64() / 2.0;
                let half_width = self.period.as_secs_f64() / 2.0;
                if (elapsed.as_secs_f64() - midpoint).abs() <= half_width {
                    self.peak_rps
                } else {
                    self.base_rps
                }
            }
        }
    }
}

#[derive(Default)]
struct EndpointStats {
    selected: u64,
    successful: u64,
    statuses: BTreeMap<String, u64>,
    latencies_us: Vec<u64>,
}

#[derive(Default)]
struct RunStats {
    endpoints: Vec<EndpointStats>,
    scheduler_late_slots: u64,
    scheduler_max_lag_us: u64,
}

impl RunStats {
    fn with_endpoints(count: usize) -> Self {
        Self {
            endpoints: (0..count).map(|_| EndpointStats::default()).collect(),
            ..Self::default()
        }
    }
}

type Clients = Vec<Vec<ClassifyClient<Channel>>>;

#[derive(Clone, Copy, Debug, Default)]
struct CpuUsage {
    usage_usec: u64,
    user_usec: u64,
    system_usec: u64,
}

fn parse_cpu_stat(raw: &str) -> Option<CpuUsage> {
    let values = raw
        .lines()
        .filter_map(|line| line.split_once(' '))
        .filter_map(|(name, value)| value.parse::<u64>().ok().map(|value| (name, value)))
        .collect::<BTreeMap<_, _>>();
    Some(CpuUsage {
        usage_usec: *values.get("usage_usec")?,
        user_usec: *values.get("user_usec").unwrap_or(&0),
        system_usec: *values.get("system_usec").unwrap_or(&0),
    })
}

fn read_cpu_usage() -> Option<CpuUsage> {
    fs::read_to_string("/sys/fs/cgroup/cpu.stat")
        .ok()
        .and_then(|raw| parse_cpu_stat(&raw))
}

fn cpu_report(
    before: Option<CpuUsage>,
    after: Option<CpuUsage>,
    elapsed: Duration,
) -> serde_json::Value {
    match (before, after) {
        (Some(before), Some(after)) if after.usage_usec >= before.usage_usec => {
            let usage_usec = after.usage_usec - before.usage_usec;
            let user_usec = after.user_usec.saturating_sub(before.user_usec);
            let system_usec = after.system_usec.saturating_sub(before.system_usec);
            serde_json::json!({
                "available": true,
                "source": "cgroup-v2-cpu.stat",
                "usage_seconds": usage_usec as f64 / 1_000_000.0,
                "user_seconds": user_usec as f64 / 1_000_000.0,
                "system_seconds": system_usec as f64 / 1_000_000.0,
                "average_cores": usage_usec as f64 / 1_000_000.0 / elapsed.as_secs_f64()
            })
        }
        _ => serde_json::json!({"available": false, "source": "cgroup-v2-cpu.stat"}),
    }
}

async fn connect(target: &str) -> Channel {
    Endpoint::from_shared(format!("http://{target}"))
        .expect("valid target")
        .connect_timeout(Duration::from_secs(10))
        .tcp_nodelay(true)
        .connect()
        .await
        .unwrap_or_else(|error| panic!("connect to {target}: {error}"))
}

async fn connect_all(config: &Config) -> Clients {
    let mut clients = Vec::with_capacity(config.targets.len());
    for target in &config.targets {
        let mut target_clients = Vec::with_capacity(config.connections_per_target);
        for _ in 0..config.connections_per_target {
            target_clients.push(ClassifyClient::new(connect(target).await));
        }
        clients.push(target_clients);
    }
    clients
}

fn stable_context(bytes: usize) -> String {
    let prefix = "benchmark-stable-cache-key|";
    format!("{prefix}{}", "h".repeat(bytes.saturating_sub(prefix.len())))
}

fn unique_context(run_id: &str, sequence: u64, bytes: usize) -> String {
    let prefix = format!("benchmark-unique|{run_id}|{sequence:020}|");
    let mut context = format!("{prefix}{}", "u".repeat(bytes.saturating_sub(prefix.len())));
    context.truncate(bytes);
    context
}

fn is_hit(config: &Config, sequence: u64) -> bool {
    match config.cache_mode {
        CacheMode::Hit => true,
        CacheMode::Miss => false,
        CacheMode::Mixed => sequence % 10_000 < config.hit_ratio_bps as u64,
    }
}

fn request(config: &Config, sequence: u64) -> ClassifyRequest {
    ClassifyRequest {
        request_id: format!("{}-{sequence}", config.run_id),
        session_id: config.run_id.clone(),
        context: if is_hit(config, sequence) {
            stable_context(config.context_bytes)
        } else {
            unique_context(&config.run_id, sequence, config.context_bytes)
        },
        signals: config.signals.clone(),
    }
}

fn response_valid(response: &ClassifyResponse) -> bool {
    response.status == ClassificationStatus::Ok as i32
        && !response.classifier_id.is_empty()
        && !response.ranked.is_empty()
}

async fn warm(config: &Config) {
    if config.cache_mode == CacheMode::Miss {
        return;
    }
    for (index, target) in config.warm_targets.iter().enumerate() {
        let mut client = ClassifyClient::new(connect(target).await);
        let response = client
            .classify(ClassifyRequest {
                request_id: format!("{}-warm-{index}", config.run_id),
                session_id: config.run_id.clone(),
                context: stable_context(config.context_bytes),
                signals: config.signals.clone(),
            })
            .await
            .unwrap_or_else(|error| panic!("warm {target}: {error}"));
        assert!(
            response_valid(response.get_ref()),
            "invalid warm response from {target}"
        );
    }
}

fn status_name(result: &Result<tonic::Response<ClassifyResponse>, tonic::Status>) -> String {
    match result {
        Ok(response) if response_valid(response.get_ref()) => "OK".to_string(),
        Ok(_) => "INVALID_RESPONSE".to_string(),
        Err(status) => format!("GRPC_{:?}", status.code()).to_uppercase(),
    }
}

async fn issue(
    config: Arc<Config>,
    clients: Arc<Clients>,
    stats: Arc<Mutex<RunStats>>,
    sequence: u64,
) {
    let endpoint = sequence as usize % clients.len();
    let connection = (sequence as usize / clients.len()) % clients[endpoint].len();
    let mut client = clients[endpoint][connection].clone();
    let started = Instant::now();
    let result = tokio::time::timeout(
        config.rpc_timeout,
        client.classify(request(&config, sequence)),
    )
    .await;
    let elapsed_us = started.elapsed().as_micros().min(u64::MAX as u128) as u64;
    let (status, successful) = match result {
        Ok(result) => {
            let status = status_name(&result);
            let successful = status == "OK";
            (status, successful)
        }
        Err(_) => ("CLIENT_TIMEOUT".to_string(), false),
    };
    let mut stats = stats.lock().unwrap();
    let endpoint_stats = &mut stats.endpoints[endpoint];
    endpoint_stats.selected += 1;
    *endpoint_stats.statuses.entry(status).or_default() += 1;
    if successful {
        endpoint_stats.successful += 1;
        endpoint_stats.latencies_us.push(elapsed_us);
    }
}

async fn wait_for_scheduled_start(epoch_ms: Option<u64>) {
    let Some(epoch_ms) = epoch_ms else { return };
    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock before epoch")
        .as_millis() as u64;
    assert!(epoch_ms >= now_ms, "scheduled start is in the past");
    tokio::time::sleep(Duration::from_millis(epoch_ms - now_ms)).await;
}

async fn run_closed_loop(
    config: Arc<Config>,
    clients: Arc<Clients>,
    stats: Arc<Mutex<RunStats>>,
) -> Duration {
    let total = config.requests.expect("closed-loop requests");
    let sequence = Arc::new(AtomicU64::new(0));
    let started = Instant::now();
    let mut tasks = JoinSet::new();
    for _ in 0..config.concurrency {
        let config = config.clone();
        let clients = clients.clone();
        let stats = stats.clone();
        let sequence = sequence.clone();
        tasks.spawn(async move {
            loop {
                let current = sequence.fetch_add(1, Ordering::Relaxed);
                if current >= total {
                    break;
                }
                issue(config.clone(), clients.clone(), stats.clone(), current).await;
            }
        });
    }
    while let Some(result) = tasks.join_next().await {
        result.expect("closed-loop worker panic");
    }
    started.elapsed()
}

async fn run_open_loop(
    config: Arc<Config>,
    clients: Arc<Clients>,
    stats: Arc<Mutex<RunStats>>,
) -> Duration {
    let duration = config.duration.expect("open-loop duration");
    let semaphore = Arc::new(Semaphore::new(config.max_in_flight));
    let mut tasks = JoinSet::new();
    let mut sequence = 0_u64;
    let mut offset = Duration::ZERO;
    let started = Instant::now();
    while offset < duration {
        let deadline = tokio::time::Instant::from_std(started + offset);
        tokio::time::sleep_until(deadline).await;
        let lag = Instant::now().saturating_duration_since(started + offset);
        if !lag.is_zero() {
            let mut locked = stats.lock().unwrap();
            locked.scheduler_late_slots += 1;
            locked.scheduler_max_lag_us = locked
                .scheduler_max_lag_us
                .max(lag.as_micros().min(u64::MAX as u128) as u64);
        }
        let permit = semaphore
            .clone()
            .acquire_owned()
            .await
            .expect("semaphore closed");
        let config_for_task = config.clone();
        let clients = clients.clone();
        let stats = stats.clone();
        let current = sequence;
        tasks.spawn(async move {
            let _permit = permit;
            issue(config_for_task, clients, stats, current).await;
        });
        sequence += 1;
        let rate = config.rate_at(offset, duration);
        offset += Duration::from_secs_f64(1.0 / rate);
        while tasks.len() > config.max_in_flight * 2 {
            tasks
                .join_next()
                .await
                .expect("join set empty")
                .expect("open-loop worker panic");
        }
    }
    while let Some(result) = tasks.join_next().await {
        result.expect("open-loop worker panic");
    }
    started.elapsed()
}

fn percentile(values: &mut [u64], quantile: f64) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    values.sort_unstable();
    let rank = (quantile * values.len() as f64).ceil() as usize;
    values[rank.saturating_sub(1).min(values.len() - 1)] as f64 / 1000.0
}

fn report(
    config: &Config,
    elapsed: Duration,
    stats: &mut RunStats,
    driver_cpu: serde_json::Value,
) -> serde_json::Value {
    let selected_total = stats
        .endpoints
        .iter()
        .map(|value| value.selected)
        .sum::<u64>();
    let successful_total = stats
        .endpoints
        .iter()
        .map(|value| value.successful)
        .sum::<u64>();
    let ideal = 1.0 / stats.endpoints.len() as f64;
    let shares = stats
        .endpoints
        .iter()
        .map(|value| value.selected as f64 / selected_total.max(1) as f64)
        .collect::<Vec<_>>();
    let mean = shares.iter().sum::<f64>() / shares.len() as f64;
    let variance = shares
        .iter()
        .map(|share| (share - mean).powi(2))
        .sum::<f64>()
        / shares.len() as f64;
    let coefficient_of_variation = if mean > 0.0 {
        variance.sqrt() / mean
    } else {
        0.0
    };
    let max_share_over_ideal = shares.iter().copied().fold(0.0_f64, f64::max) / ideal;
    let mut aggregate_latencies = stats
        .endpoints
        .iter()
        .flat_map(|value| value.latencies_us.iter().copied())
        .collect::<Vec<_>>();
    let mut p50_values = aggregate_latencies.clone();
    let mut p95_values = aggregate_latencies.clone();
    let p50 = percentile(&mut p50_values, 0.50);
    let p95 = percentile(&mut p95_values, 0.95);
    let p99 = percentile(&mut aggregate_latencies, 0.99);
    let max = aggregate_latencies.last().copied().unwrap_or(0) as f64 / 1000.0;
    let endpoints = stats
        .endpoints
        .iter_mut()
        .enumerate()
        .map(|(index, value)| {
            let mut p99_values = value.latencies_us.clone();
            let endpoint_p99 = percentile(&mut p99_values, 0.99);
            serde_json::json!({
                "target": config.targets[index],
                "selected": value.selected,
                "share": shares[index],
                "successful": value.successful,
                "statuses": value.statuses,
                "successful_p99_ms": endpoint_p99
            })
        })
        .collect::<Vec<_>>();
    serde_json::json!({
        "schema_version": 1,
        "kind": "llm-d-sc-signal-emulator-result",
        "run_id": config.run_id,
        "topology": config.topology,
        "targets": config.targets,
        "warm_targets": config.warm_targets,
        "signals": config.signals,
        "cache": {"mode": config.cache_mode.as_str(), "hit_ratio_bps": config.hit_ratio_bps},
        "payload": {"context_bytes": config.context_bytes},
        "transport": {
            "connections_per_target": config.connections_per_target,
            "total_connections": config.connections_per_target * config.targets.len(),
            "concurrency": config.concurrency
        },
        "arrival": {
            "mode": if config.requests.is_some() {"closed_loop"} else {"open_loop"},
            "pattern": config.pattern.as_str(),
            "base_rps": config.base_rps,
            "peak_rps": config.peak_rps,
            "period_ms": config.period.as_millis(),
            "duty_cycle_bps": config.duty_cycle_bps,
            "requested_requests": config.requests,
            "requested_duration_seconds": config.duration.map(|value| value.as_secs_f64()),
            "scheduler_late_slots": stats.scheduler_late_slots,
            "scheduler_max_lag_ms": stats.scheduler_max_lag_us as f64 / 1000.0
        },
        "elapsed_seconds": elapsed.as_secs_f64(),
        "driver_cpu": driver_cpu,
        "selected_requests": selected_total,
        "successful_requests": successful_total,
        "useful_requests_per_second": successful_total as f64 / elapsed.as_secs_f64(),
        "successful_rtt_ms": {"p50": p50, "p95": p95, "p99": p99, "max": max},
        "endpoint_balance": {
            "coefficient_of_variation": coefficient_of_variation,
            "max_share_over_ideal": max_share_over_ideal,
            "passes_cv_10_percent": coefficient_of_variation <= 0.10,
            "passes_max_share_1_25x": max_share_over_ideal <= 1.25,
            "zero_traffic_endpoints": stats.endpoints.iter().filter(|value| value.selected == 0).count()
        },
        "endpoints": endpoints
    })
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let config = Arc::new(Config::from_args());
    warm(&config).await;
    let clients = Arc::new(connect_all(&config).await);
    let stats = Arc::new(Mutex::new(RunStats::with_endpoints(config.targets.len())));
    wait_for_scheduled_start(config.scheduled_start_epoch_ms).await;
    let cpu_before = read_cpu_usage();
    let elapsed = if config.requests.is_some() {
        run_closed_loop(config.clone(), clients, stats.clone()).await
    } else {
        run_open_loop(config.clone(), clients, stats.clone()).await
    };
    let driver_cpu = cpu_report(cpu_before, read_cpu_usage(), elapsed);
    let mut stats = stats.lock().unwrap();
    println!(
        "{}",
        serde_json::to_string_pretty(&report(&config, elapsed, &mut stats, driver_cpu)).unwrap()
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(pattern: ArrivalPattern) -> Config {
        Config {
            targets: vec!["a:1".into(), "b:1".into()],
            warm_targets: vec![],
            run_id: "test".into(),
            topology: "test".into(),
            cache_mode: CacheMode::Mixed,
            hit_ratio_bps: 2500,
            context_bytes: 64,
            signals: vec!["complexity".into()],
            connections_per_target: 1,
            concurrency: 1,
            requests: None,
            duration: Some(Duration::from_secs(10)),
            base_rps: 10.0,
            peak_rps: 50.0,
            pattern,
            period: Duration::from_secs(4),
            duty_cycle_bps: 2500,
            max_in_flight: 1,
            rpc_timeout: Duration::from_secs(1),
            scheduled_start_epoch_ms: None,
        }
    }

    #[test]
    fn mixed_ratio_is_exact_per_ten_thousand_requests() {
        let config = config(ArrivalPattern::Constant);
        assert_eq!(
            (0..10_000)
                .filter(|sequence| is_hit(&config, *sequence))
                .count(),
            2500
        );
    }

    #[test]
    fn unique_contexts_have_fixed_size_and_do_not_collide() {
        let left = unique_context("run", 1, 64);
        let right = unique_context("run", 2, 64);
        assert_eq!(left.len(), 64);
        assert_eq!(right.len(), 64);
        assert_ne!(left, right);
    }

    #[test]
    fn burst_and_sawtooth_rates_follow_declared_shape() {
        let burst = config(ArrivalPattern::Burst);
        assert_eq!(burst.rate_at(Duration::ZERO, Duration::from_secs(10)), 50.0);
        assert_eq!(
            burst.rate_at(Duration::from_secs(2), Duration::from_secs(10)),
            10.0
        );
        let sawtooth = config(ArrivalPattern::Sawtooth);
        assert_eq!(
            sawtooth.rate_at(Duration::ZERO, Duration::from_secs(10)),
            10.0
        );
        assert_eq!(
            sawtooth.rate_at(Duration::from_secs(2), Duration::from_secs(10)),
            30.0
        );
    }

    #[test]
    fn cgroup_v2_cpu_stat_is_parsed_and_reported() {
        let parsed = parse_cpu_stat("usage_usec 1000000\nuser_usec 750000\nsystem_usec 250000\n")
            .expect("valid cpu.stat");
        assert_eq!(parsed.usage_usec, 1_000_000);
        let report = cpu_report(
            Some(parsed),
            Some(CpuUsage {
                usage_usec: 3_000_000,
                user_usec: 2_000_000,
                system_usec: 1_000_000,
            }),
            Duration::from_secs(2),
        );
        assert_eq!(report["available"], true);
        assert_eq!(report["average_cores"], 1.0);
    }
}
