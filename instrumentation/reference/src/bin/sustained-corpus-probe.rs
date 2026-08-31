#![recursion_limit = "256"]

//! Fixed-duration, exact-token, cache-miss benchmark client.
//!
//! This is an external benchmark driver. It never modifies server behavior.
//! Every warm-up and plateau request claims a distinct corpus row, supplied by
//! JSONL or generated from a disjoint sequence range. Exhaustion is reported
//! instead of cycling into cache hits. Closed loop remains the default;
//! `--offered-rps` opts into deterministic open-loop scheduling.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::{self, Write};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use llm_d_sc::grpc::classify::{generated, ClassifyRequest, ClassifyResponse};
use serde::Deserialize;
use tokio::sync::{Barrier, Semaphore};
use tokio::task::JoinSet;
use tonic::transport::{Channel, Endpoint};

const HISTOGRAM_BOUNDS_US: &[u64] = &[
    250, 500, 1_000, 2_000, 4_000, 8_000, 16_000, 32_000, 64_000, 125_000, 250_000, 500_000,
    1_000_000, 2_000_000, 4_000_000, 8_000_000, 16_000_000, 32_000_000, 60_000_000,
];
const VERIFIED_TOKENIZER_SHA256: &str =
    "851ca67100d372ca3ae031a6abd168f53489eebfd7d89523f35c5c9b4d372c3c";
const NANOS_PER_SECOND: u128 = 1_000_000_000;
const ARMED_SCHEMA: &str = "llm-d-sc.benchmark-driver.armed";
const ARMED_PROTOCOL_VERSION: &str = "sustained-corpus-probe-armed-v1";
const ARMED_CONFIG_CANONICALIZATION: &str = "sorted-string-map-v1";

#[derive(Clone, Deserialize)]
struct CorpusRow {
    sequence: u64,
    context: String,
    token_count_including_specials: usize,
    tokenizer_sha256: String,
}

#[derive(Clone)]
struct Histogram {
    counts: Vec<u64>,
}

impl Histogram {
    fn new() -> Self {
        Self {
            counts: vec![0; HISTOGRAM_BOUNDS_US.len() + 1],
        }
    }

    fn observe(&mut self, micros: u64) {
        let bucket = HISTOGRAM_BOUNDS_US
            .iter()
            .position(|bound| micros <= *bound)
            .unwrap_or(HISTOGRAM_BOUNDS_US.len());
        self.counts[bucket] += 1;
    }

    fn merge(&mut self, other: &Histogram) {
        for (left, right) in self.counts.iter_mut().zip(&other.counts) {
            *left += right;
        }
    }

    fn json(&self) -> serde_json::Value {
        let mut buckets = HISTOGRAM_BOUNDS_US
            .iter()
            .zip(&self.counts)
            .map(|(upper, count)| {
                serde_json::json!({"upper_bound_ms": *upper as f64 / 1000.0, "count": count})
            })
            .collect::<Vec<_>>();
        buckets.push(serde_json::json!({
            "upper_bound_ms": null,
            "count": self.counts[HISTOGRAM_BOUNDS_US.len()]
        }));
        serde_json::json!({
            "semantics": "non_cumulative",
            "unit": "milliseconds",
            "buckets": buckets
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct OfferedRate {
    source: String,
    numerator: u64,
    denominator: u64,
}

impl OfferedRate {
    fn parse(source: String) -> Result<Self, String> {
        if source.is_empty() || source.starts_with('+') || source.starts_with('-') {
            return Err("--offered-rps must be an unsigned decimal".to_string());
        }
        let mut pieces = source.split('.');
        let whole = pieces.next().unwrap();
        let fractional = pieces.next();
        if pieces.next().is_some()
            || whole.is_empty()
            || !whole.bytes().all(|byte| byte.is_ascii_digit())
        {
            return Err("--offered-rps must be an unsigned decimal".to_string());
        }
        let fractional = fractional.unwrap_or("");
        if fractional.len() > 9 || !fractional.bytes().all(|byte| byte.is_ascii_digit()) {
            return Err(
                "--offered-rps supports at most nine decimal places and no exponent".to_string(),
            );
        }
        let denominator = 10_u64
            .checked_pow(fractional.len() as u32)
            .ok_or_else(|| "--offered-rps denominator overflow".to_string())?;
        let whole: u64 = whole
            .parse()
            .map_err(|_| "--offered-rps is too large".to_string())?;
        let fractional: u64 = if fractional.is_empty() {
            0
        } else {
            fractional
                .parse()
                .map_err(|_| "--offered-rps is invalid".to_string())?
        };
        let numerator = whole
            .checked_mul(denominator)
            .and_then(|value| value.checked_add(fractional))
            .ok_or_else(|| "--offered-rps is too large".to_string())?;
        if numerator == 0 {
            return Err("--offered-rps must be greater than zero".to_string());
        }
        if numerator as u128 > NANOS_PER_SECOND * denominator as u128 {
            return Err("--offered-rps cannot exceed 1,000,000,000".to_string());
        }
        let divisor = gcd(numerator, denominator);
        Ok(Self {
            source,
            numerator: numerator / divisor,
            denominator: denominator / divisor,
        })
    }

    fn slots_for(&self, duration_seconds: u64) -> usize {
        let numerator = duration_seconds as u128 * self.numerator as u128;
        let denominator = self.denominator as u128;
        let slots = numerator.div_ceil(denominator);
        usize::try_from(slots).expect("open-loop offered slot count exceeds usize")
    }

    fn offset_for(&self, slot: usize) -> Duration {
        let nanos = (slot as u128)
            .checked_mul(NANOS_PER_SECOND)
            .and_then(|value| value.checked_mul(self.denominator as u128))
            .expect("open-loop scheduled offset arithmetic overflow")
            / self.numerator as u128;
        Duration::from_nanos(
            u64::try_from(nanos).expect("open-loop scheduled offset exceeds u64 nanoseconds"),
        )
    }

    fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "requested_decimal": self.source,
            "exact_rational_requests_per_second": {
                "numerator": self.numerator,
                "denominator": self.denominator
            }
        })
    }
}

fn gcd(mut left: u64, mut right: u64) -> u64 {
    while right != 0 {
        let remainder = left % right;
        left = right;
        right = remainder;
    }
    left
}

#[derive(Clone, Debug)]
struct OpenLoopConfig {
    offered_rate: OfferedRate,
    max_in_flight: usize,
    dispatch_late_after: Duration,
    drop_late_after: Duration,
    rpc_timeout: Duration,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ArmedIdentity {
    run_id: String,
    job_id: String,
    nonce: String,
}

struct ArmedRecordInput<'a> {
    identity: &'a ArmedIdentity,
    target: &'a str,
    scheduled_start_epoch_ms: u64,
    expected_slots: usize,
    duration_seconds: u64,
    armed_epoch_ms: u64,
    scheduled_rows_blake3: &'a str,
    config: &'a OpenLoopConfig,
    target_image: &'a str,
    driver_image: &'a Option<String>,
    model_sha256: &'a str,
    tokenizer_sha256: &'a str,
    topology: &'a str,
    corpus_mode: &'a str,
    corpus_blake3: &'a Option<String>,
    generator_scheme: &'a Option<&'a str>,
    selected_rows_blake3: &'a str,
    corpus_offset: usize,
    first_sequence: u64,
    last_sequence: u64,
    warmup_requests: usize,
    candidate_rows: usize,
    token_count: usize,
    connections: usize,
    concurrency: usize,
    raw_latencies: bool,
}

impl OpenLoopConfig {
    fn from_args(offered_rps: String, default_max_in_flight: usize) -> Self {
        let offered_rate =
            OfferedRate::parse(offered_rps).unwrap_or_else(|error| panic!("{error}"));
        let default_max_in_flight = default_max_in_flight.to_string();
        let max_in_flight: usize = arg("--max-in-flight", Some(&default_max_in_flight))
            .parse()
            .expect("--max-in-flight must be an integer");
        let dispatch_late_after_ms: u64 = arg("--dispatch-late-after-ms", Some("1"))
            .parse()
            .expect("--dispatch-late-after-ms must be an integer");
        let drop_late_after_ms: u64 = arg("--drop-late-after-ms", Some("100"))
            .parse()
            .expect("--drop-late-after-ms must be an integer");
        let rpc_timeout_ms: u64 = arg("--rpc-timeout-ms", Some("30000"))
            .parse()
            .expect("--rpc-timeout-ms must be an integer");
        assert!(max_in_flight > 0, "--max-in-flight must be positive");
        assert!(rpc_timeout_ms > 0, "--rpc-timeout-ms must be positive");
        assert!(
            drop_late_after_ms >= dispatch_late_after_ms,
            "--drop-late-after-ms must be at least --dispatch-late-after-ms"
        );
        Self {
            offered_rate,
            max_in_flight,
            dispatch_late_after: Duration::from_millis(dispatch_late_after_ms),
            drop_late_after: Duration::from_millis(drop_late_after_ms),
            rpc_timeout: Duration::from_millis(rpc_timeout_ms),
        }
    }
}

fn armed_identity(
    run_id: Option<String>,
    job_id: Option<String>,
    nonce: Option<String>,
) -> Result<Option<ArmedIdentity>, String> {
    match (run_id, job_id, nonce) {
        (None, None, None) => Ok(None),
        (Some(run_id), Some(job_id), Some(nonce)) => {
            validate_armed_identity_component("--armed-run-id", &run_id)?;
            validate_armed_identity_component("--armed-job-id", &job_id)?;
            validate_armed_identity_component("--armed-nonce", &nonce)?;
            Ok(Some(ArmedIdentity {
                run_id,
                job_id,
                nonce,
            }))
        }
        _ => Err(
            "--armed-run-id, --armed-job-id, and --armed-nonce must be supplied together"
                .to_string(),
        ),
    }
}

fn validate_armed_identity_component(name: &str, value: &str) -> Result<(), String> {
    if value.is_empty() || value.len() > 253 {
        return Err(format!("{name} must contain 1 to 253 characters"));
    }
    if !value
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || b"-._:".contains(&byte))
    {
        return Err(format!(
            "{name} may contain only ASCII letters, digits, '-', '.', '_', and ':'"
        ));
    }
    Ok(())
}

fn scheduled_rows_digest(rows: &[CorpusRow], offered_slots: usize) -> String {
    assert!(
        offered_slots <= rows.len(),
        "--max-rows must reserve at least {offered_slots} rows for this open-loop schedule"
    );
    let mut hasher = blake3::Hasher::new();
    for row in &rows[..offered_slots] {
        hasher.update(&row.sequence.to_le_bytes());
        hasher.update(row.context.as_bytes());
        hasher.update(&[0]);
    }
    hasher.finalize().to_hex().to_string()
}

// ARMED config canonicalization contract (`sorted-string-map-v1`): build one
// flat JSON object from a BTreeMap, preserving JSON scalar/null types; serialize
// the exact emitted `config` value as compact UTF-8 with serde_json::to_vec;
// hash those bytes with BLAKE3 and encode the 32-byte result as lower-case hex.
// Orchestrators must compare the explicit fields to their frozen plan before
// release.  The digest is an integrity/linkage check, not a substitute for that
// comparison.
fn armed_config(input: &ArmedRecordInput<'_>) -> serde_json::Value {
    let mut canonical = BTreeMap::<&str, serde_json::Value>::new();
    canonical.insert("candidate_rows", input.candidate_rows.into());
    canonical.insert("closed_loop_concurrency_argument", input.concurrency.into());
    canonical.insert("connections", input.connections.into());
    canonical.insert(
        "corpus_blake3",
        serde_json::to_value(input.corpus_blake3)
            .expect("corpus digest must serialize for ARMED config"),
    );
    canonical.insert("corpus_mode", input.corpus_mode.into());
    canonical.insert("corpus_offset", input.corpus_offset.into());
    canonical.insert(
        "dispatch_late_after_ms",
        u64::try_from(input.config.dispatch_late_after.as_millis())
            .expect("dispatch lateness exceeds u64")
            .into(),
    );
    canonical.insert(
        "driver_image",
        serde_json::to_value(input.driver_image)
            .expect("driver image must serialize for ARMED config"),
    );
    canonical.insert("driver_package_version", env!("CARGO_PKG_VERSION").into());
    canonical.insert(
        "drop_late_after_ms",
        u64::try_from(input.config.drop_late_after.as_millis())
            .expect("drop lateness exceeds u64")
            .into(),
    );
    canonical.insert("duration_seconds", input.duration_seconds.into());
    canonical.insert("expected_slots", input.expected_slots.into());
    canonical.insert("first_sequence", input.first_sequence.into());
    canonical.insert(
        "generator_scheme",
        serde_json::to_value(input.generator_scheme)
            .expect("generator scheme must serialize for ARMED config"),
    );
    canonical.insert("job_id", input.identity.job_id.as_str().into());
    canonical.insert("last_sequence", input.last_sequence.into());
    canonical.insert("max_in_flight", input.config.max_in_flight.into());
    canonical.insert("model_sha256", input.model_sha256.into());
    canonical.insert("nonce", input.identity.nonce.as_str().into());
    canonical.insert(
        "offered_rate_denominator",
        input.config.offered_rate.denominator.into(),
    );
    canonical.insert(
        "offered_rate_numerator",
        input.config.offered_rate.numerator.into(),
    );
    canonical.insert(
        "offered_rate_requested_decimal",
        input.config.offered_rate.source.as_str().into(),
    );
    canonical.insert(
        "offered_rps",
        input.config.offered_rate.source.as_str().into(),
    );
    canonical.insert("protocol_version", ARMED_PROTOCOL_VERSION.into());
    canonical.insert("raw_latencies", input.raw_latencies.into());
    canonical.insert(
        "rpc_timeout_ms",
        u64::try_from(input.config.rpc_timeout.as_millis())
            .expect("RPC timeout exceeds u64")
            .into(),
    );
    canonical.insert("run_id", input.identity.run_id.as_str().into());
    canonical.insert("scheduled_rows_blake3", input.scheduled_rows_blake3.into());
    canonical.insert(
        "scheduled_start_epoch_ms",
        input.scheduled_start_epoch_ms.into(),
    );
    canonical.insert("selected_rows_blake3", input.selected_rows_blake3.into());
    canonical.insert("target_endpoint", input.target.into());
    canonical.insert("target_image", input.target_image.into());
    canonical.insert("token_count_including_specials", input.token_count.into());
    canonical.insert("tokenizer_sha256", input.tokenizer_sha256.into());
    canonical.insert("topology", input.topology.into());
    canonical.insert("warmup_requests", input.warmup_requests.into());

    serde_json::to_value(canonical).expect("ARMED config canonicalization must serialize")
}

fn armed_config_digest(config: &serde_json::Value) -> String {
    let canonical_json =
        serde_json::to_vec(config).expect("ARMED canonical config must serialize for hashing");
    blake3::hash(&canonical_json).to_hex().to_string()
}

fn armed_record(input: &ArmedRecordInput<'_>) -> serde_json::Value {
    let config = armed_config(input);
    let config_digest = armed_config_digest(&config);
    serde_json::json!({
        "schema": ARMED_SCHEMA,
        "schema_version": 1,
        "record_type": "ARMED",
        "protocol_version": ARMED_PROTOCOL_VERSION,
        "run_id": input.identity.run_id,
        "job_id": input.identity.job_id,
        "nonce": input.identity.nonce,
        "endpoint": input.target,
        "scheduled_start_epoch_ms": input.scheduled_start_epoch_ms,
        "expected_slots": input.expected_slots,
        "duration_seconds": input.duration_seconds,
        "armed_epoch_ms": input.armed_epoch_ms,
        "scheduled_rows_blake3": input.scheduled_rows_blake3,
        "config": config,
        "config_digest": {
            "algorithm": "blake3",
            "canonicalization": ARMED_CONFIG_CANONICALIZATION,
            "hex": config_digest
        }
    })
}

fn write_armed_record(
    writer: &mut impl Write,
    record: &serde_json::Value,
) -> Result<(), io::Error> {
    let line = serde_json::to_string(record).expect("ARMED record must serialize");
    assert!(
        !line.contains('\n') && !line.contains('\r'),
        "ARMED record must remain one physical line"
    );
    writeln!(writer, "{line}")?;
    writer.flush()
}

fn emit_armed_record(record: &serde_json::Value) {
    let stdout = io::stdout();
    let mut stdout = stdout.lock();
    write_armed_record(&mut stdout, record).expect("cannot write and flush ARMED record to stdout");
}

struct OpenLoopCompletion {
    status: String,
    rtt_us: u64,
    completed_within_plateau: bool,
}

struct OpenLoopResults {
    offered_slots: usize,
    initiated: u64,
    completed: u64,
    completed_within_plateau: u64,
    completed_after_plateau: u64,
    dropped_in_flight_limit: u64,
    dropped_schedule_late: u64,
    dispatch_late_slots: u64,
    initiated_late: u64,
    statuses: BTreeMap<String, u64>,
    statuses_within_plateau: BTreeMap<String, u64>,
    statuses_after_plateau: BTreeMap<String, u64>,
    rtt_raw_us_by_status: BTreeMap<String, Vec<u64>>,
    successful_within_plateau_rtt_raw_us: Vec<u64>,
    dispatch_lag_raw_us: Vec<u64>,
    dropped_in_flight_lag_raw_us: Vec<u64>,
    dropped_schedule_lag_raw_us: Vec<u64>,
    successful_within_plateau_histogram: Histogram,
}

impl OpenLoopResults {
    fn new(offered_slots: usize) -> Self {
        Self {
            offered_slots,
            initiated: 0,
            completed: 0,
            completed_within_plateau: 0,
            completed_after_plateau: 0,
            dropped_in_flight_limit: 0,
            dropped_schedule_late: 0,
            dispatch_late_slots: 0,
            initiated_late: 0,
            statuses: BTreeMap::new(),
            statuses_within_plateau: BTreeMap::new(),
            statuses_after_plateau: BTreeMap::new(),
            rtt_raw_us_by_status: BTreeMap::new(),
            successful_within_plateau_rtt_raw_us: Vec::new(),
            dispatch_lag_raw_us: Vec::new(),
            dropped_in_flight_lag_raw_us: Vec::new(),
            dropped_schedule_lag_raw_us: Vec::new(),
            successful_within_plateau_histogram: Histogram::new(),
        }
    }

    fn record_completion(&mut self, completion: OpenLoopCompletion) {
        self.completed += 1;
        *self.statuses.entry(completion.status.clone()).or_default() += 1;
        self.rtt_raw_us_by_status
            .entry(completion.status.clone())
            .or_default()
            .push(completion.rtt_us);
        if completion.completed_within_plateau {
            self.completed_within_plateau += 1;
            *self
                .statuses_within_plateau
                .entry(completion.status.clone())
                .or_default() += 1;
            if completion.status == "OK" {
                self.successful_within_plateau_histogram
                    .observe(completion.rtt_us);
                self.successful_within_plateau_rtt_raw_us
                    .push(completion.rtt_us);
            }
        } else {
            self.completed_after_plateau += 1;
            *self
                .statuses_after_plateau
                .entry(completion.status)
                .or_default() += 1;
        }
    }

    fn finalize(&mut self) {
        self.dispatch_lag_raw_us.sort_unstable();
        self.dropped_in_flight_lag_raw_us.sort_unstable();
        self.dropped_schedule_lag_raw_us.sort_unstable();
        for values in self.rtt_raw_us_by_status.values_mut() {
            values.sort_unstable();
        }
        self.successful_within_plateau_rtt_raw_us.sort_unstable();
        let dropped = self.dropped_in_flight_limit + self.dropped_schedule_late;
        assert_eq!(
            self.offered_slots as u64,
            self.initiated + dropped,
            "every offered slot must be initiated or dropped"
        );
        assert_eq!(
            self.initiated, self.completed,
            "every initiated RPC must finish during the bounded drain"
        );
        assert_eq!(
            self.completed,
            self.completed_within_plateau + self.completed_after_plateau,
            "every completion must be assigned to the plateau or drain"
        );
    }
}

async fn run_open_loop(
    channels: &[Channel],
    rows: &[CorpusRow],
    config: &OpenLoopConfig,
    start_instant: Instant,
    deadline: Instant,
    duration_seconds: u64,
    run_id: &str,
) -> OpenLoopResults {
    let offered_slots = config.offered_rate.slots_for(duration_seconds);
    assert!(
        offered_slots <= rows.len(),
        "open-loop schedule requires {offered_slots} candidate rows, but only {} were selected",
        rows.len()
    );
    let semaphore = Arc::new(Semaphore::new(config.max_in_flight));
    let mut tasks = JoinSet::<OpenLoopCompletion>::new();
    let mut results = OpenLoopResults::new(offered_slots);

    for slot in 0..offered_slots {
        let scheduled = start_instant + config.offered_rate.offset_for(slot);
        tokio::time::sleep_until(tokio::time::Instant::from_std(scheduled)).await;
        while let Some(completion) = tasks.try_join_next() {
            results.record_completion(completion.expect("open-loop RPC task panicked"));
        }

        let dispatch_time = Instant::now();
        let dispatch_lag = dispatch_time.saturating_duration_since(scheduled);
        let dispatch_lag_us = dispatch_lag.as_micros() as u64;
        let dispatch_is_late = dispatch_lag > config.dispatch_late_after;
        if dispatch_is_late {
            results.dispatch_late_slots += 1;
        }
        if dispatch_time >= deadline || dispatch_lag > config.drop_late_after {
            results.dropped_schedule_late += 1;
            results.dropped_schedule_lag_raw_us.push(dispatch_lag_us);
            continue;
        }

        let Ok(permit) = semaphore.clone().try_acquire_owned() else {
            results.dropped_in_flight_limit += 1;
            results.dropped_in_flight_lag_raw_us.push(dispatch_lag_us);
            continue;
        };
        results.initiated += 1;
        if dispatch_is_late {
            results.initiated_late += 1;
        }
        results.dispatch_lag_raw_us.push(dispatch_lag_us);

        let row = rows[slot].clone();
        let mut client = generated::classify_client::ClassifyClient::new(
            channels[slot % channels.len()].clone(),
        );
        let request_id = format!("{run_id}-open-{slot}");
        let rpc_timeout = config.rpc_timeout;
        tasks.spawn(async move {
            let _permit = permit;
            let started = Instant::now();
            let result = tokio::time::timeout(
                rpc_timeout,
                client.classify(request(request_id, row.context)),
            )
            .await;
            let rtt_us = started.elapsed().as_micros() as u64;
            let status = match result {
                Ok(Ok(response)) if valid(response.get_ref()) => "OK".to_string(),
                Ok(Ok(_)) => "INVALID_RESPONSE".to_string(),
                Ok(Err(status)) => format!("GRPC_{:?}", status.code()).to_uppercase(),
                Err(_) => "CLIENT_RPC_TIMEOUT".to_string(),
            };
            OpenLoopCompletion {
                status,
                rtt_us,
                completed_within_plateau: Instant::now() <= deadline,
            }
        });
    }

    while let Some(completion) = tasks.join_next().await {
        results.record_completion(completion.expect("open-loop RPC task panicked"));
    }
    results.finalize();
    results
}

fn optional_arg(name: &str) -> Option<String> {
    let argv: Vec<String> = std::env::args().collect();
    argv.iter()
        .position(|value| value == name)
        .and_then(|index| argv.get(index + 1).cloned())
}

fn has_flag(name: &str) -> bool {
    std::env::args().any(|value| value == name)
}

fn arg(name: &str, default: Option<&str>) -> String {
    optional_arg(name)
        .or_else(|| default.map(str::to_owned))
        .unwrap_or_else(|| panic!("missing required argument {name}"))
}

fn generated_rows(
    token_count: usize,
    sequence_base: u64,
    count: usize,
    tokenizer_sha256: &str,
) -> Vec<CorpusRow> {
    assert_eq!(
        tokenizer_sha256, VERIFIED_TOKENIZER_SHA256,
        "generated mode is only verified for the pinned tokenizer digest"
    );
    assert!(token_count >= 3, "--token-count must be at least 3");
    let identity_bits = (token_count - 2).min(63);
    let capacity = 1_u128 << identity_bits;
    let end = sequence_base as u128 + count as u128;
    assert!(
        end <= capacity,
        "generated sequence range exceeds exact-token capacity"
    );
    (0..count)
        .map(|offset| {
            let sequence = sequence_base + offset as u64;
            let mut words = (0..identity_bits)
                .map(|bit| {
                    if sequence & (1_u64 << bit) == 0 {
                        "alpha"
                    } else {
                        "bravo"
                    }
                })
                .collect::<Vec<_>>();
            words.extend(std::iter::repeat("service").take(token_count - 2 - identity_bits));
            CorpusRow {
                sequence,
                context: words.join(" "),
                token_count_including_specials: token_count,
                tokenizer_sha256: tokenizer_sha256.to_string(),
            }
        })
        .collect()
}

fn now_epoch_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before Unix epoch")
        .as_millis() as u64
}

fn request(id: String, context: String) -> ClassifyRequest {
    ClassifyRequest {
        request_id: id,
        session_id: "sustained-corpus-probe".to_string(),
        context,
        signals: Vec::new(),
    }
}

fn valid(response: &ClassifyResponse) -> bool {
    response.status == 1
        && !response.ranked.is_empty()
        && !response.classifier_id.is_empty()
        && !response.model_revision.is_empty()
        && !response.tokenizer_revision.is_empty()
}

async fn connect(target: &str) -> Channel {
    Endpoint::from_shared(format!("http://{target}"))
        .expect("target must be a valid URI authority")
        .connect_timeout(Duration::from_secs(5))
        .tcp_nodelay(true)
        .connect()
        .await
        .unwrap_or_else(|error| panic!("cannot connect to {target}: {error}"))
}

fn validate_rows(rows: &[CorpusRow]) -> (usize, String) {
    assert!(!rows.is_empty(), "selected corpus must not be empty");
    let token_counts = rows
        .iter()
        .map(|row| row.token_count_including_specials)
        .collect::<BTreeSet<_>>();
    let tokenizer_hashes = rows
        .iter()
        .map(|row| row.tokenizer_sha256.clone())
        .collect::<BTreeSet<_>>();
    let contexts = rows.iter().map(|row| &row.context).collect::<BTreeSet<_>>();
    let sequences = rows.iter().map(|row| row.sequence).collect::<BTreeSet<_>>();
    assert_eq!(
        token_counts.len(),
        1,
        "selected corpus must have one token count"
    );
    assert_eq!(
        tokenizer_hashes.len(),
        1,
        "selected corpus must have one tokenizer digest"
    );
    assert_eq!(
        contexts.len(),
        rows.len(),
        "selected contexts must be globally unique"
    );
    assert_eq!(
        sequences.len(),
        rows.len(),
        "selected sequence IDs must be globally unique"
    );
    (
        *token_counts.iter().next().unwrap(),
        tokenizer_hashes.iter().next().unwrap().clone(),
    )
}

#[tokio::main]
async fn main() {
    let target = arg("--target", Some("127.0.0.1:50051"));
    let corpus_path = optional_arg("--corpus");
    let corpus_offset: usize = arg("--corpus-offset", Some("0")).parse().unwrap();
    let candidate_rows: usize = optional_arg("--max-rows")
        .or_else(|| optional_arg("--candidate-rows"))
        .expect("missing required argument --max-rows (or --candidate-rows)")
        .parse()
        .unwrap();
    let concurrency: usize = arg("--concurrency", Some("4")).parse().unwrap();
    let connections: usize = arg("--connections", Some("1")).parse().unwrap();
    let default_warmup_requests = connections.to_string();
    let warmup_requests: usize = arg("--warmup-requests", Some(&default_warmup_requests))
        .parse()
        .unwrap();
    let duration_seconds: u64 = arg("--duration-seconds", Some("60")).parse().unwrap();
    let requested_start_epoch_ms: u64 = arg("--start-epoch-ms", Some("0")).parse().unwrap();
    let target_image = arg("--target-image", None);
    let driver_image = optional_arg("--driver-image");
    let model_sha256 = arg("--model-sha256", None);
    let topology = arg("--topology", None);
    let raw_latencies = has_flag("--raw-latencies");
    let open_loop = optional_arg("--offered-rps")
        .map(|offered_rps| OpenLoopConfig::from_args(offered_rps, concurrency));
    let armed_identity = armed_identity(
        optional_arg("--armed-run-id"),
        optional_arg("--armed-job-id"),
        optional_arg("--armed-nonce"),
    )
    .unwrap_or_else(|error| panic!("{error}"));

    assert!(concurrency > 0 && connections > 0 && duration_seconds > 0);
    assert!(candidate_rows > 0);
    if armed_identity.is_some() {
        assert!(
            open_loop.is_some(),
            "the ARMED protocol requires --offered-rps"
        );
        assert!(
            requested_start_epoch_ms > 0,
            "the ARMED protocol requires an explicit nonzero --start-epoch-ms"
        );
    }

    let total_rows = warmup_requests
        .checked_add(candidate_rows)
        .expect("row count overflow");
    let (selected, corpus_blake3, corpus_mode, generator_scheme) = if let Some(path) = corpus_path {
        let bytes = fs::read(path).expect("corpus must be readable");
        let rows = String::from_utf8(bytes.clone())
            .expect("corpus must be UTF-8")
            .lines()
            .filter(|line| !line.trim().is_empty())
            .map(|line| serde_json::from_str::<CorpusRow>(line).expect("valid corpus JSONL row"))
            .collect::<Vec<_>>();
        let selected_end = corpus_offset
            .checked_add(total_rows)
            .expect("corpus selection overflow");
        assert!(
            selected_end <= rows.len(),
            "selected corpus slice is unavailable"
        );
        (
            rows[corpus_offset..selected_end].to_vec(),
            Some(blake3::hash(&bytes).to_hex().to_string()),
            "jsonl",
            None,
        )
    } else {
        assert_eq!(
            corpus_offset, 0,
            "--corpus-offset only applies to JSONL mode"
        );
        let token_count = arg("--token-count", None).parse().unwrap();
        let sequence_base = arg("--sequence-base", None).parse().unwrap();
        let tokenizer = arg("--tokenizer-sha256", None);
        (
            generated_rows(token_count, sequence_base, total_rows, &tokenizer),
            None,
            "generated",
            Some("alpha_bravo_lsb_identity_service_fill_v1"),
        )
    };
    let selected = &selected;
    let (token_count, tokenizer_sha256) = validate_rows(selected);
    let mut selected_hasher = blake3::Hasher::new();
    for row in selected {
        selected_hasher.update(&row.sequence.to_le_bytes());
        selected_hasher.update(row.context.as_bytes());
        selected_hasher.update(&[0]);
    }
    let selected_rows_blake3 = selected_hasher.finalize().to_hex().to_string();
    let first_sequence = selected.first().unwrap().sequence;
    let last_sequence = selected.last().unwrap().sequence;
    let warmup = selected[..warmup_requests].to_vec();
    let plateau_rows = Arc::new(selected[warmup_requests..].to_vec());

    let mut channels = Vec::with_capacity(connections);
    for _ in 0..connections {
        channels.push(connect(&target).await);
    }

    // The opt-in ARMED record is deliberately emitted before the first RPC.
    // Existing callers remain a one-object stdout stream unless they provide
    // all three --armed-* identity arguments.
    let mut armed_schedule = None;
    if let Some(identity) = armed_identity.as_ref() {
        let config = open_loop
            .as_ref()
            .expect("ARMED open-loop validation must have completed");
        let accepted_epoch_ms = now_epoch_ms();
        assert!(
            accepted_epoch_ms < requested_start_epoch_ms,
            "connections did not complete before the explicit ARMED start epoch"
        );
        let offered_slots = config.offered_rate.slots_for(duration_seconds);
        let scheduled_rows_blake3 = scheduled_rows_digest(plateau_rows.as_slice(), offered_slots);
        let scheduler_ready_epoch_ms = now_epoch_ms();
        assert!(
            scheduler_ready_epoch_ms < requested_start_epoch_ms,
            "scheduler did not become ready before the explicit ARMED start epoch"
        );
        let record = armed_record(&ArmedRecordInput {
            identity,
            target: &target,
            scheduled_start_epoch_ms: requested_start_epoch_ms,
            expected_slots: offered_slots,
            duration_seconds,
            armed_epoch_ms: scheduler_ready_epoch_ms,
            scheduled_rows_blake3: &scheduled_rows_blake3,
            config,
            target_image: &target_image,
            driver_image: &driver_image,
            model_sha256: &model_sha256,
            tokenizer_sha256: &tokenizer_sha256,
            topology: &topology,
            corpus_mode,
            corpus_blake3: &corpus_blake3,
            generator_scheme: &generator_scheme,
            selected_rows_blake3: &selected_rows_blake3,
            corpus_offset,
            first_sequence,
            last_sequence,
            warmup_requests,
            candidate_rows,
            token_count,
            connections,
            concurrency,
            raw_latencies,
        });
        emit_armed_record(&record);
        armed_schedule = Some((
            offered_slots,
            scheduled_rows_blake3,
            scheduler_ready_epoch_ms,
        ));
    }

    for (index, row) in warmup.iter().enumerate() {
        let mut client = generated::classify_client::ClassifyClient::new(
            channels[index % channels.len()].clone(),
        );
        let response = client
            .classify(request(format!("warmup-{index}"), row.context.clone()))
            .await
            .expect("warm-up RPC failed");
        assert!(valid(response.get_ref()), "warm-up response invalid");
    }

    let start_epoch_ms = if requested_start_epoch_ms == 0 {
        now_epoch_ms() + 10_000
    } else {
        requested_start_epoch_ms
    };
    let ready_epoch_ms = now_epoch_ms();
    assert!(
        ready_epoch_ms < start_epoch_ms,
        "connections and warm-up did not complete before shared start epoch"
    );
    let start_instant = Instant::now() + Duration::from_millis(start_epoch_ms - ready_epoch_ms);
    let deadline = start_instant + Duration::from_secs(duration_seconds);
    let run_id = format!("{}-{start_epoch_ms}", std::process::id());

    if let Some(config) = open_loop {
        let (offered_slots, scheduled_rows_blake3, scheduler_ready_epoch_ms) =
            if let Some(schedule) = armed_schedule {
                schedule
            } else {
                let offered_slots = config.offered_rate.slots_for(duration_seconds);
                let scheduled_rows_blake3 =
                    scheduled_rows_digest(plateau_rows.as_slice(), offered_slots);
                let scheduler_ready_epoch_ms = now_epoch_ms();
                assert!(
                    scheduler_ready_epoch_ms < start_epoch_ms,
                    "scheduler did not become ready before shared start epoch"
                );
                (
                    offered_slots,
                    scheduled_rows_blake3,
                    scheduler_ready_epoch_ms,
                )
            };
        let results = run_open_loop(
            &channels,
            plateau_rows.as_slice(),
            &config,
            start_instant,
            deadline,
            duration_seconds,
            &run_id,
        )
        .await;
        assert_eq!(
            results.offered_slots, offered_slots,
            "prepared and executed open-loop schedules disagree"
        );
        let successful_within_plateau = results
            .statuses_within_plateau
            .get("OK")
            .copied()
            .unwrap_or(0);
        let dropped_total = results.dropped_in_flight_limit + results.dropped_schedule_late;
        let dropped_or_late_union = dropped_total + results.initiated_late;
        let open_loop_provenance = serde_json::json!({
            "protocol_version": "deterministic_offered_rate_v1",
            "driver_package_version": env!("CARGO_PKG_VERSION"),
            "driver_image": driver_image,
            "offered_rate": config.offered_rate.json(),
            "schedule_origin": "shared start_epoch_ms",
            "schedule_formula": "slot k due at start + floor(k * 1e9 * rate_denominator / rate_numerator) nanoseconds",
            "slot_interval": "half-open [start, start + duration)",
            "catch_up_policy": "never burst slots delayed beyond drop_late_after_ms; count them as dropped_schedule_late",
            "max_in_flight": config.max_in_flight,
            "dispatch_late_after_ms": config.dispatch_late_after.as_millis(),
            "drop_late_after_ms": config.drop_late_after.as_millis(),
            "rpc_timeout_ms": config.rpc_timeout.as_millis(),
            "raw_latency_flag_supplied": raw_latencies,
            "raw_rtt_collection": "always enabled in open-loop mode"
        });
        let accounting = serde_json::json!({
            "offered_slots": results.offered_slots,
            "initiated_requests": results.initiated,
            "completed_requests": results.completed,
            "completed_within_plateau": results.completed_within_plateau,
            "completed_after_plateau": results.completed_after_plateau,
            "dropped_before_initiation_total": dropped_total,
            "dropped_in_flight_limit": results.dropped_in_flight_limit,
            "dropped_schedule_late": results.dropped_schedule_late,
            "dispatch_late_slots": results.dispatch_late_slots,
            "initiated_late": results.initiated_late,
            "dropped_or_late_slots_union": dropped_or_late_union,
            "invariants": {
                "offered_equals_initiated_plus_dropped": true,
                "initiated_equals_completed_after_drain": true,
                "completed_equals_plateau_plus_drain": true
            }
        });
        let latency_semantics = serde_json::json!({
            "rtt_by_status": "per-status client RPC RTT for every initiated request; microseconds; sorted arrays; includes completions during the drain",
            "successful_within_plateau_rtt": "successful client RPC RTTs completed within the plateau only; microseconds; sorted; the percentile population",
            "dispatch_lag": "actual initiation decision minus deterministic scheduled dispatch time; microseconds; sorted",
            "coordinated_omission_correction": "none",
            "population_warning": "dropped offered slots have no RTT and are represented only by explicit drop/late counters; do not infer their latency from completed requests"
        });
        let report = serde_json::json!({
            "schema_version": 2,
            "probe": "sustained_exact_token_corpus",
            "load_model": "open_loop_deterministic_offered_rate",
            "target": target,
            "topology": topology,
            "target_image": target_image,
            "model_sha256": model_sha256,
            "tokenizer_sha256": tokenizer_sha256,
            "corpus_mode": corpus_mode,
            "corpus_blake3": corpus_blake3,
            "generator_scheme": generator_scheme,
            "selected_rows_blake3": selected_rows_blake3,
            "scheduled_rows_blake3": scheduled_rows_blake3,
            "corpus_offset": corpus_offset,
            "first_sequence": first_sequence,
            "last_sequence": last_sequence,
            "warmup_requests": warmup_requests,
            "candidate_rows": candidate_rows,
            "scheduled_plateau_rows": results.offered_slots,
            "claimed_plateau_rows": results.initiated,
            "token_count_including_specials": token_count,
            "connections": connections,
            "closed_loop_concurrency_argument": concurrency,
            "ready_epoch_ms": ready_epoch_ms,
            "scheduler_ready_epoch_ms": scheduler_ready_epoch_ms,
            "start_epoch_ms": start_epoch_ms,
            "duration_seconds": duration_seconds,
            "drain_completed_epoch_ms": now_epoch_ms(),
            "corpus_exhausted": false,
            "open_loop": open_loop_provenance,
            "accounting": accounting,
            "statuses_completed_total": results.statuses,
            "statuses_completed_within_plateau": results.statuses_within_plateau,
            "drained_after_plateau": results.statuses_after_plateau,
            "offered_requests_per_second": config.offered_rate.numerator as f64
                / config.offered_rate.denominator as f64,
            "scheduled_offered_slots_per_second": results.offered_slots as f64
                / duration_seconds as f64,
            "initiated_requests_per_second": results.initiated as f64 / duration_seconds as f64,
            "useful_requests_per_second": successful_within_plateau as f64
                / duration_seconds as f64,
            "successful_rtt_histogram": results.successful_within_plateau_histogram.json(),
            "successful_rtt_raw_us": results.successful_within_plateau_rtt_raw_us,
            "rtt_raw_us_by_status": results.rtt_raw_us_by_status,
            "dispatch_lag_raw_us": results.dispatch_lag_raw_us,
            "dropped_in_flight_lag_raw_us": results.dropped_in_flight_lag_raw_us,
            "dropped_schedule_lag_raw_us": results.dropped_schedule_lag_raw_us,
            "latency_semantics": latency_semantics
        });
        println!("{}", serde_json::to_string_pretty(&report).unwrap());
        return;
    }

    let ready_barrier = Arc::new(Barrier::new(concurrency + 1));
    let release_barrier = Arc::new(Barrier::new(concurrency + 1));
    let next_row = Arc::new(AtomicUsize::new(0));
    let exhausted = Arc::new(AtomicBool::new(false));
    let statuses = Arc::new(Mutex::new(BTreeMap::<String, u64>::new()));
    let histogram = Arc::new(Mutex::new(Histogram::new()));
    let drained_after_deadline = Arc::new(Mutex::new(BTreeMap::<String, u64>::new()));
    let raw_rtt_us = Arc::new(Mutex::new(Vec::<u64>::new()));
    let mut tasks = Vec::with_capacity(concurrency);

    for worker in 0..concurrency {
        let rows = plateau_rows.clone();
        let ready_barrier = ready_barrier.clone();
        let release_barrier = release_barrier.clone();
        let next_row = next_row.clone();
        let exhausted = exhausted.clone();
        let statuses = statuses.clone();
        let histogram = histogram.clone();
        let drained = drained_after_deadline.clone();
        let raw_rtt = raw_rtt_us.clone();
        let run_id = run_id.clone();
        let mut client = generated::classify_client::ClassifyClient::new(
            channels[worker % channels.len()].clone(),
        );
        tasks.push(tokio::spawn(async move {
            let mut local_statuses = BTreeMap::<String, u64>::new();
            let mut local_histogram = Histogram::new();
            let mut local_drained = BTreeMap::<String, u64>::new();
            let mut local_raw_rtt = Vec::<u64>::new();
            ready_barrier.wait().await;
            release_barrier.wait().await;
            tokio::time::sleep_until(tokio::time::Instant::from_std(start_instant)).await;
            loop {
                if Instant::now() >= deadline {
                    break;
                }
                let index = next_row.fetch_add(1, Ordering::Relaxed);
                let Some(row) = rows.get(index) else {
                    exhausted.store(true, Ordering::Relaxed);
                    break;
                };
                let started = Instant::now();
                let result = client
                    .classify(request(
                        format!("{run_id}-{worker}-{index}"),
                        row.context.clone(),
                    ))
                    .await;
                let elapsed_us = started.elapsed().as_micros() as u64;
                let status = match result {
                    Ok(response) if valid(response.get_ref()) => "OK".to_string(),
                    Ok(_) => "INVALID_RESPONSE".to_string(),
                    Err(status) => format!("GRPC_{:?}", status.code()).to_uppercase(),
                };
                if Instant::now() <= deadline {
                    *local_statuses.entry(status.clone()).or_default() += 1;
                    if status == "OK" {
                        local_histogram.observe(elapsed_us);
                        if raw_latencies {
                            local_raw_rtt.push(elapsed_us);
                        }
                    }
                } else {
                    *local_drained.entry(status).or_default() += 1;
                }
            }
            let mut shared_statuses = statuses.lock().unwrap();
            for (status, count) in local_statuses {
                *shared_statuses.entry(status).or_default() += count;
            }
            drop(shared_statuses);
            histogram.lock().unwrap().merge(&local_histogram);
            let mut shared_drained = drained.lock().unwrap();
            for (status, count) in local_drained {
                *shared_drained.entry(status).or_default() += count;
            }
            drop(shared_drained);
            if raw_latencies {
                raw_rtt.lock().unwrap().extend(local_raw_rtt);
            }
        }));
    }

    ready_barrier.wait().await;
    let workers_ready_epoch_ms = now_epoch_ms();
    assert!(
        workers_ready_epoch_ms < start_epoch_ms,
        "workers did not reach the start barrier before shared start epoch"
    );
    release_barrier.wait().await;
    for task in tasks {
        task.await.expect("worker task panicked");
    }

    let statuses = statuses.lock().unwrap().clone();
    let successful = statuses.get("OK").copied().unwrap_or(0);
    let claimed = next_row.load(Ordering::Relaxed).min(plateau_rows.len());
    let mut successful_rtt_raw_us = raw_rtt_us.lock().unwrap().clone();
    successful_rtt_raw_us.sort_unstable();
    let report = serde_json::json!({
        "schema_version": 1,
        "probe": "sustained_exact_token_corpus",
        "target": target,
        "topology": topology,
        "target_image": target_image,
        "model_sha256": model_sha256,
        "tokenizer_sha256": tokenizer_sha256,
        "corpus_mode": corpus_mode,
        "corpus_blake3": corpus_blake3,
        "generator_scheme": generator_scheme,
        "selected_rows_blake3": selected_rows_blake3,
        "corpus_offset": corpus_offset,
        "first_sequence": first_sequence,
        "last_sequence": last_sequence,
        "warmup_requests": warmup_requests,
        "candidate_rows": candidate_rows,
        "claimed_plateau_rows": claimed,
        "token_count_including_specials": token_count,
        "connections": connections,
        "concurrency": concurrency,
        "ready_epoch_ms": ready_epoch_ms,
        "workers_ready_epoch_ms": workers_ready_epoch_ms,
        "start_epoch_ms": start_epoch_ms,
        "duration_seconds": duration_seconds,
        "corpus_exhausted": exhausted.load(Ordering::Relaxed),
        "statuses_completed_within_plateau": statuses,
        "drained_after_plateau": drained_after_deadline.lock().unwrap().clone(),
        "useful_requests_per_second": successful as f64 / duration_seconds as f64,
        "successful_rtt_histogram": histogram.lock().unwrap().json(),
        "successful_rtt_raw_us": if raw_latencies { Some(successful_rtt_raw_us) } else { None },
        "raw_latency_semantics": "successful RPC RTTs completed within plateau; microseconds; sorted"
    });
    println!("{}", serde_json::to_string_pretty(&report).unwrap());

    if report["corpus_exhausted"].as_bool() == Some(true) {
        std::process::exit(3);
    }
}

#[cfg(test)]
mod tests {
    use super::{
        armed_config_digest, armed_identity, armed_record, generated_rows, scheduled_rows_digest,
        validate_rows, write_armed_record, ArmedIdentity, ArmedRecordInput, CorpusRow, Histogram,
        OfferedRate, OpenLoopCompletion, OpenLoopConfig, OpenLoopResults,
        ARMED_CONFIG_CANONICALIZATION, ARMED_PROTOCOL_VERSION, ARMED_SCHEMA, HISTOGRAM_BOUNDS_US,
        VERIFIED_TOKENIZER_SHA256,
    };
    use std::time::Duration;

    fn row(sequence: u64, context: &str) -> CorpusRow {
        CorpusRow {
            sequence,
            context: context.to_string(),
            token_count_including_specials: 64,
            tokenizer_sha256: "tokenizer".to_string(),
        }
    }

    fn test_armed_record(
        identity: &ArmedIdentity,
        config: &OpenLoopConfig,
        expected_slots: usize,
        armed_epoch_ms: u64,
    ) -> serde_json::Value {
        let driver_image = Some("driver@sha256:1234".to_string());
        let corpus_blake3 = None;
        let generator_scheme = Some("test-generator-v1");
        armed_record(&ArmedRecordInput {
            identity,
            target: "10.0.0.7:50051",
            scheduled_start_epoch_ms: 1_900_000_000_000,
            expected_slots,
            duration_seconds: 180,
            armed_epoch_ms,
            scheduled_rows_blake3: "scheduled-rows",
            config,
            target_image: "sha256:target",
            driver_image: &driver_image,
            model_sha256: "model",
            tokenizer_sha256: "tokenizer",
            topology: "direct-pod-ip",
            corpus_mode: "generated",
            corpus_blake3: &corpus_blake3,
            generator_scheme: &generator_scheme,
            selected_rows_blake3: "selected-rows",
            corpus_offset: 0,
            first_sequence: 19_000_000_000,
            last_sequence: 19_000_009_999,
            warmup_requests: 0,
            candidate_rows: 10_000,
            token_count: 64,
            connections: 1,
            concurrency: 1,
            raw_latencies: true,
        })
    }

    #[test]
    fn histogram_buckets_are_fixed_and_mergeable() {
        let mut left = Histogram::new();
        left.observe(250);
        left.observe(251);
        let mut right = Histogram::new();
        right.observe(u64::MAX);
        left.merge(&right);
        assert_eq!(left.counts[0], 1);
        assert_eq!(left.counts[1], 1);
        assert_eq!(left.counts[HISTOGRAM_BOUNDS_US.len()], 1);
    }

    #[test]
    fn corpus_validation_accepts_unique_exact_token_rows() {
        assert_eq!(validate_rows(&[row(1, "alpha"), row(2, "bravo")]).0, 64);
    }

    #[test]
    #[should_panic(expected = "contexts must be globally unique")]
    fn corpus_validation_rejects_context_reuse() {
        validate_rows(&[row(1, "alpha"), row(2, "alpha")]);
    }

    #[test]
    fn generated_rows_are_unique_and_have_the_requested_word_budget() {
        let rows = generated_rows(64, 10_000, 100, VERIFIED_TOKENIZER_SHA256);
        assert_eq!(rows.len(), 100);
        assert_eq!(rows[0].sequence, 10_000);
        assert_eq!(rows[99].sequence, 10_099);
        assert!(rows
            .iter()
            .all(|row| row.context.split_whitespace().count() + 2 == 64));
        validate_rows(&rows);
    }

    #[test]
    fn decimal_offered_rate_is_exact_and_normalized() {
        let rate = OfferedRate::parse("12.500".to_string()).unwrap();
        assert_eq!(rate.numerator, 25);
        assert_eq!(rate.denominator, 2);
        assert_eq!(rate.source, "12.500");
    }

    #[test]
    fn offered_schedule_uses_a_deterministic_half_open_interval() {
        let rate = OfferedRate::parse("2.5".to_string()).unwrap();
        assert_eq!(rate.slots_for(2), 5);
        assert_eq!(rate.offset_for(0).as_nanos(), 0);
        assert_eq!(rate.offset_for(1).as_millis(), 400);
        assert_eq!(rate.offset_for(4).as_millis(), 1_600);
    }

    #[test]
    fn fractional_offered_rate_rounds_slot_count_up_without_drift() {
        let rate = OfferedRate::parse("0.333333333".to_string()).unwrap();
        assert_eq!(rate.slots_for(10), 4);
        assert_eq!(rate.offset_for(3).as_nanos(), 9_000_000_009);
    }

    #[test]
    fn invalid_offered_rates_are_rejected() {
        for value in ["0", "-1", "+1", "1e3", "1.2.3", "1.0000000001"] {
            assert!(OfferedRate::parse(value.to_string()).is_err(), "{value}");
        }
    }

    #[test]
    fn armed_identity_is_strictly_opt_in_and_non_partial() {
        assert_eq!(armed_identity(None, None, None).unwrap(), None);
        assert!(armed_identity(Some("run".to_string()), None, None).is_err());
        assert!(armed_identity(
            Some("run with space".to_string()),
            Some("job".to_string()),
            Some("nonce".to_string())
        )
        .is_err());
        assert_eq!(
            armed_identity(
                Some("run-1".to_string()),
                Some("job-01".to_string()),
                Some("sha256:0123".to_string())
            )
            .unwrap(),
            Some(ArmedIdentity {
                run_id: "run-1".to_string(),
                job_id: "job-01".to_string(),
                nonce: "sha256:0123".to_string()
            })
        );
    }

    #[test]
    fn armed_record_is_one_line_and_has_a_stable_config_digest() {
        let identity = ArmedIdentity {
            run_id: "recovery-a".to_string(),
            job_id: "scr-recovery-a-j00".to_string(),
            nonce: "sha256:aaaaaaaa".to_string(),
        };
        let config = OpenLoopConfig {
            offered_rate: OfferedRate::parse("35".to_string()).unwrap(),
            max_in_flight: 512,
            dispatch_late_after: Duration::from_millis(1),
            drop_late_after: Duration::from_millis(100),
            rpc_timeout: Duration::from_millis(30_000),
        };
        let first = test_armed_record(&identity, &config, 6_300, 1_899_999_900_000);
        let second = test_armed_record(&identity, &config, 6_300, 1_899_999_900_001);

        assert_eq!(first["schema"], ARMED_SCHEMA);
        assert_eq!(first["schema_version"], 1);
        assert_eq!(first["record_type"], "ARMED");
        assert_eq!(first["protocol_version"], ARMED_PROTOCOL_VERSION);
        assert_eq!(first["job_id"], "scr-recovery-a-j00");
        assert_eq!(first["endpoint"], "10.0.0.7:50051");
        assert_eq!(first["expected_slots"], 6_300);
        assert_eq!(first["config"]["offered_rps"], "35");
        assert_eq!(first["config"]["offered_rate_numerator"], 35);
        assert_eq!(first["config"]["offered_rate_denominator"], 1);
        assert_eq!(first["config"]["max_in_flight"], 512);
        assert_eq!(first["config"]["dispatch_late_after_ms"], 1);
        assert_eq!(first["config"]["drop_late_after_ms"], 100);
        assert_eq!(first["config"]["rpc_timeout_ms"], 30_000);
        assert_eq!(first["config"]["token_count_including_specials"], 64);
        assert_eq!(first["config"]["duration_seconds"], 180);
        assert_eq!(first["config"]["expected_slots"], 6_300);
        assert_eq!(
            first["config"]["scheduled_start_epoch_ms"],
            1_900_000_000_000_u64
        );
        assert_eq!(first["config"]["target_endpoint"], "10.0.0.7:50051");
        assert_eq!(first["config"]["first_sequence"], 19_000_000_000_u64);
        assert_eq!(first["config"]["scheduled_rows_blake3"], "scheduled-rows");
        assert_eq!(first["config"]["selected_rows_blake3"], "selected-rows");
        assert_eq!(first["config"]["target_image"], "sha256:target");
        assert_eq!(first["config"]["model_sha256"], "model");
        assert_eq!(first["config"]["tokenizer_sha256"], "tokenizer");
        assert_eq!(first["config"]["driver_image"], "driver@sha256:1234");
        assert_eq!(
            first["config_digest"]["canonicalization"],
            ARMED_CONFIG_CANONICALIZATION
        );
        assert_eq!(
            first["config_digest"]["hex"],
            second["config_digest"]["hex"]
        );
        assert_eq!(
            first["config_digest"]["hex"],
            armed_config_digest(&first["config"])
        );
        assert_eq!(
            first["config_digest"]["hex"].as_str().unwrap(),
            blake3::hash(&serde_json::to_vec(&first["config"]).unwrap())
                .to_hex()
                .as_str()
        );
        let line = serde_json::to_string(&first).unwrap();
        assert!(!line.contains('\n') && !line.contains('\r'));
        let mut stdout = Vec::new();
        write_armed_record(&mut stdout, &first).unwrap();
        assert_eq!(stdout, format!("{line}\n").as_bytes());
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&stdout).unwrap(),
            first
        );

        let different_identity = ArmedIdentity {
            nonce: "sha256:bbbbbbbb".to_string(),
            ..identity
        };
        let changed = test_armed_record(&different_identity, &config, 6_300, 1_899_999_900_000);
        assert_ne!(
            first["config_digest"]["hex"],
            changed["config_digest"]["hex"]
        );

        let mut mismatched_config = first["config"].clone();
        mismatched_config["max_in_flight"] = 256.into();
        assert_ne!(
            first["config_digest"]["hex"],
            armed_config_digest(&mismatched_config)
        );
    }

    #[test]
    fn scheduled_row_digest_covers_only_the_expected_slots() {
        let rows = [row(1, "alpha"), row(2, "bravo"), row(3, "charlie")];
        assert_eq!(
            scheduled_rows_digest(&rows, 2),
            scheduled_rows_digest(&rows[..2], 2)
        );
        assert_ne!(
            scheduled_rows_digest(&rows, 2),
            scheduled_rows_digest(&rows, 3)
        );
    }

    #[test]
    fn open_loop_accounting_keeps_drops_separate_from_status_rtt() {
        let mut results = OpenLoopResults::new(4);
        results.initiated = 3;
        results.dropped_in_flight_limit = 1;
        results.record_completion(OpenLoopCompletion {
            status: "GRPC_UNAVAILABLE".to_string(),
            rtt_us: 42,
            completed_within_plateau: true,
        });
        results.record_completion(OpenLoopCompletion {
            status: "OK".to_string(),
            rtt_us: 30,
            completed_within_plateau: false,
        });
        results.record_completion(OpenLoopCompletion {
            status: "OK".to_string(),
            rtt_us: 20,
            completed_within_plateau: true,
        });
        results.finalize();
        assert_eq!(results.completed, 3);
        assert_eq!(results.statuses["GRPC_UNAVAILABLE"], 1);
        assert_eq!(results.rtt_raw_us_by_status["GRPC_UNAVAILABLE"], [42]);
        assert_eq!(results.rtt_raw_us_by_status["OK"], [20, 30]);
        assert_eq!(results.successful_within_plateau_rtt_raw_us, [20]);
        assert!(!results.rtt_raw_us_by_status.contains_key("DROPPED"));
    }
}
