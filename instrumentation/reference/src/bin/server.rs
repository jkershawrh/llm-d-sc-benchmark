//! llm-d-sc service binary (Kubernetes container entrypoint).
//!
//! Binds the existing [`ClassifyServer`] on `LLM_D_SC_LISTEN` (default
//! `0.0.0.0:50051`) and reads the ModelCar mount directory from
//! `LLM_D_SC_MODEL_DIR` (default `/models`). The served pipeline is the RESIDENT
//! Candle classifier: the binary reads the ModelCar dir, validates its required
//! layout, loads tokenizer + config + safetensors, constructs the real
//! `CandleClassifier`, and runs a WARMUP FORWARD on a fixture input — only then
//! does it report READY. ANY failure leaves the service NOT ready with an
//! actionable typed error (a directory that merely exists never produces READY,
//! AC-002/AC-003). The deterministic synthetic pipeline is NOT used here; it is
//! reserved for weight-free tests.

use std::env;
use std::io;

use llm_d_sc::classify::load_and_warm_modelcar;
use llm_d_sc::grpc::classify::{parse_queue_bound, ClassifyServer, QUEUE_BOUND_ENV};
use llm_d_sc::metrics::LatencyStage;

/// Default TCP listen address.
const DEFAULT_LISTEN: &str = "0.0.0.0:50051";
/// Default ModelCar mount directory.
const DEFAULT_MODEL_DIR: &str = "/models";
const DEFAULT_METRICS_LISTEN: &str = "0.0.0.0:9464";

fn main() -> io::Result<()> {
    let otel_guard = llm_d_sc::otel::init_from_env()?;
    if let Some(guard) = &otel_guard {
        eprintln!(
            "llm-d-sc: OTLP tracing enabled with ratio {}",
            guard.sample_ratio
        );
    }
    let listen = env::var("LLM_D_SC_LISTEN").unwrap_or_else(|_| DEFAULT_LISTEN.to_string());
    let model_dir =
        env::var("LLM_D_SC_MODEL_DIR").unwrap_or_else(|_| DEFAULT_MODEL_DIR.to_string());
    if model_dir.trim().is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "LLM_D_SC_MODEL_DIR must not be empty",
        ));
    }

    // Real model lifecycle: validate the ModelCar required-files layout, load
    // tokenizer + config + safetensors, build the Candle classifier, and run a
    // WARMUP FORWARD on a fixture input. ANY failure leaves the service NOT
    // ready with an actionable typed error — a directory that merely exists
    // must NOT produce READY (AC-002/AC-003).
    let classifier = load_and_warm_modelcar(&model_dir).map_err(|e| {
        io::Error::new(
            io::ErrorKind::NotFound,
            format!("llm-d-sc NOT ready: {model_dir}: {e}"),
        )
    })?;

    // Only a loaded+warmed classifier reaches here, so the server reports READY.
    let queue_bound = parse_queue_bound(env::var(QUEUE_BOUND_ENV).ok().as_deref())?;
    let server = ClassifyServer::bind_with_classifier_and_bound(&listen, classifier, queue_bound)?;
    eprintln!(
        "llm-d-sc: bound {listen} -> {}; ModelCar dir {model_dir}; queue bound {queue_bound}; READY (resident Candle classifier loaded and warmed)",
        server.local_addr(),
    );

    // Periodically log the per-stage latency DECOMPOSITION.
    //
    // S-080 requires system evidence that distinguishes round-trip time from
    // queue and forward time. RTT is measurable from outside by any client; the
    // internal stages are not, and there is no metrics endpoint yet (tracked for
    // 0.3). Logging percentiles, not means, keeps this consistent with the rule
    // that a latency claim from an average is not evidence. Emitted only when
    // requests have actually been served, so an idle service stays quiet.
    let metrics = server.metrics();
    if let Ok(metrics_listen) = env::var("LLM_D_SC_METRICS_LISTEN") {
        if !metrics_listen.trim().is_empty() {
            let bound = llm_d_sc::metrics_exporter::start(&metrics_listen, metrics.clone())?;
            eprintln!("llm-d-sc: Prometheus/OTEL metrics bound on {bound}");
        }
    } else if env::var_os("LLM_D_SC_OTEL_ENABLED").is_some() {
        let bound = llm_d_sc::metrics_exporter::start(DEFAULT_METRICS_LISTEN, metrics.clone())?;
        eprintln!("llm-d-sc: Prometheus/OTEL metrics bound on {bound}");
    }
    let interval = env::var("LLM_D_SC_METRICS_LOG_SECS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(30);
    std::thread::Builder::new()
        .name("metrics-log".to_string())
        .spawn(move || {
            let mut last_total = 0u64;
            loop {
                std::thread::sleep(std::time::Duration::from_secs(interval));
                let snap = metrics.snapshot();
                let served = snap.cache_hits + snap.cache_misses;
                if served == last_total {
                    continue;
                }
                last_total = served;
                let stage = |s| metrics.stage_percentiles(s);
                let q = stage(LatencyStage::Queue);
                let t = stage(LatencyStage::Tokenize);
                let f = stage(LatencyStage::Forward);
                let tot = stage(LatencyStage::Total);
                eprintln!(
                    "llm-d-sc metrics: served={served} hits={} misses={} | \
                     queue p50={:?} p99={:?} | tokenize p50={:?} p99={:?} | \
                     forward p50={:?} p99={:?} | total p50={:?} p99={:?}",
                    snap.cache_hits,
                    snap.cache_misses,
                    q.p50,
                    q.p99,
                    t.p50,
                    t.p99,
                    f.p50,
                    f.p99,
                    tot.p50,
                    tot.p99
                );
            }
        })
        .expect("metrics log thread must spawn");

    // Keep serving until Kubernetes or an operator requests shutdown. Returning
    // from main drops the gRPC runtime and then the OTEL guard, which flushes the
    // provider instead of waiting for kubelet's SIGKILL at the grace deadline.
    let mut signals = signal_hook::iterator::Signals::new([
        signal_hook::consts::SIGTERM,
        signal_hook::consts::SIGINT,
    ])?;
    let _ = signals.forever().next();
    eprintln!("llm-d-sc: shutdown signal received; draining runtime and flushing telemetry");
    drop(server);
    drop(otel_guard);
    Ok(())
}
