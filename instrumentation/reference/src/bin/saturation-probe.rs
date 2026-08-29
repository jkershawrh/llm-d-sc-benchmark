//! Remote saturation probe for finding the concurrency knee and overload shape.
//!
//! Unlike `gateway-probe`, this client deliberately overlaps requests. It emits
//! machine-readable evidence: achieved throughput, RTT percentiles, gRPC status
//! counts, and a post-load recovery probe. It never treats overload as a client
//! transport failure: `RESOURCE_EXHAUSTED` is counted separately because it is
//! the service's required load-shedding contract.

use std::collections::BTreeMap;
use std::sync::{Arc, Barrier, Mutex};
use std::time::Instant;

use llm_d_sc::grpc::classify::{ClassifyClient, ClassifyRequest, ClassifyResponse};

fn arg(name: &str, default: &str) -> String {
    let args: Vec<String> = std::env::args().collect();
    args.iter()
        .position(|value| value == name)
        .and_then(|index| args.get(index + 1).cloned())
        .unwrap_or_else(|| default.to_string())
}

fn percentile(sorted: &[u128], quantile: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let rank = (quantile * sorted.len() as f64).ceil() as usize;
    let index = rank.saturating_sub(1).min(sorted.len() - 1);
    sorted[index] as f64 / 1000.0
}

fn request(id: String, context: String) -> ClassifyRequest {
    ClassifyRequest {
        request_id: id,
        session_id: "saturation-probe".to_string(),
        context,
        signals: Vec::new(),
    }
}

fn valid_response(response: &ClassifyResponse) -> bool {
    response.status == 1
        && !response.ranked.is_empty()
        && !response.classifier_id.is_empty()
        && !response.model_revision.is_empty()
        && !response.tokenizer_revision.is_empty()
        && !response.taxonomy_revision.is_empty()
}

fn main() {
    let target = arg("--target", "127.0.0.1:50051");
    let topology = arg("--topology", "oberon-unspecified");
    let cache_mode = arg("--cache-mode", "miss");
    let concurrency: usize = arg("--concurrency", "32")
        .parse()
        .expect("--concurrency must be a positive integer");
    let requests: usize = arg("--requests", "1000")
        .parse()
        .expect("--requests must be a positive integer");
    let context_bytes: usize = arg("--context-bytes", "256")
        .parse()
        .expect("--context-bytes must be a positive integer");

    if concurrency == 0 || requests == 0 || context_bytes == 0 {
        eprintln!("concurrency, requests, and context-bytes must all be greater than zero");
        std::process::exit(2);
    }
    if cache_mode != "hit" && cache_mode != "miss" {
        eprintln!("--cache-mode must be 'hit' or 'miss'");
        std::process::exit(2);
    }

    let run_id = format!("{}-{}", std::process::id(), concurrency);
    let stable_context = "h".repeat(context_bytes);
    if cache_mode == "hit" {
        let mut warm = ClassifyClient::connect(&target).unwrap_or_else(|error| {
            eprintln!("cannot reach llm-d-sc at {target}: {error}");
            std::process::exit(1);
        });
        warm.classify(request(format!("{run_id}-warm"), stable_context.clone()))
            .unwrap_or_else(|status| {
                eprintln!("cache warm failed: {status}");
                std::process::exit(1);
            });
    }

    let barrier = Arc::new(Barrier::new(concurrency + 1));
    let latencies = Arc::new(Mutex::new(Vec::<u128>::with_capacity(requests)));
    let statuses = Arc::new(Mutex::new(BTreeMap::<String, u64>::new()));
    let mut handles = Vec::with_capacity(concurrency);

    for worker in 0..concurrency {
        let worker_requests = requests / concurrency + usize::from(worker < requests % concurrency);
        let target = target.clone();
        let cache_mode = cache_mode.clone();
        let stable_context = stable_context.clone();
        let run_id = run_id.clone();
        let barrier = barrier.clone();
        let latencies = latencies.clone();
        let statuses = statuses.clone();
        handles.push(std::thread::spawn(move || {
            let mut client = match ClassifyClient::connect(&target) {
                Ok(client) => client,
                Err(_error) => {
                    *statuses
                        .lock()
                        .unwrap()
                        .entry("CONNECT_ERROR".to_string())
                        .or_default() += worker_requests as u64;
                    barrier.wait();
                    return;
                }
            };
            barrier.wait();
            for sequence in 0..worker_requests {
                let context = if cache_mode == "hit" {
                    stable_context.clone()
                } else {
                    let suffix = format!("-{run_id}-{worker}-{sequence}");
                    let mut value = "m".repeat(context_bytes.saturating_sub(suffix.len()));
                    value.push_str(&suffix);
                    value
                };
                let started = Instant::now();
                let result =
                    client.classify(request(format!("{run_id}-{worker}-{sequence}"), context));
                let elapsed = started.elapsed().as_micros();
                let status = match result {
                    Ok(response) if valid_response(&response) => {
                        latencies.lock().unwrap().push(elapsed);
                        "OK".to_string()
                    }
                    Ok(_) => "INVALID_RESPONSE".to_string(),
                    Err(status) => format!("GRPC_{:?}", status.code()).to_uppercase(),
                };
                *statuses.lock().unwrap().entry(status).or_default() += 1;
            }
        }));
    }

    barrier.wait();
    let started = Instant::now();
    for handle in handles {
        handle.join().expect("probe worker panicked");
    }
    let elapsed = started.elapsed();

    let mut observed = latencies.lock().unwrap().clone();
    observed.sort_unstable();
    let statuses = statuses.lock().unwrap().clone();
    let successful = statuses.get("OK").copied().unwrap_or(0);

    let recovery_started = Instant::now();
    let recovery = ClassifyClient::connect(&target).and_then(|mut client| {
        let response = client
            .classify(request(
                format!("{run_id}-recovery"),
                format!("recovery-{run_id}"),
            ))
            .map_err(std::io::Error::other)?;
        if valid_response(&response) {
            Ok(())
        } else {
            Err(std::io::Error::other("invalid recovery response"))
        }
    });
    let recovery_ms = recovery_started.elapsed().as_secs_f64() * 1000.0;

    let report = serde_json::json!({
        "schema_version": 1,
        "target": target,
        "topology": topology,
        "cache_mode": cache_mode,
        "context_bytes": context_bytes,
        "concurrency": concurrency,
        "requests": requests,
        "elapsed_seconds": elapsed.as_secs_f64(),
        "offered_requests_per_second": requests as f64 / elapsed.as_secs_f64(),
        "useful_requests_per_second": successful as f64 / elapsed.as_secs_f64(),
        "statuses": statuses,
        "successful_rtt_ms": {
            "samples": observed.len(),
            "p50": percentile(&observed, 0.50),
            "p95": percentile(&observed, 0.95),
            "p99": percentile(&observed, 0.99),
            "max": percentile(&observed, 1.0)
        },
        "post_load_recovery": {
            "ok": recovery.is_ok(),
            "latency_ms": recovery_ms,
            "error": recovery.err().map(|error| error.to_string())
        }
    });
    println!("{}", serde_json::to_string_pretty(&report).unwrap());

    if !report["post_load_recovery"]["ok"]
        .as_bool()
        .unwrap_or(false)
    {
        std::process::exit(1);
    }
}

#[cfg(test)]
mod tests {
    use super::percentile;

    #[test]
    fn u110_percentile_uses_nearest_rank_and_microseconds_to_milliseconds() {
        let samples = [1_000, 2_000, 3_000, 4_000];
        assert_eq!(percentile(&samples, 0.50), 2.0);
        assert_eq!(percentile(&samples, 0.99), 4.0);
    }
}
