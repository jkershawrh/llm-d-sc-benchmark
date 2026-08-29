//! External benchmark driver that separates HTTP/2 connection count from RPC concurrency.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use llm_d_sc::grpc::classify::{generated, ClassifyRequest, ClassifyResponse};
use tokio::sync::Barrier;
use tonic::transport::{Channel, Endpoint};

fn arg(name: &str, default: &str) -> String {
    let args: Vec<String> = std::env::args().collect();
    args.iter()
        .position(|v| v == name)
        .and_then(|i| args.get(i + 1).cloned())
        .unwrap_or_else(|| default.to_string())
}

fn percentile(sorted: &[u128], quantile: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let rank = (quantile * sorted.len() as f64).ceil() as usize;
    sorted[rank.saturating_sub(1).min(sorted.len() - 1)] as f64 / 1000.0
}

fn request(id: String, context: String) -> ClassifyRequest {
    ClassifyRequest {
        request_id: id,
        session_id: "connection-probe".into(),
        context,
        signals: vec![],
    }
}

fn valid(response: &ClassifyResponse) -> bool {
    response.status == 1 && !response.ranked.is_empty() && !response.classifier_id.is_empty()
}

async fn connect(target: &str) -> Result<Channel, tonic::transport::Error> {
    Endpoint::from_shared(format!("http://{target}"))
        .expect("valid target")
        .connect_timeout(std::time::Duration::from_secs(5))
        .tcp_nodelay(true)
        .connect()
        .await
}

fn main() {
    let target = arg("--target", "127.0.0.1:50051");
    let topology = arg("--topology", "unspecified");
    let concurrency: usize = arg("--concurrency", "64")
        .parse()
        .expect("positive concurrency");
    let connections: usize = arg("--connections", "1")
        .parse()
        .expect("positive connections");
    let requests: usize = arg("--requests", "1000")
        .parse()
        .expect("positive requests");
    let context_bytes: usize = arg("--context-bytes", "256")
        .parse()
        .expect("positive context bytes");
    if concurrency == 0 || connections == 0 || requests == 0 || context_bytes == 0 {
        eprintln!("concurrency, connections, requests, and context-bytes must be positive");
        std::process::exit(2);
    }

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    let report = runtime.block_on(async move {
        let run_id = format!("{}-{concurrency}-{connections}", std::process::id());
        let stable_context = "h".repeat(context_bytes);
        let mut channels = Vec::with_capacity(connections);
        for _ in 0..connections {
            channels.push(connect(&target).await.unwrap_or_else(|e| { eprintln!("connect failed: {e}"); std::process::exit(1) }));
        }
        let mut warm = generated::classify_client::ClassifyClient::new(channels[0].clone());
        warm.classify(request(format!("{run_id}-warm"), stable_context.clone())).await.expect("cache warm failed");

        let barrier = Arc::new(Barrier::new(concurrency + 1));
        let latencies = Arc::new(Mutex::new(Vec::<u128>::with_capacity(requests)));
        let statuses = Arc::new(Mutex::new(BTreeMap::<String, u64>::new()));
        let mut tasks = Vec::with_capacity(concurrency);
        for worker in 0..concurrency {
            let count = requests / concurrency + usize::from(worker < requests % concurrency);
            let mut client = generated::classify_client::ClassifyClient::new(channels[worker % connections].clone());
            let barrier = barrier.clone();
            let latencies = latencies.clone();
            let statuses = statuses.clone();
            let context = stable_context.clone();
            let run_id = run_id.clone();
            tasks.push(tokio::spawn(async move {
                barrier.wait().await;
                for sequence in 0..count {
                    let started = Instant::now();
                    let result = client.classify(request(format!("{run_id}-{worker}-{sequence}"), context.clone())).await;
                    let elapsed = started.elapsed().as_micros();
                    let status = match result {
                        Ok(response) if valid(response.get_ref()) => { latencies.lock().unwrap().push(elapsed); "OK".into() }
                        Ok(_) => "INVALID_RESPONSE".into(),
                        Err(status) => format!("GRPC_{:?}", status.code()).to_uppercase(),
                    };
                    *statuses.lock().unwrap().entry(status).or_default() += 1;
                }
            }));
        }
        barrier.wait().await;
        let started = Instant::now();
        for task in tasks { task.await.expect("worker panic"); }
        let elapsed = started.elapsed();
        let mut observed = latencies.lock().unwrap().clone(); observed.sort_unstable();
        let statuses = statuses.lock().unwrap().clone();
        let successful = statuses.get("OK").copied().unwrap_or(0);
        serde_json::json!({
            "schema_version": 1, "target": target, "topology": topology,
            "cache_mode": "hit", "context_bytes": context_bytes,
            "connections": connections, "concurrency": concurrency, "requests": requests,
            "elapsed_seconds": elapsed.as_secs_f64(),
            "offered_requests_per_second": requests as f64 / elapsed.as_secs_f64(),
            "useful_requests_per_second": successful as f64 / elapsed.as_secs_f64(),
            "statuses": statuses,
            "successful_rtt_ms": { "samples": observed.len(), "p50": percentile(&observed,0.5), "p95": percentile(&observed,0.95), "p99": percentile(&observed,0.99), "max": percentile(&observed,1.0) }
        })
    });
    println!("{}", serde_json::to_string_pretty(&report).unwrap());
}
