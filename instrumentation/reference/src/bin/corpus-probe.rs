//! External JSONL-corpus load probe. This binary is a benchmark client only.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::sync::{Arc, Barrier, Mutex};
use std::time::Instant;

use llm_d_sc::grpc::classify::{ClassifyClient, ClassifyRequest, ClassifyResponse};
use serde::Deserialize;

#[derive(Clone, Deserialize)]
struct CorpusRow {
    context: String,
    token_count_including_specials: usize,
    tokenizer_sha256: String,
}

fn arg(name: &str, default: Option<&str>) -> String {
    let args: Vec<String> = std::env::args().collect();
    args.iter()
        .position(|value| value == name)
        .and_then(|index| args.get(index + 1).cloned())
        .or_else(|| default.map(str::to_string))
        .unwrap_or_else(|| panic!("missing required argument {name}"))
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
        session_id: "corpus-probe".to_string(),
        context,
        signals: Vec::new(),
    }
}

fn valid(response: &ClassifyResponse) -> bool {
    response.status == 1
        && !response.ranked.is_empty()
        && !response.model_revision.is_empty()
        && !response.tokenizer_revision.is_empty()
}

fn main() {
    let target = arg("--target", Some("127.0.0.1:50051"));
    let corpus_path = arg("--corpus", None);
    let concurrency: usize = arg("--concurrency", Some("1")).parse().unwrap();
    let offset: usize = arg("--offset", Some("0")).parse().unwrap();
    let requests: usize = arg("--requests", Some("100")).parse().unwrap();
    let target_image = arg("--target-image", None);
    let model_sha256 = arg("--model-sha256", None);
    let node = arg("--node", None);
    assert!(concurrency > 0 && requests > 0);

    let corpus_bytes = fs::read(&corpus_path).expect("corpus must be readable");
    let all_rows: Vec<CorpusRow> = String::from_utf8(corpus_bytes.clone())
        .expect("corpus must be UTF-8")
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| serde_json::from_str(line).expect("corpus row must be valid JSON"))
        .collect();
    assert!(
        offset + requests <= all_rows.len(),
        "corpus does not contain requested slice"
    );
    let rows = all_rows[offset..offset + requests].to_vec();
    let token_counts: BTreeSet<_> = rows
        .iter()
        .map(|row| row.token_count_including_specials)
        .collect();
    let tokenizer_hashes: BTreeSet<_> = rows
        .iter()
        .map(|row| row.tokenizer_sha256.clone())
        .collect();
    assert_eq!(
        token_counts.len(),
        1,
        "corpus slice must have one token count"
    );
    assert_eq!(
        tokenizer_hashes.len(),
        1,
        "corpus slice must have one tokenizer digest"
    );
    assert_eq!(
        rows.iter()
            .map(|row| &row.context)
            .collect::<BTreeSet<_>>()
            .len(),
        rows.len(),
        "contexts must be unique misses"
    );

    let run_id = format!("corpus-{}-{concurrency}-{offset}", std::process::id());
    let rows = Arc::new(rows);
    let barrier = Arc::new(Barrier::new(concurrency + 1));
    let latencies = Arc::new(Mutex::new(Vec::<u128>::with_capacity(requests)));
    let statuses = Arc::new(Mutex::new(BTreeMap::<String, u64>::new()));
    let mut handles = Vec::new();

    for worker in 0..concurrency {
        let rows = rows.clone();
        let target = target.clone();
        let barrier = barrier.clone();
        let latencies = latencies.clone();
        let statuses = statuses.clone();
        let run_id = run_id.clone();
        handles.push(std::thread::spawn(move || {
            let mut client = ClassifyClient::connect(&target).expect("target connection failed");
            barrier.wait();
            for index in (worker..rows.len()).step_by(concurrency) {
                let started = Instant::now();
                let result = client.classify(request(
                    format!("{run_id}-{index}"),
                    rows[index].context.clone(),
                ));
                let elapsed = started.elapsed().as_micros();
                let status = match result {
                    Ok(response) if valid(&response) => {
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
        handle.join().unwrap();
    }
    let elapsed = started.elapsed().as_secs_f64();
    let mut observed = latencies.lock().unwrap().clone();
    observed.sort_unstable();
    let statuses = statuses.lock().unwrap().clone();
    let ok = statuses.get("OK").copied().unwrap_or(0);
    let tokenizer_sha256 = tokenizer_hashes.into_iter().next().unwrap();
    let token_count = token_counts.into_iter().next().unwrap();

    println!(
        "{}",
        serde_json::to_string_pretty(&serde_json::json!({
            "schema_version": 1,
            "target": target,
            "target_image": target_image,
            "model_sha256": model_sha256,
            "tokenizer_sha256": tokenizer_sha256,
            "node": node,
            "corpus_blake3": blake3::hash(&corpus_bytes).to_hex().to_string(),
            "corpus_offset": offset,
            "token_count_including_specials": token_count,
            "concurrency": concurrency,
            "requests": requests,
            "elapsed_seconds": elapsed,
            "useful_requests_per_second": ok as f64 / elapsed,
            "statuses": statuses,
            "successful_rtt_ms": {
                "samples": observed.len(),
                "p50": percentile(&observed, 0.50),
                "p95": percentile(&observed, 0.95),
                "p99": percentile(&observed, 0.99),
                "max": percentile(&observed, 1.0)
            }
        }))
        .unwrap()
    );
}

#[cfg(test)]
mod tests {
    use super::percentile;

    #[test]
    fn percentile_is_nearest_rank_milliseconds() {
        assert_eq!(percentile(&[1_000, 2_000, 3_000, 4_000], 0.50), 2.0);
    }
}
