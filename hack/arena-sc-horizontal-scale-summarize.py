#!/usr/bin/env python3
"""Validate and summarize one unchanged-SC horizontal scale campaign."""

from __future__ import annotations

import argparse
import json
import math
import random
import statistics
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any, Iterable


class ValidationError(RuntimeError):
    pass


def require(condition: bool, message: str) -> None:
    if not condition:
        raise ValidationError(message)


def load_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text())
    except (OSError, json.JSONDecodeError) as exc:
        raise ValidationError(f"cannot read JSON {path}: {exc}") from exc


def percentile(values: Iterable[float], fraction: float) -> float | None:
    samples = sorted(float(value) for value in values)
    if not samples:
        return None
    index = max(0, math.ceil(fraction * len(samples)) - 1)
    return samples[index]


def sample_cv(values: Iterable[float]) -> float | None:
    samples = [float(value) for value in values]
    if len(samples) < 2:
        return None
    mean = statistics.fmean(samples)
    return statistics.stdev(samples) / mean if mean else None


def close(left: float, right: float, tolerance: float = 1e-9) -> bool:
    return math.isclose(float(left), float(right), rel_tol=tolerance, abs_tol=tolerance)


def cpuset_count(text: str) -> int:
    total = 0
    for part in text.split(","):
        if "-" in part:
            first, last = (int(value) for value in part.split("-", 1))
            require(last >= first, f"invalid cpuset interval {part!r}")
            total += last - first + 1
        else:
            int(part)
            total += 1
    return total


def validate_plan(plan: dict[str, Any], ledger: dict[str, Any]) -> None:
    require(plan.get("schema_version") == 1, "unsupported campaign plan schema")
    require(plan.get("protocol") == "unchanged_sc_horizontal_scale_knee_v1", "wrong campaign protocol")
    replicas = plan.get("rung_replicas")
    require(replicas in (20, 30, 40, 50), "invalid scale rung")
    require(len(plan.get("cells", [])) == 16, "campaign must contain exactly 16 serial cells")
    require(plan.get("scale_protocol", {}).get("batch_increment") == 2, "scale increment is not +2")
    require(plan.get("scale_protocol", {}).get("stability_seconds_per_batch") == 120, "scale stability is not 120s")
    require(plan.get("scale_protocol", {}).get("cells_serial") is True, "cells are not serial")
    require(plan.get("scale_protocol", {}).get("no_oc_exec_during_plateau") is True, "plateau exec prohibition absent")
    identity = plan.get("identity", {})
    require(identity.get("token_count_including_specials") == 64, "wrong token count")
    require(identity.get("corpus_mode") == "generated", "wrong corpus mode")
    require(identity.get("generator_scheme") == "alpha_bravo_lsb_identity_service_fill_v1", "wrong generator")
    require(plan.get("driver_shape", {}).get("warmup_requests") == 0, "warm-up is not zero")
    require(plan.get("driver_shape", {}).get("connections_per_job") == 1, "connections are not one")
    require(plan.get("target_shape", {}).get("inference_workers") == "1", "target is not W1")
    require(plan.get("target_shape", {}).get("rayon_num_threads") == "1", "target is not RT1")
    require(plan.get("target_shape", {}).get("candle_num_threads") == "unset", "Candle override is present")
    require(plan.get("target_shape", {}).get("qos_class") == "Guaranteed", "target is not Guaranteed")
    require(
        plan.get("target_shape", {}).get("resources")
        == {"requests": {"cpu": "2", "memory": "4Gi"}, "limits": {"cpu": "2", "memory": "4Gi"}},
        "wrong target resources",
    )

    knee = [cell for cell in plan["cells"] if cell["phase"] == "knee"]
    require(len(knee) == 10, "expected ten knee cells")
    for block in range(1, 6):
        pair = [cell for cell in knee if cell["block"] == block]
        require(len(pair) == 2 and {cell["period"] for cell in pair} == {"A", "B"}, f"block {block}: invalid periods")
        rates = []
        for cell in pair:
            active_rates = [rate for rate in cell["endpoint_offered_rps"] if rate is not None]
            require(len(active_rates) == replicas, f"{cell['cell_id']}: endpoint coverage mismatch")
            require(len(set(active_rates)) == 1, f"{cell['cell_id']}: primary scale cell is not pure rate")
            rates.append(active_rates[0])
        require(set(rates) == {41, 42}, f"block {block}: not a 41/42 pair")

    reservation = plan.get("sequence_reservation", {})
    require(reservation.get("start_inclusive", 0) >= 22_000_000_000, "reservation is below audited namespace")
    require(reservation.get("end_exclusive", 0) <= 23_000_000_000, "reservation exceeds audited namespace")
    require(ledger.get("reservation_status") == "claimed_by_plan", "sequence reservation is not claimed")
    require(ledger.get("reservation") == reservation, "plan/ledger reservation mismatch")
    require(ledger.get("identity") == identity, "plan/ledger identity mismatch")
    jobs = plan.get("jobs", [])
    entries = ledger.get("entries", [])
    require(len(jobs) == len(entries), "plan/ledger job count mismatch")
    require(len({job["job_id"] for job in jobs}) == len(jobs), "duplicate job IDs")
    intervals: list[tuple[int, int, str]] = []
    entries_by_job = {entry["job_id"]: entry for entry in entries}
    for job in jobs:
        entry = entries_by_job.get(job["job_id"])
        require(entry is not None, f"{job['job_id']}: ledger entry missing")
        interval = entry.get("planned", {}).get("reserved_interval", {})
        start = interval.get("start_inclusive")
        end = interval.get("end_exclusive")
        require(start == job["sequence_base"] and end == job["reserved_end_exclusive"], f"{job['job_id']}: interval mismatch")
        require(end - start == 10_001, f"{job['job_id']}: wrong sequence span")
        require(reservation["start_inclusive"] <= start < end <= reservation["end_exclusive"], f"{job['job_id']}: out of reservation")
        intervals.append((start, end, job["job_id"]))
    intervals.sort()
    for left, right in zip(intervals, intervals[1:]):
        require(left[1] <= right[0], f"overlapping sequence allocations {left[2]} and {right[2]}")


def update_ledger(run_dir: Path, output: Path | None = None) -> dict[str, Any]:
    plan = load_json(run_dir / "campaign-plan.json")
    ledger = load_json(run_dir / "sequence-ledger.json")
    entries = {entry["job_id"]: entry for entry in ledger.get("entries", [])}
    inventory_path = run_dir / "target-inventory.json"
    inventory = load_json(inventory_path) if inventory_path.exists() else {"pods": []}
    endpoints = {int(pod["endpoint_ordinal"]): pod for pod in inventory.get("pods", [])}
    baseline_path = run_dir / "target-counters-baseline.json"
    final_path = run_dir / "target-counters-final.json"
    baselines = {}
    finals = {}
    if baseline_path.exists():
        baselines = {int(row["endpoint_ordinal"]): row["counters"] for row in load_json(baseline_path).get("pods", [])}
    if final_path.exists():
        finals = {int(row["endpoint_ordinal"]): row["counters"] for row in load_json(final_path).get("pods", [])}

    for cell in plan.get("cells", []):
        cell_dir = run_dir / "cells" / f"c{cell['ordinal']:02d}-{cell['cell_id']}"
        armed_path = cell_dir / "driver-armed.json"
        armed_records: dict[str, dict[str, Any]] = {}
        if armed_path.exists():
            document = load_json(armed_path)
            armed_records = {record.get("job_id"): record for record in document.get("records", [])}
        for job_id in cell.get("job_ids", []):
            entry = entries[job_id]
            endpoint = endpoints.get(int(entry["endpoint_ordinal"]))
            if endpoint:
                entry["target_binding"] = {
                    "endpoint_ordinal": entry["endpoint_ordinal"],
                    "pod_name": endpoint.get("name"),
                    "pod_uid": endpoint.get("uid"),
                    "pod_ip": endpoint.get("ip"),
                }
                entry["fresh_cache_counters"]["baseline_observed"] = baselines.get(entry["endpoint_ordinal"])
                entry["fresh_cache_counters"]["final_observed"] = finals.get(entry["endpoint_ordinal"])
            record = armed_records.get(job_id)
            if record:
                entry["armed"] = {
                    "observed": True,
                    "scheduled_rows_blake3": record.get("scheduled_rows_blake3"),
                    "selected_rows_blake3": record.get("config", {}).get("selected_rows_blake3"),
                    "config_digest": record.get("config_digest"),
                    "armed_epoch_ms": record.get("armed_epoch_ms"),
                }
                entry["lifecycle_status"] = "armed"
            driver_path = cell_dir / "drivers" / f"e{entry['endpoint_ordinal']:02d}.json"
            if driver_path.exists():
                report = load_json(driver_path)
                entry["emitted"] = {
                    "observed": True,
                    "actual_first_sequence": report.get("first_sequence"),
                    "actual_last_sequence": report.get("last_sequence"),
                    "scheduled_rows_blake3": report.get("scheduled_rows_blake3"),
                    "completed_requests": report.get("accounting", {}).get("completed_requests"),
                }
                entry["lifecycle_status"] = "emitted"
    ledger["entries"] = [entries[entry["job_id"]] for entry in ledger["entries"]]
    statuses: dict[str, int] = defaultdict(int)
    for entry in ledger["entries"]:
        statuses[entry["lifecycle_status"]] += 1
    ledger["lifecycle_counts"] = dict(sorted(statuses.items()))
    ledger["reservation_status"] = "partially_or_fully_emitted" if statuses.get("emitted") else "claimed_by_plan"
    destination = output or run_dir / "sequence-ledger-final.json"
    destination.write_text(json.dumps(ledger, indent=2, sort_keys=True) + "\n")
    return ledger


def validate_inventory(plan: dict[str, Any], run_dir: Path) -> list[dict[str, Any]]:
    inventory = load_json(run_dir / "target-inventory.json")
    pods = sorted(inventory.get("pods", []), key=lambda row: row.get("endpoint_ordinal", -1))
    replicas = plan["rung_replicas"]
    require(len(pods) == replicas, "target inventory does not cover the full rung")
    require([pod.get("endpoint_ordinal") for pod in pods] == list(range(replicas)), "endpoint ordinals are not contiguous")
    names: set[str] = set()
    uids: set[str] = set()
    ips: set[str] = set()
    for pod in pods:
        prefix = f"endpoint {pod.get('endpoint_ordinal')}"
        require(pod.get("name") and pod["name"] not in names, f"{prefix}: duplicate/missing name")
        require(pod.get("uid") and pod["uid"] not in uids, f"{prefix}: duplicate/missing UID")
        require(pod.get("ip") and pod["ip"] not in ips, f"{prefix}: duplicate/missing IP")
        names.add(pod["name"]); uids.add(pod["uid"]); ips.add(pod["ip"])
        require(pod.get("node") == plan["target_shape"]["node"], f"{prefix}: wrong node")
        require(pod.get("ready") is True and pod.get("restart_count") == 0, f"{prefix}: unhealthy inventory")
        require(str(pod.get("image_id", "")).endswith(plan["identity"]["target_image"]), f"{prefix}: wrong image")
        require(pod.get("qos_class") == "Guaranteed", f"{prefix}: not Guaranteed")
        require(cpuset_count(pod.get("cpuset_cpus_effective", "")) == 2, f"{prefix}: cpuset is not two logical CPUs")
        require(pod.get("complete_smt_sibling_sets") is True, f"{prefix}: incomplete SMT sibling allocation")
        require(str(pod.get("cpu_max", "")).split()[0] == "max", f"{prefix}: CPU quota present")
        require(pod.get("pid1_executable") == "/usr/local/bin/llm-d-sc", f"{prefix}: wrong PID1")
        env = pod.get("environment", {})
        require(env.get("LLM_D_SC_INFERENCE_WORKERS") == "1", f"{prefix}: wrong worker count")
        require(env.get("RAYON_NUM_THREADS") == "1", f"{prefix}: wrong Rayon count")
        require(env.get("LLM_D_SC_METRICS_LOG_SECS") == "10", f"{prefix}: wrong metric interval")
        require("CANDLE_NUM_THREADS" not in env, f"{prefix}: Candle override present")
    return pods


def validate_scale_batches(plan: dict[str, Any], run_dir: Path) -> None:
    batches = load_json(run_dir / "scale-batches.json").get("batches", [])
    expected = list(range(2, plan["rung_replicas"] + 1, 2))
    require([row.get("replicas") for row in batches] == expected, "scale batches are not exact +2 increments")
    for row in batches:
        require(row.get("stable_seconds_observed", 0) >= 120, f"r{row.get('replicas')}: stability shorter than 120s")
        require(row.get("ready_replicas") == row.get("replicas"), f"r{row.get('replicas')}: not fully Ready")
        require(row.get("uid_set_stable") is True, f"r{row.get('replicas')}: UID set changed during stability")
        require(row.get("restart_free") is True and row.get("nodes_ready") is True, f"r{row.get('replicas')}: health failure")


def _series_for(document: dict[str, Any], label: str, value: str) -> list[tuple[float, float]]:
    matches = [
        series for series in document.get("data", {}).get("result", [])
        if series.get("metric", {}).get(label) == value
    ]
    require(len(matches) == 1, f"telemetry {label}={value}: expected one series, found {len(matches)}")
    return [(float(timestamp), float(sample)) for timestamp, sample in matches[0].get("values", [])]


def validate_telemetry(plan: dict[str, Any], run_dir: Path, pods: list[dict[str, Any]], cells: list[dict[str, Any]]) -> None:
    metric_dir = run_dir / "metrics"
    required = {
        "pod_cpu_otel": ("k8s_pod_name", None),
        "container_cpu_otel": ("k8s_pod_name", None),
        "container_cpu_cadvisor": ("pod", None),
        "memory_working_set": ("pod", None),
        "restarts": ("pod", 0.0),
        "pod_ready": ("pod", 1.0),
    }
    for metric, (label, expected) in required.items():
        document = load_json(metric_dir / f"{metric}.json")
        require(document.get("status") == "success", f"{metric}: query failed")
        for pod in pods:
            series = _series_for(document, label, pod["name"])
            for cell in cells:
                start = cell["start_epoch_ms"] / 1000
                end = start + cell["duration_seconds"]
                window = [(ts, value) for ts, value in series if start <= ts <= end]
                require(window, f"{metric}/{pod['name']}/{cell['cell_id']}: no samples")
                require(window[0][0] - start <= 10 and end - window[-1][0] <= 10, f"{metric}/{pod['name']}/{cell['cell_id']}: edge gap")
                require(max((right[0] - left[0] for left, right in zip(window, window[1:])), default=0) <= 10, f"{metric}/{pod['name']}/{cell['cell_id']}: sample gap")
                if expected is not None:
                    require(all(close(value, expected) for _, value in window), f"{metric}/{pod['name']}: unexpected value")
    node_document = load_json(metric_dir / "node_ready.json")
    require(node_document.get("status") == "success", "node_ready query failed")
    for node in {plan["target_shape"]["node"], plan["driver_shape"]["node"]}:
        series = _series_for(node_document, "node", node)
        require(series and all(close(value, 1) for _, value in series), f"node_ready/{node}: incomplete or non-Ready")
    for metric in ("throttle_ratio", "cpu_pressure_waiting"):
        require(load_json(metric_dir / f"{metric}.json").get("status") == "success", f"{metric}: supporting query failed")


def validate_health(plan: dict[str, Any], run_dir: Path, pods: list[dict[str, Any]]) -> None:
    expected = {(pod["uid"], pod["ip"], pod["image_id"]) for pod in pods}
    health_path = run_dir / "health-monitor.ndjson"
    try:
        lines = [json.loads(line) for line in health_path.read_text().splitlines() if line.strip()]
    except (OSError, json.JSONDecodeError) as exc:
        raise ValidationError(f"cannot read health monitor: {exc}") from exc
    require(lines, "health monitor is empty")
    for index, sample in enumerate(lines):
        observed = {
            (pod.get("uid"), pod.get("ip"), pod.get("image_id"))
            for pod in sample.get("targets", [])
        }
        require(observed == expected, f"health sample {index}: target identity set changed")
        require(all(pod.get("ready") is True and pod.get("restart_count") == 0 for pod in sample.get("targets", [])), f"health sample {index}: readiness/restart failure")
        require(sample.get("nodes_ready") is True, f"health sample {index}: node readiness failure")
    violations = load_json(run_dir / "health-event-violations.json")
    require(violations.get("violations") == [], "health/event violations were observed")


def validate_armed(
    plan: dict[str, Any],
    cell: dict[str, Any],
    runtime_cell: dict[str, Any],
    armed: dict[str, Any],
    jobs: list[dict[str, Any]],
    pods_by_endpoint: dict[int, dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    records = armed.get("records", [])
    require(armed.get("all_armed") is True and len(records) == len(jobs), f"{cell['cell_id']}: incomplete ARMED barrier")
    by_job = {record.get("job_id"): record for record in records}
    require(len(by_job) == len(records), f"{cell['cell_id']}: duplicate ARMED job IDs")
    t0 = runtime_cell["start_epoch_ms"]
    require(runtime_cell.get("armed_verified_epoch_ms", t0) <= t0 - 90_000, f"{cell['cell_id']}: ARMED lead below 90s")
    identity = plan["identity"]
    for job in jobs:
        record = by_job.get(job["job_id"])
        require(record is not None, f"{job['job_id']}: ARMED record missing")
        endpoint = pods_by_endpoint[job["endpoint_ordinal"]]
        require(record.get("schema") == "llm-d-sc.benchmark-driver.armed", f"{job['job_id']}: wrong ARMED schema")
        require(record.get("schema_version") == 1 and record.get("record_type") == "ARMED", f"{job['job_id']}: malformed ARMED record")
        require(record.get("protocol_version") == "sustained-corpus-probe-armed-v1", f"{job['job_id']}: wrong ARMED protocol")
        require(record.get("run_id") == plan["run_id"] and record.get("nonce") == job["arming_nonce"], f"{job['job_id']}: run/nonce mismatch")
        require(record.get("endpoint") == f"{endpoint['ip']}:50051", f"{job['job_id']}: endpoint mismatch")
        require(record.get("scheduled_start_epoch_ms") == t0, f"{job['job_id']}: T0 mismatch")
        require(record.get("expected_slots") == job["expected_slots"], f"{job['job_id']}: slot mismatch")
        config = record.get("config", {})
        expected = {
            "candidate_rows": 10000,
            "closed_loop_concurrency_argument": 1,
            "connections": 1,
            "corpus_blake3": None,
            "corpus_mode": "generated",
            "corpus_offset": 0,
            "dispatch_late_after_ms": 1,
            "driver_image": identity["driver_image"],
            "driver_package_version": "0.1.0",
            "drop_late_after_ms": 100,
            "duration_seconds": 180,
            "expected_slots": job["expected_slots"],
            "first_sequence": job["sequence_base"],
            "generator_scheme": identity["generator_scheme"],
            "job_id": job["job_id"],
            "last_sequence": job["sequence_base"] + 9999,
            "max_in_flight": 512,
            "model_sha256": identity["model_sha256"],
            "nonce": job["arming_nonce"],
            "offered_rate_denominator": 1,
            "offered_rate_numerator": job["offered_rps"],
            "offered_rate_requested_decimal": str(job["offered_rps"]),
            "offered_rps": str(job["offered_rps"]),
            "protocol_version": "sustained-corpus-probe-armed-v1",
            "raw_latencies": True,
            "rpc_timeout_ms": 30000,
            "run_id": plan["run_id"],
            "scheduled_start_epoch_ms": t0,
            "target_endpoint": f"{endpoint['ip']}:50051",
            "target_image": identity["target_image"],
            "token_count_including_specials": 64,
            "tokenizer_sha256": identity["tokenizer_sha256"],
            "topology": f"cross-node-direct-{plan['target_shape']['node']}-from-{plan['driver_shape']['node']}",
            "warmup_requests": 0,
        }
        observed = {key: value for key, value in config.items() if key not in {"selected_rows_blake3", "scheduled_rows_blake3"}}
        require(observed == expected, f"{job['job_id']}: explicit ARMED config mismatch")
        require(config.get("scheduled_rows_blake3") == record.get("scheduled_rows_blake3"), f"{job['job_id']}: ARMED row digest mismatch")
        require(len(str(record.get("scheduled_rows_blake3", ""))) == 64, f"{job['job_id']}: missing scheduled digest")
        digest = record.get("config_digest", {})
        require(digest.get("algorithm") == "blake3" and len(str(digest.get("hex", ""))) == 64, f"{job['job_id']}: invalid config digest")
    return by_job


def materialize_armed_cell(run_dir: Path, ordinal: int, output: Path) -> dict[str, Any]:
    plan = load_json(run_dir / "campaign-plan.json")
    ledger = load_json(run_dir / "sequence-ledger.json")
    validate_plan(plan, ledger)
    pods = validate_inventory(plan, run_dir)
    pods_by_endpoint = {int(pod["endpoint_ordinal"]): pod for pod in pods}
    try:
        cell = next(row for row in plan["cells"] if int(row["ordinal"]) == ordinal)
    except StopIteration as exc:
        raise ValidationError(f"cell ordinal {ordinal} is not in the plan") from exc
    cell_dir = run_dir / "cells" / f"c{cell['ordinal']:02d}-{cell['cell_id']}"
    runtime_cell = load_json(cell_dir / "cell-runtime.json")
    records = []
    for endpoint in cell["active_endpoints"]:
        raw_path = cell_dir / "arming" / f"e{endpoint:02d}.stdout"
        try:
            documents = [json.loads(line) for line in raw_path.read_text().splitlines() if line.strip()]
        except (OSError, json.JSONDecodeError) as exc:
            raise ValidationError(f"cannot parse ARMED stream {raw_path}: {exc}") from exc
        matches = [
            document for document in documents
            if document.get("schema") == "llm-d-sc.benchmark-driver.armed"
            and document.get("schema_version") == 1
            and document.get("record_type") == "ARMED"
        ]
        require(len(matches) == 1, f"{cell['cell_id']}/endpoint {endpoint}: expected exactly one ARMED record")
        records.append(matches[0])
    verified_epoch_ms = int(__import__("time").time() * 1000)
    candidate = {"schema_version": 1, "all_armed": True, "verified_epoch_ms": verified_epoch_ms, "records": records}
    runtime_for_validation = dict(runtime_cell)
    runtime_for_validation["armed_verified_epoch_ms"] = verified_epoch_ms
    jobs = sorted(
        (job for job in plan["jobs"] if int(job["cell_ordinal"]) == ordinal),
        key=lambda row: row["endpoint_ordinal"],
    )
    validate_armed(plan, cell, runtime_for_validation, candidate, jobs, pods_by_endpoint)
    output.write_text(json.dumps(candidate, indent=2, sort_keys=True) + "\n")
    return candidate


def validate_report(
    plan: dict[str, Any],
    cell: dict[str, Any],
    runtime_cell: dict[str, Any],
    job: dict[str, Any],
    endpoint: dict[str, Any],
    armed: dict[str, Any],
    report: dict[str, Any],
) -> dict[str, Any]:
    prefix = job["job_id"]
    require(report.get("schema_version") == 2 and report.get("probe") == "sustained_exact_token_corpus", f"{prefix}: wrong report schema")
    require(report.get("load_model") == "open_loop_deterministic_offered_rate", f"{prefix}: wrong load model")
    require(report.get("target") == f"{endpoint['ip']}:50051", f"{prefix}: target mismatch")
    require(report.get("target_image") == plan["identity"]["target_image"], f"{prefix}: image mismatch")
    require(report.get("model_sha256") == plan["identity"]["model_sha256"], f"{prefix}: model mismatch")
    require(report.get("tokenizer_sha256") == plan["identity"]["tokenizer_sha256"], f"{prefix}: tokenizer mismatch")
    require(report.get("token_count_including_specials") == 64, f"{prefix}: token mismatch")
    require(report.get("warmup_requests") == 0, f"{prefix}: warm-up was nonzero")
    require(report.get("connections") == 1 and report.get("closed_loop_concurrency_argument") == 1, f"{prefix}: connection/concurrency mismatch")
    require(report.get("first_sequence") == job["sequence_base"], f"{prefix}: first sequence mismatch")
    require(report.get("last_sequence") == job["sequence_base"] + 10_000, f"{prefix}: last sequence mismatch")
    require(report.get("scheduled_rows_blake3") == armed.get("scheduled_rows_blake3"), f"{prefix}: ARMED/emitted digest mismatch")
    require(report.get("start_epoch_ms") == runtime_cell["start_epoch_ms"], f"{prefix}: common T0 mismatch")
    require(report.get("duration_seconds") == 180, f"{prefix}: duration mismatch")
    require(close(report.get("offered_requests_per_second", -1), job["offered_rps"]), f"{prefix}: rate mismatch")
    accounting = report.get("accounting", {})
    offered = job["expected_slots"]
    initiated = accounting.get("initiated_requests")
    completed = accounting.get("completed_requests")
    schedule_drops = accounting.get("dropped_schedule_late")
    in_flight_drops = accounting.get("dropped_in_flight_limit")
    within = accounting.get("completed_within_plateau")
    after = accounting.get("completed_after_plateau")
    require(offered == accounting.get("offered_slots") == initiated + schedule_drops + in_flight_drops, f"{prefix}: offered accounting mismatch")
    require(initiated == completed == within + after, f"{prefix}: completion accounting mismatch")
    require(schedule_drops == 0 and in_flight_drops == 0, f"{prefix}: driver dropped offered work")
    dispatch = report.get("dispatch_lag_raw_us", [])
    require(dispatch == sorted(dispatch) and len(dispatch) == initiated, f"{prefix}: invalid dispatch population")
    dispatch_p99 = percentile(dispatch, 0.99)
    require(dispatch_p99 is not None and dispatch_p99 <= 5000, f"{prefix}: dispatch p99 exceeds 5ms")
    drain_seconds = max(0.0, (report.get("drain_completed_epoch_ms", 0) - (runtime_cell["start_epoch_ms"] + 180_000)) / 1000)
    require(drain_seconds <= 90, f"{prefix}: drain exceeds 90s")
    statuses = report.get("statuses_completed_total", {})
    forbidden = set(plan["validity_gates"]["transport"]["forbidden_statuses"])
    observed_forbidden = {name: count for name, count in statuses.items() if count and name in forbidden}
    require(not observed_forbidden, f"{prefix}: forbidden transport status {observed_forbidden}")
    success_raw = report.get("successful_rtt_raw_us", [])
    require(success_raw == sorted(success_raw), f"{prefix}: success RTTs are not sorted")
    ok_within = report.get("statuses_completed_within_plateau", {}).get("OK", 0)
    require(len(success_raw) == ok_within, f"{prefix}: successful RTT population mismatch")
    errors = sum(count for name, count in statuses.items() if name != "OK")
    return {
        "job_id": prefix,
        "endpoint_ordinal": job["endpoint_ordinal"],
        "offered_rps": job["offered_rps"],
        "offered_slots": offered,
        "completed_within_plateau": within,
        "ok_within_plateau": ok_within,
        "ok_total": statuses.get("OK", 0),
        "errors_total": errors,
        "drained_after_plateau": after,
        "success_ratio": ok_within / offered,
        "drain_ratio": after / offered,
        "useful_rps": ok_within / 180,
        "p50_us": percentile(success_raw, 0.50),
        "p99_us": percentile(success_raw, 0.99),
        "dispatch_p99_us": dispatch_p99,
        "drain_seconds": drain_seconds,
        "statuses": statuses,
    }


def aggregate_cell(cell: dict[str, Any], runtime_cell: dict[str, Any], endpoints: list[dict[str, Any]]) -> dict[str, Any]:
    offered = sum(row["offered_slots"] for row in endpoints)
    ok = sum(row["ok_within_plateau"] for row in endpoints)
    drains = sum(row["drained_after_plateau"] for row in endpoints)
    errors = sum(row["errors_total"] for row in endpoints)
    # Median endpoint percentiles are retained to avoid presenting a pooled
    # request population as N independent replicas of the cluster experiment.
    p50s = [row["p50_us"] for row in endpoints if row["p50_us"] is not None]
    p99s = [row["p99_us"] for row in endpoints if row["p99_us"] is not None]
    rates = sorted({row["offered_rps"] for row in endpoints})
    return {
        "cell_id": cell["cell_id"],
        "ordinal": cell["ordinal"],
        "phase": cell["phase"],
        "scope": cell["scope"],
        "block": cell.get("block"),
        "period": cell.get("period"),
        "start_epoch_ms": runtime_cell["start_epoch_ms"],
        "duration_seconds": 180,
        "endpoints": len(endpoints),
        "offered_rps_per_endpoint": rates[0] if len(rates) == 1 else rates,
        "aggregate_offered_rps": sum(row["offered_rps"] for row in endpoints),
        "aggregate_useful_rps": ok / 180,
        "per_pod_useful_rps": ok / 180 / len(endpoints),
        "offered_slots": offered,
        "success_ratio": ok / offered,
        "drain_ratio": drains / offered,
        "errors_total": errors,
        "median_endpoint_p50_us": statistics.median(p50s) if p50s else None,
        "median_endpoint_p99_us": statistics.median(p99s) if p99s else None,
        "endpoint_useful_rps_cv": sample_cv(row["useful_rps"] for row in endpoints),
        "endpoint_results": endpoints,
    }


def materialize_completed_cell(run_dir: Path, ordinal: int, output: Path) -> dict[str, Any]:
    plan = load_json(run_dir / "campaign-plan.json")
    ledger = load_json(run_dir / "sequence-ledger.json")
    validate_plan(plan, ledger)
    pods = validate_inventory(plan, run_dir)
    pods_by_endpoint = {int(pod["endpoint_ordinal"]): pod for pod in pods}
    try:
        cell = next(row for row in plan["cells"] if int(row["ordinal"]) == ordinal)
    except StopIteration as exc:
        raise ValidationError(f"cell ordinal {ordinal} is not in the plan") from exc
    cell_dir = run_dir / "cells" / f"c{cell['ordinal']:02d}-{cell['cell_id']}"
    runtime_cell = load_json(cell_dir / "cell-runtime.json")
    armed = load_json(cell_dir / "driver-armed.json")
    jobs = sorted(
        (job for job in plan["jobs"] if int(job["cell_ordinal"]) == ordinal),
        key=lambda row: row["endpoint_ordinal"],
    )
    armed_by_job = validate_armed(plan, cell, runtime_cell, armed, jobs, pods_by_endpoint)
    endpoints = []
    for job in jobs:
        report = load_json(cell_dir / "drivers" / f"e{job['endpoint_ordinal']:02d}.json")
        endpoints.append(
            validate_report(
                plan,
                cell,
                runtime_cell,
                job,
                pods_by_endpoint[job["endpoint_ordinal"]],
                armed_by_job[job["job_id"]],
                report,
            )
        )
    summary = aggregate_cell(cell, runtime_cell, endpoints)
    output.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")
    return summary


def bootstrap_ci(values: list[float], *, samples: int = 100_000, seed: int = 20260829) -> list[float]:
    require(len(values) == 5, "bootstrap requires five block values")
    rng = random.Random(seed)
    medians = []
    for _ in range(samples):
        draw = [values[rng.randrange(len(values))] for _ in values]
        medians.append(statistics.median(draw))
    medians.sort()
    return [float(percentile(medians, 0.025)), float(percentile(medians, 0.975))]


def decision(plan: dict[str, Any], cells: list[dict[str, Any]]) -> dict[str, Any]:
    by_id = {cell["cell_id"]: cell for cell in cells}
    thresholds = plan["analysis"]

    def clean41(cell: dict[str, Any]) -> bool:
        return (
            cell["success_ratio"] >= thresholds["rate_41_success_min"]
            and cell["drain_ratio"] <= thresholds["rate_41_drain_max"]
            and cell["errors_total"] == 0
            and cell["median_endpoint_p99_us"] is not None
            and cell["median_endpoint_p99_us"] <= thresholds["clean_p99_ms_max"] * 1000
        )

    def stressed42(cell: dict[str, Any]) -> bool:
        return (
            cell["success_ratio"] < thresholds["rate_41_success_min"]
            or cell["drain_ratio"] > thresholds["rate_41_drain_max"]
            or cell["errors_total"] > 0
        )

    block_rows = []
    for block in range(1, 6):
        pair = [cell for cell in cells if cell["phase"] == "knee" and cell["block"] == block]
        rate41 = next(cell for cell in pair if cell["offered_rps_per_endpoint"] == 41)
        rate42 = next(cell for cell in pair if cell["offered_rps_per_endpoint"] == 42)
        p99_ratio = rate42["median_endpoint_p99_us"] / rate41["median_endpoint_p99_us"]
        block_rows.append(
            {
                "block": block,
                "rate_41_cell": rate41["cell_id"],
                "rate_42_cell": rate42["cell_id"],
                "delta_success": rate42["success_ratio"] - rate41["success_ratio"],
                "delta_drain": rate42["drain_ratio"] - rate41["drain_ratio"],
                "p99_ratio": p99_ratio,
                "marginal_useful_rps_per_pod": rate42["per_pod_useful_rps"] - rate41["per_pod_useful_rps"],
                "rate_41_clean": clean41(rate41),
                "rate_42_stressed": stressed42(rate42) and p99_ratio > thresholds["paired_p99_ratio_min_exclusive"],
            }
        )

    effects = {
        key: [row[key] for row in block_rows]
        for key in ("delta_success", "delta_drain", "p99_ratio", "marginal_useful_rps_per_pod")
    }
    bootstrap = {key: {"median": statistics.median(values), "ci95": bootstrap_ci(values)} for key, values in effects.items()}
    knee_cells_41 = [cell for cell in cells if cell["phase"] == "knee" and cell["offered_rps_per_endpoint"] == 41]
    knee_cells_42 = [cell for cell in cells if cell["phase"] == "knee" and cell["offered_rps_per_endpoint"] == 42]
    variability = {
        "useful_41_cv": sample_cv(cell["per_pod_useful_rps"] for cell in knee_cells_41),
        "useful_42_cv": sample_cv(cell["per_pod_useful_rps"] for cell in knee_cells_42),
        "p99_41_cv": sample_cv(cell["median_endpoint_p99_us"] for cell in knee_cells_41),
        "p99_42_cv": sample_cv(cell["median_endpoint_p99_us"] for cell in knee_cells_42),
    }
    paired_pass = all(
        row["delta_success"] < 0
        and row["delta_drain"] > 0
        and row["p99_ratio"] > 1
        and row["marginal_useful_rps_per_pod"] < 1
        for row in block_rows
    )
    bootstrap_pass = (
        bootstrap["delta_success"]["ci95"][1] < 0
        and bootstrap["delta_drain"]["ci95"][0] > 0
        and bootstrap["p99_ratio"]["ci95"][0] > 1.25
        and bootstrap["marginal_useful_rps_per_pod"]["ci95"][1] < 1
    )
    variability_pass = (
        variability["useful_41_cv"] <= 0.02
        and variability["useful_42_cv"] <= 0.02
        and variability["p99_41_cv"] <= 0.10
        and variability["p99_42_cv"] <= 0.20
    )

    pre35 = by_id["scale-pre-35"]
    post35 = by_id["scale-post-35"]
    pre_post_useful_delta = abs(post35["aggregate_useful_rps"] - pre35["aggregate_useful_rps"]) / pre35["aggregate_useful_rps"]
    pre_post_p99_ratio = max(post35["median_endpoint_p99_us"], pre35["median_endpoint_p99_us"]) / min(post35["median_endpoint_p99_us"], pre35["median_endpoint_p99_us"])
    scale_sentinels_pass = (
        pre35["errors_total"] == post35["errors_total"] == 0
        and pre35["drain_ratio"] <= 0.01 and post35["drain_ratio"] <= 0.01
        and pre_post_useful_delta <= thresholds["pre_post_35_useful_relative_delta_max"]
        and pre_post_p99_ratio <= thresholds["pre_post_35_p99_ratio_max"]
    )

    r1_pre_41 = by_id["r1-pre-41"]
    r1_pre_42 = by_id["r1-pre-42"]
    r1_post_41 = by_id["r1-post-41"]
    r1_post_42 = by_id["r1-post-42"]
    r1_41_drift = abs(r1_post_41["per_pod_useful_rps"] - r1_pre_41["per_pod_useful_rps"]) / r1_pre_41["per_pod_useful_rps"]
    r1_41_p99_ratio = max(r1_post_41["median_endpoint_p99_us"], r1_pre_41["median_endpoint_p99_us"]) / min(r1_post_41["median_endpoint_p99_us"], r1_pre_41["median_endpoint_p99_us"])
    r1_sentinels_pass = (
        clean41(r1_pre_41) and clean41(r1_post_41)
        and stressed42(r1_pre_42) and stressed42(r1_post_42)
        and r1_pre_42["median_endpoint_p99_us"] / r1_pre_41["median_endpoint_p99_us"] > 1.25
        and r1_post_42["median_endpoint_p99_us"] / r1_post_41["median_endpoint_p99_us"] > 1.25
        and r1_41_drift <= 0.02 and r1_41_p99_ratio <= 1.20
    )

    knee_confirmed = (
        all(row["rate_41_clean"] and row["rate_42_stressed"] for row in block_rows)
        and paired_pass and bootstrap_pass and variability_pass
        and scale_sentinels_pass and r1_sentinels_pass
    )
    if knee_confirmed:
        status = "confirmed_at_rung"
        interpretation = "The scoped per-Pod service/SLO knee remains in (41,42] at this horizontal rung."
    elif not all(row["rate_41_clean"] for row in block_rows):
        status = "knee_at_or_below_41_or_scale_interference"
        interpretation = "At least one pure q=41N cell was not clean; inspect infrastructure and endpoint dispersion before attributing a code-knee shift."
    elif not any(row["rate_42_stressed"] for row in block_rows):
        status = "knee_above_42_at_rung"
        interpretation = "The q=42N cells did not reproduce the scoped stressed side."
    else:
        status = "inconclusive"
        interpretation = "The valid campaign produced mixed knee, drift, bootstrap, or variability evidence."

    r1_reference_41 = statistics.median([r1_pre_41["per_pod_useful_rps"], r1_post_41["per_pod_useful_rps"]])
    horizontal_efficiency_41 = statistics.median(cell["aggregate_useful_rps"] for cell in knee_cells_41) / (plan["rung_replicas"] * r1_reference_41)
    return {
        "status": status,
        "knee_confirmed": knee_confirmed,
        "interpretation": interpretation,
        "block_effects": block_rows,
        "bootstrap_100000_seed_20260829": bootstrap,
        "variability_ddof_1": variability,
        "paired_direction_gate": paired_pass,
        "bootstrap_gate": bootstrap_pass,
        "variability_gate": variability_pass,
        "scale_pre_post_35": {
            "useful_relative_delta": pre_post_useful_delta,
            "p99_ratio": pre_post_p99_ratio,
            "passed": scale_sentinels_pass,
        },
        "r1_sentinels": {
            "rate_41_useful_relative_delta": r1_41_drift,
            "rate_41_p99_ratio": r1_41_p99_ratio,
            "passed": r1_sentinels_pass,
        },
        "horizontal_efficiency_at_41": horizontal_efficiency_41,
        "horizontal_efficiency_definition": "median aggregate useful RPS in pure q=41N cells divided by N times median pre/post r1 41-RPS useful rate",
    }


def summarize(run_dir: Path) -> dict[str, Any]:
    plan = load_json(run_dir / "campaign-plan.json")
    ledger = load_json(run_dir / "sequence-ledger.json")
    validate_plan(plan, ledger)
    preflight = load_json(run_dir / "capacity-preflight.json")
    require(preflight.get("load_authorized") is True, "capacity preflight did not authorize the rung")
    validate_scale_batches(plan, run_dir)
    pods = validate_inventory(plan, run_dir)
    pods_by_endpoint = {int(pod["endpoint_ordinal"]): pod for pod in pods}

    summaries: list[dict[str, Any]] = []
    runtime_cells: list[dict[str, Any]] = []
    jobs_by_cell: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for job in plan["jobs"]:
        jobs_by_cell[int(job["cell_ordinal"])].append(job)
    for cell in plan["cells"]:
        cell_dir = run_dir / "cells" / f"c{cell['ordinal']:02d}-{cell['cell_id']}"
        runtime_cell = load_json(cell_dir / "cell-runtime.json")
        require(runtime_cell.get("cell_id") == cell["cell_id"], f"{cell['cell_id']}: runtime identity mismatch")
        require(runtime_cell.get("jobs_deleted_before_next_cell") is True, f"{cell['cell_id']}: not independently cleaned before next cell")
        runtime_cells.append(runtime_cell)
        jobs = sorted(jobs_by_cell[cell["ordinal"]], key=lambda row: row["endpoint_ordinal"])
        armed_by_job = validate_armed(plan, cell, runtime_cell, load_json(cell_dir / "driver-armed.json"), jobs, pods_by_endpoint)
        endpoint_results = []
        for job in jobs:
            report = load_json(cell_dir / "drivers" / f"e{job['endpoint_ordinal']:02d}.json")
            endpoint_results.append(
                validate_report(plan, cell, runtime_cell, job, pods_by_endpoint[job["endpoint_ordinal"]], armed_by_job[job["job_id"]], report)
            )
        summaries.append(aggregate_cell(cell, runtime_cell, endpoint_results))

    ordered = sorted(runtime_cells, key=lambda row: row["start_epoch_ms"])
    require([row["cell_id"] for row in ordered] == [cell["cell_id"] for cell in plan["cells"]], "cells overlapped or ran out of order")
    for left, right in zip(ordered, ordered[1:]):
        require(left["end_epoch_ms"] <= right["start_epoch_ms"], f"cells {left['cell_id']} and {right['cell_id']} overlap")

    validate_health(plan, run_dir, pods)
    validate_telemetry(plan, run_dir, pods, runtime_cells)
    baseline = load_json(run_dir / "target-counters-baseline.json")
    final = load_json(run_dir / "target-counters-final.json")
    baseline_by_endpoint = {row["endpoint_ordinal"]: row["counters"] for row in baseline.get("pods", [])}
    final_by_endpoint = {row["endpoint_ordinal"]: row["counters"] for row in final.get("pods", [])}
    expected_ok: dict[int, int] = defaultdict(int)
    for cell in summaries:
        for endpoint in cell["endpoint_results"]:
            expected_ok[endpoint["endpoint_ordinal"]] += endpoint["ok_total"]
    for endpoint in range(plan["rung_replicas"]):
        require(baseline_by_endpoint.get(endpoint) == {"served": 0, "hits": 0, "misses": 0}, f"endpoint {endpoint}: non-fresh baseline")
        counters = final_by_endpoint.get(endpoint)
        require(counters is not None, f"endpoint {endpoint}: final counters missing")
        require(counters.get("served") == expected_ok[endpoint], f"endpoint {endpoint}: served counter mismatch")
        require(counters.get("hits") == 0 and counters.get("misses") == expected_ok[endpoint], f"endpoint {endpoint}: cache contamination")

    final_ledger = update_ledger(run_dir)
    require(final_ledger.get("lifecycle_counts", {}).get("emitted") == len(plan["jobs"]), "sequence ledger is not fully emitted")
    result_decision = decision(plan, summaries)
    return {
        "schema_version": 1,
        "protocol": plan["protocol"],
        "run_id": plan["run_id"],
        "rung_replicas": plan["rung_replicas"],
        "validity": {"status": "valid", "all_external_attribution_gates_passed": True},
        "decision": result_decision,
        "cells": summaries,
        "sequence_ledger": {
            "reservation": final_ledger["reservation"],
            "lifecycle_counts": final_ledger["lifecycle_counts"],
            "path": "sequence-ledger-final.json",
        },
        "scope_note": "This is unchanged W1/RT1 SC over direct Pod IPs; it is not ClusterIP, gateway, production-routing, or multi-node target evidence.",
    }


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("run_dir", type=Path)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--ledger-only", action="store_true")
    parser.add_argument("--validate-armed-cell", type=int)
    parser.add_argument("--validate-completed-cell", type=int)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(sys.argv[1:] if argv is None else argv)
    if args.ledger_only:
        try:
            update_ledger(args.run_dir, args.output)
        except ValidationError as exc:
            print(f"ERROR: {exc}", file=sys.stderr)
            return 2
        return 0
    if args.validate_armed_cell is not None:
        if args.output is None:
            print("ERROR: --validate-armed-cell requires --output", file=sys.stderr)
            return 2
        try:
            materialize_armed_cell(args.run_dir, args.validate_armed_cell, args.output)
        except ValidationError as exc:
            print(f"ERROR: {exc}", file=sys.stderr)
            return 2
        return 0
    if args.validate_completed_cell is not None:
        if args.output is None:
            print("ERROR: --validate-completed-cell requires --output", file=sys.stderr)
            return 2
        try:
            materialize_completed_cell(args.run_dir, args.validate_completed_cell, args.output)
        except ValidationError as exc:
            print(f"ERROR: {exc}", file=sys.stderr)
            return 2
        return 0
    try:
        result = summarize(args.run_dir)
    except ValidationError as exc:
        invalid = {
            "schema_version": 1,
            "validity": {"status": "invalid", "all_external_attribution_gates_passed": False, "error": str(exc)},
            "decision": None,
        }
        if args.output:
            args.output.write_text(json.dumps(invalid, indent=2, sort_keys=True) + "\n")
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2
    rendered = json.dumps(result, indent=2, sort_keys=True) + "\n"
    if args.output:
        args.output.write_text(rendered)
    else:
        sys.stdout.write(rendered)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
