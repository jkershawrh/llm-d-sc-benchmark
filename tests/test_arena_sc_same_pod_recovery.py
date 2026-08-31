#!/usr/bin/env python3
"""Focused, cluster-free tests for the same-Pod recovery harness."""

from __future__ import annotations

import importlib.util
import hashlib
import json
import os
import subprocess
import sys
import tempfile
import time
import unittest
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
ORCHESTRATOR = ROOT / "hack" / "arena-sc-same-pod-recovery-cycle.sh"
SUMMARIZER = ROOT / "hack" / "arena-sc-same-pod-recovery-summarize.py"
SPEC = importlib.util.spec_from_file_location("arena_sc_same_pod_recovery_summarize", SUMMARIZER)
assert SPEC and SPEC.loader
SUMMARY = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = SUMMARY
SPEC.loader.exec_module(SUMMARY)


DRIVER_IMAGE = SUMMARY.ARMED_DRIVER_IMAGE
DRIVER_SOURCE_SHA256 = SUMMARY.ARMED_DRIVER_SOURCE_SHA256
TARGET_IMAGE = "sha256:" + "b" * 64
MODEL = "c" * 64
TOKENIZER = "d" * 64


def plan(cycle_index: int = 0) -> dict:
    t0 = 2_000_000_000_000
    base = 19_000_000_000 + cycle_index * 150_000
    offsets = [0, 1, 2, 3, 5, 8, 13, 21, 34, 55, 89]
    jobs = [
        {
            "ordinal": 0,
            "name": "scr-test-j00",
            "phase": "pre",
            "offered_rps": "35",
            "duration_seconds": 180,
            "start_epoch_ms": t0,
            "expected_slots": 6_300,
            "recovery_offset_seconds": None,
            "sequence_base": base,
            "candidate_rows": 10_000,
            "warmup_requests": 0,
            "max_in_flight": 512,
        },
        {
            "ordinal": 1,
            "name": "scr-test-j01",
            "phase": "overload",
            "offered_rps": "47",
            "duration_seconds": 120,
            "start_epoch_ms": t0 + 185_000,
            "expected_slots": 5_640,
            "recovery_offset_seconds": None,
            "sequence_base": base + 10_001,
            "candidate_rows": 10_000,
            "warmup_requests": 0,
            "max_in_flight": 512,
        },
    ]
    for index, offset in enumerate(offsets, 2):
        jobs.append(
            {
                "ordinal": index,
                "name": f"scr-test-j{index:02d}",
                "phase": "recovery_probe",
                "offered_rps": "1",
                "duration_seconds": 1,
                "start_epoch_ms": t0 + 305_000 + offset * 1_000,
                "expected_slots": 1,
                "recovery_offset_seconds": offset,
                "sequence_base": base + index * 10_001,
                "candidate_rows": 10_000,
                "warmup_requests": 0,
                "max_in_flight": 1,
            }
        )
    jobs.append(
        {
            "ordinal": 13,
            "name": "scr-test-j13",
            "phase": "post",
            "offered_rps": "35",
            "duration_seconds": 180,
            "start_epoch_ms": t0 + 400_000,
            "expected_slots": 6_300,
            "recovery_offset_seconds": None,
            "sequence_base": base + 13 * 10_001,
            "candidate_rows": 10_000,
            "warmup_requests": 0,
            "max_in_flight": 512,
        }
    )
    for job in jobs:
        job["arming_nonce"] = hashlib.sha256(
            f"test|{cycle_index}|{job['ordinal']}|{t0}".encode()
        ).hexdigest()
    return {
        "schema_version": 1,
        "protocol": "same_pod_open_loop_recovery_v1",
        "run_id": "test",
        "cycle_index": cycle_index,
        "t0_epoch_ms": t0,
        "sequence_reservation": {
            "cycle_base": base,
            "job_span": 10_001,
            "reserved_end_exclusive": base + 150_000,
        },
        "target_shape": {
            "inference_workers": "1",
            "rayon_num_threads": "1",
            "candle_num_threads": "unset",
            "metrics_log_seconds": "10",
            "qos_class": "Guaranteed",
            "resources": {
                "requests": {"cpu": "2", "memory": "4Gi"},
                "limits": {"cpu": "2", "memory": "4Gi"},
            },
            "runtime_cpu_max": "max",
            "runtime_cpuset_logical_cpus": 2,
            "complete_smt_sibling_sets": True,
            "runtime_pid1_executable": "/usr/local/bin/llm-d-sc",
            "runtime_environment_verified": True,
        },
        "arming": {
            "required": True,
            "protocol": "sustained-corpus-probe-armed-v1",
            "pinned_driver_supports_protocol": True,
            "live_executable": True,
            "pair_matches_allowlist": True,
            "allowlist": {
                "driver_image": DRIVER_IMAGE,
                "driver_source_sha256": DRIVER_SOURCE_SHA256,
            },
            "blocker": None,
            "validation_contract": {
                "records": 14,
                "deadline": "T0-180s",
                "all_jobs_required": True,
                "schema": "llm-d-sc.benchmark-driver.armed",
                "schema_version": 1,
                "record_type": "ARMED",
                "explicit_config_required": True,
                "all_config_fields_must_match_frozen_job": True,
                "digest_role": "recorded pinned-driver provenance; explicit config equality authorizes load",
            },
        },
        "pinned": {
            "driver_image": DRIVER_IMAGE,
            "driver_source_sha256": DRIVER_SOURCE_SHA256,
            "driver_package_version": "0.1.0",
            "target_image": TARGET_IMAGE,
            "model_sha256": MODEL,
            "tokenizer_sha256": TOKENIZER,
            "local_driver_source_sha256": DRIVER_SOURCE_SHA256,
            "local_source_matches_pinned": True,
        },
        "jobs": jobs,
        "gates": {
            "target_bound_schedule_lead_seconds": 175,
            "target_bound_completion_lead_seconds": 155,
            "pre_t0_cancellation_completion_lead_seconds": 25,
            "pre_t0_foreground_delete_timeout_seconds_max": 90,
            "pre_t0_zero_object_verification_budget_seconds": 15,
            "pre_t0_cancellation_safety_margin_seconds": 10,
        },
        "checkpoints": [
            {
                "name": "target-bound",
                "scheduled_epoch_ms": t0 - 175_000,
                "completion_deadline_epoch_ms": t0 - 155_000,
                "load_authorizing": True,
            },
            {"name": "pre-mid", "scheduled_epoch_ms": t0 + 90_000},
            {"name": "gap-mid", "scheduled_epoch_ms": t0 + 182_000},
            {"name": "overload-mid", "scheduled_epoch_ms": t0 + 245_000},
            {"name": "recovery-30", "scheduled_epoch_ms": t0 + 335_000},
            {"name": "recovery-50", "scheduled_epoch_ms": t0 + 355_000},
            {"name": "post-mid", "scheduled_epoch_ms": t0 + 490_000},
            {"name": "post-after", "scheduled_epoch_ms": t0 + 582_000},
        ],
    }


def provenance() -> dict:
    return {
        "plan_created_epoch_ms": 1_999_999_600_000,
        "target": {
            "name": "target-1",
            "uid": "uid-1",
            "ip": "10.0.0.1",
            "node": "target-node",
            "image_id": "registry.invalid/target@" + TARGET_IMAGE,
            "container_started_at": "2026-01-01T00:00:00Z",
            "cpuset_cpus_effective": "5,149",
            "cpu_max": "max 100000",
        },
        "target_image": TARGET_IMAGE,
        "model_sha256": MODEL,
        "tokenizer_sha256": TOKENIZER,
        "driver_image": DRIVER_IMAGE,
        "driver_package_version": "0.1.0",
        "topology": "cross-node-direct-target-node-from-driver-node",
        "target_container": "llm-d-sc",
        "target_shape": {
            "inference_workers": "1",
            "rayon_num_threads": "1",
            "candle_num_threads": "unset",
            "metrics_log_seconds": "10",
            "qos_class": "Guaranteed",
            "resources": {
                "requests": {"cpu": "2", "memory": "4Gi"},
                "limits": {"cpu": "2", "memory": "4Gi"},
            },
            "runtime": {
                "cpu_max_quota": "max",
                "cpuset_logical_cpus": 2,
                "complete_smt_sibling_sets": True,
                "pid1_executable": "/usr/local/bin/llm-d-sc",
                "environment_verified": True,
            },
        },
        "counter_attribution": {
            "baseline": {"served": 0, "hits": 0, "misses": 0},
            "baseline_log_sha256": "unused",
            "tolerance": 0,
            "tolerance_justification": "zero: a traffic-clean Pod, exclusive lock, completed drivers, and two metrics-log settle intervals permit exact reconciliation",
        },
        "scheduler_thresholds": {
            "dispatch_late_after_ms": 1,
            "drop_late_after_ms": 100,
            "rpc_timeout_ms": 30_000,
            "max_dispatch_p99_lag_ms": 5,
            "max_drain_seconds": 90,
        },
    }


def driver_report(job: dict, *, ok_within: int | None = None, ok_after: int = 0) -> dict:
    offered = job["expected_slots"]
    ok_within = offered if ok_within is None else ok_within
    completed = ok_within + ok_after
    within_statuses = {"OK": ok_within} if ok_within else {}
    after_statuses = {"OK": ok_after} if ok_after else {}
    total_statuses = {"OK": completed} if completed else {}
    rtts = [24_000] * ok_within
    total_rtts = [24_000] * completed
    end = job["start_epoch_ms"] + job["duration_seconds"] * 1_000
    return {
        "schema_version": 2,
        "probe": "sustained_exact_token_corpus",
        "load_model": "open_loop_deterministic_offered_rate",
        "target": "10.0.0.1:50051",
        "target_image": TARGET_IMAGE,
        "model_sha256": MODEL,
        "tokenizer_sha256": TOKENIZER,
        "topology": "cross-node-direct-target-node-from-driver-node",
        "corpus_mode": "generated",
        "generator_scheme": "alpha_bravo_lsb_identity_service_fill_v1",
        "token_count_including_specials": 64,
        "connections": 1,
        "closed_loop_concurrency_argument": 1,
        "warmup_requests": 0,
        "candidate_rows": 10_000,
        "scheduled_plateau_rows": offered,
        "first_sequence": job["sequence_base"],
        "last_sequence": job["sequence_base"] + 9_999,
        "start_epoch_ms": job["start_epoch_ms"],
        "duration_seconds": job["duration_seconds"],
        "drain_completed_epoch_ms": end,
        "scheduler_ready_epoch_ms": job["start_epoch_ms"] - 1_000,
        "corpus_exhausted": False,
        "selected_rows_blake3": "e" * 64,
        "scheduled_rows_blake3": "f" * 64,
        "open_loop": {
            "protocol_version": "deterministic_offered_rate_v1",
            "driver_image": DRIVER_IMAGE,
            "offered_rate": {"requested_decimal": job["offered_rps"]},
            "max_in_flight": job["max_in_flight"],
            "dispatch_late_after_ms": 1,
            "drop_late_after_ms": 100,
            "rpc_timeout_ms": 30_000,
            "raw_rtt_collection": "always enabled in open-loop mode",
        },
        "accounting": {
            "offered_slots": offered,
            "initiated_requests": completed,
            "completed_requests": completed,
            "completed_within_plateau": ok_within,
            "completed_after_plateau": ok_after,
            "dropped_in_flight_limit": offered - completed,
            "dropped_schedule_late": 0,
        },
        "statuses_completed_within_plateau": within_statuses,
        "drained_after_plateau": after_statuses,
        "statuses_completed_total": total_statuses,
        "successful_rtt_raw_us": rtts,
        "rtt_raw_us_by_status": {"OK": total_rtts} if total_rtts else {},
        "dispatch_lag_raw_us": [1_000] * completed,
    }


def phase_result(phase: str, *, offset: int | None = None, good: bool = True) -> dict:
    if phase in {"pre", "post"}:
        offered = 6_300
        return {
            "phase": phase,
            "offered_slots": offered,
            "initiated": offered,
            "completed": offered,
            "offered_success_ratio": 1.0,
            "drain_ratio": 0.0,
            "useful_rps": 35.0,
            "resource_exhausted": 0,
            "latency_us": {"p50": 24_000, "p99": 28_000},
            "ok_total": offered,
            "ok_rtt_us_total": [24_000] * offered,
            "statuses_total": {"OK": offered},
        }
    if phase == "overload":
        return {
            "phase": phase,
            "offered_slots": 5_640,
            "initiated": 5_640,
            "completed": 5_640,
            "offered_success_ratio": 0.88,
            "drain_ratio": 0.04,
            "useful_rps": 41.3,
            "resource_exhausted": 40,
            "latency_us": {"p50": 2_000_000, "p99": 6_000_000},
            "ok_total": 5_600,
            "ok_rtt_us_total": [100_000] * 5_600,
            "statuses_total": {"OK": 5_600, "GRPC_RESOURCEEXHAUSTED": 40},
        }
    return {
        "phase": "recovery_probe",
        "recovery_offset_seconds": offset,
        "offered_slots": 1,
        "initiated": 1,
        "completed": 1,
        "offered_success_ratio": 1.0 if good else 0.0,
        "drain_ratio": 0.0,
        "useful_rps": 1.0 if good else 0.0,
        "resource_exhausted": 0 if good else 1,
        "latency_us": {"p50": 30_000 if good else None, "p99": 30_000 if good else None},
        "ok_total": 1 if good else 0,
        "ok_rtt_us_total": [30_000] if good else [],
        "statuses_total": {"OK": 1} if good else {"GRPC_RESOURCEEXHAUSTED": 1},
    }


SERVICE_THRESHOLDS = {
    "steady_success_min": 0.999,
    "steady_drain_max": 0.001,
    "post_useful_relative_delta_max": 0.02,
    "post_p50_ratio_max": 1.10,
    "post_p99_ratio_max": 1.20,
    "overload_queue_ratio_min_exclusive": 10,
    "overload_drain_ratio_min_exclusive": 0.01,
}


def write_shape_artifacts(
    run_dir: Path,
    *,
    workers: str = "1",
    rayon: str = "1",
    candle: str | None = None,
    qos: str = "Guaranteed",
    cpu_request: str = "2",
    cpu_limit: str = "2",
    memory_request: str = "4Gi",
    memory_limit: str = "4Gi",
    cpu_max: str = "max 100000",
    cpuset: str = "5,149",
    env_from: list[dict] | None = None,
    command: list[str] | None = None,
    args: list[str] | None = None,
    runtime_candle: str | None = None,
    pid1_executable: str = "/usr/local/bin/llm-d-sc",
) -> None:
    environment = [
        {"name": "LLM_D_SC_INFERENCE_WORKERS", "value": workers},
        {"name": "RAYON_NUM_THREADS", "value": rayon},
        {"name": "LLM_D_SC_METRICS_LOG_SECS", "value": "10"},
    ]
    if candle is not None:
        environment.append({"name": "CANDLE_NUM_THREADS", "value": candle})
    target_container = {
        "name": "llm-d-sc",
        "env": environment,
        "resources": {
            "requests": {"cpu": cpu_request, "memory": memory_request},
            "limits": {"cpu": cpu_limit, "memory": memory_limit},
        },
    }
    if env_from is not None:
        target_container["envFrom"] = env_from
    if command is not None:
        target_container["command"] = command
    if args is not None:
        target_container["args"] = args
    (run_dir / "deployment-before.json").write_text(
        json.dumps(
            {
                "spec": {
                    "template": {
                        "spec": {
                            "containers": [target_container]
                        }
                    }
                }
            }
        ),
        encoding="utf-8",
    )
    (run_dir / "targets-before.json").write_text(
        json.dumps({"items": [{"status": {"qosClass": qos}}]}), encoding="utf-8"
    )
    fields = cpu_max.split()
    (run_dir / "runtime-cgroup-before.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "cpuset_cpus_effective": cpuset,
                "cpu_max": cpu_max,
                "cpu_max_quota": fields[0] if fields else "",
                "logical_cpus": 2,
            }
        ),
        encoding="utf-8",
    )
    (run_dir / "runtime-process-before.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "pid1_executable": pid1_executable,
                "environment": {
                    "LLM_D_SC_INFERENCE_WORKERS": workers,
                    "RAYON_NUM_THREADS": rayon,
                    "LLM_D_SC_METRICS_LOG_SECS": "10",
                    "CANDLE_NUM_THREADS": runtime_candle,
                },
                "candle_num_threads_present": runtime_candle is not None,
            }
        ),
        encoding="utf-8",
    )


def write_counter_baseline(run_dir: Path, document: dict, before: str) -> dict:
    before_path = run_dir / "target-logs-before.txt"
    before_path.write_text(before, encoding="utf-8")
    digest = hashlib.sha256(before.encode()).hexdigest()
    (run_dir / "target-counter-baseline.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "target_uid": "uid-1",
                "target_ip": "10.0.0.1",
                "container_started_at": "2026-01-01T00:00:00Z",
                "captured_epoch_ms": document["t0_epoch_ms"] - 220_000,
                "quiet_interval_seconds": 12,
                "log_sha256": digest,
                "traffic_clean": True,
                "counters": {"served": 0, "hits": 0, "misses": 0},
            }
        ),
        encoding="utf-8",
    )
    result = provenance()
    result["counter_attribution"]["baseline_log_sha256"] = digest
    return result


def armed_document(document: dict) -> dict:
    records = []
    for job in document["jobs"]:
        config = {
            "candidate_rows": 10_000,
            "closed_loop_concurrency_argument": 1,
            "connections": 1,
            "corpus_blake3": None,
            "corpus_mode": "generated",
            "corpus_offset": 0,
            "dispatch_late_after_ms": 1,
            "driver_image": DRIVER_IMAGE,
            "driver_package_version": "0.1.0",
            "drop_late_after_ms": 100,
            "duration_seconds": job["duration_seconds"],
            "expected_slots": job["expected_slots"],
            "first_sequence": job["sequence_base"],
            "generator_scheme": "alpha_bravo_lsb_identity_service_fill_v1",
            "job_id": job["name"],
            "last_sequence": job["sequence_base"] + 9_999,
            "max_in_flight": job["max_in_flight"],
            "model_sha256": MODEL,
            "nonce": job["arming_nonce"],
            "offered_rate_denominator": 1,
            "offered_rate_numerator": int(job["offered_rps"]),
            "offered_rate_requested_decimal": job["offered_rps"],
            "offered_rps": job["offered_rps"],
            "protocol_version": "sustained-corpus-probe-armed-v1",
            "raw_latencies": True,
            "rpc_timeout_ms": 30_000,
            "run_id": document["run_id"],
            "scheduled_rows_blake3": "f" * 64,
            "scheduled_start_epoch_ms": job["start_epoch_ms"],
            "selected_rows_blake3": "e" * 64,
            "target_endpoint": "10.0.0.1:50051",
            "target_image": TARGET_IMAGE,
            "token_count_including_specials": 64,
            "tokenizer_sha256": TOKENIZER,
            "topology": "cross-node-direct-target-node-from-driver-node",
            "warmup_requests": 0,
        }
        records.append(
            {
                "schema": "llm-d-sc.benchmark-driver.armed",
                "schema_version": 1,
                "record_type": "ARMED",
                "protocol_version": "sustained-corpus-probe-armed-v1",
                "run_id": document["run_id"],
                "job_id": job["name"],
                "nonce": job["arming_nonce"],
                "endpoint": "10.0.0.1:50051",
                "scheduled_start_epoch_ms": job["start_epoch_ms"],
                "expected_slots": job["expected_slots"],
                "duration_seconds": job["duration_seconds"],
                "armed_epoch_ms": document["t0_epoch_ms"] - 200_000,
                "scheduled_rows_blake3": "f" * 64,
                "config": config,
                "config_digest": {
                    "algorithm": "blake3",
                    "canonicalization": "sorted-string-map-v1",
                    "hex": hashlib.sha256(job["name"].encode()).hexdigest(),
                },
            }
        )
    return {
        "schema_version": 1,
        "protocol": "sustained-corpus-probe-armed-v1",
        "verified_epoch_ms": document["t0_epoch_ms"] - 200_000,
        "all_14_armed": True,
        "records": records,
    }


def iso_timestamp(epoch_seconds: int) -> str:
    return datetime.fromtimestamp(epoch_seconds, tz=timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%S.000000Z"
    )


def write_full_valid_run(run_dir: Path) -> None:
    document = plan()
    t0_seconds = document["t0_epoch_ms"] // 1_000
    document["created_epoch_ms"] = document["t0_epoch_ms"] - 400_000
    driver_source = DRIVER_SOURCE_SHA256
    document["pinned"] = {
        "driver_image": DRIVER_IMAGE,
        "driver_source_sha256": driver_source,
        "driver_package_version": "0.1.0",
        "target_image": TARGET_IMAGE,
        "model_sha256": MODEL,
        "tokenizer_sha256": TOKENIZER,
        "local_driver_source_sha256": driver_source,
        "local_source_matches_pinned": True,
    }
    (run_dir / "recovery-plan.json").write_text(
        json.dumps(document), encoding="utf-8"
    )

    write_shape_artifacts(run_dir)
    live_provenance = provenance()
    live_provenance.update(
        {
            "schema_version": 1,
            "run_id": "test",
            "namespace": "test-ns",
            "deployment": "classifier-target",
            "plan_created_epoch_ms": document["t0_epoch_ms"] - 400_000,
            "driver_node": "driver-node",
            "driver_source_sha256": driver_source,
            "service_thresholds": dict(SERVICE_THRESHOLDS),
        }
    )
    live_provenance["target"].update(
        {
            "ready_transition_time": iso_timestamp(t0_seconds - 240),
            "container_started_at": iso_timestamp(t0_seconds - 300),
        }
    )

    pod = {
        "metadata": {"name": "target-1", "uid": "uid-1"},
        "spec": {"nodeName": "target-node"},
        "status": {
            "phase": "Running",
            "podIP": "10.0.0.1",
            "qosClass": "Guaranteed",
            "conditions": [
                {
                    "type": "Ready",
                    "status": "True",
                    "lastTransitionTime": iso_timestamp(t0_seconds - 240),
                }
            ],
            "containerStatuses": [
                {
                    "name": "llm-d-sc",
                    "restartCount": 0,
                    "imageID": "registry.invalid/target@" + TARGET_IMAGE,
                    "state": {
                        "running": {"startedAt": iso_timestamp(t0_seconds - 300)}
                    },
                }
            ],
        },
    }
    for artifact in ("targets-before.json", "targets-after.json"):
        (run_dir / artifact).write_text(
            json.dumps({"items": [pod]}), encoding="utf-8"
        )

    (run_dir / "runtime-cgroup-before.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "cpuset_cpus_effective": "5,149",
                "cpu_max": "max 100000",
                "cpu_max_quota": "max",
                "logical_cpus": 2,
            }
        ),
        encoding="utf-8",
    )

    topology_report = {
        "schema_version": 1,
        "verdict": "PASS",
        "placement_verdict": "PASS",
        "gate_passed": True,
        "pods": [
            {
                "name": "target-1",
                "uid": "uid-1",
                "node": "target-node",
                "cpuset": "5,149",
                "complete_smt_sibling_sets": True,
            }
        ],
    }
    topology_report_path = run_dir / "topology-preflight-report.json"
    topology_stdout_path = run_dir / "topology-preflight-stdout.txt"
    topology_stderr_path = run_dir / "topology-preflight-stderr.txt"
    topology_report_path.write_text(json.dumps(topology_report), encoding="utf-8")
    topology_stdout_path.write_text(json.dumps(topology_report), encoding="utf-8")
    topology_stderr_path.write_text("", encoding="utf-8")
    (run_dir / "topology-preflight-execution.json").write_text(
        json.dumps(
            {
                "runner_exit_code": 0,
                "report_json_valid": True,
                "report_gate_valid": True,
                "target_identity_match": True,
                "load_authorized": True,
                "evidence_sha256": {
                    "report": hashlib.sha256(topology_report_path.read_bytes()).hexdigest(),
                    "raw_stdout": hashlib.sha256(topology_stdout_path.read_bytes()).hexdigest(),
                    "stderr": hashlib.sha256(topology_stderr_path.read_bytes()).hexdigest(),
                },
            }
        ),
        encoding="utf-8",
    )

    job_items = []
    driver_pods = []
    for job in document["jobs"]:
        args = [
            "--target",
            "10.0.0.1:50051",
            "--start-epoch-ms",
            str(job["start_epoch_ms"]),
            "--armed-run-id",
            "test",
            "--armed-job-id",
            job["name"],
            "--armed-nonce",
            job["arming_nonce"],
        ]
        job_items.append(
            {
                "metadata": {
                    "name": job["name"],
                    "creationTimestamp": iso_timestamp(t0_seconds - 250),
                    "annotations": {
                        "benchmark.llm-d/target-uid": "uid-1",
                        "benchmark.llm-d/target-ip": "10.0.0.1",
                    },
                },
                "spec": {
                    "suspend": True,
                    "template": {
                        "spec": {"containers": [{"image": DRIVER_IMAGE, "args": args}]}
                    },
                },
            }
        )
        driver_pods.append(
            {
                "spec": {"nodeName": "driver-node"},
                "status": {
                    "phase": "Running",
                    "conditions": [{"type": "Ready", "status": "True"}],
                    "containerStatuses": [
                        {
                            "restartCount": 0,
                            "imageID": "registry.invalid/driver@" + DRIVER_IMAGE.split("@", 1)[1],
                        }
                    ],
                },
            }
        )
    (run_dir / "jobs-precreated.json").write_text(
        json.dumps({"items": job_items}), encoding="utf-8"
    )
    (run_dir / "driver-pods-before.json").write_text(
        json.dumps({"items": driver_pods}), encoding="utf-8"
    )
    (run_dir / "driver-kubernetes-readiness.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "verified_epoch_ms": document["t0_epoch_ms"] - 200_000,
                "t0_epoch_ms": document["t0_epoch_ms"],
                "ready_driver_pods": 14,
                "minimum_lead_seconds": 180,
                "load_authorizing": False,
            }
        ),
        encoding="utf-8",
    )

    armed = armed_document(document)
    reports = []
    for job, record in zip(document["jobs"], armed["records"]):
        report = (
            driver_report(job, ok_within=5_580, ok_after=60)
            if job["phase"] == "overload"
            else driver_report(job)
        )
        selected_hash = f"{job['ordinal'] + 1:064x}"
        scheduled_hash = f"{job['ordinal'] + 101:064x}"
        report["selected_rows_blake3"] = selected_hash
        report["scheduled_rows_blake3"] = scheduled_hash
        record["scheduled_rows_blake3"] = scheduled_hash
        record["config"]["selected_rows_blake3"] = selected_hash
        record["config"]["scheduled_rows_blake3"] = scheduled_hash
        reports.append(report)
    (run_dir / "driver-armed.json").write_text(
        json.dumps(armed), encoding="utf-8"
    )
    drivers = run_dir / "drivers"
    drivers.mkdir()
    for job, report in zip(document["jobs"], reports):
        (drivers / f"j{job['ordinal']:02d}.json").write_text(
            json.dumps(report), encoding="utf-8"
        )

    expected_target = {
        "name": "target-1",
        "uid": "uid-1",
        "ip": "10.0.0.1",
        "node": "target-node",
        "ready": True,
        "restart_count": 0,
        "image_id": "registry.invalid/target@" + TARGET_IMAGE,
    }
    checkpoint_dir = run_dir / "checkpoints"
    checkpoint_dir.mkdir()
    for checkpoint in document["checkpoints"]:
        (checkpoint_dir / f"{checkpoint['name']}.json").write_text(
            json.dumps(
                {
                    "schema_version": 1,
                    "name": checkpoint["name"],
                    "scheduled_epoch_ms": checkpoint["scheduled_epoch_ms"],
                    "observed_epoch_ms": checkpoint["scheduled_epoch_ms"],
                    "target": expected_target,
                    "cpuset_cpus_effective": "5,149",
                    "cpu_max": "max 100000",
                }
            ),
            encoding="utf-8",
        )
    target_bound = document["checkpoints"][0]
    (checkpoint_dir / "target-bound-gate.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "name": "target-bound",
                "completion_epoch_ms": target_bound["scheduled_epoch_ms"] + 1_000,
                "completion_deadline_epoch_ms": target_bound[
                    "completion_deadline_epoch_ms"
                ],
                "load_authorized": True,
            }
        ),
        encoding="utf-8",
    )
    post_end = document["jobs"][13]["start_epoch_ms"] // 1_000 + 180
    monitor = [
        json.dumps(
            {
                "schema_version": 1,
                "sample_epoch_s": epoch,
                "target": expected_target,
                "nodes_ready": True,
            }
        )
        for epoch in range(t0_seconds - 100, post_end + 1, 10)
    ]
    (run_dir / "health-monitor.ndjson").write_text(
        "\n".join(monitor) + "\n", encoding="utf-8"
    )
    for artifact in ("events-before.json", "events-after.json"):
        (run_dir / artifact).write_text(json.dumps({"items": []}), encoding="utf-8")

    before = "server ready\n"
    before_path = run_dir / "target-logs-before.txt"
    before_path.write_text(before, encoding="utf-8")
    baseline_hash = hashlib.sha256(before.encode()).hexdigest()
    baseline = {
        "schema_version": 1,
        "target_uid": "uid-1",
        "target_ip": "10.0.0.1",
        "container_started_at": iso_timestamp(t0_seconds - 300),
        "captured_epoch_ms": document["t0_epoch_ms"] - 220_000,
        "quiet_interval_seconds": 12,
        "log_sha256": baseline_hash,
        "traffic_clean": True,
        "counters": {"served": 0, "hits": 0, "misses": 0},
    }
    (run_dir / "target-counter-baseline.json").write_text(
        json.dumps(baseline), encoding="utf-8"
    )
    live_provenance["counter_attribution"]["baseline_log_sha256"] = baseline_hash
    total_ok = sum(report["statuses_completed_total"].get("OK", 0) for report in reports)
    overload_start = document["jobs"][1]["start_epoch_ms"] // 1_000
    (run_dir / "target-logs-full.txt").write_text(
        f"{iso_timestamp(t0_seconds + 170)} llm-d-sc metrics: served=6000 hits=0 misses=6000 | queue p50=4µs p99=8ms | tokenize p50=80µs p99=88µs\n"
        f"{iso_timestamp(overload_start + 110)} llm-d-sc metrics: served=11000 hits=0 misses=11000 | queue p50=1s p99=2s | tokenize p50=80µs p99=88µs\n"
        f"{iso_timestamp(post_end + 10)} llm-d-sc metrics: served={total_ok} hits=0 misses={total_ok} | queue p50=1s p99=2s | tokenize p50=80µs p99=88µs\n",
        encoding="utf-8",
    )

    telemetry_start = t0_seconds - 30
    telemetry_end = t0_seconds + 610
    (run_dir / "telemetry-window.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "start_epoch_s": telemetry_start,
                "end_epoch_s": telemetry_end,
                "step_seconds": 5,
                "max_gap_seconds": 10,
            }
        ),
        encoding="utf-8",
    )
    metric_dir = run_dir / "metrics"
    metric_dir.mkdir()
    epochs = list(range(telemetry_start, telemetry_end + 1, 5))

    def metric_document(label: str, names: list[str], value: str) -> dict:
        return {
            "status": "success",
            "data": {
                "result": [
                    {
                        "metric": {label: name},
                        "values": [[epoch, value] for epoch in epochs],
                    }
                    for name in names
                ]
            },
        }

    metric_contracts = {
        "pod_cpu_otel": ("k8s_pod_name", ["target-1"], "1"),
        "container_cpu_otel": ("k8s_pod_name", ["target-1"], "1"),
        "container_cpu_cadvisor": ("pod", ["target-1"], "1"),
        "memory_working_set": ("pod", ["target-1"], "1000000"),
        "restarts": ("pod", ["target-1"], "0"),
        "pod_ready": ("pod", ["target-1"], "1"),
        "node_ready": ("node", ["driver-node", "target-node"], "1"),
    }
    for name, (label, names, value) in metric_contracts.items():
        (metric_dir / f"{name}.json").write_text(
            json.dumps(metric_document(label, names, value)), encoding="utf-8"
        )
    for name in ("throttle_ratio", "cpu_pressure_waiting"):
        (metric_dir / f"{name}.json").write_text(
            json.dumps({"status": "success", "data": {"result": []}}),
            encoding="utf-8",
        )

    (run_dir / "run-provenance.json").write_text(
        json.dumps(live_provenance), encoding="utf-8"
    )


class PlanOnlyTests(unittest.TestCase):
    def test_checkpoint_delay_calculation_executes_under_nounset(self) -> None:
        completed = subprocess.run(
            [
                str(ORCHESTRATOR),
                "--internal-checkpoint-delay",
                "200000",
                "150",
            ],
            cwd=ROOT,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)
        self.assertEqual(completed.stdout.strip(), "50")

    def _assert_stalled_observer_cancellation(
        self, mode: str, *, expect_verified_zero: bool
    ) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            fake_bin = root / "bin"
            fake_bin.mkdir()
            result_root = root / "results"
            delete_log = root / "delete.log"
            observer_pid_file = result_root / "observer-stall-test" / "synthetic-observer.pid"
            fake_oc = fake_bin / "oc"
            fake_oc.write_text(
                "#!/bin/sh\n"
                "observer_alive=0\n"
                'if [ -s "$OBSERVER_PID_FILE" ]; then\n'
                '  observer_pid=$(cat "$OBSERVER_PID_FILE")\n'
                '  if kill -0 "$observer_pid" 2>/dev/null; then observer_alive=1; fi\n'
                "fi\n"
                'timestamp_ns=$(python3 -c \'import time; print(time.time_ns())\')\n'
                'printf \'%s|%s|%s\\n\' "$timestamp_ns" "$observer_alive" "$*" >>"$DELETE_LOG"\n'
                'case " $* " in\n'
                '  *" get jobs,pods "*) printf \'{"items":[]}\\n\' ;;\n'
                "esac\n"
                "exit 0\n",
                encoding="utf-8",
            )
            fake_oc.chmod(0o755)

            now = int(time.time())
            synthetic_deadline = now + 1
            synthetic_t0 = now + (5 if expect_verified_zero else 3)
            environment = os.environ.copy()
            environment.update(
                {
                    "PATH": f"{fake_bin}:{environment['PATH']}",
                    "RECOVERY_RUN_ID": "observer-stall-test",
                    "RESULT_ROOT": str(result_root),
                    "NAMESPACE": "observer-test",
                    "OBSERVER_PID_FILE": str(observer_pid_file),
                    "DELETE_LOG": str(delete_log),
                }
            )
            completed = subprocess.run(
                [str(ORCHESTRATOR), mode, str(synthetic_deadline), str(synthetic_t0)],
                cwd=ROOT,
                env=environment,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=False,
                timeout=15,
            )
            self.assertEqual(completed.returncode, 2, completed.stderr)

            marker_delay = synthetic_t0 - time.time()
            if marker_delay > 0:
                time.sleep(marker_delay)
            load_marker_ns = time.time_ns()
            (root / "synthetic-load-marker").write_text(
                str(load_marker_ns), encoding="utf-8"
            )

            log_rows = [line.split("|", 2) for line in delete_log.read_text().splitlines()]
            delete_rows = [row for row in log_rows if " delete " in f" {row[2]} "]
            self.assertGreaterEqual(len(delete_rows), 2)
            self.assertEqual(delete_rows[0][1], "1", "observer was killed before deletion began")
            self.assertTrue(
                all(
                    "-l benchmark.llm-d/run-id=observer-stall-test" in row[2]
                    for row in delete_rows
                ),
                "an observer cleanup delete was not scoped to the exact run label",
            )
            self.assertTrue(
                all(int(row[0]) < load_marker_ns for row in delete_rows),
                "a cancellation request occurred after the synthetic load marker",
            )
            self.assertIn(" delete jobs,pods ", f" {delete_rows[0][2]} ")
            self.assertIn("--wait=false", delete_rows[0][2])
            self.assertTrue(
                any(" delete jobs,pods " in f" {row[2]} " for row in delete_rows),
                "direct driver Pod deletion was not attempted",
            )

            run_dir = result_root / "observer-stall-test"
            cancellation = json.loads(
                (run_dir / "pre-t0-cancellation.json").read_text()
            )
            self.assertEqual(
                cancellation["verified_zero_before_t0"], expect_verified_zero
            )
            self.assertLess(
                cancellation["deletion_completed_epoch_ms"],
                cancellation["t0_epoch_ms"],
            )
            if expect_verified_zero:
                self.assertEqual(cancellation["remaining_labeled_jobs_and_pods"], 0)
                self.assertEqual(cancellation["foreground_delete_exit_status"], 0)
                self.assertEqual(cancellation["zero_object_snapshot_exit_status"], 0)
                self.assertLessEqual(
                    cancellation["deletion_completed_epoch_ms"],
                    cancellation["completion_deadline_epoch_ms"],
                )
                self.assertTrue(
                    any(
                        " delete jobs,pods " in f" {row[2]} "
                        and "--wait=true" in row[2]
                        for row in delete_rows
                    ),
                    "bounded foreground deletion was not executed",
                )
                status = json.loads((run_dir / "recovery-status.json").read_text())
                self.assertEqual(status["status"], "aborted")
            else:
                self.assertEqual(cancellation["foreground_delete_exit_status"], 124)
                status = json.loads((run_dir / "recovery-status.json").read_text())
                self.assertEqual(status["status"], "cleanup_failed")

            observer_pid = int(observer_pid_file.read_text())
            with self.assertRaises(ProcessLookupError):
                os.kill(observer_pid, 0)

    def test_stalled_observer_is_deleted_and_verified_zero_before_t0(self) -> None:
        self._assert_stalled_observer_cancellation(
            "--internal-observer-stall-fail-fast", expect_verified_zero=True
        )

    def test_late_stalled_observer_still_requests_job_and_pod_deletion(self) -> None:
        self._assert_stalled_observer_cancellation(
            "--internal-observer-late-stall-fail-fast", expect_verified_zero=False
        )

    def test_plan_only_is_cluster_free_and_emits_frozen_schedule(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            fake_bin = root / "bin"
            fake_bin.mkdir()
            oc_marker = root / "oc-called"
            fake_oc = fake_bin / "oc"
            fake_oc.write_text(f"#!/bin/sh\ntouch {oc_marker}\nexit 99\n", encoding="utf-8")
            fake_oc.chmod(0o755)
            result_root = root / "results"
            environment = os.environ.copy()
            environment.update(
                {
                    "PATH": f"{fake_bin}:{environment['PATH']}",
                    "RECOVERY_RUN_ID": "plan-only-test",
                    "RECOVERY_CYCLE_INDEX": "3",
                    "START_LEAD_SECONDS": "360",
                    "PLAN_ONLY": "1",
                    "RESULT_ROOT": str(result_root),
                }
            )
            completed = subprocess.run(
                [str(ORCHESTRATOR)],
                cwd=ROOT,
                env=environment,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=False,
            )
            self.assertEqual(completed.returncode, 0, completed.stderr)
            self.assertFalse(oc_marker.exists(), "PLAN_ONLY contacted the cluster")
            document = json.loads((result_root / "plan-only-test" / "recovery-plan.json").read_text())
            SUMMARY.validate_plan(document)
            self.assertEqual(document["sequence_reservation"]["cycle_base"], 19_000_450_000)
            self.assertEqual(len(document["jobs"]), 14)
            self.assertTrue(all(job["warmup_requests"] == 0 for job in document["jobs"]))
            self.assertTrue(document["arming"]["live_executable"])
            self.assertTrue(document["arming"]["pinned_driver_supports_protocol"])
            self.assertTrue(document["arming"]["pair_matches_allowlist"])
            self.assertEqual(
                document["arming"]["allowlist"],
                {
                    "driver_image": DRIVER_IMAGE,
                    "driver_source_sha256": DRIVER_SOURCE_SHA256,
                },
            )
            self.assertIsNone(document["arming"]["blocker"])
            self.assertEqual(document["pinned"]["driver_image"], DRIVER_IMAGE)
            self.assertEqual(
                document["pinned"]["driver_source_sha256"],
                DRIVER_SOURCE_SHA256,
            )
            self.assertTrue(document["pinned"]["local_source_matches_pinned"])
            self.assertEqual(document["arming"]["validation_contract"]["records"], 14)
            self.assertEqual(document["target_shape"]["runtime_cpu_max"], "max")
            status = json.loads((result_root / "plan-only-test" / "recovery-status.json").read_text())
            self.assertEqual(status["status"], "planned")

    def test_live_mode_rejects_every_non_allowlisted_driver_pair_before_cluster_call(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            fake_bin = root / "bin"
            fake_bin.mkdir()
            oc_marker = root / "oc-called"
            fake_oc = fake_bin / "oc"
            fake_oc.write_text(f"#!/bin/sh\ntouch {oc_marker}\nexit 99\n", encoding="utf-8")
            fake_oc.chmod(0o755)
            mismatches = (
                {
                    "name": "image",
                    "DRIVER_IMAGE": "registry.invalid/not-allowlisted@sha256:" + "9" * 64,
                },
                {
                    "name": "source",
                    "DRIVER_BUILD_SOURCE_SHA256": "8" * 64,
                },
            )
            for mismatch in mismatches:
                with self.subTest(mismatch=mismatch["name"]):
                    run_id = f"arming-{mismatch['name']}-fail-test"
                    environment = os.environ.copy()
                    environment.update(
                        {
                            "PATH": f"{fake_bin}:{environment['PATH']}",
                            "RECOVERY_RUN_ID": run_id,
                            "START_LEAD_SECONDS": "360",
                            "PLAN_ONLY": "0",
                            "RESULT_ROOT": str(root / "results"),
                            # Imported truth must not bypass derived equality.
                            "DRIVER_ARMING_SUPPORTED": "true",
                            **{
                                key: value
                                for key, value in mismatch.items()
                                if key != "name"
                            },
                        }
                    )
                    completed = subprocess.run(
                        [str(ORCHESTRATOR)],
                        cwd=ROOT,
                        env=environment,
                        text=True,
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE,
                        check=False,
                    )
                    self.assertEqual(completed.returncode, 2)
                    self.assertIn("not the exact smoke-validated allowlist", completed.stderr)
                    self.assertIn("sustained-corpus-probe-armed-v1", completed.stderr)
                    self.assertIn("not an application-level ARMED barrier", completed.stderr)
                    self.assertFalse(
                        oc_marker.exists(),
                        "failed ARMED allowlist check contacted the cluster",
                    )
                    run_dir = root / "results" / run_id
                    document = json.loads(
                        (run_dir / "recovery-plan.json").read_text()
                    )
                    SUMMARY.validate_plan(document)
                    self.assertFalse(document["arming"]["pair_matches_allowlist"])
                    self.assertFalse(document["arming"]["pinned_driver_supports_protocol"])
                    self.assertFalse(document["arming"]["live_executable"])
                    self.assertEqual(
                        document["arming"]["blocker"],
                        "driver image/source pair is not the exact smoke-validated ARMED allowlist",
                    )
                    status = json.loads(
                        (run_dir / "recovery-status.json").read_text()
                    )
                    self.assertEqual(status["status"], "aborted")

    def test_analyzer_rejects_forged_support_for_non_allowlisted_pair(self) -> None:
        document = plan()
        document["pinned"]["driver_source_sha256"] = "8" * 64
        with self.assertRaisesRegex(
            SUMMARY.ValidationError,
            "support must be derived from exact image/source allowlist equality",
        ):
            SUMMARY.validate_plan(document)

    def test_orchestrator_precreates_before_release_and_never_scales_target(self) -> None:
        source = ORCHESTRATOR.read_text(encoding="utf-8")
        topology = source.index('"$TOPOLOGY_PREFLIGHT_RUNNER" live')
        first_driver_manifest = source.index('create job "$job_name"')
        capture = source.index('>"${RUN_DIR}/jobs-precreated.json"')
        release = source.index('patch job "$job_name"')
        self.assertLess(topology, first_driver_manifest)
        self.assertLess(capture, release)
        self.assertNotIn('scale deployment', source)
        self.assertIn('"--warmup-requests","0"', source)
        self.assertIn('.spec.suspend=true', source)
        self.assertIn('"--armed-run-id",$armed_run_id', source)
        self.assertIn('application-level ARMED barrier failed', source)
        self.assertIn('[[ "$topology_valid" == true ]] || die', source)
        self.assertIn('phase-local 90s drain limit', source)
        self.assertIn('unexpected status', source)
        self.assertIn('--request-timeout="$OC_REQUEST_TIMEOUT"', source)
        self.assertIn('--connect-timeout "$CURL_CONNECT_TIMEOUT_SECONDS"', source)
        self.assertIn('run_with_timeout "$TOPOLOGY_PREFLIGHT_TIMEOUT_SECONDS"', source)
        self.assertIn('and .items[0].status.qosClass == "Guaranteed"', source)

    def test_checkpoint_observers_are_reaped_by_a_pre_t0_fail_fast_gate(self) -> None:
        source = ORCHESTRATOR.read_text(encoding="utf-8")
        launch = source.index(
            '( capture_checkpoint "$checkpoint_name" "$checkpoint_epoch" "$checkpoint_deadline" ) &'
        )
        startup_gate = source.index(
            'check_background_observers "pre-T0 startup gate"'
        )
        job_poll = source.index('jobs_json=$("${k[@]}" get jobs', startup_gate)
        self.assertLess(launch, startup_gate)
        self.assertLess(startup_gate, job_poll)
        self.assertIn(
            'check_background_observers "concurrent observer gate"', source
        )
        self.assertIn('${context}: checkpoint observer ${name} failed', source)
        self.assertIn(
            'observer exited unexpectedly with status ${exit_status}', source
        )
        self.assertIn(
            '--ignore-not-found --cascade=foreground --wait=false', source
        )
        self.assertNotIn(
            'local scheduled_s=$((scheduled_ms / 1000)) delay=', source
        )

    def test_cleanup_faults_are_fail_closed_and_label_scoped(self) -> None:
        source = ORCHESTRATOR.read_text(encoding="utf-8")
        self.assertIn('cleanup_error="failed to delete recovery driver Jobs/Pods"', source)
        self.assertIn('status=cleanup_failed', source)
        self.assertIn('if [[ -n "$cleanup_error" && $exit_status -eq 0 ]]', source)
        self.assertIn('-l "benchmark.llm-d/run-id=${RECOVERY_RUN_ID}"', source)


class DriverAndDecisionTests(unittest.TestCase):
    def test_exact_driver_accounting_accepts_clean_pre_phase(self) -> None:
        job = plan()["jobs"][0]
        result = SUMMARY.validate_driver_report(job, driver_report(job), provenance())
        self.assertEqual(result["offered_success_ratio"], 1.0)
        self.assertEqual(result["dispatch_p99_us"], 1_000)

    def test_driver_originated_drop_fails_closed(self) -> None:
        job = plan()["jobs"][0]
        report = driver_report(job, ok_within=6_299)
        with self.assertRaisesRegex(SUMMARY.ValidationError, "driver-originated drops"):
            SUMMARY.validate_driver_report(job, report, provenance())

    def test_missing_accounting_field_fails_closed(self) -> None:
        job = plan()["jobs"][0]
        report = driver_report(job)
        del report["accounting"]["completed_after_plateau"]
        with self.assertRaisesRegex(SUMMARY.ValidationError, "completed_after_plateau"):
            SUMMARY.validate_driver_report(job, report, provenance())

    def results_with_first_good(self, first_offset: int) -> list[dict]:
        offsets = [0, 1, 2, 3, 5, 8, 13, 21, 34, 55, 89]
        results = [phase_result("pre"), phase_result("overload")]
        results.extend(
            phase_result("recovery_probe", offset=offset, good=offset >= first_offset)
            for offset in offsets
        )
        results.append(phase_result("post"))
        return results

    def test_green_recovery_requires_stable_sparse_tail(self) -> None:
        decision = SUMMARY.evaluate_cycle(
            self.results_with_first_good(21), 11.0, SERVICE_THRESHOLDS
        )
        self.assertEqual(decision["status"], "green")
        self.assertEqual(
            decision["recovery"]["first_probe_with_three_consecutive_and_all_later_good_seconds"],
            21,
        )

    def test_exact_34s_boundary_is_green(self) -> None:
        decision = SUMMARY.evaluate_cycle(
            self.results_with_first_good(34), 11.0, SERVICE_THRESHOLDS
        )
        self.assertEqual(decision["status"], "green")
        self.assertEqual(decision["recovery"]["recovery_time_seconds"], 34)

    def test_amber_55s_uses_strictly_clean_post_continuation(self) -> None:
        decision = SUMMARY.evaluate_cycle(
            self.results_with_first_good(55), 11.0, SERVICE_THRESHOLDS
        )
        self.assertEqual(decision["status"], "amber")
        self.assertTrue(decision["recovery"]["post_continuation_good"])

    def test_failed_post_continuation_prevents_55s_confirmation(self) -> None:
        results = self.results_with_first_good(55)
        post = results[-1]
        post["ok_total"] -= 1
        post["ok_rtt_us_total"] = post["ok_rtt_us_total"][:-1]
        post["statuses_total"] = {"OK": post["ok_total"], "GRPC_RESOURCEEXHAUSTED": 1}
        decision = SUMMARY.evaluate_cycle(results, 11.0, SERVICE_THRESHOLDS)
        self.assertEqual(decision["status"], "red")
        self.assertIsNone(decision["recovery"]["recovery_time_seconds"])
        self.assertFalse(decision["recovery"]["post_continuation_good"])

    def test_missing_89s_recovery_is_red(self) -> None:
        decision = SUMMARY.evaluate_cycle(
            self.results_with_first_good(100), 11.0, SERVICE_THRESHOLDS
        )
        self.assertEqual(decision["status"], "red")
        self.assertIsNone(
            decision["recovery"]["first_probe_with_three_consecutive_and_all_later_good_seconds"]
        )

    def test_first_passing_sparse_probe_at_89s_is_red(self) -> None:
        decision = SUMMARY.evaluate_cycle(
            self.results_with_first_good(89), 11.0, SERVICE_THRESHOLDS
        )
        self.assertEqual(decision["status"], "red")
        self.assertEqual(
            decision["recovery"]["first_probe_with_three_consecutive_and_all_later_good_seconds"],
            89,
        )

    def test_later_sparse_failure_invalidates_an_earlier_candidate(self) -> None:
        results = self.results_with_first_good(21)
        probe_55 = next(
            result
            for result in results
            if result.get("recovery_offset_seconds") == 55
        )
        probe_55.update(phase_result("recovery_probe", offset=55, good=False))
        decision = SUMMARY.evaluate_cycle(results, 11.0, SERVICE_THRESHOLDS)
        self.assertEqual(decision["status"], "red")
        self.assertEqual(
            decision["recovery"]["first_probe_with_three_consecutive_and_all_later_good_seconds"],
            89,
        )

    def test_post_stream_cannot_manufacture_failed_or_missing_sparse_probe(self) -> None:
        results = self.results_with_first_good(55)
        probe_55 = next(
            result
            for result in results
            if result.get("recovery_offset_seconds") == 55
        )
        probe_55.update(phase_result("recovery_probe", offset=55, good=False))
        decision = SUMMARY.evaluate_cycle(results, 11.0, SERVICE_THRESHOLDS)
        self.assertNotEqual(
            decision["recovery"]["first_probe_with_three_consecutive_and_all_later_good_seconds"],
            55,
        )
        missing = [
            result
            for result in self.results_with_first_good(55)
            if result.get("recovery_offset_seconds") != 55
        ]
        with self.assertRaisesRegex(SUMMARY.ValidationError, "exactly 14"):
            SUMMARY.evaluate_cycle(missing, 11.0, SERVICE_THRESHOLDS)

    def test_overload_without_queue_growth_is_red(self) -> None:
        decision = SUMMARY.evaluate_cycle(
            self.results_with_first_good(21), 9.9, SERVICE_THRESHOLDS
        )
        self.assertEqual(decision["status"], "red")
        self.assertFalse(decision["checks"]["overload_queue_above_10x"])


class TargetShapeAndAttributionTests(unittest.TestCase):
    def test_application_level_armed_barrier_requires_all_14_exact_records(self) -> None:
        document = plan()
        armed = armed_document(document)
        hashes = SUMMARY.validate_armed_barrier(document, provenance(), armed)
        self.assertEqual(len(hashes), 14)
        missing = json.loads(json.dumps(armed))
        missing["records"].pop()
        with self.assertRaisesRegex(SUMMARY.ValidationError, "ARMED barrier evidence is incomplete"):
            SUMMARY.validate_armed_barrier(document, provenance(), missing)

    def test_armed_nonce_mismatch_fails_closed(self) -> None:
        document = plan()
        armed = armed_document(document)
        armed["records"][4]["nonce"] = "0" * 64
        with self.assertRaisesRegex(SUMMARY.ValidationError, "run/nonce binding"):
            SUMMARY.validate_armed_barrier(document, provenance(), armed)

    def test_explicit_armed_config_mismatch_fails_before_authorization(self) -> None:
        document = plan()
        armed = armed_document(document)
        armed["records"][1]["config"]["max_in_flight"] = 999
        with self.assertRaisesRegex(SUMMARY.ValidationError, "explicit ARMED config differs"):
            SUMMARY.validate_armed_barrier(document, provenance(), armed)

    def test_final_report_must_link_to_armed_schedule(self) -> None:
        document = plan()
        armed = armed_document(document)
        reports = [driver_report(job) for job in document["jobs"]]
        linked = SUMMARY.validate_armed_final_linkage(
            document, provenance(), armed, reports
        )
        self.assertEqual(linked["linked_jobs"], 14)
        reports[8]["start_epoch_ms"] += 1
        with self.assertRaisesRegex(SUMMARY.ValidationError, "final start differs"):
            SUMMARY.validate_armed_final_linkage(
                document, provenance(), armed, reports
            )

    def test_exact_w1_rt1_guaranteed_runtime_shape_is_accepted(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            run_dir = Path(temp)
            write_shape_artifacts(run_dir)
            result = SUMMARY.validate_target_shape(run_dir, provenance())
            self.assertEqual(result["cpu_max"], "max 100000")
            self.assertEqual(result["shape"], "W1/RT1/Candle-unset Guaranteed 2CPU/4Gi")

    def test_worker_rayon_and_candle_environment_fail_closed(self) -> None:
        cases = (
            ({"workers": "4"}, "LLM_D_SC_INFERENCE_WORKERS"),
            ({"rayon": "2"}, "RAYON_NUM_THREADS"),
            ({"candle": "1"}, "CANDLE_NUM_THREADS"),
        )
        for overrides, message in cases:
            with self.subTest(overrides=overrides), tempfile.TemporaryDirectory() as temp:
                run_dir = Path(temp)
                write_shape_artifacts(run_dir, **overrides)
                with self.assertRaisesRegex(SUMMARY.ValidationError, message):
                    SUMMARY.validate_target_shape(run_dir, provenance())

    def test_envfrom_command_wrappers_and_runtime_candle_fail_closed(self) -> None:
        cases = (
            ({"env_from": [{"configMapRef": {"name": "hidden-env"}}]}, "envFrom"),
            ({"command": ["/bin/sh"], "args": ["-c", "exec /usr/local/bin/llm-d-sc"]}, "command/args"),
            ({"runtime_candle": "4"}, "actual PID1"),
            ({"pid1_executable": "/bin/sh"}, "actual PID1"),
        )
        for overrides, message in cases:
            with self.subTest(overrides=overrides), tempfile.TemporaryDirectory() as temp:
                run_dir = Path(temp)
                write_shape_artifacts(run_dir, **overrides)
                with self.assertRaisesRegex(SUMMARY.ValidationError, message):
                    SUMMARY.validate_target_shape(run_dir, provenance())

    def test_resources_and_qos_fail_closed(self) -> None:
        cases = (
            ({"cpu_limit": "3"}, "resources"),
            ({"memory_request": "3Gi"}, "resources"),
            ({"qos": "Burstable"}, "QoS"),
        )
        for overrides, message in cases:
            with self.subTest(overrides=overrides), tempfile.TemporaryDirectory() as temp:
                run_dir = Path(temp)
                write_shape_artifacts(run_dir, **overrides)
                with self.assertRaisesRegex(SUMMARY.ValidationError, message):
                    SUMMARY.validate_target_shape(run_dir, provenance())

    def test_cpu_max_quota_and_cpuset_fail_closed(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            run_dir = Path(temp)
            write_shape_artifacts(run_dir, cpu_max="200000 100000")
            changed = provenance()
            changed["target"]["cpu_max"] = "200000 100000"
            with self.assertRaisesRegex(SUMMARY.ValidationError, "cpu.max"):
                SUMMARY.validate_target_shape(run_dir, changed)
        with tempfile.TemporaryDirectory() as temp:
            run_dir = Path(temp)
            write_shape_artifacts(run_dir, cpuset="5")
            changed = provenance()
            changed["target"]["cpuset_cpus_effective"] = "5"
            with self.assertRaisesRegex(SUMMARY.ValidationError, "two logical CPUs"):
                SUMMARY.validate_target_shape(run_dir, changed)

    def test_checkpoint_cpu_max_change_fails_closed(self) -> None:
        document = plan()
        live_provenance = provenance()
        expected_target = {
            "name": "target-1",
            "uid": "uid-1",
            "ip": "10.0.0.1",
            "node": "target-node",
            "ready": True,
            "restart_count": 0,
            "image_id": "registry.invalid/target@" + TARGET_IMAGE,
        }
        pod = {
            "metadata": {"name": "target-1", "uid": "uid-1"},
            "spec": {"nodeName": "target-node"},
            "status": {
                "podIP": "10.0.0.1",
                "qosClass": "Guaranteed",
                "conditions": [{"type": "Ready", "status": "True"}],
                "containerStatuses": [
                    {
                        "name": "llm-d-sc",
                        "restartCount": 0,
                        "imageID": "registry.invalid/target@" + TARGET_IMAGE,
                    }
                ],
            },
        }
        with tempfile.TemporaryDirectory() as temp:
            run_dir = Path(temp)
            for artifact in ("targets-before.json", "targets-after.json"):
                (run_dir / artifact).write_text(
                    json.dumps({"items": [pod]}), encoding="utf-8"
                )
            checkpoint_dir = run_dir / "checkpoints"
            checkpoint_dir.mkdir()
            for checkpoint in document["checkpoints"]:
                cpu_max = (
                    "200000 100000"
                    if checkpoint["name"] == "recovery-30"
                    else "max 100000"
                )
                (checkpoint_dir / f"{checkpoint['name']}.json").write_text(
                    json.dumps(
                        {
                            "schema_version": 1,
                            "name": checkpoint["name"],
                            "scheduled_epoch_ms": checkpoint["scheduled_epoch_ms"],
                            "observed_epoch_ms": checkpoint["scheduled_epoch_ms"],
                            "target": expected_target,
                            "cpuset_cpus_effective": "5,149",
                            "cpu_max": cpu_max,
                        }
                    ),
                    encoding="utf-8",
                )
            target_bound = document["checkpoints"][0]
            (checkpoint_dir / "target-bound-gate.json").write_text(
                json.dumps(
                    {
                        "schema_version": 1,
                        "name": "target-bound",
                        "completion_epoch_ms": target_bound["scheduled_epoch_ms"]
                        + 1_000,
                        "completion_deadline_epoch_ms": target_bound[
                            "completion_deadline_epoch_ms"
                        ],
                        "load_authorized": True,
                    }
                ),
                encoding="utf-8",
            )
            start = document["t0_epoch_ms"] // 1_000 - 80
            post_end = document["jobs"][13]["start_epoch_ms"] // 1_000 + 180
            samples = [
                json.dumps(
                    {
                        "schema_version": 1,
                        "sample_epoch_s": epoch,
                        "target": expected_target,
                        "nodes_ready": True,
                    }
                )
                for epoch in range(start, post_end + 1, 10)
            ]
            (run_dir / "health-monitor.ndjson").write_text(
                "\n".join(samples) + "\n", encoding="utf-8"
            )
            with self.assertRaisesRegex(SUMMARY.ValidationError, "cpu.max changed"):
                SUMMARY.validate_identity(run_dir, document, live_provenance)

    def test_exact_driver_target_counter_reconciliation(self) -> None:
        owner = DriverAndDecisionTests()
        results = owner.results_with_first_good(0)
        ok = sum(result["statuses_total"].get("OK", 0) for result in results)
        queue = {
            "counter_deltas": {"served": ok, "hits": 0, "misses": ok}
        }
        result = SUMMARY.validate_counter_reconciliation(queue, results, provenance())
        self.assertEqual(result["driver_initiated"], result["driver_ok"] + 40)
        self.assertTrue(result["equations"]["target_served_equals_target_misses_equals_driver_ok"])

    def test_one_unattributed_target_request_invalidates_reconciliation(self) -> None:
        owner = DriverAndDecisionTests()
        results = owner.results_with_first_good(0)
        ok = sum(result["statuses_total"].get("OK", 0) for result in results)
        queue = {
            "counter_deltas": {"served": ok + 1, "hits": 0, "misses": ok + 1}
        }
        with self.assertRaisesRegex(SUMMARY.ValidationError, "do not exactly equal"):
            SUMMARY.validate_counter_reconciliation(queue, results, provenance())


class TelemetryFailClosedTests(unittest.TestCase):
    def test_timestamped_queue_logs_prove_more_than_ten_x_growth(self) -> None:
        document = plan()
        pre_start = document["jobs"][0]["start_epoch_ms"] // 1_000
        overload_start = document["jobs"][1]["start_epoch_ms"] // 1_000

        def timestamp(epoch: int) -> str:
            from datetime import datetime, timezone

            return datetime.fromtimestamp(epoch, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000000Z")

        with tempfile.TemporaryDirectory() as temp:
            run_dir = Path(temp)
            live_provenance = write_counter_baseline(run_dir, document, "server ready\n")
            (run_dir / "target-logs-full.txt").write_text(
                f"{timestamp(pre_start + 170)} llm-d-sc metrics: served=6000 hits=0 misses=6000 | queue p50=4µs p99=8ms | tokenize p50=80µs p99=88µs\n"
                f"{timestamp(overload_start + 110)} llm-d-sc metrics: served=11000 hits=0 misses=11000 | queue p50=1s p99=2s | tokenize p50=80µs p99=88µs\n",
                encoding="utf-8",
            )
            result = SUMMARY.validate_queue_logs(run_dir, document, live_provenance)
            self.assertEqual(result["ratio"], 250.0)

    def test_prior_target_traffic_rejects_cumulative_queue_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            run_dir = Path(temp)
            before = "2026-01-01T00:00:00Z llm-d-sc metrics: served=1 hits=0 misses=1\n"
            live_provenance = write_counter_baseline(
                run_dir, plan(), before
            )
            (run_dir / "target-logs-full.txt").write_text("", encoding="utf-8")
            with self.assertRaisesRegex(SUMMARY.ValidationError, "prior classification traffic"):
                SUMMARY.validate_queue_logs(run_dir, plan(), live_provenance)

    def test_missing_required_series_is_rejected(self) -> None:
        document = plan()
        provenance_document = {
            "target": {"name": "target-1", "node": "target-node"},
            "driver_node": "driver-node",
        }
        with tempfile.TemporaryDirectory() as temp:
            run_dir = Path(temp)
            metrics = run_dir / "metrics"
            metrics.mkdir()
            post = document["jobs"][13]
            (run_dir / "telemetry-window.json").write_text(
                json.dumps(
                    {
                        "schema_version": 1,
                        "start_epoch_s": document["t0_epoch_ms"] // 1_000 - 30,
                        "end_epoch_s": post["start_epoch_ms"] // 1_000 + 210,
                        "step_seconds": 5,
                        "max_gap_seconds": 10,
                    }
                ),
                encoding="utf-8",
            )
            for name in (
                "pod_cpu_otel",
                "container_cpu_otel",
                "container_cpu_cadvisor",
                "memory_working_set",
                "restarts",
                "pod_ready",
                "node_ready",
                "throttle_ratio",
                "cpu_pressure_waiting",
            ):
                (metrics / f"{name}.json").write_text(
                    json.dumps({"status": "success", "data": {"result": []}}),
                    encoding="utf-8",
                )
            with self.assertRaisesRegex(SUMMARY.ValidationError, "telemetry series set"):
                SUMMARY.validate_telemetry(run_dir, document, provenance_document)


class FullAnalyzerIntegrationTests(unittest.TestCase):
    def test_complete_synthetic_evidence_bundle_is_green(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            run_dir = Path(temp)
            write_full_valid_run(run_dir)
            result = SUMMARY.analyze_run(run_dir)
            self.assertTrue(result["validity"]["valid"])
            self.assertEqual(result["decision"]["status"], "green")
            self.assertEqual(result["precreated_jobs"]["application_level_armed_before_t0_minus_180s"], 14)
            self.assertTrue(
                result["counter_reconciliation"]["equations"][
                    "target_served_equals_target_misses_equals_driver_ok"
                ]
            )

    def test_analyzer_rejects_plan_created_less_than_360_seconds_before_t0(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            run_dir = Path(temp)
            write_full_valid_run(run_dir)
            document = json.loads((run_dir / "recovery-plan.json").read_text())
            live_provenance = json.loads((run_dir / "run-provenance.json").read_text())
            short_lead_created = document["t0_epoch_ms"] - 359_000
            document["created_epoch_ms"] = short_lead_created
            live_provenance["plan_created_epoch_ms"] = short_lead_created
            (run_dir / "recovery-plan.json").write_text(
                json.dumps(document), encoding="utf-8"
            )
            (run_dir / "run-provenance.json").write_text(
                json.dumps(live_provenance), encoding="utf-8"
            )
            with self.assertRaisesRegex(
                SUMMARY.ValidationError, "at least 360 seconds"
            ):
                SUMMARY.analyze_run(run_dir)


if __name__ == "__main__":
    unittest.main()
