#!/usr/bin/env python3
"""Create the immutable, cluster-free plan and sequence ledger for an SC scale rung."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from pathlib import Path
from typing import Any


SCHEMA_VERSION = 1
SEQUENCE_NAMESPACE_START = 22_000_000_000
SEQUENCE_NAMESPACE_END = 23_000_000_000
CAMPAIGN_SPAN = 10_000_000
JOB_SEQUENCE_SPAN = 10_001
MAX_ENDPOINTS = 50
ALLOWED_RUNGS = (20, 30, 40, 50)
TOKEN_COUNT = 64
MAX_ROWS = 10_000
DRIVER_IMAGE = (
    "image-registry.openshift-image-registry.svc:5000/llm-d-sc-gremlins/"
    "llm-d-sc-benchmark-driver-armed-51541f00e5fa@"
    "sha256:ef0f32ad3a7a29f4cd1f68ae8b8cfbc1bf36d66a173df8f68fd531db9d762aae"
)
DRIVER_SOURCE_SHA256 = "51541f00e5fa6e1918b4e57b9bfa432337345b1854b7289c836c3752543929d9"
TARGET_IMAGE = "sha256:04323612ce3f73873b4c3ed6e09264e828241537e2c1a4231b43f32e9744d5aa"
MODEL_SHA256 = "7914abbd152278879b4c3235d188e3006753bb778b7de6266fbcbe4c4ba2ef2f"
TOKENIZER_SHA256 = "851ca67100d372ca3ae031a6abd168f53489eebfd7d89523f35c5c9b4d372c3c"
GENERATOR_SCHEME = "alpha_bravo_lsb_identity_service_fill_v1"


def _cell(
    ordinal: int,
    cell_id: str,
    scope: str,
    phase: str,
    endpoint_rates: list[int | None],
    *,
    block: int | None = None,
    period: str | None = None,
) -> dict[str, Any]:
    active = [index for index, rate in enumerate(endpoint_rates) if rate is not None]
    rates = [int(endpoint_rates[index]) for index in active]
    return {
        "ordinal": ordinal,
        "cell_id": cell_id,
        "scope": scope,
        "phase": phase,
        "block": block,
        "period": period,
        "duration_seconds": 180,
        "active_endpoints": active,
        "endpoint_offered_rps": endpoint_rates,
        "driver_jobs": len(active),
        "aggregate_offered_rps": sum(rates),
        "common_t0_assignment": "runtime; one future absolute epoch shared by every Job in this cell",
        "warmup_requests": 0,
    }


def build_cells(replicas: int, campaign_index: int = 0) -> list[dict[str, Any]]:
    """Return the frozen cell order.

    The ten primary scale-knee cells are pure-rate pairs: every endpoint is at
    41 in one period and every endpoint is at 42 in the other.  The order
    alternates AB/BA.  With an odd number of blocks, the fifth order is selected
    by campaign-index parity so adjacent independently reserved campaigns
    counterbalance one another.  Each campaign still contains exactly five
    cells at each rate.
    """
    if replicas not in ALLOWED_RUNGS:
        raise ValueError(f"replicas must be one of {ALLOWED_RUNGS}")
    if replicas % 2:
        raise ValueError("replicas must be even for exact endpoint crossover balance")

    cells: list[dict[str, Any]] = []
    only_zero: list[int | None] = [None] * replicas
    only_zero[0] = 41
    cells.append(_cell(0, "r1-pre-41", "r1_sentinel", "sentinel_pre", only_zero.copy()))
    only_zero[0] = 42
    cells.append(_cell(1, "r1-pre-42", "r1_sentinel", "sentinel_pre", only_zero.copy()))
    cells.append(_cell(2, "scale-pre-35", "rung", "pre", [35] * replicas))

    ordinal = 3
    orders = [(41, 42), (42, 41), (41, 42), (42, 41)]
    orders.append((41, 42) if campaign_index % 2 == 0 else (42, 41))
    for block, (rate_a, rate_b) in enumerate(orders, start=1):
        rates_a = [rate_a] * replicas
        rates_b = [rate_b] * replicas
        cells.append(
            _cell(
                ordinal,
                f"scale-b{block}-a",
                "rung",
                "knee",
                rates_a,
                block=block,
                period="A",
            )
        )
        ordinal += 1
        cells.append(
            _cell(
                ordinal,
                f"scale-b{block}-b",
                "rung",
                "knee",
                rates_b,
                block=block,
                period="B",
            )
        )
        ordinal += 1

    cells.append(_cell(13, "scale-post-35", "rung", "post", [35] * replicas))
    only_zero[0] = 42
    cells.append(_cell(14, "r1-post-42", "r1_sentinel", "sentinel_post", only_zero.copy()))
    only_zero[0] = 41
    cells.append(_cell(15, "r1-post-41", "r1_sentinel", "sentinel_post", only_zero.copy()))
    return cells


def build_plan(
    run_id: str,
    campaign_index: int,
    replicas: int,
    *,
    namespace: str = "llm-d-sc-scaleout",
    target_node: str = "gnr2.fm2aihpcsed.com",
    driver_node: str = "rhgnr1",
) -> tuple[dict[str, Any], dict[str, Any]]:
    if not re.fullmatch(r"[a-z0-9](?:[-a-z0-9]*[a-z0-9])?", run_id):
        raise ValueError("run_id must be a lower-case DNS label")
    if len(run_id) > 38:
        raise ValueError("run_id must be at most 38 characters")
    if not 0 <= campaign_index < (SEQUENCE_NAMESPACE_END - SEQUENCE_NAMESPACE_START) // CAMPAIGN_SPAN:
        raise ValueError("campaign_index is outside the audited [22B,23B) namespace")

    cells = build_cells(replicas, campaign_index)
    reservation_start = SEQUENCE_NAMESPACE_START + campaign_index * CAMPAIGN_SPAN
    reservation_end = reservation_start + CAMPAIGN_SPAN
    allocation = 0
    entries: list[dict[str, Any]] = []
    jobs: list[dict[str, Any]] = []

    for cell in cells:
        cell_jobs: list[str] = []
        for endpoint in cell["active_endpoints"]:
            rate = cell["endpoint_offered_rps"][endpoint]
            sequence_base = reservation_start + allocation * JOB_SEQUENCE_SPAN
            reserved_end = sequence_base + JOB_SEQUENCE_SPAN
            if reserved_end > reservation_end:
                raise ValueError("campaign allocation exceeds its 10,000,000-sequence reservation")
            job_id = f"sso-{run_id}-c{cell['ordinal']:02d}-e{endpoint:02d}"
            nonce_material = (
                f"{run_id}|{campaign_index}|{cell['ordinal']}|{endpoint}|{rate}|"
                f"{sequence_base}|{DRIVER_IMAGE}"
            )
            nonce = hashlib.sha256(nonce_material.encode()).hexdigest()
            job = {
                "allocation_ordinal": allocation,
                "job_id": job_id,
                "cell_ordinal": cell["ordinal"],
                "cell_id": cell["cell_id"],
                "endpoint_ordinal": endpoint,
                "offered_rps": rate,
                "duration_seconds": cell["duration_seconds"],
                "expected_slots": rate * cell["duration_seconds"],
                "sequence_base": sequence_base,
                "reserved_end_exclusive": reserved_end,
                "candidate_rows": MAX_ROWS,
                "max_in_flight": 512,
                "arming_nonce": nonce,
            }
            jobs.append(job)
            cell_jobs.append(job_id)
            entries.append(
                {
                    "allocation_ordinal": allocation,
                    "job_id": job_id,
                    "cell_id": cell["cell_id"],
                    "endpoint_ordinal": endpoint,
                    "planned": {
                        "sequence_base": sequence_base,
                        "candidate_rows": MAX_ROWS,
                        "reserved_interval": {
                            "start_inclusive": sequence_base,
                            "end_exclusive": reserved_end,
                        },
                        "expected_first_sequence": sequence_base,
                        "expected_last_sequence": sequence_base + MAX_ROWS,
                    },
                    "armed": {
                        "observed": False,
                        "scheduled_rows_blake3": None,
                        "selected_rows_blake3": None,
                        "config_digest": None,
                        "armed_epoch_ms": None,
                    },
                    "emitted": {
                        "observed": False,
                        "actual_first_sequence": None,
                        "actual_last_sequence": None,
                        "scheduled_rows_blake3": None,
                        "completed_requests": None,
                    },
                    "target_binding": {
                        "endpoint_ordinal": endpoint,
                        "pod_name": None,
                        "pod_uid": None,
                        "pod_ip": None,
                    },
                    "fresh_cache_counters": {
                        "required": True,
                        "baseline_expected": {"served": 0, "hits": 0, "misses": 0},
                        "baseline_observed": None,
                        "final_observed": None,
                    },
                    "lifecycle_status": "planned",
                }
            )
            allocation += 1
        cell["job_ids"] = cell_jobs

    plan: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "protocol": "unchanged_sc_horizontal_scale_knee_v1",
        "run_id": run_id,
        "campaign_index": campaign_index,
        "namespace": namespace,
        "rung_replicas": replicas,
        "first_executable_rung": 20,
        "allowed_rungs": list(ALLOWED_RUNGS),
        "workload_isolation": {
            "temporary_deployment": f"sso-{run_id}-target",
            "run_label": f"benchmark.llm-d/run-id={run_id}",
            "shared_exclusion_lock": "sc-benchmark-matrix-lock",
            "namespace_scoped": True,
            "independently_deletable": True,
            "requires_reference_target_scaled_to_zero": True,
        },
        "target_shape": {
            "image": TARGET_IMAGE,
            "node": target_node,
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
                "cpu_max": "max",
                "logical_cpus": 2,
                "complete_smt_sibling_sets": True,
                "pid1_executable": "/usr/local/bin/llm-d-sc",
            },
        },
        "driver_shape": {
            "image": DRIVER_IMAGE,
            "source_sha256": DRIVER_SOURCE_SHA256,
            "node": driver_node,
            "one_job_per_direct_pod_ip": True,
            "connections_per_job": 1,
            "closed_loop_concurrency_argument": 1,
            "warmup_requests": 0,
            "start_lead_seconds": 180,
            "armed_barrier_lead_seconds": 90,
            "resources": {
                "requests": {"cpu": "500m", "memory": "256Mi"},
                "limits": {"cpu": "4", "memory": "1Gi"},
            },
        },
        "scale_protocol": {
            "initial_replicas": 0,
            "batch_increment": 2,
            "stability_seconds_per_batch": 120,
            "cells_serial": True,
            "no_oc_exec_during_plateau": True,
            "target_inventory_frozen_before_first_load": True,
        },
        "identity": {
            "target_image": TARGET_IMAGE,
            "driver_image": DRIVER_IMAGE,
            "driver_source_sha256": DRIVER_SOURCE_SHA256,
            "model_sha256": MODEL_SHA256,
            "tokenizer_sha256": TOKENIZER_SHA256,
            "token_count_including_specials": TOKEN_COUNT,
            "generator_scheme": GENERATOR_SCHEME,
            "corpus_mode": "generated",
            "corpus_digest": None,
        },
        "cells": cells,
        "jobs": jobs,
        "sequence_reservation": {
            "audited_namespace": {
                "start_inclusive": SEQUENCE_NAMESPACE_START,
                "end_exclusive": SEQUENCE_NAMESPACE_END,
            },
            "campaign_span": CAMPAIGN_SPAN,
            "start_inclusive": reservation_start,
            "end_exclusive": reservation_end,
            "allocated_jobs": allocation,
            "allocated_end_exclusive": reservation_start + allocation * JOB_SEQUENCE_SPAN,
            "never_recycle_if_ledger_exists": True,
        },
        "resource_envelope_incremental": {
            "peak_pods": 2 * replicas,
            "requests": {"cpu_millicores": 2500 * replicas, "memory_mib": 4352 * replicas},
            "limits": {"cpu_millicores": 6000 * replicas, "memory_mib": 5120 * replicas},
            "target_node_requests": {"cpu_millicores": 2000 * replicas, "memory_mib": 4096 * replicas},
            "driver_node_requests": {"cpu_millicores": 500 * replicas, "memory_mib": 256 * replicas},
            "note": "live preflight adds these increments to actual quota usage and node allocations",
        },
        "validity_gates": {
            "driver": {
                "accounting_exact": True,
                "schedule_drops_max": 0,
                "in_flight_drops_max": 0,
                "dispatch_p99_lag_ms_max": 5,
                "drain_seconds_max": 90,
            },
            "transport": {
                "direct_pod_ip_only": True,
                "connections_per_endpoint": 1,
                "forbidden_statuses": [
                    "GRPC_UNAVAILABLE",
                    "GRPC_UNKNOWN",
                    "GRPC_INTERNAL",
                    "GRPC_DATA_LOSS",
                    "CONNECT_ERROR",
                ],
            },
            "health": {
                "pod_uid_ip_image_cpuset_stable": True,
                "pod_ready_continuous": True,
                "restart_count_max": 0,
                "nodes_ready_continuous": True,
            },
            "telemetry": {
                "required": [
                    "pod_cpu_otel",
                    "container_cpu_otel",
                    "container_cpu_cadvisor",
                    "memory_working_set",
                    "restarts",
                    "pod_ready",
                    "node_ready",
                ],
                "maximum_gap_seconds": 10,
                "supporting": ["throttle_ratio", "cpu_pressure_waiting"],
            },
            "fresh_cache": {
                "baseline_counters": {"served": 0, "hits": 0, "misses": 0},
                "final_hits": 0,
                "exact_driver_to_target_reconciliation": True,
            },
        },
        "analysis": {
            "experimental_unit": "paired block; endpoint summaries are retained and never treated as independent cluster replicates",
            "knee_interval": "(41,42] offered RPS per Pod",
            "clean_p99_ms_max": 35.363,
            "rate_41_success_min": 0.99,
            "rate_41_drain_max": 0.01,
            "paired_p99_ratio_min_exclusive": 1.25,
            "paired_marginal_useful_max_exclusive": 1.0,
            "pre_post_35_useful_relative_delta_max": 0.02,
            "pre_post_35_p99_ratio_max": 1.20,
            "r1_sentinel_required": True,
        },
    }

    ledger: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "ledger": "llm-d-sc-generated-sequence-allocation-v1",
        "allocation_owner": {
            "run_id": run_id,
            "campaign_index": campaign_index,
            "framework": "unchanged_sc_horizontal_scale_knee_v1",
        },
        "reservation_status": "claimed_by_plan",
        "reservation": plan["sequence_reservation"],
        "identity": plan["identity"],
        "entries": entries,
        "rule": "The existence of this ledger permanently consumes the full campaign reservation; aborted and plan-only allocations are never recycled.",
    }
    return plan, ledger


def _iter_existing_ledgers(root: Path) -> list[Path]:
    if not root.exists():
        return []
    return sorted(path for path in root.rglob("sequence-ledger.json") if path.is_file())


def assert_no_overlap(ledger: dict[str, Any], ledger_root: Path, own_path: Path) -> None:
    wanted = ledger["reservation"]
    start = int(wanted["start_inclusive"])
    end = int(wanted["end_exclusive"])
    conflicts: list[str] = []
    for path in _iter_existing_ledgers(ledger_root):
        try:
            if path.resolve() == own_path.resolve():
                continue
            existing = json.loads(path.read_text())
            reservation = existing["reservation"]
            other_start = int(reservation["start_inclusive"])
            other_end = int(reservation["end_exclusive"])
        except (OSError, ValueError, KeyError, TypeError, json.JSONDecodeError):
            # A malformed ledger is itself a safety blocker; it cannot prove
            # that its referenced range is disjoint.
            conflicts.append(f"{path}: unreadable reservation")
            continue
        if start < other_end and other_start < end:
            conflicts.append(f"{path}: [{other_start},{other_end})")
    if conflicts:
        raise ValueError("sequence reservation is not globally disjoint: " + "; ".join(conflicts))


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--campaign-index", required=True, type=int)
    parser.add_argument("--replicas", type=int, default=20, choices=ALLOWED_RUNGS)
    parser.add_argument("--namespace", default="llm-d-sc-scaleout")
    parser.add_argument("--target-node", default="gnr2.fm2aihpcsed.com")
    parser.add_argument("--driver-node", default="rhgnr1")
    parser.add_argument("--output-plan", type=Path, required=True)
    parser.add_argument("--output-ledger", type=Path, required=True)
    parser.add_argument("--ledger-root", type=Path, required=True)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(sys.argv[1:] if argv is None else argv)
    try:
        plan, ledger = build_plan(
            args.run_id,
            args.campaign_index,
            args.replicas,
            namespace=args.namespace,
            target_node=args.target_node,
            driver_node=args.driver_node,
        )
        assert_no_overlap(ledger, args.ledger_root, args.output_ledger)
        args.output_plan.parent.mkdir(parents=True, exist_ok=True)
        args.output_plan.write_text(json.dumps(plan, indent=2, sort_keys=True) + "\n")
        # The ledger is deliberately written last: its existence is the
        # irreversible reservation marker.
        args.output_ledger.write_text(json.dumps(ledger, indent=2, sort_keys=True) + "\n")
    except (OSError, ValueError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
