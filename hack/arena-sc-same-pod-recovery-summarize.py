#!/usr/bin/env python3
"""Validate and summarize one unchanged-image, same-Pod recovery cycle.

The analyzer deliberately separates evidence validity from the observed service
result.  Missing, malformed, mis-bound, or incomplete evidence raises
``ValidationError``.  A valid run can still have a green, amber, or red recovery
decision.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable


ARMED_DRIVER_IMAGE = (
    "image-registry.openshift-image-registry.svc:5000/llm-d-sc-gremlins/"
    "llm-d-sc-benchmark-driver-armed-51541f00e5fa@"
    "sha256:ef0f32ad3a7a29f4cd1f68ae8b8cfbc1bf36d66a173df8f68fd531db9d762aae"
)
ARMED_DRIVER_SOURCE_SHA256 = (
    "51541f00e5fa6e1918b4e57b9bfa432337345b1854b7289c836c3752543929d9"
)


class ValidationError(RuntimeError):
    """The evidence cannot support a recovery conclusion."""


def require(condition: bool, message: str) -> None:
    if not condition:
        raise ValidationError(message)


def read_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as error:
        raise ValidationError(f"missing required artifact: {path.name}") from error
    except (OSError, json.JSONDecodeError) as error:
        raise ValidationError(f"unreadable JSON artifact: {path.name}: {error}") from error


def read_ndjson(path: Path) -> list[dict[str, Any]]:
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except FileNotFoundError as error:
        raise ValidationError(f"missing required artifact: {path.name}") from error
    documents: list[dict[str, Any]] = []
    for line_number, line in enumerate(lines, 1):
        if not line.strip():
            continue
        try:
            document = json.loads(line)
        except json.JSONDecodeError as error:
            raise ValidationError(
                f"invalid NDJSON in {path.name} line {line_number}: {error}"
            ) from error
        require(isinstance(document, dict), f"{path.name} line {line_number} is not an object")
        documents.append(document)
    require(documents, f"{path.name} contains no samples")
    return documents


def integer(value: Any, field: str) -> int:
    require(isinstance(value, int) and not isinstance(value, bool), f"{field} must be an integer")
    return value


def number(value: Any, field: str) -> float:
    require(isinstance(value, (int, float)) and not isinstance(value, bool), f"{field} must be numeric")
    result = float(value)
    require(math.isfinite(result), f"{field} must be finite")
    return result


def sha256(path: Path) -> str:
    try:
        return hashlib.sha256(path.read_bytes()).hexdigest()
    except OSError as error:
        raise ValidationError(f"cannot hash {path.name}: {error}") from error


def nearest_rank(values: Iterable[int], quantile: float) -> int | None:
    ordered = sorted(values)
    if not ordered:
        return None
    rank = max(1, math.ceil(len(ordered) * quantile))
    return ordered[rank - 1]


def status_counts(value: Any, field: str) -> dict[str, int]:
    require(isinstance(value, dict), f"{field} must be an object")
    result: dict[str, int] = {}
    for key, raw_count in value.items():
        require(isinstance(key, str) and key, f"{field} has an invalid status name")
        count = integer(raw_count, f"{field}.{key}")
        require(count >= 0, f"{field}.{key} must be non-negative")
        result[key] = count
    return result


def add_counts(*counts: dict[str, int]) -> dict[str, int]:
    result: dict[str, int] = {}
    for document in counts:
        for key, value in document.items():
            result[key] = result.get(key, 0) + value
    return result


def validate_plan(plan: dict[str, Any]) -> None:
    require(plan.get("schema_version") == 1, "unsupported recovery plan schema")
    require(plan.get("protocol") == "same_pod_open_loop_recovery_v1", "unexpected recovery protocol")
    cycle_index = integer(plan.get("cycle_index"), "plan.cycle_index")
    require(cycle_index >= 0, "plan.cycle_index must be non-negative")
    sequence = plan.get("sequence_reservation")
    require(isinstance(sequence, dict), "plan.sequence_reservation is missing")
    expected_base = 19_000_000_000 + 150_000 * cycle_index
    require(sequence.get("cycle_base") == expected_base, "cycle sequence base violates C_r formula")
    require(sequence.get("job_span") == 10_001, "job sequence span must be 10,001")

    jobs = plan.get("jobs")
    require(isinstance(jobs, list) and len(jobs) == 14, "plan must contain exactly 14 Jobs")
    require([job.get("ordinal") for job in jobs] == list(range(14)), "Job ordinals must be j0..j13")
    require([job.get("sequence_base") for job in jobs] == [expected_base + 10_001 * index for index in range(14)], "Job sequence reservations are not disjoint")
    require(all(job.get("candidate_rows") == 10_000 for job in jobs), "every Job must reserve 10,000 candidate rows")
    require(all(job.get("warmup_requests") == 0 for job in jobs), "all Jobs must have zero warm-up arrivals")
    require(
        all(
            isinstance(job.get("arming_nonce"), str)
            and re.fullmatch(r"[0-9a-f]{64}", job["arming_nonce"]) is not None
            for job in jobs
        )
        and len({job["arming_nonce"] for job in jobs}) == 14,
        "every Job must have one unique SHA-256 ARMED nonce",
    )

    expected_shape = {
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
    }
    require(plan.get("target_shape") == expected_shape, "plan target shape is not frozen W1/RT1/Candle-unset Guaranteed 2CPU/4Gi")
    arming = plan.get("arming")
    require(isinstance(arming, dict) and arming.get("required") is True, "application-level ARMED barrier is not required")
    require(arming.get("protocol") == "sustained-corpus-probe-armed-v1", "unexpected driver ARMED protocol")
    allowlist = {
        "driver_image": ARMED_DRIVER_IMAGE,
        "driver_source_sha256": ARMED_DRIVER_SOURCE_SHA256,
    }
    require(arming.get("allowlist") == allowlist, "driver ARMED allowlist differs from the smoke-validated immutable pair")
    pinned = plan.get("pinned")
    pair_matches_allowlist = (
        isinstance(pinned, dict)
        and pinned.get("driver_image") == ARMED_DRIVER_IMAGE
        and pinned.get("driver_source_sha256") == ARMED_DRIVER_SOURCE_SHA256
    )
    require(
        arming.get("pair_matches_allowlist") is pair_matches_allowlist
        and arming.get("pinned_driver_supports_protocol") is pair_matches_allowlist
        and arming.get("live_executable") is pair_matches_allowlist,
        "driver ARMED support must be derived from exact image/source allowlist equality",
    )
    expected_blocker = (
        None
        if pair_matches_allowlist
        else "driver image/source pair is not the exact smoke-validated ARMED allowlist"
    )
    require(arming.get("blocker") == expected_blocker, "driver ARMED blocker does not match allowlist eligibility")
    contract = arming.get("validation_contract")
    require(
        isinstance(contract, dict)
        and contract.get("records") == 14
        and contract.get("deadline") == "T0-180s"
        and contract.get("all_jobs_required") is True
        and contract.get("schema") == "llm-d-sc.benchmark-driver.armed"
        and contract.get("schema_version") == 1
        and contract.get("record_type") == "ARMED"
        and contract.get("explicit_config_required") is True
        and contract.get("all_config_fields_must_match_frozen_job") is True
        and contract.get("digest_role")
        == "recorded pinned-driver provenance; explicit config equality authorizes load",
        "driver ARMED validation contract was weakened",
    )

    t0 = integer(plan.get("t0_epoch_ms"), "plan.t0_epoch_ms")
    pre, overload, post = jobs[0], jobs[1], jobs[13]
    require(pre.get("phase") == "pre" and pre.get("offered_rps") == "35", "j0 must be pre35")
    require(pre.get("duration_seconds") == 180 and pre.get("expected_slots") == 6_300, "pre35 must be 180s/6,300 slots")
    require(pre.get("start_epoch_ms") == t0, "pre35 must start at T0")
    require(pre.get("max_in_flight") == 512, "pre35 max-in-flight must be 512")
    require(overload.get("phase") == "overload" and overload.get("offered_rps") == "47", "j1 must be overload47")
    require(overload.get("duration_seconds") == 120 and overload.get("expected_slots") == 5_640, "overload47 must be 120s/5,640 slots")
    require(overload.get("start_epoch_ms") == t0 + 185_000, "overload must follow a 5s no-arrival gap")
    require(overload.get("max_in_flight") == 512, "overload max-in-flight must be 512")

    expected_offsets = [0, 1, 2, 3, 5, 8, 13, 21, 34, 55, 89]
    overload_end = t0 + 305_000
    probes = jobs[2:13]
    require([probe.get("recovery_offset_seconds") for probe in probes] == expected_offsets, "recovery probe offsets differ from the frozen sparse schedule")
    for probe, offset in zip(probes, expected_offsets):
        require(probe.get("phase") == "recovery_probe", "j2..j12 must be recovery probes")
        require(probe.get("offered_rps") == "1", "recovery probes must offer one request per second")
        require(probe.get("duration_seconds") == 1 and probe.get("expected_slots") == 1, "each recovery probe must schedule exactly one request")
        require(probe.get("max_in_flight") == 1, "recovery probe max-in-flight must be one")
        require(probe.get("start_epoch_ms") == overload_end + offset * 1_000, "recovery probe timestamp mismatch")

    require(post.get("phase") == "post" and post.get("offered_rps") == "35", "j13 must be post35")
    require(post.get("duration_seconds") == 180 and post.get("expected_slots") == 6_300, "post35 must be 180s/6,300 slots")
    require(post.get("start_epoch_ms") == overload_end + 95_000, "post35 must start five seconds after the 90s recovery window")
    require(post.get("max_in_flight") == 512, "post35 max-in-flight must be 512")
    require(sequence.get("reserved_end_exclusive") == expected_base + 150_000, "cycle reservation must end at the next C_r boundary")
    require(jobs[-1]["sequence_base"] + 10_000 <= sequence["reserved_end_exclusive"], "Job sequence range escapes cycle reservation")
    checkpoints = plan.get("checkpoints")
    expected_checkpoints = [
        ("target-bound", t0 - 175_000, t0 - 155_000),
        ("pre-mid", t0 + 90_000, None),
        ("gap-mid", t0 + 182_000, None),
        ("overload-mid", t0 + 245_000, None),
        ("recovery-30", t0 + 335_000, None),
        ("recovery-50", t0 + 355_000, None),
        ("post-mid", t0 + 490_000, None),
        ("post-after", t0 + 582_000, None),
    ]
    require(
        isinstance(checkpoints, list)
        and [
            (
                item.get("name"),
                item.get("scheduled_epoch_ms"),
                item.get("completion_deadline_epoch_ms"),
            )
            for item in checkpoints
        ]
        == expected_checkpoints,
        "identity/topology checkpoint schedule differs from the frozen protocol",
    )
    require(
        checkpoints[0].get("load_authorizing") is True
        and all("load_authorizing" not in item for item in checkpoints[1:]),
        "only target-bound may be a load-authorizing checkpoint",
    )
    gates = plan.get("gates")
    require(
        isinstance(gates, dict)
        and gates.get("target_bound_schedule_lead_seconds") == 175
        and gates.get("target_bound_completion_lead_seconds") == 155
        and gates.get("pre_t0_cancellation_completion_lead_seconds") == 25
        and gates.get("pre_t0_foreground_delete_timeout_seconds_max") == 90
        and gates.get("pre_t0_zero_object_verification_budget_seconds") == 15
        and gates.get("pre_t0_cancellation_safety_margin_seconds") == 10,
        "target-bound pre-T0 deadline gate was weakened",
    )


def validate_driver_report(
    job: dict[str, Any], report: dict[str, Any], provenance: dict[str, Any]
) -> dict[str, Any]:
    prefix = f"j{job['ordinal']:02d}"
    target = provenance["target"]
    thresholds = provenance["scheduler_thresholds"]
    require(report.get("schema_version") == 2, f"{prefix}: driver schema must be 2")
    require(report.get("probe") == "sustained_exact_token_corpus", f"{prefix}: unexpected probe")
    require(report.get("load_model") == "open_loop_deterministic_offered_rate", f"{prefix}: not open loop")
    require(report.get("target") == f"{target['ip']}:50051", f"{prefix}: target IP binding changed")
    require(report.get("target_image") == provenance["target_image"], f"{prefix}: target image assertion mismatch")
    require(report.get("model_sha256") == provenance["model_sha256"], f"{prefix}: model assertion mismatch")
    require(report.get("tokenizer_sha256") == provenance["tokenizer_sha256"], f"{prefix}: tokenizer assertion mismatch")
    require(report.get("topology") == provenance["topology"], f"{prefix}: topology assertion mismatch")
    require(report.get("corpus_mode") == "generated", f"{prefix}: generated exact-token corpus required")
    require(report.get("generator_scheme") == "alpha_bravo_lsb_identity_service_fill_v1", f"{prefix}: generator scheme mismatch")
    require(report.get("token_count_including_specials") == 64, f"{prefix}: token count mismatch")
    require(report.get("connections") == 1, f"{prefix}: connections must equal one")
    require(report.get("closed_loop_concurrency_argument") == 1, f"{prefix}: concurrency argument must equal one")
    require(report.get("warmup_requests") == 0, f"{prefix}: warm-up traffic is forbidden")
    require(report.get("candidate_rows") == 10_000, f"{prefix}: candidate-row reservation mismatch")
    require(report.get("scheduled_plateau_rows") == job["expected_slots"], f"{prefix}: scheduled slots mismatch")
    require(report.get("first_sequence") == job["sequence_base"], f"{prefix}: first sequence mismatch")
    require(report.get("last_sequence") == job["sequence_base"] + 9_999, f"{prefix}: sequence span mismatch")
    require(report.get("start_epoch_ms") == job["start_epoch_ms"], f"{prefix}: start epoch mismatch")
    require(report.get("duration_seconds") == job["duration_seconds"], f"{prefix}: duration mismatch")
    require(report.get("corpus_exhausted") is False, f"{prefix}: corpus exhausted")
    for digest_field in ("selected_rows_blake3", "scheduled_rows_blake3"):
        digest = report.get(digest_field)
        require(isinstance(digest, str) and re.fullmatch(r"[0-9a-f]{64}", digest) is not None, f"{prefix}: invalid {digest_field}")

    open_loop = report.get("open_loop")
    require(isinstance(open_loop, dict), f"{prefix}: missing open-loop provenance")
    require(open_loop.get("protocol_version") == "deterministic_offered_rate_v1", f"{prefix}: driver protocol mismatch")
    require(open_loop.get("driver_image") == provenance["driver_image"], f"{prefix}: driver image mismatch")
    offered_rate = open_loop.get("offered_rate")
    require(isinstance(offered_rate, dict) and offered_rate.get("requested_decimal") == job["offered_rps"], f"{prefix}: offered-rate mismatch")
    require(open_loop.get("max_in_flight") == job["max_in_flight"], f"{prefix}: max-in-flight mismatch")
    require(open_loop.get("dispatch_late_after_ms") == thresholds["dispatch_late_after_ms"], f"{prefix}: dispatch threshold mismatch")
    require(open_loop.get("drop_late_after_ms") == thresholds["drop_late_after_ms"], f"{prefix}: drop threshold mismatch")
    require(open_loop.get("rpc_timeout_ms") == thresholds["rpc_timeout_ms"], f"{prefix}: RPC timeout mismatch")
    require(open_loop.get("raw_rtt_collection") == "always enabled in open-loop mode", f"{prefix}: raw RTT evidence missing")

    accounting = report.get("accounting")
    require(isinstance(accounting, dict), f"{prefix}: accounting is missing")
    offered = integer(accounting.get("offered_slots"), f"{prefix}.offered_slots")
    initiated = integer(accounting.get("initiated_requests"), f"{prefix}.initiated_requests")
    completed = integer(accounting.get("completed_requests"), f"{prefix}.completed_requests")
    within = integer(accounting.get("completed_within_plateau"), f"{prefix}.completed_within_plateau")
    drained = integer(accounting.get("completed_after_plateau"), f"{prefix}.completed_after_plateau")
    dropped_in_flight = integer(accounting.get("dropped_in_flight_limit"), f"{prefix}.dropped_in_flight_limit")
    dropped_schedule = integer(accounting.get("dropped_schedule_late"), f"{prefix}.dropped_schedule_late")
    require(offered == job["expected_slots"], f"{prefix}: offered-slot total mismatch")
    require(offered == initiated + dropped_in_flight + dropped_schedule, f"{prefix}: offered accounting does not close")
    require(initiated == completed, f"{prefix}: initiated/completed accounting does not close")
    require(completed == within + drained, f"{prefix}: plateau/drain accounting does not close")
    require(dropped_in_flight == 0 and dropped_schedule == 0, f"{prefix}: driver-originated drops invalidate target attribution")

    within_statuses = status_counts(report.get("statuses_completed_within_plateau"), f"{prefix}.statuses_within")
    drain_statuses = status_counts(report.get("drained_after_plateau"), f"{prefix}.statuses_after")
    total_statuses = status_counts(report.get("statuses_completed_total"), f"{prefix}.statuses_total")
    require(sum(within_statuses.values()) == within, f"{prefix}: within-plateau status counts disagree")
    require(sum(drain_statuses.values()) == drained, f"{prefix}: drain status counts disagree")
    require(add_counts(within_statuses, drain_statuses) == total_statuses, f"{prefix}: total status counts disagree")
    allowed = {"OK"} if job["phase"] in {"pre", "post"} else {"OK", "GRPC_RESOURCEEXHAUSTED"}
    unexpected = sorted(status for status, count in total_statuses.items() if count and status not in allowed)
    require(not unexpected, f"{prefix}: unexpected statuses: {unexpected}")

    successful = report.get("successful_rtt_raw_us")
    require(isinstance(successful, list), f"{prefix}: successful raw RTTs are missing")
    successful_values = [integer(value, f"{prefix}.successful_rtt_raw_us") for value in successful]
    require(successful_values == sorted(successful_values), f"{prefix}: successful RTTs are not sorted")
    require(len(successful_values) == within_statuses.get("OK", 0), f"{prefix}: successful RTT population mismatch")
    rtt_by_status = report.get("rtt_raw_us_by_status")
    require(isinstance(rtt_by_status, dict), f"{prefix}: per-status RTTs are missing")
    normalized_rtt: dict[str, list[int]] = {}
    for status, values in rtt_by_status.items():
        require(isinstance(values, list), f"{prefix}: RTT array for {status} is invalid")
        normalized = [integer(value, f"{prefix}.rtt_raw_us_by_status.{status}") for value in values]
        require(normalized == sorted(normalized), f"{prefix}: RTTs for {status} are not sorted")
        require(len(normalized) == total_statuses.get(status, 0), f"{prefix}: RTT/status population mismatch for {status}")
        normalized_rtt[status] = normalized
    require(sum(len(values) for values in normalized_rtt.values()) == completed, f"{prefix}: total raw RTT population mismatch")

    dispatch_lags_raw = report.get("dispatch_lag_raw_us")
    require(isinstance(dispatch_lags_raw, list), f"{prefix}: dispatch lag evidence missing")
    dispatch_lags = [integer(value, f"{prefix}.dispatch_lag_raw_us") for value in dispatch_lags_raw]
    require(dispatch_lags == sorted(dispatch_lags), f"{prefix}: dispatch lags are not sorted")
    require(len(dispatch_lags) == initiated, f"{prefix}: dispatch-lag population mismatch")
    dispatch_p99 = nearest_rank(dispatch_lags, 0.99)
    require(dispatch_p99 is not None and dispatch_p99 <= thresholds["max_dispatch_p99_lag_ms"] * 1_000, f"{prefix}: scheduler p99 lag exceeds attribution threshold")
    scheduler_ready = integer(report.get("scheduler_ready_epoch_ms"), f"{prefix}.scheduler_ready_epoch_ms")
    require(scheduler_ready < job["start_epoch_ms"], f"{prefix}: scheduler was not ready before its phase")
    drain_completed = integer(report.get("drain_completed_epoch_ms"), f"{prefix}.drain_completed_epoch_ms")
    phase_end = job["start_epoch_ms"] + job["duration_seconds"] * 1_000
    # A finite open-loop schedule can finish its final RPC just before the
    # half-open plateau endpoint (especially a one-slot probe).  That is zero
    # drain, not a clock error.
    drain_ms = max(0, drain_completed - phase_end)
    require(drain_completed <= phase_end + thresholds["max_drain_seconds"] * 1_000, f"{prefix}: drain exceeded 90 seconds")

    total_ok = total_statuses.get("OK", 0)
    return {
        "ordinal": job["ordinal"],
        "phase": job["phase"],
        "recovery_offset_seconds": job.get("recovery_offset_seconds"),
        "offered_slots": offered,
        "initiated": initiated,
        "completed": completed,
        "completed_within_plateau": within,
        "drained_after_plateau": drained,
        "drain_ratio": drained / offered,
        "ok_within_plateau": within_statuses.get("OK", 0),
        "ok_total": total_ok,
        "offered_success_ratio": within_statuses.get("OK", 0) / offered,
        "useful_rps": within_statuses.get("OK", 0) / job["duration_seconds"],
        "statuses_total": total_statuses,
        "resource_exhausted": total_statuses.get("GRPC_RESOURCEEXHAUSTED", 0),
        "latency_us": {
            "samples": len(successful_values),
            "p50": nearest_rank(successful_values, 0.50),
            "p99": nearest_rank(successful_values, 0.99),
        },
        "ok_rtt_us_total": normalized_rtt.get("OK", []),
        "dispatch_p99_us": dispatch_p99,
        "drain_duration_ms": drain_ms,
    }


def evaluate_cycle(
    results: list[dict[str, Any]], queue_ratio: float, thresholds: dict[str, Any]
) -> dict[str, Any]:
    require(len(results) == 14, "exactly 14 validated driver results are required")
    pre, overload, post = results[0], results[1], results[13]
    require(pre["phase"] == "pre" and overload["phase"] == "overload" and post["phase"] == "post", "phase ordering changed")
    require(pre["latency_us"]["p50"] is not None and pre["latency_us"]["p99"] is not None, "pre phase has no successful latency population")
    require(post["latency_us"]["p50"] is not None and post["latency_us"]["p99"] is not None, "post phase has no successful latency population")

    recovery_limit_us = max(2 * pre["latency_us"]["p99"], 50_000)
    probes: list[dict[str, Any]] = []
    for result in results[2:13]:
        rtts = result["ok_rtt_us_total"]
        good = (
            result["offered_slots"] == 1
            and result["initiated"] == 1
            and result["ok_total"] == 1
            and len(rtts) == 1
            and rtts[0] <= recovery_limit_us
        )
        probes.append(
            {
                "offset_seconds": result["recovery_offset_seconds"],
                "good": good,
                "status_counts": result["statuses_total"],
                "rtt_us": rtts[0] if len(rtts) == 1 else None,
            }
        )

    # The sparse schedule has only two observations at/after +55 and one at
    # +89.  A strict "three sparse probes" interpretation would therefore make
    # the preregistered amber band impossible.  The clean post35 stream begins
    # at +95 and can supply the missing confirmation(s), but only when *every*
    # post35 response is OK and its maximum RTT remains inside the recovery
    # budget.  That stronger condition makes the chronological first post
    # responses safe even though the driver intentionally emits sorted RTTs.
    post_continuation_good = (
        post["ok_total"] == post["offered_slots"]
        and len(post["ok_rtt_us_total"]) == post["offered_slots"]
        and max(post["ok_rtt_us_total"], default=recovery_limit_us + 1) <= recovery_limit_us
    )
    first_stable: int | None = None
    for index, candidate in enumerate(probes):
        # recovery_time is always anchored by a passing sparse probe.  Post35
        # can only supply one or two *following* confirmations; it can never
        # turn a failed or missing sparse candidate into recovery.
        if not candidate["good"]:
            continue
        later_sparse = probes[index + 1 :]
        if not all(probe["good"] for probe in later_sparse):
            continue
        next_sparse = later_sparse[:2]
        needed_post_observations = 2 - len(next_sparse)
        if not all(probe["good"] for probe in next_sparse):
            continue
        if needed_post_observations > 0 and not post_continuation_good:
            continue
        first_stable = candidate["offset_seconds"]
        break
    if first_stable is None or first_stable > 55:
        recovery_color = "red"
    elif first_stable <= 34:
        recovery_color = "green"
    else:
        recovery_color = "amber"

    useful_ratio = post["useful_rps"] / pre["useful_rps"]
    p50_ratio = post["latency_us"]["p50"] / pre["latency_us"]["p50"]
    p99_ratio = post["latency_us"]["p99"] / pre["latency_us"]["p99"]
    checks = {
        "pre_success_at_least_99_9pct": pre["offered_success_ratio"] >= thresholds["steady_success_min"],
        "pre_drain_at_most_0_1pct": pre["drain_ratio"] <= thresholds["steady_drain_max"],
        "post_success_at_least_99_9pct": post["offered_success_ratio"] >= thresholds["steady_success_min"],
        "post_drain_at_most_0_1pct": post["drain_ratio"] <= thresholds["steady_drain_max"],
        "post_pre_useful_within_2pct": abs(useful_ratio - 1.0) <= thresholds["post_useful_relative_delta_max"],
        "post_p50_at_most_1_10x": p50_ratio <= thresholds["post_p50_ratio_max"],
        "post_p99_at_most_1_20x": p99_ratio <= thresholds["post_p99_ratio_max"],
        "overload_queue_above_10x": queue_ratio > thresholds["overload_queue_ratio_min_exclusive"],
        "overload_has_drain_or_resource_exhausted": overload["drain_ratio"] > thresholds["overload_drain_ratio_min_exclusive"] or overload["resource_exhausted"] > 0,
        "stable_recovery_observed_by_89s": first_stable is not None,
    }
    steady_and_overload = all(checks.values())
    gate_pass = steady_and_overload and recovery_color == "green"
    return {
        "checks": checks,
        "recovery": {
            "threshold_us": recovery_limit_us,
            "recovery_time_seconds": first_stable,
            "first_probe_with_three_consecutive_and_all_later_good_seconds": first_stable,
            "classification": recovery_color,
            "confirmation_rule": "recovery_time is the earliest passing sparse probe whose next two chronological observations (later sparse probes, then post35 if needed) pass and whose every later sparse probe passes; post35 confirms but never creates or advances a sparse recovery_time",
            "post_observation_policy": "because the pinned driver emits sorted rather than chronological post RTTs, post35 is eligible as a tail confirmation only when every post response is OK and the maximum post RTT is within the recovery budget; this proves its first two chronological observations pass",
            "post_continuation_good": post_continuation_good,
            "probes": probes,
        },
        "comparisons": {
            "post_pre_useful_ratio": useful_ratio,
            "post_pre_p50_ratio": p50_ratio,
            "post_pre_p99_ratio": p99_ratio,
            "overload_pre_queue_p99_ratio": queue_ratio,
        },
        "benchmark_gate_pass": gate_pass,
        "status": "green" if gate_pass else ("amber" if steady_and_overload and recovery_color == "amber" else "red"),
    }


def parse_epoch(timestamp: str) -> float:
    match = re.fullmatch(r"(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d)(?:\.(\d+))?Z", timestamp)
    require(match is not None, f"invalid RFC3339 log timestamp: {timestamp}")
    fraction = ((match.group(2) or "") + "000000")[:6]
    parsed = datetime.fromisoformat(f"{match.group(1)}.{fraction}+00:00")
    return parsed.timestamp()


_DURATION = re.compile(r"^(\d+(?:\.\d+)?)(ns|us|µs|ms|s)$")


def duration_seconds(value: str) -> float:
    match = _DURATION.fullmatch(value)
    require(match is not None, f"unrecognized duration in target log: {value}")
    scale = {"ns": 1e-9, "us": 1e-6, "µs": 1e-6, "ms": 1e-3, "s": 1.0}[match.group(2)]
    return float(match.group(1)) * scale


_METRIC_LINE = re.compile(
    r"^(?P<timestamp>\S+) .*llm-d-sc metrics: served=(?P<served>\d+) hits=(?P<hits>\d+) misses=(?P<misses>\d+) \| "
    r"queue p50=(?P<queue_p50>\S+) p99=(?P<queue_p99>\S+)"
)


def validate_queue_logs(run_dir: Path, plan: dict[str, Any], provenance: dict[str, Any]) -> dict[str, Any]:
    before_path = run_dir / "target-logs-before.txt"
    after_path = run_dir / "target-logs-full.txt"
    try:
        before = before_path.read_text(encoding="utf-8")
        lines = after_path.read_text(encoding="utf-8").splitlines()
    except FileNotFoundError as error:
        raise ValidationError(f"missing target log artifact: {error.filename}") from error
    baseline = read_json(run_dir / "target-counter-baseline.json")
    expected_baseline = provenance.get("counter_attribution", {}).get("baseline")
    require(
        baseline.get("schema_version") == 1
        and baseline.get("target_uid") == provenance.get("target", {}).get("uid")
        and baseline.get("target_ip") == provenance.get("target", {}).get("ip")
        and baseline.get("container_started_at") == provenance.get("target", {}).get("container_started_at")
        and baseline.get("traffic_clean") is True
        and baseline.get("counters") == {"served": 0, "hits": 0, "misses": 0}
        and baseline.get("counters") == expected_baseline
        and integer(baseline.get("quiet_interval_seconds"), "counter baseline quiet interval") >= 10
        and provenance.get("plan_created_epoch_ms")
        <= integer(baseline.get("captured_epoch_ms"), "counter baseline capture epoch")
        <= plan["t0_epoch_ms"] - 180_000
        and baseline.get("log_sha256") == sha256(before_path)
        and baseline.get("log_sha256") == provenance.get("counter_attribution", {}).get("baseline_log_sha256"),
        "traffic-clean target counter baseline is missing, late, or not bound to the exact logs/Pod",
    )
    require("llm-d-sc metrics:" not in before, "target had prior classification traffic; cumulative queue telemetry is contaminated")
    samples: list[dict[str, Any]] = []
    for line in lines:
        match = _METRIC_LINE.search(line)
        if not match:
            continue
        samples.append(
            {
                "epoch": parse_epoch(match.group("timestamp")),
                "served": int(match.group("served")),
                "hits": int(match.group("hits")),
                "misses": int(match.group("misses")),
                "queue_p50_seconds": duration_seconds(match.group("queue_p50")),
                "queue_p99_seconds": duration_seconds(match.group("queue_p99")),
            }
        )
    require(samples, "target logs contain no timestamped internal queue telemetry")
    require(all(sample["hits"] == 0 and sample["served"] == sample["misses"] for sample in samples), "cache hits or counter inconsistency contaminate the unique-miss cycle")
    require([sample["epoch"] for sample in samples] == sorted(sample["epoch"] for sample in samples), "target counter/queue logs are not timestamp ordered")
    for left, right in zip(samples, samples[1:]):
        require(
            all(right[key] >= left[key] for key in ("served", "hits", "misses")),
            "target cumulative counters decreased during the cycle",
        )
    pre_job, overload_job = plan["jobs"][0], plan["jobs"][1]
    pre_window = [sample for sample in samples if pre_job["start_epoch_ms"] / 1_000 <= sample["epoch"] <= (pre_job["start_epoch_ms"] / 1_000 + pre_job["duration_seconds"])]
    overload_window = [sample for sample in samples if overload_job["start_epoch_ms"] / 1_000 <= sample["epoch"] <= (overload_job["start_epoch_ms"] / 1_000 + overload_job["duration_seconds"] + 10)]
    require(pre_window, "no internal queue sample falls inside pre35")
    require(overload_window, "no internal queue sample falls inside overload47")
    pre_p99 = pre_window[-1]["queue_p99_seconds"]
    overload_p99 = max(sample["queue_p99_seconds"] for sample in overload_window)
    require(pre_p99 > 0, "pre35 queue p99 is zero and cannot anchor a ratio")
    return {
        "source": "timestamped process-local cumulative histogram logs on a traffic-clean Pod",
        "pre_last_queue_p99_seconds": pre_p99,
        "overload_max_queue_p99_seconds": overload_p99,
        "ratio": overload_p99 / pre_p99,
        "samples": len(samples),
        "baseline_counters": baseline["counters"],
        "last_counters": {key: samples[-1][key] for key in ("served", "hits", "misses")},
        "counter_deltas": {
            key: samples[-1][key] - baseline["counters"][key]
            for key in ("served", "hits", "misses")
        },
    }


def validate_counter_reconciliation(
    queue: dict[str, Any], results: list[dict[str, Any]], provenance: dict[str, Any]
) -> dict[str, Any]:
    attribution = provenance.get("counter_attribution")
    require(isinstance(attribution, dict), "counter-attribution provenance is missing")
    tolerance = integer(attribution.get("tolerance"), "target counter tolerance")
    require(tolerance == 0, "target counter tolerance must remain zero for exclusive attribution")
    require(
        attribution.get("tolerance_justification")
        == "zero: a traffic-clean Pod, exclusive lock, completed drivers, and two metrics-log settle intervals permit exact reconciliation",
        "target counter tolerance justification differs from the frozen protocol",
    )
    initiated = sum(integer(result.get("initiated"), "driver initiated total") for result in results)
    completed = sum(integer(result.get("completed"), "driver completed total") for result in results)
    ok = sum(integer(result.get("statuses_total", {}).get("OK", 0), "driver OK status total") for result in results)
    rejected = sum(
        integer(result.get("statuses_total", {}).get("GRPC_RESOURCEEXHAUSTED", 0), "driver RESOURCE_EXHAUSTED status total")
        for result in results
    )
    status_completed = sum(sum(result.get("statuses_total", {}).values()) for result in results)
    require(initiated == completed == status_completed == ok + rejected, "all-driver initiated/completed/status accounting does not reconcile")
    delta = queue.get("counter_deltas")
    require(isinstance(delta, dict), "target counter deltas are missing")
    require(delta.get("hits") == 0, "target cache-hit delta is nonzero in the unique-miss cycle")
    require(delta.get("served") == ok and delta.get("misses") == ok, "target served/miss deltas do not exactly equal all driver OK completions")
    return {
        "tolerance": tolerance,
        "driver_initiated": initiated,
        "driver_completed": completed,
        "driver_ok": ok,
        "driver_resource_exhausted": rejected,
        "target_counter_deltas": delta,
        "equations": {
            "initiated_equals_completed_equals_ok_plus_resource_exhausted": True,
            "target_served_equals_target_misses_equals_driver_ok": True,
            "target_hits_equals_zero": True,
        },
    }


def metric_values(series: dict[str, Any]) -> list[tuple[float, float]]:
    raw_values = series.get("values")
    require(isinstance(raw_values, list), "Prometheus series values are missing")
    values: list[tuple[float, float]] = []
    for raw in raw_values:
        require(isinstance(raw, list) and len(raw) == 2, "invalid Prometheus sample")
        epoch = number(raw[0], "Prometheus timestamp")
        try:
            value = float(raw[1])
        except (TypeError, ValueError) as error:
            raise ValidationError("invalid Prometheus value") from error
        require(math.isfinite(value), "non-finite Prometheus value")
        values.append((epoch, value))
    require(values == sorted(values), "Prometheus samples are not timestamp ordered")
    return values


def coverage(values: list[tuple[float, float]], start: float, end: float, max_gap: float) -> dict[str, Any]:
    selected = [sample for sample in values if start <= sample[0] <= end]
    require(selected, f"telemetry phase [{start},{end}] has no samples")
    gaps = [right[0] - left[0] for left, right in zip(selected, selected[1:])]
    observed_gap = max(gaps, default=0.0)
    require(selected[0][0] - start <= max_gap, "telemetry begins too late for a phase")
    require(end - selected[-1][0] <= max_gap, "telemetry ends too early for a phase")
    require(observed_gap <= max_gap, "telemetry has an excessive sample gap")
    return {
        "samples": len(selected),
        "first_epoch": selected[0][0],
        "last_epoch": selected[-1][0],
        "max_gap_seconds": observed_gap,
        "min": min(value for _, value in selected),
        "max": max(value for _, value in selected),
        "mean": sum(value for _, value in selected) / len(selected),
    }


def validate_telemetry(run_dir: Path, plan: dict[str, Any], provenance: dict[str, Any]) -> dict[str, Any]:
    window = read_json(run_dir / "telemetry-window.json")
    require(window.get("schema_version") == 1 and window.get("step_seconds") == 5, "telemetry window contract mismatch")
    expected_start = plan["t0_epoch_ms"] // 1_000 - 30
    post = plan["jobs"][13]
    expected_end = post["start_epoch_ms"] // 1_000 + post["duration_seconds"] + 30
    require(window.get("start_epoch_s") == expected_start and window.get("end_epoch_s") == expected_end, "telemetry window does not cover the frozen cycle")
    max_gap = number(window.get("max_gap_seconds"), "telemetry.max_gap_seconds")
    require(max_gap <= 10, "telemetry max-gap policy was weakened")

    target_name = provenance["target"]["name"]
    target_node = provenance["target"]["node"]
    driver_node = provenance["driver_node"]
    required = {
        "pod_cpu_otel": ("k8s_pod_name", [target_name]),
        "container_cpu_otel": ("k8s_pod_name", [target_name]),
        "container_cpu_cadvisor": ("pod", [target_name]),
        "memory_working_set": ("pod", [target_name]),
        "restarts": ("pod", [target_name]),
        "pod_ready": ("pod", [target_name]),
        "node_ready": ("node", sorted(set([target_node, driver_node]))),
    }
    optional = ["throttle_ratio", "cpu_pressure_waiting"]
    phases = {
        "pre": (plan["jobs"][0]["start_epoch_ms"] / 1_000, plan["jobs"][0]["start_epoch_ms"] / 1_000 + 180),
        "gap": (plan["jobs"][0]["start_epoch_ms"] / 1_000 + 180, plan["jobs"][1]["start_epoch_ms"] / 1_000),
        "overload": (plan["jobs"][1]["start_epoch_ms"] / 1_000, plan["jobs"][1]["start_epoch_ms"] / 1_000 + 120),
        "recovery": (plan["jobs"][1]["start_epoch_ms"] / 1_000 + 120, plan["jobs"][1]["start_epoch_ms"] / 1_000 + 210),
        "post": (post["start_epoch_ms"] / 1_000, post["start_epoch_ms"] / 1_000 + 180),
    }
    summary: dict[str, Any] = {"required": {}, "optional_query_status": {}, "phases": list(phases)}
    for metric_name, (label, expected_names) in required.items():
        document = read_json(run_dir / "metrics" / f"{metric_name}.json")
        require(document.get("status") == "success", f"telemetry query failed: {metric_name}")
        raw_series = document.get("data", {}).get("result")
        require(isinstance(raw_series, list), f"telemetry result is malformed: {metric_name}")
        reported_names = sorted(
            series.get("metric", {}).get(label)
            for series in raw_series
            if isinstance(series.get("metric", {}).get(label), str)
        )
        require(reported_names == sorted(expected_names), f"{metric_name}: telemetry series set differs from the bound target/node set")
        metric_summary: dict[str, Any] = {}
        for expected_name in expected_names:
            matching = [series for series in raw_series if series.get("metric", {}).get(label) == expected_name]
            require(len(matching) == 1, f"{metric_name}: expected exactly one series for {expected_name}, found {len(matching)}")
            values = metric_values(matching[0])
            full = coverage(values, expected_start, expected_end, max_gap)
            phase_slices = {name: coverage(values, start, end, max_gap) for name, (start, end) in phases.items()}
            metric_summary[expected_name] = {"full_window": full, "phases": phase_slices}
            if metric_name in {"pod_ready", "node_ready"}:
                require(full["min"] == 1.0, f"{metric_name}: readiness dropped below one")
            if metric_name == "restarts":
                require(full["min"] == 0.0 and full["max"] == 0.0, "target restart telemetry changed")
        summary["required"][metric_name] = metric_summary
    for metric_name in optional:
        document = read_json(run_dir / "metrics" / f"{metric_name}.json")
        require(document.get("status") == "success", f"supporting telemetry query failed: {metric_name}")
        summary["optional_query_status"][metric_name] = "success"
    summary["all_required_series_complete"] = True
    return summary


def pod_identity(pod: dict[str, Any], container: str) -> dict[str, Any]:
    statuses = pod.get("status", {}).get("containerStatuses", [])
    status = next((entry for entry in statuses if entry.get("name") == container), None)
    require(status is not None, f"target container {container} is absent")
    conditions = pod.get("status", {}).get("conditions", [])
    return {
        "name": pod.get("metadata", {}).get("name"),
        "uid": pod.get("metadata", {}).get("uid"),
        "ip": pod.get("status", {}).get("podIP"),
        "node": pod.get("spec", {}).get("nodeName"),
        "ready": any(condition.get("type") == "Ready" and condition.get("status") == "True" for condition in conditions),
        "restart_count": status.get("restartCount"),
        "image_id": status.get("imageID"),
    }


def expand_cpuset(value: Any) -> set[int]:
    require(isinstance(value, str) and value, "effective cpuset is missing")
    cpus: set[int] = set()
    try:
        for item in value.split(","):
            bounds = item.split("-", 1)
            start = int(bounds[0])
            end = int(bounds[1]) if len(bounds) == 2 else start
            require(0 <= start <= end, "effective cpuset range is invalid")
            cpus.update(range(start, end + 1))
    except ValueError as error:
        raise ValidationError("effective cpuset is invalid") from error
    require(cpus, "effective cpuset is empty")
    return cpus


def validate_target_shape(run_dir: Path, provenance: dict[str, Any]) -> dict[str, Any]:
    expected_shape = {
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
    }
    require(provenance.get("target_shape") == expected_shape, "live target-shape provenance is not exact W1/RT1/Candle-unset Guaranteed 2CPU/4Gi")

    deployment = read_json(run_dir / "deployment-before.json")
    containers = [
        container
        for container in deployment.get("spec", {}).get("template", {}).get("spec", {}).get("containers", [])
        if container.get("name") == provenance.get("target_container")
    ]
    require(len(containers) == 1, "target Deployment must contain exactly one named target container")
    container = containers[0]
    environment = container.get("env", [])

    def exact_env(name: str, expected: str) -> None:
        matches = [entry for entry in environment if entry.get("name") == name]
        require(
            len(matches) == 1
            and matches[0].get("value") == expected
            and "valueFrom" not in matches[0],
            f"target Deployment {name} must be exactly {expected!r}",
        )

    exact_env("LLM_D_SC_INFERENCE_WORKERS", "1")
    exact_env("RAYON_NUM_THREADS", "1")
    exact_env("LLM_D_SC_METRICS_LOG_SECS", "10")
    require(not any(entry.get("name") == "CANDLE_NUM_THREADS" for entry in environment), "target Deployment must leave CANDLE_NUM_THREADS unset")
    require(not container.get("envFrom"), "target Deployment envFrom is forbidden because it obscures the benchmark environment")
    require(not container.get("command") and not container.get("args"), "target Deployment command/args overrides are forbidden because they can wrap or alter PID1")
    require(container.get("resources") == expected_shape["resources"], "target Deployment resources must be exact Guaranteed 2 CPU/4Gi requests and limits")

    targets = read_json(run_dir / "targets-before.json").get("items")
    require(isinstance(targets, list) and len(targets) == 1, "target-shape validation requires exactly one target Pod")
    require(targets[0].get("status", {}).get("qosClass") == "Guaranteed", "target Pod QoS class is not Guaranteed")

    runtime = read_json(run_dir / "runtime-cgroup-before.json")
    cpu_max = runtime.get("cpu_max")
    cpu_max_fields = cpu_max.split() if isinstance(cpu_max, str) else []
    require(
        len(cpu_max_fields) == 2
        and cpu_max_fields[0] == "max"
        and cpu_max_fields[1].isdigit()
        and int(cpu_max_fields[1]) > 0,
        "runtime cpu.max must be unquotaed ('max <positive-period>')",
    )
    target_cpuset = provenance.get("target", {}).get("cpuset_cpus_effective")
    require(runtime.get("schema_version") == 1, "runtime cgroup evidence schema mismatch")
    require(runtime.get("cpuset_cpus_effective") == target_cpuset, "runtime cpuset differs from bound target cpuset")
    require(runtime.get("cpu_max") == provenance.get("target", {}).get("cpu_max"), "runtime cpu.max differs from provenance")
    require(runtime.get("cpu_max_quota") == "max", "runtime cpu.max quota is not max")
    require(runtime.get("logical_cpus") == 2 and len(expand_cpuset(target_cpuset)) == 2, "runtime target cpuset is not exactly two logical CPUs")
    process = read_json(run_dir / "runtime-process-before.json")
    require(
        process
        == {
            "schema_version": 1,
            "pid1_executable": "/usr/local/bin/llm-d-sc",
            "environment": {
                "LLM_D_SC_INFERENCE_WORKERS": "1",
                "RAYON_NUM_THREADS": "1",
                "LLM_D_SC_METRICS_LOG_SECS": "10",
                "CANDLE_NUM_THREADS": None,
            },
            "candle_num_threads_present": False,
        },
        "actual PID1 executable/environment is not exact W1/RT1/Candle-unset/metrics-log=10",
    )
    return {
        "shape": "W1/RT1/Candle-unset Guaranteed 2CPU/4Gi",
        "qos_class": "Guaranteed",
        "cpuset_cpus_effective": target_cpuset,
        "cpu_max": cpu_max,
        "pid1_executable": process["pid1_executable"],
        "pid1_environment": process["environment"],
    }


def validate_identity(run_dir: Path, plan: dict[str, Any], provenance: dict[str, Any]) -> dict[str, Any]:
    target = provenance["target"]
    container = provenance["target_container"]
    expected = {key: target[key] for key in ("name", "uid", "ip", "node", "image_id")}
    expected.update({"ready": True, "restart_count": 0})
    for artifact in ("targets-before.json", "targets-after.json"):
        document = read_json(run_dir / artifact)
        items = document.get("items")
        require(isinstance(items, list) and len(items) == 1, f"{artifact}: expected exactly one target Pod")
        require(pod_identity(items[0], container) == expected, f"{artifact}: target identity/health changed")
        require(items[0].get("status", {}).get("qosClass") == "Guaranteed", f"{artifact}: target QoS class changed")

    checkpoints = plan.get("checkpoints")
    require(isinstance(checkpoints, list) and checkpoints, "checkpoint plan is missing")
    checkpoint_summary = []
    for checkpoint in checkpoints:
        name = checkpoint["name"]
        document = read_json(run_dir / "checkpoints" / f"{name}.json")
        require(document.get("scheduled_epoch_ms") == checkpoint["scheduled_epoch_ms"], f"checkpoint {name}: schedule mismatch")
        observed = integer(document.get("observed_epoch_ms"), f"checkpoint {name}.observed_epoch_ms")
        require(checkpoint["scheduled_epoch_ms"] - 2_000 <= observed <= checkpoint["scheduled_epoch_ms"] + 15_000, f"checkpoint {name}: observation outside timing budget")
        require(document.get("target") == expected, f"checkpoint {name}: target identity/health changed")
        require(document.get("cpuset_cpus_effective") == target["cpuset_cpus_effective"], f"checkpoint {name}: cpuset changed")
        require(document.get("cpu_max") == target["cpu_max"], f"checkpoint {name}: cpu.max changed")
        require(str(document.get("cpu_max", "")).split()[0] == "max", f"checkpoint {name}: cpu.max became quota-limited")
        checkpoint_summary.append({"name": name, "observed_epoch_ms": observed, "cpu_max": document["cpu_max"]})

    target_bound = checkpoints[0]
    target_bound_gate = read_json(run_dir / "checkpoints" / "target-bound-gate.json")
    require(
        target_bound_gate.get("schema_version") == 1
        and target_bound_gate.get("name") == "target-bound"
        and target_bound_gate.get("load_authorized") is True
        and target_bound_gate.get("completion_deadline_epoch_ms")
        == target_bound["completion_deadline_epoch_ms"],
        "target-bound pre-T0 authorization artifact is invalid",
    )
    target_bound_completion = integer(
        target_bound_gate.get("completion_epoch_ms"),
        "target-bound completion_epoch_ms",
    )
    require(
        target_bound_completion <= target_bound["completion_deadline_epoch_ms"]
        and target_bound_completion < plan["t0_epoch_ms"],
        "target-bound checkpoint did not complete by its hard pre-T0 deadline",
    )

    monitor = read_ndjson(run_dir / "health-monitor.ndjson")
    monitor_expected = {key: expected[key] for key in ("name", "uid", "ip", "node", "ready", "restart_count", "image_id")}
    require(all(sample.get("target") == monitor_expected and sample.get("nodes_ready") is True for sample in monitor), "health monitor observed identity, readiness, restart, image, or node failure")
    monitor_epochs = [integer(sample.get("sample_epoch_s"), "health monitor epoch") for sample in monitor]
    require(monitor_epochs == sorted(monitor_epochs), "health monitor timestamps are unordered")
    require(max((right - left for left, right in zip(monitor_epochs, monitor_epochs[1:])), default=0) <= 15, "health monitor has a gap above 15 seconds")
    require(monitor_epochs[0] <= plan["t0_epoch_ms"] // 1_000, "health monitor began after T0")
    post_end = plan["jobs"][13]["start_epoch_ms"] // 1_000 + 180
    require(monitor_epochs[-1] >= post_end, "health monitor ended before post35 completed")
    return {"target": expected, "checkpoints": checkpoint_summary, "health_samples": len(monitor)}


def validate_topology(run_dir: Path, provenance: dict[str, Any]) -> dict[str, Any]:
    execution_path = run_dir / "topology-preflight-execution.json"
    report_path = run_dir / "topology-preflight-report.json"
    stdout_path = run_dir / "topology-preflight-stdout.txt"
    stderr_path = run_dir / "topology-preflight-stderr.txt"
    execution = read_json(execution_path)
    report = read_json(report_path)
    require(
        execution.get("runner_exit_code") == 0
        and execution.get("report_json_valid") is True
        and execution.get("report_gate_valid") is True
        and execution.get("target_identity_match") is True
        and execution.get("load_authorized") is True,
        "topology preflight did not authorize load",
    )
    hashes = execution.get("evidence_sha256", {})
    require(hashes.get("report") == sha256(report_path), "topology report hash mismatch")
    require(hashes.get("raw_stdout") == sha256(stdout_path), "topology raw-stdout hash mismatch")
    require(hashes.get("stderr") == sha256(stderr_path), "topology stderr hash mismatch")
    require(report.get("verdict") == "PASS" and report.get("placement_verdict") == "PASS" and report.get("gate_passed") is True, "topology report is not PASS")
    pods = report.get("pods")
    require(isinstance(pods, list) and len(pods) == 1, "topology report must bind one Pod")
    pod = pods[0]
    target = provenance["target"]
    require([pod.get("name"), pod.get("uid"), pod.get("node"), pod.get("cpuset")] == [target["name"], target["uid"], target["node"], target["cpuset_cpus_effective"]], "topology report is bound to a different target")
    require(pod.get("complete_smt_sibling_sets") is True, "target cpuset is not a complete SMT sibling set")
    require(len(expand_cpuset(pod["cpuset"])) == 2, "topology report does not contain exactly two logical CPUs")
    return {"verdict": "PASS", "report_sha256": sha256(report_path), "cpuset": pod["cpuset"], "logical_cpus": 2, "complete_smt_sibling_sets": True}


def validate_armed_barrier(
    plan: dict[str, Any], provenance: dict[str, Any], armed: dict[str, Any]
) -> dict[str, str]:
    require(plan.get("arming", {}).get("pinned_driver_supports_protocol") is True, "live plan admits a pinned driver without the required ARMED protocol")
    require(plan.get("arming", {}).get("live_executable") is True, "live plan was marked non-executable")
    records = armed.get("records")
    require(
        armed.get("schema_version") == 1
        and armed.get("protocol") == "sustained-corpus-probe-armed-v1"
        and armed.get("all_14_armed") is True
        and isinstance(records, list)
        and len(records) == 14,
        "application-level ARMED barrier evidence is incomplete",
    )
    require(integer(armed.get("verified_epoch_ms"), "ARMED barrier verification epoch") <= plan["t0_epoch_ms"] - 180_000, "ARMED barrier closed less than 180 seconds before T0")
    records_by_job = {record.get("job_id"): record for record in records}
    require(len(records_by_job) == 14, "ARMED records do not cover 14 unique Jobs")
    scheduled_hashes: dict[str, str] = {}
    for job in plan["jobs"]:
        record = records_by_job.get(job["name"])
        require(isinstance(record, dict), f"{job['name']}: ARMED record is missing")
        require(
            record.get("schema") == "llm-d-sc.benchmark-driver.armed"
            and record.get("schema_version") == 1
            and record.get("record_type") == "ARMED"
            and record.get("protocol_version") == "sustained-corpus-probe-armed-v1",
            f"{job['name']}: ARMED record protocol mismatch",
        )
        require(record.get("run_id") == plan["run_id"] and record.get("nonce") == job["arming_nonce"], f"{job['name']}: ARMED run/nonce binding mismatch")
        require(record.get("endpoint") == f"{provenance['target']['ip']}:50051", f"{job['name']}: ARMED endpoint differs from exact target Pod IP")
        require(record.get("scheduled_start_epoch_ms") == job["start_epoch_ms"], f"{job['name']}: ARMED start mismatch")
        require(record.get("expected_slots") == job["expected_slots"] and record.get("duration_seconds") == job["duration_seconds"], f"{job['name']}: ARMED schedule dimensions mismatch")
        armed_epoch = integer(record.get("armed_epoch_ms"), f"{job['name']}.armed_epoch_ms")
        require(provenance["plan_created_epoch_ms"] <= armed_epoch <= plan["t0_epoch_ms"] - 180_000, f"{job['name']}: ARMED timestamp is outside the pre-T0 barrier window")
        scheduled_hash = record.get("scheduled_rows_blake3")
        require(isinstance(scheduled_hash, str) and re.fullmatch(r"[0-9a-f]{64}", scheduled_hash) is not None, f"{job['name']}: ARMED scheduled-row digest is invalid")
        config = record.get("config")
        require(isinstance(config, dict), f"{job['name']}: explicit ARMED canonical config is missing")
        selected_hash = config.get("selected_rows_blake3")
        require(isinstance(selected_hash, str) and re.fullmatch(r"[0-9a-f]{64}", selected_hash) is not None, f"{job['name']}: ARMED selected-row digest is invalid")
        expected_config = {
            "candidate_rows": 10_000,
            "closed_loop_concurrency_argument": 1,
            "connections": 1,
            "corpus_blake3": None,
            "corpus_mode": "generated",
            "corpus_offset": 0,
            "dispatch_late_after_ms": 1,
            "driver_image": provenance["driver_image"],
            "driver_package_version": provenance["driver_package_version"],
            "drop_late_after_ms": 100,
            "duration_seconds": job["duration_seconds"],
            "expected_slots": job["expected_slots"],
            "first_sequence": job["sequence_base"],
            "generator_scheme": "alpha_bravo_lsb_identity_service_fill_v1",
            "job_id": job["name"],
            "last_sequence": job["sequence_base"] + 9_999,
            "max_in_flight": job["max_in_flight"],
            "model_sha256": provenance["model_sha256"],
            "nonce": job["arming_nonce"],
            "offered_rate_denominator": 1,
            "offered_rate_numerator": int(job["offered_rps"]),
            "offered_rate_requested_decimal": job["offered_rps"],
            "offered_rps": job["offered_rps"],
            "protocol_version": "sustained-corpus-probe-armed-v1",
            "raw_latencies": True,
            "rpc_timeout_ms": 30_000,
            "run_id": plan["run_id"],
            "scheduled_rows_blake3": scheduled_hash,
            "scheduled_start_epoch_ms": job["start_epoch_ms"],
            "selected_rows_blake3": selected_hash,
            "target_endpoint": f"{provenance['target']['ip']}:50051",
            "target_image": provenance["target_image"],
            "token_count_including_specials": 64,
            "tokenizer_sha256": provenance["tokenizer_sha256"],
            "topology": provenance["topology"],
            "warmup_requests": 0,
        }
        require(config == expected_config, f"{job['name']}: explicit ARMED config differs from the frozen manifest/plan")
        digest = record.get("config_digest")
        require(
            isinstance(digest, dict)
            and digest.get("algorithm") == "blake3"
            and digest.get("canonicalization") == "sorted-string-map-v1"
            and isinstance(digest.get("hex"), str)
            and re.fullmatch(r"[0-9a-f]{64}", digest["hex"]) is not None,
            f"{job['name']}: ARMED config digest is invalid",
        )
        scheduled_hashes[job["name"]] = scheduled_hash
    return scheduled_hashes


def validate_armed_final_linkage(
    plan: dict[str, Any], provenance: dict[str, Any], armed: dict[str, Any], reports: list[dict[str, Any]]
) -> dict[str, Any]:
    records = armed.get("records")
    require(isinstance(records, list) and len(records) == 14 and len(reports) == 14, "ARMED/final linkage requires 14 records and 14 reports")
    records_by_job = {record.get("job_id"): record for record in records}
    for job, report in zip(plan["jobs"], reports):
        record = records_by_job.get(job["name"])
        require(isinstance(record, dict), f"{job['name']}: cannot link final report to ARMED record")
        require(report.get("target") == record.get("endpoint") == f"{provenance['target']['ip']}:50051", f"{job['name']}: final target differs from ARMED endpoint")
        require(report.get("start_epoch_ms") == record.get("scheduled_start_epoch_ms"), f"{job['name']}: final start differs from ARMED start")
        require(report.get("duration_seconds") == record.get("duration_seconds"), f"{job['name']}: final duration differs from ARMED duration")
        require(report.get("accounting", {}).get("offered_slots") == record.get("expected_slots"), f"{job['name']}: final offered slots differ from ARMED expected slots")
        require(report.get("scheduled_rows_blake3") == record.get("scheduled_rows_blake3"), f"{job['name']}: final scheduled-row digest differs from ARMED digest")
        require(report.get("selected_rows_blake3") == record.get("config", {}).get("selected_rows_blake3"), f"{job['name']}: final selected-row digest differs from ARMED config")
    return {"linked_jobs": 14, "all_final_reports_match_pre_start_armed_records": True}


def validate_precreated_jobs(run_dir: Path, plan: dict[str, Any], provenance: dict[str, Any]) -> dict[str, Any]:
    document = read_json(run_dir / "jobs-precreated.json")
    items = document.get("items")
    require(isinstance(items, list) and len(items) == 14, "not all 14 future Jobs were precreated")
    by_name = {item.get("metadata", {}).get("name"): item for item in items}
    require(len(by_name) == 14, "precreated Job names are not unique")
    for job in plan["jobs"]:
        item = by_name.get(job["name"])
        require(item is not None, f"missing precreated Job {job['name']}")
        require(item.get("spec", {}).get("suspend") is True, f"{job['name']} was not captured suspended")
        creation_time = item.get("metadata", {}).get("creationTimestamp")
        require(isinstance(creation_time, str), f"{job['name']}: creation timestamp missing")
        require(parse_epoch(creation_time) * 1_000 < job["start_epoch_ms"], f"{job['name']}: Job was not precreated before its phase")
        annotations = item.get("metadata", {}).get("annotations", {})
        require(annotations.get("benchmark.llm-d/target-uid") == provenance["target"]["uid"], f"{job['name']}: target UID annotation mismatch")
        require(annotations.get("benchmark.llm-d/target-ip") == provenance["target"]["ip"], f"{job['name']}: target IP annotation mismatch")
        containers = item.get("spec", {}).get("template", {}).get("spec", {}).get("containers", [])
        require(len(containers) == 1 and containers[0].get("image") == provenance["driver_image"], f"{job['name']}: pinned driver image mismatch")
        args = containers[0].get("args", [])
        require(f"{provenance['target']['ip']}:50051" in args, f"{job['name']}: exact Pod IP absent from args")
        require(str(job["start_epoch_ms"]) in args, f"{job['name']}: frozen start absent from args")
        def exact_arg(flag: str, value: str) -> bool:
            return any(
                args[index] == flag and args[index + 1] == value
                for index in range(len(args) - 1)
            )

        require(
            exact_arg("--armed-run-id", plan["run_id"])
            and exact_arg("--armed-job-id", job["name"])
            and exact_arg("--armed-nonce", job["arming_nonce"]),
            f"{job['name']}: frozen application-level ARMED arguments are absent",
        )
    driver_pods = read_json(run_dir / "driver-pods-before.json").get("items")
    require(isinstance(driver_pods, list) and len(driver_pods) == 14, "14 driver Pods were not Ready before T0")
    digest = provenance["driver_image"].split("@", 1)[-1]
    for pod in driver_pods:
        conditions = pod.get("status", {}).get("conditions", [])
        require(any(condition.get("type") == "Ready" and condition.get("status") == "True" for condition in conditions), "a driver Pod was not Ready before T0")
        require(pod.get("spec", {}).get("nodeName") == provenance["driver_node"], "a driver Pod ran on the wrong node")
        statuses = pod.get("status", {}).get("containerStatuses", [])
        require(len(statuses) == 1 and statuses[0].get("restartCount") == 0, "a driver Pod restarted before T0")
        require(str(statuses[0].get("imageID", "")).endswith(digest), "a driver Pod did not run the pinned digest")
    readiness = read_json(run_dir / "driver-kubernetes-readiness.json")
    require(
        readiness.get("schema_version") == 1
        and readiness.get("ready_driver_pods") == 14
        and readiness.get("minimum_lead_seconds") == 180
        and readiness.get("load_authorizing") is False
        and readiness.get("t0_epoch_ms") == plan["t0_epoch_ms"]
        and integer(readiness.get("verified_epoch_ms"), "driver readiness epoch")
        <= plan["t0_epoch_ms"] - 180_000,
        "driver Pods were not all verified Ready at least 180 seconds before T0",
    )

    armed = read_json(run_dir / "driver-armed.json")
    scheduled_hashes = validate_armed_barrier(plan, provenance, armed)
    return {
        "jobs": 14,
        "all_suspended_when_captured": True,
        "driver_pods_ready_before_t0": 14,
        "application_level_armed_before_t0_minus_180s": 14,
        "armed_scheduled_rows_blake3": scheduled_hashes,
    }


def validate_event_delta(run_dir: Path, target_uid: str) -> dict[str, Any]:
    before = read_json(run_dir / "events-before.json").get("items", [])
    after = read_json(run_dir / "events-after.json").get("items", [])
    require(isinstance(before, list) and isinstance(after, list), "event artifacts are malformed")
    def event_count(event: dict[str, Any]) -> int:
        series = event.get("series")
        if isinstance(series, dict) and isinstance(series.get("count"), int):
            return series["count"]
        count = event.get("count", 1)
        return count if isinstance(count, int) else 1

    before_counts = {event.get("metadata", {}).get("uid"): event_count(event) for event in before}
    violations = []
    for event in after:
        if event.get("involvedObject", {}).get("uid") != target_uid:
            continue
        prior = before_counts.get(event.get("metadata", {}).get("uid"), 0)
        current = event_count(event)
        if current > prior and (event.get("type") == "Warning" or event.get("reason") == "Unhealthy"):
            violations.append({"reason": event.get("reason"), "message": event.get("message"), "new_occurrences": current - prior})
    require(not violations, f"target Warning/Unhealthy events occurred: {violations}")
    return {"new_warning_or_unhealthy_events": 0}


def analyze_run(run_dir: Path) -> dict[str, Any]:
    plan = read_json(run_dir / "recovery-plan.json")
    validate_plan(plan)
    provenance = read_json(run_dir / "run-provenance.json")
    require(provenance.get("schema_version") == 1 and provenance.get("run_id") == plan.get("run_id"), "run provenance does not match plan")
    pinned = plan.get("pinned")
    require(isinstance(pinned, dict), "plan pinned-provenance block is missing")
    require(
        all(
            pinned.get(key) == provenance.get(key)
            for key in (
                "driver_image",
                "driver_source_sha256",
                "driver_package_version",
                "target_image",
                "model_sha256",
                "tokenizer_sha256",
            )
        )
        and pinned.get("local_source_matches_pinned") is True
        and pinned.get("local_driver_source_sha256") == provenance.get("driver_source_sha256"),
        "live provenance differs from the frozen pinned plan/source",
    )
    require(
        provenance.get("scheduler_thresholds")
        == {
            "dispatch_late_after_ms": 1,
            "drop_late_after_ms": 100,
            "rpc_timeout_ms": 30_000,
            "max_dispatch_p99_lag_ms": 5,
            "max_drain_seconds": 90,
        },
        "scheduler attribution thresholds differ from the frozen protocol",
    )
    require(
        provenance.get("service_thresholds")
        == {
            "steady_success_min": 0.999,
            "steady_drain_max": 0.001,
            "post_useful_relative_delta_max": 0.02,
            "post_p50_ratio_max": 1.10,
            "post_p99_ratio_max": 1.20,
            "overload_queue_ratio_min_exclusive": 10,
            "overload_drain_ratio_min_exclusive": 0.01,
        },
        "service decision thresholds differ from the frozen protocol",
    )
    require(
        isinstance(provenance.get("driver_source_sha256"), str)
        and re.fullmatch(r"[0-9a-f]{64}", provenance["driver_source_sha256"]) is not None,
        "driver source digest is not pinned",
    )
    ready_transition = parse_epoch(provenance["target"]["ready_transition_time"])
    require(plan["t0_epoch_ms"] / 1_000 - ready_transition >= 180, "T0 is less than 180 seconds after target Ready")
    require(
        plan.get("created_epoch_ms") == provenance.get("plan_created_epoch_ms"),
        "plan/provenance creation epochs differ",
    )
    require(plan["t0_epoch_ms"] - plan["created_epoch_ms"] >= 360_000, "T0 was not at least 360 seconds in the future")

    target_shape = validate_target_shape(run_dir, provenance)
    topology = validate_topology(run_dir, provenance)
    jobs = validate_precreated_jobs(run_dir, plan, provenance)
    identity = validate_identity(run_dir, plan, provenance)
    events = validate_event_delta(run_dir, provenance["target"]["uid"])
    telemetry = validate_telemetry(run_dir, plan, provenance)

    reports: list[dict[str, Any]] = []
    results: list[dict[str, Any]] = []
    for job in plan["jobs"]:
        path = run_dir / "drivers" / f"j{job['ordinal']:02d}.json"
        report = read_json(path)
        reports.append(report)
        results.append(validate_driver_report(job, report, provenance))
    require(len({report["selected_rows_blake3"] for report in reports}) == 14, "selected-row hashes are not unique across Jobs")
    require(len({report["scheduled_rows_blake3"] for report in reports}) == 14, "scheduled-row hashes are not unique across Jobs")
    arming_linkage = validate_armed_final_linkage(
        plan, provenance, read_json(run_dir / "driver-armed.json"), reports
    )
    queue = validate_queue_logs(run_dir, plan, provenance)
    counter_reconciliation = validate_counter_reconciliation(queue, results, provenance)
    decision = evaluate_cycle(results, queue["ratio"], provenance["service_thresholds"])
    public_results = [
        {key: value for key, value in result.items() if key != "ok_rtt_us_total"}
        for result in results
    ]
    return {
        "schema_version": 1,
        "run_id": plan["run_id"],
        "protocol": plan["protocol"],
        "validity": {"valid": True, "fail_closed": True},
        "binding": {
            "one_target_pod_for_entire_cycle": True,
            "direct_pod_ip": provenance["target"]["ip"],
            "target_uid": provenance["target"]["uid"],
            "target_image": provenance["target_image"],
            "driver_image": provenance["driver_image"],
        },
        "target_shape": target_shape,
        "topology": topology,
        "precreated_jobs": jobs,
        "arming_final_linkage": arming_linkage,
        "identity_and_health": identity,
        "events": events,
        "telemetry": telemetry,
        "queue_evidence": queue,
        "counter_reconciliation": counter_reconciliation,
        "driver_results": public_results,
        "decision": decision,
        "limitations": [
            "One cycle is recovery evidence for the pinned one-Pod W1/RT1 shape, not a confidence interval across repeated cycles.",
            "The internal queue ratio comes from process-local cumulative histograms, so the harness requires a traffic-clean Pod before T0.",
            "The ARMED config digest is recorded pinned-driver provenance; pre-load authorization comes from exact explicit-config equality because the harness does not independently recompute BLAKE3.",
            "Direct Pod-IP traffic isolates semantic-classifier behavior and does not measure ClusterIP or ingress routing.",
        ],
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("run_dir", type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args(argv)
    output = args.output or args.run_dir / "recovery-summary.json"
    try:
        summary = analyze_run(args.run_dir)
    except ValidationError as error:
        invalid = {
            "schema_version": 1,
            "validity": {"valid": False, "fail_closed": True, "error": str(error)},
            "decision": {"status": "invalid", "benchmark_gate_pass": False},
        }
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(invalid, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        print(f"ERROR: {error}", file=sys.stderr)
        return 2
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
