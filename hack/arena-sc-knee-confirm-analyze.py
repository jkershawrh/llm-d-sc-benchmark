#!/usr/bin/env python3
"""Evaluate the frozen Arena llm-d-sc 41/42-RPS confirmation study.

The input is the schema-v2 ``sweep-summary.json`` produced by
``arena-sc-open-loop-summarize.py``.  This analyzer is intentionally separate
from the load and primary-summary paths: it does not mutate evidence and it
does not infer a new rule from the observed results.
"""

from __future__ import annotations

import argparse
import json
import math
import random
import statistics
import sys
from pathlib import Path
from typing import Any


EXPECTED_RUN_ID = "ol-rt1-knee-confirm-20260829-a"
LOW_RATE = 41
HIGH_RATE = 42
BLOCKS = 5
BOOTSTRAP_RESAMPLES = 100_000
BOOTSTRAP_SEED = 20_260_829
CLEAN_P99_LIMIT_US = 35_363.0

EXPECTED_ORDER = (
    (1, 1, 41),
    (2, 1, 42),
    (3, 2, 42),
    (4, 2, 41),
    (5, 3, 41),
    (6, 3, 42),
    (7, 4, 42),
    (8, 4, 41),
    (9, 5, 41),
    (10, 5, 42),
)


class ValidationError(RuntimeError):
    """The post-run summary cannot represent the preregistered study."""


def _required(document: dict[str, Any], key: str, path: str) -> Any:
    if key not in document:
        raise ValidationError(f"missing required field {path}.{key}")
    return document[key]


def _mapping(value: Any, path: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValidationError(f"{path} must be an object")
    return value


def _list(value: Any, path: str) -> list[Any]:
    if not isinstance(value, list):
        raise ValidationError(f"{path} must be an array")
    return value


def _boolean(value: Any, path: str) -> bool:
    if not isinstance(value, bool):
        raise ValidationError(f"{path} must be boolean")
    return value


def _integer(value: Any, path: str, minimum: int | None = None) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValidationError(f"{path} must be an integer")
    if minimum is not None and value < minimum:
        raise ValidationError(f"{path} must be at least {minimum}")
    return value


def _number(
    value: Any,
    path: str,
    minimum: float | None = None,
    maximum: float | None = None,
) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValidationError(f"{path} must be numeric")
    result = float(value)
    if not math.isfinite(result):
        raise ValidationError(f"{path} must be finite")
    if minimum is not None and result < minimum:
        raise ValidationError(f"{path} must be at least {minimum}")
    if maximum is not None and result > maximum:
        raise ValidationError(f"{path} must be at most {maximum}")
    return result


def _string(value: Any, path: str) -> str:
    if not isinstance(value, str) or not value:
        raise ValidationError(f"{path} must be a non-empty string")
    return value


def _rate(value: Any, path: str) -> int:
    if isinstance(value, bool):
        raise ValidationError(f"{path} must be a 41 or 42 RPS value")
    try:
        numeric = float(value)
    except (TypeError, ValueError) as error:
        raise ValidationError(f"{path} must be a 41 or 42 RPS value") from error
    if not math.isfinite(numeric) or numeric not in (41.0, 42.0):
        raise ValidationError(f"{path} must be exactly 41 or 42 RPS")
    return int(numeric)


def _close(left: float, right: float) -> bool:
    return math.isclose(left, right, rel_tol=1e-12, abs_tol=1e-12)


def _sample_cv(values: list[float]) -> float:
    """Sample coefficient of variation (standard deviation uses ddof=1)."""
    if len(values) < 2:
        raise ValidationError("sample CV requires at least two observations")
    mean = statistics.fmean(values)
    if mean <= 0:
        raise ValidationError("sample CV requires a positive mean")
    return statistics.stdev(values) / mean


def _percentile(values: list[float], probability: float) -> float:
    """R-7/NumPy-linear empirical percentile."""
    if not values:
        raise ValidationError("percentile requires observations")
    ordered = sorted(values)
    position = (len(ordered) - 1) * probability
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    fraction = position - lower
    return ordered[lower] + (ordered[upper] - ordered[lower]) * fraction


def _bootstrap_median_effects(effects: list[dict[str, float]]) -> dict[str, Any]:
    """Resample complete paired-effect vectors, never individual outcomes."""
    if len(effects) != BLOCKS:
        raise ValidationError(f"bootstrap requires exactly {BLOCKS} paired blocks")
    names = ("delta_success", "delta_drain", "latency_ratio", "marginal_useful_rps")
    samples: dict[str, list[float]] = {name: [] for name in names}
    rng = random.Random(BOOTSTRAP_SEED)

    # Each draw selects five complete paired blocks.  With an odd sample size,
    # the median is the third ordered value; this avoids dependency-specific
    # quantile behavior inside the bootstrap loop.
    for _ in range(BOOTSTRAP_RESAMPLES):
        indices = [rng.randrange(BLOCKS) for _ in range(BLOCKS)]
        for name in names:
            selected = sorted(effects[index][name] for index in indices)
            samples[name].append(selected[BLOCKS // 2])

    intervals: dict[str, dict[str, float]] = {}
    for name in names:
        intervals[name] = {
            "median_effect": statistics.median(effect[name] for effect in effects),
            "lower_95": _percentile(samples[name], 0.025),
            "upper_95": _percentile(samples[name], 0.975),
        }

    checks = {
        "delta_success_upper_below_zero": intervals["delta_success"]["upper_95"] < 0,
        "delta_drain_lower_above_zero": intervals["delta_drain"]["lower_95"] > 0,
        "latency_ratio_lower_above_1_25": intervals["latency_ratio"]["lower_95"] > 1.25,
        "marginal_useful_upper_below_one": intervals["marginal_useful_rps"]["upper_95"] < 1,
    }
    return {
        "resamples": BOOTSTRAP_RESAMPLES,
        "seed": BOOTSTRAP_SEED,
        "resampling_unit": "whole paired repetition block",
        "statistic": "median paired effect",
        "interval": "95% percentile, R-7 linear empirical quantile",
        "effects": intervals,
        "checks": checks,
        "passed": all(checks.values()),
    }


def _parse_cell(raw: Any, rate: int, index: int) -> dict[str, Any]:
    path = f"rates[{rate}].cells[{index}]"
    cell = _mapping(raw, path)
    order = _integer(_required(cell, "order", path), f"{path}.order", 1)
    repetition = _integer(_required(cell, "repetition", path), f"{path}.repetition", 1)
    run_id = _string(_required(cell, "run_id", path), f"{path}.run_id")
    recorded_rate = _rate(
        _required(cell, "offered_rps_per_target", path),
        f"{path}.offered_rps_per_target",
    )
    if recorded_rate != rate:
        raise ValidationError(f"{path} is grouped under {rate} RPS but records {recorded_rate} RPS")

    aggregate_offered = _number(
        _required(cell, "aggregate_offered_rps", path),
        f"{path}.aggregate_offered_rps",
        0,
    )
    if not _close(aggregate_offered, float(rate)):
        raise ValidationError(f"{path}.aggregate_offered_rps must equal {rate} for one replica")
    useful = _number(
        _required(cell, "aggregate_useful_rps", path),
        f"{path}.aggregate_useful_rps",
        0,
        aggregate_offered,
    )
    success = _number(
        _required(cell, "offered_success_ratio", path),
        f"{path}.offered_success_ratio",
        0,
        1,
    )
    acceptance = _number(
        _required(cell, "offered_acceptance_ratio", path),
        f"{path}.offered_acceptance_ratio",
        0,
        1,
    )
    errors = _integer(
        _required(cell, "error_completed_within_plateau", path),
        f"{path}.error_completed_within_plateau",
        0,
    )
    in_flight_drops = _integer(
        _required(cell, "dropped_in_flight_limit", path),
        f"{path}.dropped_in_flight_limit",
        0,
    )
    schedule_drops = _integer(
        _required(cell, "dropped_schedule_late", path),
        f"{path}.dropped_schedule_late",
        0,
    )
    drained = _integer(
        _required(cell, "drained_after_plateau", path),
        f"{path}.drained_after_plateau",
        0,
    )
    drain_ratio = _number(
        _required(cell, "drain_ratio", path), f"{path}.drain_ratio", 0, 1
    )
    in_flight_drop_ratio = _number(
        _required(cell, "in_flight_drop_ratio", path),
        f"{path}.in_flight_drop_ratio",
        0,
        1,
    )
    schedule_drop_ratio = _number(
        _required(cell, "schedule_drop_ratio", path),
        f"{path}.schedule_drop_ratio",
        0,
        1,
    )
    expected_slots = rate * 180
    ratio_counts = (
        (drained, drain_ratio, "drain_ratio"),
        (in_flight_drops, in_flight_drop_ratio, "in_flight_drop_ratio"),
        (schedule_drops, schedule_drop_ratio, "schedule_drop_ratio"),
    )
    for count, ratio, label in ratio_counts:
        if not _close(ratio, count / expected_slots):
            raise ValidationError(f"{path}.{label} does not match its count over {expected_slots} slots")

    raw_latency = _required(cell, "latency_us", path)
    if raw_latency is None:
        # No successful within-plateau completions is a retained SC outcome,
        # not malformed evidence.  It makes the p99-dependent rules fail.
        latency_samples = 0
        p99 = None
    else:
        latency = _mapping(raw_latency, f"{path}.latency_us")
        latency_samples = _integer(
            _required(latency, "samples", f"{path}.latency_us"),
            f"{path}.latency_us.samples",
            0,
        )
        raw_p99 = _required(latency, "p99", f"{path}.latency_us")
        p99 = None if raw_p99 is None else _number(raw_p99, f"{path}.latency_us.p99", 0)
    if latency_samples == 0 and p99 is not None:
        raise ValidationError(f"{path}.latency_us.p99 must be null when samples is zero")
    if latency_samples > 0 and (p99 is None or p99 <= 0):
        raise ValidationError(f"{path}.latency_us.p99 must be positive when samples exist")

    dispatch = _mapping(_required(cell, "dispatch_lag_us", path), f"{path}.dispatch_lag_us")
    dispatch_p99 = _number(
        _required(dispatch, "p99", f"{path}.dispatch_lag_us"),
        f"{path}.dispatch_lag_us.p99",
        0,
    )
    health = _integer(
        _required(cell, "health_event_violations", path),
        f"{path}.health_event_violations",
        0,
    )
    scheduler_valid = _boolean(
        _required(cell, "scheduler_valid", path), f"{path}.scheduler_valid"
    )
    scheduler_reasons = _list(
        _required(cell, "scheduler_invalid_reasons", path),
        f"{path}.scheduler_invalid_reasons",
    )
    if not all(isinstance(item, str) for item in scheduler_reasons):
        raise ValidationError(f"{path}.scheduler_invalid_reasons must contain only strings")

    telemetry = _mapping(_required(cell, "telemetry", path), f"{path}.telemetry")
    telemetry_complete = _boolean(
        _required(telemetry, "required_series_complete", f"{path}.telemetry"),
        f"{path}.telemetry.required_series_complete",
    )
    telemetry_supporting = _boolean(
        _required(telemetry, "supporting_queries_succeeded", f"{path}.telemetry"),
        f"{path}.telemetry.supporting_queries_succeeded",
    )
    telemetry_pods = _integer(
        _required(telemetry, "pod_count", f"{path}.telemetry"),
        f"{path}.telemetry.pod_count",
        0,
    )
    telemetry_nodes = _integer(
        _required(telemetry, "node_count", f"{path}.telemetry"),
        f"{path}.telemetry.node_count",
        0,
    )

    topology = _mapping(
        _required(cell, "topology_preflight", path), f"{path}.topology_preflight"
    )
    topology_required = _boolean(
        _required(topology, "required", f"{path}.topology_preflight"),
        f"{path}.topology_preflight.required",
    )
    topology_attested = _boolean(
        _required(topology, "attested", f"{path}.topology_preflight"),
        f"{path}.topology_preflight.attested",
    )
    topology_authorized = _boolean(
        _required(topology, "load_authorized", f"{path}.topology_preflight"),
        f"{path}.topology_preflight.load_authorized",
    )
    topology_verdict = _string(
        _required(topology, "verdict", f"{path}.topology_preflight"),
        f"{path}.topology_preflight.verdict",
    )
    placement_verdict = _string(
        _required(topology, "placement_verdict", f"{path}.topology_preflight"),
        f"{path}.topology_preflight.placement_verdict",
    )
    identities = _integer(
        _required(topology, "target_identities", f"{path}.topology_preflight"),
        f"{path}.topology_preflight.target_identities",
        0,
    )
    _string(
        _required(topology, "execution_sha256", f"{path}.topology_preflight"),
        f"{path}.topology_preflight.execution_sha256",
    )
    _string(
        _required(topology, "report_sha256", f"{path}.topology_preflight"),
        f"{path}.topology_preflight.report_sha256",
    )

    cpusets = _list(_required(cell, "target_cpusets", path), f"{path}.target_cpusets")
    if len(cpusets) != 1 or not all(isinstance(value, str) and value for value in cpusets):
        raise ValidationError(f"{path}.target_cpusets must contain one non-empty cpuset")
    average_cpu = _number(
        _required(cell, "average_target_cpu_cores", path),
        f"{path}.average_target_cpu_cores",
        0,
    )

    return {
        "order": order,
        "repetition": repetition,
        "run_id": run_id,
        "rate": rate,
        "aggregate_offered_rps": aggregate_offered,
        "useful_rps": useful,
        "success_ratio": success,
        "acceptance_ratio": acceptance,
        "errors": errors,
        "in_flight_drops": in_flight_drops,
        "schedule_drops": schedule_drops,
        "drained": drained,
        "drain_ratio": drain_ratio,
        "in_flight_drop_ratio": in_flight_drop_ratio,
        "schedule_drop_ratio": schedule_drop_ratio,
        "p99_us": p99,
        "latency_samples": latency_samples,
        "health_violations": health,
        "dispatch_p99_us": dispatch_p99,
        "scheduler_valid": scheduler_valid,
        "scheduler_reasons": scheduler_reasons,
        "telemetry_complete": telemetry_complete,
        "telemetry_supporting": telemetry_supporting,
        "telemetry_pods": telemetry_pods,
        "telemetry_nodes": telemetry_nodes,
        "topology_required": topology_required,
        "topology_attested": topology_attested,
        "topology_authorized": topology_authorized,
        "topology_verdict": topology_verdict,
        "placement_verdict": placement_verdict,
        "target_identities": identities,
        "target_cpusets": cpusets,
        "average_target_cpu_cores": average_cpu,
    }


def _runtime_checks(summary: dict[str, Any]) -> dict[str, bool]:
    runtime = _mapping(_required(summary, "runtime_signature", "summary"), "runtime_signature")
    threads = _mapping(_required(runtime, "runtime_threads", "runtime_signature"), "runtime_signature.runtime_threads")
    resources = _mapping(_required(runtime, "resources", "runtime_signature"), "runtime_signature.resources")
    requests = _mapping(_required(resources, "requests", "runtime_signature.resources"), "runtime_signature.resources.requests")
    limits = _mapping(_required(resources, "limits", "runtime_signature.resources"), "runtime_signature.resources.limits")

    # Presence of these content identities is required even where the frozen
    # brief specifies invariance rather than a literal value.
    for key in ("driver_image", "model_sha256", "tokenizer_sha256"):
        _string(_required(runtime, key, "runtime_signature"), f"runtime_signature.{key}")

    return {
        "run_id": _string(_required(summary, "run_id", "summary"), "run_id") == EXPECTED_RUN_ID,
        "protocol": _string(_required(summary, "protocol", "summary"), "protocol")
        == "deterministic_offered_rate_v1",
        "target_image": _string(_required(runtime, "target_image", "runtime_signature"), "runtime_signature.target_image")
        == "sha256:04323612ce3f73873b4c3ed6e09264e828241537e2c1a4231b43f32e9744d5aa",
        "namespace": _string(_required(runtime, "namespace", "runtime_signature"), "runtime_signature.namespace")
        == "llm-d-sc-scaleout",
        "deployment": _string(_required(runtime, "deployment", "runtime_signature"), "runtime_signature.deployment")
        == "classifier-target",
        "target_node": _string(_required(runtime, "target_node", "runtime_signature"), "runtime_signature.target_node")
        == "gnr2.fm2aihpcsed.com",
        "driver_node": _string(_required(runtime, "driver_node", "runtime_signature"), "runtime_signature.driver_node")
        == "rhgnr1",
        "direct_cross_node_topology": _string(
            _required(runtime, "topology", "runtime_signature"), "runtime_signature.topology"
        )
        == "cross-node-direct-gnr2.fm2aihpcsed.com-from-rhgnr1",
        "one_replica": _integer(_required(runtime, "replicas", "runtime_signature"), "runtime_signature.replicas", 0)
        == 1,
        "one_connection": _integer(
            _required(runtime, "connections_per_target", "runtime_signature"),
            "runtime_signature.connections_per_target",
            0,
        )
        == 1,
        "open_loop_concurrency_one": _integer(
            _required(runtime, "concurrency_per_target", "runtime_signature"),
            "runtime_signature.concurrency_per_target",
            0,
        )
        == 1,
        "duration_180_seconds": _integer(
            _required(runtime, "duration_seconds", "runtime_signature"),
            "runtime_signature.duration_seconds",
            0,
        )
        == 180,
        "one_inference_worker": _string(
            _required(runtime, "inference_workers", "runtime_signature"),
            "runtime_signature.inference_workers",
        )
        == "1",
        "rayon_one": _string(_required(threads, "rayon", "runtime_signature.runtime_threads"), "runtime_signature.runtime_threads.rayon")
        == "1",
        "candle_unset": _string(_required(threads, "candle", "runtime_signature.runtime_threads"), "runtime_signature.runtime_threads.candle")
        == "unset",
        "guaranteed_qos": _string(_required(runtime, "qos_class", "runtime_signature"), "runtime_signature.qos_class")
        == "Guaranteed",
        "request_cpu_2": _string(_required(requests, "cpu", "runtime_signature.resources.requests"), "runtime_signature.resources.requests.cpu")
        == "2",
        "limit_cpu_2": _string(_required(limits, "cpu", "runtime_signature.resources.limits"), "runtime_signature.resources.limits.cpu")
        == "2",
        "request_memory_4gi": _string(
            _required(requests, "memory", "runtime_signature.resources.requests"),
            "runtime_signature.resources.requests.memory",
        )
        == "4Gi",
        "limit_memory_4gi": _string(
            _required(limits, "memory", "runtime_signature.resources.limits"),
            "runtime_signature.resources.limits.memory",
        )
        == "4Gi",
    }


def _extract(summary_raw: Any) -> tuple[dict[int, dict[int, dict[str, Any]]], dict[str, Any]]:
    summary = _mapping(summary_raw, "summary")
    if _integer(_required(summary, "schema_version", "summary"), "schema_version") != 2:
        raise ValidationError("summary.schema_version must be 2")

    planned = _integer(_required(summary, "planned_cells", "summary"), "planned_cells", 0)
    attested = _integer(_required(summary, "attested_cells", "summary"), "attested_cells", 0)
    accounting = _boolean(_required(summary, "all_accounting_valid", "summary"), "all_accounting_valid")
    scheduler = _boolean(
        _required(summary, "all_scheduler_attribution_valid", "summary"),
        "all_scheduler_attribution_valid",
    )
    telemetry_required = _boolean(_required(summary, "telemetry_required", "summary"), "telemetry_required")
    telemetry_valid = _boolean(
        _required(summary, "all_required_telemetry_valid", "summary"),
        "all_required_telemetry_valid",
    )
    topology_valid = _boolean(
        _required(summary, "all_required_topology_preflights_valid", "summary"),
        "all_required_topology_preflights_valid",
    )
    topology_config = _mapping(
        _required(summary, "topology_preflight", "summary"), "topology_preflight"
    )
    topology_required = _boolean(
        _required(topology_config, "required", "topology_preflight"),
        "topology_preflight.required",
    )

    source = _mapping(_required(summary, "source_attestation", "summary"), "source_attestation")
    source_linked = _boolean(
        _required(source, "runtime_source_linkage_attested", "source_attestation"),
        "source_attestation.runtime_source_linkage_attested",
    )
    build_sha = _string(
        _required(source, "driver_build_source_sha256", "source_attestation"),
        "source_attestation.driver_build_source_sha256",
    )
    local_sha = _string(
        _required(source, "local_probe_source_sha256", "source_attestation"),
        "source_attestation.local_probe_source_sha256",
    )

    scheduler_thresholds = _mapping(
        _required(summary, "scheduler_attribution_thresholds", "summary"),
        "scheduler_attribution_thresholds",
    )
    max_dispatch_ms = _number(
        _required(scheduler_thresholds, "max_dispatch_p99_lag_ms", "scheduler_attribution_thresholds"),
        "scheduler_attribution_thresholds.max_dispatch_p99_lag_ms",
        0,
    )
    max_schedule_drop = _number(
        _required(scheduler_thresholds, "max_schedule_drop_ratio", "scheduler_attribution_thresholds"),
        "scheduler_attribution_thresholds.max_schedule_drop_ratio",
        0,
        1,
    )

    runtime = _runtime_checks(summary)
    raw_rates = _list(_required(summary, "rates", "summary"), "rates")
    if len(raw_rates) != 2:
        raise ValidationError("summary.rates must contain exactly the 41- and 42-RPS points")

    by_rate: dict[int, dict[int, dict[str, Any]]] = {}
    rate_level_checks: list[bool] = []
    for rate_index, raw_point in enumerate(raw_rates):
        point_path = f"rates[{rate_index}]"
        point = _mapping(raw_point, point_path)
        numeric_rate = _rate(
            _required(point, "offered_rps_per_target_numeric", point_path),
            f"{point_path}.offered_rps_per_target_numeric",
        )
        text_rate = _rate(
            _required(point, "offered_rps_per_target", point_path),
            f"{point_path}.offered_rps_per_target",
        )
        if numeric_rate != text_rate:
            raise ValidationError(f"{point_path} numeric and text rates disagree")
        if numeric_rate in by_rate:
            raise ValidationError(f"duplicate {numeric_rate}-RPS rate point")
        repetitions = _integer(_required(point, "repetitions", point_path), f"{point_path}.repetitions", 0)
        scheduler_repetitions = _integer(
            _required(point, "scheduler_valid_repetitions", point_path),
            f"{point_path}.scheduler_valid_repetitions",
            0,
        )
        all_scheduler = _boolean(
            _required(point, "all_scheduler_valid", point_path), f"{point_path}.all_scheduler_valid"
        )
        raw_cells = _list(_required(point, "cells", point_path), f"{point_path}.cells")
        if len(raw_cells) != BLOCKS:
            raise ValidationError(f"{point_path}.cells must contain exactly {BLOCKS} cells")
        cells = [_parse_cell(raw, numeric_rate, index) for index, raw in enumerate(raw_cells)]
        repetitions_seen = [cell["repetition"] for cell in cells]
        if sorted(repetitions_seen) != list(range(1, BLOCKS + 1)):
            raise ValidationError(f"{point_path} must contain repetitions 1 through {BLOCKS} exactly once")
        by_rate[numeric_rate] = {cell["repetition"]: cell for cell in cells}
        rate_level_checks.extend((repetitions == BLOCKS, scheduler_repetitions == BLOCKS, all_scheduler))

    if set(by_rate) != {LOW_RATE, HIGH_RATE}:
        raise ValidationError("summary must contain exactly 41- and 42-RPS rate points")

    all_cells = [cell for cells in by_rate.values() for cell in cells.values()]
    orders = [cell["order"] for cell in all_cells]
    if sorted(orders) != list(range(1, 2 * BLOCKS + 1)):
        raise ValidationError("cell orders must be unique and contiguous from 1 through 10")

    expected_order_pass = tuple(
        (cell["order"], cell["repetition"], cell["rate"])
        for cell in sorted(all_cells, key=lambda item: item["order"])
    ) == EXPECTED_ORDER
    pairs_adjacent = all(
        abs(by_rate[LOW_RATE][block]["order"] - by_rate[HIGH_RATE][block]["order"]) == 1
        for block in range(1, BLOCKS + 1)
    )
    cell_scheduler = all(
        cell["scheduler_valid"]
        and not cell["scheduler_reasons"]
        and cell["schedule_drops"] == 0
        and cell["schedule_drop_ratio"] == 0
        and cell["dispatch_p99_us"] <= 5_000
        for cell in all_cells
    )
    cell_telemetry = all(
        cell["telemetry_complete"]
        and cell["telemetry_supporting"]
        and cell["telemetry_pods"] == 1
        and cell["telemetry_nodes"] == 2
        for cell in all_cells
    )
    cell_topology_identity = all(
        cell["topology_required"]
        and cell["topology_attested"]
        and cell["topology_authorized"]
        and cell["topology_verdict"] == "PASS"
        and cell["placement_verdict"] == "PASS"
        and cell["target_identities"] == 1
        for cell in all_cells
    )
    cell_cgroup_health_attribution = all(
        len(cell["target_cpusets"]) == 1
        and cell["average_target_cpu_cores"] >= 0
        and cell["health_violations"] >= 0
        for cell in all_cells
    )

    external_checks = {
        "ten_planned_and_attested_cells": planned == 10 and attested == 10,
        "accounting": accounting,
        "source_runtime_linkage": source_linked and build_sha == local_sha,
        "scheduler_attribution": scheduler
        and cell_scheduler
        and all(rate_level_checks)
        and _close(max_dispatch_ms, 5.0)
        and _close(max_schedule_drop, 0.0),
        "telemetry_and_zero_restart_attestation": telemetry_required and telemetry_valid and cell_telemetry,
        "topology_and_identity": topology_required and topology_valid and cell_topology_identity,
        "cgroup_and_health_attribution": cell_cgroup_health_attribution,
        "frozen_runtime_shape": all(runtime.values()),
        "frozen_randomized_order": expected_order_pass,
        "adjacent_pairing_by_repetition": pairs_adjacent,
    }
    validation = {
        "structure_valid": True,
        "pairing_method": "join rates 41 and 42 on repetition; require adjacent order positions",
        "expected_order": [
            {"order": order, "repetition": repetition, "offered_rps": rate}
            for order, repetition, rate in EXPECTED_ORDER
        ],
        "checks": external_checks,
        "all_external_attribution_and_protocol_gates_passed": all(external_checks.values()),
    }
    return by_rate, validation


def _cell_observation(cell: dict[str, Any]) -> dict[str, Any]:
    return {
        "order": cell["order"],
        "run_id": cell["run_id"],
        "success_ratio": cell["success_ratio"],
        "drain_ratio": cell["drain_ratio"],
        "drained_after_plateau": cell["drained"],
        "service_errors": cell["errors"],
        "in_flight_limit_drops": cell["in_flight_drops"],
        "health_event_violations": cell["health_violations"],
        "successful_completion_p99_us": cell["p99_us"],
        "useful_rps": cell["useful_rps"],
    }


def analyze(summary_raw: Any) -> dict[str, Any]:
    """Return a deterministic JSON-compatible confirmation decision."""
    claim = (
        "The service/SLO knee is confirmed between 41 and 42 offered RPS per Pod "
        "for the unchanged W1/RT1, 64-token unique-miss, direct-Pod-IP shape over "
        "a 180-second horizon."
    )
    try:
        by_rate, validation = _extract(summary_raw)
    except ValidationError as error:
        return {
            "schema_version": 1,
            "decision": {
                "status": "inconclusive",
                "claim": None,
                "reasons": [f"input validation failed: {error}"],
                "full_study_rerun_required": True,
            },
            "validation": {
                "structure_valid": False,
                "all_external_attribution_and_protocol_gates_passed": False,
                "reasons": [str(error)],
            },
        }

    blocks: list[dict[str, Any]] = []
    effect_vectors: list[dict[str, float]] = []
    latency_effects_evaluable = True
    for block in range(1, BLOCKS + 1):
        low = by_rate[LOW_RATE][block]
        high = by_rate[HIGH_RATE][block]
        low_checks = {
            "success_at_least_0_99": low["success_ratio"] >= 0.99,
            "drain_at_most_0_01": low["drain_ratio"] <= 0.01,
            "zero_service_errors": low["errors"] == 0,
            "zero_in_flight_limit_drops": low["in_flight_drops"] == 0,
            "zero_health_violations": low["health_violations"] == 0,
            # The primary summarizer only emits a telemetry-attested cell after
            # verifying that every restart sample is zero.
            "zero_restarts_attested": low["telemetry_complete"]
            and low["telemetry_supporting"],
            "p99_at_most_35_363_ms": low["p99_us"] is not None
            and low["p99_us"] <= CLEAN_P99_LIMIT_US,
        }
        # ``error_completed_within_plateau`` contains service shedding such as
        # RESOURCE_EXHAUSTED.  Driver in-flight-limit drops are reported, but
        # are not relabeled as service shedding here.
        shedding_or_error = high["errors"] > 0
        stress_signal = (
            high["success_ratio"] < 0.99
            or high["drain_ratio"] > 0.01
            or shedding_or_error
        )
        latency_ratio = None
        if low["p99_us"] is not None and high["p99_us"] is not None and low["p99_us"] > 0:
            latency_ratio = high["p99_us"] / low["p99_us"]
        else:
            latency_effects_evaluable = False
        high_checks = {
            "stress_signal": stress_signal,
            "paired_p99_ratio_above_1_25": latency_ratio is not None and latency_ratio > 1.25,
        }
        effects = {
            "delta_success": high["success_ratio"] - low["success_ratio"],
            "delta_drain": high["drain_ratio"] - low["drain_ratio"],
            "latency_ratio": latency_ratio,
            "marginal_useful_rps": high["useful_rps"] - low["useful_rps"],
        }
        direction_checks = {
            "delta_success_below_zero": effects["delta_success"] < 0,
            "delta_drain_above_zero": effects["delta_drain"] > 0,
            "latency_ratio_above_one": latency_ratio is not None and latency_ratio > 1,
            "marginal_useful_below_one": effects["marginal_useful_rps"] < 1,
        }
        blocks.append(
            {
                "block": block,
                "rate_41": {
                    "observation": _cell_observation(low),
                    "clean_checks": low_checks,
                    "clean": all(low_checks.values()),
                },
                "rate_42": {
                    "observation": _cell_observation(high),
                    "stress_checks": high_checks,
                    "stressed": all(high_checks.values()),
                },
                "paired_effects": effects,
                "direction_checks": direction_checks,
                "all_directions_agree": all(direction_checks.values()),
            }
        )
        if latency_ratio is not None:
            effect_vectors.append({key: float(value) for key, value in effects.items()})

    useful_41 = [by_rate[LOW_RATE][block]["useful_rps"] for block in range(1, BLOCKS + 1)]
    useful_42 = [by_rate[HIGH_RATE][block]["useful_rps"] for block in range(1, BLOCKS + 1)]
    p99_41 = [by_rate[LOW_RATE][block]["p99_us"] for block in range(1, BLOCKS + 1)]
    p99_42 = [by_rate[HIGH_RATE][block]["p99_us"] for block in range(1, BLOCKS + 1)]
    cv_evaluable = all(value is not None for value in p99_41 + p99_42)
    if cv_evaluable:
        try:
            cv_values = {
                "useful_rps_41": _sample_cv(useful_41),
                "useful_rps_42": _sample_cv(useful_42),
                "p99_41": _sample_cv([float(value) for value in p99_41]),
                "p99_42": _sample_cv([float(value) for value in p99_42]),
            }
        except ValidationError:
            cv_evaluable = False
            cv_values = {}
            cv_checks = {"positive_means_and_complete_latency_populations": False}
        else:
            cv_checks = {
                "useful_rps_41_at_most_0_02": cv_values["useful_rps_41"] <= 0.02,
                "useful_rps_42_at_most_0_02": cv_values["useful_rps_42"] <= 0.02,
                "p99_41_at_most_0_10": cv_values["p99_41"] <= 0.10,
                "p99_42_at_most_0_20": cv_values["p99_42"] <= 0.20,
            }
    else:
        cv_values = {}
        cv_checks = {"successful_latency_population_present_in_all_cells": False}
    variability = {
        "definition": "sample standard deviation (ddof=1) divided by arithmetic mean",
        "drain_and_error_cv_intentionally_omitted": True,
        "values": cv_values,
        "checks": cv_checks,
        "passed": cv_evaluable and all(cv_checks.values()),
    }

    bootstrap = (
        _bootstrap_median_effects(effect_vectors)
        if latency_effects_evaluable and len(effect_vectors) == BLOCKS
        else {
            "resamples": BOOTSTRAP_RESAMPLES,
            "seed": BOOTSTRAP_SEED,
            "resampling_unit": "whole paired repetition block",
            "passed": False,
            "reason": "one or more paired successful-latency effects is unavailable",
        }
    )

    condition_checks = {
        "all_10_cells_attributed": validation[
            "all_external_attribution_and_protocol_gates_passed"
        ],
        "rate_41_clean_in_all_five_blocks": all(block["rate_41"]["clean"] for block in blocks),
        "rate_42_stressed_in_all_five_blocks": all(block["rate_42"]["stressed"] for block in blocks),
        "all_five_paired_directions_agree": all(
            block["all_directions_agree"] for block in blocks
        ),
        "paired_bootstrap_intervals_pass": bootstrap["passed"],
        "sample_cv_limits_pass": variability["passed"],
    }
    reasons: list[str] = []
    failed_external = [
        name for name, passed in validation["checks"].items() if not passed
    ]
    if failed_external:
        reasons.append(
            "external attribution or frozen-protocol gates failed: "
            + ", ".join(failed_external)
        )
    failed_low = [block["block"] for block in blocks if not block["rate_41"]["clean"]]
    if failed_low:
        reasons.append(f"41 RPS was not clean in every block; failed blocks: {failed_low}")
    failed_high = [block["block"] for block in blocks if not block["rate_42"]["stressed"]]
    if failed_high:
        reasons.append(f"42 RPS was not stressed in every block; failed blocks: {failed_high}")
    failed_directions = [block["block"] for block in blocks if not block["all_directions_agree"]]
    if failed_directions:
        reasons.append(f"paired directions did not all agree; failed blocks: {failed_directions}")
    if not bootstrap["passed"]:
        failed = [name for name, passed in bootstrap.get("checks", {}).items() if not passed]
        reasons.append(
            "paired whole-block bootstrap did not pass"
            + (f": {', '.join(failed)}" if failed else "")
        )
    if not variability["passed"]:
        failed = [name for name, passed in cv_checks.items() if not passed]
        reasons.append("sample CV limits did not pass: " + ", ".join(failed))

    confirmed = all(condition_checks.values())
    if confirmed:
        reasons = ["all six preregistered confirmation conditions passed"]
    return {
        "schema_version": 1,
        "preregistration": {
            "run_id": EXPECTED_RUN_ID,
            "lower_clean_rate_rps_per_pod": LOW_RATE,
            "upper_stress_rate_rps_per_pod": HIGH_RATE,
            "blocks": BLOCKS,
            "clean_p99_reference_ms": 28.290,
            "clean_p99_limit_ms": CLEAN_P99_LIMIT_US / 1_000,
        },
        "decision": {
            "status": "confirmed" if confirmed else "inconclusive",
            "claim": claim if confirmed else None,
            "reasons": reasons,
            "full_study_rerun_required": not validation[
                "all_external_attribution_and_protocol_gates_passed"
            ],
        },
        "validation": validation,
        "blocks": blocks,
        "variability": variability,
        "bootstrap": bootstrap,
        "conditions": condition_checks,
        "interpretation_limit": (
            "This decision does not establish an absolute throughput ceiling, "
            "same-Pod recovery, ClusterIP behavior, or horizontal 20-50 replica scaling."
        ),
    }


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Apply the frozen five-block 41/42-RPS knee confirmation rule"
    )
    parser.add_argument("--summary", type=Path, required=True, help="schema-v2 sweep-summary JSON")
    parser.add_argument("--output-json", type=Path, help="optional decision artifact path")
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    try:
        with args.summary.open(encoding="utf-8") as handle:
            source = json.load(handle)
    except (OSError, json.JSONDecodeError) as error:
        print(f"ERROR: cannot read summary {args.summary}: {error}", file=sys.stderr)
        return 2
    result = analyze(source)
    rendered = json.dumps(result, indent=2, sort_keys=True) + "\n"
    if args.output_json is not None:
        args.output_json.parent.mkdir(parents=True, exist_ok=True)
        args.output_json.write_text(rendered, encoding="utf-8")
    sys.stdout.write(rendered)
    return 0 if result["decision"]["status"] == "confirmed" else 3


if __name__ == "__main__":
    raise SystemExit(main())
