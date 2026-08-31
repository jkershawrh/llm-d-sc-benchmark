#!/usr/bin/env python3
"""Validate and summarize an Arena SC deterministic open-loop sweep.

This program is deliberately read-only.  It treats corpus/accounting/runtime
disagreements as harness errors, keeps load-generator schedule quality separate
from SC saturation signals, and emits a conservative knee bracket rather than
claiming a precise capacity point from a single sweep.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import statistics
import sys
from collections import defaultdict
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Iterable


class ValidationError(RuntimeError):
    pass


def load_json(path: Path) -> Any:
    try:
        with path.open(encoding="utf-8") as handle:
            return json.load(handle)
    except (OSError, json.JSONDecodeError) as error:
        raise ValidationError(f"cannot read JSON {path}: {error}") from error


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    try:
        with path.open("rb") as handle:
            for block in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(block)
    except OSError as error:
        raise ValidationError(f"cannot hash {path}: {error}") from error
    return digest.hexdigest()


def decimal_rate(source: str) -> Decimal:
    try:
        value = Decimal(source)
    except InvalidOperation as error:
        raise ValidationError(f"invalid offered rate {source!r}") from error
    if not value.is_finite() or value <= 0:
        raise ValidationError(f"offered rate must be finite and positive: {source!r}")
    return value


def require(condition: bool, message: str) -> None:
    if not condition:
        raise ValidationError(message)


def median(values: Iterable[float]) -> float | None:
    samples = list(values)
    return statistics.median(samples) if samples else None


def cv(values: Iterable[float]) -> float | None:
    samples = list(values)
    if len(samples) < 2:
        return None
    mean = statistics.fmean(samples)
    return statistics.pstdev(samples) / mean if mean else None


def close(left: float, right: float, tolerance: float = 1e-9) -> bool:
    return math.isclose(left, right, rel_tol=tolerance, abs_tol=tolerance)


def series_window(
    document: dict[str, Any],
    label: str,
    name: str,
    start: float,
    end: float,
) -> list[tuple[float, float]]:
    matches = [
        series
        for series in document.get("data", {}).get("result", [])
        if series.get("metric", {}).get(label) == name
    ]
    require(len(matches) == 1, f"telemetry {label}={name}: expected one series, found {len(matches)}")
    values: list[tuple[float, float]] = []
    for raw_time, raw_value in matches[0].get("values", []):
        timestamp = float(raw_time)
        if start <= timestamp <= end:
            values.append((timestamp, float(raw_value)))
    return values


def validate_complete_series(
    document: dict[str, Any],
    metric: str,
    label: str,
    names: list[str],
    start: float,
    end: float,
    max_gap: float,
    expected_value: float | None = None,
) -> None:
    require(document.get("status") == "success", f"{metric}: telemetry query did not succeed")
    for name in names:
        values = series_window(document, label, name, start, end)
        require(values, f"{metric}: no plateau samples for {name}")
        require(values[0][0] - start <= max_gap, f"{metric}: late first sample for {name}")
        require(end - values[-1][0] <= max_gap, f"{metric}: early last sample for {name}")
        gaps = [right[0] - left[0] for left, right in zip(values, values[1:])]
        require(max(gaps, default=0.0) <= max_gap, f"{metric}: sample gap exceeds {max_gap}s for {name}")
        if expected_value is not None:
            require(
                all(close(value, expected_value) for _, value in values),
                f"{metric}: unexpected value for {name}; expected {expected_value}",
            )


def validate_telemetry(cell_dir: Path, summary: dict[str, Any], max_gap: float) -> dict[str, Any]:
    targets = load_json(cell_dir / "targets-before.json")
    pod_names = sorted(item["metadata"]["name"] for item in targets.get("items", []))
    replicas = summary["cell"]["replicas"]
    require(len(pod_names) == replicas, f"{cell_dir.name}: target telemetry pod count mismatch")

    start = summary["cell"]["start_epoch_ms"] / 1000
    end = start + summary["cell"]["duration_seconds"]
    required = (
        ("pod_cpu_otel", "k8s_pod_name", pod_names, None),
        ("container_cpu_otel", "k8s_pod_name", pod_names, None),
        ("memory_working_set", "pod", pod_names, None),
        ("restarts", "pod", pod_names, 0.0),
        ("pod_ready", "pod", pod_names, 1.0),
    )
    for metric, label, names, expected in required:
        document = load_json(cell_dir / "metrics" / f"{metric}.json")
        validate_complete_series(document, metric, label, names, start, end, max_gap, expected)

    nodes = sorted({summary["cell"]["target_node"], summary["cell"]["driver_node"]})
    node_document = load_json(cell_dir / "metrics" / "node_ready.json")
    validate_complete_series(
        node_document, "node_ready", "node", nodes, start, end, max_gap, 1.0
    )

    supporting = ("container_cpu_cadvisor", "throttle_ratio", "cpu_pressure_waiting")
    for metric in supporting:
        document = load_json(cell_dir / "metrics" / f"{metric}.json")
        require(document.get("status") == "success", f"{metric}: supporting query failed")

    cgroups = summary.get("cgroup_cpu")
    require(isinstance(cgroups, list) and len(cgroups) == replicas, f"{cell_dir.name}: cgroup coverage mismatch")
    for cgroup in cgroups:
        require(
            cgroup.get("cpuset_cpus_effective", {}).get("start")
            == cgroup.get("cpuset_cpus_effective", {}).get("end"),
            f"{cell_dir.name}: cpuset changed for {cgroup.get('pod')}",
        )
        require(
            cgroup.get("cpu_max", {}).get("start") == cgroup.get("cpu_max", {}).get("end"),
            f"{cell_dir.name}: cpu.max changed for {cgroup.get('pod')}",
        )
        require(cgroup.get("usage_usec_delta", -1) >= 0, f"{cell_dir.name}: invalid CPU delta")

    return {
        "required_series_complete": True,
        "pod_count": len(pod_names),
        "node_count": len(nodes),
        "supporting_queries_succeeded": True,
    }


def validate_topology_preflight(
    cell_dir: Path,
    cell: dict[str, Any],
    provenance: dict[str, Any],
) -> dict[str, Any]:
    config = provenance.get("topology_preflight", {})
    required = config.get("required", False)
    require(isinstance(required, bool), "topology preflight required flag must be boolean")
    recorded = cell.get("topology_preflight")
    if not required:
        return {
            "required": False,
            "attested": False,
            "note": "CPU-sibling topology preflight was not required by sweep provenance",
        }

    require(isinstance(recorded, dict), f"{cell_dir.name}: topology preflight metadata missing")
    require(recorded.get("enabled") is True, f"{cell_dir.name}: topology preflight was not enabled")
    require(
        recorded.get("required_by_caller") is True,
        f"{cell_dir.name}: topology preflight was not required by the sweep",
    )
    require(recorded.get("runner_exit_code") == 0, f"{cell_dir.name}: topology runner was non-zero")
    require(recorded.get("report_json_valid") is True, f"{cell_dir.name}: topology report is invalid JSON")
    require(recorded.get("report_gate_valid") is True, f"{cell_dir.name}: topology report gate is invalid")
    require(recorded.get("target_identity_match") is True, f"{cell_dir.name}: topology target identity mismatch")
    require(recorded.get("load_authorized") is True, f"{cell_dir.name}: topology gate denied load")
    require(recorded.get("disposition") == "pass", f"{cell_dir.name}: topology disposition is not pass")
    require(recorded.get("report_verdict") == "PASS", f"{cell_dir.name}: topology verdict is not PASS")
    require(
        recorded.get("placement_verdict") == "PASS",
        f"{cell_dir.name}: topology placement verdict is not PASS",
    )

    execution_path = cell_dir / "topology-preflight-execution.json"
    report_path = cell_dir / "topology-preflight-report.json"
    stdout_path = cell_dir / "topology-preflight-stdout.txt"
    stderr_path = cell_dir / "topology-preflight-stderr.txt"
    execution = load_json(execution_path)
    report = load_json(report_path)
    raw_report = load_json(stdout_path)
    require(execution.get("gate") == "cpu_topology_pre_load", f"{cell_dir.name}: wrong topology gate")
    require(
        execution.get("runner") == config.get("runner"),
        f"{cell_dir.name}: topology runner differs from sweep provenance",
    )
    require(execution.get("runner_exit_code") == 0, f"{cell_dir.name}: saved topology runner exit is non-zero")
    require(execution.get("load_authorized") is True, f"{cell_dir.name}: saved topology execution denied load")
    require(execution.get("target_identity_match") is True, f"{cell_dir.name}: saved topology identity mismatch")
    require(execution.get("disposition") == "pass", f"{cell_dir.name}: saved topology execution is not pass")
    require(report.get("schema_version") == 1, f"{cell_dir.name}: unsupported topology report schema")
    require(report.get("verdict") == "PASS", f"{cell_dir.name}: saved topology report is not PASS")
    require(report.get("placement_verdict") == "PASS", f"{cell_dir.name}: saved placement is not PASS")
    require(report.get("gate_passed") is True, f"{cell_dir.name}: saved topology gate did not pass")
    require(report.get("exit_code") == 0, f"{cell_dir.name}: saved topology report exit is non-zero")
    require(raw_report == report, f"{cell_dir.name}: canonical topology report differs from raw stdout")
    require(
        report.get("snapshot", {}).get("capture", {}).get("mode") == "live-read-only",
        f"{cell_dir.name}: topology report was not captured live/read-only",
    )
    capture = report.get("snapshot", {}).get("capture", {})
    require(capture.get("namespace") == provenance["namespace"], f"{cell_dir.name}: topology namespace mismatch")
    require(capture.get("selector") == config.get("selector"), f"{cell_dir.name}: topology selector mismatch")
    require(capture.get("expected_pods") == provenance["replicas"], f"{cell_dir.name}: topology expected-pod mismatch")

    for key in (
        "runner",
        "runner_exit_code",
        "report_json_valid",
        "report_gate_valid",
        "target_identity_match",
        "load_authorized",
        "disposition",
        "evidence_sha256",
    ):
        require(
            recorded.get(key) == execution.get(key),
            f"{cell_dir.name}: embedded topology {key} differs from execution evidence",
        )

    hashes = execution.get("evidence_sha256", {})
    require(hashes.get("report") == sha256_file(report_path), f"{cell_dir.name}: topology report hash mismatch")
    require(hashes.get("raw_stdout") == sha256_file(stdout_path), f"{cell_dir.name}: topology stdout hash mismatch")
    require(hashes.get("stderr") == sha256_file(stderr_path), f"{cell_dir.name}: topology stderr hash mismatch")
    require(
        recorded.get("execution_sha256") == sha256_file(execution_path),
        f"{cell_dir.name}: topology execution hash mismatch",
    )

    targets = load_json(cell_dir / "targets-before.json")
    expected_identities = sorted(
        (
            item.get("metadata", {}).get("name"),
            item.get("metadata", {}).get("uid"),
            item.get("spec", {}).get("nodeName"),
        )
        for item in targets.get("items", [])
    )
    report_identities = sorted(
        (item.get("name"), item.get("uid"), item.get("node"))
        for item in report.get("pods", [])
    )
    snapshot_identities = sorted(
        (item.get("name"), item.get("uid"), item.get("node"))
        for item in report.get("snapshot", {}).get("pods", [])
    )
    require(
        len(expected_identities) == provenance["replicas"],
        f"{cell_dir.name}: topology identity target count mismatch",
    )
    require(report_identities == expected_identities, f"{cell_dir.name}: topology report identities differ from targets")
    require(snapshot_identities == expected_identities, f"{cell_dir.name}: topology snapshot identities differ from targets")
    report_summary = report.get("summary", {})
    require(report_summary.get("pods") == provenance["replicas"], f"{cell_dir.name}: topology report pod count mismatch")
    require(
        report_summary.get("pods_validated") == provenance["replicas"],
        f"{cell_dir.name}: topology validated-pod count mismatch",
    )
    require(report_summary.get("placement_violations") == 0, f"{cell_dir.name}: topology violations present")
    require(report_summary.get("invalid_reasons") == 0, f"{cell_dir.name}: topology report is invalid")
    require(
        report_summary.get("gate_ineligibility_reasons") == 0,
        f"{cell_dir.name}: topology report is gate-ineligible",
    )
    require(
        recorded.get("report_summary") == report.get("summary"),
        f"{cell_dir.name}: embedded topology summary differs from evidence",
    )
    return {
        "required": True,
        "attested": True,
        "load_authorized": True,
        "verdict": report["verdict"],
        "placement_verdict": report["placement_verdict"],
        "target_identities": len(expected_identities),
        "execution_sha256": recorded["execution_sha256"],
        "report_sha256": hashes["report"],
    }


def runtime_signature(cell: dict[str, Any]) -> dict[str, Any]:
    keys = (
        "namespace",
        "deployment",
        "target_node",
        "driver_node",
        "target_image",
        "driver_image",
        "model_sha256",
        "tokenizer_sha256",
        "topology",
        "inference_workers",
        "runtime_threads",
        "qos_class",
        "resources",
        "replicas",
        "concurrency_per_target",
        "connections_per_target",
        "duration_seconds",
    )
    return {key: cell.get(key) for key in keys}


@dataclass
class CellRecord:
    order: int
    repetition: int
    rate_text: str
    rate: Decimal
    run_id: str
    summary: dict[str, Any]
    scheduler_valid: bool
    scheduler_reasons: list[str]
    telemetry: dict[str, Any] | None
    topology_preflight: dict[str, Any]

    def compact(self) -> dict[str, Any]:
        offered = self.summary["offered_slots"]
        endpoint_rps = self.summary["endpoint_rps"]
        return {
            "order": self.order,
            "repetition": self.repetition,
            "run_id": self.run_id,
            "offered_rps_per_target": self.rate_text,
            "aggregate_offered_rps": self.summary["aggregate_offered_rps"],
            "aggregate_useful_rps": self.summary["aggregate_useful_rps"],
            "offered_success_ratio": self.summary["offered_success_ratio"],
            "offered_acceptance_ratio": self.summary["offered_acceptance_ratio"],
            "error_completed_within_plateau": self.summary["error_completed_within_plateau"],
            "dropped_in_flight_limit": self.summary["dropped_in_flight_limit"],
            "dropped_schedule_late": self.summary["dropped_schedule_late"],
            "drained_after_plateau": self.summary["drained_after_plateau"],
            "in_flight_drop_ratio": self.summary["dropped_in_flight_limit"] / offered,
            "schedule_drop_ratio": self.summary["dropped_schedule_late"] / offered,
            "drain_ratio": self.summary["drained_after_plateau"] / offered,
            "latency_us": self.summary["latency_us"],
            "dispatch_lag_us": self.summary["dispatch_lag_us"],
            "health_event_violations": self.summary["health_event_violations"],
            "endpoint_useful_rps_cv": cv(endpoint_rps),
            "target_cpusets": [
                item["cpuset_cpus_effective"]["start"]
                for item in self.summary["cgroup_cpu"]
            ],
            "average_target_cpu_cores": statistics.fmean(
                item["average_cpu_cores"] for item in self.summary["cgroup_cpu"]
            ),
            "scheduler_valid": self.scheduler_valid,
            "scheduler_invalid_reasons": self.scheduler_reasons,
            "telemetry": self.telemetry,
            "topology_preflight": self.topology_preflight,
        }


def validate_cell(
    row: dict[str, str],
    provenance: dict[str, Any],
    cell_root: Path,
    max_scheduler_p99_lag_us: float,
    max_schedule_drop_ratio: float,
    telemetry_required: bool,
    metric_max_gap_seconds: float,
) -> CellRecord:
    order = int(row["order"])
    repetition = int(row["repetition"])
    rate_text = row["offered_rps_per_target"]
    rate = decimal_rate(rate_text)
    slots = int(row["scheduled_slots_per_target"])
    sequence_base = int(row["sequence_base"])
    run_id = row["run_id"]
    cell_dir = cell_root / run_id
    summary = load_json(cell_dir / "summary.json")
    cell = summary.get("cell", {})

    require(summary.get("load_model") == "open_loop_deterministic_offered_rate", f"{run_id}: wrong load model")
    require(cell.get("run_id") == run_id, f"{run_id}: cell run ID mismatch")
    require(cell.get("open_loop", {}).get("offered_rps_per_target") == rate_text, f"{run_id}: offered rate mismatch")
    require(cell.get("open_loop", {}).get("scheduled_slots_per_target") == slots, f"{run_id}: scheduled slot mismatch")
    require(cell.get("sequence_base") == sequence_base, f"{run_id}: sequence base mismatch")
    require(cell.get("replicas") == provenance["replicas"], f"{run_id}: replica mismatch")
    require(cell.get("duration_seconds") == provenance["duration_seconds"], f"{run_id}: duration mismatch")
    require(cell.get("connections_per_target") == provenance["connections"], f"{run_id}: connection mismatch")
    require(cell.get("concurrency_per_target") == provenance["concurrency"], f"{run_id}: concurrency mismatch")
    require(cell.get("target_image") == provenance["target_image"], f"{run_id}: target image mismatch")
    require(cell.get("driver_image") == provenance["driver_image"], f"{run_id}: driver image mismatch")
    require(cell.get("model_sha256") == provenance["model_sha256"], f"{run_id}: model mismatch")
    require(cell.get("tokenizer_sha256") == provenance["tokenizer_sha256"], f"{run_id}: tokenizer mismatch")
    require(cell.get("namespace") == provenance["namespace"], f"{run_id}: namespace mismatch")
    require(cell.get("deployment") == provenance["deployment"], f"{run_id}: deployment mismatch")
    require(cell.get("target_node") == provenance["target_node"], f"{run_id}: target node mismatch")
    require(cell.get("driver_node") == provenance["driver_node"], f"{run_id}: driver node mismatch")
    require(summary.get("accounting_valid") is True, f"{run_id}: accounting is invalid")
    require(summary.get("workers_late") is False, f"{run_id}: scheduler was not ready by start")
    require(summary.get("corpus_exhausted") is False, f"{run_id}: corpus exhausted")
    topology_preflight = validate_topology_preflight(cell_dir, cell, provenance)

    expected_offered = slots * provenance["replicas"]
    require(summary.get("offered_slots") == expected_offered, f"{run_id}: aggregate offered slots mismatch")
    latency = summary.get("latency_us") or {}
    require(
        latency.get("samples", 0) == summary.get("ok_completed_within_plateau"),
        f"{run_id}: latency population mismatch",
    )
    endpoints = summary.get("endpoint_offered_rps", [])
    require(len(endpoints) == provenance["replicas"], f"{run_id}: endpoint offered-rate coverage mismatch")
    require(all(close(float(value), float(rate)) for value in endpoints), f"{run_id}: endpoint offered rate mismatch")
    require(len(summary.get("endpoint_rps", [])) == provenance["replicas"], f"{run_id}: endpoint throughput coverage mismatch")

    span = provenance["sequence"]["cell_stride"]
    require(
        cell.get("reserved_sequence_end_exclusive") == sequence_base + span,
        f"{run_id}: reserved sequence range mismatch",
    )

    dispatch_p99 = (summary.get("dispatch_lag_us") or {}).get("p99")
    schedule_drop_ratio = summary.get("dropped_schedule_late", 0) / expected_offered
    scheduler_reasons: list[str] = []
    if dispatch_p99 is None:
        scheduler_reasons.append("no initiated dispatch-lag population")
    elif dispatch_p99 > max_scheduler_p99_lag_us:
        scheduler_reasons.append(
            f"dispatch p99 {dispatch_p99}us exceeds {max_scheduler_p99_lag_us:g}us"
        )
    if schedule_drop_ratio > max_schedule_drop_ratio:
        scheduler_reasons.append(
            f"schedule-drop ratio {schedule_drop_ratio:.6f} exceeds {max_schedule_drop_ratio:.6f}"
        )

    telemetry = None
    if telemetry_required:
        telemetry = validate_telemetry(cell_dir, summary, metric_max_gap_seconds)

    return CellRecord(
        order=order,
        repetition=repetition,
        rate_text=rate_text,
        rate=rate,
        run_id=run_id,
        summary=summary,
        scheduler_valid=not scheduler_reasons,
        scheduler_reasons=scheduler_reasons,
        telemetry=telemetry,
        topology_preflight=topology_preflight,
    )


def aggregate_rates(records: list[CellRecord]) -> list[dict[str, Any]]:
    grouped: dict[Decimal, list[CellRecord]] = defaultdict(list)
    for record in records:
        grouped[record.rate].append(record)

    aggregates: list[dict[str, Any]] = []
    for rate in sorted(grouped):
        cells = grouped[rate]
        compact = [cell.compact() for cell in cells]
        useful = [cell["aggregate_useful_rps"] for cell in compact]
        success = [cell["offered_success_ratio"] for cell in compact]
        p50 = [cell["latency_us"]["p50"] for cell in compact if cell["latency_us"]]
        p99 = [cell["latency_us"]["p99"] for cell in compact if cell["latency_us"]]
        in_flight = [cell["in_flight_drop_ratio"] for cell in compact]
        schedule_drop = [cell["schedule_drop_ratio"] for cell in compact]
        drain = [cell["drain_ratio"] for cell in compact]
        aggregate = {
            "offered_rps_per_target": cells[0].rate_text,
            "offered_rps_per_target_numeric": float(rate),
            "repetitions": len(cells),
            "scheduler_valid_repetitions": sum(cell.scheduler_valid for cell in cells),
            "all_scheduler_valid": all(cell.scheduler_valid for cell in cells),
            "median_aggregate_useful_rps": median(useful),
            "useful_rps_cv": cv(useful),
            "median_offered_success_ratio": median(success),
            "median_latency_p50_us": median(p50),
            "median_latency_p99_us": median(p99),
            "latency_p99_cv": cv(p99),
            "median_in_flight_drop_ratio": median(in_flight),
            "max_schedule_drop_ratio": max(schedule_drop),
            "median_drain_ratio": median(drain),
            "total_errors_within_plateau": sum(cell["error_completed_within_plateau"] for cell in compact),
            "total_health_event_violations": sum(cell["health_event_violations"] for cell in compact),
            "median_average_target_cpu_cores": median(
                cell["average_target_cpu_cores"] for cell in compact
            ),
            "max_endpoint_useful_rps_cv": max(
                (cell["endpoint_useful_rps_cv"] for cell in compact
                 if cell["endpoint_useful_rps_cv"] is not None),
                default=None,
            ),
            "observed_target_cpusets": sorted(
                {cpuset for cell in compact for cpuset in cell["target_cpusets"]}
            ),
            "cells": compact,
        }
        aggregates.append(aggregate)
    return aggregates


def infer_knee(aggregates: list[dict[str, Any]]) -> dict[str, Any]:
    invalid_rates = [
        item["offered_rps_per_target"]
        for item in aggregates
        if not item["all_scheduler_valid"]
    ]
    if invalid_rates:
        return {
            "status": "not_evaluable",
            "reason": "one or more offered-rate points lack valid load-generator schedule attribution",
            "invalid_rates_per_target": invalid_rates,
        }
    valid = aggregates
    if len(valid) < 2:
        return {
            "status": "not_evaluable",
            "reason": "fewer than two offered-rate points were attested",
        }
    baseline = valid[0]
    baseline_p99 = baseline["median_latency_p99_us"]
    previous = baseline
    # A percentile at one offered-rate point can move because of a transient
    # host/runtime disturbance even when every request succeeds and useful
    # throughput tracks offered load exactly.  Hold a latency-only signal until
    # the next higher offered-rate point confirms that the elevation persists.
    # Capacity signals (lost throughput, drains, errors, and health events)
    # remain immediate because they are direct evidence that offered work was
    # not served cleanly within the plateau.
    pending_latency: dict[str, Any] | None = None
    for current in valid[1:]:
        capacity_triggers: list[str] = []
        if current["median_offered_success_ratio"] < 0.99:
            capacity_triggers.append("median offered-success ratio below 99%")
        if current["median_in_flight_drop_ratio"] > 0:
            capacity_triggers.append("client in-flight ceiling reached")
        if current["median_drain_ratio"] > 0.01:
            capacity_triggers.append("more than 1% of offered work drained after plateau")
        if current["total_errors_within_plateau"] > 0:
            capacity_triggers.append("service errors observed")
        if current["total_health_event_violations"] > 0:
            capacity_triggers.append("target health event observed")
        current_p99 = current["median_latency_p99_us"]
        if current_p99 is None:
            capacity_triggers.append("no successful within-plateau latency population")
        latency_elevated = (
            current_p99 is not None
            and baseline_p99 is not None
            and current_p99 >= baseline_p99 * 1.25
        )

        useful_delta = (
            current["median_aggregate_useful_rps"]
            - previous["median_aggregate_useful_rps"]
        )
        marginal_efficiency = None
        # Aggregate offered load already includes replicas.  Derive the slope
        # directly from each cell's recorded aggregate offered rate.
        previous_offered = statistics.median(
            cell["aggregate_offered_rps"] for cell in previous["cells"]
        )
        current_offered = statistics.median(
            cell["aggregate_offered_rps"] for cell in current["cells"]
        )
        aggregate_offered_delta = current_offered - previous_offered
        if aggregate_offered_delta > 0:
            marginal_efficiency = useful_delta / aggregate_offered_delta
            if marginal_efficiency < 0.5:
                capacity_triggers.append("marginal useful-throughput gain below 50% of added offer")

        if pending_latency is not None and latency_elevated:
            lower = pending_latency["lower"]
            onset = pending_latency["onset"]
            repetitions = min(
                lower["repetitions"],
                onset["repetitions"],
                current["repetitions"],
            )
            repeatable = (
                repetitions >= 5
                and lower["useful_rps_cv"] is not None
                and onset["useful_rps_cv"] is not None
                and lower["latency_p99_cv"] is not None
                and onset["latency_p99_cv"] is not None
                and current["latency_p99_cv"] is not None
                and lower["useful_rps_cv"] <= 0.10
                and onset["useful_rps_cv"] <= 0.10
                and current["useful_rps_cv"] <= 0.10
                and lower["latency_p99_cv"] <= 0.15
                and onset["latency_p99_cv"] <= 0.15
                and current["latency_p99_cv"] <= 0.15
            )
            return {
                "status": "bracketed",
                "lower_clean_rate_per_target": lower["offered_rps_per_target"],
                "upper_stress_rate_per_target": onset["offered_rps_per_target"],
                "triggers": [
                    "median p99 at least 25% above low-rate baseline",
                    "latency elevation persisted at the next higher offered-rate point",
                ],
                "latency_confirmation_rate_per_target": current["offered_rps_per_target"],
                "marginal_efficiency": pending_latency["marginal_efficiency"],
                "confidence": "repeatable" if repeatable else "exploratory",
                "confirmation_limit": "a promotion claim still requires paired-direction consistency, confidence intervals, and same-Pod recovery evidence",
                "interpretation": "the knee lies within this offered-rate interval; do not report the upper point as sustainable capacity",
            }

        if capacity_triggers:
            triggers = list(capacity_triggers)
            if latency_elevated:
                triggers.append("median p99 at least 25% above low-rate baseline")
            repetitions = min(previous["repetitions"], current["repetitions"])
            repeatable = (
                repetitions >= 5
                and previous["useful_rps_cv"] is not None
                and current["useful_rps_cv"] is not None
                and previous["latency_p99_cv"] is not None
                and current["latency_p99_cv"] is not None
                and previous["useful_rps_cv"] <= 0.10
                and current["useful_rps_cv"] <= 0.10
                and previous["latency_p99_cv"] <= 0.15
                and current["latency_p99_cv"] <= 0.15
            )
            return {
                "status": "bracketed",
                "lower_clean_rate_per_target": previous["offered_rps_per_target"],
                "upper_stress_rate_per_target": current["offered_rps_per_target"],
                "triggers": triggers,
                "marginal_efficiency": marginal_efficiency,
                "confidence": "repeatable" if repeatable else "exploratory",
                "confirmation_limit": "a promotion claim still requires paired-direction consistency, confidence intervals, and same-Pod recovery evidence",
                "interpretation": "the knee lies within this offered-rate interval; do not report the upper point as sustainable capacity",
            }

        if latency_elevated:
            pending_latency = {
                "lower": previous,
                "onset": current,
                "marginal_efficiency": marginal_efficiency,
            }
        else:
            pending_latency = None
        previous = current
    result = {
        "status": "not_reached",
        "reason": "no scheduler-valid rate met the declared stress criteria",
        "highest_tested_rate_per_target": valid[-1]["offered_rps_per_target"],
    }
    if pending_latency is not None:
        result["unconfirmed_latency_candidate_rate_per_target"] = pending_latency[
            "onset"
        ]["offered_rps_per_target"]
        result["latency_confirmation_required"] = (
            "test a higher offered-rate point; one isolated p99 elevation does not bracket a knee"
        )
    return result


def write_tsv(path: Path, aggregates: list[dict[str, Any]]) -> None:
    fields = (
        "offered_rps_per_target",
        "repetitions",
        "scheduler_valid_repetitions",
        "median_aggregate_useful_rps",
        "median_offered_success_ratio",
        "median_latency_p50_us",
        "median_latency_p99_us",
        "latency_p99_cv",
        "median_in_flight_drop_ratio",
        "max_schedule_drop_ratio",
        "median_drain_ratio",
        "useful_rps_cv",
        "median_average_target_cpu_cores",
        "max_endpoint_useful_rps_cv",
        "observed_target_cpusets",
        "total_errors_within_plateau",
        "total_health_event_violations",
    )
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, delimiter="\t", extrasaction="ignore")
        writer.writeheader()
        writer.writerows(aggregates)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--plan", type=Path, required=True)
    parser.add_argument("--cell-root", type=Path, required=True)
    parser.add_argument("--provenance", type=Path, required=True)
    parser.add_argument("--output-json", type=Path, required=True)
    parser.add_argument("--output-tsv", type=Path, required=True)
    parser.add_argument("--max-scheduler-p99-lag-ms", type=float, default=5.0)
    parser.add_argument("--max-schedule-drop-ratio", type=float, default=0.0)
    parser.add_argument("--telemetry-required", choices=("0", "1"), default="1")
    parser.add_argument("--metric-max-gap-seconds", type=float, default=10.0)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    require(args.max_scheduler_p99_lag_ms >= 0, "scheduler p99 lag threshold must be non-negative")
    require(
        0 <= args.max_schedule_drop_ratio <= 1,
        "schedule-drop ratio threshold must be between zero and one",
    )
    require(args.metric_max_gap_seconds > 0, "metric max gap must be positive")
    provenance = load_json(args.provenance)
    with args.plan.open(encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle, delimiter="\t"))
    require(rows, "sweep plan is empty")
    require([int(row["order"]) for row in rows] == list(range(1, len(rows) + 1)), "plan order is not contiguous")

    records = [
        validate_cell(
            row,
            provenance,
            args.cell_root,
            args.max_scheduler_p99_lag_ms * 1000,
            args.max_schedule_drop_ratio,
            args.telemetry_required == "1",
            args.metric_max_gap_seconds,
        )
        for row in rows
    ]
    signatures = {json.dumps(runtime_signature(record.summary["cell"]), sort_keys=True) for record in records}
    require(len(signatures) == 1, "runtime/resource/topology signature changed within sweep")

    ranges = sorted(
        (
            record.summary["cell"]["sequence_base"],
            record.summary["cell"]["reserved_sequence_end_exclusive"],
            record.run_id,
        )
        for record in records
    )
    for left, right in zip(ranges, ranges[1:]):
        require(left[1] <= right[0], f"sequence ranges overlap: {left[2]} and {right[2]}")

    actual_dirs = sorted(path.name for path in args.cell_root.iterdir() if (path / "summary.json").is_file())
    planned_dirs = sorted(record.run_id for record in records)
    require(actual_dirs == planned_dirs, "cell summary set does not exactly match sweep plan")

    aggregates = aggregate_rates(records)
    document = {
        "schema_version": 2,
        "run_id": provenance["run_id"],
        "protocol": "deterministic_offered_rate_v1",
        "source_attestation": provenance.get("source_attestation"),
        "limitations": provenance.get("limitations"),
        "planned_cells": len(rows),
        "attested_cells": len(records),
        "all_accounting_valid": all(record.summary["accounting_valid"] for record in records),
        "all_scheduler_attribution_valid": all(record.scheduler_valid for record in records),
        "telemetry_required": args.telemetry_required == "1",
        "all_required_telemetry_valid": all(record.telemetry is not None for record in records)
        if args.telemetry_required == "1"
        else None,
        "topology_preflight": provenance.get("topology_preflight"),
        "all_required_topology_preflights_valid": all(
            record.topology_preflight.get("attested") is True for record in records
        )
        if provenance.get("topology_preflight", {}).get("required", False)
        else None,
        "runtime_signature": json.loads(next(iter(signatures))),
        "scheduler_attribution_thresholds": {
            "max_dispatch_p99_lag_ms": args.max_scheduler_p99_lag_ms,
            "max_schedule_drop_ratio": args.max_schedule_drop_ratio,
            "policy": "driver schedule failures invalidate SC capacity attribution; in-flight-limit drops remain an overload signal",
        },
        "knee_inference_policy": {
            "minimum_offered_success_ratio": 0.99,
            "maximum_clean_drain_ratio": 0.01,
            "minimum_marginal_useful_throughput_efficiency": 0.5,
            "latency_p99_ratio_to_low_rate_baseline": 1.25,
            "latency_only_confirmation": "the elevation must persist at the next higher offered-rate point; an isolated p99 elevation does not bracket a knee",
            "capacity_signal_timing": "throughput loss, in-flight drops, drains, errors, missing success latency, and health events bracket immediately",
        },
        "rates": aggregates,
        "knee": infer_knee(aggregates),
    }
    args.output_json.parent.mkdir(parents=True, exist_ok=True)
    with args.output_json.open("w", encoding="utf-8") as handle:
        json.dump(document, handle, indent=2, sort_keys=True)
        handle.write("\n")
    write_tsv(args.output_tsv, aggregates)
    json.dump(document, sys.stdout, indent=2, sort_keys=True)
    sys.stdout.write("\n")
    return 0 if document["all_scheduler_attribution_valid"] else 3


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except ValidationError as error:
        print(f"ERROR: {error}", file=sys.stderr)
        raise SystemExit(2)
