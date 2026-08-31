#!/usr/bin/env python3
"""Read-only aggregation and knee analysis for an Arena SC matrix run.

The matrix harness remains the authority for its fail-closed validity gates.
This utility independently recomputes the request/latency/CPU summaries from
the immutable cell artifacts and only includes harness-attested cells in
scaling-efficiency calculations.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import statistics
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Iterable


REQUIRED_CELL_ARTIFACTS = (
    "cell.json",
    "matrix-cell.json",
    "drivers.json",
    "summary.json",
    "cgroup-summary.json",
    "health-event-violations.json",
    "targets-before.json",
    "targets-after.json",
    "driver-jobs.json",
    "driver-pods.json",
    "recovery-anchor.json",
    "recovery-timeline.ndjson",
    "metrics-summary.json",
)


# These fields define whether two matrix plans describe the same workload and
# placement envelope.  Seed and sequence reservations are deliberately absent:
# they must differ between independent runs to keep the generated corpus unique.
MATRIX_MERGE_FIELDS = (
    "namespace",
    "deployment",
    "target_node",
    "driver_node",
    "target_image",
    "driver_image",
    "model_sha256",
    "tokenizer_sha256",
    "topology",
    "duration_seconds",
    "token_count",
)


# Replica count is intentionally excluded because it is the horizontal scale
# variable.  Concurrency and connection count remain grouping dimensions below,
# while these fields must be byte-for-byte equivalent for every included cell.
CELL_MERGE_FIELDS = (
    "target_image",
    "driver_image",
    "model_sha256",
    "tokenizer_sha256",
    "topology",
    "duration_seconds",
    "load_model",
    "open_loop",
    "inference_workers",
    "runtime_threads",
    "qos_class",
    "resources",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Validate and aggregate an arena-sc-inference-matrix run"
    )
    parser.add_argument("matrix_dir", type=Path, help="primary matrix directory")
    parser.add_argument(
        "--extra-matrix",
        "--extra-matrix-dir",
        dest="extra_matrix_dirs",
        action="append",
        default=[],
        type=Path,
        help=(
            "additional matrix directory to merge; repeat for more than one. "
            "Only independently valid, attested, provenance-identical cells "
            "are included"
        ),
    )
    parser.add_argument(
        "--format",
        choices=("markdown", "json"),
        default="markdown",
        help="output format (default: markdown)",
    )
    parser.add_argument(
        "--details",
        action="store_true",
        help="include per-cell details in JSON output",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="exit non-zero unless every planned cell is valid and attested",
    )
    return parser.parse_args()


def read_json(path: Path) -> Any:
    with path.open(encoding="utf-8") as handle:
        return json.load(handle)


def read_ndjson(path: Path) -> list[Any]:
    values: list[Any] = []
    if not path.exists():
        return values
    with path.open(encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, 1):
            if not line.strip():
                continue
            try:
                values.append(json.loads(line))
            except json.JSONDecodeError as error:
                raise ValueError(f"{path}:{line_number}: {error}") from error
    return values


def as_int(value: Any, default: int = 0) -> int:
    if isinstance(value, bool):
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def as_float(value: Any, default: float = math.nan) -> float:
    if isinstance(value, bool):
        return default
    try:
        result = float(value)
    except (TypeError, ValueError):
        return default
    return result if math.isfinite(result) else default


def close(left: Any, right: Any, tolerance: float = 1e-9) -> bool:
    a, b = as_float(left), as_float(right)
    return math.isfinite(a) and math.isfinite(b) and math.isclose(
        a, b, rel_tol=tolerance, abs_tol=tolerance
    )


def nearest_rank(sorted_values: list[int], fraction: float) -> int | None:
    if not sorted_values:
        return None
    index = max(0, math.ceil(len(sorted_values) * fraction) - 1)
    return sorted_values[index]


def distribution(values: Iterable[float]) -> dict[str, float | int | None]:
    finite: list[float] = []
    for value in values:
        number = as_float(value)
        if math.isfinite(number):
            finite.append(number)
    if not finite:
        return {"count": 0, "min": None, "max": None, "mean": None, "cv": None}
    mean = statistics.fmean(finite)
    cv = statistics.pstdev(finite) / mean if len(finite) > 1 and mean else 0.0
    return {
        "count": len(finite),
        "min": min(finite),
        "max": max(finite),
        "mean": mean,
        "cv": cv,
    }


def fairness(values: Iterable[float]) -> dict[str, float | int | None]:
    """Return endpoint fairness measures without hiding a zero-rate endpoint."""
    finite: list[float] = []
    for value in values:
        number = as_float(value)
        if math.isfinite(number) and number >= 0:
            finite.append(number)
    if not finite:
        return {
            "count": 0,
            "jain_index": None,
            "min_to_max_ratio": None,
        }
    total = sum(finite)
    square_total = sum(value * value for value in finite)
    maximum = max(finite)
    jain = total * total / (len(finite) * square_total) if square_total else 1.0
    return {
        "count": len(finite),
        "jain_index": jain,
        "min_to_max_ratio": min(finite) / maximum if maximum else 1.0,
    }


def selected_fields(document: dict[str, Any], fields: Iterable[str]) -> dict[str, Any]:
    """Build a stable, JSON-native merge contract from selected fields."""
    return {field: document.get(field) for field in fields}


def status_counts(document: Any) -> Counter[str]:
    result: Counter[str] = Counter()
    if isinstance(document, dict):
        for key, value in document.items():
            result[str(key)] += as_int(value)
    return result


def pod_ready(pod: dict[str, Any]) -> bool:
    conditions = pod.get("status", {}).get("conditions", [])
    return any(
        item.get("type") == "Ready" and item.get("status") == "True"
        for item in conditions
        if isinstance(item, dict)
    )


def pod_restarts(pod: dict[str, Any]) -> int:
    statuses = pod.get("status", {}).get("containerStatuses", [])
    return sum(
        as_int(item.get("restartCount")) for item in statuses if isinstance(item, dict)
    )


def target_snapshot_valid(document: Any, expected_uids: set[str], replicas: int) -> bool:
    if not isinstance(document, dict) or not isinstance(document.get("items"), list):
        return False
    items = document["items"]
    uids = {str(item.get("metadata", {}).get("uid", "")) for item in items}
    return (
        len(items) == replicas
        and uids == expected_uids
        and all(
            item.get("metadata", {}).get("deletionTimestamp") is None
            and item.get("status", {}).get("phase") == "Running"
            and pod_ready(item)
            and pod_restarts(item) == 0
            for item in items
        )
    )


def node_snapshot_valid(document: Any) -> bool:
    if not isinstance(document, dict) or not isinstance(document.get("items"), list):
        return False
    items = document["items"]
    if not items:
        return False
    return all(
        any(
            condition.get("type") == "Ready" and condition.get("status") == "True"
            for condition in item.get("status", {}).get("conditions", [])
            if isinstance(condition, dict)
        )
        for item in items
    )


def warning_event_delta(
    before: Any, after: Any, target_uids: set[str]
) -> list[dict[str, Any]]:
    before_counts = {
        str(item.get("metadata", {}).get("uid", "")): as_int(item.get("count"), 1)
        for item in before.get("items", [])
        if isinstance(item, dict)
    }
    deltas: list[dict[str, Any]] = []
    for item in after.get("items", []) if isinstance(after, dict) else []:
        if not isinstance(item, dict):
            continue
        involved_uid = str(item.get("involvedObject", {}).get("uid", ""))
        count = as_int(item.get("count"), 1)
        old_count = before_counts.get(str(item.get("metadata", {}).get("uid", "")), 0)
        if (
            involved_uid in target_uids
            and count > old_count
            and (item.get("type") == "Warning" or item.get("reason") == "Unhealthy")
        ):
            deltas.append(item)
    return deltas


def recovery_validation(
    cell_dir: Path,
    cell: dict[str, Any],
    before_targets: dict[str, Any],
    provenance: dict[str, Any],
) -> tuple[bool, list[str], list[dict[str, Any]]]:
    errors: list[str] = []
    timeline: list[dict[str, Any]] = []
    recovery = provenance.get("recovery", {})
    checkpoint_text = str(recovery.get("checkpoints_seconds", ""))
    checkpoints = [as_int(value, -1) for value in checkpoint_text.split()]
    checkpoints = [value for value in checkpoints if value >= 0]
    max_delay = as_int(recovery.get("max_observation_delay_seconds"), -1)
    plateau_start_ms = as_int(cell.get("start_epoch_ms"), -1)
    plateau_end = plateau_start_ms // 1000 + as_int(cell.get("duration_seconds"), -1)
    expected_uids = {
        str(item.get("metadata", {}).get("uid", ""))
        for item in before_targets.get("items", [])
        if isinstance(item, dict)
    }

    try:
        anchor = read_json(cell_dir / "recovery-anchor.json")
        timeline = read_ndjson(cell_dir / "recovery-timeline.ndjson")
    except (OSError, ValueError, json.JSONDecodeError) as error:
        return False, [f"recovery artifacts unreadable: {error}"], timeline

    if (
        anchor.get("source") != "driver_job_argument:--start-epoch-ms"
        or as_int(anchor.get("start_epoch_ms"), -2) != plateau_start_ms
        or as_int(anchor.get("plateau_end_epoch"), -2) != plateau_end
    ):
        errors.append("recovery anchor does not match the measured plateau")
    if [as_int(item.get("requested_seconds"), -1) for item in timeline] != checkpoints:
        errors.append("recovery checkpoints differ from matrix provenance")

    try:
        before_events = read_json(cell_dir / "events-before.json")
    except (OSError, json.JSONDecodeError) as error:
        before_events = {}
        errors.append(f"events-before.json unreadable: {error}")

    for item in timeline:
        checkpoint = as_int(item.get("requested_seconds"), -1)
        target_epoch = plateau_end + checkpoint
        delay = as_int(item.get("observation_delay_seconds"), -1)
        if (
            item.get("anchor") != "plateau_end"
            or as_int(item.get("plateau_start_epoch_ms"), -2) != plateau_start_ms
            or as_int(item.get("plateau_end_epoch"), -2) != plateau_end
            or as_int(item.get("target_epoch"), -2) != target_epoch
            or as_int(item.get("observation_started_epoch"), -2) < target_epoch
            or as_int(item.get("observed_epoch"), -2)
            < as_int(item.get("observation_started_epoch"), -1)
            or delay != as_int(item.get("observed_epoch"), -2) - target_epoch
            or delay < 0
            or max_delay < 0
            or delay > max_delay
        ):
            errors.append(f"recovery {checkpoint}s timing gate failed")

        prefix = cell_dir / f"recovery-{checkpoint}s"
        try:
            targets = read_json(Path(f"{prefix}-targets.json"))
            nodes = read_json(Path(f"{prefix}-nodes.json"))
            events = read_json(Path(f"{prefix}-events.json"))
            deployment = read_json(Path(f"{prefix}-deployment.json"))
        except (OSError, json.JSONDecodeError) as error:
            errors.append(f"recovery {checkpoint}s snapshot unreadable: {error}")
            continue
        if not target_snapshot_valid(targets, expected_uids, as_int(cell.get("replicas"))):
            errors.append(f"recovery {checkpoint}s target health/identity failed")
        if not node_snapshot_valid(nodes):
            errors.append(f"recovery {checkpoint}s node readiness failed")
        if warning_event_delta(before_events, events, expected_uids):
            errors.append(f"recovery {checkpoint}s added target Warning/Unhealthy events")
        status = deployment.get("status", {})
        replicas = as_int(cell.get("replicas"))
        if (
            as_int(status.get("readyReplicas")) != replicas
            or as_int(status.get("availableReplicas")) != replicas
        ):
            errors.append(f"recovery {checkpoint}s deployment was not fully available")

    return not errors, errors, timeline


def matrix_plan(matrix_dir: Path) -> list[dict[str, Any]]:
    plan_path = matrix_dir / "matrix-plan.tsv"
    with plan_path.open(encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle, delimiter="\t"))
    integer_fields = (
        "order",
        "repetition",
        "workers",
        "replicas",
        "concurrency",
        "connections",
        "sequence_base",
    )
    for row in rows:
        for field in integer_fields:
            row[field] = as_int(row.get(field), -1)
    return rows


def attestation_map(matrix_dir: Path) -> dict[str, dict[str, Any]]:
    path = matrix_dir / "matrix-results.ndjson"
    try:
        records = read_ndjson(path)
    except (OSError, ValueError):
        return {}
    result: dict[str, dict[str, Any]] = {}
    for record in records:
        run_id = record.get("expected", {}).get("run_id")
        if isinstance(run_id, str):
            if run_id in result:
                raise ValueError(
                    f"duplicate matrix-results attestation for {run_id}: {path}"
                )
            result[run_id] = record
    return result


def compare(errors: list[str], condition: bool, message: str) -> None:
    if not condition:
        errors.append(message)


def matrix_merge_contract(provenance: dict[str, Any]) -> dict[str, Any]:
    missing = [field for field in MATRIX_MERGE_FIELDS if field not in provenance]
    if missing:
        matrix_id = provenance.get("run_id", "unknown")
        raise ValueError(
            f"matrix {matrix_id} lacks required merge provenance: "
            + ", ".join(missing)
        )
    contract = selected_fields(provenance, MATRIX_MERGE_FIELDS)
    empty = [field for field, value in contract.items() if value in (None, "")]
    if empty:
        matrix_id = provenance.get("run_id", "unknown")
        raise ValueError(
            f"matrix {matrix_id} has empty required merge provenance: "
            + ", ".join(empty)
        )
    if as_int(contract["duration_seconds"], -1) <= 0:
        raise ValueError("matrix duration_seconds must be positive")
    if as_int(contract["token_count"], -1) <= 0:
        raise ValueError("matrix token_count must be positive")
    return contract


def provenance_mismatches(
    reference: dict[str, Any], candidate: dict[str, Any]
) -> list[str]:
    return [
        field
        for field in reference
        if reference.get(field) != candidate.get(field)
    ]


def format_contract_value(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def enforce_matrix_merge_contract(
    sources: list[dict[str, Any]],
) -> dict[str, Any]:
    reference_source = sources[0]
    reference = matrix_merge_contract(reference_source["provenance"])
    reference_id = reference_source["provenance"].get("run_id", "unknown")
    errors: list[str] = []
    for source in sources[1:]:
        candidate = matrix_merge_contract(source["provenance"])
        candidate_id = source["provenance"].get("run_id", "unknown")
        for field in provenance_mismatches(reference, candidate):
            errors.append(
                f"{candidate_id}.{field}={format_contract_value(candidate[field])} "
                f"does not match {reference_id}.{field}="
                f"{format_contract_value(reference[field])}"
            )
    if errors:
        raise ValueError("matrix provenance mismatch: " + "; ".join(errors))
    return reference


def enforce_cell_merge_contract(
    cells: list[dict[str, Any]],
) -> dict[str, Any] | None:
    included = [cell for cell in cells if cell.get("valid") is True]
    if not included:
        return None
    reference_cell = included[0]
    reference = reference_cell["_merge_contract"]
    errors: list[str] = []
    for cell in included[1:]:
        candidate = cell["_merge_contract"]
        for field in provenance_mismatches(reference, candidate):
            errors.append(
                f"{cell['run_id']}.{field}={format_contract_value(candidate[field])} "
                f"does not match {reference_cell['run_id']}.{field}="
                f"{format_contract_value(reference[field])}"
            )
    ranges = sorted(
        (
            as_int(cell.get("sequence_base"), -1),
            as_int(cell.get("_sequence_end_exclusive"), -1),
            cell["run_id"],
        )
        for cell in included
    )
    for start, end, run_id in ranges:
        if start < 0 or end <= start:
            errors.append(
                f"included cell sequence reservation is invalid: "
                f"{run_id}=[{start},{end})"
            )
    for previous, current in zip(ranges, ranges[1:]):
        previous_start, previous_end, previous_id = previous
        current_start, current_end, current_id = current
        if current_start < previous_end:
            errors.append(
                "included cell sequence reservations overlap: "
                f"{previous_id}=[{previous_start},{previous_end}), "
                f"{current_id}=[{current_start},{current_end})"
            )
    if errors:
        raise ValueError("valid-cell runtime/resource mismatch: " + "; ".join(errors))
    return reference


def analyze_cell(
    matrix_dir: Path,
    expected: dict[str, Any],
    attestation: dict[str, Any] | None,
    provenance: dict[str, Any],
) -> dict[str, Any]:
    run_id = str(expected["run_id"])
    cell_dir = matrix_dir / "cells" / run_id
    missing = [name for name in REQUIRED_CELL_ARTIFACTS if not (cell_dir / name).is_file()]
    result: dict[str, Any] = {
        "run_id": run_id,
        "source_matrix_run_id": provenance.get("run_id", matrix_dir.name),
        "source_matrix_directory": str(matrix_dir),
        "order": expected["order"],
        "phase": expected["phase"],
        "repetition": expected["repetition"],
        "workers": expected["workers"],
        "replicas": expected["replicas"],
        "concurrency": expected["concurrency"],
        "connections": expected["connections"],
        "sequence_base": expected["sequence_base"],
        "topology": provenance.get("topology"),
        "harness_attested": bool(attestation and attestation.get("valid") is True),
        "state": "pending" if not cell_dir.exists() else "incomplete",
        "missing_artifacts": missing,
        "validation_errors": [],
    }
    if missing:
        return result

    try:
        cell = read_json(cell_dir / "cell.json")
        matrix_cell = read_json(cell_dir / "matrix-cell.json")
        drivers = read_json(cell_dir / "drivers.json")
        summary = read_json(cell_dir / "summary.json")
        cgroup = read_json(cell_dir / "cgroup-summary.json")
        health_events = read_json(cell_dir / "health-event-violations.json")
        targets_before = read_json(cell_dir / "targets-before.json")
        targets_after = read_json(cell_dir / "targets-after.json")
        driver_jobs = read_json(cell_dir / "driver-jobs.json")
        driver_pods = read_json(cell_dir / "driver-pods.json")
        metrics = read_json(cell_dir / "metrics-summary.json")
    except (OSError, json.JSONDecodeError) as error:
        result["validation_errors"] = [f"cell artifact unreadable: {error}"]
        return result

    errors: list[str] = []
    replicas = expected["replicas"]
    duration = as_int(cell.get("duration_seconds"), -1)
    compare(errors, duration > 0, "duration_seconds is not positive")
    compare(errors, cell.get("run_id") == run_id, "cell run_id differs from plan")
    compare(errors, matrix_cell.get("run_id") == run_id, "matrix-cell run_id differs from plan")
    for field in ("replicas", "concurrency", "connections"):
        expected_value = expected[field]
        cell_field = field if field == "replicas" else f"{field}_per_target"
        compare(
            errors,
            as_int(cell.get(cell_field), -1) == expected_value,
            f"cell {cell_field} differs from plan",
        )
        compare(
            errors,
            as_int(matrix_cell.get(field), -1) == expected_value,
            f"matrix-cell {field} differs from plan",
        )
    compare(
        errors,
        str(cell.get("inference_workers")) == str(expected["workers"]),
        "inference worker width differs from plan",
    )
    missing_merge_fields = [field for field in CELL_MERGE_FIELDS if field not in cell]
    compare(
        errors,
        not missing_merge_fields,
        "cell lacks required merge provenance: " + ", ".join(missing_merge_fields),
    )
    compare(errors, bool(cell.get("load_model")), "cell load_model is absent")
    compare(
        errors,
        isinstance(cell.get("runtime_threads"), dict)
        and {"rayon", "candle"} <= set(cell.get("runtime_threads", {})),
        "cell runtime_threads provenance is malformed",
    )
    compare(
        errors,
        isinstance(cell.get("resources"), dict)
        and all(
            isinstance(cell.get("resources", {}).get(kind), dict)
            and all(
                cell.get("resources", {}).get(kind, {}).get(resource) not in (None, "")
                for resource in ("cpu", "memory")
            )
            for kind in ("requests", "limits")
        ),
        "cell resource provenance is malformed",
    )
    compare(errors, bool(cell.get("qos_class")), "cell qos_class is absent")
    compare(
        errors,
        as_int(cell.get("inference_workers"), -1) > 0,
        "cell inference_workers is not positive",
    )
    compare(
        errors,
        as_int(cell.get("sequence_base"), -1) == expected["sequence_base"],
        "sequence base differs from plan",
    )
    for field in ("target_image", "driver_image", "model_sha256", "tokenizer_sha256", "topology"):
        compare(
            errors,
            cell.get(field) == provenance.get(field),
            f"cell {field} differs from matrix provenance",
        )
    compare(errors, isinstance(drivers, list) and len(drivers) == replicas, "driver count differs from replicas")

    all_latencies: list[int] = []
    endpoint_rps: list[float] = []
    endpoint_rows: list[dict[str, Any]] = []
    aggregate_statuses: Counter[str] = Counter()
    aggregate_drained: Counter[str] = Counter()
    initiated = 0
    target_to_rps: dict[str, float] = {}
    first_sequences: list[int] = []
    sequence_span = as_int(cell.get("sequence_span_per_endpoint"), -1)
    for index, driver in enumerate(drivers if isinstance(drivers, list) else []):
        raw = driver.get("successful_rtt_raw_us", [])
        if not isinstance(raw, list) or any(not isinstance(value, int) or value < 0 for value in raw):
            errors.append(f"driver {index + 1} raw latency data is malformed")
            raw = []
        if raw != sorted(raw):
            errors.append(f"driver {index + 1} raw latency data is not sorted")
        statuses = status_counts(driver.get("statuses_completed_within_plateau", {}))
        drained = status_counts(driver.get("drained_after_plateau", {}))
        compare(
            errors,
            len(raw) == statuses.get("OK", 0),
            f"driver {index + 1} raw latency count differs from completed OK",
        )
        compare(
            errors,
            not bool(driver.get("corpus_exhausted")),
            f"driver {index + 1} exhausted its corpus",
        )
        compare(
            errors,
            as_int(driver.get("workers_ready_epoch_ms"), 2**63 - 1)
            < as_int(driver.get("start_epoch_ms"), -1),
            f"driver {index + 1} was not ready before plateau",
        )
        compare(
            errors,
            as_int(driver.get("start_epoch_ms"), -1) == as_int(cell.get("start_epoch_ms"), -2),
            f"driver {index + 1} plateau start differs from cell",
        )
        compare(
            errors,
            as_int(driver.get("duration_seconds"), -1) == duration,
            f"driver {index + 1} duration differs from cell",
        )
        for field in ("target_image", "model_sha256", "tokenizer_sha256", "topology"):
            compare(
                errors,
                driver.get(field) == cell.get(field),
                f"driver {index + 1} {field} differs from cell provenance",
            )
        target = str(driver.get("target", ""))
        rps = statuses.get("OK", 0) / duration if duration > 0 else math.nan
        reported_rps = as_float(driver.get("useful_requests_per_second"))
        compare(errors, close(rps, reported_rps), f"driver {index + 1} useful RPS mismatch")
        target_to_rps[target] = rps
        endpoint_rps.append(rps)
        aggregate_statuses.update(statuses)
        aggregate_drained.update(drained)
        initiated += as_int(driver.get("claimed_plateau_rows"))
        first_sequences.append(as_int(driver.get("first_sequence"), -1))
        compare(
            errors,
            as_int(driver.get("last_sequence"), -2)
            == as_int(driver.get("first_sequence"), -1)
            + as_int(driver.get("warmup_requests"))
            + as_int(driver.get("candidate_rows"))
            - 1,
            f"driver {index + 1} sequence boundary mismatch",
        )
        all_latencies.extend(raw)

    compare(errors, set(aggregate_statuses) <= {"OK"}, "non-OK plateau status observed")
    completed = sum(aggregate_statuses.values())
    ok_completed = aggregate_statuses.get("OK", 0)
    drained = sum(aggregate_drained.values())
    compare(errors, initiated > 0, "no requests were initiated in the plateau")
    compare(errors, initiated == completed + drained, "initiated/completed/drained accounting mismatch")
    compare(errors, len(set(first_sequences)) == len(first_sequences), "driver sequence ranges overlap")
    compare(
        errors,
        sorted(first_sequences)
        == [expected["sequence_base"] + index * sequence_span for index in range(replicas)],
        "driver sequence ranges differ from the reserved cell ranges",
    )
    compare(
        errors,
        as_int(cell.get("reserved_sequence_end_exclusive"), -1)
        == expected["sequence_base"] + replicas * sequence_span,
        "reserved cell sequence boundary mismatch",
    )

    all_latencies.sort()
    latency = {
        "samples": len(all_latencies),
        "min": all_latencies[0] if all_latencies else None,
        "p50": nearest_rank(all_latencies, 0.50),
        "p95": nearest_rank(all_latencies, 0.95),
        "p99": nearest_rank(all_latencies, 0.99),
        "max": all_latencies[-1] if all_latencies else None,
    }
    aggregate_rps = ok_completed / duration if duration > 0 else math.nan
    compare(errors, as_int(summary.get("initiated_within_plateau"), -1) == initiated, "summary initiated count mismatch")
    compare(errors, as_int(summary.get("completed_within_plateau"), -1) == completed, "summary completed count mismatch")
    compare(errors, as_int(summary.get("ok_completed_within_plateau"), -1) == ok_completed, "summary OK count mismatch")
    compare(errors, as_int(summary.get("drained_after_plateau"), -1) == drained, "summary drained count mismatch")
    compare(errors, close(summary.get("aggregate_useful_rps"), aggregate_rps), "summary aggregate RPS mismatch")
    compare(errors, summary.get("latency_us") == latency, "summary merged latency percentiles mismatch")
    compare(errors, status_counts(summary.get("statuses", {})) == aggregate_statuses, "summary status counts mismatch")
    compare(errors, summary.get("endpoint_rps") == [driver.get("useful_requests_per_second") for driver in drivers], "summary endpoint RPS mismatch")

    jobs = driver_jobs.get("items", []) if isinstance(driver_jobs, dict) else []
    compare(errors, len(jobs) == replicas, "driver Job count differs from replicas")
    compare(
        errors,
        all(
            as_int(job.get("status", {}).get("failed")) == 0
            and as_int(job.get("status", {}).get("succeeded")) == 1
            and any(
                condition.get("type") == "Complete" and condition.get("status") == "True"
                for condition in job.get("status", {}).get("conditions", [])
                if isinstance(condition, dict)
            )
            for job in jobs
        ),
        "one or more driver Jobs did not complete exactly once",
    )
    driver_pod_items = driver_pods.get("items", []) if isinstance(driver_pods, dict) else []
    compare(errors, len(driver_pod_items) == replicas, "driver Pod count differs from replicas")
    compare(
        errors,
        all(
            pod.get("status", {}).get("phase") == "Succeeded" and pod_restarts(pod) == 0
            for pod in driver_pod_items
        ),
        "one or more driver Pods failed or restarted",
    )

    before_items = targets_before.get("items", []) if isinstance(targets_before, dict) else []
    expected_uids = {
        str(item.get("metadata", {}).get("uid", ""))
        for item in before_items
        if isinstance(item, dict)
    }
    compare(errors, len(before_items) == replicas, "targets-before count differs from replicas")
    compare(errors, target_snapshot_valid(targets_before, expected_uids, replicas), "targets were not healthy at plateau start")
    compare(errors, target_snapshot_valid(targets_after, expected_uids, replicas), "targets changed health/identity during plateau")
    ip_to_pod = {
        str(item.get("status", {}).get("podIP", "")): str(item.get("metadata", {}).get("name", ""))
        for item in before_items
        if isinstance(item, dict)
    }

    cgroup_by_pod: dict[str, dict[str, Any]] = {}
    if not isinstance(cgroup, list) or len(cgroup) != replicas:
        errors.append("cgroup sample count differs from replicas")
        cgroup = []
    for sample in cgroup:
        pod = str(sample.get("pod", ""))
        cpu = as_float(sample.get("average_cpu_cores"))
        if not math.isfinite(cpu) or cpu < 0:
            errors.append(f"invalid cgroup CPU sample for {pod or 'unknown pod'}")
        if as_int(sample.get("usage_usec_delta"), -1) < 0:
            errors.append(f"negative cgroup CPU delta for {pod or 'unknown pod'}")
        cgroup_by_pod[pod] = sample

    for target, rps in target_to_rps.items():
        ip = target.rsplit(":", 1)[0]
        pod = ip_to_pod.get(ip)
        sample = cgroup_by_pod.get(pod or "", {})
        endpoint_rows.append(
            {
                "pod": pod,
                "endpoint": target,
                "useful_rps": rps,
                "average_cpu_cores": (
                    as_float(sample.get("average_cpu_cores"))
                    if math.isfinite(as_float(sample.get("average_cpu_cores")))
                    else None
                ),
            }
        )
    compare(errors, all(row["pod"] for row in endpoint_rows), "driver endpoint could not be mapped to a target Pod")
    compare(errors, set(cgroup_by_pod) == set(ip_to_pod.values()), "cgroup samples do not match target Pods")

    compare(errors, isinstance(health_events, list), "health-event violations artifact is malformed")
    health_event_count = len(health_events) if isinstance(health_events, list) else -1
    compare(errors, health_event_count == 0, "target health-event violations observed")
    compare(errors, as_int(summary.get("health_event_violations"), -1) == health_event_count, "summary health-event count mismatch")

    recovery_valid, recovery_errors, timeline = recovery_validation(
        cell_dir, cell, targets_before, provenance
    )
    errors.extend(recovery_errors)

    pod_names = set(ip_to_pod.values())
    pod_ready_min = {
        str(item.get("pod")): as_float(item.get("min"))
        for item in metrics.get("pod_ready_min", [])
        if isinstance(item, dict)
    }
    compare(
        errors,
        pod_names <= {pod for pod, value in pod_ready_min.items() if value == 1},
        "metrics summary does not prove every target stayed Ready",
    )
    compare(
        errors,
        all(as_float(item.get("min")) == 1 for item in metrics.get("node_ready_min", []))
        and len(metrics.get("node_ready_min", [])) >= 2,
        "metrics summary does not prove target/driver nodes stayed Ready",
    )

    harness_attested = bool(attestation and attestation.get("valid") is True)
    if not harness_attested:
        errors.append("cell is not attested valid in matrix-results.ndjson")
    else:
        compare(
            errors,
            attestation.get("expected") == matrix_cell,
            "matrix-results attestation expected-cell record differs from matrix-cell.json",
        )
        compare(
            errors,
            attestation.get("summary") == summary,
            "matrix-results attestation summary differs from summary.json",
        )
        compare(
            errors,
            attestation.get("metrics") == metrics,
            "matrix-results attestation metrics differ from metrics-summary.json",
        )

    cpu_values = [
        as_float(item.get("average_cpu_cores"))
        for item in cgroup
        if math.isfinite(as_float(item.get("average_cpu_cores")))
    ]
    result.update(
        {
            "state": "complete" if harness_attested else "unattested",
            "valid": not errors,
            "validation_errors": errors,
            "topology": cell.get("topology"),
            "duration_seconds": duration,
            "initiated_within_plateau": initiated,
            "completed_within_plateau": completed,
            "ok_completed_within_plateau": ok_completed,
            "drained_after_plateau": drained,
            "aggregate_useful_rps": aggregate_rps,
            "latency_us": latency,
            "endpoint_rps": distribution(endpoint_rps),
            "per_pod_cpu_cores": distribution(cpu_values),
            "per_endpoint": endpoint_rows,
            "health": {
                "event_violations": health_event_count,
                "targets_healthy_after_plateau": target_snapshot_valid(
                    targets_after, expected_uids, replicas
                ),
                "metrics_ready": pod_names
                <= {pod for pod, value in pod_ready_min.items() if value == 1},
            },
            "recovery": {
                "valid": recovery_valid,
                "checkpoints": timeline,
            },
            "_merge_contract": selected_fields(cell, CELL_MERGE_FIELDS),
            "_sequence_end_exclusive": as_int(
                cell.get("reserved_sequence_end_exclusive"), -1
            ),
            "_raw_latencies_us": all_latencies,
        }
    )
    return result


def median_or_none(values: list[float]) -> float | None:
    return statistics.median(values) if values else None


def aggregate_groups(
    cells: list[dict[str, Any]], matrix_terminal: bool
) -> list[dict[str, Any]]:
    buckets: defaultdict[tuple[Any, ...], list[dict[str, Any]]] = defaultdict(list)
    for cell in cells:
        key = (
            cell["phase"],
            cell["workers"],
            cell["replicas"],
            cell["concurrency"],
            cell["connections"],
            cell.get("topology"),
        )
        buckets[key].append(cell)

    groups: list[dict[str, Any]] = []
    for key, catalog_cells in buckets.items():
        phase, workers, replicas, concurrency, connections, topology = key
        planned_cells = [
            cell for cell in catalog_cells if cell.get("state") != "abandoned"
        ]
        valid_cells = [cell for cell in planned_cells if cell.get("valid") is True]
        rps_values = [float(cell["aggregate_useful_rps"]) for cell in valid_cells]
        p99_values = [float(cell["latency_us"]["p99"]) for cell in valid_cells]
        merged_latencies: list[int] = []
        endpoint_values: list[float] = []
        per_cell_jain: list[float] = []
        per_cell_endpoint_cv: list[float] = []
        cpu_values: list[float] = []
        for cell in valid_cells:
            merged_latencies.extend(cell.get("_raw_latencies_us", []))
            cell_endpoint_values = [
                row["useful_rps"] for row in cell.get("per_endpoint", [])
            ]
            endpoint_values.extend(cell_endpoint_values)
            cell_fairness = fairness(cell_endpoint_values)
            if cell_fairness["jain_index"] is not None:
                per_cell_jain.append(float(cell_fairness["jain_index"]))
            cell_endpoint_distribution = distribution(cell_endpoint_values)
            if cell_endpoint_distribution["cv"] is not None:
                per_cell_endpoint_cv.append(
                    float(cell_endpoint_distribution["cv"])
                )
            cpu_values.extend(
                row["average_cpu_cores"]
                for row in cell.get("per_endpoint", [])
                if math.isfinite(as_float(row.get("average_cpu_cores")))
            )
        merged_latencies.sort()
        merged_latency = {
            "samples": len(merged_latencies),
            "min": merged_latencies[0] if merged_latencies else None,
            "p50": nearest_rank(merged_latencies, 0.50),
            "p95": nearest_rank(merged_latencies, 0.95),
            "p99": nearest_rank(merged_latencies, 0.99),
            "max": merged_latencies[-1] if merged_latencies else None,
        }
        latency_medians = {
            name: median_or_none(
                [float(cell["latency_us"][name]) for cell in valid_cells]
            )
            for name in ("p50", "p95", "p99")
        }
        invalid_finished = [
            cell
            for cell in planned_cells
            if (
                cell.get("state") == "complete"
                or (
                    cell.get("_matrix_terminal", matrix_terminal)
                    and cell.get("state") in {"unattested", "incomplete"}
                )
            )
            and not cell.get("valid")
        ]
        groups.append(
            {
                "phase": phase,
                "workers": workers,
                "replicas": replicas,
                "concurrency": concurrency,
                "connections": connections,
                "topology": topology,
                "catalog_repetitions": len(catalog_cells),
                "planned_repetitions": len(planned_cells),
                "abandoned_repetitions": sum(
                    cell.get("state") == "abandoned" for cell in catalog_cells
                ),
                "valid_repetitions": len(valid_cells),
                "invalid_finished_repetitions": len(invalid_finished),
                "pending_repetitions": sum(
                    cell.get("state") == "pending"
                    or (
                        cell.get("state") == "incomplete"
                        and not cell.get("_matrix_terminal", matrix_terminal)
                    )
                    for cell in planned_cells
                ),
                "median_aggregate_useful_rps": median_or_none(rps_values),
                "repeat_rps": distribution(rps_values),
                "median_cell_latency_us": latency_medians,
                "merged_latency_us": merged_latency,
                "repeat_p99_us": distribution(p99_values),
                "endpoint_rps": distribution(endpoint_values),
                "endpoint_fairness": {
                    **fairness(endpoint_values),
                    "per_cell_jain_index": distribution(per_cell_jain),
                    "per_cell_cv": distribution(per_cell_endpoint_cv),
                },
                "per_pod_cpu_cores": distribution(cpu_values),
                "source_matrix_run_ids": sorted(
                    {
                        str(cell.get("source_matrix_run_id"))
                        for cell in valid_cells
                    }
                ),
                "catalog_source_matrix_run_ids": sorted(
                    {
                        str(cell.get("source_matrix_run_id"))
                        for cell in catalog_cells
                    }
                ),
                "valid_cell_run_ids": [cell["run_id"] for cell in valid_cells],
                "health_grade": "PENDING",
                "horizontal_efficiency": None,
                "baseline_useful_rps_per_pod": None,
                "ideal_linear_useful_rps": None,
            }
        )
    groups.sort(
        key=lambda item: (
            str(item["phase"]),
            as_int(item["workers"]),
            as_int(item["concurrency"]),
            as_int(item["connections"]),
            as_int(item["replicas"]),
        )
    )
    return groups


def apply_efficiency_and_knee(
    groups: list[dict[str, Any]], expected_repeats: int
) -> tuple[dict[str, Any] | None, dict[str, Any]]:
    baselines: dict[tuple[Any, ...], dict[str, Any]] = {}
    for group in groups:
        if group["replicas"] != 1 or group["median_aggregate_useful_rps"] is None:
            continue
        key = (
            group["phase"],
            group["workers"],
            group["concurrency"],
            group["connections"],
            group["topology"],
        )
        baselines[key] = group

    candidates: list[dict[str, Any]] = []
    for group in groups:
        key = (
            group["phase"],
            group["workers"],
            group["concurrency"],
            group["connections"],
            group["topology"],
        )
        baseline = baselines.get(key)
        if (
            baseline
            and group["median_aggregate_useful_rps"] is not None
            and baseline["median_aggregate_useful_rps"]
        ):
            efficiency = group["median_aggregate_useful_rps"] / (
                group["replicas"] * baseline["median_aggregate_useful_rps"]
            )
            group["horizontal_efficiency"] = efficiency
            group["baseline_useful_rps_per_pod"] = baseline[
                "median_aggregate_useful_rps"
            ]
            group["ideal_linear_useful_rps"] = (
                group["replicas"] * baseline["median_aggregate_useful_rps"]
            )
        else:
            efficiency = None

        if group["planned_repetitions"] == 0:
            grade = "NOT_RUN"
        elif group["invalid_finished_repetitions"]:
            grade = "RED"
        elif group["valid_repetitions"] < group["planned_repetitions"]:
            grade = "PENDING"
        elif efficiency is None:
            grade = "UNEVALUATED"
        elif efficiency < 0.60:
            grade = "RED"
        elif efficiency < 0.80:
            grade = "YELLOW"
        else:
            grade = "GREEN"
        if (
            grade == "GREEN"
            and group["valid_repetitions"] > 1
            and (
                (group["repeat_rps"]["cv"] or 0) > 0.10
                or (group["repeat_p99_us"]["cv"] or 0) > 0.15
            )
        ):
            grade = "YELLOW"
            group["stability_warning"] = "repeat CV exceeds the health rubric"
        group["health_grade"] = grade
        if group["phase"] == "horizontal" and group["replicas"] > 1 and (
            group["invalid_finished_repetitions"]
            or (efficiency is not None and efficiency < 0.80)
        ):
            candidates.append(group)

    candidate = min(candidates, key=lambda item: item["replicas"]) if candidates else None
    if candidate:
        baseline_key = (
            candidate["phase"],
            candidate["workers"],
            candidate["concurrency"],
            candidate["connections"],
            candidate["topology"],
        )
        baseline = baselines.get(baseline_key)
        repeats_sufficient = (
            candidate["valid_repetitions"] >= 5
            and bool(baseline)
            and baseline["valid_repetitions"] >= 5
        )
        knee = {
            "status": "confirmed" if repeats_sufficient else "candidate",
            "replicas": candidate["replicas"],
            "reason": (
                "health RED"
                if candidate["invalid_finished_repetitions"]
                else "horizontal efficiency below 80%"
            ),
            "horizontal_efficiency": candidate["horizontal_efficiency"],
            "confirmation_note": (
                "at least five valid repetitions exist at the baseline and knee rung"
                if repeats_sufficient
                else "exploratory only; repeat the topology-matched baseline and knee-adjacent rung at least five times"
            ),
        }
    else:
        complete = bool(groups) and all(
            group["planned_repetitions"] > 0
            and group["valid_repetitions"] == group["planned_repetitions"]
            for group in groups
        )
        max_rung = max((group["replicas"] for group in groups), default=None)
        knee = {
            "status": "not_observed" if complete else "not_yet_evaluable",
            "replicas": None,
            "reason": (
                f"no horizontal knee through r{max_rung}"
                if complete and max_rung is not None
                else "baseline and/or planned cells are not yet valid and complete"
            ),
            "horizontal_efficiency": None,
            "confirmation_note": (
                "one repetition is exploratory" if expected_repeats < 5 else None
            ),
        }
    baseline_summary = None
    if len(baselines) == 1:
        baseline = next(iter(baselines.values()))
        baseline_summary = {
            "replicas": 1,
            "median_useful_rps_per_pod": baseline["median_aggregate_useful_rps"],
            "valid_repetitions": baseline["valid_repetitions"],
            "topology": baseline["topology"],
        }
    return baseline_summary, knee


def horizontal_rung_coverage(groups: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Report planned horizontal rungs that do not yet have valid evidence."""
    series: defaultdict[tuple[Any, ...], list[dict[str, Any]]] = defaultdict(list)
    for group in groups:
        if group["phase"] != "horizontal":
            continue
        key = (
            group["workers"],
            group["concurrency"],
            group["connections"],
            group["topology"],
        )
        series[key].append(group)

    coverage: list[dict[str, Any]] = []
    for key, rung_groups in series.items():
        workers, concurrency, connections, topology = key
        rung_groups.sort(key=lambda item: item["replicas"])
        planned = [group["replicas"] for group in rung_groups]
        valid = [
            group["replicas"]
            for group in rung_groups
            if group["valid_repetitions"] > 0
        ]
        missing = []
        for group in rung_groups:
            if group["valid_repetitions"]:
                continue
            if group["invalid_finished_repetitions"]:
                state = "invalid"
            elif group["pending_repetitions"]:
                state = "pending"
            elif group["abandoned_repetitions"]:
                state = "abandoned"
            else:
                state = "unattested"
            missing.append(
                {
                    "replicas": group["replicas"],
                    "state": state,
                    "planned_repetitions": group["planned_repetitions"],
                    "invalid_finished_repetitions": group[
                        "invalid_finished_repetitions"
                    ],
                    "pending_repetitions": group["pending_repetitions"],
                }
            )
        coverage.append(
            {
                "workers": workers,
                "concurrency_per_target": concurrency,
                "connections_per_target": connections,
                "topology": topology,
                "planned_rungs": planned,
                "valid_rungs": valid,
                "missing_rungs": missing,
                "baseline_r1_valid": 1 in valid,
            }
        )
    coverage.sort(
        key=lambda item: (
            as_int(item["workers"]),
            as_int(item["concurrency_per_target"]),
            as_int(item["connections_per_target"]),
            str(item["topology"]),
        )
    )
    return coverage


def format_number(value: Any, digits: int = 2) -> str:
    number = as_float(value)
    return "—" if not math.isfinite(number) else f"{number:.{digits}f}"


def render_markdown(report: dict[str, Any]) -> str:
    matrix = report["matrix"]
    lines = [
        f"# SC matrix aggregation: {matrix['run_id']}",
        "",
        (
            f"Status: **{matrix['status']}** · valid/attested "
            f"**{matrix['valid_cells']}/{matrix['planned_cells']}** · "
            f"artifacts analyzed **{matrix['analyzed_cells']}**"
        ),
    ]
    if len(report["source_matrices"]) > 1:
        lines.extend(
            [
                "",
                "Sources: "
                + ", ".join(
                    f"`{source['run_id']}` ({source['status']})"
                    for source in report["source_matrices"]
                )
                + ". Provenance contract: **MATCHED**.",
            ]
        )
    lines.extend(
        [
            "",
            "| replicas | valid/planned | useful RPS | baseline efficiency | p50 / p95 / p99 | endpoint CV | Jain fairness | CPU cores/Pod | gate |",
            "|---:|---:|---:|---:|---:|---:|---:|---:|:---|",
        ]
    )
    for group in report["groups"]:
        latency = group["merged_latency_us"]
        latency_text = " / ".join(
            format_number((latency[name] / 1000) if latency[name] is not None else None, 2)
            for name in ("p50", "p95", "p99")
        ) + " ms"
        efficiency = group["horizontal_efficiency"]
        efficiency_text = "—" if efficiency is None else f"{efficiency * 100:.1f}%"
        endpoint_cv = group["endpoint_rps"]["cv"]
        endpoint_cv_text = "—" if endpoint_cv is None else f"{endpoint_cv * 100:.2f}%"
        jain = group["endpoint_fairness"]["jain_index"]
        lines.append(
            "| {replicas} | {valid}/{planned} | {rps} | {efficiency} | {latency} | {endpoint_cv} | {jain} | {cpu} | {grade} |".format(
                replicas=group["replicas"],
                valid=group["valid_repetitions"],
                planned=group["planned_repetitions"],
                rps=format_number(group["median_aggregate_useful_rps"], 2),
                efficiency=efficiency_text,
                latency=latency_text,
                endpoint_cv=endpoint_cv_text,
                jain=format_number(jain, 5),
                cpu=format_number(group["per_pod_cpu_cores"]["mean"], 3),
                grade=group["health_grade"],
            )
        )
    knee = report["knee"]
    lines.extend(
        [
            "",
            f"Knee: **{knee['status']}** — {knee['reason']}.",
            "",
            (
                "Rubric: the first horizontal rung below 80% topology-matched r1 "
                "efficiency, or with a health RED. A candidate is not confirmed "
                "until knee-adjacent and r1 controls have at least five repetitions."
            ),
        ]
    )
    for series in report["horizontal_rung_coverage"]:
        missing = series["missing_rungs"]
        missing_text = (
            ", ".join(
                f"r{item['replicas']} ({item['state']})" for item in missing
            )
            if missing
            else "none"
        )
        lines.extend(
            [
                "",
                "Horizontal rung coverage: "
                + ", ".join(f"r{value}" for value in series["valid_rungs"])
                + f" valid; missing {missing_text}.",
            ]
        )
    exclusions = report["exclusions"]
    if exclusions:
        abandoned = sum(item["state"] == "abandoned" for item in exclusions)
        visible_exclusions = [
            item for item in exclusions if item["state"] != "abandoned"
        ]
        lines.extend(["", "Excluded/incomplete cells:", ""])
        if abandoned:
            lines.append(
                f"- {abandoned} unstarted entries from terminal source plans were "
                "marked abandoned and do not count as repetitions."
            )
        for item in visible_exclusions:
            lines.append(f"- `{item['run_id']}`: {item['reason']}")
    return "\n".join(lines) + "\n"


def read_matrix_source(matrix_dir: Path) -> dict[str, Any]:
    if not matrix_dir.is_dir():
        raise ValueError(f"matrix directory does not exist: {matrix_dir}")
    plan = matrix_plan(matrix_dir)
    provenance = read_json(matrix_dir / "matrix-provenance.json")
    if not isinstance(provenance, dict):
        raise ValueError(f"matrix provenance is not an object: {matrix_dir}")
    attestations = attestation_map(matrix_dir)
    status_path = matrix_dir / "matrix-status.json"
    if status_path.exists():
        try:
            status = read_json(status_path).get("status", "unknown")
        except (OSError, json.JSONDecodeError):
            status = "unreadable"
    else:
        status = "running"
    return {
        "directory": matrix_dir,
        "plan": plan,
        "provenance": provenance,
        "attestations": attestations,
        "status": status,
    }


def combined_matrix_status(sources: list[dict[str, Any]]) -> str:
    statuses = [str(source["status"]) for source in sources]
    if len(statuses) == 1:
        return statuses[0]
    if all(status == "completed" for status in statuses):
        return "completed"
    if any(status == "running" for status in statuses):
        return "running"
    if all(status in {"completed", "aborted"} for status in statuses):
        return "partial"
    return "mixed"


def main() -> int:
    args = parse_args()
    matrix_dirs = [args.matrix_dir, *args.extra_matrix_dirs]
    matrix_dirs = [path.resolve() for path in matrix_dirs]
    if len(set(matrix_dirs)) != len(matrix_dirs):
        print("error: the same matrix directory was supplied more than once", file=sys.stderr)
        return 2
    try:
        sources = [read_matrix_source(path) for path in matrix_dirs]
        matrix_contract = enforce_matrix_merge_contract(sources)
    except (OSError, json.JSONDecodeError, ValueError) as error:
        print(f"error: cannot read matrix metadata: {error}", file=sys.stderr)
        return 2

    source_ids = [
        str(source["provenance"].get("run_id", source["directory"].name))
        for source in sources
    ]
    if len(set(source_ids)) != len(source_ids):
        print("error: matrix run_id values must be unique", file=sys.stderr)
        return 2

    planned_run_ids = [
        str(row["run_id"]) for source in sources for row in source["plan"]
    ]
    duplicate_cell_ids = sorted(
        run_id for run_id, count in Counter(planned_run_ids).items() if count > 1
    )
    if duplicate_cell_ids:
        print(
            "error: planned cell run_id values collide across matrices: "
            + ", ".join(duplicate_cell_ids),
            file=sys.stderr,
        )
        return 2

    cells: list[dict[str, Any]] = []
    for source in sources:
        terminal = source["status"] in {"aborted", "completed"}
        for row in source["plan"]:
            cell = analyze_cell(
                source["directory"],
                row,
                source["attestations"].get(str(row["run_id"])),
                source["provenance"],
            )
            cell["_matrix_terminal"] = terminal
            if (
                len(sources) > 1
                and source["status"] == "aborted"
                and cell.get("state") == "pending"
            ):
                # An unstarted cell in a terminal source is a historical plan
                # entry, not an outstanding repetition.  A continuation matrix
                # may supersede the same rung without creating a false 1/2 gate.
                cell["state"] = "abandoned"
            elif source["status"] == "completed" and cell.get("state") == "pending":
                cell["state"] = "incomplete"
            cells.append(cell)

    try:
        if len(sources) > 1:
            cell_contract = enforce_cell_merge_contract(cells)
        else:
            cell_contract = next(
                (
                    cell.get("_merge_contract")
                    for cell in cells
                    if cell.get("valid") is True
                ),
                None,
            )
    except ValueError as error:
        print(f"error: refusing matrix merge: {error}", file=sys.stderr)
        return 2

    matrix_status = combined_matrix_status(sources)
    groups = aggregate_groups(
        cells, all(source["status"] in {"aborted", "completed"} for source in sources)
    )
    expected_repeats = max(
        (as_int(group.get("planned_repetitions"), 1) for group in groups),
        default=1,
    )
    baseline, knee = apply_efficiency_and_knee(groups, expected_repeats)
    valid_cells = sum(cell.get("valid") is True for cell in cells)
    analyzed_cells = sum(cell.get("state") in {"complete", "unattested"} for cell in cells)
    effective_planned_cells = sum(cell.get("state") != "abandoned" for cell in cells)
    exclusions = []
    for cell in cells:
        if cell.get("valid") is True:
            continue
        reason_parts = []
        if cell.get("missing_artifacts"):
            reason_parts.append("missing " + ", ".join(cell["missing_artifacts"]))
        if cell.get("validation_errors"):
            reason_parts.extend(cell["validation_errors"])
        exclusions.append(
            {
                "run_id": cell["run_id"],
                "source_matrix_run_id": cell["source_matrix_run_id"],
                "state": cell["state"],
                "reason": "; ".join(reason_parts) or cell["state"],
            }
        )
    horizontal_coverage = horizontal_rung_coverage(groups)
    source_reports = [
        {
            "run_id": source["provenance"].get(
                "run_id", source["directory"].name
            ),
            "directory": str(source["directory"]),
            "status": source["status"],
            "planned_cells": len(source["plan"]),
            "harness_attested_cells": len(source["attestations"]),
        }
        for source in sources
    ]
    report = {
        "schema_version": 2,
        "matrix": {
            "run_id": " + ".join(source_ids),
            "directory": str(matrix_dirs[0]),
            "directories": [str(path) for path in matrix_dirs],
            "status": matrix_status,
            "planned_cells": effective_planned_cells,
            "catalog_cells": len(cells),
            "abandoned_unstarted_cells": sum(
                cell.get("state") == "abandoned" for cell in cells
            ),
            "analyzed_cells": analyzed_cells,
            "valid_cells": valid_cells,
            "harness_attested_cells": sum(
                len(source["attestations"]) for source in sources
            ),
            "exploratory": all(
                group["valid_repetitions"] < 5 for group in groups
            ),
        },
        "source_matrices": source_reports,
        "merge": {
            "enabled": len(sources) > 1,
            "provenance_match": True,
            "matrix_contract": matrix_contract,
            "included_cell_contract": cell_contract,
            "included_valid_attested_cells": valid_cells,
            "policy": (
                "invalid, incomplete, or unattested cells are excluded; valid cells "
                "are merged only when target/driver images, model, tokenizer, runtime, "
                "resources, topology, and duration match exactly and their generated "
                "sequence reservations do not overlap"
            ),
        },
        "rubric": {
            "horizontal_efficiency": "median useful RPS(N) / (N * median topology-matched r1 useful RPS)",
            "raw_percentiles": "nearest-rank percentiles recomputed from merged successful RTT samples",
            "endpoint_fairness": "pooled endpoint CV, Jain index, and min/max ratio; per-cell Jain distribution is also retained",
            "green": ">=80% with no health/validity failure",
            "yellow": "60-80%, or repeat RPS CV >10%, or repeat p99 CV >15%",
            "red": "<60%, health failure, recovery failure, or invalid/missing evidence",
            "first_knee": "first horizontal rung below 80% or with a health RED",
            "confirmation": "at least five repetitions at topology-matched r1 and knee-adjacent rung",
        },
        "baseline": baseline,
        "groups": groups,
        "horizontal_rung_coverage": horizontal_coverage,
        "knee": knee,
        "exclusions": exclusions,
    }
    if args.details:
        report["cells"] = [
            {key: value for key, value in cell.items() if not key.startswith("_")}
            for cell in cells
        ]

    if args.format == "json":
        json.dump(report, sys.stdout, indent=2, sort_keys=False, allow_nan=False)
        sys.stdout.write("\n")
    else:
        sys.stdout.write(render_markdown(report))

    missing_rungs = any(
        series["missing_rungs"] for series in horizontal_coverage
    )
    if args.strict and (
        valid_cells != effective_planned_cells or missing_rungs
    ):
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
