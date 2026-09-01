#!/usr/bin/env python3
"""Independently audit a set of Arena transport campaigns from raw cell files."""

from __future__ import annotations

import argparse
import hashlib
import json
import statistics
from collections import Counter
from pathlib import Path


def load(path: Path) -> dict:
    return json.loads(path.read_text())


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def active_pod_nodes(path: Path) -> list[str]:
    document = load(path)
    return sorted(
        {
            item.get("spec", {}).get("nodeName")
            for item in document.get("items", [])
            if not item.get("metadata", {}).get("deletionTimestamp")
            and item.get("spec", {}).get("nodeName")
        }
    )


def driver_node(path: Path) -> str | None:
    document = load(path)
    items = document.get("items", [])
    return items[0].get("spec", {}).get("nodeName") if items else None


def group_values(summary: dict, section: str, metric: str, field: str) -> dict[str, float]:
    groups = summary.get(section, {}).get(metric, {}).get("groups", {})
    return {
        name: float(values[field])
        for name, values in groups.items()
        if isinstance(values, dict) and values.get(field) is not None
    }


def audit_cell(cell_dir: Path) -> dict:
    result_path = cell_dir / "result.json"
    health_path = cell_dir / "health-summary.json"
    resource_path = cell_dir / "resource-summary.json"
    result = load(result_path)
    health = load(health_path)
    resources = load(resource_path)
    external_path = cell_dir / "external-telemetry-summary.json"
    external = load(external_path) if external_path.exists() else None

    statuses: Counter[str] = Counter()
    for endpoint in result.get("endpoints", []):
        statuses.update(endpoint.get("statuses", {}))
    selected = int(result["selected_requests"])
    successful = int(result["successful_requests"])
    accounted = sum(statuses.values())
    if accounted != selected:
        raise ValueError(f"{cell_dir}: accounted {accounted} != selected {selected}")
    if statuses.get("OK", 0) != successful:
        raise ValueError(f"{cell_dir}: OK status does not match successful_requests")

    target_cpu = resources.get("target_cpu_cores", {})
    driver_cpu = resources.get("driver_cpu_cores", {})
    throttle = resources.get("target_throttle_ratio", {})
    audited = {
        "cell": cell_dir.name,
        "repetition": int(cell_dir.name.split("-", 1)[0]),
        "treatment": cell_dir.name.split("-", 1)[1],
        "concurrency": int(result["transport"]["concurrency"]),
        "selected_requests": selected,
        "successful_requests": successful,
        "error_requests": selected - successful,
        "statuses": dict(sorted(statuses.items())),
        "elapsed_seconds": result["elapsed_seconds"],
        "useful_rps": result["useful_requests_per_second"],
        "p50_ms": result["successful_rtt_ms"]["p50"],
        "p95_ms": result["successful_rtt_ms"]["p95"],
        "p99_ms": result["successful_rtt_ms"]["p99"],
        "health_slo_pass": bool(health["health_slo_pass"]),
        "identity_stable": bool(health["identity_stable"]),
        "restart_delta": int(health["restart_delta_count"]),
        "warning_event_delta": int(health["warning_event_delta_count"]),
        "warning_by_probe": health.get("warning_event_deltas_by_probe", {}),
        "warning_by_failure": health.get("warning_event_deltas_by_failure", {}),
        "target_cpu_aggregate_max_cores": target_cpu.get("aggregate_max"),
        "target_cpu_aggregate_samples": target_cpu.get("aggregate_samples"),
        "target_cpu_limit_cores": target_cpu.get("limit_cores_per_pod", 0) * 5,
        "target_throttle_ratio_max": throttle.get("aggregate_max"),
        "driver_cpu_average_cores": driver_cpu.get("aggregate_max"),
        "driver_cpu_limit_cores": driver_cpu.get("limit_cores"),
        "driver_node": driver_node(cell_dir / "driver-pod.json"),
        "result_sha256": sha256(result_path),
        "health_sha256": sha256(health_path),
        "resource_sha256": sha256(resource_path),
    }
    if external:
        target_cpu_max = group_values(external, "gauges", "otel_target_cpu_cores", "max")
        target_memory_max = group_values(
            external, "gauges", "otel_target_memory_working_set_bytes", "max"
        )
        conntrack = group_values(external, "gauges", "node_conntrack_entries", "max")
        conntrack_limit = group_values(external, "gauges", "node_conntrack_limit", "max")
        audited["external_telemetry"] = {
            "complete": bool(external.get("critical_signals_complete")),
            "sha256": sha256(external_path),
            "target_cpu_sum_of_pod_max_cores": sum(target_cpu_max.values()),
            "target_memory_sum_of_pod_max_bytes": sum(target_memory_max.values()),
            "node_conntrack_max": conntrack,
            "node_conntrack_limit": conntrack_limit,
            "node_conntrack_max_fraction": {
                node: value / conntrack_limit[node]
                for node, value in conntrack.items()
                if conntrack_limit.get(node, 0) > 0
            },
            "node_tcp_retransmit_delta": group_values(
                external, "counters", "node_tcp_retransmits", "delta"
            ),
            "node_softnet_drop_delta": group_values(
                external, "counters", "node_softnet_drops", "delta"
            ),
            "target_network_error_delta": sum(
                group_values(external, "counters", "otel_target_network_errors", "delta").values()
            ),
            "pod_receive_drop_delta": sum(
                group_values(external, "counters", "pod_receive_drops", "delta").values()
            ),
            "pod_transmit_drop_delta": sum(
                group_values(external, "counters", "pod_transmit_drops", "delta").values()
            ),
        }
    return audited


def median(values: list[float]) -> float:
    return statistics.median(values)


def audit_run(run_dir: Path) -> dict:
    cell_dirs = sorted(
        path
        for path in run_dir.iterdir()
        if path.is_dir() and (path / "result.json").exists()
    )
    if not cell_dirs:
        raise ValueError(f"{run_dir}: no result cells")
    cells = [audit_cell(path) for path in cell_dirs]
    concurrencies = {cell["concurrency"] for cell in cells}
    if len(concurrencies) != 1:
        raise ValueError(f"{run_dir}: mixed concurrency values {concurrencies}")

    topology = load(run_dir / "requested-topology.json")
    target_nodes = active_pod_nodes(run_dir / "target-pods-start.json")
    treatments = sorted({cell["treatment"] for cell in cells})
    aggregates = {}
    for treatment in treatments:
        subset = [cell for cell in cells if cell["treatment"] == treatment]
        statuses: Counter[str] = Counter()
        warning_by_probe: Counter[str] = Counter()
        warning_by_failure: Counter[str] = Counter()
        for cell in subset:
            statuses.update(cell["statuses"])
            warning_by_probe.update(cell["warning_by_probe"])
            warning_by_failure.update(cell["warning_by_failure"])
        aggregates[treatment] = {
            "cells": len(subset),
            "median_useful_rps": median([cell["useful_rps"] for cell in subset]),
            "median_p99_ms": median([cell["p99_ms"] for cell in subset]),
            "median_success_rate": median(
                [cell["successful_requests"] / cell["selected_requests"] for cell in subset]
            ),
            "selected_requests": sum(cell["selected_requests"] for cell in subset),
            "statuses": dict(sorted(statuses.items())),
            "health_break_cells": sum(not cell["health_slo_pass"] for cell in subset),
            "restart_delta": sum(cell["restart_delta"] for cell in subset),
            "warning_event_delta": sum(cell["warning_event_delta"] for cell in subset),
            "warning_by_probe": dict(sorted(warning_by_probe.items())),
            "warning_by_failure": dict(sorted(warning_by_failure.items())),
            "max_target_cpu_aggregate_cores": max(
                cell["target_cpu_aggregate_max_cores"] for cell in subset
            ),
            "max_target_throttle_ratio": max(
                cell["target_throttle_ratio_max"] for cell in subset
            ),
            "max_driver_cpu_average_cores": max(
                cell["driver_cpu_average_cores"] for cell in subset
            ),
        }

    return {
        "run_id": run_dir.name,
        "concurrency": next(iter(concurrencies)),
        "requested_topology": topology,
        "observed_target_nodes": target_nodes,
        "observed_driver_nodes": sorted({cell["driver_node"] for cell in cells}),
        "topology_isolated": (
            target_nodes == [topology.get("target_node")]
            and sorted({cell["driver_node"] for cell in cells}) == [topology.get("driver_node")]
            and topology.get("target_node") != topology.get("driver_node")
        ),
        "cells": cells,
        "aggregates": aggregates,
        "transport_summary_sha256": sha256(run_dir / "transport-summary.json"),
    }


def percent_change(before: float, after: float) -> float:
    return (after / before - 1.0) * 100.0


def aggregate_cells(cells: list[dict]) -> dict:
    statuses: Counter[str] = Counter()
    for cell in cells:
        statuses.update(cell["statuses"])
    rps = [cell["useful_rps"] for cell in cells]
    p99 = [cell["p99_ms"] for cell in cells]
    external = [cell["external_telemetry"] for cell in cells if cell.get("external_telemetry")]
    conntrack_fractions = [
        value
        for item in external
        for value in item.get("node_conntrack_max_fraction", {}).values()
    ]
    retransmits = [
        value
        for item in external
        for value in item.get("node_tcp_retransmit_delta", {}).values()
    ]
    return {
        "cells": len(cells),
        "median_useful_rps": median(rps),
        "min_useful_rps": min(rps),
        "max_useful_rps": max(rps),
        "median_p99_ms": median(p99),
        "min_p99_ms": min(p99),
        "max_p99_ms": max(p99),
        "selected_requests": sum(cell["selected_requests"] for cell in cells),
        "error_requests": sum(cell["error_requests"] for cell in cells),
        "error_cells": sum(cell["error_requests"] > 0 for cell in cells),
        "health_break_cells": sum(not cell["health_slo_pass"] for cell in cells),
        "restart_delta": sum(cell["restart_delta"] for cell in cells),
        "warning_event_delta": sum(cell["warning_event_delta"] for cell in cells),
        "statuses": dict(sorted(statuses.items())),
        "external_telemetry_cells": len(external),
        "external_telemetry_complete_cells": sum(item.get("complete", False) for item in external),
        "max_otel_target_cpu_sum_of_pod_max_cores": max(
            (item["target_cpu_sum_of_pod_max_cores"] for item in external), default=None
        ),
        "max_otel_target_memory_sum_of_pod_max_bytes": max(
            (item["target_memory_sum_of_pod_max_bytes"] for item in external), default=None
        ),
        "max_node_conntrack_fraction": max(conntrack_fractions, default=None),
        "max_node_tcp_retransmit_delta": max(retransmits, default=None),
        "node_softnet_drop_delta": sum(
            value
            for item in external
            for value in item.get("node_softnet_drop_delta", {}).values()
        ),
        "target_network_error_delta": sum(
            item.get("target_network_error_delta", 0) for item in external
        ),
        "pod_receive_drop_delta": sum(item.get("pod_receive_drop_delta", 0) for item in external),
        "pod_transmit_drop_delta": sum(
            item.get("pod_transmit_drop_delta", 0) for item in external
        ),
    }


def aggregate_by_concurrency(runs: list[dict]) -> list[dict]:
    rows = []
    for concurrency in sorted({run["concurrency"] for run in runs}):
        selected_runs = [run for run in runs if run["concurrency"] == concurrency]
        cells = [cell for run in selected_runs for cell in run["cells"]]
        treatments = sorted({cell["treatment"] for cell in cells})
        rows.append(
            {
                "concurrency": concurrency,
                "runs": len(selected_runs),
                "all_topologies_isolated": all(run["topology_isolated"] for run in selected_runs),
                "treatments": {
                    treatment: aggregate_cells(
                        [cell for cell in cells if cell["treatment"] == treatment]
                    )
                    for treatment in treatments
                },
            }
        )
    return rows


def summarize(runs: list[dict]) -> dict:
    by_concurrency = aggregate_by_concurrency(runs)
    loaded = [row for row in by_concurrency if row["concurrency"] > 1]
    transitions = []
    for before, after in zip(loaded, loaded[1:]):
        row = {"from": before["concurrency"], "to": after["concurrency"], "treatments": {}}
        for treatment in sorted(set(before["treatments"]) & set(after["treatments"])):
            left = before["treatments"][treatment]
            right = after["treatments"][treatment]
            row["treatments"][treatment] = {
                "useful_rps_change_percent": percent_change(
                    left["median_useful_rps"], right["median_useful_rps"]
                ),
                "p99_change_percent": percent_change(
                    left["median_p99_ms"], right["median_p99_ms"]
                ),
            }
        transitions.append(row)

    accounting: Counter[str] = Counter()
    selected = 0
    health_break_cells = 0
    restarts = 0
    warnings = 0
    for run in runs:
        for cell in run["cells"]:
            selected += cell["selected_requests"]
            accounting.update(cell["statuses"])
            health_break_cells += not cell["health_slo_pass"]
            restarts += cell["restart_delta"]
            warnings += cell["warning_event_delta"]

    return {
        "schema_version": 2,
        "kind": "llm-d-sc-transport-knee-independent-audit",
        "source": "raw result.json, health-summary.json, resource-summary.json, and topology snapshots",
        "runs": runs,
        "by_concurrency": by_concurrency,
        "transitions": transitions,
        "accounting": {
            "selected_requests": selected,
            "statuses": dict(sorted(accounting.items())),
            "health_break_cells": health_break_cells,
            "restart_delta": restarts,
            "warning_event_delta": warnings,
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("run_dirs", nargs="+", type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    result = summarize([audit_run(path) for path in args.run_dirs])
    rendered = json.dumps(result, indent=2, sort_keys=True) + "\n"
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered)
    print(rendered, end="")


if __name__ == "__main__":
    main()
