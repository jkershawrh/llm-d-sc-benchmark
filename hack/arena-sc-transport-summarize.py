#!/usr/bin/env python3
"""Validate and summarize a matched Arena transport campaign."""

from __future__ import annotations

import argparse
import json
import statistics
from pathlib import Path


DEFAULT_TREATMENTS = ("clusterip", "gateway", "direct")


def load(path: Path) -> dict:
    return json.loads(path.read_text())


def cell_summary(run_dir: Path, repetition: int, treatment: str) -> dict:
    cell_dir = run_dir / f"{repetition}-{treatment}"
    result_path = cell_dir / "result.json"
    network_path = cell_dir / "network-distribution.json"
    resource_path = cell_dir / "resource-summary.json"
    if not result_path.exists():
        return {"repetition": repetition, "treatment": treatment, "complete": False}
    result = load(result_path)
    network = load(network_path) if network_path.exists() else None
    resources = load(resource_path) if resource_path.exists() else None
    selected = result.get("selected_requests", 0)
    successful = result.get("successful_requests", 0)
    statuses = {}
    for endpoint in result.get("endpoints", []):
        for status, count in endpoint.get("statuses", {}).items():
            statuses[status] = statuses.get(status, 0) + count
    accounted = sum(statuses.values())
    core_valid = (
        result.get("kind") == "llm-d-sc-signal-emulator-result"
        and selected > 0
        and accounted == selected
        and result.get("cache", {}).get("mode") == "hit"
    )
    resource_complete = bool(
        resources
        and resources.get("target_cpu_cores", {}).get("available")
        and resources.get("driver_cpu_cores", {}).get("available")
        and resources.get("target_throttle_ratio", {}).get("available")
        and (
            treatment != "gateway"
            or resources.get("gateway_cpu_cores", {}).get("available")
        )
    )
    return {
        "repetition": repetition,
        "treatment": treatment,
        "complete": bool(network),
        "core_valid": core_valid,
        "telemetry_complete": resource_complete,
        "selected_requests": selected,
        "successful_requests": successful,
        "error_requests": selected - successful,
        "success_rate": successful / selected if selected else None,
        "zero_error_slo_pass": selected == successful,
        "statuses": statuses,
        "elapsed_seconds": result.get("elapsed_seconds"),
        "useful_requests_per_second": result.get("useful_requests_per_second"),
        "successful_rtt_ms": result.get("successful_rtt_ms"),
        "network_distribution": (
            {
                "coefficient_of_variation": network.get("coefficient_of_variation"),
                "max_share_over_ideal": network.get("max_share_over_ideal"),
            }
            if network
            else None
        ),
        "resource_summary": (
            {
                "target_cpu_cores": resources.get("target_cpu_cores"),
                "driver_cpu_cores": resources.get("driver_cpu_cores"),
                "gateway_cpu_cores": resources.get("gateway_cpu_cores"),
                "target_throttle_ratio": resources.get("target_throttle_ratio"),
            }
            if resources
            else None
        ),
    }


def summarize(run_dir: Path, repetitions: int, treatments=DEFAULT_TREATMENTS) -> dict:
    cells = [
        cell_summary(run_dir, repetition, treatment)
        for repetition in range(1, repetitions + 1)
        for treatment in treatments
    ]
    aggregates = {}
    for treatment in treatments:
        valid = [
            cell
            for cell in cells
            if cell["treatment"] == treatment and cell.get("complete") and cell.get("core_valid")
        ]
        aggregates[treatment] = {
            "valid_repetitions": len(valid),
            "median_useful_requests_per_second": (
                statistics.median(cell["useful_requests_per_second"] for cell in valid) if valid else None
            ),
            "median_p99_ms": (
                statistics.median(cell["successful_rtt_ms"]["p99"] for cell in valid) if valid else None
            ),
            "median_success_rate": (
                statistics.median(cell["success_rate"] for cell in valid) if valid else None
            ),
            "total_error_requests": sum(cell["error_requests"] for cell in valid),
            "median_network_cv": (
                statistics.median(cell["network_distribution"]["coefficient_of_variation"] for cell in valid)
                if valid
                else None
            ),
        }

    paired = []
    for repetition in range(1, repetitions + 1):
        by_treatment = {
            cell["treatment"]: cell
            for cell in cells
            if cell["repetition"] == repetition and cell.get("complete") and cell.get("core_valid")
        }
        if set(by_treatment) == set(treatments):
            row = {"repetition": repetition}
            if "direct" in by_treatment:
                direct_rps = by_treatment["direct"]["useful_requests_per_second"]
                for treatment in treatments:
                    if treatment != "direct":
                        row[f"{treatment}_over_direct_rps"] = (
                            by_treatment[treatment]["useful_requests_per_second"] / direct_rps
                        )
            paired.append(row)

    complete = all(cell.get("complete") and cell.get("core_valid") for cell in cells)
    telemetry_complete = complete and all(cell.get("telemetry_complete") for cell in cells)
    overload_cells = sum(
        bool(cell.get("complete") and cell.get("core_valid") and not cell.get("zero_error_slo_pass"))
        for cell in cells
    )
    return {
        "schema_version": 1,
        "kind": "llm-d-sc-transport-campaign-summary",
        "run_id": run_dir.name,
        "expected_repetitions": repetitions,
        "campaign_complete": complete,
        "telemetry_complete": telemetry_complete,
        "validity": {
            "expected_cells": repetitions * len(treatments),
            "complete_valid_cells": sum(
                bool(cell.get("complete") and cell.get("core_valid")) for cell in cells
            ),
            "cells_with_resource_telemetry": sum(bool(cell.get("telemetry_complete")) for cell in cells),
            "overload_cells": overload_cells,
        },
        "cells": cells,
        "aggregates": aggregates,
        "paired_repetitions": paired,
        "claim_gate": (
            (
                "eligible for matched transport conclusions; overload responses are part of the result"
                if overload_cells
                else "eligible for matched transport conclusions"
            )
            if complete and telemetry_complete
            else "partial: do not use for final bottleneck attribution"
        ),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("run_dir", type=Path)
    parser.add_argument("--repetitions", type=int, required=True)
    parser.add_argument("--treatments", nargs="+", default=list(DEFAULT_TREATMENTS))
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    result = summarize(args.run_dir, args.repetitions, tuple(args.treatments))
    output = args.output or args.run_dir / "transport-summary.json"
    output.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
