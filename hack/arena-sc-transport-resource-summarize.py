#!/usr/bin/env python3
"""Summarize cAdvisor CPU and throttling series for one transport cell."""

from __future__ import annotations

import argparse
from datetime import datetime
import json
from pathlib import Path


def epoch(raw: str) -> float:
    return datetime.fromisoformat(raw.replace("Z", "+00:00")).timestamp()


def rate_summary(payload: dict, start: float, end: float) -> dict:
    if payload.get("status") != "success":
        raise ValueError("Thanos query was not successful")
    rows = []
    totals: dict[float, float] = {}
    for item in payload.get("data", {}).get("result", []):
        values = [
            (float(ts), float(value))
            for ts, value in item.get("values", [])
            if start <= float(ts) <= end and value not in ("NaN", "+Inf", "-Inf")
        ]
        if not values:
            continue
        numbers = [value for _, value in values]
        for ts, value in values:
            totals[ts] = totals.get(ts, 0.0) + value
        rows.append(
            {
                "metric": item.get("metric", {}),
                "samples": len(numbers),
                "mean": sum(numbers) / len(numbers),
                "max": max(numbers),
            }
        )
    aggregate = list(totals.values())
    return {
        "available": bool(aggregate),
        "series": rows,
        "aggregate_samples": len(aggregate),
        "aggregate_mean": sum(aggregate) / len(aggregate) if aggregate else None,
        "aggregate_max": max(aggregate) if aggregate else None,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("cell_dir", type=Path)
    parser.add_argument("output_json", type=Path)
    parser.add_argument("--driver-cpu-limit", type=float, default=8.0)
    parser.add_argument("--target-cpu-limit-per-pod", type=float, default=4.0)
    args = parser.parse_args()

    job = json.loads((args.cell_dir / "job.json").read_text())
    start = epoch(job["status"]["startTime"])
    end = epoch(job["status"]["completionTime"])

    def load(name: str) -> dict:
        return json.loads((args.cell_dir / name).read_text())

    target = rate_summary(load("target-cpu-query.json"), start, end)
    driver = rate_summary(load("driver-cpu-query.json"), start, end)
    result_path = args.cell_dir / "result.json"
    if result_path.exists():
        embedded = json.loads(result_path.read_text()).get("driver_cpu", {})
        if embedded.get("available"):
            cores = embedded["average_cores"]
            driver = {
                "available": True,
                "source": embedded.get("source"),
                "series": [],
                "aggregate_samples": 1,
                "aggregate_mean": cores,
                "aggregate_max": cores,
                "usage_seconds": embedded.get("usage_seconds"),
                "user_seconds": embedded.get("user_seconds"),
                "system_seconds": embedded.get("system_seconds"),
            }
    gateway = rate_summary(load("gateway-cpu-query.json"), start, end)
    throttle = rate_summary(load("target-throttle-query.json"), start, end)
    if driver["available"]:
        driver["limit_cores"] = args.driver_cpu_limit
        driver["max_limit_fraction"] = driver["aggregate_max"] / args.driver_cpu_limit
    if target["available"]:
        target["limit_cores_per_pod"] = args.target_cpu_limit_per_pod
        target["max_aggregate_limit_fraction"] = target["aggregate_max"] / (
            args.target_cpu_limit_per_pod * len(target["series"])
        )

    result = {
        "schema_version": 1,
        "method": "60-second cAdvisor rates sampled during the Kubernetes Job interval",
        "cell_start_epoch": start,
        "cell_completion_epoch": end,
        "target_cpu_cores": target,
        "target_throttle_ratio": throttle,
        "driver_cpu_cores": driver,
        "gateway_cpu_cores": gateway,
    }
    args.output_json.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")


if __name__ == "__main__":
    main()
