#!/usr/bin/env python3
"""Derive CPU-mode and per-request evidence from completed Arena SC cells.

This helper is deliberately offline: it only reads the evidence already saved
by arena-sc-inference-cell.sh.  It does not contact or mutate the cluster.
"""

from __future__ import annotations

import argparse
import json
import statistics
from pathlib import Path
from typing import Any


COUNTERS = (
    "usage_usec",
    "user_usec",
    "system_usec",
    "nr_periods",
    "nr_throttled",
    "throttled_usec",
)


def load_json(path: Path) -> Any:
    with path.open(encoding="utf-8") as stream:
        return json.load(stream)


def parse_snapshot(path: Path) -> dict[str, Any]:
    values: dict[str, Any] = {}
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        fields = raw_line.split()
        if not fields:
            continue
        key = fields[0]
        if key in COUNTERS and len(fields) == 2:
            values[key] = int(fields[1])
        elif key == "cpuset_cpus_effective" and len(fields) == 2:
            values[key] = fields[1]
        elif key == "cpu_max" and len(fields) >= 2:
            values[key] = " ".join(fields[1:])
        elif key == "scaling_cur_freq_khz" and len(fields) == 2:
            values[key] = None if fields[1] == "unavailable" else int(fields[1])
    missing = [key for key in COUNTERS if key not in values]
    if missing:
        raise ValueError(f"{path}: missing counters: {', '.join(missing)}")
    return values


def stats(values: list[float]) -> dict[str, float | int] | None:
    if not values:
        return None
    mean = statistics.fmean(values)
    return {
        "samples": len(values),
        "min": min(values),
        "max": max(values),
        "mean": mean,
        "median": statistics.median(values),
        "cv_percent": 0.0
        if len(values) == 1 or mean == 0
        else statistics.pstdev(values) / mean * 100.0,
    }


def analyze_cell(cell_dir: Path) -> dict[str, Any]:
    summary_path = cell_dir / "summary.json"
    targets_path = cell_dir / "targets-before.json"
    drivers_path = cell_dir / "drivers.json"
    cgroup_dir = cell_dir / "cgroup"
    for required in (summary_path, targets_path, drivers_path, cgroup_dir):
        if not required.exists():
            raise ValueError(f"{cell_dir}: required evidence missing: {required.name}")

    summary = load_json(summary_path)
    targets = load_json(targets_path)
    drivers = load_json(drivers_path)
    by_ip = {
        item["status"]["podIP"]: {
            "pod": item["metadata"]["name"],
            "uid": item["metadata"]["uid"],
        }
        for item in targets["items"]
    }
    ok_by_pod: dict[str, int] = {}
    rps_by_pod: dict[str, float] = {}
    for driver in drivers:
        address = driver["target"].rsplit(":", 1)[0]
        if address not in by_ip:
            raise ValueError(f"{cell_dir}: driver target {address} has no target Pod")
        pod = by_ip[address]["pod"]
        ok_by_pod[pod] = int(driver["statuses_completed_within_plateau"].get("OK", 0))
        rps_by_pod[pod] = float(driver["useful_requests_per_second"])

    pods: list[dict[str, Any]] = []
    for start_path in sorted(cgroup_dir.glob("*-start.txt")):
        pod = start_path.name.removesuffix("-start.txt")
        end_path = cgroup_dir / f"{pod}-end.txt"
        if not end_path.exists():
            raise ValueError(f"{cell_dir}: missing end snapshot for {pod}")
        if pod not in ok_by_pod:
            raise ValueError(f"{cell_dir}: no driver result for {pod}")
        start = parse_snapshot(start_path)
        end = parse_snapshot(end_path)
        deltas = {key: end[key] - start[key] for key in COUNTERS}
        if any(deltas[key] < 0 for key in COUNTERS):
            raise ValueError(f"{cell_dir}: counter regression for {pod}")
        if start.get("cpuset_cpus_effective") != end.get("cpuset_cpus_effective"):
            raise ValueError(f"{cell_dir}: cpuset changed for {pod}")
        ok = ok_by_pod[pod]
        accounted = deltas["user_usec"] + deltas["system_usec"]
        pods.append(
            {
                "pod": pod,
                "uid": next(value["uid"] for value in by_ip.values() if value["pod"] == pod),
                "cpuset_cpus_effective": start.get("cpuset_cpus_effective"),
                "ok_completed_within_plateau": ok,
                "useful_rps": rps_by_pod[pod],
                "usage_usec": deltas["usage_usec"],
                "user_usec": deltas["user_usec"],
                "system_usec": deltas["system_usec"],
                "unaccounted_cpu_usec": deltas["usage_usec"] - accounted,
                "system_cpu_percent": None
                if accounted == 0
                else deltas["system_usec"] / accounted * 100.0,
                "usage_usec_per_ok": None if ok == 0 else deltas["usage_usec"] / ok,
                "user_usec_per_ok": None if ok == 0 else deltas["user_usec"] / ok,
                "system_usec_per_ok": None if ok == 0 else deltas["system_usec"] / ok,
                "nr_throttled": deltas["nr_throttled"],
                "throttled_usec": deltas["throttled_usec"],
                "throttled_period_ratio": None
                if deltas["nr_periods"] == 0
                else deltas["nr_throttled"] / deltas["nr_periods"],
                "boundary_scaling_cur_freq_khz": {
                    "start": start.get("scaling_cur_freq_khz"),
                    "end": end.get("scaling_cur_freq_khz"),
                    "interpretation": "boundary-only; not an in-plateau effective-frequency measurement",
                },
            }
        )

    total_ok = sum(pod["ok_completed_within_plateau"] for pod in pods)
    total_usage = sum(pod["usage_usec"] for pod in pods)
    total_user = sum(pod["user_usec"] for pod in pods)
    total_system = sum(pod["system_usec"] for pod in pods)
    accounted = total_user + total_system
    expected_ok = int(summary["ok_completed_within_plateau"])
    if total_ok != expected_ok:
        raise ValueError(f"{cell_dir}: per-driver OK={total_ok}, summary OK={expected_ok}")

    cell = summary["cell"]
    runtime_threads = cell.get("runtime_threads")
    if runtime_threads is None:
        deployment = load_json(cell_dir / "deployment-before.json")
        env = {
            item["name"]: item.get("value", "valueFrom")
            for item in deployment["spec"]["template"]["spec"]["containers"][0].get("env", [])
        }
        runtime_threads = {
            "rayon": env.get("RAYON_NUM_THREADS", "unset"),
            "candle": env.get("CANDLE_NUM_THREADS", "unset"),
        }
    return {
        "cell_dir": str(cell_dir),
        "run_id": cell["run_id"],
        "target_image": cell["target_image"],
        "model_sha256": cell["model_sha256"],
        "load_model": summary.get("load_model", "closed_loop"),
        "replicas": int(cell["replicas"]),
        "inference_workers": cell["inference_workers"],
        "rayon_threads": runtime_threads["rayon"],
        "candle_threads": runtime_threads["candle"],
        "concurrency_per_target": int(cell["concurrency_per_target"]),
        "duration_seconds": int(cell["duration_seconds"]),
        "aggregate_useful_rps": float(summary["aggregate_useful_rps"]),
        "latency_us": summary["latency_us"],
        "ok_completed_within_plateau": total_ok,
        "cpu": {
            "usage_usec": total_usage,
            "user_usec": total_user,
            "system_usec": total_system,
            "unaccounted_cpu_usec": total_usage - accounted,
            "system_cpu_percent": None if accounted == 0 else total_system / accounted * 100.0,
            "usage_usec_per_ok": None if total_ok == 0 else total_usage / total_ok,
            "user_usec_per_ok": None if total_ok == 0 else total_user / total_ok,
            "system_usec_per_ok": None if total_ok == 0 else total_system / total_ok,
        },
        "endpoint": {
            "useful_rps": stats([pod["useful_rps"] for pod in pods]),
            "usage_usec_per_ok": stats(
                [pod["usage_usec_per_ok"] for pod in pods if pod["usage_usec_per_ok"] is not None]
            ),
            "system_cpu_percent": stats(
                [pod["system_cpu_percent"] for pod in pods if pod["system_cpu_percent"] is not None]
            ),
        },
        "pods": pods,
        "valid": True,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("cell_dirs", nargs="+", type=Path)
    parser.add_argument("--pretty", action="store_true")
    args = parser.parse_args()
    cells = [analyze_cell(path.resolve()) for path in args.cell_dirs]
    document = {
        "schema_version": 1,
        "source": "saved direct-cgroup snapshots and driver results; no live cluster access",
        "cells": cells,
        "all_valid": all(cell["valid"] for cell in cells),
    }
    json.dump(document, fp=__import__("sys").stdout, indent=2 if args.pretty else None)
    print()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
