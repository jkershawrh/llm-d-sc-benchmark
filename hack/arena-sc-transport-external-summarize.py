#!/usr/bin/env python3
"""Summarize external OTel and node-exporter signals for one transport cell."""

from __future__ import annotations

import argparse
from collections import defaultdict
from datetime import datetime
import json
import math
from pathlib import Path


def epoch(raw: str) -> float:
    return datetime.fromisoformat(raw.replace("Z", "+00:00")).timestamp()


def finite_values(item: dict) -> list[tuple[float, float]]:
    values = []
    for raw_ts, raw_value in item.get("values", []):
        try:
            ts, value = float(raw_ts), float(raw_value)
        except (TypeError, ValueError):
            continue
        if math.isfinite(value):
            values.append((ts, value))
    return sorted(values)


def gauge_summary(payload: dict, start: float, end: float, group_label: str) -> dict:
    if payload.get("status") != "success":
        raise ValueError("Thanos gauge query was not successful")
    grouped: dict[str, list[float]] = defaultdict(list)
    for item in payload.get("data", {}).get("result", []):
        group = item.get("metric", {}).get(group_label)
        if not group:
            continue
        grouped[group].extend(value for ts, value in finite_values(item) if start <= ts <= end)
    rows = {
        group: {
            "samples": len(values),
            "min": min(values),
            "mean": sum(values) / len(values),
            "max": max(values),
        }
        for group, values in sorted(grouped.items())
        if values
    }
    return {"available": bool(rows), "group_label": group_label, "groups": rows}


def counter_delta_summary(payload: dict, start: float, end: float, group_label: str) -> dict:
    if payload.get("status") != "success":
        raise ValueError("Thanos counter query was not successful")
    grouped_deltas: dict[str, float] = defaultdict(float)
    grouped_series: dict[str, int] = defaultdict(int)
    rejected_resets: dict[str, int] = defaultdict(int)
    for item in payload.get("data", {}).get("result", []):
        group = item.get("metric", {}).get(group_label)
        if not group:
            continue
        values = finite_values(item)
        before = [point for point in values if point[0] <= start]
        after = [point for point in values if point[0] >= end]
        if not before or not after:
            continue
        delta = after[0][1] - before[-1][1]
        if delta < 0:
            rejected_resets[group] += 1
            continue
        grouped_deltas[group] += delta
        grouped_series[group] += 1
    rows = {
        group: {"delta": grouped_deltas[group], "series": grouped_series[group]}
        for group in sorted(grouped_series)
    }
    return {
        "available": bool(rows),
        "method": "nearest sample at-or-before start to nearest sample at-or-after completion",
        "group_label": group_label,
        "groups": rows,
        "rejected_counter_resets": dict(sorted(rejected_resets.items())),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("cell_dir", type=Path)
    parser.add_argument("output_json", type=Path)
    args = parser.parse_args()

    job = json.loads((args.cell_dir / "job.json").read_text())
    start = epoch(job["status"]["startTime"])
    end = epoch(job["status"]["completionTime"])

    def load(name: str) -> dict:
        return json.loads((args.cell_dir / f"{name}-query.json").read_text())

    gauges = {
        "otel_target_cpu_cores": ("otel-target-cpu", "k8s_pod_name"),
        "otel_target_memory_working_set_bytes": ("otel-target-memory", "k8s_pod_name"),
        "node_conntrack_entries": ("node-conntrack", "instance"),
        "node_conntrack_limit": ("node-conntrack-limit", "instance"),
        "node_tcp_inuse": ("node-tcp-inuse", "instance"),
        "node_tcp_time_wait": ("node-tcp-timewait", "instance"),
        "node_load1": ("node-load1", "instance"),
        "otel_collector_up": ("otel-collector-up", "pod"),
    }
    counters = {
        "otel_target_network_errors": ("otel-target-network-errors", "k8s_pod_name"),
        "node_tcp_retransmits": ("node-retransmits", "instance"),
        "node_softnet_drops": ("node-softnet-drops", "instance"),
        "pod_receive_errors": ("pod-receive-errors", "pod"),
        "pod_transmit_errors": ("pod-transmit-errors", "pod"),
        "pod_receive_drops": ("pod-receive-drops", "pod"),
        "pod_transmit_drops": ("pod-transmit-drops", "pod"),
    }
    result = {
        "schema_version": 1,
        "kind": "llm-d-sc-external-telemetry-summary",
        "application_code_modified": False,
        "cell_start_epoch": start,
        "cell_completion_epoch": end,
        "gauges": {
            name: gauge_summary(load(query), start, end, label)
            for name, (query, label) in gauges.items()
        },
        "counters": {
            name: counter_delta_summary(load(query), start, end, label)
            for name, (query, label) in counters.items()
        },
    }
    critical = [
        result["gauges"]["otel_target_cpu_cores"],
        result["gauges"]["otel_target_memory_working_set_bytes"],
        result["gauges"]["node_conntrack_entries"],
        result["gauges"]["node_conntrack_limit"],
        result["gauges"]["otel_collector_up"],
        result["counters"]["node_tcp_retransmits"],
        result["counters"]["node_softnet_drops"],
    ]
    result["critical_signals_complete"] = all(item["available"] for item in critical)
    args.output_json.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")


if __name__ == "__main__":
    main()
