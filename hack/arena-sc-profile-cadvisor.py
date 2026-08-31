#!/usr/bin/env python3
"""Capture read-only cAdvisor profiler signals for fixed Arena target Pods.

The sampler runs on the benchmark client, not in the target Pods.  It performs
one read-only kubelet cAdvisor request per target node per interval and retains
only the named target containers.  No Kubernetes object is created or patched.
"""

from __future__ import annotations

import argparse
import json
import math
import re
import subprocess
import sys
import time
from collections import defaultdict
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import quote


METRICS = {
    "container_cpu_usage_seconds_total",
    "container_cpu_user_seconds_total",
    "container_cpu_system_seconds_total",
    "container_cpu_cfs_periods_total",
    "container_cpu_cfs_throttled_periods_total",
    "container_cpu_cfs_throttled_seconds_total",
    "container_pressure_cpu_waiting_seconds_total",
    "container_pressure_cpu_stalled_seconds_total",
    "container_threads",
    "container_tasks_state",
}
COUNTERS = METRICS - {"container_threads", "container_tasks_state"}
LINE_RE = re.compile(
    r"^([A-Za-z_:][A-Za-z0-9_:]*)(?:\{(.*)\})?\s+([^\s]+)(?:\s+([0-9]+))?$"
)
LABEL_RE = re.compile(r'(\w+)="((?:\\.|[^"\\])*)"(?:,|$)')


def load_json(path: Path) -> Any:
    with path.open(encoding="utf-8") as stream:
        return json.load(stream)


def run_kubectl(kubeconfig: str, arguments: list[str]) -> str:
    command = ["kubectl", "--kubeconfig", kubeconfig, *arguments]
    result = subprocess.run(command, text=True, capture_output=True, check=False)
    if result.returncode != 0:
        detail = result.stderr.strip() or result.stdout.strip()
        raise RuntimeError(f"kubectl exit {result.returncode}: {detail}")
    return result.stdout


def parse_labels(raw: str | None) -> dict[str, str]:
    if not raw:
        return {}
    labels: dict[str, str] = {}
    position = 0
    while position < len(raw):
        match = LABEL_RE.match(raw, position)
        if match is None:
            raise ValueError(f"unsupported Prometheus label set near: {raw[position:position + 80]}")
        labels[match.group(1)] = json.loads(f'"{match.group(2)}"')
        position = match.end()
    return labels


def parse_metrics(
    payload: str,
    *,
    namespace: str,
    container: str,
    pod_names: set[str],
    node: str,
    observed_at_ms: int,
) -> Iterable[dict[str, Any]]:
    for line in payload.splitlines():
        if not line or line.startswith("#"):
            continue
        match = LINE_RE.match(line)
        if match is None or match.group(1) not in METRICS:
            continue
        labels = parse_labels(match.group(2))
        if (
            labels.get("namespace") != namespace
            or labels.get("container") != container
            or labels.get("pod") not in pod_names
        ):
            continue
        metric = match.group(1)
        if metric == "container_cpu_usage_seconds_total" and labels.get("cpu") != "total":
            continue
        value = float(match.group(3))
        if not math.isfinite(value):
            continue
        yield {
            "observed_at_epoch_ms": observed_at_ms,
            "source_epoch_ms": None if match.group(4) is None else int(match.group(4)),
            "node": node,
            "pod": labels["pod"],
            "container": container,
            "metric": metric,
            "state": labels.get("state"),
            "value": value,
        }


def target_snapshot(kubeconfig: str, namespace: str, pod_names: list[str], container: str) -> dict[str, Any]:
    payload = run_kubectl(
        kubeconfig,
        ["-n", namespace, "get", "pod", *pod_names, "-o", "json"],
    )
    document = json.loads(payload)
    items = document.get("items", [document])
    snapshot: dict[str, Any] = {}
    for item in items:
        statuses = {
            status["name"]: status for status in item.get("status", {}).get("containerStatuses", [])
        }
        status = statuses.get(container, {})
        snapshot[item["metadata"]["name"]] = {
            "uid": item["metadata"]["uid"],
            "node": item["spec"].get("nodeName"),
            "phase": item.get("status", {}).get("phase"),
            "image_id": status.get("imageID"),
            "restart_count": status.get("restartCount"),
            "ready": status.get("ready"),
        }
    return snapshot


def unique_series(observations: list[dict[str, Any]]) -> list[tuple[int, float]]:
    by_timestamp: dict[int, float] = {}
    for observation in observations:
        timestamp = observation["source_epoch_ms"]
        if timestamp is not None:
            by_timestamp[int(timestamp)] = float(observation["value"])
    return sorted(by_timestamp.items())


def interpolate(series: list[tuple[int, float]], timestamp_ms: int) -> float | None:
    before: tuple[int, float] | None = None
    for point in series:
        if point[0] == timestamp_ms:
            return point[1]
        if point[0] < timestamp_ms:
            before = point
            continue
        if before is None:
            return None
        fraction = (timestamp_ms - before[0]) / (point[0] - before[0])
        return before[1] + fraction * (point[1] - before[1])
    return None


def summarize(
    observations: list[dict[str, Any]],
    *,
    pod_names: list[str],
    plateau_start_ms: int,
    plateau_end_ms: int,
) -> tuple[list[dict[str, Any]], bool]:
    grouped: dict[tuple[str, str, str | None], list[dict[str, Any]]] = defaultdict(list)
    for observation in observations:
        grouped[(observation["pod"], observation["metric"], observation["state"])].append(
            observation
        )

    pod_summaries: list[dict[str, Any]] = []
    all_valid = True
    for pod in pod_names:
        counter_deltas: dict[str, float | None] = {}
        counter_points: dict[str, int] = {}
        maximum_source_gap_ms = 0
        boundary_coverage = True
        for metric in sorted(COUNTERS):
            series = unique_series(grouped[(pod, metric, None)])
            counter_points[metric] = len(series)
            if len(series) > 1:
                maximum_source_gap_ms = max(
                    maximum_source_gap_ms,
                    max(series[index][0] - series[index - 1][0] for index in range(1, len(series))),
                )
            start_value = interpolate(series, plateau_start_ms)
            end_value = interpolate(series, plateau_end_ms)
            if start_value is None or end_value is None:
                boundary_coverage = False
                counter_deltas[metric] = None
            else:
                counter_deltas[metric] = end_value - start_value

        plateau_threads = [
            value
            for timestamp, value in unique_series(grouped[(pod, "container_threads", None)])
            if plateau_start_ms <= timestamp <= plateau_end_ms
        ]
        task_peaks: dict[str, float] = {}
        states = {
            key[2]
            for key in grouped
            if key[0] == pod and key[1] == "container_tasks_state" and key[2] is not None
        }
        for state in sorted(states):
            values = [
                value
                for timestamp, value in unique_series(grouped[(pod, "container_tasks_state", state)])
                if plateau_start_ms <= timestamp <= plateau_end_ms
            ]
            if values:
                task_peaks[state] = max(values)

        usage = counter_deltas["container_cpu_usage_seconds_total"]
        user = counter_deltas["container_cpu_user_seconds_total"]
        system = counter_deltas["container_cpu_system_seconds_total"]
        periods = counter_deltas["container_cpu_cfs_periods_total"]
        throttled_periods = counter_deltas["container_cpu_cfs_throttled_periods_total"]
        pod_valid = boundary_coverage and bool(plateau_threads)
        all_valid = all_valid and pod_valid
        pod_summaries.append(
            {
                "pod": pod,
                "boundary_coverage": boundary_coverage,
                "valid": pod_valid,
                "unique_counter_points": counter_points,
                "maximum_source_gap_ms": maximum_source_gap_ms,
                "average_cpu_cores": None
                if usage is None
                else usage / ((plateau_end_ms - plateau_start_ms) / 1000.0),
                "cpu_seconds": {"usage": usage, "user": user, "system": system},
                "system_cpu_percent": None
                if user is None or system is None or user + system == 0
                else system / (user + system) * 100.0,
                "throttled_period_ratio": None
                if periods in (None, 0) or throttled_periods is None
                else throttled_periods / periods,
                "cpu_pressure_waiting_seconds": counter_deltas[
                    "container_pressure_cpu_waiting_seconds_total"
                ],
                "cpu_pressure_stalled_seconds": counter_deltas[
                    "container_pressure_cpu_stalled_seconds_total"
                ],
                "threads": {
                    "samples": len(plateau_threads),
                    "min": None if not plateau_threads else min(plateau_threads),
                    "max": None if not plateau_threads else max(plateau_threads),
                },
                "task_state_peak": task_peaks,
            }
        )
    return pod_summaries, all_valid


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--kubeconfig", default="/tmp/llm-d-sc-arena-kubeconfig")
    parser.add_argument("--targets-json", type=Path, required=True)
    parser.add_argument("--container", default="llm-d-sc")
    parser.add_argument("--start-epoch-ms", type=int, required=True)
    parser.add_argument("--duration-seconds", type=int, required=True)
    parser.add_argument("--interval-seconds", type=float, default=5.0)
    parser.add_argument("--padding-seconds", type=float, default=20.0)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    if args.duration_seconds <= 0 or args.interval_seconds <= 0 or args.padding_seconds < 0:
        parser.error("duration and interval must be positive; padding cannot be negative")
    if args.output_dir.exists():
        parser.error(f"output directory already exists: {args.output_dir}")

    targets = load_json(args.targets_json)
    items = targets.get("items", [])
    if not items:
        parser.error("targets JSON contains no Pods")
    namespaces = {item["metadata"]["namespace"] for item in items}
    if len(namespaces) != 1:
        parser.error("all targets must be in one namespace")
    namespace = namespaces.pop()
    expected: dict[str, dict[str, Any]] = {}
    for item in items:
        statuses = {
            status["name"]: status
            for status in item.get("status", {}).get("containerStatuses", [])
        }
        if args.container not in statuses:
            parser.error(
                f"target {item['metadata']['name']} has no {args.container} container status"
            )
        expected[item["metadata"]["name"]] = {
            "uid": item["metadata"]["uid"],
            "node": item["spec"]["nodeName"],
            "image_id": statuses[args.container].get("imageID"),
        }
    pod_names = sorted(expected)
    nodes: dict[str, set[str]] = defaultdict(set)
    for pod, evidence in expected.items():
        nodes[evidence["node"]].add(pod)

    args.output_dir.mkdir(parents=True)
    raw_path = args.output_dir / "cadvisor-profile.ndjson"
    summary_path = args.output_dir / "cadvisor-profile-summary.json"
    status_path = args.output_dir / "cadvisor-profile-status.json"
    observations: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    before = target_snapshot(args.kubeconfig, namespace, pod_names, args.container)
    collect_start = args.start_epoch_ms / 1000.0 - args.padding_seconds
    collect_end = (
        args.start_epoch_ms / 1000.0 + args.duration_seconds + args.padding_seconds
    )
    late_by_seconds = max(0.0, time.time() - collect_start)

    with raw_path.open("x", encoding="utf-8") as raw_stream:
        scheduled = collect_start
        while scheduled <= collect_end + 1e-9:
            delay = scheduled - time.time()
            if delay > 0:
                time.sleep(delay)
            for node, node_pods in sorted(nodes.items()):
                observed_at_ms = time.time_ns() // 1_000_000
                try:
                    path = f"/api/v1/nodes/{quote(node, safe='')}/proxy/metrics/cadvisor"
                    payload = run_kubectl(args.kubeconfig, ["get", "--raw", path])
                    selected = list(
                        parse_metrics(
                            payload,
                            namespace=namespace,
                            container=args.container,
                            pod_names=node_pods,
                            node=node,
                            observed_at_ms=observed_at_ms,
                        )
                    )
                    record = {
                        "schema_version": 1,
                        "scheduled_epoch_ms": round(scheduled * 1000),
                        "observed_at_epoch_ms": observed_at_ms,
                        "node": node,
                        "metrics": selected,
                    }
                    observations.extend(selected)
                except Exception as error:  # Preserve an auditable gap and keep sampling.
                    record = {
                        "schema_version": 1,
                        "scheduled_epoch_ms": round(scheduled * 1000),
                        "observed_at_epoch_ms": observed_at_ms,
                        "node": node,
                        "error": str(error),
                    }
                    errors.append(record)
                raw_stream.write(json.dumps(record, separators=(",", ":")) + "\n")
                raw_stream.flush()
            scheduled += args.interval_seconds

    after = target_snapshot(args.kubeconfig, namespace, pod_names, args.container)
    identity_clean = set(before) == set(expected) == set(after)
    for pod, evidence in expected.items():
        identity_clean = identity_clean and (
            before.get(pod, {}).get("uid") == evidence["uid"]
            and after.get(pod, {}).get("uid") == evidence["uid"]
            and before.get(pod, {}).get("node") == evidence["node"]
            and after.get(pod, {}).get("node") == evidence["node"]
            and before.get(pod, {}).get("image_id") == evidence["image_id"]
            and after.get(pod, {}).get("image_id") == evidence["image_id"]
            and before.get(pod, {}).get("restart_count") == 0
            and after.get(pod, {}).get("restart_count") == 0
            and before.get(pod, {}).get("ready") is True
            and after.get(pod, {}).get("ready") is True
        )

    plateau_end_ms = args.start_epoch_ms + args.duration_seconds * 1000
    pod_summaries, metrics_valid = summarize(
        observations,
        pod_names=pod_names,
        plateau_start_ms=args.start_epoch_ms,
        plateau_end_ms=plateau_end_ms,
    )
    valid = identity_clean and metrics_valid and not errors and late_by_seconds <= args.interval_seconds
    document = {
        "schema_version": 1,
        "source": "read-only kubelet cAdvisor node proxy; no target exec and no Kubernetes mutation",
        "namespace": namespace,
        "container": args.container,
        "plateau_start_epoch_ms": args.start_epoch_ms,
        "plateau_end_epoch_ms": plateau_end_ms,
        "interval_seconds": args.interval_seconds,
        "padding_seconds": args.padding_seconds,
        "late_by_seconds": late_by_seconds,
        "expected_targets": expected,
        "targets_before": before,
        "targets_after": after,
        "identity_clean": identity_clean,
        "poll_errors": errors,
        "pods": pod_summaries,
        "valid": valid,
        "limitations": [
            "cAdvisor exposes no per-container context-switch, migration, PMU cycle/instruction, futex, off-CPU, or effective-frequency metric on Arena",
            "counter boundaries are linearly interpolated from cAdvisor source timestamps and require pre/post plateau coverage",
        ],
    }
    summary_path.write_text(json.dumps(document, indent=2) + "\n", encoding="utf-8")
    status_path.write_text(
        json.dumps({"schema_version": 1, "status": "completed", "valid": valid}, indent=2)
        + "\n",
        encoding="utf-8",
    )
    json.dump(document, sys.stdout, indent=2)
    print()
    return 0 if valid else 3


if __name__ == "__main__":
    raise SystemExit(main())
