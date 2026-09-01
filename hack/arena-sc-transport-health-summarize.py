#!/usr/bin/env python3
"""Summarize target health changes across one transport benchmark cell."""

from __future__ import annotations

import argparse
from collections import Counter
import json
from pathlib import Path


def load(path: Path) -> dict:
    return json.loads(path.read_text())


def pod_state(payload: dict) -> dict[str, dict]:
    rows = {}
    for pod in payload.get("items", []):
        name = pod.get("metadata", {}).get("name")
        if not name:
            continue
        conditions = pod.get("status", {}).get("conditions", [])
        statuses = pod.get("status", {}).get("containerStatuses", [])
        rows[name] = {
            "uid": pod.get("metadata", {}).get("uid"),
            "pod_ip": pod.get("status", {}).get("podIP"),
            "node": pod.get("spec", {}).get("nodeName"),
            "ready": any(
                condition.get("type") == "Ready" and condition.get("status") == "True"
                for condition in conditions
            ),
            "restart_count": sum(status.get("restartCount", 0) for status in statuses),
        }
    return rows


def warning_counts(payload: dict, target_names: set[str]) -> dict[str, dict]:
    rows = {}
    for event in payload.get("items", []):
        involved = event.get("involvedObject", {})
        if event.get("type") != "Warning" or involved.get("name") not in target_names:
            continue
        metadata = event.get("metadata", {})
        key = metadata.get("uid") or metadata.get("name")
        if not key:
            continue
        message = event.get("message", "")
        probe = (
            "readiness"
            if message.startswith("Readiness probe failed")
            else "liveness"
            if message.startswith("Liveness probe failed")
            else "other"
        )
        failure = (
            "timeout"
            if "i/o timeout" in message
            else "connection_refused"
            if "connection refused" in message
            else "other"
        )
        rows[key] = {
            "count": int(event.get("count", 1)),
            "pod": involved.get("name"),
            "reason": event.get("reason"),
            "probe": probe,
            "failure": failure,
            "message": message,
            "first_timestamp": event.get("firstTimestamp") or event.get("eventTime"),
            "last_timestamp": event.get("lastTimestamp") or event.get("eventTime"),
        }
    return rows


def summarize(before_pods: dict, after_pods: dict, before_events: dict, after_events: dict) -> dict:
    before = pod_state(before_pods)
    after = pod_state(after_pods)
    before_names = set(before)
    after_names = set(after)
    identity_stable = before_names == after_names and all(
        before[name]["uid"] == after[name]["uid"]
        and before[name]["pod_ip"] == after[name]["pod_ip"]
        for name in before_names & after_names
    )

    pods = []
    for name in sorted(before_names | after_names):
        old = before.get(name)
        new = after.get(name)
        pods.append(
            {
                "name": name,
                "before": old,
                "after": new,
                "restart_delta": (
                    new["restart_count"] - old["restart_count"] if old and new else None
                ),
            }
        )

    old_events = warning_counts(before_events, before_names)
    new_events = warning_counts(after_events, after_names)
    event_deltas = []
    for key, event in sorted(new_events.items()):
        delta = event["count"] - old_events.get(key, {}).get("count", 0)
        if delta > 0:
            event_deltas.append({"event_id": key, "delta": delta, **event})

    warning_delta_count = sum(event["delta"] for event in event_deltas)
    warnings_by_probe = Counter()
    warnings_by_failure = Counter()
    affected_pods = set()
    for event in event_deltas:
        warnings_by_probe[event["probe"]] += event["delta"]
        warnings_by_failure[event["failure"]] += event["delta"]
        affected_pods.add(event["pod"])
    restart_delta_count = sum(
        max(pod["restart_delta"], 0)
        for pod in pods
        if pod["restart_delta"] is not None
    )
    before_ready = len(before) == 5 and all(pod["ready"] for pod in before.values())
    after_ready = len(after) == 5 and all(pod["ready"] for pod in after.values())
    health_slo_pass = bool(
        identity_stable
        and before_ready
        and after_ready
        and restart_delta_count == 0
        and warning_delta_count == 0
    )
    return {
        "schema_version": 1,
        "kind": "llm-d-sc-transport-health-summary",
        "identity_stable": identity_stable,
        "before_ready": before_ready,
        "after_ready": after_ready,
        "restart_delta_count": restart_delta_count,
        "warning_event_delta_count": warning_delta_count,
        "warning_event_deltas_by_probe": dict(sorted(warnings_by_probe.items())),
        "warning_event_deltas_by_failure": dict(sorted(warnings_by_failure.items())),
        "warning_affected_pods": sorted(affected_pods),
        "health_slo_pass": health_slo_pass,
        "pods": pods,
        "warning_event_deltas": event_deltas,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("before_pods", type=Path)
    parser.add_argument("after_pods", type=Path)
    parser.add_argument("before_events", type=Path)
    parser.add_argument("after_events", type=Path)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    result = summarize(
        load(args.before_pods),
        load(args.after_pods),
        load(args.before_events),
        load(args.after_events),
    )
    args.output.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
