#!/usr/bin/env python3
"""Summarize per-Pod network counter deltas around one transport cell."""

from __future__ import annotations

import argparse
import json
import math
from pathlib import Path


def require(condition: bool, message: str) -> None:
    if not condition:
        raise ValueError(message)


def summarize(payload: dict, start_epoch: float, completion_epoch: float, expected: set[str]) -> dict:
    require(payload.get("status") == "success", "Thanos query was not successful")
    series = payload.get("data", {}).get("result", [])
    by_pod = {item.get("metric", {}).get("pod"): item.get("values", []) for item in series}
    require(set(by_pod) == expected, f"network series mismatch: expected {sorted(expected)}, got {sorted(by_pod)}")

    rows = []
    for pod in sorted(expected):
        samples = [(float(ts), float(value)) for ts, value in by_pod[pod]]
        before = [sample for sample in samples if sample[0] <= start_epoch]
        after = [sample for sample in samples if sample[0] >= completion_epoch]
        require(before, f"{pod}: no counter sample at or before cell start")
        require(after, f"{pod}: no counter sample at or after cell completion")
        baseline = before[-1]
        # Scrape samples can retain the pre-cell value immediately after a
        # short Job completes. Cells remain isolated until the full bracket
        # is collected, so the last post-completion sample is the terminal.
        terminal = after[-1]
        delta = terminal[1] - baseline[1]
        require(delta >= 0, f"{pod}: network counter reset during cell")
        rows.append(
            {
                "pod": pod,
                "baseline_epoch": baseline[0],
                "terminal_epoch": terminal[0],
                "baseline_bytes": baseline[1],
                "terminal_bytes": terminal[1],
                "receive_bytes_delta": delta,
            }
        )

    total = sum(row["receive_bytes_delta"] for row in rows)
    require(total > 0, "aggregate receive-byte delta is zero")
    shares = [row["receive_bytes_delta"] / total for row in rows]
    ideal = 1.0 / len(rows)
    mean = sum(shares) / len(shares)
    cv = math.sqrt(sum((share - mean) ** 2 for share in shares) / len(shares)) / mean
    for row, share in zip(rows, shares):
        row["share"] = share

    return {
        "schema_version": 1,
        "method": "last pre-start and last isolated post-completion container_network_receive_bytes_total samples",
        "cell_start_epoch": start_epoch,
        "cell_completion_epoch": completion_epoch,
        "total_receive_bytes_delta": total,
        "ideal_share": ideal,
        "coefficient_of_variation": cv,
        "max_share_over_ideal": max(shares) / ideal,
        "pods": rows,
        "interpretation": (
            "Fixed request and response payloads make receive-byte share a transport-distribution proxy; "
            "it is not an application request counter."
        ),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("query_json", type=Path)
    parser.add_argument("job_json", type=Path)
    parser.add_argument("pods_json", type=Path)
    parser.add_argument("output_json", type=Path)
    args = parser.parse_args()

    payload = json.loads(args.query_json.read_text())
    job = json.loads(args.job_json.read_text())
    pods = json.loads(args.pods_json.read_text())
    start = job.get("status", {}).get("startTime")
    completion = job.get("status", {}).get("completionTime")
    require(bool(start and completion), "Job startTime/completionTime are required")

    from datetime import datetime

    start_epoch = datetime.fromisoformat(start.replace("Z", "+00:00")).timestamp()
    completion_epoch = datetime.fromisoformat(completion.replace("Z", "+00:00")).timestamp()
    expected = {item["metadata"]["name"] for item in pods.get("items", [])}
    require(bool(expected), "expected Pod list is empty")
    result = summarize(payload, start_epoch, completion_epoch, expected)
    args.output_json.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")


if __name__ == "__main__":
    main()
