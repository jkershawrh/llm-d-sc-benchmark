#!/usr/bin/env python3
"""Take minimally invasive PID-1 scheduler snapshots around an Arena SC plateau.

This helper performs two read-only ``kubectl exec`` operations per target Pod.
It changes no Kubernetes object and writes nothing in the container, but each
exec briefly adds a shell process to the target cgroup.  Its overhead must be
measured with paired profiler-OFF/ON cells before using the deltas as evidence.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import json
import subprocess
import sys
import time
from collections import Counter
from pathlib import Path
from typing import Any


SNAPSHOT_SCRIPT = r"""set -eu
cpuset=unavailable
IFS= read -r cpuset < /sys/fs/cgroup/cpuset.cpus.effective || :
printf 'META\tcpuset_cpus_effective\t%s\n' "$cpuset"
while IFS=' ' read -r key value rest; do
  case "$key" in
    usage_usec|user_usec|system_usec|nr_periods|nr_throttled|throttled_usec)
      printf 'CGROUP\t%s\t%s\n' "$key" "$value"
      ;;
  esac
done < /sys/fs/cgroup/cpu.stat
for task in /proc/1/task/[0-9]*; do
  tid=${task##*/}
  comm=unavailable
  wchan=unavailable
  run_ns=-1
  runqueue_wait_ns=-1
  timeslices=-1
  migrations=-1
  switches=-1
  voluntary=-1
  involuntary=-1
  IFS= read -r comm < "$task/comm" || :
  IFS= read -r wchan < "$task/wchan" || :
  IFS=' ' read -r run_ns runqueue_wait_ns timeslices < "$task/schedstat" || :
  while IFS=' ' read -r key colon value rest; do
    case "$key" in
      se.nr_migrations) migrations=$value ;;
      nr_switches) switches=$value ;;
      nr_voluntary_switches) voluntary=$value ;;
      nr_involuntary_switches) involuntary=$value ;;
    esac
  done < "$task/sched"
  printf 'TASK\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
    "$tid" "$comm" "$wchan" "$run_ns" "$runqueue_wait_ns" "$timeslices" \
    "$migrations" "$switches" "$voluntary" "$involuntary"
done
"""


TASK_COUNTERS = (
    "run_ns",
    "runqueue_wait_ns",
    "timeslices",
    "migrations",
    "switches",
    "voluntary_switches",
    "involuntary_switches",
)


def load_json(path: Path) -> Any:
    with path.open(encoding="utf-8") as stream:
        return json.load(stream)


def run_kubectl(kubeconfig: str, arguments: list[str], stdin: str | None = None) -> str:
    result = subprocess.run(
        ["kubectl", "--kubeconfig", kubeconfig, *arguments],
        input=stdin,
        text=True,
        capture_output=True,
        check=False,
    )
    if result.returncode != 0:
        detail = result.stderr.strip() or result.stdout.strip()
        raise RuntimeError(f"kubectl exit {result.returncode}: {detail}")
    return result.stdout


def target_snapshot(kubeconfig: str, namespace: str, pod_names: list[str], container: str) -> dict[str, Any]:
    document = json.loads(
        run_kubectl(
            kubeconfig,
            ["-n", namespace, "get", "pod", *pod_names, "-o", "json"],
        )
    )
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


def parse_snapshot(payload: str) -> dict[str, Any]:
    document: dict[str, Any] = {"metadata": {}, "cgroup": {}, "tasks": {}}
    for line in payload.splitlines():
        fields = line.split("\t")
        if not fields:
            continue
        if fields[0] == "META" and len(fields) == 3:
            document["metadata"][fields[1]] = fields[2]
        elif fields[0] == "CGROUP" and len(fields) == 3:
            document["cgroup"][fields[1]] = int(fields[2])
        elif fields[0] == "TASK" and len(fields) == 11:
            tid = fields[1]
            document["tasks"][tid] = {
                "comm": fields[2],
                "wchan": fields[3],
                "run_ns": int(fields[4]),
                "runqueue_wait_ns": int(fields[5]),
                "timeslices": int(fields[6]),
                "migrations": int(fields[7]),
                "switches": int(fields[8]),
                "voluntary_switches": int(fields[9]),
                "involuntary_switches": int(fields[10]),
            }
        else:
            raise ValueError(f"malformed profiler output line: {line[:160]}")
    required_cgroup = {
        "usage_usec",
        "user_usec",
        "system_usec",
        "nr_periods",
        "nr_throttled",
        "throttled_usec",
    }
    if not document["tasks"]:
        raise ValueError("PID 1 task list is empty")
    if not required_cgroup.issubset(document["cgroup"]):
        raise ValueError("cgroup CPU counters are incomplete")
    if any(task[counter] < 0 for task in document["tasks"].values() for counter in TASK_COUNTERS):
        raise ValueError("one or more scheduler counters were unavailable")
    return document


def capture_one(
    kubeconfig: str, namespace: str, pod: str, container: str, phase: str
) -> dict[str, Any]:
    before_ms = time.time_ns() // 1_000_000
    payload = run_kubectl(
        kubeconfig,
        ["-n", namespace, "exec", pod, "-c", container, "--", "sh", "-s"],
        stdin=SNAPSHOT_SCRIPT,
    )
    after_ms = time.time_ns() // 1_000_000
    return {
        "phase": phase,
        "pod": pod,
        "client_before_epoch_ms": before_ms,
        "client_after_epoch_ms": after_ms,
        "exec_elapsed_ms": after_ms - before_ms,
        "snapshot": parse_snapshot(payload),
        "raw": payload,
    }


def capture_all(
    kubeconfig: str, namespace: str, pod_names: list[str], container: str, phase: str
) -> dict[str, dict[str, Any]]:
    captures: dict[str, dict[str, Any]] = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(pod_names)) as executor:
        pending = {
            executor.submit(capture_one, kubeconfig, namespace, pod, container, phase): pod
            for pod in pod_names
        }
        for future in concurrent.futures.as_completed(pending):
            pod = pending[future]
            captures[pod] = future.result()
    return captures


def derive_pod(start: dict[str, Any], end: dict[str, Any]) -> dict[str, Any]:
    start_snapshot = start["snapshot"]
    end_snapshot = end["snapshot"]
    start_tids = set(start_snapshot["tasks"])
    end_tids = set(end_snapshot["tasks"])
    common_tids = sorted(start_tids & end_tids, key=int)
    tasks_stable = start_tids == end_tids
    counter_deltas = {counter: 0 for counter in TASK_COUNTERS}
    counter_regression = False
    for tid in common_tids:
        for counter in TASK_COUNTERS:
            delta = end_snapshot["tasks"][tid][counter] - start_snapshot["tasks"][tid][counter]
            counter_regression = counter_regression or delta < 0
            counter_deltas[counter] += delta
    cgroup_delta = {
        key: end_snapshot["cgroup"][key] - start_snapshot["cgroup"][key]
        for key in start_snapshot["cgroup"]
    }
    cgroup_regression = any(value < 0 for value in cgroup_delta.values())
    return {
        "pod": start["pod"],
        "cpuset_cpus_effective": {
            "start": start_snapshot["metadata"].get("cpuset_cpus_effective"),
            "end": end_snapshot["metadata"].get("cpuset_cpus_effective"),
        },
        "thread_count": {"start": len(start_tids), "end": len(end_tids)},
        "task_ids_stable": tasks_stable,
        "tasks_started": sorted(end_tids - start_tids, key=int),
        "tasks_ended": sorted(start_tids - end_tids, key=int),
        "common_tasks": len(common_tids),
        "scheduler_delta": counter_deltas,
        "cgroup_cpu_delta": cgroup_delta,
        "wchan_snapshot": {
            "start": dict(Counter(task["wchan"] for task in start_snapshot["tasks"].values())),
            "end": dict(Counter(task["wchan"] for task in end_snapshot["tasks"].values())),
            "interpretation": "two point-in-time histograms; not futex time or total off-CPU time",
        },
        "exec_elapsed_ms": {"start": start["exec_elapsed_ms"], "end": end["exec_elapsed_ms"]},
        "valid": tasks_stable
        and not counter_regression
        and not cgroup_regression
        and start_snapshot["metadata"].get("cpuset_cpus_effective")
        == end_snapshot["metadata"].get("cpuset_cpus_effective"),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--kubeconfig", default="/tmp/llm-d-sc-arena-kubeconfig")
    parser.add_argument("--targets-json", type=Path, required=True)
    parser.add_argument("--container", default="llm-d-sc")
    parser.add_argument("--start-epoch-ms", type=int, required=True)
    parser.add_argument("--duration-seconds", type=int, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    if args.duration_seconds <= 0:
        parser.error("duration must be positive")
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
    args.output_dir.mkdir(parents=True)

    before = target_snapshot(args.kubeconfig, namespace, pod_names, args.container)
    start_delay = args.start_epoch_ms / 1000.0 - time.time()
    if start_delay > 0:
        time.sleep(start_delay)
    late_by_seconds = max(0.0, time.time() - args.start_epoch_ms / 1000.0)
    start_captures = capture_all(
        args.kubeconfig, namespace, pod_names, args.container, "start"
    )
    end_epoch = args.start_epoch_ms / 1000.0 + args.duration_seconds
    end_delay = end_epoch - time.time()
    if end_delay > 0:
        time.sleep(end_delay)
    end_late_by_seconds = max(0.0, time.time() - end_epoch)
    end_captures = capture_all(args.kubeconfig, namespace, pod_names, args.container, "end")
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

    pod_summaries = [derive_pod(start_captures[pod], end_captures[pod]) for pod in pod_names]
    valid = (
        identity_clean
        and all(pod["valid"] for pod in pod_summaries)
        and late_by_seconds <= 2.0
        and end_late_by_seconds <= 2.0
    )
    raw_document = {
        "schema_version": 1,
        "start": start_captures,
        "end": end_captures,
    }
    (args.output_dir / "proc-sched-raw.json").write_text(
        json.dumps(raw_document, indent=2) + "\n", encoding="utf-8"
    )
    document = {
        "schema_version": 1,
        "source": "two read-only target exec snapshots; adds a transient shell to each target cgroup",
        "namespace": namespace,
        "container": args.container,
        "plateau_start_epoch_ms": args.start_epoch_ms,
        "plateau_end_epoch_ms": args.start_epoch_ms + args.duration_seconds * 1000,
        "late_by_seconds": late_by_seconds,
        "end_late_by_seconds": end_late_by_seconds,
        "expected_targets": expected,
        "targets_before": before,
        "targets_after": after,
        "identity_clean": identity_clean,
        "pods": pod_summaries,
        "valid": valid,
        "limitations": [
            "the exec shell briefly consumes target-cgroup CPU and requires a paired profiler-overhead A/B",
            "only tasks present at both boundaries can contribute complete scheduler deltas",
            "schedstat runqueue wait excludes sleeping/futex time",
            "wchan values are boundary snapshots and cannot quantify futex or total off-CPU time",
            "this helper exposes no PMU cycles/instructions or effective-frequency measurement",
        ],
    }
    (args.output_dir / "proc-sched-summary.json").write_text(
        json.dumps(document, indent=2) + "\n", encoding="utf-8"
    )
    (args.output_dir / "proc-sched-status.json").write_text(
        json.dumps({"schema_version": 1, "status": "completed", "valid": valid}, indent=2)
        + "\n",
        encoding="utf-8",
    )
    json.dump(document, sys.stdout, indent=2)
    print()
    return 0 if valid else 3


if __name__ == "__main__":
    raise SystemExit(main())
