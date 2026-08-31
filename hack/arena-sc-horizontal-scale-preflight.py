#!/usr/bin/env python3
"""Evaluate quota, node, and storage headroom for an SC horizontal-scale rung.

The program is deliberately cluster-free.  The orchestrator captures immutable
Kubernetes JSON snapshots and passes them here; a failed or incomplete input is
a load-denying result rather than an estimate.
"""

from __future__ import annotations

import argparse
import json
import math
import re
import sys
from pathlib import Path
from typing import Any


CPU_MULTIPLIERS = {"n": 1e-6, "u": 1e-3, "m": 1.0, "": 1000.0}
BINARY_MEMORY = {
    "Ki": 2**10,
    "Mi": 2**20,
    "Gi": 2**30,
    "Ti": 2**40,
    "Pi": 2**50,
    "Ei": 2**60,
}
DECIMAL_MEMORY = {
    "k": 10**3,
    "K": 10**3,
    "M": 10**6,
    "G": 10**9,
    "T": 10**12,
    "P": 10**15,
    "E": 10**18,
    "": 1,
}


def cpu_millicores(value: str | int | float | None) -> int:
    if value in (None, ""):
        return 0
    match = re.fullmatch(r"([0-9]+(?:\.[0-9]+)?)(n|u|m)?", str(value))
    if not match:
        raise ValueError(f"unsupported CPU quantity: {value!r}")
    number = float(match.group(1))
    unit = match.group(2) or ""
    return math.ceil(number * CPU_MULTIPLIERS[unit] - 1e-12)


def memory_bytes(value: str | int | float | None) -> int:
    if value in (None, ""):
        return 0
    match = re.fullmatch(r"([0-9]+(?:\.[0-9]+)?)([KMGTPE]i|[kKMGTPE])?", str(value))
    if not match:
        raise ValueError(f"unsupported memory quantity: {value!r}")
    number = float(match.group(1))
    unit = match.group(2) or ""
    multiplier = BINARY_MEMORY.get(unit, DECIMAL_MEMORY.get(unit))
    if multiplier is None:
        raise ValueError(f"unsupported memory unit: {unit!r}")
    return math.ceil(number * multiplier - 1e-12)


def scalar(value: str | int | float | None) -> int:
    if value in (None, ""):
        return 0
    number = float(str(value))
    if not number.is_integer():
        raise ValueError(f"expected integral scalar quantity, got {value!r}")
    return int(number)


def resource_value(resources: dict[str, Any], name: str) -> int:
    value = resources.get(name, "0")
    if name.endswith("cpu") or name == "cpu":
        return cpu_millicores(value)
    if name.endswith("memory") or name == "memory":
        return memory_bytes(value)
    return scalar(value)


def pod_scheduler_requests(pod: dict[str, Any]) -> dict[str, int]:
    spec = pod.get("spec", {})
    regular_cpu = 0
    regular_memory = 0
    for container in spec.get("containers", []):
        requests = container.get("resources", {}).get("requests", {})
        regular_cpu += cpu_millicores(requests.get("cpu"))
        regular_memory += memory_bytes(requests.get("memory"))
    init_cpu = 0
    init_memory = 0
    for container in spec.get("initContainers", []):
        requests = container.get("resources", {}).get("requests", {})
        init_cpu = max(init_cpu, cpu_millicores(requests.get("cpu")))
        init_memory = max(init_memory, memory_bytes(requests.get("memory")))
    overhead = spec.get("overhead", {})
    return {
        "cpu_millicores": max(regular_cpu, init_cpu) + cpu_millicores(overhead.get("cpu")),
        "memory_bytes": max(regular_memory, init_memory) + memory_bytes(overhead.get("memory")),
        "pods": 1,
    }


def is_active(pod: dict[str, Any]) -> bool:
    return pod.get("metadata", {}).get("deletionTimestamp") is None and pod.get("status", {}).get("phase") not in {
        "Succeeded",
        "Failed",
    }


def _gate(name: str, passed: bool, evidence: dict[str, Any], reason: str | None = None) -> dict[str, Any]:
    return {"name": name, "passed": bool(passed), "reason": None if passed else reason, "evidence": evidence}


def evaluate(
    resourcequotas: dict[str, Any],
    pods: dict[str, Any],
    nodes: dict[str, Any],
    pvc: dict[str, Any],
    *,
    replicas: int,
    namespace: str,
    target_node: str,
    driver_node: str,
    claim_name: str,
) -> dict[str, Any]:
    if replicas not in {20, 30, 40, 50}:
        raise ValueError("replicas must be one of 20, 30, 40, or 50")
    if resourcequotas.get("kind") == "ResourceQuota":
        quotas = [resourcequotas]
    else:
        quotas = resourcequotas.get("items", [])
    all_pods = pods.get("items", [])
    node_items = {node.get("metadata", {}).get("name"): node for node in nodes.get("items", [])}
    gates: list[dict[str, Any]] = []

    required_quota_additions = {
        "pods": 2 * replicas,
        "requests.cpu": 2500 * replicas,
        "requests.memory": 4352 * replicas * 2**20,
        "limits.cpu": 6000 * replicas,
        "limits.memory": 5120 * replicas * 2**20,
    }
    quota_rows: list[dict[str, Any]] = []
    quota_pass = bool(quotas)
    required_names = set(required_quota_additions)
    for quota in quotas:
        hard = quota.get("status", {}).get("hard", {})
        used = quota.get("status", {}).get("used", {})
        missing = sorted(required_names - set(hard))
        resources: dict[str, Any] = {}
        row_pass = not missing
        for name, addition in required_quota_additions.items():
            if name not in hard:
                resources[name] = {"hard": None, "used": None, "addition": addition, "remaining_after": None, "passed": False}
                continue
            hard_value = resource_value(hard, name)
            used_value = resource_value(used, name)
            remaining_after = hard_value - used_value - addition
            passed = remaining_after >= 0
            row_pass = row_pass and passed
            resources[name] = {
                "hard": hard_value,
                "used": used_value,
                "addition": addition,
                "remaining_after": remaining_after,
                "passed": passed,
            }
        quota_rows.append(
            {
                "name": quota.get("metadata", {}).get("name"),
                "missing_required_hard_limits": missing,
                "resources": resources,
                "passed": row_pass,
            }
        )
        quota_pass = quota_pass and row_pass
    gates.append(
        _gate(
            "namespace_resource_quota_peak",
            quota_pass,
            {"quotas": quota_rows, "required_increment": required_quota_additions},
            "no complete ResourceQuota admits targets plus one full driver cell at peak",
        )
    )

    requested_by_node: dict[str, dict[str, int]] = {}
    active_namespaced = 0
    for pod in all_pods:
        if not is_active(pod):
            continue
        if pod.get("metadata", {}).get("namespace") == namespace:
            active_namespaced += 1
        node_name = pod.get("spec", {}).get("nodeName")
        if not node_name:
            continue
        row = requested_by_node.setdefault(node_name, {"cpu_millicores": 0, "memory_bytes": 0, "pods": 0})
        request = pod_scheduler_requests(pod)
        for key in row:
            row[key] += request[key]

    node_additions: dict[str, dict[str, int]] = {
        target_node: {"cpu_millicores": 2000 * replicas, "memory_bytes": 4096 * replicas * 2**20, "pods": replicas},
        driver_node: {"cpu_millicores": 500 * replicas, "memory_bytes": 256 * replicas * 2**20, "pods": replicas},
    }
    if target_node == driver_node:
        node_additions[target_node] = {
            key: node_additions[target_node][key] + node_additions[driver_node][key] for key in node_additions[target_node]
        }
    node_rows: list[dict[str, Any]] = []
    node_pass = target_node in node_items and driver_node in node_items
    for node_name in sorted(set((target_node, driver_node))):
        node = node_items.get(node_name)
        if node is None:
            node_rows.append({"name": node_name, "passed": False, "reason": "node missing from snapshot"})
            continue
        allocatable = node.get("status", {}).get("allocatable", {})
        alloc = {
            "cpu_millicores": cpu_millicores(allocatable.get("cpu")),
            "memory_bytes": memory_bytes(allocatable.get("memory")),
            "pods": scalar(allocatable.get("pods")),
        }
        used = requested_by_node.get(node_name, {"cpu_millicores": 0, "memory_bytes": 0, "pods": 0})
        addition = node_additions[node_name]
        remaining = {key: alloc[key] - used[key] - addition[key] for key in alloc}
        ready = any(
            condition.get("type") == "Ready" and condition.get("status") == "True"
            for condition in node.get("status", {}).get("conditions", [])
        )
        pressure = any(
            condition.get("type") in {"MemoryPressure", "DiskPressure", "PIDPressure", "NetworkUnavailable"}
            and condition.get("status") == "True"
            for condition in node.get("status", {}).get("conditions", [])
        )
        schedulable = not node.get("spec", {}).get("unschedulable", False)
        row_pass = ready and not pressure and schedulable and all(value >= 0 for value in remaining.values())
        node_pass = node_pass and row_pass
        node_rows.append(
            {
                "name": node_name,
                "ready": ready,
                "pressure": pressure,
                "schedulable": schedulable,
                "allocatable": alloc,
                "currently_requested": used,
                "campaign_addition": addition,
                "remaining_after": remaining,
                "passed": row_pass,
            }
        )
    gates.append(
        _gate(
            "node_scheduler_headroom",
            node_pass,
            {"nodes": node_rows},
            "target or driver node cannot admit the full peak request envelope",
        )
    )

    pvc_name_ok = pvc.get("metadata", {}).get("name") == claim_name
    pvc_namespace_ok = pvc.get("metadata", {}).get("namespace") in (None, namespace)
    pvc_bound = pvc.get("status", {}).get("phase") == "Bound"
    modes = set(pvc.get("spec", {}).get("accessModes", []))
    pvc_mode_ok = bool(modes & {"ReadWriteOnce", "ReadWriteMany", "ReadOnlyMany"})
    foreign_mounts: list[dict[str, Any]] = []
    for pod in all_pods:
        if not is_active(pod):
            continue
        mounted = any(
            volume.get("persistentVolumeClaim", {}).get("claimName") == claim_name
            for volume in pod.get("spec", {}).get("volumes", [])
        )
        if mounted and pod.get("spec", {}).get("nodeName") not in (None, target_node):
            foreign_mounts.append(
                {
                    "namespace": pod.get("metadata", {}).get("namespace"),
                    "name": pod.get("metadata", {}).get("name"),
                    "node": pod.get("spec", {}).get("nodeName"),
                }
            )
    storage_pass = pvc_name_ok and pvc_namespace_ok and pvc_bound and pvc_mode_ok and not foreign_mounts
    gates.append(
        _gate(
            "model_volume_single_node_mount",
            storage_pass,
            {
                "claim_name": pvc.get("metadata", {}).get("name"),
                "phase": pvc.get("status", {}).get("phase"),
                "access_modes": sorted(modes),
                "target_node": target_node,
                "active_mounts_on_other_nodes": foreign_mounts,
            },
            "model PVC is not safely mountable by all targets on the one target node",
        )
    )

    authorized = all(gate["passed"] for gate in gates)
    return {
        "schema_version": 1,
        "gate": "horizontal_scale_live_capacity_preflight",
        "namespace": namespace,
        "rung_replicas": replicas,
        "target_node": target_node,
        "driver_node": driver_node,
        "active_namespace_pods_before": active_namespaced,
        "gates": gates,
        "load_authorized": authorized,
        "disposition": "pass" if authorized else "blocked_before_mutation",
    }


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--resourcequotas", required=True, type=Path)
    parser.add_argument("--pods", required=True, type=Path)
    parser.add_argument("--nodes", required=True, type=Path)
    parser.add_argument("--pvc", required=True, type=Path)
    parser.add_argument("--replicas", required=True, type=int)
    parser.add_argument("--namespace", required=True)
    parser.add_argument("--target-node", required=True)
    parser.add_argument("--driver-node", required=True)
    parser.add_argument("--claim-name", default="classifier-model")
    parser.add_argument("--output", type=Path)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(sys.argv[1:] if argv is None else argv)
    try:
        result = evaluate(
            json.loads(args.resourcequotas.read_text()),
            json.loads(args.pods.read_text()),
            json.loads(args.nodes.read_text()),
            json.loads(args.pvc.read_text()),
            replicas=args.replicas,
            namespace=args.namespace,
            target_node=args.target_node,
            driver_node=args.driver_node,
            claim_name=args.claim_name,
        )
    except (OSError, ValueError, KeyError, TypeError, json.JSONDecodeError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2
    rendered = json.dumps(result, indent=2, sort_keys=True) + "\n"
    if args.output:
        args.output.write_text(rendered)
    else:
        sys.stdout.write(rendered)
    return 0 if result["load_authorized"] else 3


if __name__ == "__main__":
    raise SystemExit(main())
