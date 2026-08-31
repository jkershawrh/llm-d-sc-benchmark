#!/usr/bin/env python3
"""Fail-closed CPU-topology preflight for Arena classifier targets.

The live mode performs only Kubernetes GETs and reads through ``oc exec``.  It
does not create, patch, scale, or delete cluster objects.  Validation rejects a
target placement when its effective cpuset is not a union of complete SMT
sibling sets, overlaps housekeeping CPUs, or occupies a physical core that has
a housekeeping sibling.

Exit codes:
  0  topology placement passed
  1  topology placement failed
  2  evidence was incomplete or malformed, so no placement verdict is valid
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import re
import shlex
import subprocess
import sys
from pathlib import Path
from typing import Any, Iterable


SCHEMA_VERSION = 1
CPU_TOKEN = re.compile(r"^[0-9]+(?:-[0-9]+)?$")
CPUSET_LINE = re.compile(r"^__CPUSET__=(.+)$")
ONLINE_LINE = re.compile(r"^__ONLINE__=(.+)$")
SIBLING_LINE = re.compile(r"^__SIBLING__=([0-9]+):(.+)$")


class EvidenceError(ValueError):
    """The preflight cannot make a valid claim from the supplied evidence."""


def parse_cpu_list(value: str) -> set[int]:
    """Parse the Linux cpulist syntax used by sysfs and cgroup v2."""

    if not isinstance(value, str) or not value.strip():
        raise EvidenceError("CPU list is empty")
    cpus: set[int] = set()
    for token in value.strip().split(","):
        token = token.strip()
        if not CPU_TOKEN.fullmatch(token):
            raise EvidenceError(f"invalid CPU-list token: {token!r}")
        if "-" in token:
            first_text, last_text = token.split("-", 1)
            first, last = int(first_text), int(last_text)
            if last < first:
                raise EvidenceError(f"descending CPU range: {token!r}")
            cpus.update(range(first, last + 1))
        else:
            cpus.add(int(token))
    return cpus


def format_cpu_list(cpus: Iterable[int]) -> str:
    """Return a stable, compact Linux cpulist."""

    ordered = sorted(set(cpus))
    if not ordered:
        return ""
    ranges: list[str] = []
    first = previous = ordered[0]
    for cpu in ordered[1:]:
        if cpu == previous + 1:
            previous = cpu
            continue
        ranges.append(str(first) if first == previous else f"{first}-{previous}")
        first = previous = cpu
    ranges.append(str(first) if first == previous else f"{first}-{previous}")
    return ",".join(ranges)


def utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat().replace("+00:00", "Z")


def command_json(command: list[str]) -> Any:
    completed = subprocess.run(
        command,
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    if completed.returncode != 0:
        detail = completed.stderr.strip() or completed.stdout.strip()
        raise EvidenceError(f"command failed ({completed.returncode}): {' '.join(command)}: {detail}")
    try:
        return json.loads(completed.stdout)
    except json.JSONDecodeError as error:
        raise EvidenceError(f"command did not return JSON: {' '.join(command)}: {error}") from error


def command_text(command: list[str]) -> str:
    completed = subprocess.run(
        command,
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    if completed.returncode != 0:
        detail = completed.stderr.strip() or completed.stdout.strip()
        raise EvidenceError(f"command failed ({completed.returncode}): {' '.join(command)}: {detail}")
    return completed.stdout


def oc_base(args: argparse.Namespace) -> list[str]:
    command = [args.oc]
    if args.kubeconfig:
        command.extend(("--kubeconfig", args.kubeconfig))
    if args.context:
        command.extend(("--context", args.context))
    return command


def pod_is_ready(pod: dict[str, Any]) -> bool:
    if pod.get("metadata", {}).get("deletionTimestamp"):
        return False
    if pod.get("status", {}).get("phase") != "Running":
        return False
    return any(
        condition.get("type") == "Ready" and condition.get("status") == "True"
        for condition in pod.get("status", {}).get("conditions", [])
    )


def parse_isolated_from_bootcmdline(bootcmdline: str) -> set[int]:
    try:
        words = shlex.split(bootcmdline)
    except ValueError as error:
        raise EvidenceError(f"cannot parse tuned boot command line: {error}") from error
    values = [word.split("=", 1)[1] for word in words if word.startswith("isolcpus=")]
    if len(values) != 1:
        raise EvidenceError(f"expected exactly one isolcpus= setting, found {len(values)}")
    cpu_tokens = [token for token in values[0].split(",") if CPU_TOKEN.fullmatch(token)]
    if not cpu_tokens:
        raise EvidenceError("isolcpus= contains no CPU list")
    return parse_cpu_list(",".join(cpu_tokens))


def parse_reserved_overrides(values: list[str]) -> dict[str, str]:
    overrides: dict[str, str] = {}
    for value in values:
        if "=" not in value:
            raise EvidenceError("live --reserved-cpus must use NODE=CPU_LIST")
        node, cpulist = value.split("=", 1)
        if not node or not cpulist:
            raise EvidenceError(f"invalid reserved CPU override: {value!r}")
        parse_cpu_list(cpulist)
        if node in overrides:
            raise EvidenceError(f"duplicate reserved CPU override for node {node!r}")
        overrides[node] = format_cpu_list(parse_cpu_list(cpulist))
    return overrides


def parse_exec_capture(output: str, require_topology: bool) -> tuple[str, str | None, list[str]]:
    cpuset: str | None = None
    online: str | None = None
    per_cpu: dict[int, str] = {}
    for line in output.splitlines():
        match = CPUSET_LINE.fullmatch(line)
        if match:
            cpuset = format_cpu_list(parse_cpu_list(match.group(1)))
            continue
        match = ONLINE_LINE.fullmatch(line)
        if match:
            online = format_cpu_list(parse_cpu_list(match.group(1)))
            continue
        match = SIBLING_LINE.fullmatch(line)
        if match:
            cpu = int(match.group(1))
            siblings = format_cpu_list(parse_cpu_list(match.group(2)))
            if cpu in per_cpu and per_cpu[cpu] != siblings:
                raise EvidenceError(f"CPU {cpu} returned inconsistent sibling groups")
            per_cpu[cpu] = siblings
    if cpuset is None:
        raise EvidenceError("pod capture did not contain __CPUSET__")
    if not require_topology:
        return cpuset, None, []
    if online is None:
        raise EvidenceError("node capture did not contain __ONLINE__")
    online_cpus = parse_cpu_list(online)
    missing = online_cpus - set(per_cpu)
    extra = set(per_cpu) - online_cpus
    if missing or extra:
        raise EvidenceError(
            "sysfs topology coverage mismatch: "
            f"missing={format_cpu_list(missing) or '-'} extra={format_cpu_list(extra) or '-'}"
        )
    groups = sorted(set(per_cpu.values()), key=lambda item: min(parse_cpu_list(item)))
    for cpu, group_text in per_cpu.items():
        group = parse_cpu_list(group_text)
        if cpu not in group:
            raise EvidenceError(f"CPU {cpu} is absent from its own sibling set {group_text}")
        for sibling in group:
            if per_cpu.get(sibling) != group_text:
                raise EvidenceError(
                    f"asymmetric sibling map: CPU {cpu} says {group_text}, "
                    f"CPU {sibling} says {per_cpu.get(sibling)!r}"
                )
    return cpuset, online, groups


def capture_live(args: argparse.Namespace) -> dict[str, Any]:
    base = oc_base(args)
    pods_json = command_json(base + ["get", "pods", "-n", args.namespace, "-l", args.selector, "-o", "json"])
    items = sorted(pods_json.get("items", []), key=lambda pod: pod.get("metadata", {}).get("name", ""))
    if not items:
        raise EvidenceError("selector matched no target pods")
    if args.expected_pods is not None and len(items) != args.expected_pods:
        raise EvidenceError(f"expected {args.expected_pods} pods, selector matched {len(items)}")
    not_ready = [pod.get("metadata", {}).get("name", "<unnamed>") for pod in items if not pod_is_ready(pod)]
    if not_ready:
        raise EvidenceError(f"target pods are not stable and Ready: {', '.join(not_ready)}")
    for pod in items:
        if not pod.get("spec", {}).get("nodeName"):
            raise EvidenceError(f"pod {pod.get('metadata', {}).get('name')} has no nodeName")

    overrides = parse_reserved_overrides(args.reserved_cpus)
    node_names = sorted({pod["spec"]["nodeName"] for pod in items})
    unknown_overrides = set(overrides) - set(node_names)
    if unknown_overrides:
        raise EvidenceError(f"reserved CPU override names unused node(s): {', '.join(sorted(unknown_overrides))}")

    nodes_json = {node: command_json(base + ["get", "node", node, "-o", "json"]) for node in node_names}
    pods: list[dict[str, Any]] = []
    nodes: dict[str, dict[str, Any]] = {}

    cpuset_command = "printf '__CPUSET__='; cat /sys/fs/cgroup/cpuset.cpus.effective"
    topology_command = r"""
set -eu
printf '__CPUSET__='
cat /sys/fs/cgroup/cpuset.cpus.effective
printf '__ONLINE__='
cat /sys/devices/system/cpu/online
for path in /sys/devices/system/cpu/cpu[0-9]*/topology/thread_siblings_list; do
  directory=${path%/topology/thread_siblings_list}
  cpu=${directory##*/cpu}
  printf '__SIBLING__=%s:' "$cpu"
  cat "$path"
done
""".strip()

    topology_seen: set[str] = set()
    for pod in items:
        name = pod["metadata"]["name"]
        node = pod["spec"]["nodeName"]
        require_topology = node not in topology_seen
        command = base + ["exec", "-n", args.namespace, name]
        if args.container:
            command.extend(("-c", args.container))
        command.extend(("--", "sh", "-c", topology_command if require_topology else cpuset_command))
        cpuset, online, groups = parse_exec_capture(command_text(command), require_topology)
        if require_topology:
            assert online is not None
            online_cpus = parse_cpu_list(online)
            node_json = nodes_json[node]
            annotations = node_json.get("metadata", {}).get("annotations", {})
            bootcmdline = annotations.get("tuned.openshift.io/bootcmdline", "")
            isolated = parse_isolated_from_bootcmdline(bootcmdline)
            if not isolated <= online_cpus:
                raise EvidenceError(f"node {node}: isolated CPUs are not a subset of online CPUs")
            if node in overrides:
                reserved = parse_cpu_list(overrides[node])
                reserved_source = "explicit --reserved-cpus"
            else:
                reserved = online_cpus - isolated
                reserved_source = "derived: online minus tuned.openshift.io/bootcmdline isolcpus"
            nodes[node] = {
                "online_cpus": online,
                "isolated_cpus": format_cpu_list(isolated),
                "reserved_cpus": format_cpu_list(reserved),
                "reserved_source": reserved_source,
                "thread_sibling_groups": groups,
                "topology_source": "pod-visible kernel sysfs",
                "topology_authoritative": True,
                "machine_config": annotations.get("machineconfiguration.openshift.io/currentConfig"),
                "boot_id": node_json.get("status", {}).get("nodeInfo", {}).get("bootID"),
            }
            topology_seen.add(node)
        pods.append(
            {
                "name": name,
                "uid": pod["metadata"].get("uid"),
                "node": node,
                "cpuset": cpuset,
                "qos_class": pod.get("status", {}).get("qosClass"),
                "ready": True,
            }
        )

    # Fail if the target set moved while it was being sampled.
    final_json = command_json(base + ["get", "pods", "-n", args.namespace, "-l", args.selector, "-o", "json"])
    final_items = final_json.get("items", [])
    final_not_ready = [
        pod.get("metadata", {}).get("name", "<unnamed>")
        for pod in final_items
        if not pod_is_ready(pod)
    ]
    if final_not_ready:
        raise EvidenceError(f"target pods lost stability during topology capture: {', '.join(final_not_ready)}")
    final_identity = sorted(
        (
            pod.get("metadata", {}).get("name"),
            pod.get("metadata", {}).get("uid"),
            pod.get("spec", {}).get("nodeName"),
        )
        for pod in final_items
    )
    initial_identity = sorted((pod["name"], pod["uid"], pod["node"]) for pod in pods)
    if final_identity != initial_identity:
        raise EvidenceError("target pod identity or placement changed during topology capture")

    return {
        "schema_version": SCHEMA_VERSION,
        "capture": {
            "mode": "live-read-only",
            "captured_at": utc_now(),
            "namespace": args.namespace,
            "selector": args.selector,
            "expected_pods": args.expected_pods,
        },
        "nodes": nodes,
        "pods": pods,
    }


def node_from_list(nodes_json: dict[str, Any], node_name: str) -> dict[str, Any]:
    items = nodes_json.get("items")
    if isinstance(items, list):
        matches = [item for item in items if item.get("metadata", {}).get("name") == node_name]
    elif nodes_json.get("metadata", {}).get("name") == node_name:
        matches = [nodes_json]
    else:
        matches = []
    if len(matches) != 1:
        raise EvidenceError(f"expected one node named {node_name!r}, found {len(matches)}")
    return matches[0]


def sibling_groups_from_offset(logical_cpu_count: int, offset: int) -> list[str]:
    if offset <= 0 or logical_cpu_count != offset * 2:
        raise EvidenceError(
            "artifact replay requires exactly two threads per core: "
            f"logical_cpu_count={logical_cpu_count}, sibling_offset={offset}"
        )
    return [format_cpu_list((cpu, cpu + offset)) for cpu in range(offset)]


def capture_artifacts(args: argparse.Namespace) -> dict[str, Any]:
    try:
        cgroups = json.loads(args.cgroup_summary.read_text(encoding="utf-8"))
        nodes_json = json.loads(args.nodes_file.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise EvidenceError(f"cannot read saved artifact: {error}") from error
    if not isinstance(cgroups, list) or not cgroups:
        raise EvidenceError("cgroup summary must be a non-empty JSON array")
    node_json = node_from_list(nodes_json, args.node)
    capacity_text = node_json.get("status", {}).get("capacity", {}).get("cpu")
    try:
        logical_cpu_count = int(capacity_text)
    except (TypeError, ValueError) as error:
        raise EvidenceError(f"node capacity.cpu is not an integer: {capacity_text!r}") from error
    online = set(range(logical_cpu_count))
    annotations = node_json.get("metadata", {}).get("annotations", {})
    isolated = parse_isolated_from_bootcmdline(annotations.get("tuned.openshift.io/bootcmdline", ""))
    if args.reserved_cpus:
        reserved = parse_cpu_list(args.reserved_cpus)
        reserved_source = "explicit --reserved-cpus"
    else:
        reserved = online - isolated
        reserved_source = "derived: capacity CPU IDs minus saved tuned isolcpus annotation"
    pods: list[dict[str, Any]] = []
    for record in cgroups:
        cpusets = record.get("cpuset_cpus_effective", {})
        start, end = cpusets.get("start"), cpusets.get("end")
        if start != end:
            raise EvidenceError(f"pod {record.get('pod')}: cpuset changed from {start!r} to {end!r}")
        parse_cpu_list(start)
        pods.append(
            {
                "name": record.get("pod"),
                "uid": record.get("pod_uid"),
                "node": args.node,
                "cpuset": format_cpu_list(parse_cpu_list(start)),
                "qos_class": None,
                "ready": True,
            }
        )
    return {
        "schema_version": SCHEMA_VERSION,
        "capture": {
            "mode": "saved-artifact-replay",
            "captured_at": utc_now(),
            "cgroup_summary": str(args.cgroup_summary),
            "nodes_file": str(args.nodes_file),
            "warning": "sibling offset is an explicit forensic assumption; do not use this mode as a live load gate",
        },
        "nodes": {
            args.node: {
                "online_cpus": format_cpu_list(online),
                "isolated_cpus": format_cpu_list(isolated),
                "reserved_cpus": format_cpu_list(reserved),
                "reserved_source": reserved_source,
                "thread_sibling_groups": sibling_groups_from_offset(logical_cpu_count, args.sibling_offset),
                "topology_source": f"explicit forensic sibling offset {args.sibling_offset}",
                "topology_authoritative": False,
                "machine_config": annotations.get("machineconfiguration.openshift.io/currentConfig"),
                "boot_id": node_json.get("status", {}).get("nodeInfo", {}).get("bootID"),
            }
        },
        "pods": pods,
    }


def read_snapshot(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise EvidenceError(f"cannot read snapshot: {error}") from error
    if isinstance(value, dict) and isinstance(value.get("snapshot"), dict):
        value = value["snapshot"]
    if not isinstance(value, dict):
        raise EvidenceError("snapshot must be a JSON object")
    return value


def violation(code: str, scope: str, message: str, **details: Any) -> dict[str, Any]:
    return {"code": code, "scope": scope, "message": message, "details": details}


def validate_snapshot(snapshot: dict[str, Any]) -> dict[str, Any]:
    invalid_reasons: list[str] = []
    gate_ineligibility_reasons: list[str] = []
    warnings: list[dict[str, Any]] = []
    violations: list[dict[str, Any]] = []
    node_state: dict[str, dict[str, Any]] = {}

    if snapshot.get("schema_version") != SCHEMA_VERSION:
        invalid_reasons.append(
            f"unsupported snapshot schema_version {snapshot.get('schema_version')!r}; expected {SCHEMA_VERSION}"
        )
    raw_nodes = snapshot.get("nodes")
    raw_pods = snapshot.get("pods")
    if not isinstance(raw_nodes, dict) or not raw_nodes:
        invalid_reasons.append("snapshot.nodes must be a non-empty object")
        raw_nodes = {}
    if not isinstance(raw_pods, list) or not raw_pods:
        invalid_reasons.append("snapshot.pods must be a non-empty array")
        raw_pods = []

    for node_name, raw_node in raw_nodes.items():
        try:
            online = parse_cpu_list(raw_node["online_cpus"])
            reserved = parse_cpu_list(raw_node["reserved_cpus"])
            if not reserved <= online:
                raise EvidenceError("reserved CPUs are not a subset of online CPUs")
            raw_groups = raw_node["thread_sibling_groups"]
            if not isinstance(raw_groups, list) or not raw_groups:
                raise EvidenceError("thread_sibling_groups must be a non-empty array")
            groups = [frozenset(parse_cpu_list(group)) for group in raw_groups]
            cpu_to_group: dict[int, frozenset[int]] = {}
            for group in groups:
                if not group <= online:
                    raise EvidenceError(f"sibling group {format_cpu_list(group)} contains an offline CPU")
                for cpu in group:
                    previous = cpu_to_group.get(cpu)
                    if previous is not None and previous != group:
                        raise EvidenceError(f"CPU {cpu} appears in inconsistent sibling groups")
                    cpu_to_group[cpu] = group
            missing = online - set(cpu_to_group)
            if missing:
                raise EvidenceError(f"topology omits online CPUs {format_cpu_list(missing)}")
            split_reserved = sorted(
                {group for group in groups if group & reserved and not group <= reserved},
                key=lambda group: min(group),
            )
            if split_reserved:
                unsafe = set().union(*(group - reserved for group in split_reserved))
                warnings.append(
                    violation(
                        "housekeeping_set_splits_smt_cores",
                        f"node/{node_name}",
                        "housekeeping CPUs are not a union of complete SMT sibling sets",
                        reserved_cpus=format_cpu_list(reserved),
                        allocatable_siblings_on_housekeeping_cores=format_cpu_list(unsafe),
                    )
                )
            if not raw_node.get("topology_authoritative", False):
                gate_ineligibility_reasons.append(
                    f"node {node_name}: topology source is not authoritative for a live load gate"
                )
                warnings.append(
                    violation(
                        "non_authoritative_topology",
                        f"node/{node_name}",
                        "topology was inferred; replay can support forensics but cannot authorize a live load",
                        topology_source=raw_node.get("topology_source"),
                    )
                )
            node_state[node_name] = {
                "online": online,
                "reserved": reserved,
                "groups": groups,
                "cpu_to_group": cpu_to_group,
            }
        except (KeyError, TypeError, EvidenceError) as error:
            invalid_reasons.append(f"node {node_name}: {error}")

    pod_reports: list[dict[str, Any]] = []
    parsed_pods: list[tuple[str, str, set[int]]] = []
    seen_names: set[str] = set()
    for raw_pod in raw_pods:
        name = raw_pod.get("name") if isinstance(raw_pod, dict) else None
        node_name = raw_pod.get("node") if isinstance(raw_pod, dict) else None
        scope = f"pod/{name or '<unnamed>'}"
        pod_violations: list[dict[str, Any]] = []
        try:
            if not name or not isinstance(name, str):
                raise EvidenceError("pod name is absent")
            if name in seen_names:
                raise EvidenceError("duplicate pod name")
            seen_names.add(name)
            if raw_pod.get("ready") is not True:
                raise EvidenceError("pod was not captured as Ready")
            if node_name not in node_state:
                raise EvidenceError(f"node {node_name!r} has no valid topology")
            cpuset = parse_cpu_list(raw_pod["cpuset"])
            state = node_state[node_name]
            outside = cpuset - state["online"]
            if outside:
                raise EvidenceError(f"cpuset contains offline CPUs {format_cpu_list(outside)}")
            missing_topology = cpuset - set(state["cpu_to_group"])
            if missing_topology:
                raise EvidenceError(f"cpuset CPUs lack topology {format_cpu_list(missing_topology)}")

            touched_groups = {state["cpu_to_group"][cpu] for cpu in cpuset}
            incomplete = sorted(
                (group for group in touched_groups if not group <= cpuset),
                key=lambda group: min(group),
            )
            if incomplete:
                missing_siblings = set().union(*(group - cpuset for group in incomplete))
                pod_violations.append(
                    violation(
                        "incomplete_smt_sibling_set",
                        scope,
                        "effective cpuset is not a union of complete SMT sibling sets",
                        cpuset=format_cpu_list(cpuset),
                        incomplete_groups=[format_cpu_list(group) for group in incomplete],
                        missing_siblings=format_cpu_list(missing_siblings),
                    )
                )

            direct_reserved = cpuset & state["reserved"]
            if direct_reserved:
                pod_violations.append(
                    violation(
                        "housekeeping_cpu_overlap",
                        scope,
                        "effective cpuset directly overlaps housekeeping CPUs",
                        overlapping_cpus=format_cpu_list(direct_reserved),
                    )
                )
            shared_groups = sorted(
                {group for group in touched_groups if group & state["reserved"]},
                key=lambda group: min(group),
            )
            if shared_groups:
                pod_violations.append(
                    violation(
                        "housekeeping_core_shared",
                        scope,
                        "target occupies an SMT core that also contains a housekeeping CPU",
                        shared_sibling_groups=[format_cpu_list(group) for group in shared_groups],
                        housekeeping_siblings=format_cpu_list(
                            set().union(*(group & state["reserved"] for group in shared_groups))
                        ),
                    )
                )
            parsed_pods.append((name, node_name, cpuset))
            violations.extend(pod_violations)
            pod_reports.append(
                {
                    "name": name,
                    "uid": raw_pod.get("uid"),
                    "node": node_name,
                    "cpuset": format_cpu_list(cpuset),
                    "complete_smt_sibling_sets": not incomplete,
                    "housekeeping_cpu_overlap": format_cpu_list(direct_reserved),
                    "housekeeping_core_shared": bool(shared_groups),
                    "violations": pod_violations,
                }
            )
        except (KeyError, TypeError, EvidenceError) as error:
            invalid_reasons.append(f"{scope}: {error}")

    # Static CPU Manager should never assign the same CPU or physical core to
    # two Guaranteed target pods.  This is independently useful evidence and
    # prevents a superficially complete sibling set from passing twice.
    for index, (left_name, left_node, left_cpuset) in enumerate(parsed_pods):
        left_state = node_state[left_node]
        left_groups = {left_state["cpu_to_group"][cpu] for cpu in left_cpuset}
        for right_name, right_node, right_cpuset in parsed_pods[index + 1 :]:
            if left_node != right_node:
                continue
            overlap = left_cpuset & right_cpuset
            if overlap:
                violations.append(
                    violation(
                        "target_cpu_overlap",
                        f"node/{left_node}",
                        "two target pods have overlapping effective cpusets",
                        pods=[left_name, right_name],
                        overlapping_cpus=format_cpu_list(overlap),
                    )
                )
            right_groups = {left_state["cpu_to_group"][cpu] for cpu in right_cpuset}
            shared = left_groups & right_groups
            if shared and not overlap:
                violations.append(
                    violation(
                        "target_core_shared",
                        f"node/{left_node}",
                        "two target pods occupy sibling threads of the same physical core",
                        pods=[left_name, right_name],
                        shared_sibling_groups=[format_cpu_list(group) for group in sorted(shared, key=min)],
                    )
                )

    placement_verdict = "INVALID" if invalid_reasons else ("FAIL" if violations else "PASS")
    if invalid_reasons:
        verdict = "INVALID"
        exit_code = 2
    elif violations:
        verdict = "FAIL"
        exit_code = 1
    elif gate_ineligibility_reasons:
        verdict = "INVALID"
        exit_code = 2
    else:
        verdict = "PASS"
        exit_code = 0
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at": utc_now(),
        "verdict": verdict,
        "placement_verdict": placement_verdict,
        "gate_passed": verdict == "PASS",
        "exit_code": exit_code,
        "summary": {
            "nodes": len(raw_nodes),
            "pods": len(raw_pods),
            "pods_validated": len(parsed_pods),
            "placement_violations": len(violations),
            "warnings": len(warnings),
            "invalid_reasons": len(invalid_reasons),
            "gate_ineligibility_reasons": len(gate_ineligibility_reasons),
        },
        "invalid_reasons": invalid_reasons,
        "gate_ineligibility_reasons": gate_ineligibility_reasons,
        "warnings": warnings,
        "violations": violations,
        "pods": pod_reports,
        "snapshot": snapshot,
    }


def render_text(report: dict[str, Any]) -> str:
    summary = report["summary"]
    lines = [
        f"topology preflight: {report['verdict']}",
        (
            f"pods validated: {summary['pods_validated']}/{summary['pods']}; "
            f"violations: {summary['placement_violations']}; "
            f"warnings: {summary['warnings']}; invalid reasons: {summary['invalid_reasons']}"
        ),
    ]
    for item in report["invalid_reasons"]:
        lines.append(f"INVALID: {item}")
    for item in report["gate_ineligibility_reasons"]:
        lines.append(f"GATE-INELIGIBLE: {item}")
    for item in report["violations"]:
        lines.append(f"FAIL {item['code']} {item['scope']}: {item['message']}")
    for item in report["warnings"]:
        lines.append(f"WARN {item['code']} {item['scope']}: {item['message']}")
    return "\n".join(lines)


def add_output_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--format", choices=("json", "text"), default="json")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="mode", required=True)

    live = subparsers.add_parser("live", help="capture and validate current target placements read-only")
    live.add_argument("--namespace", required=True)
    live.add_argument("--selector", required=True, help="target Pod label selector")
    live.add_argument("--expected-pods", type=int)
    live.add_argument("--container", help="target container name when a Pod has multiple containers")
    live.add_argument("--reserved-cpus", action="append", default=[], metavar="NODE=CPU_LIST")
    live.add_argument("--oc", default="oc", help="oc executable (default: oc)")
    live.add_argument("--kubeconfig")
    live.add_argument("--context")
    add_output_args(live)

    saved = subparsers.add_parser("snapshot", help="revalidate a prior JSON report or snapshot")
    saved.add_argument("path", type=Path)
    add_output_args(saved)

    artifacts = subparsers.add_parser(
        "artifacts",
        help="forensic replay of a saved cgroup summary and node snapshot",
    )
    artifacts.add_argument("--cgroup-summary", required=True, type=Path)
    artifacts.add_argument("--nodes-file", required=True, type=Path)
    artifacts.add_argument("--node", required=True)
    artifacts.add_argument("--sibling-offset", required=True, type=int)
    artifacts.add_argument("--reserved-cpus", help="override saved isolcpus-derived housekeeping set")
    add_output_args(artifacts)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        if args.mode == "live":
            snapshot = capture_live(args)
        elif args.mode == "artifacts":
            snapshot = capture_artifacts(args)
        else:
            snapshot = read_snapshot(args.path)
        report = validate_snapshot(snapshot)
    except EvidenceError as error:
        report = {
            "schema_version": SCHEMA_VERSION,
            "generated_at": utc_now(),
            "verdict": "INVALID",
            "placement_verdict": "INVALID",
            "gate_passed": False,
            "exit_code": 2,
            "summary": {
                "nodes": 0,
                "pods": 0,
                "pods_validated": 0,
                "placement_violations": 0,
                "warnings": 0,
                "invalid_reasons": 1,
                "gate_ineligibility_reasons": 0,
            },
            "invalid_reasons": [str(error)],
            "gate_ineligibility_reasons": [],
            "warnings": [],
            "violations": [],
            "pods": [],
            "snapshot": None,
        }
    if args.format == "json":
        json.dump(report, sys.stdout, indent=2, sort_keys=True)
        sys.stdout.write("\n")
    else:
        print(render_text(report))
    return int(report["exit_code"])


if __name__ == "__main__":
    raise SystemExit(main())
