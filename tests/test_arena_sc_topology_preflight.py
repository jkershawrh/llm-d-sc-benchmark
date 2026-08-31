#!/usr/bin/env python3
"""Focused regression tests for the Arena CPU-topology preflight."""

from __future__ import annotations

import importlib.util
import json
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "hack" / "arena-sc-topology-preflight.py"
SAVED_R20 = (
    ROOT
    / "tests/fixtures/topology/r20-cgroup-summary.json"
)
SAVED_NODES = ROOT / "tests/fixtures/topology/nodes-original.json"
SPEC = importlib.util.spec_from_file_location("arena_sc_topology_preflight", SCRIPT)
assert SPEC and SPEC.loader
MODULE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


def snapshot(*cpusets: str) -> dict[str, object]:
    return {
        "schema_version": 1,
        "capture": {"mode": "test"},
        "nodes": {
            "gnr2.fm2aihpcsed.com": {
                "online_cpus": "0-7",
                "isolated_cpus": "2-7",
                "reserved_cpus": "0-1",
                "reserved_source": "test fixture",
                "thread_sibling_groups": ["0,4", "1,5", "2,6", "3,7"],
                "topology_source": "test fixture",
                "topology_authoritative": True,
            }
        },
        "pods": [
            {
                "name": f"target-{index}",
                "uid": f"uid-{index}",
                "node": "gnr2.fm2aihpcsed.com",
                "cpuset": cpuset,
                "qos_class": "Guaranteed",
                "ready": True,
            }
            for index, cpuset in enumerate(cpusets, 1)
        ],
    }


class CpuListTest(unittest.TestCase):
    def test_round_trip(self) -> None:
        value = "0-3,8,10-12,144-145"
        self.assertEqual(MODULE.format_cpu_list(MODULE.parse_cpu_list(value)), value)

    def test_rejects_descending_range(self) -> None:
        with self.assertRaises(MODULE.EvidenceError):
            MODULE.parse_cpu_list("7-3")


class PlacementTest(unittest.TestCase):
    def test_complete_isolated_sibling_sets_pass(self) -> None:
        report = MODULE.validate_snapshot(snapshot("2,6", "3,7"))
        self.assertEqual(report["verdict"], "PASS")
        self.assertEqual(report["violations"], [])

    def test_arena_style_orphan_siblings_fail(self) -> None:
        # Mirrors the saved Arena pattern: cpuset 144-145 mapped to two cores
        # whose other threads (0 and 1) are housekeeping CPUs.
        value = snapshot("4-5")
        report = MODULE.validate_snapshot(value)
        self.assertEqual(report["verdict"], "FAIL")
        codes = {item["code"] for item in report["violations"]}
        self.assertIn("incomplete_smt_sibling_set", codes)
        self.assertIn("housekeeping_core_shared", codes)
        self.assertNotIn("housekeeping_cpu_overlap", codes)

    def test_missing_topology_is_invalid_not_pass(self) -> None:
        value = snapshot("2,6")
        value["nodes"]["gnr2.fm2aihpcsed.com"]["thread_sibling_groups"] = ["0,4", "1,5", "2,6"]
        report = MODULE.validate_snapshot(value)
        self.assertEqual(report["verdict"], "INVALID")

    def test_cross_pod_cpu_reuse_fails(self) -> None:
        report = MODULE.validate_snapshot(snapshot("2,6", "2,6"))
        self.assertEqual(report["verdict"], "FAIL")
        self.assertIn("target_cpu_overlap", {item["code"] for item in report["violations"]})

    def test_report_can_be_revalidated(self) -> None:
        initial = MODULE.validate_snapshot(snapshot("2,6"))
        report = MODULE.validate_snapshot(initial["snapshot"])
        self.assertEqual(report["verdict"], "PASS")
        json.dumps(report)

    @unittest.skipUnless(SAVED_R20.exists() and SAVED_NODES.exists(), "saved Arena r20 evidence is absent")
    def test_saved_arena_r20_identifies_only_the_outlier_placement(self) -> None:
        evidence = MODULE.capture_artifacts(
            SimpleNamespace(
                cgroup_summary=SAVED_R20,
                nodes_file=SAVED_NODES,
                node="gnr2.fm2aihpcsed.com",
                sibling_offset=144,
                reserved_cpus=None,
            )
        )
        report = MODULE.validate_snapshot(evidence)
        self.assertEqual(report["verdict"], "FAIL")
        failed_pods = [pod["name"] for pod in report["pods"] if pod["violations"]]
        self.assertEqual(failed_pods, ["classifier-target-595b8fbf9c-zk2dm"])
        failed_report = next(pod for pod in report["pods"] if pod["name"] == failed_pods[0])
        self.assertEqual(
            {item["code"] for item in failed_report["violations"]},
            {"incomplete_smt_sibling_set", "housekeeping_core_shared"},
        )


if __name__ == "__main__":
    unittest.main()
