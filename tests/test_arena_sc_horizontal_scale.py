#!/usr/bin/env python3
"""Focused, cluster-free tests for the unchanged-SC scaleout framework."""

from __future__ import annotations

import copy
import importlib.util
import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
PLANNER_PATH = ROOT / "hack" / "arena-sc-horizontal-scale-plan.py"
PREFLIGHT_PATH = ROOT / "hack" / "arena-sc-horizontal-scale-preflight.py"
SUMMARY_PATH = ROOT / "hack" / "arena-sc-horizontal-scale-summarize.py"
ORCHESTRATOR = ROOT / "hack" / "arena-sc-horizontal-scale-campaign.sh"


def import_file(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


PLAN = import_file("arena_sc_horizontal_scale_plan", PLANNER_PATH)
PREFLIGHT = import_file("arena_sc_horizontal_scale_preflight", PREFLIGHT_PATH)
SUMMARY = import_file("arena_sc_horizontal_scale_summarize", SUMMARY_PATH)


def node(name: str, cpu: str = "96", memory: str = "512Gi", pods: str = "250") -> dict:
    return {
        "metadata": {"name": name},
        "spec": {"unschedulable": False},
        "status": {
            "allocatable": {"cpu": cpu, "memory": memory, "pods": pods},
            "conditions": [
                {"type": "Ready", "status": "True"},
                {"type": "MemoryPressure", "status": "False"},
                {"type": "DiskPressure", "status": "False"},
                {"type": "PIDPressure", "status": "False"},
                {"type": "NetworkUnavailable", "status": "False"},
            ],
        },
    }


def quota() -> dict:
    return {
        "items": [
            {
                "metadata": {"name": "benchmark"},
                "status": {
                    "hard": {
                        "pods": "60",
                        "requests.cpu": "60",
                        "requests.memory": "120Gi",
                        "limits.cpu": "240",
                        "limits.memory": "240Gi",
                    },
                    "used": {
                        "pods": "2",
                        "requests.cpu": "1",
                        "requests.memory": "2Gi",
                        "limits.cpu": "2",
                        "limits.memory": "2Gi",
                    },
                },
            }
        ]
    }


def pvc() -> dict:
    return {
        "metadata": {"name": "classifier-model", "namespace": "llm-d-sc-scaleout"},
        "spec": {"accessModes": ["ReadWriteOnce"]},
        "status": {"phase": "Bound"},
    }


def aggregate_cells(plan: dict, *, fail_41: bool = False) -> list[dict]:
    cells = []
    for cell in plan["cells"]:
        active = len(cell["active_endpoints"])
        rates = [rate for rate in cell["endpoint_offered_rps"] if rate is not None]
        rate = rates[0]
        if rate in (35, 41):
            success = 0.985 if (rate == 41 and fail_41 and cell["phase"] == "knee") else 0.9995
            drain = 1 - success
            p99 = 30_000.0 if rate == 41 else 25_000.0
            useful = rate * success
            errors = 0
        else:
            success = 0.985
            drain = 0.015
            p99 = 2_000_000.0
            useful = 41.4
            errors = 0
        cells.append(
            {
                "cell_id": cell["cell_id"],
                "ordinal": cell["ordinal"],
                "phase": cell["phase"],
                "scope": cell["scope"],
                "block": cell.get("block"),
                "period": cell.get("period"),
                "start_epoch_ms": 2_000_000_000_000 + cell["ordinal"] * 400_000,
                "duration_seconds": 180,
                "endpoints": active,
                "offered_rps_per_endpoint": rate,
                "aggregate_offered_rps": active * rate,
                "aggregate_useful_rps": active * useful,
                "per_pod_useful_rps": useful,
                "offered_slots": active * rate * 180,
                "success_ratio": success,
                "drain_ratio": drain,
                "errors_total": errors,
                "median_endpoint_p50_us": 24_000.0 if rate != 42 else 1_000_000.0,
                "median_endpoint_p99_us": p99,
                "endpoint_useful_rps_cv": 0.0,
                "endpoint_results": [],
            }
        )
    return cells


class PlanTests(unittest.TestCase):
    def test_r20_primary_design_and_resource_envelope(self) -> None:
        plan, ledger = PLAN.build_plan("scale-r20-test", 0, 20)
        SUMMARY.validate_plan(plan, ledger)
        self.assertEqual(len(plan["cells"]), 16)
        self.assertEqual(len(plan["jobs"]), 244)
        self.assertEqual(plan["resource_envelope_incremental"]["peak_pods"], 40)
        self.assertEqual(plan["resource_envelope_incremental"]["requests"]["cpu_millicores"], 50_000)
        knee = [cell for cell in plan["cells"] if cell["phase"] == "knee"]
        self.assertEqual(len(knee), 10)
        self.assertEqual(sum(cell["endpoint_offered_rps"][0] == 41 for cell in knee), 5)
        self.assertEqual(sum(cell["endpoint_offered_rps"][0] == 42 for cell in knee), 5)
        for cell in knee:
            self.assertEqual(len(set(cell["endpoint_offered_rps"])), 1)
            self.assertEqual(cell["driver_jobs"], 20)
        for block in range(1, 6):
            pair = [cell for cell in knee if cell["block"] == block]
            self.assertEqual({cell["endpoint_offered_rps"][0] for cell in pair}, {41, 42})

    def test_r50_fits_campaign_reservation(self) -> None:
        plan, ledger = PLAN.build_plan("scale-r50-test", 98, 50)
        SUMMARY.validate_plan(plan, ledger)
        self.assertEqual(len(plan["jobs"]), 604)
        reservation = plan["sequence_reservation"]
        self.assertLessEqual(reservation["allocated_end_exclusive"], reservation["end_exclusive"])
        self.assertGreaterEqual(reservation["start_inclusive"], 22_000_000_000)
        self.assertLessEqual(reservation["end_exclusive"], 23_000_000_000)

    def test_fifth_order_counterbalances_by_campaign_parity(self) -> None:
        even, _ = PLAN.build_plan("scale-even", 4, 20)
        odd, _ = PLAN.build_plan("scale-odd", 5, 20)
        even_b5 = [cell for cell in even["cells"] if cell.get("block") == 5]
        odd_b5 = [cell for cell in odd["cells"] if cell.get("block") == 5]
        self.assertEqual([cell["endpoint_offered_rps"][0] for cell in even_b5], [41, 42])
        self.assertEqual([cell["endpoint_offered_rps"][0] for cell in odd_b5], [42, 41])

    def test_mixed_rate_primary_cell_is_rejected(self) -> None:
        plan, ledger = PLAN.build_plan("scale-mixed", 1, 20)
        mixed = next(cell for cell in plan["cells"] if cell["phase"] == "knee")
        mixed["endpoint_offered_rps"][0] = 42 if mixed["endpoint_offered_rps"][0] == 41 else 41
        with self.assertRaisesRegex(SUMMARY.ValidationError, "not pure rate"):
            SUMMARY.validate_plan(plan, ledger)

    def test_existing_ledger_overlap_is_fail_closed_even_when_malformed(self) -> None:
        _, ledger = PLAN.build_plan("scale-one", 2, 20)
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            prior = root / "prior" / "sequence-ledger.json"
            prior.parent.mkdir()
            prior.write_text("{not-json}\n")
            with self.assertRaisesRegex(ValueError, "unreadable reservation"):
                PLAN.assert_no_overlap(ledger, root, root / "new" / "sequence-ledger.json")
            prior.write_text(json.dumps(ledger))
            with self.assertRaisesRegex(ValueError, "not globally disjoint"):
                PLAN.assert_no_overlap(ledger, root, root / "new" / "sequence-ledger.json")

    def test_plan_only_is_cluster_free_and_claims_preview_range(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            run_dir = root / "plancheck"
            environment = os.environ.copy()
            environment.update(
                {
                    "SCALE_RUN_ID": "scale-plancheck-test",
                    "CAMPAIGN_INDEX": "97",
                    "RUNG_REPLICAS": "20",
                    "PLAN_ONLY": "1",
                    "RESULT_ROOT": str(root),
                    "RUN_DIR": str(run_dir),
                    # An impossible kubeconfig makes accidental cluster access
                    # observable even on a host with oc credentials.
                    "KUBECONFIG_PATH": str(root / "must-not-be-read"),
                }
            )
            result = subprocess.run(
                [str(ORCHESTRATOR)],
                cwd=ROOT,
                env=environment,
                text=True,
                capture_output=True,
                timeout=20,
                check=False,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertTrue((run_dir / "campaign-plan.json").exists())
            ledger = json.loads((run_dir / "sequence-ledger.json").read_text())
            self.assertEqual(ledger["reservation_status"], "claimed_by_plan")
            self.assertEqual(json.loads((run_dir / "campaign-status.json").read_text())["status"], "planned")


class PreflightTests(unittest.TestCase):
    def evaluate(self, replicas: int, **changes):
        quotas = changes.get("quotas", quota())
        pods = changes.get("pods", {"items": []})
        nodes = changes.get("nodes", {"items": [node("gnr2.fm2aihpcsed.com"), node("rhgnr1")]})
        claim = changes.get("claim", pvc())
        return PREFLIGHT.evaluate(
            quotas,
            pods,
            nodes,
            claim,
            replicas=replicas,
            namespace="llm-d-sc-scaleout",
            target_node="gnr2.fm2aihpcsed.com",
            driver_node="rhgnr1",
            claim_name="classifier-model",
        )

    def test_current_quota_shape_admits_r20(self) -> None:
        result = self.evaluate(20)
        self.assertTrue(result["load_authorized"])
        self.assertEqual(result["disposition"], "pass")

    def test_current_quota_shape_blocks_r30_before_mutation(self) -> None:
        result = self.evaluate(30)
        self.assertFalse(result["load_authorized"])
        self.assertEqual(result["disposition"], "blocked_before_mutation")
        quota_gate = next(gate for gate in result["gates"] if gate["name"] == "namespace_resource_quota_peak")
        self.assertFalse(quota_gate["passed"])
        self.assertLess(quota_gate["evidence"]["quotas"][0]["resources"]["requests.cpu"]["remaining_after"], 0)

    def test_foreign_rwo_mount_blocks(self) -> None:
        foreign = {
            "metadata": {"name": "foreign", "namespace": "llm-d-sc-scaleout"},
            "spec": {
                "nodeName": "other-node",
                "containers": [{"resources": {"requests": {}}}],
                "volumes": [{"persistentVolumeClaim": {"claimName": "classifier-model"}}],
            },
            "status": {"phase": "Running"},
        }
        result = self.evaluate(20, pods={"items": [foreign]})
        self.assertFalse(result["load_authorized"])
        storage = next(gate for gate in result["gates"] if gate["name"] == "model_volume_single_node_mount")
        self.assertEqual(storage["evidence"]["active_mounts_on_other_nodes"][0]["name"], "foreign")

    def test_scheduler_accounts_regular_init_and_overhead(self) -> None:
        pod = {
            "spec": {
                "containers": [
                    {"resources": {"requests": {"cpu": "250m", "memory": "128Mi"}}},
                    {"resources": {"requests": {"cpu": "250m", "memory": "128Mi"}}},
                ],
                "initContainers": [{"resources": {"requests": {"cpu": "2", "memory": "1Gi"}}}],
                "overhead": {"cpu": "10m", "memory": "4Mi"},
            }
        }
        self.assertEqual(
            PREFLIGHT.pod_scheduler_requests(pod),
            {"cpu_millicores": 2010, "memory_bytes": 1028 * 2**20, "pods": 1},
        )


class DecisionAndStaticSafetyTests(unittest.TestCase):
    def test_clean_synthetic_r20_confirms_scoped_knee(self) -> None:
        plan, _ = PLAN.build_plan("scale-decision", 0, 20)
        result = SUMMARY.decision(plan, aggregate_cells(plan))
        self.assertEqual(result["status"], "confirmed_at_rung")
        self.assertTrue(result["knee_confirmed"])
        self.assertGreater(result["horizontal_efficiency_at_41"], 0.99)

    def test_failed_41_is_not_misreported_as_confirmed(self) -> None:
        plan, _ = PLAN.build_plan("scale-decision-bad", 0, 20)
        result = SUMMARY.decision(plan, aggregate_cells(plan, fail_41=True))
        self.assertEqual(result["status"], "knee_at_or_below_41_or_scale_interference")
        self.assertFalse(result["knee_confirmed"])

    def test_no_oc_exec_occurs_inside_or_after_cell_loop(self) -> None:
        script = ORCHESTRATOR.read_text()
        cell_loop = script.index("for (( cell_ordinal=0;")
        self.assertNotIn('"${k[@]}" exec', script[cell_loop:])
        self.assertIn("no_oc_exec_during_plateau", (PLANNER_PATH.read_text()))

    def test_cleanup_is_run_labeled_and_reference_is_never_scaled(self) -> None:
        script = ORCHESTRATOR.read_text()
        self.assertIn('delete jobs -n "$NAMESPACE" -l "$RUN_SELECTOR"', script)
        self.assertIn('delete deployment "$TARGET_DEPLOYMENT"', script)
        self.assertNotIn('scale deployment "$REFERENCE_DEPLOYMENT"', script)
        self.assertIn("reference_deployment_spec_unchanged", script)

    def test_driver_allowlist_is_literal_and_not_environment_overridable(self) -> None:
        script = ORCHESTRATOR.read_text()
        self.assertIn("readonly ARMED_DRIVER_IMAGE=", script)
        self.assertIn("readonly ARMED_DRIVER_SOURCE_SHA256=", script)
        self.assertNotIn("DRIVER_IMAGE=${DRIVER_IMAGE", script)
        self.assertIn('[[ "$local_driver_source_sha" == "$ARMED_DRIVER_SOURCE_SHA256" ]]', script)


if __name__ == "__main__":
    unittest.main()
