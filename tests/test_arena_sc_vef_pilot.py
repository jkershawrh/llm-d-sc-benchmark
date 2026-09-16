import importlib.util
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "hack" / "vef-export-pilot.py"
SPEC = importlib.util.spec_from_file_location("vef_export_pilot", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)


def pilot_input():
    efforts = []
    for activity in sorted(MODULE.EFFORT_ACTIVITIES):
        efforts.append(
            {
                "activity": activity,
                "lifecycle": "recurring" if activity in {"ruleset_monitoring", "ruleset_maintenance"} else "initial",
                "role": "platform_engineer",
                "hours": 1,
                "loaded_rate_usd": 100,
                "source": "bounded_work_log",
            }
        )
    return {
        "schema_version": "llm-d-sc.vef-pilot-input.v1alpha1",
        "pilot_id": "pilot-001",
        "product": "llm-d-sc-benchmark",
        "outcome_id": "cost-per-qualified-classification",
        "population": {
            "workload_digest": "sha256:workload",
            "period_start": "2026-09-01T00:00:00Z",
            "period_end": "2026-09-02T00:00:00Z",
            "representative": True,
            "total_signals": 2000,
        },
        "baseline": {"workload_digest": "sha256:workload", "qualified_successes": 1000, "operating_cost_usd": 200, "false_negative_count": 0, "dropped_requests": 0, "unknown_outcomes": 0},
        "treatment": {"workload_digest": "sha256:workload", "qualified_successes": 1000, "operating_cost_usd": 150, "false_negative_count": 0, "dropped_requests": 0, "unknown_outcomes": 0},
        "counterfactual": {
            "independent": True,
            "matched_population": True,
            "competing_factors": ["platform contention", "workload mix"],
        },
        "safety": {
            "accounting_complete": True,
            "overload_events": 0,
            "probe_failures": 0,
            "restart_delta": 0,
            "recovery_complete": True,
            "failure_threshold_breached": False,
        },
        "attribution": {"product_share": 0.5},
        "economics": {
            "currency": "USD",
            "engineering_effort": efforts,
            "other_realization_cost_usd": 25,
            "marginal_delivery_cost_measured": True,
        },
        "approvals": {"customer_validated": True, "finance_approved": True, "privacy_approved": True},
        "evidence_sources": ["sanitized/pilot-summary.json", "evidence/benchmark-decision.json"],
    }


class VefPilotTests(unittest.TestCase):
    def test_versioned_value_evidence_contract_is_compatible(self):
        data = pilot_input()
        MODULE.validate(data)
        report = MODULE.build_export(data)
        self.assertEqual(report["schema_version"], "llm-d-sc.vef-pilot-claim.v1alpha1")
        value_evidence = report["claim"]["measurement"]["value_evidence_contract"]
        self.assertEqual(value_evidence, "vef.claim.v1alpha1")

    def test_complete_pilot_can_become_value_eligible(self):
        report = MODULE.build_export(pilot_input())
        self.assertTrue(report["value_eligible"])
        self.assertEqual(report["claim_status"], "decision-grade")
        self.assertEqual(report["claim"]["financial_model"]["gross_value"], 50.0)
        self.assertEqual(report["claim"]["realization_cost"], 625.0)
        self.assertEqual(report["claim"]["counterfactual"]["method"], "matched_control")

    def test_missing_approval_fails_closed_without_hiding_observation(self):
        data = pilot_input()
        data["approvals"]["customer_validated"] = False
        report = MODULE.build_export(data)
        self.assertFalse(report["value_eligible"])
        self.assertEqual(report["claim_status"], "unproven")
        self.assertEqual(report["claim"]["financial_model"]["gross_value"], 0.0)
        self.assertEqual(report["claim"]["measurement"]["observed_gross_value_candidate_usd"], 50.0)
        self.assertIn("customer validated is missing", report["eligibility_gaps"])

    def test_negative_result_is_preserved(self):
        data = pilot_input()
        data["treatment"]["operating_cost_usd"] = 250
        report = MODULE.build_export(data)
        self.assertTrue(report["value_eligible"])
        self.assertEqual(report["claim"]["financial_model"]["gross_value"], 0.0)
        self.assertGreater(report["claim"]["realization_cost"], 0)

    def test_safety_failure_blocks_value_claim(self):
        data = pilot_input()
        data["safety"]["failure_threshold_breached"] = True
        report = MODULE.build_export(data)
        self.assertFalse(report["value_eligible"])
        self.assertIn("preregistered safety threshold was breached", report["eligibility_gaps"])

    def test_unknown_outcome_is_not_interpreted_as_zero(self):
        data = pilot_input()
        data["treatment"]["unknown_outcomes"] = 1
        report = MODULE.build_export(data)
        self.assertFalse(report["value_eligible"])
        self.assertIn("cohort contains unknown outcomes", report["eligibility_gaps"])

    def test_raw_or_sensitive_fields_are_rejected(self):
        for forbidden in ("payload", "content", "labels", "namespace", "cluster", "credentials"):
            data = pilot_input()
            data["unexpected"] = {forbidden: "must-not-appear"}
            with self.subTest(forbidden=forbidden):
                with self.assertRaisesRegex(ValueError, "raw or sensitive field"):
                    MODULE.build_export(data)

    def test_output_is_deterministic(self):
        data = pilot_input()
        with tempfile.TemporaryDirectory() as tmp:
            input_path = Path(tmp) / "input.json"
            first = Path(tmp) / "first.json"
            second = Path(tmp) / "second.json"
            input_path.write_text(json.dumps(data), encoding="utf-8")
            for output in (first, second):
                completed = subprocess.run(
                    [sys.executable, str(SCRIPT), "--input", str(input_path), "--output", str(output)],
                    check=False,
                    capture_output=True,
                    text=True,
                )
                self.assertEqual(completed.returncode, 0, completed.stderr)
            self.assertEqual(first.read_bytes(), second.read_bytes())


if __name__ == "__main__":
    unittest.main()
