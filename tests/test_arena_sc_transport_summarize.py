import importlib.util
import json
import tempfile
import unittest
from pathlib import Path


SCRIPT = Path(__file__).parents[1] / "hack" / "arena-sc-transport-summarize.py"
SPEC = importlib.util.spec_from_file_location("transport_summary", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


class TransportSummaryTests(unittest.TestCase):
    def write_cell(self, root, treatment, rps, resources=True, health_pass=True):
        cell = root / f"1-{treatment}"
        cell.mkdir()
        (cell / "result.json").write_text(
            json.dumps(
                {
                    "kind": "llm-d-sc-signal-emulator-result",
                    "cache": {"mode": "hit"},
                    "selected_requests": 100,
                    "successful_requests": 100,
                    "endpoints": [{"statuses": {"OK": 100}}],
                    "elapsed_seconds": 1,
                    "useful_requests_per_second": rps,
                    "successful_rtt_ms": {"p99": 2},
                }
            )
        )
        (cell / "network-distribution.json").write_text(
            json.dumps({"coefficient_of_variation": 0.1, "max_share_over_ideal": 1.1})
        )
        if resources:
            (cell / "resource-summary.json").write_text(
                json.dumps(
                    {
                        "target_cpu_cores": {"available": True},
                        "driver_cpu_cores": {"available": True},
                        "gateway_cpu_cores": {"available": True},
                        "target_throttle_ratio": {"available": True},
                    }
                )
            )
        (cell / "health-summary.json").write_text(
            json.dumps(
                {
                    "identity_stable": True,
                    "before_ready": True,
                    "after_ready": True,
                    "restart_delta_count": 0 if health_pass else 1,
                    "warning_event_delta_count": 0,
                    "health_slo_pass": health_pass,
                }
            )
        )

    def test_complete_matched_campaign_is_claim_eligible(self):
        with tempfile.TemporaryDirectory() as value:
            root = Path(value)
            self.write_cell(root, "clusterip", 90)
            self.write_cell(root, "gateway", 50)
            self.write_cell(root, "direct", 100)
            result = MODULE.summarize(root, 1)
            self.assertTrue(result["campaign_complete"])
            self.assertTrue(result["telemetry_complete"])
            self.assertTrue(result["steady_state_eligible"])
            self.assertEqual(result["paired_repetitions"][0]["clusterip_over_direct_rps"], 0.9)

    def test_missing_cell_is_partial(self):
        with tempfile.TemporaryDirectory() as value:
            root = Path(value)
            self.write_cell(root, "clusterip", 90)
            self.write_cell(root, "gateway", 50)
            result = MODULE.summarize(root, 1)
            self.assertFalse(result["campaign_complete"])
            self.assertIn("partial", result["claim_gate"])

    def test_two_treatment_campaign_has_direct_pair(self):
        with tempfile.TemporaryDirectory() as value:
            root = Path(value)
            self.write_cell(root, "clusterip", 90)
            self.write_cell(root, "direct", 100)
            result = MODULE.summarize(root, 1, ("clusterip", "direct"))
            self.assertTrue(result["campaign_complete"])
            self.assertEqual(result["validity"]["expected_cells"], 2)
            self.assertEqual(result["paired_repetitions"][0]["clusterip_over_direct_rps"], 0.9)

    def test_overload_cell_remains_measurement_valid(self):
        with tempfile.TemporaryDirectory() as value:
            root = Path(value)
            self.write_cell(root, "clusterip", 90)
            path = root / "1-clusterip" / "result.json"
            result = json.loads(path.read_text())
            result["successful_requests"] = 99
            result["endpoints"] = [{"statuses": {"OK": 99, "GRPC_RESOURCEEXHAUSTED": 1}}]
            path.write_text(json.dumps(result))
            summary = MODULE.summarize(root, 1, ("clusterip",))
            self.assertTrue(summary["campaign_complete"])
            self.assertEqual(summary["validity"]["overload_cells"], 1)
            self.assertIn("overload responses", summary["claim_gate"])

    def test_health_break_is_valid_break_evidence_not_steady_state_capacity(self):
        with tempfile.TemporaryDirectory() as value:
            root = Path(value)
            self.write_cell(root, "clusterip", 90, health_pass=False)
            summary = MODULE.summarize(root, 1, ("clusterip",))
            self.assertTrue(summary["campaign_complete"])
            self.assertFalse(summary["steady_state_eligible"])
            self.assertEqual(summary["validity"]["health_break_cells"], 1)
            self.assertIn("observed-break", summary["claim_gate"])


if __name__ == "__main__":
    unittest.main()
