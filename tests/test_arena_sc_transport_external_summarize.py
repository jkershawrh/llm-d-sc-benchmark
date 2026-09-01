import importlib.util
import unittest
from pathlib import Path


SCRIPT = Path(__file__).parents[1] / "hack" / "arena-sc-transport-external-summarize.py"
SPEC = importlib.util.spec_from_file_location("transport_external", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


class TransportExternalSummaryTests(unittest.TestCase):
    def test_gauge_summary_uses_only_job_window(self):
        payload = {"status": "success", "data": {"result": [
            {"metric": {"instance": "target"}, "values": [[90, "99"], [100, "1"], [110, "3"], [130, "99"]]}
        ]}}
        result = MODULE.gauge_summary(payload, 100, 120, "instance")
        self.assertEqual(result["groups"]["target"]["mean"], 2)
        self.assertEqual(result["groups"]["target"]["max"], 3)

    def test_counter_delta_brackets_and_aggregates_cpu_series(self):
        payload = {"status": "success", "data": {"result": [
            {"metric": {"instance": "target", "cpu": "0"}, "values": [[95, "5"], [100, "7"], [120, "10"], [125, "12"]]},
            {"metric": {"instance": "target", "cpu": "1"}, "values": [[95, "2"], [120, "6"], [125, "8"]]},
        ]}}
        result = MODULE.counter_delta_summary(payload, 100, 120, "instance")
        self.assertEqual(result["groups"]["target"]["delta"], 7)
        self.assertEqual(result["groups"]["target"]["series"], 2)

    def test_counter_reset_is_not_reported_as_negative_delta(self):
        payload = {"status": "success", "data": {"result": [
            {"metric": {"instance": "target"}, "values": [[95, "10"], [125, "1"]]}
        ]}}
        result = MODULE.counter_delta_summary(payload, 100, 120, "instance")
        self.assertFalse(result["available"])
        self.assertEqual(result["rejected_counter_resets"], {"target": 1})


if __name__ == "__main__":
    unittest.main()
