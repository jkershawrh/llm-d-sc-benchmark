import importlib.util
import unittest
from pathlib import Path


SCRIPT = Path(__file__).parents[1] / "hack" / "arena-sc-transport-resource-summarize.py"
SPEC = importlib.util.spec_from_file_location("transport_resource", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


class TransportResourceSummaryTests(unittest.TestCase):
    def test_aggregates_only_plateau_samples(self):
        payload = {
            "status": "success",
            "data": {
                "result": [
                    {"metric": {"pod": "a"}, "values": [[90, "9"], [100, "1"], [110, "2"]]},
                    {"metric": {"pod": "b"}, "values": [[100, "3"], [110, "4"], [130, "9"]]},
                ]
            },
        }
        result = MODULE.rate_summary(payload, 100, 120)
        self.assertEqual(result["aggregate_samples"], 2)
        self.assertEqual(result["aggregate_mean"], 5)
        self.assertEqual(result["aggregate_max"], 6)

    def test_empty_plateau_is_explicit(self):
        payload = {"status": "success", "data": {"result": []}}
        result = MODULE.rate_summary(payload, 100, 120)
        self.assertFalse(result["available"])
        self.assertIsNone(result["aggregate_mean"])


if __name__ == "__main__":
    unittest.main()
