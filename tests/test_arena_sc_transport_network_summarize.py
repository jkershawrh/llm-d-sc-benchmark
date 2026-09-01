import importlib.util
import unittest
from pathlib import Path


SCRIPT = Path(__file__).parents[1] / "hack" / "arena-sc-transport-network-summarize.py"
SPEC = importlib.util.spec_from_file_location("transport_network", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


class TransportNetworkSummaryTests(unittest.TestCase):
    def test_uses_samples_bracketing_the_job(self):
        payload = {
            "status": "success",
            "data": {
                "result": [
                    {"metric": {"pod": "a"}, "values": [[90, "100"], [121, "100"], [130, "300"]]},
                    {"metric": {"pod": "b"}, "values": [[90, "50"], [121, "50"], [130, "250"]]},
                ]
            },
        }
        result = MODULE.summarize(payload, 100, 120, {"a", "b"})
        self.assertEqual(result["total_receive_bytes_delta"], 400)
        self.assertEqual([row["share"] for row in result["pods"]], [0.5, 0.5])
        self.assertEqual(result["coefficient_of_variation"], 0)
        self.assertEqual(result["max_share_over_ideal"], 1)

    def test_rejects_missing_pod_series(self):
        payload = {
            "status": "success",
            "data": {"result": [{"metric": {"pod": "a"}, "values": [[90, "1"], [130, "2"]]}]},
        }
        with self.assertRaisesRegex(ValueError, "series mismatch"):
            MODULE.summarize(payload, 100, 120, {"a", "b"})

    def test_rejects_counter_reset(self):
        payload = {
            "status": "success",
            "data": {"result": [{"metric": {"pod": "a"}, "values": [[90, "10"], [130, "2"]]}]},
        }
        with self.assertRaisesRegex(ValueError, "counter reset"):
            MODULE.summarize(payload, 100, 120, {"a"})


if __name__ == "__main__":
    unittest.main()
