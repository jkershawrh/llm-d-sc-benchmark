import importlib.util
import unittest
from pathlib import Path


SCRIPT = Path(__file__).parents[1] / "hack" / "arena-sc-render-knee-brief.py"
SPEC = importlib.util.spec_from_file_location("render_knee", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


class RenderKneeBriefTests(unittest.TestCase):
    def test_chart_is_labeled_and_contains_both_transports(self):
        rows = [{
            "concurrency": 250,
            "treatments": {
                "clusterip": {"median_useful_rps": 100.0},
                "direct": {"median_useful_rps": 101.0},
            },
        }]
        rendered = MODULE.chart(rows, "median_useful_rps", "Useful throughput", "Requests per second")
        self.assertIn("Useful throughput", rendered)
        self.assertIn("Requests per second", rendered)
        self.assertIn("ClusterIP", rendered)
        self.assertIn("Direct Pod", rendered)


if __name__ == "__main__":
    unittest.main()
