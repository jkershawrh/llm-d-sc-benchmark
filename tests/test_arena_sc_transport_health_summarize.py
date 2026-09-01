import importlib.util
import unittest
from pathlib import Path


SCRIPT = Path(__file__).parents[1] / "hack" / "arena-sc-transport-health-summarize.py"
SPEC = importlib.util.spec_from_file_location("transport_health", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


def pods(restarts=0, ready=True, uid_suffix=""):
    return {
        "items": [
            {
                "metadata": {"name": f"target-{index}", "uid": f"uid-{index}{uid_suffix}"},
                "spec": {"nodeName": "target-node"},
                "status": {
                    "podIP": f"10.0.0.{index}",
                    "conditions": [{"type": "Ready", "status": "True" if ready else "False"}],
                    "containerStatuses": [{"restartCount": restarts if index == 1 else 0}],
                },
            }
            for index in range(1, 6)
        ]
    }


def events(count=0, probe="Readiness"):
    if not count:
        return {"items": []}
    return {
        "items": [
            {
                "metadata": {"uid": "event-1", "name": "event-1"},
                "type": "Warning",
                "reason": "Unhealthy",
                "count": count,
                "message": f"{probe} probe failed: dial tcp: i/o timeout",
                "involvedObject": {"name": "target-1"},
            }
        ]
    }


class TransportHealthSummaryTests(unittest.TestCase):
    def test_clean_cell_passes(self):
        result = MODULE.summarize(pods(), pods(), events(), events())
        self.assertTrue(result["health_slo_pass"])
        self.assertTrue(result["identity_stable"])

    def test_restart_delta_fails_health_slo(self):
        result = MODULE.summarize(pods(), pods(restarts=1), events(), events())
        self.assertFalse(result["health_slo_pass"])
        self.assertEqual(result["restart_delta_count"], 1)

    def test_aggregated_event_count_uses_delta(self):
        result = MODULE.summarize(pods(), pods(), events(3), events(7))
        self.assertFalse(result["health_slo_pass"])
        self.assertEqual(result["warning_event_delta_count"], 4)
        self.assertEqual(result["warning_event_deltas_by_probe"], {"readiness": 4})
        self.assertEqual(result["warning_event_deltas_by_failure"], {"timeout": 4})
        self.assertEqual(result["warning_affected_pods"], ["target-1"])
        self.assertEqual(result["warning_event_deltas"][0]["probe"], "readiness")

    def test_replaced_pod_is_identity_failure(self):
        result = MODULE.summarize(pods(), pods(uid_suffix="-new"), events(), events())
        self.assertFalse(result["health_slo_pass"])
        self.assertFalse(result["identity_stable"])

    def test_runner_captures_health_after_metric_bracket(self):
        runner = (Path(__file__).parents[1] / "hack" / "arena-sc-transport-matrix.sh").read_text()
        bracket = runner.index('sleep "$METRIC_BRACKET_SECONDS"', runner.index("run_cell()"))
        after = runner.index('capture_active_target_pods "$cell_dir/target-pods-after.json"', bracket)
        health = runner.index('"$cell_dir/health-summary.json"', after)
        self.assertLess(bracket, after)
        self.assertLess(after, health)
        self.assertIn("TARGET_NODE and DRIVER_NODE must differ", runner)
        self.assertIn("wait_for_five_stable_targets", runner)
        self.assertIn(".metadata.deletionTimestamp == null", runner)


if __name__ == "__main__":
    unittest.main()
