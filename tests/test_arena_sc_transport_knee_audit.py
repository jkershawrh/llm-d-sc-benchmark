import importlib.util
import json
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location(
    "transport_knee_audit", ROOT / "hack" / "arena-sc-transport-knee-audit.py"
)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader
SPEC.loader.exec_module(MODULE)


class TransportKneeAuditTests(unittest.TestCase):
    def write(self, path: Path, value: dict) -> None:
        path.write_text(json.dumps(value))

    def make_cell(self, root: Path, statuses=None) -> Path:
        cell = root / "1-clusterip"
        cell.mkdir(parents=True)
        statuses = statuses or {"OK": 100}
        self.write(
            cell / "result.json",
            {
                "selected_requests": 100,
                "successful_requests": statuses.get("OK", 0),
                "elapsed_seconds": 1.0,
                "useful_requests_per_second": 100.0,
                "successful_rtt_ms": {"p50": 1.0, "p95": 2.0, "p99": 3.0},
                "transport": {"concurrency": 50},
                "endpoints": [{"statuses": statuses}],
            },
        )
        self.write(
            cell / "health-summary.json",
            {
                "health_slo_pass": True,
                "identity_stable": True,
                "restart_delta_count": 0,
                "warning_event_delta_count": 0,
                "warning_event_deltas_by_probe": {},
                "warning_event_deltas_by_failure": {},
            },
        )
        self.write(
            cell / "resource-summary.json",
            {
                "target_cpu_cores": {
                    "aggregate_max": 1.0,
                    "aggregate_samples": 4,
                    "limit_cores_per_pod": 4.0,
                },
                "driver_cpu_cores": {"aggregate_max": 2.0, "limit_cores": 8.0},
                "target_throttle_ratio": {"aggregate_max": 0.0},
            },
        )
        self.write(
            cell / "driver-pod.json",
            {"items": [{"spec": {"nodeName": "driver-node"}}]},
        )
        return cell

    def test_audit_cell_recomputes_raw_accounting(self):
        with tempfile.TemporaryDirectory() as directory:
            cell = self.make_cell(Path(directory), {"OK": 99, "GRPC_RESOURCEEXHAUSTED": 1})
            result = MODULE.audit_cell(cell)
            self.assertEqual(result["successful_requests"], 99)
            self.assertEqual(result["error_requests"], 1)
            self.assertEqual(result["statuses"]["GRPC_RESOURCEEXHAUSTED"], 1)
            self.assertEqual(result["driver_node"], "driver-node")

    def test_audit_cell_rejects_incomplete_status_accounting(self):
        with tempfile.TemporaryDirectory() as directory:
            cell = self.make_cell(Path(directory), {"OK": 99})
            with self.assertRaisesRegex(ValueError, "accounted 99 != selected 100"):
                MODULE.audit_cell(cell)

    def test_percent_change(self):
        self.assertAlmostEqual(MODULE.percent_change(100.0, 104.0), 4.0)

    def test_aggregation_pools_repeated_runs_at_same_concurrency(self):
        cell = {
            "treatment": "clusterip",
            "useful_rps": 100.0,
            "p99_ms": 3.0,
            "selected_requests": 100,
            "error_requests": 0,
            "health_slo_pass": True,
            "restart_delta": 0,
            "warning_event_delta": 0,
            "statuses": {"OK": 100},
        }
        runs = [
            {"concurrency": 50, "topology_isolated": True, "cells": [cell]},
            {
                "concurrency": 50,
                "topology_isolated": True,
                "cells": [{**cell, "useful_rps": 120.0}],
            },
        ]
        rows = MODULE.aggregate_by_concurrency(runs)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["runs"], 2)
        self.assertEqual(rows[0]["treatments"]["clusterip"]["median_useful_rps"], 110.0)


if __name__ == "__main__":
    unittest.main()
