#!/usr/bin/env python3

from __future__ import annotations

import copy
import importlib.util
import math
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "hack" / "arena-sc-knee-confirm-analyze.py"
SPEC = importlib.util.spec_from_file_location("arena_sc_knee_confirm_analyze", SCRIPT)
assert SPEC and SPEC.loader
ANALYZER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(ANALYZER)


ORDER = {
    (1, 41): 1,
    (1, 42): 2,
    (2, 42): 3,
    (2, 41): 4,
    (3, 41): 5,
    (3, 42): 6,
    (4, 42): 7,
    (4, 41): 8,
    (5, 41): 9,
    (5, 42): 10,
}


def cell(rate: int, repetition: int) -> dict:
    if rate == 41:
        useful = 40.94 + repetition * 0.005
        success = 0.9994 - repetition * 0.00002
        drain_count = repetition % 2
        errors = 0
        in_flight = 0
        p99 = 27_900 + repetition * 100
    else:
        useful = 41.18 + repetition * 0.006
        success = 0.975 - repetition * 0.001
        drain_count = 150 + repetition
        errors = repetition
        in_flight = repetition * 2
        p99 = 56_000 + repetition * 200
    slots = rate * 180
    return {
        "order": ORDER[(repetition, rate)],
        "repetition": repetition,
        "run_id": f"synthetic-r{repetition}-{rate}",
        "offered_rps_per_target": str(rate),
        "aggregate_offered_rps": rate,
        "aggregate_useful_rps": useful,
        "offered_success_ratio": success,
        "offered_acceptance_ratio": 1.0,
        "error_completed_within_plateau": errors,
        "dropped_in_flight_limit": in_flight,
        "dropped_schedule_late": 0,
        "drained_after_plateau": drain_count,
        "in_flight_drop_ratio": in_flight / slots,
        "schedule_drop_ratio": 0.0,
        "drain_ratio": drain_count / slots,
        "latency_us": {"samples": int(useful * 180), "p99": p99},
        "dispatch_lag_us": {"p99": 2_000},
        "health_event_violations": 0,
        "scheduler_valid": True,
        "scheduler_invalid_reasons": [],
        "telemetry": {
            "required_series_complete": True,
            "supporting_queries_succeeded": True,
            "pod_count": 1,
            "node_count": 2,
        },
        "topology_preflight": {
            "required": True,
            "attested": True,
            "load_authorized": True,
            "verdict": "PASS",
            "placement_verdict": "PASS",
            "target_identities": 1,
            "execution_sha256": "e" * 64,
            "report_sha256": "f" * 64,
        },
        "target_cpusets": ["5,149"],
        "average_target_cpu_cores": 0.98,
    }


def summary() -> dict:
    rates = []
    for rate in (41, 42):
        rates.append(
            {
                "offered_rps_per_target": str(rate),
                "offered_rps_per_target_numeric": float(rate),
                "repetitions": 5,
                "scheduler_valid_repetitions": 5,
                "all_scheduler_valid": True,
                "cells": [cell(rate, repetition) for repetition in range(1, 6)],
            }
        )
    return {
        "schema_version": 2,
        "run_id": "ol-rt1-knee-confirm-20260829-a",
        "protocol": "deterministic_offered_rate_v1",
        "planned_cells": 10,
        "attested_cells": 10,
        "all_accounting_valid": True,
        "all_scheduler_attribution_valid": True,
        "telemetry_required": True,
        "all_required_telemetry_valid": True,
        "all_required_topology_preflights_valid": True,
        "topology_preflight": {"required": True},
        "source_attestation": {
            "runtime_source_linkage_attested": True,
            "driver_build_source_sha256": "a" * 64,
            "local_probe_source_sha256": "a" * 64,
        },
        "scheduler_attribution_thresholds": {
            "max_dispatch_p99_lag_ms": 5.0,
            "max_schedule_drop_ratio": 0.0,
        },
        "runtime_signature": {
            "namespace": "llm-d-sc-scaleout",
            "deployment": "classifier-target",
            "target_node": "gnr2.fm2aihpcsed.com",
            "driver_node": "rhgnr1",
            "target_image": "sha256:04323612ce3f73873b4c3ed6e09264e828241537e2c1a4231b43f32e9744d5aa",
            "driver_image": "registry/driver@sha256:123",
            "model_sha256": "b" * 64,
            "tokenizer_sha256": "c" * 64,
            "topology": "cross-node-direct-gnr2.fm2aihpcsed.com-from-rhgnr1",
            "inference_workers": "1",
            "runtime_threads": {"rayon": "1", "candle": "unset"},
            "qos_class": "Guaranteed",
            "resources": {
                "requests": {"cpu": "2", "memory": "4Gi"},
                "limits": {"cpu": "2", "memory": "4Gi"},
            },
            "replicas": 1,
            "concurrency_per_target": 1,
            "connections_per_target": 1,
            "duration_seconds": 180,
        },
        "rates": rates,
    }


def rate_point(document: dict, rate: int) -> dict:
    return next(
        point for point in document["rates"] if point["offered_rps_per_target_numeric"] == rate
    )


class KneeConfirmationAnalyzerTests(unittest.TestCase):
    def test_synthetic_pass_confirms_all_frozen_rules(self) -> None:
        result = ANALYZER.analyze(summary())

        self.assertEqual(result["decision"]["status"], "confirmed")
        self.assertTrue(all(result["conditions"].values()))
        self.assertTrue(result["bootstrap"]["passed"])
        self.assertEqual(result["bootstrap"]["resamples"], 100_000)
        self.assertEqual(result["bootstrap"]["seed"], 20_260_829)
        self.assertEqual(len(result["blocks"]), 5)
        self.assertFalse(result["decision"]["full_study_rerun_required"])

    def test_mixed_high_rate_outcome_is_inconclusive(self) -> None:
        document = summary()
        high = rate_point(document, 42)["cells"][2]
        high["offered_success_ratio"] = 0.995
        high["drained_after_plateau"] = 1
        high["drain_ratio"] = 1 / (42 * 180)
        high["error_completed_within_plateau"] = 0
        high["dropped_in_flight_limit"] = 0
        high["in_flight_drop_ratio"] = 0.0

        result = ANALYZER.analyze(document)

        self.assertEqual(result["decision"]["status"], "inconclusive")
        self.assertFalse(result["conditions"]["rate_42_stressed_in_all_five_blocks"])
        self.assertIn("failed blocks: [3]", " ".join(result["decision"]["reasons"]))
        self.assertFalse(result["decision"]["full_study_rerun_required"])

    def test_external_gate_failure_requires_complete_rerun(self) -> None:
        document = summary()
        document["all_required_telemetry_valid"] = False

        result = ANALYZER.analyze(document)

        self.assertEqual(result["decision"]["status"], "inconclusive")
        self.assertTrue(result["decision"]["full_study_rerun_required"])
        self.assertFalse(result["conditions"]["all_10_cells_attributed"])
        self.assertIn(
            "telemetry_and_zero_restart_attestation",
            " ".join(result["decision"]["reasons"]),
        )

    def test_nonadjacent_or_changed_frozen_order_fails_protocol_gate(self) -> None:
        document = summary()
        low_block_2 = rate_point(document, 41)["cells"][1]
        high_block_1 = rate_point(document, 42)["cells"][0]
        low_block_2["order"], high_block_1["order"] = (
            high_block_1["order"],
            low_block_2["order"],
        )

        result = ANALYZER.analyze(document)

        self.assertEqual(result["decision"]["status"], "inconclusive")
        self.assertFalse(result["validation"]["checks"]["frozen_randomized_order"])
        self.assertFalse(result["validation"]["checks"]["adjacent_pairing_by_repetition"])
        self.assertTrue(result["decision"]["full_study_rerun_required"])

    def test_missing_required_cell_field_fails_closed(self) -> None:
        document = summary()
        del rate_point(document, 41)["cells"][0]["aggregate_useful_rps"]

        result = ANALYZER.analyze(document)

        self.assertEqual(result["decision"]["status"], "inconclusive")
        self.assertFalse(result["validation"]["structure_valid"])
        self.assertIn("aggregate_useful_rps", result["decision"]["reasons"][0])

    def test_missing_success_latency_is_retained_as_inconclusive_outcome(self) -> None:
        document = summary()
        rate_point(document, 42)["cells"][0]["latency_us"] = None

        result = ANALYZER.analyze(document)

        self.assertEqual(result["decision"]["status"], "inconclusive")
        self.assertTrue(result["validation"]["structure_valid"])
        self.assertFalse(result["conditions"]["paired_bootstrap_intervals_pass"])

    def test_sample_cv_uses_ddof_one(self) -> None:
        # Mean=2, sample standard deviation=1; population SD would be sqrt(2/3).
        self.assertTrue(math.isclose(ANALYZER._sample_cv([1.0, 2.0, 3.0]), 0.5))

    def test_bootstrap_is_deterministic_and_resamples_whole_pairs(self) -> None:
        effects = [
            {
                "delta_success": -0.01 - index * 0.001,
                "delta_drain": 0.02 + index * 0.001,
                "latency_ratio": 1.8 + index * 0.02,
                "marginal_useful_rps": 0.2 + index * 0.01,
            }
            for index in range(5)
        ]
        first = ANALYZER._bootstrap_median_effects(copy.deepcopy(effects))
        second = ANALYZER._bootstrap_median_effects(copy.deepcopy(effects))

        self.assertEqual(first, second)
        self.assertEqual(first["resampling_unit"], "whole paired repetition block")
        self.assertTrue(first["passed"])

    def test_bootstrap_confidence_boundaries_are_strict(self) -> None:
        effects = [
            {
                "delta_success": 0.0,
                "delta_drain": 0.0,
                "latency_ratio": 1.25,
                "marginal_useful_rps": 1.0,
            }
            for _ in range(5)
        ]

        result = ANALYZER._bootstrap_median_effects(effects)

        self.assertFalse(result["passed"])
        self.assertFalse(any(result["checks"].values()))


if __name__ == "__main__":
    unittest.main()
