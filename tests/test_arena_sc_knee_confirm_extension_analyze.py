#!/usr/bin/env python3

from __future__ import annotations

import copy
import importlib.util
import json
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "hack" / "arena-sc-knee-confirm-extension-analyze.py"
SPEC = importlib.util.spec_from_file_location("arena_sc_knee_confirm_extension", SCRIPT)
assert SPEC and SPEC.loader
ANALYZER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(ANALYZER)

STAGE_A_PATH = (
    ROOT
    / "tests/fixtures/knee-confirm/stage-a-sweep-summary.json"
)
B_ORDER = {
    (1, 42): 1,
    (1, 41): 2,
    (2, 41): 3,
    (2, 42): 4,
    (3, 42): 5,
    (3, 41): 6,
    (4, 41): 7,
    (4, 42): 8,
    (5, 42): 9,
    (5, 41): 10,
}


def stage_a() -> dict:
    return json.loads(STAGE_A_PATH.read_text(encoding="utf-8"))


def rate_point(document: dict, rate: int) -> dict:
    return next(
        point for point in document["rates"]
        if int(float(point["offered_rps_per_target_numeric"])) == rate
    )


def stage_b_from(stage_a_summary: dict) -> dict:
    document = copy.deepcopy(stage_a_summary)
    document["run_id"] = "ol-rt1-knee-confirm-ext-20260829-b"
    for rate in (41, 42):
        for cell in rate_point(document, rate)["cells"]:
            repetition = cell["repetition"]
            order = B_ORDER[(repetition, rate)]
            cell["order"] = order
            cell["run_id"] = f"ol-ol-rt1-knee-confirm-ext-20260829-b-o{order:03d}"
            if rate == 41:
                cell["latency_us"]["p99"] = 29_800 + repetition * 100
            else:
                slots = 42 * 180
                drained = 100 + repetition
                successes = slots - drained
                cell["drained_after_plateau"] = drained
                cell["drain_ratio"] = drained / slots
                cell["error_completed_within_plateau"] = 0
                cell["dropped_in_flight_limit"] = 0
                cell["in_flight_drop_ratio"] = 0.0
                cell["offered_success_ratio"] = successes / slots
                cell["aggregate_useful_rps"] = successes / 180
                cell["latency_us"]["samples"] = successes
                cell["latency_us"]["p99"] = 2_250_000 + repetition * 50_000
    return document


class ExtensionAnalyzerTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.a = stage_a()

    def test_combined_pass_preserves_stage_a_inconclusive_decision(self) -> None:
        result = ANALYZER.analyze(self.a, stage_b_from(self.a))

        self.assertEqual(result["decision"]["status"], "confirmed")
        self.assertTrue(result["combined"]["pooling_permitted"])
        self.assertEqual(result["combined"]["block_count"], 10)
        self.assertEqual(result["combined"]["cell_count"], 20)
        self.assertEqual(
            result["stage_a"]["original_analysis_preserved_unchanged"]["decision"]["status"],
            "inconclusive",
        )
        self.assertTrue(result["stage_a"]["original_decision_artifact_matches"])
        self.assertEqual(
            result["stage_a"]["recomputed_original_decision_sha256"],
            "772a2d1038de5a89c74bfa49d18518a798b2ecd9e2f1525a244496423b4fd413",
        )
        self.assertEqual(result["pooling_validation"]["forward_order_blocks"], 5)
        self.assertEqual(result["pooling_validation"]["reverse_order_blocks"], 5)
        self.assertEqual(
            result["combined"]["bootstrap"]["median_definition"],
            "arithmetic mean of the fifth and sixth ordered values",
        )
        self.assertEqual(
            [block["global_block"] for block in result["combined"]["blocks"]],
            list(range(1, 11)),
        )
        self.assertIsNone(result["stage_b"]["standalone_sensitivity_analysis"])

    def test_optional_stage_b_standalone_uses_original_five_block_rules(self) -> None:
        result = ANALYZER.analyze(
            self.a,
            stage_b_from(self.a),
            include_stage_b_standalone=True,
        )

        standalone = result["stage_b"]["standalone_sensitivity_analysis"]
        self.assertEqual(standalone["decision"]["status"], "confirmed")
        self.assertEqual(standalone["block_count"], 5)
        self.assertEqual(standalone["bootstrap"]["resamples"], 100_000)
        self.assertEqual(standalone["bootstrap"]["seed"], 20_260_829)
        self.assertEqual(
            [block["global_block"] for block in standalone["blocks"]],
            [6, 7, 8, 9, 10],
        )

    def test_changed_stage_b_order_prohibits_pooling_and_requires_b_rerun(self) -> None:
        b = stage_b_from(self.a)
        low_rep_1 = rate_point(b, 41)["cells"][0]
        low_rep_2 = rate_point(b, 41)["cells"][1]
        low_rep_1["order"], low_rep_2["order"] = low_rep_2["order"], low_rep_1["order"]

        result = ANALYZER.analyze(self.a, b)

        self.assertEqual(result["decision"]["status"], "inconclusive")
        self.assertFalse(result["combined"]["pooling_permitted"])
        self.assertTrue(result["decision"]["full_stage_b_rerun_required"])
        self.assertFalse(
            result["stage_b"]["validation"]["checks"][
                "stage_b_complementary_seed_generated_order"
            ]
        )

    def test_stage_b_external_gate_failure_is_not_pooled(self) -> None:
        b = stage_b_from(self.a)
        b["all_required_telemetry_valid"] = False

        result = ANALYZER.analyze(self.a, b, include_stage_b_standalone=True)

        self.assertEqual(result["decision"]["status"], "inconclusive")
        self.assertFalse(result["combined"]["pooling_permitted"])
        self.assertTrue(result["decision"]["full_stage_b_rerun_required"])
        self.assertEqual(
            result["stage_b"]["standalone_sensitivity_analysis"]["decision"]["status"],
            "inconclusive",
        )

    def test_malformed_stage_b_fails_closed_and_requires_complete_b_rerun(self) -> None:
        b = stage_b_from(self.a)
        del rate_point(b, 42)["cells"][0]["aggregate_useful_rps"]

        result = ANALYZER.analyze(self.a, b)

        self.assertEqual(result["decision"]["status"], "inconclusive")
        self.assertFalse(result["combined"]["pooling_permitted"])
        self.assertTrue(result["decision"]["full_stage_b_rerun_required"])
        self.assertIn("Stage B input validation failed", result["decision"]["reasons"][0])

    def test_mixed_combined_outcome_is_final_inconclusive_without_third_extension(self) -> None:
        b = stage_b_from(self.a)
        high = rate_point(b, 42)["cells"][2]
        slots = 42 * 180
        high["drained_after_plateau"] = 1
        high["drain_ratio"] = 1 / slots
        high["offered_success_ratio"] = (slots - 1) / slots
        high["aggregate_useful_rps"] = (slots - 1) / 180
        high["latency_us"]["samples"] = slots - 1

        result = ANALYZER.analyze(self.a, b)

        self.assertTrue(result["combined"]["pooling_permitted"])
        self.assertEqual(result["decision"]["status"], "inconclusive")
        self.assertFalse(result["decision"]["further_sample_size_extension_permitted"])
        self.assertFalse(result["decision"]["full_stage_b_rerun_required"])
        self.assertFalse(result["combined"]["conditions"]["rate_42_stressed_in_every_block"])
        self.assertIn("failed global blocks: [8]", " ".join(result["decision"]["reasons"]))

    def test_cross_stage_runtime_change_prohibits_pooling(self) -> None:
        b = stage_b_from(self.a)
        b["runtime_signature"]["driver_image"] = "registry/changed@sha256:123"

        result = ANALYZER.analyze(self.a, b)

        self.assertFalse(result["combined"]["pooling_permitted"])
        self.assertFalse(
            result["cross_stage_validation"]["checks"]["runtime_signatures_identical"]
        )
        self.assertTrue(result["decision"]["full_stage_b_rerun_required"])

    def test_even_median_is_mean_of_fifth_and_sixth_positions(self) -> None:
        self.assertEqual(ANALYZER._median([10, 1, 9, 2, 8, 3, 7, 4, 6, 5]), 5.5)
        effects = [
            {
                "delta_success": -float(index + 1),
                "delta_drain": float(index + 1),
                "latency_ratio": 2.0 + index,
                "marginal_useful_rps": -float(index + 1),
            }
            for index in range(10)
        ]

        bootstrap = ANALYZER._bootstrap(effects)

        self.assertEqual(bootstrap["effects"]["delta_success"]["median_effect"], -5.5)
        self.assertEqual(bootstrap["effects"]["delta_drain"]["median_effect"], 5.5)
        self.assertEqual(bootstrap["resamples"], 100_000)


if __name__ == "__main__":
    unittest.main()
