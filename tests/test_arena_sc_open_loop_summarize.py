#!/usr/bin/env python3
"""Focused regression tests for deterministic open-loop knee inference."""

from __future__ import annotations

import importlib.util
import sys
import unittest
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
SUMMARY_RUNNER = ROOT / "hack" / "arena-sc-open-loop-summarize.py"
SPEC = importlib.util.spec_from_file_location("arena_sc_open_loop_knee", SUMMARY_RUNNER)
assert SPEC and SPEC.loader
SUMMARY = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = SUMMARY
SPEC.loader.exec_module(SUMMARY)


def rate_point(
    rate: int,
    p99_us: int,
    *,
    useful_rps: float | None = None,
    success_ratio: float = 1.0,
    drain_ratio: float = 0.0,
    errors: int = 0,
) -> dict[str, Any]:
    useful = float(rate) if useful_rps is None else useful_rps
    return {
        "offered_rps_per_target": str(rate),
        "all_scheduler_valid": True,
        "median_offered_success_ratio": success_ratio,
        "median_in_flight_drop_ratio": 0.0,
        "median_drain_ratio": drain_ratio,
        "total_errors_within_plateau": errors,
        "total_health_event_violations": 0,
        "median_latency_p99_us": p99_us,
        "median_aggregate_useful_rps": useful,
        "cells": [{"aggregate_offered_rps": float(rate)}],
        "repetitions": 1,
        "useful_rps_cv": None,
        "latency_p99_cv": None,
    }


class OpenLoopKneeInferenceTests(unittest.TestCase):
    def actual_clean_prefix(self) -> list[dict[str, Any]]:
        return [
            rate_point(20, 27_062),
            rate_point(29, 35_491),
            rate_point(35, 29_709),
            rate_point(39, 28_251, useful_rps=38.983333333333334,
                       success_ratio=0.9995726495726496,
                       drain_ratio=0.00042735042735042735),
            rate_point(41, 33_203, useful_rps=40.983333333333334,
                       success_ratio=0.9995934959349594,
                       drain_ratio=0.0004065040650406504),
        ]

    def test_non_monotonic_clean_p99_outlier_does_not_bracket(self) -> None:
        knee = SUMMARY.infer_knee(self.actual_clean_prefix())
        self.assertEqual(knee["status"], "not_reached")
        self.assertNotIn("unconfirmed_latency_candidate_rate_per_target", knee)

    def test_actual_shape_brackets_first_real_stress_at_41_to_43(self) -> None:
        rates = self.actual_clean_prefix()
        rates.append(
            rate_point(
                43,
                2_125_997,
                useful_rps=41.46666666666667,
                success_ratio=0.9643410852713178,
                drain_ratio=0.03565891472868217,
            )
        )
        knee = SUMMARY.infer_knee(rates)
        self.assertEqual(knee["status"], "bracketed")
        self.assertEqual(knee["lower_clean_rate_per_target"], "41")
        self.assertEqual(knee["upper_stress_rate_per_target"], "43")
        self.assertIn("median offered-success ratio below 99%", knee["triggers"])
        self.assertIn("more than 1% of offered work drained after plateau", knee["triggers"])

    def test_persistent_clean_latency_elevation_brackets_latency_only_knee(self) -> None:
        rates = [
            rate_point(20, 27_062),
            rate_point(29, 28_100),
            rate_point(35, 35_000),
            rate_point(39, 36_500),
        ]
        knee = SUMMARY.infer_knee(rates)
        self.assertEqual(knee["status"], "bracketed")
        self.assertEqual(knee["lower_clean_rate_per_target"], "29")
        self.assertEqual(knee["upper_stress_rate_per_target"], "35")
        self.assertEqual(knee["latency_confirmation_rate_per_target"], "39")
        self.assertEqual(knee["marginal_efficiency"], 1.0)
        self.assertIn(
            "latency elevation persisted at the next higher offered-rate point",
            knee["triggers"],
        )

    def test_last_point_latency_elevation_is_reported_but_not_bracketed(self) -> None:
        rates = [
            rate_point(20, 27_062),
            rate_point(29, 28_100),
            rate_point(35, 35_000),
        ]
        knee = SUMMARY.infer_knee(rates)
        self.assertEqual(knee["status"], "not_reached")
        self.assertEqual(knee["unconfirmed_latency_candidate_rate_per_target"], "35")


if __name__ == "__main__":
    unittest.main()
