#!/usr/bin/env python3
"""Analyze the frozen Stage A + Stage B llm-d-sc knee extension.

This read-only analyzer accepts the two schema-v2 sweep summaries.  It keeps
the original Stage A five-block decision intact, treats Stage B as a complete
five-block unit, and pools only the ten paired blocks authorized by the frozen
extension preregistration.
"""

from __future__ import annotations

import argparse
import copy
import hashlib
import importlib.util
import json
import math
import random
import statistics
import sys
from pathlib import Path
from typing import Any


_FIVE_PATH = Path(__file__).with_name("arena-sc-knee-confirm-analyze.py")
_FIVE_SPEC = importlib.util.spec_from_file_location("arena_sc_knee_confirm_five", _FIVE_PATH)
if _FIVE_SPEC is None or _FIVE_SPEC.loader is None:  # pragma: no cover - installation failure
    raise RuntimeError(f"cannot load frozen five-block analyzer {_FIVE_PATH}")
FIVE = importlib.util.module_from_spec(_FIVE_SPEC)
_FIVE_SPEC.loader.exec_module(FIVE)


STAGE_A_RUN_ID = "ol-rt1-knee-confirm-20260829-a"
STAGE_B_RUN_ID = "ol-rt1-knee-confirm-ext-20260829-b"
STAGE_A_SEED = 8_294_102
STAGE_B_SEED = 8_294_103
EXPECTED_STAGE_A_DECISION_SHA256 = (
    "772a2d1038de5a89c74bfa49d18518a798b2ecd9e2f1525a244496423b4fd413"
)
BOOTSTRAP_SEED = 20_260_829
BOOTSTRAP_RESAMPLES = 100_000
LOW_RATE = 41
HIGH_RATE = 42
CLEAN_P99_LIMIT_US = 35_363.0

STAGE_B_ORDER = (
    (1, 1, 42),
    (2, 1, 41),
    (3, 2, 41),
    (4, 2, 42),
    (5, 3, 42),
    (6, 3, 41),
    (7, 4, 41),
    (8, 4, 42),
    (9, 5, 42),
    (10, 5, 41),
)
STAGE_A_ORDER_BY_PAIR = {
    (repetition, rate): order for order, repetition, rate in FIVE.EXPECTED_ORDER
}


def _render(document: dict[str, Any]) -> str:
    return json.dumps(document, indent=2, sort_keys=True) + "\n"


def _sha256_document(document: dict[str, Any]) -> str:
    return hashlib.sha256(_render(document).encode("utf-8")).hexdigest()


def _median(values: list[float]) -> float:
    if not values:
        raise FIVE.ValidationError("median requires observations")
    ordered = sorted(values)
    middle = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[middle]
    return (ordered[middle - 1] + ordered[middle]) / 2


def _bootstrap(effects: list[dict[str, float]]) -> dict[str, Any]:
    count = len(effects)
    if count not in (5, 10):
        raise FIVE.ValidationError("bootstrap requires five or ten paired blocks")
    names = ("delta_success", "delta_drain", "latency_ratio", "marginal_useful_rps")
    samples: dict[str, list[float]] = {name: [] for name in names}
    rng = random.Random(BOOTSTRAP_SEED)
    for _ in range(BOOTSTRAP_RESAMPLES):
        indices = [rng.randrange(count) for _ in range(count)]
        for name in names:
            samples[name].append(_median([effects[index][name] for index in indices]))

    intervals = {
        name: {
            "median_effect": _median([effect[name] for effect in effects]),
            "lower_95": FIVE._percentile(samples[name], 0.025),
            "upper_95": FIVE._percentile(samples[name], 0.975),
        }
        for name in names
    }
    checks = {
        "delta_success_upper_below_zero": intervals["delta_success"]["upper_95"] < 0,
        "delta_drain_lower_above_zero": intervals["delta_drain"]["lower_95"] > 0,
        "latency_ratio_lower_above_1_25": intervals["latency_ratio"]["lower_95"] > 1.25,
        "marginal_useful_upper_below_one": intervals["marginal_useful_rps"]["upper_95"] < 1,
    }
    return {
        "resamples": BOOTSTRAP_RESAMPLES,
        "seed": BOOTSTRAP_SEED,
        "resampling_unit": "whole paired block",
        "statistic": "median paired effect",
        "median_definition": (
            "middle ordered value" if count % 2 else
            "arithmetic mean of the fifth and sixth ordered values"
        ),
        "interval": "95% percentile, R-7 linear empirical quantile",
        "effects": intervals,
        "checks": checks,
        "passed": all(checks.values()),
    }


def _observed_order(summary: dict[str, Any], label: str) -> tuple[tuple[int, int, int], ...]:
    rates = FIVE._list(FIVE._required(summary, "rates", label), f"{label}.rates")
    observed: list[tuple[int, int, int]] = []
    for rate_index, raw_point in enumerate(rates):
        point_path = f"{label}.rates[{rate_index}]"
        point = FIVE._mapping(raw_point, point_path)
        rate = FIVE._rate(
            FIVE._required(point, "offered_rps_per_target", point_path),
            f"{point_path}.offered_rps_per_target",
        )
        cells = FIVE._list(FIVE._required(point, "cells", point_path), f"{point_path}.cells")
        for cell_index, raw_cell in enumerate(cells):
            cell_path = f"{point_path}.cells[{cell_index}]"
            cell = FIVE._mapping(raw_cell, cell_path)
            observed.append(
                (
                    FIVE._integer(FIVE._required(cell, "order", cell_path), f"{cell_path}.order", 1),
                    FIVE._integer(
                        FIVE._required(cell, "repetition", cell_path),
                        f"{cell_path}.repetition",
                        1,
                    ),
                    rate,
                )
            )
    if len(observed) != 10:
        raise FIVE.ValidationError(f"{label} must contain exactly 10 cells")
    if sorted(order for order, _, _ in observed) != list(range(1, 11)):
        raise FIVE.ValidationError(f"{label} cell orders must be unique and contiguous from 1 to 10")
    return tuple(sorted(observed))


def _extract_stage_b(
    raw_summary: Any,
) -> tuple[dict[int, dict[int, dict[str, Any]]], dict[str, Any]]:
    summary = FIVE._mapping(raw_summary, "stage_b")
    observed = _observed_order(summary, "stage_b")
    run_id = FIVE._string(FIVE._required(summary, "run_id", "stage_b"), "stage_b.run_id")
    expected_order_pass = observed == STAGE_B_ORDER
    pair_orders: dict[tuple[int, int], int] = {}
    for order, repetition, rate in observed:
        key = (repetition, rate)
        if key in pair_orders:
            raise FIVE.ValidationError(
                f"stage_b contains duplicate repetition/rate pair {repetition}/{rate}"
            )
        pair_orders[key] = order
    expected_pairs = {
        (repetition, rate)
        for repetition in range(1, 6)
        for rate in (LOW_RATE, HIGH_RATE)
    }
    if set(pair_orders) != expected_pairs:
        raise FIVE.ValidationError(
            "stage_b must contain rates 41 and 42 exactly once in each repetition 1 through 5"
        )
    adjacent_pass = all(
        abs(pair_orders[(block, LOW_RATE)] - pair_orders[(block, HIGH_RATE)]) == 1
        for block in range(1, 6)
    )
    expected_run_ids = all(
        any(
            cell.get("order") == order
            and cell.get("run_id") == f"ol-{STAGE_B_RUN_ID}-o{order:03d}"
            for point in summary["rates"]
            for cell in point["cells"]
        )
        for order in range(1, 11)
    )

    # Reuse the original analyzer's exhaustive schema-v2 field and frozen
    # runtime validation.  Only the run ID and order are adapted in this copy;
    # the original Stage B summary and all evidence fields remain untouched.
    adapted = copy.deepcopy(summary)
    adapted["run_id"] = STAGE_A_RUN_ID
    original_orders = pair_orders
    for point in adapted["rates"]:
        rate = FIVE._rate(point["offered_rps_per_target"], "stage_b.adapted.rate")
        for cell in point["cells"]:
            repetition = FIVE._integer(cell["repetition"], "stage_b.adapted.repetition", 1)
            cell["order"] = STAGE_A_ORDER_BY_PAIR[(repetition, rate)]
    cells_by_rate, validation = FIVE._extract(adapted)
    for rate, cells in cells_by_rate.items():
        for repetition, cell in cells.items():
            cell["order"] = original_orders[(repetition, rate)]

    checks = dict(validation["checks"])
    checks.update(
        {
            "stage_b_frozen_run_id": run_id == STAGE_B_RUN_ID,
            "stage_b_complementary_seed_generated_order": expected_order_pass,
            "stage_b_adjacent_pairing_by_repetition": adjacent_pass,
            "stage_b_cell_run_ids_match_order": expected_run_ids,
        }
    )
    validation = {
        **validation,
        "pairing_method": "map Stage B repetitions 1-5 to global blocks 6-10",
        "expected_randomization_seed": STAGE_B_SEED,
        "seed_evidence_scope": (
            "schema-v2 summary omits the seed value; its exact frozen generated order is validated"
        ),
        "expected_order": [
            {
                "order": order,
                "stage_repetition": repetition,
                "global_block": repetition + 5,
                "offered_rps": rate,
            }
            for order, repetition, rate in STAGE_B_ORDER
        ],
        "checks": checks,
        "all_external_attribution_and_protocol_gates_passed": all(checks.values()),
    }
    return cells_by_rate, validation


def _cell_observation(cell: dict[str, Any]) -> dict[str, Any]:
    return {
        "order": cell["order"],
        "run_id": cell["run_id"],
        "success_ratio": cell["success_ratio"],
        "drain_ratio": cell["drain_ratio"],
        "drained_after_plateau": cell["drained"],
        "service_errors": cell["errors"],
        "in_flight_limit_drops": cell["in_flight_drops"],
        "health_event_violations": cell["health_violations"],
        "successful_completion_p99_us": cell["p99_us"],
        "useful_rps": cell["useful_rps"],
    }


def _make_blocks(
    cells_by_rate: dict[int, dict[int, dict[str, Any]]],
    stage: str,
    global_offset: int,
) -> list[dict[str, Any]]:
    blocks: list[dict[str, Any]] = []
    for repetition in range(1, 6):
        low = cells_by_rate[LOW_RATE][repetition]
        high = cells_by_rate[HIGH_RATE][repetition]
        latency_ratio = None
        if low["p99_us"] is not None and high["p99_us"] is not None and low["p99_us"] > 0:
            latency_ratio = high["p99_us"] / low["p99_us"]
        low_checks = {
            "success_at_least_0_99": low["success_ratio"] >= 0.99,
            "drain_at_most_0_01": low["drain_ratio"] <= 0.01,
            "zero_service_errors": low["errors"] == 0,
            "zero_in_flight_limit_drops": low["in_flight_drops"] == 0,
            "zero_health_violations": low["health_violations"] == 0,
            "zero_restarts_attested": low["telemetry_complete"] and low["telemetry_supporting"],
            "p99_at_most_35_363_ms": low["p99_us"] is not None
            and low["p99_us"] <= CLEAN_P99_LIMIT_US,
        }
        high_checks = {
            "stress_signal": high["success_ratio"] < 0.99
            or high["drain_ratio"] > 0.01
            or high["errors"] > 0,
            "paired_p99_ratio_above_1_25": latency_ratio is not None and latency_ratio > 1.25,
        }
        effects = {
            "delta_success": high["success_ratio"] - low["success_ratio"],
            "delta_drain": high["drain_ratio"] - low["drain_ratio"],
            "latency_ratio": latency_ratio,
            "marginal_useful_rps": high["useful_rps"] - low["useful_rps"],
        }
        directions = {
            "delta_success_below_zero": effects["delta_success"] < 0,
            "delta_drain_above_zero": effects["delta_drain"] > 0,
            "latency_ratio_above_one": latency_ratio is not None and latency_ratio > 1,
            "marginal_useful_below_one": effects["marginal_useful_rps"] < 1,
        }
        blocks.append(
            {
                "global_block": repetition + global_offset,
                "stage": stage,
                "stage_repetition": repetition,
                "rate_41": {
                    "observation": _cell_observation(low),
                    "clean_checks": low_checks,
                    "clean": all(low_checks.values()),
                },
                "rate_42": {
                    "observation": _cell_observation(high),
                    "stress_checks": high_checks,
                    "stressed": all(high_checks.values()),
                },
                "paired_effects": effects,
                "direction_checks": directions,
                "all_directions_agree": all(directions.values()),
            }
        )
    return blocks


def _evaluate(
    blocks: list[dict[str, Any]],
    external_gates_passed: bool,
    allowed_claim: str,
) -> dict[str, Any]:
    count = len(blocks)
    effects = [block["paired_effects"] for block in blocks]
    latency_complete = all(effect["latency_ratio"] is not None for effect in effects)
    bootstrap = (
        _bootstrap([{key: float(value) for key, value in effect.items()} for effect in effects])
        if latency_complete
        else {
            "resamples": BOOTSTRAP_RESAMPLES,
            "seed": BOOTSTRAP_SEED,
            "resampling_unit": "whole paired block",
            "passed": False,
            "reason": "one or more paired successful-latency effects is unavailable",
        }
    )

    useful_41 = [block["rate_41"]["observation"]["useful_rps"] for block in blocks]
    useful_42 = [block["rate_42"]["observation"]["useful_rps"] for block in blocks]
    p99_41 = [block["rate_41"]["observation"]["successful_completion_p99_us"] for block in blocks]
    p99_42 = [block["rate_42"]["observation"]["successful_completion_p99_us"] for block in blocks]
    cv_evaluable = all(value is not None for value in p99_41 + p99_42)
    try:
        if not cv_evaluable:
            raise FIVE.ValidationError("missing p99")
        cv_values = {
            "useful_rps_41": FIVE._sample_cv(useful_41),
            "useful_rps_42": FIVE._sample_cv(useful_42),
            "p99_41": FIVE._sample_cv([float(value) for value in p99_41]),
            "p99_42": FIVE._sample_cv([float(value) for value in p99_42]),
        }
    except FIVE.ValidationError:
        cv_evaluable = False
        cv_values = {}
        cv_checks = {"positive_means_and_complete_latency_populations": False}
    else:
        cv_checks = {
            "useful_rps_41_at_most_0_02": cv_values["useful_rps_41"] <= 0.02,
            "useful_rps_42_at_most_0_02": cv_values["useful_rps_42"] <= 0.02,
            "p99_41_at_most_0_10": cv_values["p99_41"] <= 0.10,
            "p99_42_at_most_0_20": cv_values["p99_42"] <= 0.20,
        }
    variability = {
        "definition": "sample standard deviation (ddof=1) divided by arithmetic mean",
        "pooled_block_count": count,
        "drain_and_error_cv_intentionally_omitted": True,
        "values": cv_values,
        "checks": cv_checks,
        "passed": cv_evaluable and all(cv_checks.values()),
    }

    conditions = {
        "all_cells_attributed": external_gates_passed,
        "rate_41_clean_in_every_block": all(block["rate_41"]["clean"] for block in blocks),
        "rate_42_stressed_in_every_block": all(block["rate_42"]["stressed"] for block in blocks),
        "all_paired_directions_agree": all(block["all_directions_agree"] for block in blocks),
        "paired_bootstrap_intervals_pass": bootstrap["passed"],
        "sample_cv_limits_pass": variability["passed"],
    }
    reasons: list[str] = []
    if not external_gates_passed:
        reasons.append("one or more external attribution or frozen-protocol gates failed")
    failed_low = [block["global_block"] for block in blocks if not block["rate_41"]["clean"]]
    failed_high = [block["global_block"] for block in blocks if not block["rate_42"]["stressed"]]
    failed_directions = [
        block["global_block"] for block in blocks if not block["all_directions_agree"]
    ]
    if failed_low:
        reasons.append(f"41 RPS was not clean in every block; failed global blocks: {failed_low}")
    if failed_high:
        reasons.append(f"42 RPS was not stressed in every block; failed global blocks: {failed_high}")
    if failed_directions:
        reasons.append(f"paired directions did not all agree; failed global blocks: {failed_directions}")
    if not bootstrap["passed"]:
        failed = [name for name, passed in bootstrap.get("checks", {}).items() if not passed]
        reasons.append(
            "paired whole-block bootstrap did not pass"
            + (f": {', '.join(failed)}" if failed else "")
        )
    if not variability["passed"]:
        failed = [name for name, passed in cv_checks.items() if not passed]
        reasons.append("sample CV limits did not pass: " + ", ".join(failed))

    confirmed = all(conditions.values())
    if confirmed:
        reasons = ["all six preregistered numeric and attribution conditions passed"]
    return {
        "block_count": count,
        "cell_count": count * 2,
        "decision": {
            "status": "confirmed" if confirmed else "inconclusive",
            "claim": allowed_claim if confirmed else None,
            "reasons": reasons,
        },
        "conditions": conditions,
        "blocks": blocks,
        "bootstrap": bootstrap,
        "variability": variability,
    }


def _cross_stage_checks(stage_a: dict[str, Any], stage_b: dict[str, Any]) -> dict[str, bool]:
    a_source = FIVE._mapping(
        FIVE._required(stage_a, "source_attestation", "stage_a"), "stage_a.source_attestation"
    )
    b_source = FIVE._mapping(
        FIVE._required(stage_b, "source_attestation", "stage_b"), "stage_b.source_attestation"
    )
    source_keys = ("driver_build_source_sha256", "local_probe_source_sha256")
    return {
        "runtime_signatures_identical": stage_a.get("runtime_signature")
        == stage_b.get("runtime_signature"),
        "source_identities_identical": all(a_source.get(key) == b_source.get(key) for key in source_keys),
        "scheduler_thresholds_identical": stage_a.get("scheduler_attribution_thresholds")
        == stage_b.get("scheduler_attribution_thresholds"),
        "topology_preflight_configuration_identical": stage_a.get("topology_preflight")
        == stage_b.get("topology_preflight"),
        "telemetry_requirement_identical": stage_a.get("telemetry_required")
        == stage_b.get("telemetry_required"),
    }


def _inconclusive_without_pooling(
    stage_a_original: dict[str, Any],
    stage_a_hash: str,
    stage_a_validation: dict[str, Any] | None,
    stage_b_validation: dict[str, Any] | None,
    stage_b_standalone: dict[str, Any] | None,
    cross_checks: dict[str, bool],
    reasons: list[str],
    stage_b_rerun_required: bool,
) -> dict[str, Any]:
    decision = {
        "status": "inconclusive",
        "claim": None,
        "reasons": reasons,
        "full_stage_b_rerun_required": stage_b_rerun_required,
        "further_sample_size_extension_permitted": False,
    }
    return {
        "schema_version": 1,
        "decision": decision,
        "stage_a": {
            "expected_original_decision_sha256": EXPECTED_STAGE_A_DECISION_SHA256,
            "recomputed_original_decision_sha256": stage_a_hash,
            "original_decision_artifact_matches": stage_a_hash
            == EXPECTED_STAGE_A_DECISION_SHA256,
            "original_analysis_preserved_unchanged": stage_a_original,
            "validation": stage_a_validation,
        },
        "stage_b": {
            "validation": stage_b_validation,
            "standalone_sensitivity_analysis": stage_b_standalone,
        },
        "cross_stage_validation": {
            "checks": cross_checks,
            "passed": bool(cross_checks) and all(cross_checks.values()),
        },
        "combined": {
            "pooling_permitted": False,
            "not_pooled_reasons": reasons,
        },
    }


def analyze(
    stage_a_raw: Any,
    stage_b_raw: Any,
    include_stage_b_standalone: bool = False,
) -> dict[str, Any]:
    """Return the frozen hierarchical Stage A, Stage B, and combined analysis."""
    stage_a_original = FIVE.analyze(stage_a_raw)
    stage_a_hash = _sha256_document(stage_a_original)
    stage_a_validation: dict[str, Any] | None = None
    stage_b_validation: dict[str, Any] | None = None
    stage_b_standalone: dict[str, Any] | None = None
    cross_checks: dict[str, bool] = {}

    try:
        stage_a_summary = FIVE._mapping(stage_a_raw, "stage_a")
        stage_a_cells, stage_a_validation = FIVE._extract(stage_a_summary)
    except FIVE.ValidationError as error:
        return _inconclusive_without_pooling(
            stage_a_original,
            stage_a_hash,
            stage_a_validation,
            stage_b_validation,
            stage_b_standalone,
            cross_checks,
            [f"Stage A input validation failed: {error}"],
            stage_b_rerun_required=False,
        )
    stage_a_validation = {
        **stage_a_validation,
        "expected_randomization_seed": STAGE_A_SEED,
        "seed_evidence_scope": (
            "schema-v2 summary omits the seed value; its exact frozen generated order is validated"
        ),
    }

    try:
        stage_b_summary = FIVE._mapping(stage_b_raw, "stage_b")
        stage_b_cells, stage_b_validation = _extract_stage_b(stage_b_summary)
        cross_checks = _cross_stage_checks(stage_a_summary, stage_b_summary)
    except FIVE.ValidationError as error:
        return _inconclusive_without_pooling(
            stage_a_original,
            stage_a_hash,
            stage_a_validation,
            stage_b_validation,
            stage_b_standalone,
            cross_checks,
            [f"Stage B input validation failed: {error}"],
            stage_b_rerun_required=True,
        )

    stage_a_blocks = _make_blocks(stage_a_cells, "A", 0)
    stage_b_blocks = _make_blocks(stage_b_cells, "B", 5)
    if include_stage_b_standalone:
        stage_b_standalone = _evaluate(
            stage_b_blocks,
            stage_b_validation["all_external_attribution_and_protocol_gates_passed"],
            (
                "Stage B alone satisfies the original five-block rule as an internal "
                "replication/sensitivity result; it is not independent external validation."
            ),
        )

    stage_a_artifact_matches = stage_a_hash == EXPECTED_STAGE_A_DECISION_SHA256
    stage_a_status_preserved = stage_a_original.get("decision", {}).get("status") == "inconclusive"
    stage_a_external = stage_a_validation["all_external_attribution_and_protocol_gates_passed"]
    stage_b_external = stage_b_validation["all_external_attribution_and_protocol_gates_passed"]
    cross_pass = all(cross_checks.values())
    forward = sum(
        block["rate_41"]["observation"]["order"]
        < block["rate_42"]["observation"]["order"]
        for block in stage_a_blocks + stage_b_blocks
    )
    balanced_order = forward == 5

    pooling_checks = {
        "stage_a_original_decision_artifact_matches": stage_a_artifact_matches,
        "stage_a_original_status_remains_inconclusive": stage_a_status_preserved,
        "stage_a_external_gates": stage_a_external,
        "stage_b_external_gates": stage_b_external,
        "cross_stage_settings_identical": cross_pass,
        "combined_order_balanced_five_each_direction": balanced_order,
    }
    pooling_permitted = all(pooling_checks.values())
    if not pooling_permitted:
        failed = [name for name, passed in pooling_checks.items() if not passed]
        reasons = ["pooling prohibited because prerequisite checks failed: " + ", ".join(failed)]
        return _inconclusive_without_pooling(
            stage_a_original,
            stage_a_hash,
            stage_a_validation,
            stage_b_validation,
            stage_b_standalone,
            cross_checks,
            reasons,
            stage_b_rerun_required=not stage_b_external or not cross_pass or not balanced_order,
        )

    allowed_claim = (
        "Across the original five blocks and the pre-authorized five-block extension, "
        "the scoped service/SLO knee is confirmed in (41, 42] offered RPS per Pod for "
        "the unchanged W1/RT1, 64-token unique-miss, direct-Pod-IP shape over a "
        "180-second horizon."
    )
    combined = _evaluate(stage_a_blocks + stage_b_blocks, True, allowed_claim)
    decision = {
        **combined["decision"],
        "full_stage_b_rerun_required": False,
        "further_sample_size_extension_permitted": False,
    }
    return {
        "schema_version": 1,
        "preregistration": {
            "stage_a_run_id": STAGE_A_RUN_ID,
            "stage_a_randomization_seed": STAGE_A_SEED,
            "stage_b_run_id": STAGE_B_RUN_ID,
            "stage_b_randomization_seed": STAGE_B_SEED,
            "stage_b_repetitions_to_global_blocks": {
                str(repetition): repetition + 5 for repetition in range(1, 6)
            },
            "stage_a_decision_sha256": EXPECTED_STAGE_A_DECISION_SHA256,
            "no_third_sample_size_extension": True,
        },
        "decision": decision,
        "stage_a": {
            "expected_original_decision_sha256": EXPECTED_STAGE_A_DECISION_SHA256,
            "recomputed_original_decision_sha256": stage_a_hash,
            "original_decision_artifact_matches": stage_a_artifact_matches,
            "original_analysis_preserved_unchanged": stage_a_original,
            "validation": stage_a_validation,
        },
        "stage_b": {
            "validation": stage_b_validation,
            "standalone_sensitivity_analysis": stage_b_standalone,
        },
        "cross_stage_validation": {
            "checks": cross_checks,
            "passed": cross_pass,
        },
        "pooling_validation": {
            "checks": pooling_checks,
            "pooling_permitted": True,
            "forward_order_blocks": forward,
            "reverse_order_blocks": 10 - forward,
        },
        "combined": {
            "pooling_permitted": True,
            **combined,
        },
        "interpretation_limit": (
            "This is not an absolute throughput ceiling, universal llm-d-sc limit, "
            "ClusterIP or production-routing result, same-Pod recovery result, or "
            "20-50 replica scale-out result."
        ),
    }


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Apply the frozen combined 10-block llm-d-sc knee extension rule"
    )
    parser.add_argument("--stage-a-summary", type=Path, required=True)
    parser.add_argument("--stage-b-summary", type=Path, required=True)
    parser.add_argument("--include-stage-b-standalone", action="store_true")
    parser.add_argument("--output-json", type=Path)
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    try:
        with args.stage_a_summary.open(encoding="utf-8") as handle:
            stage_a = json.load(handle)
        with args.stage_b_summary.open(encoding="utf-8") as handle:
            stage_b = json.load(handle)
    except (OSError, json.JSONDecodeError) as error:
        print(f"ERROR: cannot read input summary: {error}", file=sys.stderr)
        return 2
    result = analyze(stage_a, stage_b, args.include_stage_b_standalone)
    rendered = _render(result)
    if args.output_json is not None:
        args.output_json.parent.mkdir(parents=True, exist_ok=True)
        args.output_json.write_text(rendered, encoding="utf-8")
    sys.stdout.write(rendered)
    return 0 if result["decision"]["status"] == "confirmed" else 3


if __name__ == "__main__":
    raise SystemExit(main())
