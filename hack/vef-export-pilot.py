#!/usr/bin/env python3
"""Create a deterministic, fail-closed VEF candidate claim from sanitized pilot data.

This tool reads only the file named by --input. It does not contact a cluster,
execute repository code, or inspect environment variables. Raw workload data is
not an accepted input.
"""

from __future__ import annotations

import argparse
from decimal import Decimal, ROUND_HALF_UP
import json
from pathlib import Path
import sys
from typing import Any

SCHEMA = "llm-d-sc.vef-pilot-input.v1alpha1"
OUTPUT_SCHEMA = "llm-d-sc.vef-pilot-claim.v1alpha1"
EFFORT_ACTIVITIES = {
    "ruleset_design",
    "ruleset_review",
    "ruleset_testing",
    "ruleset_deployment",
    "ruleset_monitoring",
    "ruleset_maintenance",
}
FORBIDDEN_KEYS = {
    "content",
    "payload",
    "labels",
    "namespace",
    "cluster",
    "credential",
    "credentials",
    "environment",
    "environment_values",
}


def _decimal(value: Any, name: str) -> Decimal:
    if isinstance(value, bool):
        raise ValueError(f"{name} must be a non-negative number")
    try:
        result = Decimal(str(value))
    except Exception as exc:
        raise ValueError(f"{name} must be a non-negative number") from exc
    if not result.is_finite() or result < 0:
        raise ValueError(f"{name} must be a non-negative number")
    return result


def _integer(value: Any, name: str, *, minimum: int = 0) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise ValueError(f"{name} must be an integer >= {minimum}")
    return value


def _required(mapping: dict[str, Any], keys: tuple[str, ...], prefix: str) -> None:
    missing = [f"{prefix}{key}" for key in keys if key not in mapping]
    if missing:
        raise ValueError("missing required field(s): " + ", ".join(missing))


def _reject_raw_fields(value: Any, path: str = "$") -> None:
    if isinstance(value, dict):
        for key, child in value.items():
            if key.lower() in FORBIDDEN_KEYS:
                raise ValueError(f"raw or sensitive field is not accepted: {path}.{key}")
            _reject_raw_fields(child, f"{path}.{key}")
    elif isinstance(value, list):
        for index, child in enumerate(value):
            _reject_raw_fields(child, f"{path}[{index}]")


def validate(data: dict[str, Any]) -> None:
    _reject_raw_fields(data)
    _required(
        data,
        (
            "schema_version", "pilot_id", "product", "outcome_id", "population",
            "baseline", "treatment", "counterfactual", "safety", "attribution",
            "economics", "approvals", "evidence_sources",
        ),
        "",
    )
    if data["schema_version"] != SCHEMA:
        raise ValueError(f"schema_version must be {SCHEMA}")
    for name in ("pilot_id", "product", "outcome_id"):
        if not isinstance(data[name], str) or not data[name].strip():
            raise ValueError(f"{name} must be a non-empty string")

    population = data["population"]
    _required(population, ("workload_digest", "period_start", "period_end", "representative", "total_signals"), "population.")
    _integer(population["total_signals"], "population.total_signals", minimum=1)
    for cohort_name in ("baseline", "treatment"):
        cohort = data[cohort_name]
        _required(cohort, ("workload_digest", "qualified_successes", "operating_cost_usd", "false_negative_count", "dropped_requests", "unknown_outcomes"), f"{cohort_name}.")
        _integer(cohort["qualified_successes"], f"{cohort_name}.qualified_successes", minimum=1)
        _decimal(cohort["operating_cost_usd"], f"{cohort_name}.operating_cost_usd")
        for field in ("false_negative_count", "dropped_requests", "unknown_outcomes"):
            _integer(cohort[field], f"{cohort_name}.{field}")

    counterfactual = data["counterfactual"]
    _required(counterfactual, ("independent", "matched_population", "competing_factors"), "counterfactual.")
    if not isinstance(counterfactual["competing_factors"], list) or not counterfactual["competing_factors"]:
        raise ValueError("counterfactual.competing_factors must be a non-empty list")

    safety = data["safety"]
    _required(safety, ("accounting_complete", "overload_events", "probe_failures", "restart_delta", "recovery_complete", "failure_threshold_breached"), "safety.")
    for name in ("overload_events", "probe_failures", "restart_delta"):
        _integer(safety[name], f"safety.{name}")

    share = _decimal(data["attribution"].get("product_share"), "attribution.product_share")
    if share > 1:
        raise ValueError("attribution.product_share must be between 0 and 1")

    economics = data["economics"]
    _required(economics, ("currency", "engineering_effort", "other_realization_cost_usd", "marginal_delivery_cost_measured"), "economics.")
    if economics["currency"] != "USD":
        raise ValueError("economics.currency must be USD in v1alpha1")
    _decimal(economics["other_realization_cost_usd"], "economics.other_realization_cost_usd")
    efforts = economics["engineering_effort"]
    if not isinstance(efforts, list) or not efforts:
        raise ValueError("economics.engineering_effort must be a non-empty list")
    for index, effort in enumerate(efforts):
        prefix = f"economics.engineering_effort[{index}]."
        _required(effort, ("activity", "lifecycle", "role", "hours", "loaded_rate_usd", "source"), prefix)
        if effort["activity"] not in EFFORT_ACTIVITIES:
            raise ValueError(prefix + "activity is unknown")
        if effort["lifecycle"] not in {"initial", "recurring"}:
            raise ValueError(prefix + "lifecycle must be initial or recurring")
        _decimal(effort["hours"], prefix + "hours")
        _decimal(effort["loaded_rate_usd"], prefix + "loaded_rate_usd")

    approvals = data["approvals"]
    _required(approvals, ("customer_validated", "finance_approved", "privacy_approved"), "approvals.")
    if not isinstance(data["evidence_sources"], list) or not data["evidence_sources"]:
        raise ValueError("evidence_sources must be a non-empty list")


def _money(value: Decimal) -> float:
    return float(value.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))


def build_export(data: dict[str, Any]) -> dict[str, Any]:
    validate(data)
    baseline = data["baseline"]
    treatment = data["treatment"]
    safety = data["safety"]
    counterfactual = data["counterfactual"]
    approvals = data["approvals"]
    economics = data["economics"]

    gaps: list[str] = []
    if not data["population"]["representative"]:
        gaps.append("population is not confirmed representative")
    if baseline["workload_digest"] != treatment["workload_digest"] or baseline["workload_digest"] != data["population"]["workload_digest"]:
        gaps.append("baseline and treatment workload digests do not match")
    if not counterfactual["independent"]:
        gaps.append("counterfactual is not independent")
    if not counterfactual["matched_population"]:
        gaps.append("counterfactual population is not matched")
    if not safety["accounting_complete"]:
        gaps.append("outcome accounting is incomplete")
    if baseline["unknown_outcomes"] or treatment["unknown_outcomes"]:
        gaps.append("cohort contains unknown outcomes")
    if safety["failure_threshold_breached"]:
        gaps.append("preregistered safety threshold was breached")
    if not safety["recovery_complete"]:
        gaps.append("recovery is incomplete")
    for name in ("customer_validated", "finance_approved", "privacy_approved"):
        if not approvals[name]:
            gaps.append(name.replace("_", " ") + " is missing")
    if not economics["marginal_delivery_cost_measured"]:
        gaps.append("marginal delivery cost is not measured")
    activities = {row["activity"] for row in economics["engineering_effort"]}
    for activity in sorted(EFFORT_ACTIVITIES - activities):
        gaps.append(f"engineering effort is missing {activity}")

    eligible = not gaps
    baseline_unit_cost = _decimal(baseline["operating_cost_usd"], "baseline cost") / Decimal(baseline["qualified_successes"])
    treatment_unit_cost = _decimal(treatment["operating_cost_usd"], "treatment cost") / Decimal(treatment["qualified_successes"])
    observed_difference = (baseline_unit_cost - treatment_unit_cost) * Decimal(treatment["qualified_successes"])
    observed_gross = max(Decimal(0), observed_difference)

    effort_rows = []
    realization_cost = _decimal(economics["other_realization_cost_usd"], "other realization cost")
    for effort in economics["engineering_effort"]:
        cost = _decimal(effort["hours"], "hours") * _decimal(effort["loaded_rate_usd"], "rate")
        realization_cost += cost
        effort_rows.append({**effort, "cost_usd": _money(cost)})

    method = "matched_control" if counterfactual["independent"] and counterfactual["matched_population"] else "assertion"
    claim = {
        "id": f"{data['product']}.{data['pilot_id']}.cost-per-qualified-success",
        "product": data["product"],
        "outcome_id": data["outcome_id"],
        "value_type": "cost_avoidance",
        "measurement": {
            "value_evidence_contract": "vef.claim.v1alpha1",
            "unit": "USD_per_SLO_qualified_success",
            "timestamp_start": data["population"]["period_start"],
            "timestamp_end": data["population"]["period_end"],
            "period_start": data["population"]["period_start"],
            "period_end": data["population"]["period_end"],
            "workload_digest": data["population"]["workload_digest"],
            "total_signals": data["population"]["total_signals"],
            "baseline_qualified_successes": baseline["qualified_successes"],
            "treatment_qualified_successes": treatment["qualified_successes"],
            "baseline_false_negative_count": baseline["false_negative_count"],
            "treatment_false_negative_count": treatment["false_negative_count"],
            "baseline_dropped_requests": baseline["dropped_requests"],
            "treatment_dropped_requests": treatment["dropped_requests"],
            "baseline_unknown_outcomes": baseline["unknown_outcomes"],
            "treatment_unknown_outcomes": treatment["unknown_outcomes"],
            "unknown": bool(baseline["unknown_outcomes"] or treatment["unknown_outcomes"]),
            "quality_failures": {
                "false_negative": baseline["false_negative_count"] + treatment["false_negative_count"],
                "dropped": baseline["dropped_requests"] + treatment["dropped_requests"],
            },
            "baseline_cost_per_qualified_success_usd": _money(baseline_unit_cost),
            "treatment_cost_per_qualified_success_usd": _money(treatment_unit_cost),
            "observed_gross_value_candidate_usd": _money(observed_gross),
            "safety": safety,
            "dangerous_misses": 1 if safety["failure_threshold_breached"] else 0,
        },
        "counterfactual": {
            "method": method,
            "expected_without_product": _money(baseline_unit_cost),
        },
        "attribution": {
            "product_share": float(data["attribution"]["product_share"]),
            "competing_factors": sorted(counterfactual["competing_factors"]),
        },
        "financial_model": {
            "gross_value": _money(observed_gross) if eligible else 0.0,
            "currency": "USD",
            "customer_validated": approvals["customer_validated"],
            "engineering_effort": effort_rows,
            "initial_engineering_cost_usd": _money(sum(Decimal(str(r["cost_usd"])) for r in effort_rows if r["lifecycle"] == "initial")),
            "recurring_engineering_cost_usd": _money(sum(Decimal(str(r["cost_usd"])) for r in effort_rows if r["lifecycle"] == "recurring")),
        },
        "evidence": {
            "confidence": "high" if eligible else "unverified",
            "source": "sanitized_bounded_pilot",
            "sources": sorted(data["evidence_sources"]),
            "reproducible": bool(safety["accounting_complete"]),
            "value_eligible": eligible,
        },
        "realization_cost": _money(realization_cost),
    }
    return {
        "schema_version": OUTPUT_SCHEMA,
        "claim_status": "decision-grade" if eligible else "unproven",
        "value_eligible": eligible,
        "eligibility_gaps": sorted(gaps),
        "claim": claim,
        "notice": "Repository structure and benchmark performance are not proof of realized customer value.",
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True, type=Path, help="sanitized pilot input JSON")
    parser.add_argument("--output", type=Path, help="candidate claim JSON (stdout when omitted)")
    args = parser.parse_args(argv)
    try:
        data = json.loads(args.input.read_text(encoding="utf-8"))
        export = build_export(data)
        rendered = json.dumps(export, indent=2, sort_keys=True) + "\n"
        if args.output:
            args.output.parent.mkdir(parents=True, exist_ok=True)
            args.output.write_text(rendered, encoding="utf-8")
        else:
            sys.stdout.write(rendered)
    except (OSError, json.JSONDecodeError, ValueError, TypeError) as exc:
        print(f"vef pilot export failed: {exc}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
