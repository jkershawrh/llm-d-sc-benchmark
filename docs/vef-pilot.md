# VEF capacity-decision pilot

This repository can supply reproducible technical evidence to a Value Evidence
Framework (VEF) pilot. It does **not** prove realized customer ROI by itself.
The benchmark answers where a classifier is healthy, unsafe, saturated, or
cost-inefficient under a declared workload. The pilot adds the business
counterfactual and costs needed to decide whether benchmark-guided capacity
planning creates value.

## Decision and hypothesis

The proposed decision is whether to adopt benchmark-guided capacity
configuration beyond a bounded pilot. The falsifiable hypothesis is:

> Benchmark-guided configuration reduces fully loaded cost per SLO-qualified
> successful classification versus the current/default capacity-planning
> process, without increasing overload, probe failures, restarts, or recovery
> risk.

ClusterIP-versus-direct transport remains a useful technical treatment. It is
not the business counterfactual. The business comparison must independently
run the current/default planning process and the benchmark-guided process over
a matched, representative workload.

## Required intake before a pilot

Copy `examples/vef-project-intake.yaml` to a private planning location and
resolve every `unknown`. In particular, name the product, evidence, finance,
privacy, and customer-validation owners; define the representative population,
SLO-qualified success, minimum meaningful improvement, failure threshold,
loaded role rates, marginal delivery cost, and result-to-decision rule.

The six engineering-effort activities are required: ruleset design, review,
testing, deployment, monitoring, and maintenance. Use role-level loaded rates,
not personal compensation or employee-level time records. Keep initial and
recurring effort distinct.

## Evidence flow

1. Preregister the matched control/treatment, workload digest, bounded window,
   success/SLO definition, unknown-outcome handling, safety thresholds, and
   stop rules.
2. Run the existing benchmark tooling. Keep raw results in `results/`, an
   external evidence volume, or another approved private store.
3. Create one sanitized input conforming to
   `contracts/vef/benchmark-pilot-input.v1alpha1.schema.json`. Cohort operating
   cost covers observed delivery/infrastructure cost; engineering and other
   realization costs are recorded separately to avoid double counting.
4. Generate the deterministic candidate claim:

   ```bash
   python3 hack/vef-export-pilot.py \
     --input private-evidence/pilot-input.json \
     --output pilot-outputs/claim.json
   ```

5. Validate and score the candidate with VEF outside this repository. Only the
   sanitized claim should cross the private/public evidence boundary.

Both `private-evidence/` and `pilot-outputs/` are ignored. Do not put request
payloads, customer data, labels, namespaces, cluster metadata, credentials, or
environment values in the pilot input. The exporter rejects common raw-field
names and never contacts a deployment.

## Fail-closed decision rules

The exporter marks `value_eligible: false`, sets claim confidence to
`unverified`, and sets claimable gross value to zero when any required gate is
missing. It retains the observed cost difference as a candidate measurement so
the gap is auditable without presenting it as realized value.

A claim is blocked by any of these conditions:

- non-representative population or mismatched workload digests;
- non-independent or unmatched control;
- incomplete accounting, breached safety threshold, or incomplete recovery;
- any unknown cohort outcome (it is never interpreted as zero);
- missing customer, finance, or privacy validation;
- unmeasured marginal delivery cost; or
- missing lifecycle engineering-effort category.

Negative results are valid. If treatment cost is higher, gross value remains
zero while realization costs remain visible. A technically successful
benchmark can therefore produce a negative economic result.

## Method mapping

- **BDD:** intake defines the customer-observable outcome, population, window,
  threshold, and falsification rule.
- **EDD:** benchmark evidence and the sanitized bounded summary retain
  provenance, failures, unknowns, and safety outcomes.
- **CDD:** versioned input/output contracts and deterministic export form the
  VEF interoperability boundary.
- **CBT:** independent matched current/default planning is the counterfactual;
  technical transport comparisons do not substitute for it.
- **TDD:** tests cover eligible, incomplete, unsafe, negative, sensitive-field,
  and deterministic-replay cases.
- **Safety and cost-plus:** hard gates prevent financial claims after unsafe
  runs and require lifecycle effort plus marginal delivery cost measurement.

This is a report-only pilot. Do not make CI block benchmark work on the VEF
grade until results have been calibrated across representative projects.
