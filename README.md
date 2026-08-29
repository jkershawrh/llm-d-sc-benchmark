# llm-d-sc benchmark framework

Repeatable Kubernetes/OpenShift benchmark tooling for finding performance
knees, overload boundaries, and horizontal scaling bottlenecks in
[`llm-d-semantic-classifier`](https://github.com/llm-d-incubation/llm-d-semantic-classifier).

This repository is deliberately separate from the classifier source. It holds
benchmark workloads, telemetry infrastructure, exact-token corpus tooling,
reference instrumentation, and the methods used to interpret results.

## What it measures

- cache-hit and unique cache-miss throughput
- exact 16/64/128/256-token workloads
- concurrency ladders and per-Pod knees
- queue/admission saturation and explicit shedding
- horizontal replica ladders and endpoint distribution
- p50/p95/p99/max latency
- readiness, liveness, restart, and node stability
- OpenTelemetry overhead at configurable trace sampling ratios
- artifact/image/model/tokenizer provenance

## Framework layout

- `deploy/arena/`: reproducible Arena workload and telemetry manifests
- `hack/token-payloads`: exact-token, unique-context corpus generator
- `hack/arena-otel-overhead-matrix.sh`: A/B/C/D/E telemetry overhead runner
- `instrumentation/reference/`: candidate telemetry and benchmark-driver source
  captured from the evaluated classifier tree
- `docs/methodology/`: tested methods, result schemas, interpretation rules,
  known harness boundaries, and reference runs

## Benchmark sequence

1. Pin the classifier image, model, tokenizer, and driver image.
2. Generate and independently validate disjoint exact-token corpora.
3. Establish a cache-hit transport baseline.
4. Run unique-miss token/concurrency cells with repeated measurements.
5. Apply the predeclared knee rule: the first step with less than 10% useful
   throughput gain and more than 25% p99 growth.
6. Run admission pressure separately and reconcile client outcomes with server
   queue/admission metrics.
7. Scale replicas in controlled batches, checking Pod and node health between
   every rung.
8. Compare telemetry-off, collectors-only, metrics-only, 1% traces, and 10%
   traces using the same binary where attribution requires it.
9. Restore the cluster to the declared safe baseline.

## Arena profile

The checked-in manifests preserve the reproducible 2026-08-28 Arena profile,
including its namespaces, node selectors, image digests, and service names.
They contain no kubeconfig, password, bearer token, pull secret, or model data.

Before using another cluster, parameterize or replace:

- namespace and service names
- node selectors and topology constraints
- driver/classifier image references
- model and tokenizer digests
- corpus PVC/ConfigMap names
- expected collector count
- resource requests and limits

Never commit cluster credentials or generated kubeconfigs. Supply access using
the operator's normal kubeconfig mechanism.

## Current maturity

The individual benchmark components are repeatable and have produced
reconciled evidence. The framework is not yet a single-command qualification
suite: corpus provisioning, the full replica ladder, structured result
aggregation, automatic knee scoring, and HTML report generation still require
orchestration. Reference results are retained so future automation can be
checked against known behavior.

## Evidence integrity

Keep these result classes distinct:

- untouched-upstream capacity measurements
- separately instrumented candidate measurements
- complete application results
- partial results affected by a harness or cluster failure

Do not extrapolate a failed or partial replica rung as application capacity.

## License

Apache License 2.0. See `LICENSE`.
