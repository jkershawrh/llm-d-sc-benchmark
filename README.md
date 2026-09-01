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
- requested-signal, cache-mix, and constant/burst/sawtooth/spike emulation
- matched ClusterIP, gRPC gateway, and deterministic endpoint-sharding treatments
- artifact/image/model/tokenizer provenance

## Framework layout

- `deploy/arena/`: reproducible Arena workload and telemetry manifests
- `hack/token-payloads`: exact-token, unique-context corpus generator
- `hack/arena-sc-inference-*.sh`: closed-loop and deterministic open-loop cell/matrix runners
- `hack/arena-sc-horizontal-scale-*.{sh,py}`: isolated horizontal campaign planning, preflight, execution, and scoring
- `hack/arena-sc-knee-confirm-*.py`: preregistered paired-block knee confirmation analyzers
- `hack/arena-sc-transport-knee-audit.py`: independent raw-cell accounting,
  topology, health, resource, transition, and checksum audit
- `hack/arena-sc-same-pod-recovery-*.{sh,py}`: recovery-cycle execution and fail-closed reconciliation
- `hack/arena-sc-profile-*.py`: cAdvisor and process/scheduler attribution helpers
- `tests/`: cluster-free regression tests for planners, analyzers, summarizers, and safety gates
- `instrumentation/reference/`: candidate telemetry and benchmark-driver source
  captured from the evaluated classifier tree
- `driver/`: standalone benchmark-owned signal emulator; it uses only the
  public gRPC contract and does not compile or modify classifier code
- `docs/methodology/`: tested methods, result schemas, interpretation rules,
  known harness boundaries, and reference runs
- `docs/results/`: concise, claim-bounded findings
- `evidence/`: compact summaries, provenance, independent audits, and checksums;
  full raw captures remain outside Git

Generated runs default to `results/`, which is intentionally ignored by Git.
Set `RESULT_ROOT` to an external evidence volume for long campaigns.

## Local verification

The analysis and safety suite is cluster-free:

```bash
python3 -m unittest discover -s tests -p 'test_arena_sc_*.py'
```

Shell syntax can be checked without contacting a cluster:

```bash
for script in hack/*.sh; do bash -n "$script"; done
```

Live execution additionally requires `oc`, `jq`, access to the target cluster,
a pinned classifier image, a pinned benchmark-driver image, and unused corpus
sequence ranges. The driver build packager accepts the classifier checkout as
an explicit input:

```bash
./hack/arena-sc-package-driver-build.sh OUTPUT_DIR CLASSIFIER_SOURCE_ROOT
```

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

## Independent signal and transport emulation

`driver/` builds `llm-d-sc-signal-emulator`, an external gRPC client that can
vary requested `signals`, cache-hit/miss ratio, context size, arrival shape,
connection count, concurrency, and endpoint selection. Multiple explicit
`--target` values are assigned deterministically, giving the suite a balanced
direct-endpoint oracle with one global timer.

The Arena transport matrix is deliberately independent of Fleet:

1. emulator to the ordinary ClusterIP;
2. emulator to a benchmark-owned Istio gRPC Gateway;
3. emulator directly and evenly to the five ready target Pod IPs.

All three cells prewarm the same stable key on every target, use the same
aggregate connection/concurrency/request budget, and retain the unchanged
classifier image. Run `hack/arena-sc-transport-matrix.sh` with a digest-pinned
emulator image and an isolated kubeconfig.

The runner brackets every cell with target-Pod and Kubernetes-event snapshots.
It records readiness/liveness warning deltas, restart deltas, and Pod identity
changes after the telemetry bracket. `FAIL_ON_TARGET_HEALTH_ERRORS=true` is the
strict healthy-capacity lane. Explicit break campaigns may set it to `false`;
those cells remain valid observed-break evidence but are not eligible for
steady-state capacity claims. Set different `DRIVER_NODE` and `TARGET_NODE`
values to remove load-generator/target node co-location.

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

The framework now includes cell, matrix, deterministic open-loop, recovery,
profiling, and horizontal-scale orchestration plus structured aggregation and
automatic scoped-knee scoring. It is not yet a single-command qualification
suite: cluster/profile parameterization, corpus allocation, and final report
generation remain explicit operator steps.

The strongest confirmed finding retained here is a service/SLO knee in
`(41, 42]` offered RPS per Pod for the exact unchanged W1/RT1, 64-token
unique-miss, direct-Pod-IP, single-connection, 180-second workload. See
`docs/results/confirmed-knee-20260829.md`.

The corrected node-isolated five-replica cache-hit transport lane confirms a
zero-error boundary in `(250, 500]` aggregate concurrency and places the
operational throughput/latency knee in the 750–1000 region. A clean idle
control contrasts with intermittent probe degradation by concurrency 50,
repeatable all-cell health failure at 125, and explicit overload from 500.
ClusterIP/direct useful throughput remained within roughly 4% at every loaded
rung, aggregate target CPU stayed far below its 20-core limit, and target
throttling was zero. The loaded ladder is observed-break evidence, not healthy
steady-state capacity. See
`docs/results/cache-hit-transport-knee-isolated-20260901.md`; the earlier
`docs/results/cache-hit-transport-knee-20260901.md` is retained as historical
audit context.

No repeatable horizontal replica knee has been established. The exploratory
lane remained efficient through r15; r20 is diagnostic only because its CPU
placement was malformed and it has only one repetition. r40/r50 application
capacity remains unproven.

## Evidence integrity

Keep these result classes distinct:

- untouched-upstream capacity measurements
- separately instrumented candidate measurements
- complete application results
- partial results affected by a harness or cluster failure

Do not extrapolate a failed or partial replica rung as application capacity.

Do not describe the confirmed knee as a hard throughput ceiling: all 42-RPS
requests eventually returned OK, but more than 1% completed outside the
measurement plateau and p99 latency rose into seconds.

## License

Apache License 2.0. See `LICENSE`.
