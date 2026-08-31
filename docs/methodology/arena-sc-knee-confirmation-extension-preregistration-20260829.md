# llm-d-sc W1/RT1 knee confirmation extension preregistration

Status: frozen after Stage A and before any Stage B load on 2026-08-29.

## Why this extension exists

Stage A (`ol-rt1-knee-confirm-20260829-a`) remains immutable and formally
inconclusive. All five paired directions, all per-cell clean/stress rules, all
external attribution gates, all whole-block bootstrap intervals, and three of
four variability limits passed. The sole failed rule was the sample CV of
42-RPS p99 latency: 0.214240 versus the frozen maximum 0.20.

Stage A decision artifact SHA-256:
`772a2d1038de5a89c74bfa49d18518a798b2ecd9e2f1525a244496423b4fd413`.

The pre-existing design allowed extension from five to ten paired blocks when
a CV or confidence interval straddled its boundary. This document fixes that
extension before Stage B. Stage A is not replaced, relabeled, or hidden.

## Stage B frozen protocol

Run ID: `ol-rt1-knee-confirm-ext-20260829-b`

Randomization seed: `8294103`

Sequence reservation: `[20000000000, 20000100010)`

All target, driver, corpus, duration, resource, topology, accounting,
scheduler, OTEL, cgroup, health, and identity settings and gates are identical
to the Stage A preregistration. No early stop, selective block replacement, or
threshold change is permitted.

| Order | Global block | Offered RPS | Slots | Sequence base |
|---:|---:|---:|---:|---:|
| 1 | 6 | 42 | 7560 | 20000000000 |
| 2 | 6 | 41 | 7380 | 20000010001 |
| 3 | 7 | 41 | 7380 | 20000020002 |
| 4 | 7 | 42 | 7560 | 20000030003 |
| 5 | 8 | 42 | 7560 | 20000040004 |
| 6 | 8 | 41 | 7380 | 20000050005 |
| 7 | 9 | 41 | 7380 | 20000060006 |
| 8 | 9 | 42 | 7560 | 20000070007 |
| 9 | 10 | 42 | 7560 | 20000080008 |
| 10 | 10 | 41 | 7380 | 20000090009 |

The combined design is balanced: five blocks run 41 then 42 and five run 42
then 41.

## Analysis hierarchy

1. Preserve and publish Stage A's original inconclusive decision unchanged.
2. Analyze Stage B alone with the original five-block rules as an internal
   replication/sensitivity result. It is not an independent external
   validation because it uses the same cluster and runtime and was triggered
   by Stage A.
3. Use the pooled ten paired blocks as the primary extension endpoint. Pool
   blocks only—never requests and never discovery/refinement cells. Map Stage B
   repetitions 1 through 5 to global blocks 6 through 10.

The combined scoped knee is confirmed only if every original numeric threshold
and all conditions below pass without modification:

1. All 20 cells pass frozen protocol, accounting, source/runtime, scheduler,
   topology, telemetry, identity, cgroup, and health-attribution gates.
2. Rate 41 is clean in all ten blocks under the original clean rule.
3. Rate 42 is stressed and has paired p99 ratio above 1.25 in all ten blocks.
4. All ten blocks have `deltaSuccess < 0`, `deltaDrain > 0`, paired p99 ratio
   above 1, and marginal useful RPS below 1.
5. A 100,000-resample whole-block bootstrap with seed `20260829` passes the
   same percentile 95% interval limits. For ten values, the median is the
   arithmetic mean of the fifth and sixth ordered values.
6. Pooled sample CV (`ddof=1`) remains at most 2% for useful RPS at each rate,
   at most 10% for 41-RPS p99, and at most 20% for 42-RPS p99.

If Stage B has an external attribution failure, do not pool it. Rerun all of
Stage B under a new run ID and disjoint sequence range; never replace only a
favorable or unfavorable cell. If any combined outcome, interval, or CV rule
fails, the result remains inconclusive. There is no third sample-size
extension.

## Allowed claim

Only if the combined rule passes:

> Across the original five blocks and the pre-authorized five-block extension,
> the scoped service/SLO knee is confirmed in `(41, 42]` offered RPS per Pod
> for the unchanged W1/RT1, 64-token unique-miss, direct-Pod-IP shape over a
> 180-second horizon.

This is not an absolute throughput ceiling, universal llm-d-sc limit,
ClusterIP or production-routing result, same-Pod recovery result, or 20-50
replica scale-out result.
