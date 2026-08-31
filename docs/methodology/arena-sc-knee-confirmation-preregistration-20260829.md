# llm-d-sc W1/RT1 knee confirmation preregistration

Status: frozen before confirmation load begins on 2026-08-29.

## Claim under test

For the exact unchanged-image test shape below, the service/SLO knee lies in
`(41, 42]` offered requests per second per Pod over a 180-second plateau. This
means 41 RPS is the highest tested clean offer and 42 RPS is the first tested
stressed offer. It is not a claim that 42 RPS is sustainable capacity, a
universal llm-d-sc limit, or a production-routing result.

The independent three-block refinement remains exploratory and will not be
pooled into the primary confirmation statistics.

## Frozen runtime and protocol

- target image digest:
  `sha256:04323612ce3f73873b4c3ed6e09264e828241537e2c1a4231b43f32e9744d5aa`
- target: one replica, `LLM_D_SC_INFERENCE_WORKERS=1`,
  `RAYON_NUM_THREADS=1`, Candle threads unset
- target resources: Guaranteed, request=limit 2 CPU and 4 GiB
- target node: `gnr2.fm2aihpcsed.com`
- driver node: `rhgnr1`
- direct Pod IP, one persistent HTTP/2 connection, open-loop concurrency 1
- exact 64-token globally unique cache-miss contexts
- fresh target Pod for each cell
- five paired randomized blocks; each adjacent 41/42 pair is one experimental
  unit
- 180-second plateau, 90-second start delay, 30-second quiescence
- `MAX_IN_FLIGHT=512`, 10,000 rows per endpoint
- required exact-Pod topology preflight, source/runtime invariance, request
  accounting, scheduler attribution, OTEL resource telemetry, cgroup, health,
  and restart gates

## Frozen order and corpus reservation

Run ID: `ol-rt1-knee-confirm-20260829-a`

Randomization seed: `8294102`

Sequence reservation: `[19000000000, 19000100010)`

| Order | Block | Offered RPS | Slots | Sequence base |
|---:|---:|---:|---:|---:|
| 1 | 1 | 41 | 7380 | 19000000000 |
| 2 | 1 | 42 | 7560 | 19000010001 |
| 3 | 2 | 42 | 7560 | 19000020002 |
| 4 | 2 | 41 | 7380 | 19000030003 |
| 5 | 3 | 41 | 7380 | 19000040004 |
| 6 | 3 | 42 | 7560 | 19000050005 |
| 7 | 4 | 42 | 7560 | 19000060006 |
| 8 | 4 | 41 | 7380 | 19000070007 |
| 9 | 5 | 41 | 7380 | 19000080008 |
| 10 | 5 | 42 | 7560 | 19000090009 |

No early stopping or post-start threshold changes are permitted.

## Frozen decision rule

For block `b` and rate `r`, define offered-success ratio `S[r,b]`, drain ratio
`D[r,b]`, successful completion-window p99 latency `L[r,b]`, and useful RPS
`U[r,b]`. Paired effects are:

- `deltaS[b] = S[42,b] - S[41,b]`
- `deltaD[b] = D[42,b] - D[41,b]`
- `latencyRatio[b] = L[42,b] / L[41,b]`
- `marginalUseful[b] = U[42,b] - U[41,b]`, because the offer increment is 1
  RPS

The clean p99 reference is frozen from the exploratory 40-RPS median at
28.290 ms. Its 1.25x threshold is 35.363 ms.

The scoped service/SLO knee is confirmed only if every condition below holds:

1. All 10 cells pass accounting, source/runtime invariance, scheduler,
   topology, telemetry, identity, cgroup, and health attribution gates.
   Application drain, latency, and shedding are retained outcomes, not reasons
   to exclude a cell.
2. Rate 41 is clean in all five blocks: success at least 0.99; drain at most
   0.01; zero service errors, in-flight-limit drops, health violations, and
   restarts; p99 at most 35.363 ms.
3. Rate 42 is stressed in all five blocks: success below 0.99, drain above
   0.01, or service shedding/errors; and paired p99 ratio above 1.25.
4. All five paired directions agree: `deltaS < 0`, `deltaD > 0`,
   `latencyRatio > 1`, and `marginalUseful < 1`.
5. A paired whole-block bootstrap with 100,000 resamples, fixed seed
   `20260829`, and percentile 95% intervals on median effects has:
   upper `deltaS < 0`; lower `deltaD > 0`; lower `latencyRatio > 1.25`; and
   upper `marginalUseful < 1`.
6. Sample CV (`ddof=1`) is at most 2% for useful RPS at each rate, at most 10%
   for 41-RPS p99, and at most 20% for 42-RPS p99. No CV is computed for
   near-zero drains or errors.

If an SC-outcome condition is mixed, a confidence interval touches its
boundary, or a CV exceeds its limit, the result is inconclusive. If an external
attribution gate fails, the complete 10-cell study must be rerun with a new run
ID and disjoint sequence range; individual cells or favorable blocks may not
be selectively replaced.

`marginalUseful < 0.5` is intentionally not a confirmation requirement. That
is an arbitrary throughput-efficiency boundary, while this experiment's
primary claim is service acceptability. `marginalUseful < 1` is secondary
evidence that linear useful-throughput scaling has ended.

## Allowed interpretation

If all rules pass, report:

> The service/SLO knee is confirmed between 41 and 42 offered RPS per Pod for
> the unchanged W1/RT1, 64-token unique-miss, direct-Pod-IP shape over a
> 180-second horizon.

This confirmation does not independently establish an absolute throughput
ceiling. Repeated higher-side points are required for that claim. It also does
not establish same-Pod recovery, ClusterIP behavior, or 20-50 replica
horizontal scaling.
