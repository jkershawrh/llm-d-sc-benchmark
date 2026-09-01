# Five-replica cache-hit transport knee — 2026-09-01

## Result

Two distinct boundaries were confirmed for the scoped five-replica cache-hit
workload:

- the zero-error/SLO boundary is **`(250, 500]` aggregate concurrency**;
- the throughput/latency knee is **`(500, 750]` aggregate concurrency**.

At concurrency 500, every one of six matched cells returned explicit
`RESOURCE_EXHAUSTED` responses. At concurrency 750, useful throughput gained
only 4.9% through ClusterIP and 4.0% through direct Pod routing versus
concurrency 500, while p99 grew 32.1% and 29.6%, respectively. This satisfies
the predeclared knee rule of less than 10% useful-throughput gain with more
than 25% p99 growth.

## Exact scope

- unchanged classifier image digest
  `sha256:04323612ce3f73873b4c3ed6e09264e828241537e2c1a4231b43f32e9744d5aa`
- five classifier replicas on Arena
- four application workers per replica; Rayon was not overridden
- stable 256-byte cache-hit context
- five million selected requests per cell
- 125 aggregate HTTP/2 connections
- aggregate concurrency 125, 250, 500, and 750
- ordinary ClusterIP versus deterministic direct Pod-IP routing
- three matched repetitions per rung, with treatment order reversed
- benchmark driver digest
  `sha256:5c7420b265163dfacd141e10be4ff297ea82d5675ad91db2323ae96a3e1fe452`

This is a different workload from the one-Pod W1/RT1 exact-64-token unique
cache-miss knee. The two results must not be combined into a universal SC
capacity number.

## Ladder

| Concurrency | ClusterIP useful RPS | ClusterIP p99 | ClusterIP success | Direct useful RPS | Direct p99 | Direct success |
|---:|---:|---:|---:|---:|---:|---:|
| 125 | 46,635.7 | 12.105 ms | 100% | 46,920.0 | 11.977 ms | 100% |
| 250 | 54,667.8 | 17.193 ms | 100% | 55,696.0 | 16.625 ms | 100% |
| 500 | 61,610.3 | 24.348 ms | 99.97334% | 62,954.8 | 24.170 ms | 99.95822% |
| 750 | 64,649.5 | 32.152 ms | 99.93966% | 65,459.1 | 31.329 ms | 99.91462% |

The 24 cells selected 120,000,000 requests. They produced 119,965,227 OK
responses and 34,773 `RESOURCE_EXHAUSTED` responses; there were no other
response statuses.

## Bottleneck attribution

ClusterIP was not the dominant ceiling in this scope. Its paired useful-RPS
ratio to direct routing stayed close to parity at every rung. At concurrency
750 the three paired ratios were 0.990, 0.976, and 1.019. ClusterIP endpoint
distribution was less even than deterministic direct routing, but bypassing
ClusterIP did not remove the knee or the explicit overload responses.

CPU was not the limiting resource:

- embedded driver CPU averaged approximately 1.2–1.6 cores;
- aggregate classifier CPU remained well below the five-Pod 20-core limit;
- classifier CPU throttling was zero in every cell;
- no target restart was required to complete the campaign.

The narrowest evidence-backed attribution is an SC/application-path
concurrency or admission boundary shared by both transports. The unchanged
image does not expose enough queue, cache-lock, executor, or gRPC-server
telemetry to distinguish those internal causes. A separately instrumented
candidate lane is required for code-level attribution; it must not replace
these unchanged-image capacity results.

## Gateway mitigation result

A single matched exploratory repetition through a benchmark-owned Istio gRPC
Gateway reached 15,578.6 useful RPS with 20.082 ms p99, versus 51,068.4 RPS
through ClusterIP and 51,799.8 RPS direct in the same repetition. The gateway
proxy averaged about 1.47 CPU cores. This is sufficient to reject that
single-proxy gateway layout as a no-tuning mitigation, but not to establish a
general Istio capacity limit.

## Integrity notes

- Raw captures are intentionally outside Git under `results/transport/`.
- Compact medians, deltas, run IDs, artifact digests, and raw-summary hashes
  are retained in `evidence/20260901-cache-hit-transport-knee/`.
- An earlier concurrency-500 strict run stopped after its first overload cell;
  it is excluded from the medians above.
- An earlier concurrency-250 run used the pre-cgroup-telemetry driver and is
  also excluded from the final ladder.
- Per-Pod receive-byte shares are a fixed-payload distribution proxy, not an
  application request counter.
