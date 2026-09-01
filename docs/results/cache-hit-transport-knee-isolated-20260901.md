# Isolated five-replica cache-hit knee and bottleneck — 2026-09-01

> Superseded for knee and ceiling conclusions by
> `cache-hit-transport-knee-corrected-otel-20260901.md`, which adds the focused
> OTel boundary repetition and aggregates all valid repetitions. Raw evidence
> and historical methodology remain valid.

## Executive result

The corrected Arena campaign found three distinct boundaries for the unchanged
five-replica `llm-d-sc` cache-hit workload:

1. **Health-probe degradation is visible by concurrency 50.** The idle control
   was clean in all three 160-second windows. At concurrency 50, four of six
   loaded cells recorded one probe timeout each. At concurrency 125, all six
   cells failed the health gate, with 148 probe timeouts and two container
   restarts across the matched pair set.
2. **The zero-error response boundary is `(250, 500]`.** Every cell at 50, 125,
   and 250 returned only `OK`. Every cell at 500, 750, 1000, and 1500 returned
   explicit `RESOURCE_EXHAUSTED` responses.
3. **The operational throughput/latency knee is the 750–1000 region.** From 750
   to 1000, median useful throughput gained only 4.0% through ClusterIP and
   3.0% direct, while direct p99 grew 99.2%. The 1500 confirmation rung added
   only 6.8–7.8% more throughput over 1000 while ClusterIP p99 grew 56.0% and
   overload responses increased sharply.

These are observed-break results, not healthy steady-state capacity claims.
Only the idle control was fully health-clean. The loaded ladder is suitable for
locating failure modes and knees, not for certifying a production SLO.

## What changed from the earlier report

The earlier ladder co-located the driver with some targets and sampled target
health before the post-cell telemetry bracket. Its numeric response results
were accurate, but the health gate missed later probe failures and one restart.

The corrected campaign:

- placed all five classifier targets on `gnr2.fm2aihpcsed.com`;
- placed every benchmark driver on `rhgnr1`;
- required three consecutive observations of exactly five active Ready targets;
- captured Pod identity, Ready state, restart counts, node state, and Kubernetes
  warning events before and after every cell;
- moved the after-health snapshot to the end of the telemetry bracket;
- hard-failed on Pod replacement or node-health loss;
- retained application and health breaks as evidence while preventing them from
  being labeled steady-state capacity.

The original report remains historical evidence. This isolated report is the
current conclusion.

## Corrected ladder

Each loaded point is the median of three five-million-request cells per
transport. There were 125 total HTTP/2 connections at every loaded rung.

| Concurrency | ClusterIP useful RPS | ClusterIP p99 | ClusterIP errors | Direct useful RPS | Direct p99 | Direct errors | Health-break cells |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 50 | 43,045.2 | 5.641 ms | 0 | 44,643.6 | 5.667 ms | 0 | 4 / 6 |
| 125 | 55,950.6 | 8.620 ms | 0 | 54,406.9 | 8.811 ms | 0 | 6 / 6 |
| 250 | 64,616.9 | 12.190 ms | 0 | 63,892.1 | 12.319 ms | 0 | 5 / 6 |
| 500 | 69,696.8 | 26.801 ms | 1,878 | 72,468.0 | 25.407 ms | 2,467 | 5 / 6 |
| 750 | 77,956.3 | 30.020 ms | 2,557 | 77,586.9 | 26.159 ms | 3,259 | 6 / 6 |
| 1000 | 81,078.0 | 34.611 ms | 5,788 | 79,925.1 | 52.103 ms | 6,918 | 6 / 6 |
| 1500 | 86,614.0 | 53.995 ms | 14,717 | 86,167.5 | 58.989 ms | 9,348 | 6 / 6 |

The loaded ladder selected exactly 210,000,000 requests. Independent raw-file
recomputation found 209,953,068 `OK`, 46,932 `RESOURCE_EXHAUSTED`, and no other
response status.

## Health evidence

The three near-idle control windows each issued one RPC and then observed the
targets for 160 seconds. All three controls had zero warning events, zero
restarts, stable Pod identity, and Ready targets before and after.

Across the 42 loaded cells:

- 38 cells failed the health SLO;
- 722 new probe timeout events were recorded;
- 638 were readiness timeouts and 84 were liveness timeouts;
- three container restarts occurred: two at concurrency 125 and one at 1000;
- no target Pod identity changed during a cell;
- both nodes stayed Ready.

The clean idle control and load-correlated increase make a purely idle node
baseline unlikely. The exact first concurrency that produces any probe miss is
not resolved below 50; the defensible statement is **intermittent degradation by
50 and repeatable degradation by 125** for this sustained workload.

## Bottleneck attribution

### ClusterIP is not the dominant ceiling

The ratio of ClusterIP median useful RPS to direct median useful RPS ranged from
0.962 to 1.028 across all loaded rungs. Direct client-side fan-out used all five
Pod IPs with the same total connection count. Both paths reproduced probe
timeouts, `RESOURCE_EXHAUSTED`, the zero-error boundary, and the post-knee
latency growth. Removing ClusterIP did not remove the break.

### CPU is not the immediate limiter

The highest observed aggregate target CPU was 1.65 cores against a 20-core
five-Pod limit. The highest driver average was 2.31 cores against its 8-core
limit. Target CPU throttling was zero in every cell. Prometheus samples are
coarse and some cells have sparse post-bracket coverage, so these values should
not be treated as precise CPU profiles; they are sufficient to reject CPU-limit
saturation as the immediate cause.

### Narrowest supported cause

The break is in a shared SC-facing service path rather than ClusterIP alone.
The evidence is consistent with admission/concurrency pressure, gRPC/TCP accept
or backlog pressure, worker/executor starvation, or health-probe starvation.
The unchanged image does not expose queue depth, admission occupancy, executor
wait, cache-lock timing, or gRPC server internals, so this campaign cannot name
the exact function, lock, or queue.

## What this says about 40–50 replicas

This experiment intentionally held the classifier at five replicas to identify
the per-shape break. It does not establish that 40 or 50 replicas will scale
linearly. The next replica-scale campaign should use multiple target nodes and
multiple independent drivers, preserve the health/event gate, and test whether
useful RPS grows proportionally while p99, endpoint balance, probe health, and
error rate remain bounded.

## Exact scope

- cluster: Arena
- classifier: unchanged image digest
  `sha256:04323612ce3f73873b4c3ed6e09264e828241537e2c1a4231b43f32e9744d5aa`
- benchmark driver digest
  `sha256:5c7420b265163dfacd141e10be4ff297ea82d5675ad91db2323ae96a3e1fe452`
- five classifier replicas, four application workers per replica, Rayon unset
- stable 256-byte cache-hit context
- closed-loop concurrency; five million selected requests per loaded cell
- 125 aggregate HTTP/2 connections
- ClusterIP versus deterministic client-side fan-out over all five Pod IPs
- three matched repetitions with reversed treatment order
- loaded concurrency: 50, 125, 250, 500, 750, 1000, and 1500
- independent idle control: three 160-second windows with one RPC per window

## Integrity

`evidence/20260901-cache-hit-transport-knee-isolated/independent-audit.json`
was recomputed from raw `result.json`, `health-summary.json`,
`resource-summary.json`, and topology snapshots. It does not consume the
generated `transport-summary.json` values. Per-cell hashes and campaign-summary
hashes are included in the audit file.
