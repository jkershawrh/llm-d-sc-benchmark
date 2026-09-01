# Corrected five-replica cache-hit knee with external telemetry — 2026-09-01

## Verdict

The operational knee is between aggregate concurrency 250 and 500 for the
five-replica, exact-key cache-hit workload. At c500:

- both ClusterIP and direct-Pod transport begin returning explicit
  `RESOURCE_EXHAUSTED` responses;
- median successful-response p99 rises 97% and 92%, respectively, from c250;
- median useful throughput gains only 7% and 12%, respectively.

The matched break on both transports and the absence of infrastructure
exhaustion signals make the application admission/serve path the strongest
bottleneck location. The exact internal primitive is not directly observed in
the unchanged runtime image because it does not export queue/stage metrics.
Repository source maps `RESOURCE_EXHAUSTED` to bounded admission/queue
overload, so this is source-corroborated rather than runtime-proven attribution.

The previous 48.8k RPS five-replica ceiling claim is withdrawn. The corrected
audited curve reaches higher rates, including a maximum observed 88.4k useful
RPS at c1500. That is not promotable capacity: the c1500 cells have overload
responses and failed health gates.

## Evidence set

- 250,000,000 exact-accounted requests;
- 249,949,710 `OK` and 50,290 `RESOURCE_EXHAUSTED`; no other response status;
- 50 loaded cells across c50, c125, c250, c500, c750, c1000, and c1500;
- three repetitions per transport per rung, plus a fourth externally
  instrumented repetition at c50, c250, c500, and c1000;
- 45/50 loaded cells with health-SLO failures, 814 probe warning deltas, and
  three target restarts;
- identical pinned target and driver digests;
- all target Pods isolated to `gnr2.fm2aihpcsed.com`, with the driver on
  `rhgnr1`;
- 125 persistent HTTP/2 connections and five million requests per cell;
- treatment order counterbalanced between boundary rungs.

The independent audit is
`evidence/20260901-cache-hit-transport-knee-corrected-otel/independent-audit.json`.
It recomputes response accounting from raw `result.json`, validates topology
and target identity, aggregates repetitions by concurrency, and hashes the
result, health, resource, and available external telemetry summaries.

## Corrected curve

Median values are shown as ClusterIP / direct Pod IP.

| Concurrency | Cells | Useful RPS | p99 | Explicit overload | Health breaks |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 50 | 4 / 4 | 43.4k / 44.5k | 5.61 / 5.65 ms | 0 / 0 | 3/4 / 2/4 |
| 125 | 3 / 3 | 56.0k / 54.4k | 8.62 / 8.81 ms | 0 / 0 | 3/3 / 3/3 |
| 250 | 4 / 4 | 64.8k / 64.2k | 12.07 / 12.32 ms | 0 / 0 | 4/4 / 3/4 |
| 500 | 4 / 4 | 69.3k / 71.9k | 23.83 / 23.63 ms | 2,331 / 2,649 | 4/4 / 3/4 |
| 750 | 3 / 3 | 78.0k / 77.6k | 30.02 / 26.16 ms | 2,557 / 3,259 | 3/3 / 3/3 |
| 1000 | 4 / 4 | 81.5k / 80.0k | 33.13 / 47.87 ms | 7,749 / 7,680 | 4/4 / 4/4 |
| 1500 | 3 / 3 | 86.6k / 86.2k | 54.00 / 58.99 ms | 14,717 / 9,348 | 3/3 / 3/3 |

This is a knee, not a hard throughput plateau. Above c250, more concurrency
still buys some useful throughput, but tail latency, explicit overload, and
health behavior make it an unsafe operating region.

## External bottleneck attribution

The fourth boundary repetition added benchmark-owned OTel kubelet statistics
and node-exporter TCP/network signals without changing the classifier image.

- Target CPU: at most 2.03 aggregate cores using the conservative sum of each
  Pod's maximum, versus 20 cores of configured limits.
- Driver CPU: at most 2.31 of 8 cores.
- Target CPU throttling: zero in available cAdvisor samples.
- Target memory: below 678 MiB aggregate in the externally instrumented cells.
- Conntrack: at most 43.9% of the node limit.
- Target network errors: zero.
- Pod receive/transmit packet drops: zero.
- Node softnet drops: zero.
- Largest node-wide retransmit delta: 393 during a five-million-request cell.

These observations rule out CPU, driver saturation, conntrack exhaustion,
packet drops, softnet drops, and ClusterIP as the immediate c250→c500 break.
They do not identify the exact queue, lock, executor, or runtime task inside the
unchanged binary.

## Promotion and next proof

1. Export the exact runtime revision's admitted depth, queue capacity,
   admission rejections, queue wait, cache-hit, and total-stage histograms to
   the already working OTel collector.
2. Exercise queue bound, inference-worker count, and health-path configuration
   separately. A larger queue alone may trade rejection for worse tail latency.
3. Give health checks a path isolated from the saturated classification
   listener, then repeat c10/c25/c50 to establish a genuinely healthy floor.
4. Keep staging red until the selected operating point has zero overload
   responses, zero probe failures, stable identity, and clean recovery.
5. Treat 10/20/40/50 replicas as a separate multi-node/model-distribution
   campaign. This five-replica result cannot validate that scale.

## Claim boundary

This evidence applies to a closed-loop exact-key cache-hit workload, one pinned
classifier/model image, five replicas, two nodes, and the tested connection
shape. It does not cover cache misses, representative token distributions,
open-loop bursts, multi-zone routing, or 40–50 replica deployability.
