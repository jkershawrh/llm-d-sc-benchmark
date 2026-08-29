# Arena OTLP trace overhead C/D/E — 2026-08-28

## Candidate identity and method

- OTLP-capable image digest:
  `sha256:35d3dac18221f7080848fa3999651198b13d49455eee092cc7935c0ad5d66425`
- one candidate replica on `rhgnr1`; driver on `gnr2.fm2aihpcsed.com`
- two OTEL collectors Ready throughout
- 100,000 measured cache-hit requests per repetition
- concurrency 100, 100 persistent connections, 256-byte repeated context
- three repetitions per condition

Conditions use the same binary and differ only by trace sample ratio:

- C0: 0% (OTLP code present, provider/exporter disabled)
- D: 1% trace-id-ratio sampling
- E: 10% trace-id-ratio sampling

Every repetition returned 100,000/100,000 `OK`. Both workers remained Ready and
the target did not restart during any measured load cell.

## Results

| Condition | Useful RPS repetitions | Mean | Median | CV | Median p99 RTT |
| --- | --- | ---: | ---: | ---: | ---: |
| C0, 0% | 47,249.3; 39,053.6; 38,759.6 | 41,687.5 | 39,053.6 | 9.44% | 6.204 ms |
| D, 1% | 40,214.9; 34,347.1; 37,885.2 | 37,482.4 | 37,885.2 | 6.44% | 6.635 ms |
| E, 10% | 37,636.2; 32,826.4; 35,793.7 | 35,418.8 | 35,793.7 | 5.59% | 6.638 ms |

Relative medians:

- D versus C0: -2.99% useful RPS; +6.95% p99 RTT
- E versus C0: -8.35% useful RPS; +7.00% p99 RTT
- E versus D: -5.52% useful RPS; p99 essentially unchanged

These are cache-hit transport-path overhead measurements, not exact-token model
capacity results. Three repetitions establish a useful signal but not a narrow
confidence interval.

## Trace completeness

A 1% smoke sent 1,000 measured requests and produced nine received spans.
Across the full matrix, collector self-metrics reported:

- 3,069 spans accepted and sent on the 1% collector path (includes the nine-span
  smoke); zero refused
- 29,920 spans accepted and sent on the 10% collector path; zero refused

The counts are consistent with probabilistic sampling of roughly 300,000 RPCs
per condition. Candidate and collector logs contained no exporter, receiver, or
dropped-span errors.

## Attribute/privacy boundary

Each server span contains only fixed/bounded attributes:

- `rpc.system=grpc`
- fixed service and method names
- classifier signal name
- bounded gRPC/result status

No prompt/context, session, request ID, caller-controlled hash, or model text is
attached. Application latency metrics retain their separate bounded-label
Prometheus surface.

## Operational gremlin: termination

Changing the trace ratio recreates the single candidate pod. The old server pod
remained running after kubelet issued `Stopping container` and consumed the full
30-second termination grace before replacement. The binary blocks indefinitely
on a channel and has no graceful SIGTERM path. This does not change steady-state
throughput, but it directly slows rollouts and scale transitions and can make a
multi-replica deployment retain terminating pods longer than expected.

Mitigation should be evaluated in the candidate branch:

1. handle SIGTERM;
2. stop accepting new RPCs and fail readiness;
3. drain admitted work within a bounded deadline;
4. force-flush/shut down the OTEL provider;
5. exit before `terminationGracePeriodSeconds`.

## Decision for the scaling ladder

Use 1% sampling for the exact-token r1/r5/r10 diagnostic ladder. It yielded
complete trace delivery with a materially smaller median throughput signal than
10%. Keep the upstream/no-application-telemetry run as the capacity baseline;
use the 1% candidate to explain queue/forward/transport behavior, not to replace
the upstream number.
