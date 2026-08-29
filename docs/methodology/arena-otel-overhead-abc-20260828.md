# Arena OTEL overhead matrix A/B/C — 2026-08-28

## Controlled method

- one classifier replica on `rhgnr1`
- one driver on `gnr2.fm2aihpcsed.com`
- 100,000 measured cache-hit requests per repetition
- concurrency 100, 100 persistent connections, 256-byte repeated context
- three repetitions per valid condition
- identical driver image digest and classifier resource envelope
- node readiness checked after every cell

Conditions:

- A: pinned upstream classifier; infrastructure collectors disabled
- B: pinned upstream classifier; two infrastructure collectors Ready
- C: code-instrumented candidate; metrics enabled; two collectors Ready

Every valid repetition returned 100,000/100,000 `OK`. Both workers remained
Ready and no target restarted.

## Valid results

| Condition | Useful RPS repetitions | Mean | Median | CV | Median p99 RTT |
| --- | --- | ---: | ---: | ---: | ---: |
| A | 42,040.7; 47,675.8; 31,837.5 | 40,518.0 | 42,040.7 | 16.18% | 5.770 ms |
| B | 46,784.6; 40,062.2; 42,713.2 | 43,186.7 | 42,713.2 | 6.40% | 6.745 ms |
| C | 39,113.3; 44,847.5; 34,310.8 | 39,423.8 | 39,113.3 | 10.93% | 6.931 ms |

Relative medians:

- B versus A: +1.60% useful RPS; this is not an improvement claim because it
  is far smaller than A's run variance
- C versus B: -8.43% useful RPS and +2.76% p99 RTT

## Interpretation

The collector-only effect is not distinguishable from run variance. The
application metrics candidate has an approximately 8% median throughput signal
in this cache-hit test, but three repetitions with 10.93% CV are insufficient
to call that the true instrumentation tax. More interleaved/randomized repeats
are required, followed by exact-token cache-miss testing where model-forward
work dominates.

This result reinforces that one-shot throughput values are not adequate knee
evidence. The framework records all repetitions and variability rather than
selecting the fastest run.

## Invalid first B/C attempt retained for audit

The first framework restoration used server-side apply after adding an
impossible node selector to disable the DaemonSet. Apply did not remove that
out-of-band selector, so collectors remained at 0/0 during the first B/C-labelled
runs. Those runs are excluded from the valid table. The framework now removes
the selector explicitly with JSON Patch and asserts `numberReady == 2` before B
or C.

The invalid runs remain as completed cluster Jobs named `otel-b-r1..r3` and
`otel-c-r1..r3`. Corrected jobs are named `otel-b-valid2-r1..r3` and
`otel-c-valid2-r1..r3`.

## Framework

`hack/arena-otel-overhead-matrix.sh` is the repeatable runner. It supports:

- `CONDITIONS` to select A, B, and/or C;
- `MATRIX_RUN_ID` to retain reruns without overwriting prior Jobs;
- configurable repetitions, request count, concurrency, connections, driver
  image, namespace, and kubeconfig;
- immutable driver image, explicit resource limits, fixed node placement,
  health checks, and cleanup restoration.

## Next gates

1. Increase randomized/interleaved A/B/C repetitions.
2. Add real OTLP RPC spans and run D at 1% and E at 10% sampling.
3. Repeat the exact-token cache-miss r1/r5/r10 ladder under the accepted
   telemetry condition.
4. Proceed to r15/r20 only if node health and telemetry completeness remain
   green.
