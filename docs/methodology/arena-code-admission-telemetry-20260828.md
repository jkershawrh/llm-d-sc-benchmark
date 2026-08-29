# Arena code admission telemetry — 2026-08-28

## Candidate

- image digest:
  `sha256:afb2bc2838f41e64d54876830cddb690ccbd699cfd51f36e69f799d25e244574`
- one candidate replica on `rhgnr1`
- queue/admission bound: 256
- inference workers: 4
- trace sampling: 0% for this pressure cell

The candidate adds atomic hot-path telemetry for current/max admitted work,
admission rejection count, queue capacity, and configured workers. It also adds
SIGTERM/SIGINT handling so the server drops its runtime and flushes OTEL before
exit.

## Graceful termination validation

A rollout of the new candidate deleted the old candidate pod in 2 seconds and
had the replacement Ready in 16 seconds. The previous binary consumed the full
30-second termination grace. This validates the candidate mitigation for the
previously observed rollout gremlin.

## Driver-boundary attempt

A single corpus driver at concurrency 300 failed locally with `EMFILE` (`Too
many open files`) before opening workload channels to the classifier. Server
metrics remained at zero. This is retained as a harness boundary and is not a
classifier result.

## Valid admission-pressure method

- four parallel Indexed Job completions on `gnr2.fm2aihpcsed.com`
- each driver: concurrency 100, 200 globally disjoint exact-64-token contexts
- aggregate offered requests: 800 cache misses
- candidate on `rhgnr1`
- corpus offsets: 0, 200, 400, 600

## Result

Aggregate outcomes:

- 512 `OK`
- 288 `GRPC_RESOURCEEXHAUSTED`
- server cache misses: 512
- maximum admitted: 256
- admission rejections: 288
- current admitted after completion: 0
- queue capacity: 256
- configured workers: 4
- queue p99/max: 25.166/26.097 seconds
- total p99/max: 25.166/26.458 seconds

The four driver outcomes reconciled exactly with server telemetry. Maximum
admitted equalled, but never exceeded, the configured bound. Every admitted
request completed successfully and every excess request was rejected explicitly
rather than silently buffered.

Both workers remained Ready. This is a code-level admission/latency knee: at 400
simultaneously offered exact-token requests against one four-worker replica, the
256-request admission envelope fills, tail latency rises to roughly 26 seconds,
and the remainder is shed. CPU alone cannot describe this failure mode; queue
policy and worker width are first-order controls.

## Reusable artifact

`deploy/arena-otel-admission-pressure.yaml` encodes the four-driver Indexed Job
without exceeding the per-process file-descriptor ceiling.
