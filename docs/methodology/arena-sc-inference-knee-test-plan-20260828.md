# Arena semantic-classifier inference-knee test plan — 2026-08-28

Status: proposed protocol; no cluster actions authorized or performed by this
document.

## Objective

Measure cache-miss inference capacity and the horizontal-scaling knee of the
unchanged upstream semantic classifier. Separate classifier limits from client,
connection, Service-dataplane, startup, probe, node, and corpus-harness limits.
The result must be repeatable and mergeable across endpoints; a successful RPC
count alone is insufficient.

## Fixed provenance

Every run envelope must record:

- target and driver image digests, git SHA, model SHA-256, tokenizer SHA-256,
  taxonomy revision, classifier ID, node names, kernel/Kubernetes versions;
- corpus generator version, corpus digest, exact token bucket, seed and row
  range assigned to every endpoint;
- Deployment resources, inference-worker count, queue bound, probe definition,
  replica count, Pod UID/IP/node/imageID and EndpointSlice snapshot;
- driver Pod UID/node/resources, target endpoint, channel count, RPC concurrency,
  scheduled barrier time, actual first/last request time and monotonic duration.

Any missing or mismatched target/model/tokenizer/corpus provenance invalidates
the cell.

## Workload

Use one representative bucket of **64 tokens including special tokens** for the
primary ladder. Generate contexts with the deployed tokenizer and verify every
row after construction. Rows must be unique across the entire experiment, not
merely within a driver. Assign immutable half-open global ranges to cells and
fail before connecting if a range crosses a corpus shard boundary.

The corpus must be large enough for the maximum fixed-duration rate plus 25%
headroom. Do not wrap, reuse, or synthesize a suffix after token verification.
Before load, sample at least 100 rows per shard and verify token count, uniqueness
and tokenizer digest. After load, assert consumed row count equals attempted RPCs.

Primary traffic is unique cache misses. Run a separate exact-key cache-hit
control at the beginning and end to detect transport/client regressions, but do
not combine cache-hit capacity with inference results.

## Topology

Use direct endpoint routing. Each target Pod receives exactly one logical load
shard; no ClusterIP is in the measured data path. Drivers must run on designated
load-generator nodes that host no classifier Pods. Pin driver and target CPU
requests and placement for the complete experiment.

Prefer one long-lived driver process per load node controlling multiple endpoint
shards. If multiple driver Pods are necessary, create all images and Pods before
the run and require them Ready before scheduling the barrier. Do not permit image
pulls, model startup, autoscaling, rescheduling or rolling updates in a measured
window.

Use a fixed number of persistent HTTP/2 channels per endpoint. Establish every
channel and complete a health-only RPC before declaring the driver armed. RPC
concurrency is independent of connection count. The primary ladder uses four
in-flight RPC workers per target over four persistent channels; a separate
connection-sensitivity control uses one and four channels at the same RPC
concurrency.

## Synchronized fixed-duration cell

Each cell has four phases:

1. **Quiescence (60 s):** targets Ready, nodes Ready, no terminating Pods,
   resource metrics available, no new probe failures, CPU below 10% of target
   limit and queue empty.
2. **Pre-connect/warm control (30 s):** establish channels, verify endpoint
   identity, run non-measured health/control RPCs, then wait idle. Do not warm
   primary miss contexts.
3. **Ramp and plateau:** ramp offered concurrency over 15 s, then hold a
   180-second measured plateau. All drivers use the same future UTC start plus
   a monotonic local timer. A two-phase coordinator records `ARMED` from every
   endpoint before releasing the barrier. A driver more than 100 ms late
   invalidates the cell.
4. **Recovery (120 s):** stop issuing load, preserve channels, send one unique
   miss and one cache hit per endpoint at 5, 30, 60 and 120 seconds, and capture
   readiness, restart and queue recovery.

The driver must use a closed-loop concurrency test for latency/capacity and a
separate open-loop offered-rate test for overload/shedding. Never infer offered
capacity from a closed-loop run alone.

## Latency and result artifacts

Record every RPC's endpoint ID, monotonic start offset, latency in microseconds,
status code and corpus sequence to a compressed artifact, or record an
HDRHistogram with identical bounds and three significant digits plus separate
status/time-series counters. Histograms must be addable without percentile
averaging. Use a range of 1 microsecond through at least 120 seconds and report
overflow explicitly.

Produce per-endpoint and globally merged p50/p95/p99/p99.9/max from successful
RPC samples. Status failures are not latency samples. Report attempted, OK,
useful RPS, offered RPS, each gRPC status, connect errors, invalid responses and
missing corpus rows separately. Preserve raw/histogram artifacts; JSON summaries
alone are not acceptable evidence.

Common-wall useful throughput is total successful plateau RPCs divided by the
intersection of the synchronized plateau window. Do not sum independently
measured endpoint rates unless their windows are proven identical.

## Server and infrastructure observability

Scrape at 1-second resolution, labeled by Pod UID:

- request totals/status, cache hits/misses, tokenize/forward/end-to-end latency;
- admission current/max, queue depth, queue wait, rejected requests, active
  inference workers and accepted connections;
- process CPU, RSS, threads, file descriptors and TCP sockets;
- container CPU usage/throttling, memory working set/OOM, network bytes/drops;
- node CPU/PSI, memory/PSI, load, conntrack usage/drops, softnet drops, disk/NFS
  latency, kubelet probe results and node conditions;
- EndpointSlice membership, Pod readiness, restarts and termination events.

The upstream server currently does not expose enough per-Pod accepted-channel,
queue and stage metrics for complete attribution. Until those are available,
collect equivalent external/kernel observations and label the causal conclusion
as bounded rather than modifying the production target for a benchmark.

Clock skew between drivers and collectors must be under 50 ms. Missing metrics
for more than 5 consecutive seconds invalidates capacity attribution, although
the health failure remains reportable.

## Ladder and repetitions

Establish a topology-matched r1 control, then r5, r10 and r20. Start replicas in
small batches (at most two through r5 and five thereafter), waiting for 60
seconds of node/Pod stability between batches. Do not include startup in the
measured window.

At each replica rung, run offered concurrency per target of 1, 2, 4, 8 and 16,
stopping higher concurrency after the first confirmed per-rung knee. Randomize
cell order after the baseline and repeat every candidate knee-adjacent cell at
least five times. Alternate two independent corpus shards to avoid order and
thermal bias. Report medians, bootstrap 95% confidence intervals and sample CV
for useful RPS and merged p99.

## Predeclared knee rules

For a fixed replica count, the concurrency knee is the first level where either:

- median useful RPS is at least 10% below the best lower-concurrency median; or
- useful RPS gains no more than 10% over the preceding level while merged p99
  increases at least 25%; or
- non-OK results, failed recovery or target restarts occur.

The usable ceiling is the preceding concurrency level. Confirm a knee only when
at least four of five paired repetitions have the same RPS/p99 direction and
their 95% confidence intervals do not make both thresholds indeterminate.

Horizontal efficiency at rung `N` is:

`median useful RPS(N) / (N × median per-Pod useful RPS of the topology-matched baseline)`.

Use the same per-target concurrency and direct-routing method. Green is at least
80%, yellow 60–80%, and red below 60%. The horizontal knee is the first rung
below 80% or with a health RED. Do not compare gnr2 results with a rhgnr1
baseline or use cache-hit RPS as the baseline.

## Health and validity gates

- **GREEN:** all expected endpoints present/Ready, zero unexpected statuses,
  zero restarts/probe failures, complete metrics/raw latency and recovery within
  30 seconds.
- **YELLOW:** no request/restart failure, but repeat CV exceeds 10% for RPS or
  15% for p99, recovery takes 30–120 seconds, or metrics have a declared gap
  shorter than 5 seconds. Repeat; do not promote.
- **RED:** node NotReady/control loss, endpoint loss, unexpected status/connect
  error, liveness/readiness failure during plateau, restart, recovery failure,
  queue growth continuing after load, missing raw data, corpus overlap/boundary
  error, or horizontal efficiency below 60%.

Node/control instability stops the experiment and triggers safe scale-down.
Application probe failures may be retained as outcomes only in an explicitly
observational run; they invalidate a capacity/promotion result.

## Required report

Publish the complete matrix, repetition distributions, merged histograms/raw
artifact links, per-endpoint CV and node split, scaling efficiency, knee decision,
health/recovery timeline and excluded/invalid cells. Every causal statement must
identify which competing bottlenecks were measured and which remain unobserved.
