# Arena cache-hit transport knee method — 2026-09-01

## Question

Determine whether the observed five-replica cache-hit ceiling belongs to
Kubernetes ClusterIP transport, classifier CPU, the load generator, or a
shared SC/application path, while leaving classifier code unchanged.

## Treatments and controls

Each rung compared two treatments against the same five ready classifier Pods:

1. 125 connections to the ordinary ClusterIP;
2. 25 connections to each of five explicit Pod IPs, for 125 total.

The direct treatment is the endpoint-allocation oracle. The driver assigns
requests deterministically across explicit targets and measures all targets
under one elapsed timer. Both treatments use the same total request count,
connections, concurrency, stable cache key, context size, driver resources,
classifier image, model volume, and nodes.

Each cell prewarms the stable key on all five targets. Rungs use 125, 250, 500,
and 750 aggregate concurrency and five million selected requests per cell.
Every rung has three repetitions. Treatment order rotates to prevent time
drift from consistently favoring one path.

## Measurement independence

The benchmark driver compiles only the public classifier gRPC contract. It
does not import Fleet or classifier implementation code. The final driver
reports cgroup-v2 CPU usage over the exact timed plateau. OpenShift monitoring
independently supplies:

- per-Pod receive-byte counter deltas for endpoint distribution;
- classifier CPU rates and throttling;
- gateway CPU where applicable;
- Pod health and events.

Receive-byte share is valid only as a distribution proxy because request and
response payloads are fixed within a cell. It is not treated as an exact
application request count.

## Validity and failure semantics

A measurement cell is valid when:

- the driver emits the expected result schema;
- selected requests are positive;
- the sum of all response-status counts equals selected requests;
- network distribution telemetry covers all five target Pods;
- resource telemetry is captured;
- the workload remains the declared cache-hit treatment.

Zero non-OK responses are an SLO gate, not an evidence-validity requirement in
explicit break mode. `RESOURCE_EXHAUSTED` is therefore retained as measured
application behavior. Driver failure, malformed accounting, missing required
telemetry, target Pod replacement, or node health failure remains fail-closed.
The corrected runner captures target Pods and events before the cell and again
after the complete telemetry bracket. Probe-warning or restart deltas fail the
strict lane. An explicit break lane may retain them as application
health-break evidence, but the resulting cell is not eligible for a healthy
steady-state capacity claim.

The original 2026-09-01 campaign captured its target health snapshot before
the post-cell metric bracket. A later audit found that this timing allowed one
restart to escape the gate. The historical numeric results are preserved, but
the corrected runner semantics above apply to all new qualification runs.

## Decision rules

- Zero-error/SLO boundary: the first tested rung where every matched treatment
  begins returning non-OK responses after the preceding rung was clean.
- Throughput/latency knee: the first tested step with less than 10% median
  useful-throughput gain and more than 25% median p99 growth.
- ClusterIP attribution: requires a repeatable, material separation from the
  direct oracle under matched aggregate connections and concurrency.
- CPU attribution: requires saturation or throttling consistent with the
  observed transition. Low CPU and zero throttling rule out CPU capacity as
  the immediate limiter but do not identify an internal lock or queue.

## Reproduction

The runner is `hack/arena-sc-transport-matrix.sh`. Pin both image digests and
use a unique run ID. The final four rung IDs were:

- `transport-r5-c125-0901a`
- `transport-r5-c250-0901b`
- `transport-r5-c500-0901b`
- `transport-r5-c750-0901a`

The strict default stops on any non-OK response. Set
`FAIL_ON_RESPONSE_ERRORS=false` only for an explicitly declared break lane.
The campaign summary distinguishes measurement validity, telemetry
completeness, overload cells, success rate, and response status counts.
