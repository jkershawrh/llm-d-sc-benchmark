# Arena isolated cache-hit transport knee methodology — 2026-09-01

## Objective

Locate the response, health, and throughput/latency knees of the unchanged
five-replica `llm-d-sc` cache-hit path while separating classifier behavior from
driver co-location and ClusterIP routing.

## Test invariants

- All five classifier Pods run on `gnr2.fm2aihpcsed.com`.
- Every benchmark driver runs on `rhgnr1`.
- Driver and target nodes must be different and Ready.
- The target deployment must stabilize at exactly five active Ready Pods for
  three consecutive observations before traffic begins.
- Target Pod name, UID, Pod IP, and node must remain stable within every cell.
- Classifier image, driver image, payload, cache mode, connection count, and
  resource settings remain fixed across loaded rungs.
- ClusterIP and direct paths operate over the same five target identities.
- Direct routing is deterministic client-side fan-out to all five Pod IPs.
- Treatment order alternates by repetition.

## Workload

- cache mode: hit
- stable context: 256 bytes
- requests per loaded cell: 5,000,000
- loaded concurrency: 50, 125, 250, 500, 750, 1000, 1500
- total connections: 125
- replicas: 5
- repetitions: 3 per transport per rung
- transports: ClusterIP and direct Pod IP

The idle control uses three ClusterIP cells with one request, concurrency one,
one connection, and a 160-second post-request observation bracket.

## Cell sequence

1. Capture the five active target Pods, both node objects, and Kubernetes events.
2. Verify target identities against the campaign-start set and require Ready.
3. Launch the pinned driver image on the isolated driver node.
4. Require exact response accounting from the driver result.
5. Preserve explicit overload responses in the break lane.
6. Wait through the declared telemetry bracket.
7. Query target CPU, target throttling, driver CPU, gateway CPU if applicable,
   and per-Pod receive-byte counters over the job-plus-bracket range.
8. Capture target Pods, nodes, and Kubernetes events again.
9. Compute restart and warning-event deltas.
10. Hard-fail on target identity replacement or node-health loss.
11. Mark health-breaking cells as observed-break evidence and prohibit
    steady-state capacity claims.

## Gates

- **Accounting:** endpoint status counts must equal selected requests, and `OK`
  counts must equal successful requests.
- **Topology:** driver and target node placements must match the requested
  isolated topology.
- **Identity:** all five target names, UIDs, and Pod IPs must remain stable.
- **Node:** driver and target nodes must remain Ready.
- **Health:** zero restart delta and zero new readiness/liveness probe warnings.
- **Telemetry:** target CPU, driver CPU, and target throttling evidence must be
  present for every loaded cell.

Application or health breaks may be retained only when the campaign is
explicitly configured as a break lane. They cannot be relabeled as healthy
steady-state capacity.

## Decision rules

- The **zero-error boundary** is bracketed by the highest tested rung with zero
  response errors in every cell and the next tested rung with overload errors.
- The **health finding** reports the idle control separately, the first loaded
  rung with any health break, and the first rung with repeatable all-cell health
  failure. It does not invent an untested exact threshold.
- The **operational throughput/latency knee zone** begins where additional
  concurrency produces single-digit median useful-throughput gain while tail
  latency or overload cost increases materially, and is confirmed by a higher
  post-knee rung.
- ClusterIP is dominant only if direct routing materially moves or removes the
  same boundary. Near-parity across both paths rejects that hypothesis.
- CPU-limit saturation requires target or driver CPU near its limit or material
  throttling. Low observed CPU and zero throttling reject it as the immediate
  limiter, subject to scrape-resolution caveats.

## Independent audit

Run `hack/arena-sc-transport-knee-audit.py` over the raw campaign directories.
The audit reads raw cell results, health summaries, resource summaries, and
topology snapshots; checks exact status accounting; recomputes medians,
transitions, health totals, restarts, and placement; and records SHA-256 hashes.

The audit intentionally does not use campaign-summary values as its numeric
input.
