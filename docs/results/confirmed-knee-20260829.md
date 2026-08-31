# Confirmed llm-d-sc service/SLO knee — 2026-08-29

## Result

The confirmed scoped knee is **`(41, 42]` offered RPS per Pod**.

This is a service/SLO completion-window knee, not a hard-error boundary or an
absolute throughput ceiling. At 42 RPS, every request eventually returned OK,
but more than 1% completed after the 180-second plateau and conditional
successful-within-window p99 latency rose from milliseconds to seconds.

## Exact scope

- unchanged classifier image
- one application worker (`W1`)
- `RAYON_NUM_THREADS=1` (`RT1`)
- exact 64-token unique cache misses
- one target Pod
- direct Pod IP, bypassing ClusterIP
- one HTTP/2 connection
- deterministic open-loop scheduling
- 180-second plateau
- ten paired randomized blocks, twenty cells total

Changing any of these dimensions creates a different capacity claim.

## Evidence

Across five original blocks and a five-block extension whose protocol and
analysis were frozen before extension load:

- 41 RPS was clean in 10/10 blocks.
- 42 RPS was stressed in 10/10 blocks.
- all ten paired directions agreed.
- p99 at 41 RPS ranged from 29.431 to 34.512 ms.
- p99 at 42 RPS ranged from 1,917.492 to 3,251.213 ms.
- the median paired p99 ratio was 67.011x.
- the median marginal useful throughput from the 42nd offered request/second
  was 0.531 RPS.
- all preregistered numeric and attribution conditions passed.

The whole-paired-block bootstrap used 100,000 resamples with seed 20260829.
Its 95% interval for the latency ratio was 63.728x to 78.932x; its interval
for marginal useful RPS was 0.450 to 0.550.

## Interpretation boundary

The evidence supports a queueing/completion-window transition between 41 and
42 offered RPS per Pod for this workload. It does not establish:

- a universal per-Pod limit for other worker or Rayon settings
- a ClusterIP or network-transport ceiling
- a repeatable horizontal-replica knee
- r40/r50 application capacity
- same-Pod overload recovery behavior
- a code-level causal bottleneck by itself

The evidence rules out neither CPU-side scheduler/runtime overhead nor
application queueing. Separate same-shape observations strongly implicate
default Rayon parallelism: an RT1 configuration materially outperformed the
unset configuration without changing the image. Admission limits govern tail
latency and shedding; they should not be described as a throughput fix.

## Horizontal status

No repeatable horizontal knee is established. The fixed-cross-node W1/RT1
exploratory lane remained above 89% r1-relative efficiency through r15. The
r20 observation also appeared near-linear, but a malformed CPU placement and
the absence of repetition prevent a promotion-quality r20 claim. Earlier r20
results also reused corpus ranges and therefore cannot establish unique-miss
capacity. r40/r50 attempts are health and startup observations, not capacity
measurements.

## Integrity notes

- This is internal same-cluster replication, not external validation.
- One Stage A 42-RPS cell had a liveness timeout before the plateau; it caused
  no restart and no plateau or post-plateau health event.
- Topology reports warned that node housekeeping CPUs split SMT; the measured
  target cpuset was unaffected.
- Driver runtime image ID and node were not persisted per cell. Pinned config,
  driver self-report, archived build evidence, and target identity are present.
- Model and tokenizer identities were supplied hashes, not rehashed from the
  PVC in every cell.

See `evidence/20260829-confirmed-knee/` for the compact decision, audit,
provenance, summaries, and checksum manifest. Full raw cell artifacts remain
in the controlled local evidence archive and are intentionally not stored in
Git.
