# llm-d-sc same-Pod overload recovery preregistration

Status: frozen before the first classifier-directed recovery load on 2026-08-29.

## Question and scope

Measure whether one unchanged `llm-d-sc` Pod returns to its pre-overload service
level after a bounded overload, and how long that recovery takes. This is a
direct-Pod-IP W1/RT1 test of the same Pod UID and IP throughout one cycle. It
does not test ClusterIP routing, a replacement Pod, default W4 behavior, or
horizontal scale-out.

The classifier image and core source are unchanged. Only the external load
driver and benchmark harness contain new measurement code.

## Immutable identities

- Target image: `sha256:04323612ce3f73873b4c3ed6e09264e828241537e2c1a4231b43f32e9744d5aa`
- Driver image: `image-registry.openshift-image-registry.svc:5000/llm-d-sc-gremlins/llm-d-sc-benchmark-driver-armed-51541f00e5fa@sha256:ef0f32ad3a7a29f4cd1f68ae8b8cfbc1bf36d66a173df8f68fd531db9d762aae`
- Driver source SHA-256: `51541f00e5fa6e1918b4e57b9bfa432337345b1854b7289c836c3752543929d9`
- Model identity: `7914abbd152278879b4c3235d188e3006753bb778b7de6266fbcbe4c4ba2ef2f`
- Tokenizer identity: `851ca67100d372ca3ae031a6abd168f53489eebfd7d89523f35c5c9b4d372c3c`
- Driver-build artifact manifest SHA-256: `26ebe7ba2dbc88480d42a5b598e05c1b33337c2144c3d45c49858d6389f7a5ea`
- Recovery orchestrator SHA-256: `deacce4bb453892fc6faf2f3443c0452d9d734dc7670b96ed37926d47a96f36a`
- Recovery summarizer SHA-256: `ce3466bb5dc81521f60eaee4c0ecbdf350fb45163e366c24be588a825defc003`
- Focused test SHA-256: `156b1437a64c54c8b55703181744f59291598357fb6c4083cfef8c170f17af00`

The ARMED driver build is attested to exact inputs and runtime-smoke-tested by
digest. It is not claimed bit-reproducible because its base images and Debian
package indexes are not digest/version pinned.

## Frozen target and topology

- Namespace/deployment: `llm-d-sc-scaleout/classifier-target`
- Target/driver nodes: `gnr2.fm2aihpcsed.com` / `rhgnr1`
- One fresh target Pod per cycle; the same UID and direct Pod IP must remain
  bound for every phase of that cycle.
- `LLM_D_SC_INFERENCE_WORKERS=1`, `RAYON_NUM_THREADS=1`,
  `CANDLE_NUM_THREADS` absent, metrics log interval 10 seconds.
- Guaranteed requests=limits `2 CPU / 4Gi`; runtime `cpu.max=max`; exactly two
  logical CPUs forming a complete SMT sibling set.
- One HTTP/2 connection, deterministic open-loop offered rate, 64-token exact
  unique misses, zero warm-up requests.
- A fresh, traffic-clean Pod is mandatory because internal queue histograms
  and service counters are cumulative.

## Frozen cycle

Every cycle precreates 14 suspended Jobs. All 14 must emit a matching
application-level `sustained-corpus-probe-armed-v1` record by `T0-90s`; no
traffic is released unless every explicit configuration field matches.

1. Pre steady state: 35 RPS for 180 seconds.
2. No-arrival gap: 5 seconds.
3. Overload: 47 RPS for 120 seconds.
4. One-request recovery probes at +0, +1, +2, +3, +5, +8, +13, +21, +34,
   +55, and +89 seconds after overload.
5. Post gap: 5 seconds.
6. Post steady state: 35 RPS for 180 seconds.

Each Job owns a disjoint 10,001-sequence span within a 150,000-sequence cycle
reservation: `C_r = 19000000000 + 150000*r`.

| Purpose | Run ID | Cycle index | Reserved range |
|---|---|---:|---|
| Harness/live smoke | `recovery-smoke-20260829-a` | 0 | `[19000000000, 19000150000)` |
| Confirmation 1 | `recovery-confirm-r1-20260829-a` | 1 | `[19000150000, 19000300000)` |
| Confirmation 2 | `recovery-confirm-r2-20260829-a` | 2 | `[19000300000, 19000450000)` |
| Confirmation 3 | `recovery-confirm-r3-20260829-a` | 3 | `[19000450000, 19000600000)` |

The smoke validates live orchestration but is excluded from the three-cycle
repeatability decision. No early stop or selective favorable-cycle replacement
is allowed.

## Per-cycle validity and outcome gates

The cycle is valid only if all accounting, scheduling, ARMED, exact config,
same-Pod identity, image, PID1 environment, cpuset/SMT, cgroup, restart,
readiness, health-event, topology, OTEL completeness, queue-counter, and
target/driver reconciliation gates pass.

- Scheduler p99 dispatch lag at most 5 ms; zero schedule and in-flight drops.
- Pre and post success at least 99.9%; drain at most 0.1%.
- Post/pre useful throughput within 2%.
- Post/pre p50 at most 1.10x; post/pre p99 at most 1.20x.
- Overload queue growth strictly above 10x, plus drain strictly above 1% or
  `RESOURCE_EXHAUSTED`.
- Target reconciliation tolerance is zero: initiated equals completed equals
  `OK + RESOURCE_EXHAUSTED`; target served/misses equal OK; hits equal zero.
- Recovery RTT budget is `max(2 * pre p99, 50 ms)`.

Recovery time is anchored only by a passing sparse probe. That probe, its next
two chronological recovery observations, and every later sparse probe must
pass. The post stream may confirm a sparse probe conservatively, but it cannot
create or move recovery earlier.

- Green: recovery time at or before 34 seconds.
- Amber: recovery time after 34 and at or before 55 seconds.
- Red: recovery after 55 seconds, no stable recovery, or failed pre/post
  equivalence. A valid red/amber outcome is a measurement, not invalid data.

## Three-cycle decision

All three confirmation cycles must be valid. Overall recovery is green only if
all three are green; amber if none is red and at least one is amber; red if any
valid cycle is red. Report all individual recovery times and never pool
requests across cycles.

If the smoke has an external-attribution or harness failure, do not start the
confirmation set until the cause is corrected and a new smoke run ID/range is
frozen. If any confirmation cycle is invalid for external attribution, do not
replace only that cycle; rerun the complete three-cycle set under new run IDs
and disjoint ranges. A valid unfavorable cycle is never rerun.
