# llm-d-sc same-Pod recovery preregistration amendment

Status: frozen on 2026-08-29 before any replacement recovery-smoke
`Classify` RPC.

This amendment supplements, and does not rewrite,
`arena-sc-same-pod-recovery-preregistration-20260829.md` (SHA-256
`287d013f41147d0e30651b8b3651859760da0d18f5178f6d5ad1bec86f631f7a`).
Every workload phase, rate, duration, service gate, recovery decision rule,
target image, driver image, model identity, tokenizer identity, and target
W1/RT1/GQ2 shape in the original preregistration remains frozen unless this
document explicitly replaces it.

## Reason for amendment

The first harness smoke, `recovery-smoke-20260829-a`, failed in a checkpoint
observer before T0. Under Bash `set -u`, one `local` statement referenced
`scheduled_s` in another assignment before that local variable existed. The
operator deleted all run-labeled Jobs and Pods before T0 and then interrupted
the controller. All 14 driver stdout files contain exactly one ARMED record,
all warm-up counts are zero, and no final driver report exists. A later query
of the same zero-restart target, 266 seconds after planned T0, contained only
its startup READY line. Smoke A emitted zero benchmark `Classify` RPCs; it is
not a recovery result and is permanently excluded.

Evidence:

- Incident JSON SHA-256:
  `efcce2bde1f7acfe3583366f9d552f60b324b401218cf8effa9af58cf413251d`
- Post-abort same-UID verification SHA-256:
  `89e9f492cabd63940ca5a268167419924a9ff4210926953e1bfbddbec80c1749`
- Pre-fix orchestrator SHA-256:
  `deacce4bb453892fc6faf2f3443c0452d9d734dc7670b96ed37926d47a96f36a`

The smoke-A reservation also overlapped the earlier Stage A knee-confirmation
reservation. This was a ledger defect, not emitted workload contamination:
Stage A completed first with zero cache hits, while smoke A emitted zero
`Classify` RPCs. Across 161 canonical generated-corpus driver artifacts and
494,256 emitted RPCs, the independent audit found zero cross-run emitted
sequence collisions. The entire smoke-A C0 allocation is burned.

- Sequence audit SHA-256:
  `d05a33f7f8073e53d9947f0c5c75cfc4dc86b0f946e4bc7d8acf0d4d2ae75094`
- Machine-readable ledger SHA-256:
  `a19a8ab9482949024c4031456415e5f2d4a593186507653f767cc0f3ea162c31`

## Replacement smoke allocation

The only replacement smoke authorized by this amendment is:

- Run ID: `recovery-smoke-20260829-b`
- Cycle index: `4`
- Half-open reservation: `[19000600000, 19000750000)`
- Expected emitted intervals: none until the live ARMED and load-authorizing
  gates below pass
- Relationship to confirmation: smoke B is excluded from the three-cycle
  aggregate decision

The sequence audit found no current reference or emitted interval collision
for C4. Plan checks and the live run may share this allocation only as the same
allocation owner. If smoke B fails for harness or external attribution, C4 is
burned and another amendment must assign a new run ID and a disjoint 21B
suballocation. The same run ID may not be rerun.

The original confirmation allocations remain unchanged and unconsumed:

- `recovery-confirm-r1-20260829-a`, C1,
  `[19000150000, 19000300000)`
- `recovery-confirm-r2-20260829-a`, C2,
  `[19000300000, 19000450000)`
- `recovery-confirm-r3-20260829-a`, C3,
  `[19000450000, 19000600000)`

They remain unauthorized until smoke B produces a valid, fully analyzed
cycle. A valid unfavorable smoke is still a valid framework result; it is not
repeated to seek a better outcome.

## Frozen harness repair and pre-T0 safety contract

The replacement smoke pins these exact local artifacts:

- Recovery orchestrator SHA-256:
  `2f10e5926b427533c7f1b2887bb0a2c2dc8f19b71e1fcd6f378974fb046cf6af`
- Recovery summarizer SHA-256:
  `c694381cc0da3800a6692011083cdf606e6fdfea0bac2bba481e8ec8532a4b99`
- Focused test SHA-256:
  `1fb68c25d213a8431965f35c3ee57aaf34f61969bbe9ebf648b874fa22bcbf12`

The amended pre-T0 contract is:

1. T0 must be at least 360 seconds after plan creation and at least 180
   seconds after the exact target becomes Ready.
2. All 14 application-level ARMED records and all 14 Kubernetes-Ready driver
   Pods must validate no later than T0-180 seconds.
3. The load-authorizing target-bound checkpoint is scheduled at T0-175
   seconds and must atomically publish valid identity, readiness, restart,
   image, cpuset, `cpu.max`, and cgroup evidence by T0-155 seconds.
4. Target-bound cluster commands run in their own process group and are
   terminated before the hard deadline; a stalled call cannot cross T0.
5. Every checkpoint and health observer has an EXIT guard. An unexpected child
   exit immediately requests run-label-scoped deletion and is also reaped by
   the parent before load, throughout the run, and at final collection.
6. On any incomplete-run cleanup, one combined run-label-scoped Jobs/Pods
   deletion request begins before any observer is killed or waited.
7. A pre-T0 failure receives at most 90 seconds for foreground Jobs/Pods
   deletion, followed by a bounded zero-object query. Zero labeled Jobs and
   Pods must be proven by T0-25 seconds. Failure to prove this is
   `cleanup_failed`, never a valid zero-load claim.
8. A late cancellation still issues immediate and repeated label-scoped
   deletion and fails closed even when the zero-before-T0 proof window has
   already elapsed.
9. All Bash locals are declared separately from their assignments; no
   `local name=value` declaration remains in the orchestrator.

The margin equation is frozen as:

`155 >= 25 + 5 + 5 + 90 + 5 + 15 + 10` seconds,

covering the cancellation-completion lead, immediate request, termination
grace, foreground deletion, second termination grace, zero-object query, and
explicit safety margin.

## Verification frozen before replacement load

The exact pinned files passed:

- `bash -n` for the orchestrator;
- 38/38 focused recovery tests;
- 9/9 Stage A analyzer tests;
- 8/8 Stage B analyzer tests;
- 4/4 open-loop summary tests;
- 5/5 topology-integration tests;
- 8/8 topology-preflight tests; and
- Python byte-code compilation with a workspace-external cache.

The focused suite executes, rather than merely text-inspects:

- the nounset-sensitive checkpoint calculation;
- a stalled observer whose foreground deletion and zero Jobs/Pods proof finish
  before synthetic T0;
- a late stalled observer that still requests Jobs/Pods deletion and is marked
  `cleanup_failed`; and
- analyzer rejection when plan creation is less than 360 seconds before T0.

## Unchanged measurement and decision rules

Smoke B retains the original pre35 / five-second gap / overload47 / sparse
recovery probes / five-second post gap / post35 cycle, direct Pod IP, one HTTP/2
connection, 64-token generated unique misses, zero warmups, exact target
counter reconciliation, OTEL completeness gates, and green/amber/red recovery
thresholds. The post stream may confirm but may not manufacture an earlier
recovery. All results, including unfavorable valid results, must be reported.

Only after smoke B passes every validity gate may the original three-cycle
confirmation set begin. Each confirmation cycle still requires a fresh,
traffic-clean target Pod and the complete three-cycle set may not selectively
replace an unfavorable valid outcome.
