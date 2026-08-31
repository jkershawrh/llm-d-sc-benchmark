# Arena SC unchanged-image profiling lane — 2026-08-28

## Outcome

Use two separate evidence lanes.  The default lane is a read-only cAdvisor
observer outside the target Pods.  It can run with every benchmark cell and
adds no process, sidecar, library, capability, mount, or environment variable
to the classifier.  A short privileged host-profiler lane is necessary only
for PMU counters, effective frequency, futex/off-CPU attribution, and kernel
scheduler events.  That lane keeps the upstream classifier image unchanged,
but it is a separate, explicitly approved cluster mutation and its results must
never be mixed with primary throughput measurements until overhead is bounded.

The already saved cgroup evidence gives a strong starting attribution:

- The exact W4/C4 comparison changed only `RAYON_NUM_THREADS`: unset delivered
  3.4667 RPS with p50 1.147890 s, while RT1 delivered 41.25 RPS with p50
  96.649 ms.
- Unset spent 41.916 s of the 58.742 s measured CPU time in system mode
  (71.355%).  RT1 spent 0.427 s of 58.058 s in system mode (0.735%).
- System CPU per successful request fell from 201,517 us to 172 us, a
  1,168.6x collapse.  This is scheduler/runtime overhead, not inference math.
- In the RT1 horizontal lane, r1 used 23,397 us CPU per successful request and
  r5 used 25,448 us, an 8.765% increase.  System share did not rise: it was
  0.950% at r1 and 0.910% at r5.  Endpoint RPS CV at r5 was only 0.470%.
- The same RT1 pattern persists through r15: roughly 37.54 RPS per Pod,
  roughly 25.47 ms CPU per request, less than 1% system CPU, and less than
  0.50% endpoint RPS CV.  This points first to active-core frequency/package
  power or shared microarchitecture effects, not routing and not a second
  Rayon scheduling collapse.
- The saved `scaling_cur_freq` value is 800 MHz at both cell boundaries for all
  these Pods.  It was captured outside active inference and is not an
  effective in-plateau frequency measurement.  It cannot support a frequency
  conclusion.

These values are reproducible without cluster access:

```sh
python3 hack/arena-sc-profile-artifacts.py --pretty \
  docs/benchmarks/runs/r1-w4-c4-gq8-r1 \
  docs/benchmarks/runs/r1-w4-rt1-c4-gq8-r1
```

## What Arena exposes today

The inspection was read-only.  No Arena object was created, patched, scaled,
or deleted.

### Target and telemetry security

- Classifier Pods and the OTEL Pods are admitted under `restricted-v2`.
- The classifier is non-root, uses `RuntimeDefault` seccomp, drops every Linux
  capability, forbids privilege escalation, and has a read-only root file
  system.
- The OTEL DaemonSet has no `hostPID`, `hostNetwork`, host path, or capability.
  Its image is distroless: neither `sh` nor `/bin/cat` exists.
- The OTEL service account can read `nodes/proxy` and `nodes/stats`.  It cannot
  create Pods, exec into Pods, or use the privileged SCC.
- The current administrator can create Pods/DaemonSets, exec, update
  `pods/ephemeralcontainers`, access `nodes/proxy`, and use the privileged SCC.
  Those administrator rights do not make the existing OTEL service account a
  profiler.
- The namespace warns/audits the restricted Pod Security Standard.  A host
  profiler therefore needs a dedicated, time-bounded privileged SCC grant; it
  must not be added to the existing OTEL service account.

### Signals available with no target-Pod perturbation

The authenticated kubelet cAdvisor endpoint on gnr2 currently exposes these
per-container series:

- total, user, and system CPU seconds;
- CFS periods, throttled periods, and throttled seconds;
- CPU-pressure waiting and stalled seconds;
- current thread count and maximum allowed threads; and
- task-state gauges.

The existing Thanos path does not currently retain the detailed user/system,
thread, or task-state series for this namespace.  The current OTEL
`kubeletstats` receiver also does not export them.  The observer must therefore
read the existing kubelet endpoint directly during the cell.  The endpoint's
source cadence observed in a live read-only check was approximately 10–18
seconds, so polling faster than five seconds adds API load without adding much
information.

`hack/arena-sc-profile-cadvisor.py` implements this lane.  It makes one
read-only request per target node every five seconds, selects only attested
target Pod UIDs and the `llm-d-sc` container, retains source timestamps, and
interpolates cumulative counters at the plateau boundaries.  The default
20-second pre/post padding is required to bracket those boundaries.  It fails
validity on missing boundary coverage, a changed Pod UID/image, a restart,
readiness loss, or a poll error.

Example, launched after the cell has written its target and cell metadata but
before the padded collection start:

```sh
cell_dir=docs/benchmarks/runs/example-cell
python3 hack/arena-sc-profile-cadvisor.py \
  --kubeconfig /tmp/llm-d-sc-arena-kubeconfig \
  --targets-json "$cell_dir/targets-before.json" \
  --start-epoch-ms "$(jq -r .start_epoch_ms "$cell_dir/cell.json")" \
  --duration-seconds "$(jq -r .duration_seconds "$cell_dir/cell.json")" \
  --output-dir "$cell_dir/profile-cadvisor"
```

This observer can establish peak threads, user/system CPU, CPU-pressure time,
and throttling.  cAdvisor on Arena does not expose container-scoped context
switches, CPU migrations, PMU cycles/instructions, futex waits, sleep/off-CPU
stacks, or effective frequency.

### Signals available with a small target-Pod perturbation

The upstream target image contains `sh` and exposes its own `/proc` and cgroup
files.  A separately invoked `oc exec` can read PID 1 and each task's `sched`,
`schedstat`, `status`, and `wchan` files without changing the image or any
Kubernetes object.  Two boundary snapshots can derive:

- voluntary and involuntary context-switch deltas;
- scheduler migration deltas;
- runnable run-queue wait versus on-CPU time from `schedstat`; and
- thread names and point-in-time wait channels.

An exec creates a shell process in the measured container's cgroup.  It is not
zero-overhead, and `wchan` boundary snapshots are not a defensible futex-time
estimate.  Use this only after an OFF/ON overhead test, and keep its evidence
in a diagnostic appendix rather than the primary benchmark row.

`hack/arena-sc-profile-proc-sched.py` implements the two-snapshot version.  It
executes all target reads concurrently, writes nothing inside the container,
requires stable task IDs across both boundaries, and fails validity on a Pod,
image, restart, readiness, cpuset, task-set, or counter discontinuity:

```sh
cell_dir=docs/benchmarks/runs/example-cell
python3 hack/arena-sc-profile-proc-sched.py \
  --kubeconfig /tmp/llm-d-sc-arena-kubeconfig \
  --targets-json "$cell_dir/targets-before.json" \
  --start-epoch-ms "$(jq -r .start_epoch_ms "$cell_dir/cell.json")" \
  --duration-seconds "$(jq -r .duration_seconds "$cell_dir/cell.json")" \
  --output-dir "$cell_dir/profile-proc-sched"
```

Its `schedstat` wait delta means runnable-but-not-running time.  Sleeping on a
futex is not runnable, so it is intentionally not labeled total off-CPU time.

### Signals that require a privileged host profiler

Cycles, instructions, reference cycles, cache misses, effective GHz, sampled
futex/off-CPU stacks, and scheduler tracepoints require access that the current
restricted target and OTEL Pods do not have.  The viable architecture is a
short-lived profiler Pod on gnr2 with host PID access and the minimum required
host mounts/capabilities, attaching to the target's host PID or cgroup.  Do not
add a sidecar or profiler binary to the classifier Pod.

Arena's CPU labels advertise fixed PMU counters for cycles, instructions,
reference cycles, and top-down slots.  Both serving nodes run kernel
`5.14.0-687.17.1.el9_8.x86_64`, use the `openshift-node-llm-compute` TuneD
profile, `intel_pstate=active`, the performance governor, and turbo.  The
cluster also contains a driver-toolkit ImageStream pinned to digest
`sha256:0b2cb4b755d32aff7b13fe1a3bc2a221d2f2e8910d65bda68c57cb6bdfe7054c`,
matching the nodes' RHCOS `9.8.20260623-0` build.

Tool and kernel-policy feasibility is not yet proven.  The driver-toolkit
manifest is present, but access to its external blob was unauthorized from the
workstation, so the presence of `perf`, `bpftrace`, and `bpftool` was not
verified.  `perf_event_paranoid`, `kptr_restrict`, tracefs access, BPF lockdown,
and an ephemeral container's SCC behavior also remain unverified.  A future
approved lane must preflight these facts and stop without load if any required
counter or tracepoint is unavailable.  A separate privileged node Pod is the
reliable design; privileged ephemeral attachment to the existing restricted
target is not assumed to work.

## Experiment sequence

### 1. Validate profiler overhead first

Use blocked, randomized OFF/ON pairs with the same target image, exact64
corpus, worker/thread/resource shape, Pod placement rules, direct Pod IP, and
unique sequences.  Do not reuse the OFF row as a baseline from another day.

- cAdvisor lane: RT1/W1/C1/GQ2 at r1 and r20, at least three 180-second pairs,
  five-second polling, and 20-second padding.
- `/proc` boundary lane, if used: RT1/W1/C1/GQ2 at r1, at least five
  180-second OFF/ON pairs.
- `perf stat` lane, after privilege preflight: one 30-second window centered
  in a 180-second plateau, at least five OFF/ON pairs for RT1 r1 and three for
  the unset control.
- Sampling profiler or scheduler/futex tracing: calibrate separately; never
  infer its overhead from `perf stat`.

Accept the cAdvisor observer only if the median paired useful-RPS change is at
most 1%, p99 change is at most 2%, CPU/request change is at most 1%, there is
no consistent direction in four of five pairs, and health/telemetry remain
clean.  Accept a counting-only PMU window at 2%/3%/2%.  If a profiler exceeds
its limit, shorten its window or lower its sample rate and repeat calibration;
do not correct benchmark numbers arithmetically for profiler overhead.

### 2. Attribute the unset-versus-bounded Rayon behavior

Use the existing isolated Rayon sweep contract: one fresh temporary target,
W1/C1, direct Pod IP, exact64 misses, identical Guaranteed 16-CPU envelope,
and blocked randomization of unset, RT1, RT2, RT4, and RT8 over five
180-second repetitions.  The reference deployment remains untouched.

Run the read-only cAdvisor observer for every cell.  On a separately calibrated
subset of unset and RT1 cells, collect:

- `task-clock`, cycles, reference cycles, instructions, branches, branch
  misses, LLC references/misses, context switches, CPU migrations, and page
  faults;
- per-task scheduler runtime, run-queue wait, and switch/migration deltas; and
- a short futex/off-CPU sample only if tracefs/BPF preflight succeeds.

The oversubscription attribution is confirmed if unset shows a much larger
peak thread population plus the already observed system CPU/request collapse,
and switch/migration/futex or run-queue-wait cost falls sharply when bounded.
Instruction count per request should remain broadly stable.  If the unset
thread count is not large, revisit Candle/Rayon pool discovery before calling
the scheduler mechanism proven.

### 3. Attribute the horizontal approximately 9% loss

Hold RT1/W1/C1/GQ2 constant and repeat r1, r3, r5, r10, and r20 in randomized
blocks.  Reject malformed SMT sibling placement before load.  Attach the
cAdvisor observer to every cell; run the calibrated PMU counter window at r1,
r5, and r20.  Preserve endpoint-level values, because a single malformed
cpuset dominated the prior r20 p99.

Interpret the counter deltas per successful request:

- Frequency/package-power effect: instructions/request, cycles/request, IPC,
  cache misses/request, switch/migration rate, and run-queue wait stay broadly
  stable; cycles per task-clock and cycles/reference-cycles fall by roughly
  the observed throughput loss while CPU time/request rises.
- Shared-cache or memory contention: instructions/request stays stable but
  cycles/request and cache misses/request rise, IPC falls, and effective GHz
  does not explain the delta.
- Scheduler/placement effect: switch/migration or run-queue-wait deltas rise,
  endpoint dispersion follows cpuset/socket placement, or a small placement
  cohort owns the latency tail.
- SC-code scaling effect: instructions/request or hot-stack composition changes
  materially with replica count after frequency, cache, and placement are
  controlled.  Only this outcome supports a core-code scaling claim.

## Evidence handling

Keep primary load results, cAdvisor observations, `/proc` diagnostics, and
privileged PMU/trace output in separate directories with separate validity
flags.  Hash each profiler executable or image digest and profiler arguments.
Record target Pod UID, image ID, cgroup/cpuset, node boot ID, kernel, TuneD
profile, PMU event list, multiplex percentage, and exact profiler window.

Do not let a successful profiler repair an invalid load cell.  Conversely, a
profiler failure invalidates only the attribution lane when the underlying
unprofiled load cell and its health gates remain valid.
