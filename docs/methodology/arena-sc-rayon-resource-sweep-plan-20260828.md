# Arena SC Rayon/resource sweep plan — 2026-08-28

## Question this experiment answers

With the upstream semantic-classifier image unchanged, what intra-request
parallelism does Candle/Rayon actually benefit from, and where does adding
Rayon workers stop improving one exact64 cache-miss classification?

This is deliberately separate from the horizontal replica matrix. Every cell
has one target Pod, `LLM_D_SC_INFERENCE_WORKERS=1`, one closed-loop client,
one connection, and direct Pod-IP traffic. The only runtime variable is
`RAYON_NUM_THREADS`: unset, 1, 2, 4, or 8.

## Why the resource envelope is Guaranteed 16, not Guaranteed 8

Arena exposes two logical SMT threads per physical core and runs CPU Manager
with `full-pcpus-only`. A Guaranteed request of 8 CPUs therefore assigns four
complete physical cores. That is enough to avoid quota throttling for eight
logical threads, but it is not enough to test whether eight Rayon workers can
scale across eight physical cores: the RT8 cell must use SMT siblings.

The sweep instead gives every variant the same Guaranteed 16-CPU, 4-GiB
envelope. The live topology gate must prove that the cpuset contains at least
eight complete physical-core sibling sets before load begins. This keeps
RT1/2/4/8 at or below the available physical-core count. The OS still chooses
which eligible CPUs execute each Rayon worker; this is resource isolation, not
per-thread affinity. Equal CPU and memory requests/limits also retain
Guaranteed QoS.

The unset variant is a different kind of control. Candle 0.11 obtains Rayon's
default from the host-visible physical CPU count, so it can create far more
workers than this Pod's cpuset. No practical shared-node envelope can make that
host-wide default "unbounded" without consuming the node. The result is
therefore labeled an ambient-default oversubscription control under the fixed
eight-core budget, not an estimate of unconstrained 144-core performance.

## Isolation and reversibility

`hack/arena-sc-rayon-resource-sweep.sh` never patches or scales
`classifier-target`. After taking the global benchmark lock, it creates a
temporary Deployment from the reference Pod template with a unique selector.
The temporary Pods intentionally do not carry the classifier Service labels;
each cell also fails if its Pod UID appears in any EndpointSlice. Measured
traffic goes from the driver node directly to that Pod IP.

Each cell scales the temporary Deployment to zero, waits for the prior Pod to
be deleted, applies the next environment value while at zero, and creates one
fresh Pod. Before starting a driver, it verifies:

- the upstream image digest, model digest, and tokenizer digest;
- Guaranteed CPU/memory requests and limits;
- one worker, the requested Rayon setting, and no Candle thread override;
- Ready state, zero restarts, and fixed target-node placement;
- no Service EndpointSlice membership; and
- a complete, non-housekeeping, non-overlapping SMT cpuset containing at least
  eight physical-core sibling sets.

Cleanup deletes only Jobs with the active cell's exact run label and deletes
the temporary Deployment. The reference Deployment is fetched again; its UID,
generation, and complete spec must match the entry snapshot. The lock is
retained if cleanup or that integrity proof fails.

## Confirmatory protocol

The default run is five blocked repetitions. Each block contains all five
Rayon variants in a deterministic seed-randomized order. Cells use a 180-second
plateau, unique generated exact64 sequences, a fresh target Pod, and 50,000
candidate rows so a faster variant cannot exhaust its corpus.

Primary comparisons are paired within repetition:

- useful RPS and p50/p99 successful RPC RTT;
- cgroup CPU cores and CPU time per successful request;
- cgroup throttling, readiness, restarts, and warning events;
- effective cpuset and full-core count; and
- PID 1 thread count before and after load.

The PID thread count is supporting evidence because the process also owns
Tokio and service threads; it is not interpreted as a direct count of Rayon
workers.

The intra-pod thread knee is the smallest explicit thread count after which
the median paired throughput gain is less than 10% while p99 or CPU/request no
longer improves. The unset control is compared with RT1 to quantify the
production-default penalty, but it is not included when choosing an explicit
thread knee.

This sweep does not establish the horizontal replica knee or the open-loop
overload knee. It identifies the correct per-Pod runtime/resource shape those
separate experiments should hold constant.

## Plan-only validation

Plan-only mode writes and prints the randomized schedule without reading or
mutating Arena:

```sh
PLAN_ONLY=1 \
SWEEP_RUN_ID=rayon-plan-0828 \
SWEEP_SEED=20260828 \
SEQUENCE_BASE=30000000000 \
DRIVER_IMAGE='registry.example/driver@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa' \
TARGET_IMAGE='sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb' \
MODEL_SHA256='cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc' \
SWEEP_DIR=/tmp/rayon-plan-0828 \
./hack/arena-sc-rayon-resource-sweep.sh
```

For a live run, replace the placeholder digests with the pinned evidence
digests and choose a globally unused sequence base and run ID. Do not start it
while another script owns `sc-benchmark-matrix-lock`.
