# Arena SC CPU-topology preflight — 2026-08-28

## Purpose

Run this gate after target Pods are Ready and before creating load-driver Jobs.
It prevents a CPU-placement defect from being mislabeled as a semantic
classifier scaling knee.

The gate reads every target Pod's effective cgroup v2 cpuset and the serving
node's kernel SMT sibling map. A placement passes only when:

- every cpuset is a union of complete `thread_siblings_list` sets;
- no target CPU is a reserved/housekeeping CPU;
- no target occupies a core whose other SMT thread is housekeeping; and
- target Pods do not overlap CPUs or physical cores.

`PASS` exits 0, a placement `FAIL` exits 1, and incomplete evidence is
`INVALID` and exits 2. Never start a measured load after either non-zero exit.

## Live read-only gate

The live path performs Kubernetes GETs and `oc exec` reads only. It does not
create, patch, scale, or delete any object.

```sh
python3 hack/arena-sc-topology-preflight.py live \
  --kubeconfig /tmp/llm-d-sc-arena-kubeconfig \
  --namespace llm-d-sc-scaleout \
  --selector 'app.kubernetes.io/component=classifier-target,app.kubernetes.io/name=llm-d-sc-scaleout' \
  --expected-pods 20 \
  --format json
```

By default, the gate derives housekeeping CPUs as the complement of the
`isolcpus=` range in the node's saved
`tuned.openshift.io/bootcmdline` annotation. To make the source explicit or
override it, repeat `--reserved-cpus NODE=CPU_LIST` once per serving node.

The JSON report embeds the complete captured snapshot. Preserve that output in
the cell's pre-load evidence and include its checksum in the cell attestation.
It can be checked again without cluster access:

```sh
python3 hack/arena-sc-topology-preflight.py snapshot topology-preflight.json
```

## Open-loop sweep integration

`hack/arena-sc-inference-open-loop-sweep.sh` requires this gate by default for
every cell. The cell launcher invokes it only after the fresh target set is
Ready and has passed image, node, restart, and replica checks. It runs before
the measurement start time is calculated, before the cgroup sampler starts,
and before any driver Job is created. Direct open-loop use of
`hack/arena-sc-inference-cell.sh` also defaults the gate on; its closed-loop
mode retains the prior opt-in behavior.

Each successful cell retains:

- `topology-preflight-report.json`, the canonical live report;
- `topology-preflight-execution.json`, the runner exit, authorization decision,
  target-identity match, and evidence hashes;
- `topology-preflight-stdout.txt` and `topology-preflight-stderr.txt`, the raw
  runner streams; and
- the execution/report verdicts and hashes under `cell.topology_preflight` in
  `cell.json` (and therefore `summary.json`).

The sweep summarizer re-hashes those files and requires both the report and its
embedded snapshot to contain exactly the target names, UIDs, and node
placements in that cell's `targets-before.json`. A missing file, non-zero
runner exit, non-PASS verdict, hash mismatch, or identity mismatch invalidates
the cell and prevents driver load or final sweep attestation.

The integration controls are:

```sh
TOPOLOGY_PREFLIGHT_ENABLED=1             # default for open-loop sweeps
TOPOLOGY_PREFLIGHT_RUNNER=hack/arena-sc-topology-preflight.py
TOPOLOGY_PREFLIGHT_CONTAINER=''          # optional target container name
TOPOLOGY_PREFLIGHT_RESERVED_CPUS=''      # optional space-separated NODE=CPU_LIST values
```

Setting `TOPOLOGY_PREFLIGHT_ENABLED=0` is an explicit forensic-only waiver. The
sweep provenance then records that CPU-sibling topology was not attested; such
a run must not support a placement-sensitive knee or promotion claim.

## Saved r20 forensic replay

The following reads only the existing Arena artifacts. The sibling offset is
explicit because the old cell did not capture the kernel's sibling map:

```sh
python3 hack/arena-sc-topology-preflight.py artifacts \
  --cgroup-summary docs/benchmarks/runs/matrices/rt1h6-rem-0828/cells/m-rt1h6-rem-0828-o0003-w1-r20-c1/cgroup-summary.json \
  --nodes-file docs/benchmarks/runs/matrices/rt1h6-rem-0828/nodes-original.json \
  --node gnr2.fm2aihpcsed.com \
  --sibling-offset 144 \
  --format text
```

That replay must fail the placement: `classifier-target-595b8fbf9c-zk2dm`
received `144-145`. With the observed two-thread layout, those CPUs belong to
the separate sibling groups `0,144` and `1,145`; both `0` and `1` are in the
housekeeping set. The other r20 targets received complete sets such as
`5,149`.

Artifact replay is forensic evidence, not a future load gate. Its topology is
reconstructed from the supplied offset and is therefore marked
non-authoritative. Future live reports read every
`/sys/devices/system/cpu/cpu*/topology/thread_siblings_list` entry and reject a
capture when the map is incomplete or asymmetric.

## Known boundary

This gate proves CPU placement, not exclusive use by firmware, interrupts,
kernel work, or processes outside the target cgroup. It also does not measure
NUMA locality, cache contention, frequency, or thermal state. Keep the existing
runtime, health, telemetry, and recovery gates; this preflight removes one
specific confounder before load rather than replacing them.
