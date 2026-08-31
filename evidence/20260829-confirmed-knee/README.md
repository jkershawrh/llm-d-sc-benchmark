# Compact evidence: confirmed `(41, 42]` RPS/Pod knee

This directory retains the smallest reviewable evidence set for the confirmed
2026-08-29 service/SLO knee. It contains structured summaries and provenance,
not the full raw cluster capture.

- `stage-a-*`: five original paired blocks and their initial decision
- `stage-b-*`: five extension blocks run under a protocol frozen before load
- `combined-confirmation-decision.json`: analyzer output for all ten blocks
- `combined-confirmation-audit.json`: independent recomputation and caveats
- `SHA256SUMS`: byte identities for every retained evidence artifact

The full raw evidence contains per-cell driver output, topology, cgroup,
health, and telemetry captures. It is intentionally excluded from Git because
it is large and may contain cluster-specific metadata. A claim should be
treated as fully auditable only when the compact artifact hashes reconcile to
the archived raw evidence.

Reporting language must follow `docs/results/confirmed-knee-20260829.md`.
In particular, do not call this an absolute capacity ceiling or generalize it
to ClusterIP, other runtime settings, or horizontal replica counts.
