# Isolated cache-hit transport-knee evidence

This directory contains the compact, reviewable evidence for the corrected
five-replica Arena campaign run on 2026-09-01.

- `decision.json` is the concise conclusion and measured ladder.
- `independent-audit.json` is a raw-file recomputation with per-cell hashes,
  topology verification, exact accounting, health totals, resource maxima, and
  transition calculations.
- `SHA256SUMS` covers the evidence files and the two current methodology/result
  documents.

The raw run directories remain under ignored `results/transport/` storage. The
shareable ZIP contains each campaign `transport-summary.json`, health summary,
topology snapshot, and the compact audit without credentials or kubeconfig.

The loaded ladder is observed-break evidence. It is not a healthy steady-state
capacity certification because 38 of 42 loaded cells failed the target-health
SLO.
