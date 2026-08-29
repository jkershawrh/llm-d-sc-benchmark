# Arena batched exact-token inference ladder — 2026-08-28

## Method

- isolated namespace `llm-d-sc-scaleout`
- pinned upstream classifier image and private model/tokenizer digests
- `maxSkew: 1`, `DoNotSchedule` target placement across gnr2 and rhgnr1
- controlled startup batches: r1→3→5, r5→7→10, r10→15→20, with readiness
  and node-stability waits between batches
- direct endpoint workload: concurrency 4 per target, 50 unique cache misses,
  exactly 64 tokens including specials
- 2,048 verified contexts split across two ConfigMaps; rung offsets intended to
  be globally disjoint
- all drivers shared a future start epoch; useful common-wall throughput uses
  successful requests divided by barrier-to-last-completion time

The corpus driver reports only per-endpoint percentiles, not raw latency
samples. A mathematically merged aggregate p99 is therefore unavailable.

## Results

| Rung | Placement | Outcomes | Common wall | Aggregate useful RPS | Mean endpoint RPS / CV | Endpoint p99 |
| --- | --- | --- | ---: | ---: | --- | --- |
| r5 | 3 gnr2 / 2 rhgnr1 | 250/250 OK | ~15 s | 16.7 | 5.221 / 20.7% | mean 1.029 s; max 1.345 s |
| r10 | 5 / 5 | 500/500 OK | ~25 s | 20.0 | 2.782 / 6.8% | mean 2.165 s; max 3.539 s |
| r20 | 10 / 10 | 950 OK; 19 jobs complete, 1 harness failure | ~48 s | 19.8 observed | 1.532 / 9.5% (19) | mean 5.426 s; max 13.980 s |

All successful envelopes carried only `OK` statuses. R5 and r10 were complete
and both workers remained stable. Controlled batched startup reached r20 with
20/20 Ready, exact 10/10 placement, and zero target restarts.

During r20 load, gnr2 was observed NotReady from approximately `22:32:24Z` to
`22:32:34Z` and cluster metrics disappeared. Unique load-window probe failures
affected 11/20 targets for readiness and 7/20 for liveness. This crossed the node-wide stop. The
Deployment was returned to one desired/actual/Ready replica; both nodes ended
`KubeletReady`.

## Harness failure at r20

`corpus-r20-6` did not fail from eviction or classifier behavior. Its requested
slice began at offset 1,000 in a 1,024-row ConfigMap and requested 50 rows,
crossing the file boundary. The driver correctly rejected the invalid slice
(`corpus does not contain requested slice`). This is an orchestration partition
bug. Consequently r20 is partial evidence; 19 endpoints completed 50/50 OK,
while one endpoint received no valid workload. The 19-endpoint common-wall
useful rate is reported, not extrapolated to twenty.

## Knee and interpretation

Aggregate useful throughput improved only about 20% from r5 to r10 while target
count doubled; endpoint p99 roughly doubled. At r20, observed aggregate useful
throughput was flat/slightly lower than r10 while endpoint mean p99 increased
about 2.5x and the node became NotReady. Thus r10 is the last complete rung and
r20 is a RED infrastructure/harness boundary—not a valid r20 capacity result.

The experiment shows batched startup solves the earlier concurrent-startup
failure through r20, but sustained exact-token inference across twenty replicas
still exceeds this two-worker topology's stable node/control envelope.
