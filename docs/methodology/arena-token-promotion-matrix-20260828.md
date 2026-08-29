# Arena exact-token promotion matrix — 2026-08-28

## Outcome

The unmodified upstream target completed 24,000/24,000 unique cache misses with
valid responses and zero process restarts. Every token bucket reached its
throughput/latency knee at concurrency 8; concurrency 4 is the evidence-backed
usable ceiling for this four-worker, four-CPU target.

This is promotion-quality performance evidence for the tested topology and
artifact set. Operational health is not fully green: kubelet probe timeouts
continued under load despite the target remaining Ready.

## Method

- token buckets: 16, 64, 128, and 256 total tokens including specials
- concurrency: 1, 4, 8, and 16 persistent client connections
- repetitions: 3
- samples: 500 unique misses per cell; 24,000 total
- order: deterministic shuffle with seed `20260828`
- isolation: dedicated namespace, corpus PVC, driver Job, and same-node target
- summary statistic: median of the three cell repetitions

Five hundred samples retain five observations at nearest-rank p99, while each
high-pressure cell remains bounded enough to avoid intentionally driving the
known probe-starvation outage. Each token bucket used 6,000 disjoint contexts;
no context was reused between cells.

## Artifact provenance

- target runtime image:
  `sha256:04323612ce3f73873b4c3ed6e09264e828241537e2c1a4231b43f32e9744d5aa`
- driver runtime image:
  `sha256:161be071693def4c6e555b33f78df62ddfdcf4780411781379a106eaa57337b8`
- model artifact:
  `7914abbd152278879b4c3235d188e3006753bb778b7de6266fbcbe4c4ba2ef2f`
- tokenizer:
  `851ca67100d372ca3ae031a6abd168f53489eebfd7d89523f35c5c9b4d372c3c`
- node: `gnr2.fm2aihpcsed.com`
- raw log SHA-256:
  `f7a5d813449aa9f4ee11eb2380f7aadd4f5bebe9fa78aee11ec8e20e6d709ed7`

The Job reported `SuccessCriteriaMet=True` and `Complete=True`, both with reason
`CompletionsReached`. After completion, the target runtime `imageID` still
matched the pinned upstream digest and the pod was Ready with zero restarts.
A separate fresh-miss recovery Job completed after the matrix; its recovery
request succeeded in 246.7 ms, below the provisional five-second budget.

## Median results

| Tokens | C | Useful RPS | RPS range | p50 | p95 | p99 | Worst max |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 16 | 1 | 3.94 | 3.70–3.96 | 246.9 ms | 288.0 ms | 354.2 ms | 491.5 ms |
| 16 | 4 | **11.11** | 9.66–11.66 | 304.3 ms | 506.7 ms | 602.5 ms | 776.8 ms |
| 16 | 8 | 11.23 | 11.00–11.30 | 664.1 ms | 961.5 ms | 1,084.4 ms | 1,463.5 ms |
| 16 | 16 | 11.18 | 10.92–11.36 | 1,394.7 ms | 1,830.2 ms | 2,003.3 ms | 2,209.1 ms |
| 64 | 1 | 3.49 | 3.42–3.59 | 278.3 ms | 346.6 ms | 400.2 ms | 448.9 ms |
| 64 | 4 | **9.36** | 9.27–9.89 | 380.5 ms | 587.5 ms | 669.7 ms | 976.8 ms |
| 64 | 8 | 9.56 | 7.34–9.99 | 796.7 ms | 1,093.1 ms | 1,433.9 ms | 1,831.9 ms |
| 64 | 16 | 9.57 | 9.56–9.72 | 1,610.6 ms | 2,146.1 ms | 2,434.9 ms | 2,780.3 ms |
| 128 | 1 | 3.23 | 2.90–3.24 | 300.0 ms | 360.8 ms | 416.0 ms | 523.5 ms |
| 128 | 4 | **8.02** | 7.80–8.11 | 467.5 ms | 683.5 ms | 805.3 ms | 946.0 ms |
| 128 | 8 | 8.07 | 7.98–8.31 | 948.5 ms | 1,236.0 ms | 1,377.0 ms | 1,557.4 ms |
| 128 | 16 | 7.91 | 7.27–7.95 | 1,967.6 ms | 2,550.8 ms | 2,799.4 ms | 3,501.0 ms |
| 256 | 1 | 2.34 | 2.31–2.34 | 411.2 ms | 528.7 ms | 663.4 ms | 805.6 ms |
| 256 | 4 | **5.01** | 4.97–5.07 | 759.3 ms | 1,031.8 ms | 1,136.2 ms | 1,309.4 ms |
| 256 | 8 | 4.36 | 4.12–4.89 | 1,744.9 ms | 2,279.5 ms | 2,440.7 ms | 2,901.2 ms |
| 256 | 16 | 4.93 | 4.56–5.01 | 3,190.9 ms | 3,793.6 ms | 4,124.0 ms | 4,882.9 ms |

Bold RPS marks the usable concurrency-4 operating point, not necessarily the
absolute numerical maximum. At concurrency 8, additional throughput is only
0.6–2.2% for 16/64/128 tokens while p99 increases 71–114%; the 256-token path
already loses 13.1% throughput as p99 rises 115%.

Sequence length is a first-order capacity dimension. At concurrency 4, moving
from 16 to 256 tokens reduces median throughput 54.9% and raises p99 88.6%.
At concurrency 16, the same comparison reduces throughput 55.9% and raises p99
105.9%.

Across-repeat useful-RPS coefficient of variation was below 10% in 14 of 16
cells. It ranged from 0.8% to 9.6% except for 64/c8 (15.9%); 256/c8 was 8.8%.
The isolated 64/c8 variability is retained as amber rather than averaged away.

## Predeclared rubric evaluation

The knee definition was fixed before this run: the first step with less than
10% throughput gain and more than 25% p99 growth from the preceding step. All
four token curves meet it at concurrency 8:

| Tokens | c4→c8 throughput | c4→c8 p99 | Classification |
| ---: | ---: | ---: | --- |
| 16 | +1.1% | +80.0% | knee |
| 64 | +2.2% | +114.1% | knee |
| 128 | +0.6% | +71.0% | knee |
| 256 | -13.1% | +114.8% | knee and throughput collapse |

Below the knee, c4 p99 remained under twice c1 p99 for every token bucket
(1.70x, 1.67x, 1.94x, and 1.71x). Non-overload error rate was zero, restart
count was zero, and post-matrix recovery was green. Those rubric cells pass.

The combined promotion gate does **not** pass production: no explicit shedding
appeared above the knee, tail latency continued growing, and the independent
health observation classifies the probe timeouts as red. This evidence supports
a staging capacity limit of concurrency 4 per tested pod, but it is not a
production-default approval.

## Health-plane finding

Independent approximately ten-second sampling observed:

- CPU: 0.893–3.253 cores against a four-core limit
- memory: 150–196 MiB against a 4 GiB limit
- pod Ready in every sample; restart count stayed zero
- readiness TCP timeout event count increased from 19 to 78 (59 in-run)
- liveness timeout event count increased from 4 to 10 (6 in-run)

Timeout timestamps span all token buckets and concurrency 4/8/16. The service
completed every request, but kubelet probes still contend with the serving path
well below CPU or memory limits. This makes the production health gate red:
successful request completion alone does not prove robust health isolation,
and liveness failures risk avoidable process replacement.

See `arena-gnr2-token-promotion-live-health-20260828.md` for the sampled event
timeline and `arena-token-promotion-matrix-raw-20260828.txt` for all 48 result
envelopes and deterministic cell markers.

## Promotion rubric

- Green: response correctness, completion, restart behavior, artifact
  provenance, exact-token control, disjoint miss corpus, repeat coverage.
- Amber: the 64-token c8 repetition RPS range warrants scrutiny.
- Red: in-run readiness/liveness probe timeouts and absence of explicit
  shedding above the knee block the production gate.
- Red beyond staging: treating byte size as sequence length or operating at c8+
  as a default.

No core semantic-classifier change is proposed by this evidence. Candidate PR
areas remain hypotheses: stage latency/queue telemetry, health isolation, and a
maintained external exact-token benchmark driver.
