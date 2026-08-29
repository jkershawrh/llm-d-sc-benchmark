# Arena tokenizer-controlled payload method — 2026-08-28

## Provenance

- target image: `sha256:04323612ce3f73873b4c3ed6e09264e828241537e2c1a4231b43f32e9744d5aa`
- target model artifact: private `classifier-model` PVC mounted read-only at `/models`
- model artifact digest: `7914abbd152278879b4c3235d188e3006753bb778b7de6266fbcbe4c4ba2ef2f`
- deployed `/models/tokenizer.json` SHA-256:
  `851ca67100d372ca3ae031a6abd168f53489eebfd7d89523f35c5c9b4d372c3c`
- tokenizer truncation: right, maximum total length 256, `LongestFirst`
- tokenizer padding: right, batch-longest

The tokenizer was copied read-only from the running upstream target and loaded
with Hugging Face `tokenizers` 0.22.2. No service or model artifact was changed.

## Confirmed harness gremlin

The existing saturation probe generates a miss payload by repeating `m` and
appending a request suffix. Repetition is not a token-length control. Direct
measurement with the deployed tokenizer produced:

| Repeated character bytes | Tokens including `[CLS]` and `[SEP]` | Interior token shape |
| ---: | ---: | --- |
| 32 `m` | 18 | WordPiece fragments |
| 128 `m` | 3 | `[UNK]` |
| 256 `m` | 3 | `[UNK]` |
| 1,024 `m` | 3 | `[UNK]` |

The same 128/256/1,024-byte collapse occurred for repeated `h`. Therefore the
earlier byte-shape sweep compared a small WordPiece input with three equivalent
unknown-token inputs; it did not measure scaling by model sequence length.

## Controlled corpus construction

`hack/token-payloads` generates distinct contexts using the minimum number of
`alpha`/`bravo` positions needed to encode the requested corpus size, then fills
the rest with `service`. Each word is exactly one content token under the
deployed tokenizer. Two special tokens are included in the requested total.

Every context is tokenized after construction and the tool fails rather than
emitting a mismatched row. Each JSONL row records the actual token count, UTF-8
byte count, and tokenizer digest.

Validation of 2,048 unique rows in every proposed bucket:

| Total tokens | Unique contexts | UTF-8 bytes per context | Verification |
| ---: | ---: | ---: | --- |
| 16 | 2,048 | 89 | exact |
| 32 | 2,048 | 217 | exact |
| 64 | 2,048 | 473 | exact |
| 128 | 2,048 | 985 | exact |
| 256 | 2,048 | 2,009 | exact |

Example:

```sh
./hack/token-payloads \
  --tokenizer /path/from-target/tokenizer.json \
  --tokens 64 \
  --count 2000 > payloads-64.jsonl
```

## Benchmark boundary

The original driver cannot consume an external context corpus; reusing it would
silently restore the invalid repeated-character method. The separate corpus
probe below consumes these JSONL rows without changing the semantic-classifier
service and includes image, model, tokenizer, node, and payload-corpus digests
in the result envelope.

The first controlled matrix should use 16/32/64/128/256 total-token buckets at
concurrency 1/4/8/16, with three repetitions around the first throughput or p99
knee. Token count is the controlled variable; byte count remains recorded but
is not treated as its proxy.

## Focused Arena validation

An external-only corpus probe consumed disjoint slices of the generated JSONL
and sent 100 unique cache misses per cell. The service stayed on the pinned
upstream image throughout. This is reconnaissance, not a stable benchmark:
cells are small, ran once, and ran sequentially.

- driver image: `sha256:161be071693def4c6e555b33f78df62ddfdcf4780411781379a106eaa57337b8`
- target image: `sha256:04323612ce3f73873b4c3ed6e09264e828241537e2c1a4231b43f32e9744d5aa`
- node: `gnr2.fm2aihpcsed.com`
- status: 600/600 OK; target remained ready with zero restarts

Post-run Kubernetes verification matched both requested image references to
their runtime `imageID` digests. Job `upstream-token-corpus-wave-1` reported
`SuccessCriteriaMet=True` and `Complete=True` (`CompletionsReached`). The target
pod was still ready with zero restarts, and a post-run read of its mounted
`tokenizer.json` reproduced SHA-256 `851ca671...72c3c`.

| Tokens | Concurrency | Useful RPS | p50 | p95 | p99 | Max |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 16 | 1 | 3.42 | 273.0 ms | 399.8 ms | 452.1 ms | 489.9 ms |
| 16 | 4 | 7.75 | 442.2 ms | 778.5 ms | 841.4 ms | 862.0 ms |
| 64 | 1 | 3.15 | 305.7 ms | 413.9 ms | 487.7 ms | 495.9 ms |
| 64 | 4 | 8.15 | 468.6 ms | 611.0 ms | 716.3 ms | 761.6 ms |
| 256 | 1 | 2.01 | 472.6 ms | 626.9 ms | 767.2 ms | 803.6 ms |
| 256 | 4 | 4.16 | 946.3 ms | 1,126.7 ms | 1,237.2 ms | 1,245.0 ms |

The controlled result contradicts the byte-repetition sweep's apparent input
length invariance. At concurrency 1, 256 tokens delivered 41.1% less throughput
than 16 tokens and p50 was 73.1% higher. At concurrency 4, 256-token throughput
was 46.3% lower and p50 was 114.0% higher than 16 tokens. The 16-versus-64
ordering varies by metric and needs repetitions; the 256-token cost is already
large enough to define the next focused boundary.

Corpus BLAKE3 digests recorded by the driver were
`0f7db34d0968496d0e4b834e45977032dc787d41ffb47502b6c56a7f6fa35ade`
(16), `dba761da133dbc28bcfec8d8da8fbc8bde30a519f8fc543d95296cd581d357d2`
(64), and `d528e265d8af2c0ad5c48fef8dc7eb02232fc3e49481a550144d6118c70c4f65`
(256). Each result also carried the model and tokenizer SHA-256 digests above.

## Repeated-matrix decision rules

The following rules are fixed before reading the repeated matrix so the knee is
not selected visually after the fact. Calculate each metric across independent
runs of the same token/concurrency cell. Use the median for the central result,
and sample coefficient of variation (`CV = sample standard deviation / mean`)
for useful RPS and p99.

A concurrency is the first **knee** for a token bucket when either:

1. median useful RPS falls at least 10% from the best lower-concurrency cell; or
2. useful RPS improves no more than 10% over the preceding cell while median
   p99 increases at least 25%.

The lower-concurrency cell immediately before the knee is the provisional
usable ceiling. A knee is confirmed only when at least three independent runs
exist on both sides and the direction of both RPS and p99 is the same in at
least two of three paired comparisons. Never average status failures into
latency: report useful throughput, statuses, and successful-request latency
separately.

### Red/yellow/green rubric

| Dimension | Green | Yellow | Red |
| --- | --- | --- | --- |
| Repeat stability | RPS CV <= 10% and p99 CV <= 15% | RPS CV <= 20% and p99 CV <= 25% | Either CV exceeds yellow |
| Request outcome | 100% valid OK; recovery OK | Expected `RESOURCE_EXHAUSTED` <= 1%; recovery OK | Any unexpected status, connect error, failed recovery, or target restart |
| Scale position | Below confirmed knee | At knee, or knee evidence lacks three stable repeats | Beyond confirmed knee |
| Token sensitivity, versus 16 tokens at same concurrency | RPS loss <= 10% and p99 increase <= 25% | RPS loss <= 25% and p99 increase <= 75% | RPS loss > 25% or p99 increase > 75% |
| Provenance | All image/model/tokenizer/corpus/node digests match | Complete but a declared non-functional dimension differs | Missing or mismatched provenance |

Overall cell color is the worst dimension color. This rubric describes evidence
quality and operational risk; it is not an SLO. Promotion requires explicit
latency and throughput SLOs from the service owner in addition to green evidence.
