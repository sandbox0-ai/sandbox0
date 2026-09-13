# Full-stream qualification of leaf-miss joint delivery

2026-09-12. Diagnostic reanalysis and actual original Reader tests, not a new
sandbox startup run. Previous goal turn was progress; this turn changes the next
action by stopping current joint-packet standalone admission and identifying a
narrower missing transport observation. The full goal remains active.

## Decision

Do not prioritize the current leaf-miss-only joint packets as the standalone
1s/2s solution. The complete retained Node streams never activate them. Coding
activates seven packets, but only four changed requests need no subsequent source
transfer; other requests still require packet or ordinary mapping delivery.
Eliminating all old waits on every changed request is not a realistic savings
claim for this mechanism.

Stop this packet branch's runtime admission here. Retain its verified byte/codec
work, but do not build a durable format, add parameter sweeps or start a full
remote candidate rollout to chase a small helper gain. A new premise would be
needed to reopen it.

## Authoritative evidence and validation

- Fetched/read current sandbox0 and infrastructure main; unchanged refs
  `0f09220460581bfc1fdc331f34ebc85bf38381e7` and
  `ceb895c6225b9b01f8535b721d1156fa58fe4cd9`.
- Reverified all 1,353 frozen product files/inventory, 164 trace artifacts,
  7,650 preceding real-byte artifacts, 13 frame-group artifacts and 12 critical
  budget artifacts. Preserve the worktree's 193 existing dirty/untracked entries.
- Reproduced eight preceding helper families (ordinary/leaf-miss-only for both
  images and both workloads), including their exact modeled event sequences.
- Replayed all 14,373 retained requests from 16 exact sandbox identities against
  the current original Reader and real exported immutable source bytes. Each
  stream starts with a fresh PRIVATE cache at the unchanged 128MiB budget.
- Every request's exact original source key/offset/length sequence matches the
  corrected ordinary model, not just aggregate counts. All 1,784 distinct
  image/offset/length output identities remain identical across occurrences.
  Missing fixtures or speculative source failures would fail the test; none did.
- Actual cache peak: Node 109,008,107 bytes; Coding 134,155,105 bytes, below the
  fixed 134,217,728-byte limit. This is cache accounting, NOT process RSS, a shared
  mixed-image cache, or occupied-node memory acceptance. Candidate eviction is
  not tested. Historical `cached-new` labels identify the input trace cohort,
  not the fresh private cache used by this diagnostic replay.
- One Go race test covering all16 streams passes; nine Ruby model tests with26
  assertions pass. No local e2e. A verifier initially used `Entry.object` instead
  of the exported Go `Entry.Object` field; that error is retained, corrected and
  only the read-only verifier rerun. The Reader/source export was not repeated.

No product source change, cloud query/start, new claim/command, object mutation,
deployment, production operation, merge or tag occurred. A Go overlay exposes
locked cache counters without modifying product files. Last known remote state
is the previous turn's 2026-09-11T22:17:29.455355000Z Stopped/StopCharging; it was
not freshly queried this turn.

## Complete-path source-shape result

All four samples per image have the following source-shape totals; different
request counts/order are still independently verified. These candidate totals
are modeled against immutable base bytes, with ideal retention and serialized
reads. They are not source GETs under the real shared runtime, HTTP round trips,
elapsed times or startup predictions.

| Full immutable stream | Ordinary | Leaf-miss-only candidate |
| --- | ---: | ---: |
| Node source events | 239 | 239 |
| Node joint packets | 0 | 0 |
| Node extra encoded bytes | — | +4,812 |
| Coding source events | 284 | 275 |
| Coding joint packets | 0 | 7 |
| Coding encoded bytes | 47,683,268 | 47,795,524 |
| Coding decoded charge | 123,996,429 | 124,733,960 |
| Coding ideal data retention | 110,759,936 | 112,259,072 |

Node's real access order has already fetched/cached the relevant mapping leaves
before the candidate's small-read triggers. The earlier isolated Node metadata
helper benefit does not transfer to its complete startup stream.

Coding has14 changed reads in a representative cold stream: seven replacement
packets, four ideal no-source requests, and three requests still using ordinary
sources. Avoiding grouped sibling fetches early can create later ordinary mapping
misses. Those costs are retained rather than silently deleted from the model.

## Why the optimistic aggregate is misleading

Observed cold Coding claim-to-first-command completion is 2,665.827–2,665.969ms
in this historical instrumented fixture. Old waits on ALL changed requests cover
666.710–681.001ms. Simply subtracting them leaves 1,984.968–1,999.118ms, but assumes
that even replacement packet requests and remaining ordinary transfers are free.
Zeroing the entire root mapping operation as well makes this look better still;
the actual candidate instead has a much larger initial parent.

A more mechanism-specific, still exceptionally generous screen:

1. Give replacement packets zero new cost and remove their entire old wait.
2. Give ideal no-source requests zero old wait.
3. For other changed requests, retain old mapping waits if ordinary mapping
   transfer remains and retain payload waits if ordinary data transfer remains.
4. Keep root operation duration unchanged despite the larger candidate parent.
5. Union intervals per exact sandbox; do not multiply leaders by their followers.

| Cold Coding fixed-other-cost screen | Combined remaining | Claim remaining |
| --- | ---: | ---: |
| Free new packets, retained ordinary stages | 2,017.337–2,022.497ms | 1,218.408–1,236.513ms |
| Even entire replaced NBD reads cost zero | 2,012.938–2,013.898ms | Not used as a readiness claim |

This is a prioritization counterfactual, NOT a universal lower bound or a future
latency guarantee. The trace lacks the full guest dependency DAG, and changing
ordering/contention can affect other costs. It nevertheless does not justify
this mechanism as the independent way to meet the unchanged gate. Real new
packet I/O, extra parent bytes and candidate cache/CPU costs have not even been
charged in the favorable screen. Node has no delivery benefit and its historical
instrumented combined samples remain about 2,017.2ms unchanged.

## One exact owned slow read changes the next question

The representative changed Coding claim read1103 waits55.366ms for mapping and
116.689ms for payload. Both shared flights are owned by read1102 in another
sandbox. Never count these follower waits as additional physical source cost.

The owner performs:

| Owned source | Encoded requested content | Source duration |
| --- | ---: | ---: |
| Original grouped mapping | 175,438 bytes | 53.527ms |
| Original full data frame | 5,490 bytes | 116.067ms |

For the data frame, its header provider call requests1,024 cipher bytes and takes
74.771ms until response-body acquisition. An independent ciphertext-range call
requests17,401 bytes and takes115.950ms. They overlap74.718ms: the data request is
ALREADY parallel with header loading. In this example the data call determines
completion; dropping header waiting alone would not erase116ms.

These provider spans do not contain DNS/TCP/TLS/connection-pool reuse, SDK retry
attempts, first-byte, or complete body delivery timings. They cannot establish
that networking, credentials, server service time or CPU is the specific cause.
The example is chosen by largest old wait among candidate packet activations,
not asserted to represent every request.

## Next evidence, with unchanged fundamentals

Use a bounded READ-ONLY remote probe on exact original ranges to attribute each
HTTP attempt: connection reuse/establishment, DNS/TLS, request sent, first byte,
body completion and retries. Separate object-header cache state from transport
connection state. Keep original software/hardware, encryption and concurrency
limits; do not increase timeout, cache or width, disable checks, or prewarm a
tenant RootFS as part of generic carrier readiness. This probe diagnoses source
cost and cannot qualify startup latency or occupied density.

Prefer the already verified exported bytes/locators and frozen source identities;
no new import, scan, guessed object inventory or production rollout is needed.
Only a demonstrated transport/runtime cause or materially wider dependency
coverage should select the next implementation. All regional ingress/procd
readiness, immediate real command, empty-node/cached-new, populated-large,
occupied actual width, isolation, durability and cleanup gates remain open.

Evidence: `/tmp/sandbox0-joint-critical.GZLjTa`, especially `analysis.json`,
`actual-reader.json`, `full-verification.json`, `qualification.json` and
`owned-provider.json`.
