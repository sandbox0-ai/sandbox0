# D-COMBINED-CACHE: real bounded cache, shared identities and a failed hit guard

2026-09-12. Diagnostic Reader qualification, not a startup improvement or runtime
admission. All [fundamental requirements](STRUCTURED-OPTIMIZATION.md) remain.

## Method and scope

The earlier unlimited-private-cache model is insufficient for node-wide costs.
This campaign reuses the actual Reader, its 128MiB ReadCache, 16MiB maximum
decoded-mapping protection, source admission, grouped mapping reads, ordinary bulk
dispatch and original checksum/length identities. A temporary Go overlay adds a
small-demand hook, eviction counters and opaque selector accounting in the SAME
cache. Ordinary source is unchanged apart from named instrumentation; no product
file or runtime configuration is edited.

The full encoded combined parent and actual parsed-selector capacity are charged
as ordinary unprotected cache entries. There is no second selector cache and no
retained whole-packet payload cache. Only fully validated original members enter
the existing content cache; raw member slices are independently owned so they
cannot retain an uncharged packet backing array. Exact original parent bindings
remain required even for cached content.

Each prototype executes 57,492 real ReadAt calls: original/candidate private-cache
controls plus original/candidate shared-cache batches, all using the same 14,373
historical request shapes. Every output must match its sealed original checksum.
All 16 ordinary private controls must also match their entire original per-request
source range schedules, not merely total counts.

The shared schedule has four Node and four Coding identities per batch. It
serializes recorded request-start order, preserving each stream's order, after
constructing its eight Readers. The first batch starts empty; the second retains
the exact same cache and creates NEW Reader identities. This tests actual cache
sharing/eviction, not historical overlapped completion order, concurrent packet
singleflight, guest dirty branches, runtime timing or occupied production width.
Fixture storage buffers, independently retained Reader roots and in-flight objects
are outside the retained-cache metric; it is NOT a physical RSS bound.

## First prototype: useful evidence, insufficient cache-hit behavior

All output and ordinary schedule comparisons pass. Private Coding source windows
remain 284 ordinary / 269 combined and Node 239 / 228, matching the previous
actual-byte model. Actual Coding cache evictions increase from 2 to 50; Node has
none in either private arm. Evictions alone do not prove repeated source reads.

| Shared batch, first prototype | Ordinary | Combined |
| --- | ---: | ---: |
| Cold source windows | 355 | 330 |
| Cold encoded plaintext source bytes | 53,571,464 | 54,313,214 |
| Cold evictions | 238 | 317 |
| Reused-cache NEW-identity source windows | 192 | 167 |
| Reused-cache source bytes | 20,248,020 | 21,173,840 |
| Reused-cache evictions in that batch | 778 | 876 |

Cold repeated bytes at the SAME physical plaintext key/ranges are 832,997 ordinary
and 827,986 combined. The second batch repeats 20,248,020 and 20,700,293 bytes
respectively; the candidate additionally reads 473,547 bytes from previously
unread physical ranges. That is physical transport geometry, not newly needed
logical content: duplicate content can reside at different pack/group addresses.
Do not turn these counts into encrypted GET counts or serial round trips.

This prototype keeps both selectors resident, charged at 545,348 bytes combined.
It does not cover eviction of a selector while the demanded mapping/data stay
hot. A separate unchanged-128MiB guard establishes that condition with 2,300
synthetic pressure ranges while keeping the ordinary demand hot. Ordinary Reader
needs zero source reads; the candidate incorrectly reads its parent once. The
admission test FAILS and is preserved. Green full-stream checks must not hide it.

## Corrected diagnostic dispatch

Before consulting a selector, walk only already cached canonical mapping bytes
to identify an ordinary hit or hole. Reuse the actual mapping-page flight/parser,
cache accounting and exact parent validation; holding a verified raw page across
eviction follows the existing Reader ownership contract. If mapping bytes are
absent, do not fetch them to discover a hit: the conditional delivery decision
still occurs before an ordinary mapping miss. Bulk dispatch is unchanged.

The same eviction/residency guard now has zero candidate source reads, identical
output and the same 128MiB cache. Original four guard cases also pass: joint then
resident-selector cache hit, corrupted packet with no partial speculative cache
publication and healthy ordinary fallback, no recursive group after ordinary
mapping miss, and selector eviction without a hidden retained copy. Twelve
existing mapping/cache/coalescing regression tests pass under race detection.

Changing hit handling changes selector recency and may change later misses.
Therefore the complete original/candidate private/shared matrix is rerun rather
than carrying forward the first prototype's counts. Both source revisions, the
failed guard and their distinct receipts remain preserved.

## Corrected full replay result and next decision

The corrected matrix completes all 57,492 ReadAt/output comparisons and all
16 original private source-schedule checks under race detection. Its complete
32 private rows and four shared rows (including calls, packet activations and
cache counters) equal the first prototype's results exactly. Both results are
retained, not inferred from the targeted fix. The fixed trace schedule did not
exercise the now-corrected selector-eviction/hot-data failure; other interleavings
and end-of-run internal LRU order are not proved equivalent by these counters.

| Corrected shared candidate minus ordinary | Cold | Reused-cache NEW identities |
| --- | ---: | ---: |
| Source windows | -25 | -25 |
| Encoded plaintext source bytes | +741,750 | +925,820 |
| Evictions within the batch | +79 | +98 |
| Repeated bytes at prior physical key/ranges | -5,011 | +452,273 |
| Node source-window / byte delta | -11 / +181,802 | -11 / +166,902 |
| Coding source-window / byte delta | -14 / +559,948 | -14 / +758,918 |

Cold candidate dispatch activates 16 data-only groups and seven joint packets;
the second batch activates 24 data-only groups and no joint packets. Combined
packet verification decodes 11,839,264 / 11,976,656 bytes including manifests,
of which 1,966,080 / 2,818,048 original payload bytes were already cached. These
are actual diagnostic verify-all costs, not a cache-aware CPU forecast. Mapping
parse counts reach 38 for both layouts and remain 38 in the second batch.
Two selectors are built in the cold batch and none in the second. Peak accounted
cache bytes remain below 134,217,728 in all arms. Higher evictions did not cancel
the window reduction in this schedule; additional bytes/decode remain real costs.

This completes the previously missing bounded shared-cache prerequisite for the
current two-image diagnostic. Stop repeating ideal/shared-cache sweeps. The next
gate is matched actual encrypted-source plus decode/CPU cost with these exact
requests, original bytes, fixed cache/admission settings and corrected dispatch.
It must determine whether reduced source windows outweigh parent/packet transport,
integrity work, allocation and re-read costs for BOTH images. Component tests may
reject the candidate; they cannot establish achieved ingress-to-procd/command SLO.
No full runtime trial is admitted on window counts alone, and durable format and
current remote-runtime-readiness prerequisites still apply before such a trial.

## Corrections, records and remaining gates

The first unit attempt failed at compilation due to an unused import left in
extracted fixture helpers. Zero tests ran in that attempt; its exact source and
receipt are preserved. A later multi-file patch had a nonmatching gofmt-expanded
anchor and was verified not to have partially applied before correction. Neither
was silently retried as a successful run. The interval-accounting analyzer has
three passing tests / 404 assertions, including an independent per-byte oracle.

Final source checks reverify 1,353 frozen product files, 164 trace files, 8,202
indexed predecessors, the preceding 22 delivery artifacts and all 1,961 actual
byte artifacts. Main refs remain sandbox0 `0f092204` and infrastructure
`ceb895c6`; all 193 preexisting worktree entries remain. Reversing only the named
hook/opaque-entry/counter instrumentation reproduces the original Reader exactly
after gofmt. This is stronger than assuming a copied cache implementation agrees.

No cloud query/start, new original-object export/import, claim, guest command,
production operation, rollout, merge or tag this turn. The remote's previous
Stopped/StopCharging and warm-ready=0 observations are not freshly queried here.
Before a future runtime trial, establish a healthy current fixture; a byte/cache
diagnostic cannot repair or waive that prerequisite.

This campaign does not establish encrypted transport latency, net decode/CPU
cost, physical retained/RSS bounds under contention, arbitrary populated-large
RootFS behavior, durable import/COW/publication/inventory/GC, or actual regional
ingress-to-procd plus first-command performance. The full 1s target / accepted2s
gate and all empty-node/cached-new/occupied-density requirements remain open.

Evidence root: `/tmp/sandbox0-combined-cache.4hue2k`. First prototype files are
`replay.json`, `analysis.json`, `summary.json`, `selector-hit-audit.json` and
their receipts; exact old source is retained as `v1-*.go`. Corrected files use
`replay-v2.json`, `analysis-v2.json`, `summary-v2.json` and
`selector-hit-audit-v2.json`. `input.json` fixes the complete deterministic schedule.

All 47 artifacts sealed; all owned handles terminal. Evidence SHA256:
`76e5fa5797b2c3a764c624a08322e70307547538ffa73cd9101bede92554e16e`.
Artifact-index SHA256:
`b7f9303e1f010286c1c685463047099f6dde74f8484988027b9fbc613f8b17dc`.

Selector memo reuse here is qualified against pinned original/wrapped-root pairs.
Do not infer complete new-format authority or lifetime correctness: independent
mismatched-parent cache-hit guards, concurrent candidate metadata/packet behavior
and cancellation remain admission tests before product/runtime use.
