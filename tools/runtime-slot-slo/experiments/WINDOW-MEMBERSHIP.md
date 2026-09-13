# D-WINDOW-MEMBERSHIP: qualify complete-unit grouping before remote adoption

2026-09-13. Evidence: `/tmp/sandbox0-window-membership.OvxY7g/evidence.json`.
All candidates are temporary Go overlays. No product default, remote runtime,
cloud resource, RootFS object or public SLO changes in this experiment.

## Decision

Reject the first start-bucket candidate: fewer source calls conceal a 29.17%
increase in complete mixed-root source bytes. Do not adopt the second
homogeneous-run candidate: full-image geometry loses many original grouping
opportunities, and per-demand backward scans grow with authenticated leaf size.

The third, indexed mixed-unit candidate passes local integrity, complete-file
mechanism, static full-image geometry and bounded CPU/memory qualification.
Retain it for an actual encrypted full-demand remote comparison, not as a
validated startup optimization or the new product default.

The immediately preceding user-question turn explained prefetch triggers and
made no new goal progress. This continuation adds a candidate implementation,
new falsifiable tests and completed evidence that changes the next action.

## Trigger and state boundaries

Claim must first identify/bind the RootFS. Only an actual Reader demand and
data-cache miss can fetch payload ranges. Eligible bulk demands remain at least
128KiB; speculative coalescing remains at most 1MiB of decoded/stored units.
A 4KiB demand does not trigger this bulk expansion. A speculative neighboring
unit may never be consumed; full-workload cost must account for it.

The indexed candidate derives greedy complete-unit groups once after a mapping
page has passed checksum/decode/Validate. Groups may contain differing unit
sizes, but stop at partial views, coarse units, holes, object boundaries,
noncontiguous physical offsets and leaf boundaries. No neighboring mapping
page is fetched to form a group, and no future file/command identity is used.

A private uint32 group-end vector is stored with the immutable decoded Reader
page, never serialized. Its capacity is charged inside the existing mapping
cache budget. Binary lookup replaces per-demand backward scanning.
This is derived metadata, not a second RootFS authority or a separate cache.
Keep cache 128MiB, source admission 8, window 1MiB and public timeout 10s.

## Full-file mechanism costs

Eight Readers share one fresh node-style cache over two equal-content 2MiB
files with different logical placements. Every 128KiB step is completed under
a per-step source barrier, then the entire file cost is counted. These are
in-memory source/Reader tests, not actual S3, live NBD or sandbox claims.

For unequal window placements (12288 and 110592 bytes), all three repetitions
produce the following complete-file results:

| Read planner | Source calls | Raw source bytes | Duplicate decoded bytes |
| --- | ---: | ---: | ---: |
| Frozen baseline | 7 | 3145728 | 1048576 |
| Start-bucket candidate, rejected | 4 | 4063232 | 1966080 |
| Homogeneous run-relative | 2 | 2097152 | 0 |
| Indexed mixed-unit | 2 | 2097152 | 0 |

The highly compressible stratum similarly changes stored bytes from 720 to
930, then 480 for the latter two candidates. Compression ratio is synthetic,
not representative of a full real RootFS. All returned bytes are verified.

Same/equivalent-placement full files change from four calls to two, with the
same total 2MiB raw bytes. Their initial envelope nevertheless increases from
983040 to 1048576 bytes: an early read that never consumes the extra unit can
still lose. Do not omit this 64KiB speculative cost.

All 4KiB cells remain one data-source call (65536 raw bytes or 15 compressed
bytes in these fixtures). A fresh Reader over the same generation/cache adds
zero source calls. This is NOT a cached-node NEW sandbox identity.

The same 16x64KiB boundary fixture previously needed two calls after a 4KiB
placement shift. The indexed candidate needs one, with unchanged verified
total source bytes: 1048576 raw or 65761 mixed raw/zstd.

## Exact ordinary-image static geometry

Profiles are the hash-bound original format2 Node and Coding artifacts, not
retired inline/combined layouts. Every view and leaf validates. Hypothetical
128KiB demands inspect all entries, not merely a hand-picked file.

| Image | Total views | Baseline eligible | Homogeneous eligible | Indexed eligible | Indexed gains / losses versus baseline |
| --- | ---: | ---: | ---: | ---: | ---: |
| Node | 4341 | 4149 | 3756 | 4341 | 192 / 0 |
| Coding | 80781 | 77546 | 71126 | 80746 | 3209 / 9 |

The homogeneous candidate loses 572 Node and 9491 Coding previously eligible
entries. Allowing mixed units removes most of that regression. The remaining
nine Coding losses are retained, not declared harmless without demand-level
evidence. Static eligibility is not a GET count, cache result or latency.
All inspected groups obey the original 1MiB stored/decoded bound.

## Planner CPU and index memory

Local Linux/arm64, Go1.25.5, 100 iterations per cell, three repetitions.
These are isolated planning/preparation benchmarks, not startup measurements;
remote x86 timing has not been measured.

At the 65536-entry admitted maximum, lookup near the end of a homogeneous leaf
costs 73.44–120.3ns with the index, versus 1.047–1.259ms for the rejected
per-demand backward-scan candidate. The frozen original planner costs
331.2–701.6ns at that position. All lookups allocate zero bytes.

The new work is not free: constructing a 65536-entry index costs 2.05–2.17ms
and 16KiB for 64KiB grouped units, or 3.20–3.79ms and 256KiB for singleton
groups. It occurs once per actual mapping decode, not once forever; eviction
can cause a later rebuild. At 1024 grouped entries preparation costs
36.8–90.5us and 256 bytes. All preparation cells allocate once.

Cache-charge tests compare otherwise identical pages with/without the index
and assert the exact added capacity charge. Constructor-only tests assert no
payload source reads. Small/disabled caches construct no speculative index;
their planner behavior matches the frozen original implementation.

## Verification and retained failures

- Indexed mechanism: 16 cells x3 race repetitions, 48 complete data records and
  54 passing test events including parents; zero failures/skips.
- Indexed geometry/invariants: 14 passing events, zero failures/skips. Cover
  mixed raw/zstd lengths, partial views, gaps, physical/object boundaries,
  complete greedy groups, wire-byte identity and exact cache charging.
- Targeted existing regression x3 race: 1314 passing events, zero failures/skips.
- Full rootfsblock package with race x1: 932 passing events, zero failures,
  two explicit skips: the opt-in sixteen-million-extent model and isolated
  RustFS integration. No local e2e or populated-image acceptance.
- `go vet` with the same indexed overlay passes.
- All 1361 current product files and 19 preceding sealed artifacts verify
  unchanged. All test-command inputs remain hash-verifiable.

Retain all first failures and their separate corrected receipts:

1. Earlier baseline-copy extra EOF newline and missing-overlay launcher failed
   before tests. The frozen baseline is byte-identical after correction.
2. Homogeneous mechanism initially asserted exactly two source calls although
   the candidate made one; separate v2 preserves the full workload and binds
   actual measured source/permit counts.
3. Indexed boundary assertion used the unindexed input fixture after a correct
   indexed Reader read. Separate test uses the actual authenticated Reader page.
4. New mixed-unit fixture wrongly encoded 96/124KiB zstd units beyond the
   admitted 64KiB limit. v2 keeps these raw; runtime validation is unchanged.
5. Four existing planner fixtures mutate a copied decoded page and validate it.
   Their derived index must be rebuilt after mutation. The test-only overlay
   does so while keeping all expected two-entry boundary assertions unchanged.
   Actual private Reader pages remain immutable.
6. Two preparation patch attempts failed before executing tests: a nonunique
   source anchor, and a rejected same-target Delete+Add patch. Both are logged.

The failed receipts are not passing populations. Earlier candidates, failed
test sources, original assertions and corrected sources are all retained.

## Next gate, not completion

Use the indexed candidate and exact frozen baseline on the same retained
ordinary-image ReadAt schedules and encrypted S3 objects. Fresh process/caches
per arm must include mapping decode/index, encryption header, total payload,
HTTP and CPU costs. Do not require the candidate to reproduce old source
ranges; require the same logical output hashes and bounded authenticated reads.

A serialized retained-demand replay can qualify source cost, not concurrency,
density or startup. Actual regional-ingress claim/readiness, immediate literal
node -v, command-only and combined latency must still be measured separately,
including cold-node and cached-node NEW sandbox cohorts.

The Node mechanism does not solve Coding's dominant remaining gap by itself.
Populated/history-bearing large roots, occupied real production width, current
runsc pin and original 1s/2s gates remain open. No timeout or contract relaxation,
production rollout, merge or tag. No remote/cloud action here; the last remote
Stopped/StopCharging receipt remains historical, not freshly re-observed.
