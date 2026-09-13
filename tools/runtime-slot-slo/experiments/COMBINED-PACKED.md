# D-COMBINED-PACKED: complete physical packs, insufficient header-only remedy

2026-09-12. Evidence: `/tmp/sandbox0-combined-packed.AldIJg`.
Local diagnostic format/range qualification only. No runtime or startup result.

## Decision

Packing the unchanged combined representations reduces encryption-header first
touches but does not remove logical payload dependencies. It also increases
returned ciphertext and introduces three cold provider-range requests in the
fixed schedule. Do not advance the inconsistent combined candidate to runtime
solely for this header reduction, or pay for another unchanged remote sweep.
This is not a universal rejection of authenticated physical packs.

The machine/S3 question remains separate. Prior identical ordinary controls
vary by 2.562s in serialized Reader time, with 2.523s HTTP-union variation.
That is HTTP-boundary variability, not an established S3 defect or an exclusive
service/network/host split. The two-core object probes do not prove healthy
production-width occupancy. See [COMBINED-TRANSPORT.md](COMBINED-TRANSPORT.md),
[HOST-S3.md](HOST-S3.md) and [OSS-SERVICE.md](OSS-SERVICE.md).

## Complete placement, not a hot-object fixture

All original bytes were already available and verified against sealed catalogs.
Visit every original selector record in logical order, data then joint. Keep
each pair together, flush at the existing 64MiB pack limit, and add no padding.
There is no command-trace-based selection, member reordering, object splitting,
missing-object substitution or synthetic filling. Preserve all ordinary and
absent-joint fallbacks. Original logical packet hashes remain independent of
physical pack SHA256/key/offset; the actual old packet bytes do not change.

| Complete image | Data / joint objects | Physical packs | Plaintext bytes |
| --- | ---: | ---: | ---: |
| Node | 61 / 59 | 1 | 7,893,173 |
| Coding | 1,789 / 1,751 | 4 | 248,972,883 |

Coding pack sizes are 66,937,033 / 66,989,546 / 67,089,740 / 47,956,564 bytes.
The five actual pack files contain all 3,660 objects, totaling 256,866,056 bytes.
No remote byte export or S3 write was necessary.

A diagnostic outer parent retains the exact old combined parent bytes and adds
a compressed placement table. The unchanged old decoder still validates the
original parent and current child identities. This avoids another copied mapping
implementation, but explicitly adds metadata; it is not free shared-cache state.

| Image | Old / new stored parent | Old / new decoded parent | Placement stored / decoded |
| --- | ---: | ---: | ---: |
| Node | 7,805 / 8,409 | 12,909 / 14,101 | 492 / 1,080 |
| Coding | 206,609 / 220,557 | 358,903 / 387,863 | 13,836 / 28,848 |

Both fit the unchanged 240KiB stored / 1MiB decoded bounds. These are two
complete measured images, not proof of arbitrary populated-root hierarchy or
size-independent startup. New table accounting/lifetime has NOT been integrated
with the shared Reader; the previous cache matrix does not qualify this change.

## Actual encryption geometry

Use the actual immutable encrypted store with both AES-GCM/RSA and ChaCha20/RSA,
16KiB chunks, header cache 8MiB/1024 entries and prefix/parallel bounds 256KiB.
The RSA2048 fixture key exists only in memory and is not the production key;
local execution time is not used as a latency prediction. Each algorithm writes
the complete five packs, two new parents and 33 accessed standalone objects to
an isolated memory provider with conditional creation. A separate fresh reader
validates all 3,662 packed ranges, including both parents, against original bytes.

Then two NEW encrypted wrappers replay the SAME 49 diagnostic-object source
calls, retaining cold-to-cached-new header state within each variant. These are
25 cold / 24 cached-new calls drawn from the already sealed full 497-call
schedule. The 448 ordinary fallback calls are omitted, not newly executed or
claimed as a full Reader replay. Complete inner parent and packet bytes still
match. All seven packed physical identities are first touched in the cold cohort.

| AES-GCM component | Standalone combined | Packed combined | Difference |
| --- | ---: | ---: | ---: |
| Cold source calls | 25 | 25 | 0 |
| Cold header operations | 25 | 7 | -18 |
| Cold provider ranges | 25 | 28 | +3 |
| Cold returned ciphertext | 1,533,601 | 1,927,937 | +394,336 |
| Cached-new source calls | 24 | 24 | 0 |
| Cached-new header operations | 8 | 0 | -8 |
| Cached-new provider ranges | 24 | 24 | 0 |
| Cached-new returned ciphertext | 1,184,796 | 1,525,572 | +340,776 |

ChaCha20 has identical call/header counts; ciphertext differences are +394,359
and +340,784 bytes. Cold returned plaintext increases 14,552 bytes solely for
the new parents; cached-new plaintext is unchanged. Packet offsets now straddle
cipher chunks within a larger object and no longer end at the standalone EOF.
Three nonzero-offset first touches of Coding packs each need a 1,024-byte header
probe plus a payload range. Count both; do not call them necessarily serial.

These are actual encrypted-store provider-range/returned-byte counts, NOT OSS
HTTP attempts, retries, timing, server cache or network measurements. Fewer
headers do not imply fewer requests, fewer dependent source windows or lower
end-to-end latency. No request timeout, cache budget or concurrency changed.

## Coverage screen and stop condition

Match the 26 removed logical header identities to the two previously measured
candidate arms, preserving exact cohort/read ID/object. Their crypto time
outside all HTTP intervals is 92.104 / 96.259ms cold and 41.880 / 41.383ms
cached-new. With all other costs fixed, even granting those savings for free
leaves the reversed cached-new candidate-minus-ordinary Reader delta at
**+449.487ms**. The positive delta remains despite ignoring the extra bytes,
GETs, parent decoding and new metadata costs. The other pair remains favorable.

This is an optimistic accounting screen, not new achieved speedup, a statistical
regression claim or a universal lower bound. Existing HTTP variation remains.
Header-only placement does not establish the previously required reduction in
mandatory payload-wait exposure. Stop this branch before new shared-cache/
lifetime implementation or remote/runtime admission unless a materially wider
dependency or demonstrated net-cost mechanism supplies a falsifiable case.

## Verification and unchanged requirements

Go race checks pass: 28 malformed placement/parent subtests, both complete
encryption algorithms, and original checksum rejection of wrong-but-in-bounds
offsets, corruption and short reads for both images. The complete qualification
test takes 42.38s locally; that is NOT sandbox startup time. Ruby analysis has
5 tests / 10 assertions covering exact schedule, warm-cache leakage, locators,
provider scope and byte/count totals. All 1,353 frozen product files remain
unchanged. Existing worktree changes are preserved; only experiment notes are
updated in the repository.

No remote/cloud operation, claim, guest command, production deployment, merge
or tag this turn. The last remote state observation remains the previous
05:57:19UTC Stopped/StopCharging receipt, not a fresh cloud query. No material
data was deleted. Durable import/COW/publication/retry/inventory/GC is not
implemented for this diagnostic format.

Generic carriers, claim-time tenant RootFS, stateless workers, cryptography,
checksums, authenticated readiness, writer fencing and cleanup are unchanged.
Current-version regional ingress-to-procd, immediate real `node -v`, empty-node
and cached-new identities, populated-large roots, occupied production width,
and the original 1s target / 2s fallback gates all remain open.
