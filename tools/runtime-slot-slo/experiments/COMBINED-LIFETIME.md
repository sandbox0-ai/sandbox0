# D-COMBINED-LIFETIME: bounded shared work and cancellation

2026-09-12. Evidence root: `/tmp/sandbox0-combined-lifetime.H5k5Kw`.
Local diagnostic-prototype qualification only. No product/runtime change,
remote operation, new startup sample or claimed latency improvement.

## Why this step follows the machine/S3 observation

D-OSS-SERVICE observes substantial HTTP coverage and actual OSS processing
times on an otherwise lightly loaded two-core fixture. It neither establishes
an OSS defect nor exonerates hardware at occupied production width. D-OSS-ID
closes the one-request client/provider correlation prerequisite. Existing
dependency traces already justify evaluating fewer dependent remote reads;
another baseline-only machine/cache/timeout sweep is not the next step.

Before measuring the combined-delivery candidate's net encrypted cost, it must
not turn cancellation into extra reads or retain abandoned work without bounds.
Its preceding selector helper uses background contexts; packet singleflight
also shares the leader's verification without a complete recipient contract.

## Reproduction and changes

The negative-control adapter presents a context-aware API while deliberately
retaining the sealed predecessor's behavior. An already-canceled selector
request still performs one GET, one parse and one publication and returns nil.
`before.json` preserves this failure. The identical test source passes with
zero GET/parse/publication in the corrected helper. This is not a newly
discovered deployed-product vulnerability: the old helper was diagnostic only.

The revised temporary overlay retains the actual ReadCache and its original
128MiB total/16MiB protected mapping budgets. It adds bounded in-flight
coordination, not a second retained selector, payload or crypto cache:

- At most eight distinct diagnostic loaders; every actual source read still
  uses the SAME existing eight-slot admission as ordinary Readers. The new
  coordination is not another eight slots of I/O capacity.
- Each caller can stop waiting independently. The shared load is canceled
  when its final recipient leaves; a fixed 10s load deadline also bounds waits.
  It never extends the existing request timeout. An uncooperative canceled
  loader stays counted, and a same-key replacement cannot overlap it.
- Context reaches source admission, context-aware transport and original-member
  decode. A source slot remains held through verification and publication, not
  merely until the body closes. All packet checks complete before a bounded,
  no-I/O commit, serialized against the last waiter's departure.
- A packet flight includes the complete expected original-parent, leaf,
  selector/trigger and delivery contract. A follower cannot inherit success
  from a verifier expecting different content. Existing per-return selector
  parent checks remain; physical key/offset are not content authority.
- Device cancellation/deadline is returned, never interpreted as permission
  for ordinary fallback. Unavailable/corrupt/truncated speculative packets
  still fall back to the unchanged authenticated ordinary Reader path.
- Diagnostic counters and fixture call appends are synchronized without
  serializing transport. Shared physical work is counted once at publication;
  those counters are not per-recipient metering or a tenant fairness contract.

The lifetime belongs to a device/node, not the short claim HTTP request. Only
the new diagnostic selector/packet flights gain independent waits. Ordinary
Reader singleflight still has its explicitly documented initiating-Reader
transport lifetime. Neither this overlay nor passing tests upgrades that
ordinary contract or makes an uncooperative provider preemptible.

## Validation

Race tests use deterministic synchronization/virtual time, not a real 10s
sleep or scheduler timing assumptions. Verified original Node and Coding
bytes are reused; synthetic coordination fixtures are labeled separately.

- Seven flight/helper tests, including leader/follower cancellation, final
  recipient abandonment, no overlapping replacement, fixed deadline, source
  slot retention, existing ordinary capacity and different recipient bindings.
- Six real-fixture tests cover both-image constructor sharing, shared Node
  reads, no cancellation fallback/publication, later new-reader recovery,
  concurrent wrong-parent rejection, truncated-packet fallback, and eight
  simultaneous independently verified packet reads for EACH image.
- Existing five parent-binding/fallback tests continue to pass. Ordinary
  lifetime/admission/cache tests and the original 128MiB resident-data/evicted-
  selector guard are also checked separately.

Review corrected one test assertion before its final expanded run: an initial
no-publication test probed a leaf with a synthetic length of 1, which could not
detect a real cached leaf. It now checks the exact current child's checksum AND
length. The original test/receipt remains; do not count its old leaf assertion
as evidence. Its trigger, cancellation and no-fallback assertions were valid.

The new complete fixed-stream matrix passes under race: 57,492 exact ReadAt
checks, covering 14,373 input shapes in ordinary/candidate private/shared arms.
All ordinary private/shared result rows match the predecessor exactly. The
same 128MiB/16MiB budgets hold, all hashes match, and no candidate packet fails.
This newly validates the changed parent charges and publication, rather than
carrying forward an old matrix as evidence for different code.

| Serialized shared-cache cohort | Ordinary source windows | Candidate windows | Extra encoded source bytes | Extra evictions |
| --- | ---: | ---: | ---: | ---: |
| Initially empty cache | 355 | 330 | 741,750 | 79 |
| Cached-new identities | 192 | 167 | 925,820 | 98 |

Window counts and byte deltas remain the same as the prior prototype. Retained
selector entries now charge 545,594 bytes, including the parent bindings,
246 bytes more than the earlier unbound selectors. The resident-data guard
still issues zero speculative reads after selector eviction at the SAME cache
budget. Private streams retain Node 239→228 and Coding 284→269 source windows.
These are encoded-plaintext source requests, not counted HTTP attempts or
serial critical-path round trips. Extra bytes/decodes/evictions remain a cost.
The race suite's 319.11s execution time is not a cold-start measurement.

## Limits and next gate

Eight local concurrent reads are not eight occupied sandbox runtimes. There
is no current ingress-to-procd/real `node -v` result, populated-large-root
acceptance, production-version parity, RSS under reclaim, durable format
import/COW/GC implementation or proof of a universal size-independent 2s bound.
Packet temporary memory is bounded per loader but is not measured node RSS.

After the exact complete-stream matrix validates, evaluate actual encrypted
source, transfer, decode/CPU and memory cost for both images at the unchanged
cache/admission budget. Reject net regressions; fewer GETs alone do not admit
the candidate. Reuse existing dependency evidence, not another tracing sweep.
Generic warm carriers, claim-time RootFS, crypto/checksums, readiness/fencing,
durability, 10s timeout and the original 1s target/2s fallback remain unchanged.
