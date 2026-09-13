# D-LAYER-DEMAND: locate the wrapper's second read expansion

2026-09-12. Evidence: `/tmp/sandbox0-layer-demand.eZhQVJ`.
One new native Node-wrapper workload, not a new regional/S3 startup result.

## Outcome

The retained wrapper's extra executable bytes arise below the inner file demand:
the buffered loop reads the outer XFS `lower.erofs` file, whose own readahead
fetches additional data from NBD. This is evidence about the **unchosen wrapper
candidate**, not a claim that production main uses this nested layout or that
the original end-to-end cold-start problem is solved.

| Attach through real node-v completion | Unique bytes | MiB |
| --- | ---: | ---: |
| Known inner requests: loop completions/XFS read-entry union | 91,402,240 | 87.168 |
| Inner requested bytes represented by outer sparse holes | 1,183,744 | 1.129 |
| Outer NBD reads projected onto the embedded image file | 149,987,328 | 143.039 |
| Outer-file bytes outside all known inner request ranges | 59,768,832 | **57.000** |

All 57MiB extra bytes are covered by NBD BIOs carrying the readahead flag, issued
from kernel worker context. The trace separately records 226 outer-file
`iomap_readahead` callbacks. The callbacks do not expose offsets, so callback
page counts are not summed into unique I/O or treated as per-BIO parent IDs.
It is the exact request-range difference, not the flag alone, that establishes
extra reads relative to this workload prefix.

Both per-file inner demand and outer bytes reproduce the prior attribution:

| File body | Inner backed demand bytes | Ordinary control bytes | Outer NBD bytes |
| --- | ---: | ---: | ---: |
| procd | 18,874,368 | 18,874,368 | 24,457,216 |
| Node executable | 65,314,816 | 65,314,816 | 102,494,208 |

The backed file-offset sets are exactly equal to the sealed ordinary control,
not merely equal totals. Inner EROFS requests also include zero regions served
from outer sparse holes; they are not missing downloads. No current requested
backed byte is absent from the full-prefix NBD projection.

The command phase has 103 observed loop issues and 103 matching completions,
71,954,432 unique requested bytes, and exactly the same XFS buffered-entry range
union. NBD reads projected onto the image file total 122,200,064 unique bytes
in that phase; 51,429,376 bytes (49.047MiB) lie
outside its known inner requests and are covered by readahead BIOs. Phase-specific
sets can overlap, and a page used in one phase may have been read earlier; do
not add these per-phase differences as a global total.

## Fixed setup and actual device state

Reuse the exact complete Node wrapper from D-WRAPPER-DEMAND and its canonical
logical-byte identity, unchanged before/after. No OCI export, image construction,
EROFS rebuild, full-tree scan or encrypted object publication. Source staging is
mounted readonly/noload in a private mount namespace. Work, COW journal, mounts
and output live under a fresh separate diagnostic directory on `/data`.

The existing real Branch/NBD, stock runsc, authenticated procd readiness and
immediate successful `node -v` are used with a new runtime identity. No manager
claim, ctld policy/lease operation or real generation is published. All existing
request/readiness deadlines, NBD geometry, cache and readahead settings remain.
The generic file-backed ReaderAt still does not use the candidate-only concrete
Reader clean-span path; no encrypted Reader or S3 request exists in this run.

Actual kernel 6.8.0-124, two CPUs, NBD 43:63 outside ctld's nbd0..15 pool, loop 7:0.
The loop reports DIO=0, offset/sizelimit 0, 512-byte logical blocks and 4096KiB
readahead; NBD also remains 4096KiB. The exact backing file is inode 132 on 43:63,
size 249,196,544. The trace contains 22,447 XFS buffered-read entries, all 4096 bytes,
and no XFS direct-read entry. These are observed states, not inferred defaults.

The readonly staging disk changed from the preceding boot's `/dev/nvme1n1` to
`/dev/nvme0n1`. It is selected by exact cloud disk serial, 64GiB size, ext4 UUID
and detached state, never by the prior device name. No disk is formatted.

## Observation completeness and retained analysis failures

Use one new tracefs instance filtered to the exact NBD/loop device identities,
the outer XFS inode, and inner/outer iomap reads. Record BIO queue, request issue,
request completion, XFS buffered/direct read and iomap readahead/readpage events.
Separate mono-clock phase markers align the workload without treating Go-local
timestamps as raw kernel timestamps. Do not change global trace options.

All 26,416 trace events are present: header and both per-CPU entry counts match;
overrun, commit-overrun and dropped-event counters are zero. All 1,388 NBD read
issues have later matching range completions, no errors, and their prefix union
equals the Go NBD observer exactly: 150,721,024 bytes with zero difference in either
direction. BIO/request unions also match at each device boundary.

Preserve the limitations instead of creating another workload:

- Initial trace discovery rejects two available paths. Inspection proves both
  paths have the same tracefs device/inode; qualified discovery deduplicates that
  identity, not arbitrary roots. The failed receipt and source remain.
- The exact loop filter is registered after losetup returns and before EROFS
  mounts. Udev has already issued setup probes. 45 early completions totaling
  1,093,632 bytes appear without their issue events, before the first captured loop
  issue. Full-prefix loop issue/completion equality is therefore false. All 131
  recorded loop issues do have later matching completions; no command-phase gap.
- The first offline analyzer correctly rejects that false full-prefix equality.
  A second attempted timeless multiset match incorrectly pairs an early
  offset-zero completion with a future same-range issue. Keep both rejected
  source versions. The final temporal pending-queue model never lets a future
  issue explain an earlier completion, and tests cover this counterexample.
- Extra-byte accounting uses the union of loop BIOs, issues, completions and XFS
  read entries. Completed loop requests and XFS entries independently produce
  the same 91,402,240-byte unique union, including 552,960 additional unique setup
  bytes omitted from the issue-only view. This prevents that known gap from
  inflating the extra-byte conclusion. Unknown completely unobserved setup work
  is not claimed as a fully traced causal graph.

XFS trace entries report requested ranges, not individual read-return values.
Kernel completion errors, actual NBD returned bytes, immutable base identity and
the real command result are checked separately. A readahead flag does not mean
all bytes in that request are unnecessary; here "extra" means outside every
known inner read in this prefix, not never needed by a future command.

## Source interpretation and next decision

Upstream [loop I/O paths](https://github.com/torvalds/linux/blob/v6.8/drivers/block/loop.c)
distinguish buffered file reads from direct IOCB_DIRECT reads. The observed
loop DIO=0 and XFS buffered entries now qualify the previously unobserved mode.
[iomap buffered reads](https://github.com/torvalds/linux/blob/v6.8/fs/iomap/buffered-io.c)
tag BIOs produced by readahead with REQ_RAHEAD;
[block trace formatting](https://github.com/torvalds/linux/blob/v6.8/kernel/trace/blktrace.c)
renders that flag as A. The running event-format files are also retained.
The web fetch of the formatting source failed; direct retrieval from the same
official tagged source confirmed the flag mapping. No secondary interpretation
is needed for the measured flags.

This is sufficient evidence to test a **structural bypass of the intermediary
buffered image-file path**, with actual DIO mode and alignment checked, rather
than another sweep of global/NBD readahead sizes. It is not yet evidence that
requesting direct I/O works for the exact backing geometry, reduces actual net
cost, or is a production-ready fix. Keep useful per-file inner readahead and
all encryption, checksums, COW/writer fencing, absence and RootFS semantics.
Use a separate causal treatment/control with exact immutable inputs; do not
rebuild images or change timeouts to get a favorable number.

Removing this candidate's extra I/O would only reopen its demand screen. It must
then demonstrate net encrypted/S3 cold and cached-new benefit; returning to
ordinary read volume alone does not justify a new format. Sparse/hardlink,
complete rebase Apply/health and authenticated worker/image/terminal contracts
remain unresolved. The independent measured OSS response tail and unqualified
occupied-machine costs remain relevant and are not replaced by this local trace.

## Machine and object-store attribution remain separate

The earlier [request-ID-correlated measurement](CORRELATED-READER.md) matched
581 client requests to exact OSS records. Cold client post-write wait was
P95 34.585ms / max 97.942ms; OSS response was P95 36ms / max 98ms. Of 121 client
waits at least 20ms, 116 also had OSS response at least 20ms. TCP smoothed RTT
P95 was 0.801ms. This is evidence of material object-response waiting, not proof
of an OSS defect or that all waiting is server processing.

That host sample was approximately 80% idle without observed steal, throttling
or OOM. It does not clear the machine under occupied production density.
This new file-backed trace independently locates added reads before any S3
transport. Read expansion and object-response/host costs can compound; neither
measurement quantifies their combined regional critical-path effect. Keep both
attribution tracks open without using a larger machine or cache-warmed result
as evidence that the original no-cache requirement is met.

## Verification and closure

Seven diagnostic Go race tests, vet and cross-build pass; four offline tests /
eleven assertions pass. 27 remote reports transfer with matching size/SHA.
The one workload, content checks and its supervision unit succeed. All guest,
loop, XFS/OverlayFS, NBD and COW resources close; the private trace instance is
removed. The global trace state is identical before/after. Final process mount
tables show no owned mounts and the original runtime files/PIDs/rows/Nomad job
are unchanged within this boot. This successful readonly-source teardown does
not fix or relabel the preceding runner's EBUSY failure.

SSH closes and its exact master exits. The same Makefile stop call completes;
a fresh 11:31:31UTC cloud read confirms Stopped/StopCharging/no public IP. Preserve
all `/data` images and reports. No production, merge, tag, deletion or local e2e.
All 1,353 frozen product files and 193 worktree entries remain unchanged apart
from the allowed experiment notes. Main refs freshly checked remain 0f092204 /
infra af04ea978; stock runsc 20260810 still differs from the production main pin.

Local attach-to-ready 228.803ms and command 275.608ms (combined 504.456ms) exclude
regional ingress/manager transactions/encrypted Reader/S3 and follow source
verification reads. They are not new <=2s passes. Original empty-node/cached-new
identities, populated large roots/upper histories, actual occupied production
width and regional ingress through immediate real-command 1s/2s gates stay open.

Follow-up: [D-LOOP-DIO](LOOP-DIO.md) now runs actual buffered/direct/direct/buffered
controls on the retained Node wrapper. Both direct treatments remove the 57MiB
expansion with unchanged backed executable demand; reverse buffered controls
reproduce it. This validates the structural mechanism, not encrypted net benefit
or adoption. It also retains a broader non-atomic loop-filter setup visibility
gap: full-prefix NBD and post-mount loop completion proofs remain separate.
