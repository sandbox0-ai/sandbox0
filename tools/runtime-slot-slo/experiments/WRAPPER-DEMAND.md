# D-WRAPPER-DEMAND: complete wrapper native-read cost screen

2026-09-12. Evidence: `/tmp/sandbox0-wrapper-demand.1VMg5U`.
This is an isolated diagnostic, not format adoption or regional startup acceptance.

## Question and fixed boundary

Does the complete single-XFS wrapper, including its actual nested lower mount,
reduce the native reads needed to start stock runsc/procd and execute a real
`node -v`, relative to a matched ordinary XFS image?

Reuse both exact complete plain EROFS images from D-COMPACT-BASE and the retained
ordinary A images. No OCI export, EROFS rebuild, resolver scan, object grouping,
cache-size, readahead, timeout, hardware or concurrency sweep. Build one wrapper
per image using the existing XFSBuilder: `lower/lower.erofs`, `upper`, `work`.
Logical capacities remain 16GiB and 1TiB. Coding contains about 4.49GB of unique
regular-file bytes, not a populated 1TiB root.

Each lane uses the existing real COW branch and kernel NBD implementation over
an O_RDONLY local-file base. NBD63 is outside ctld's configured nbd0..15 pool;
its existing 4096KiB readahead and 10s request/readiness settings stay unchanged.
The wrapper uses one NBD plus a readonly loop/EROFS mount; ordinary uses the
existing readonly lower bind. Overlay options remain index/metacopy/redirect-dir/
xino off. No production mount/terminal-proof implementation is changed.

The same stock runsc release-20260810.0, systrap, shared file access, DirectFS and
overlay2=none run both layouts. Private networks have no external route. A fresh
ephemeral diagnostic Ed25519 key authenticates the actual procd command-ready
probe and command API, without contacting the real manager or publishing a
sandbox generation. The command must finish successfully and return a Node
version. This does not manufacture a real manager/ctld readiness proof.
Go helpers compile against the frozen candidate worktree, with 1,353 product
files checked against its existing manifest, not an assertion of unmodified
production-main binaries. Fresh main refs remain core0f092204 and infraaf04ea978.

The observer records actual base and NBD ReadAt offsets, lengths, returned-byte
hashes, monotonic intervals and starting/ending stages. Count unique/repeated
bytes separately. Logical 4KiB units are NOT encrypted objects or HTTP GETs;
overlapping base/NBD intervals must not be added. Requests before command
completion can include speculation and cross-stage completions, not a proven
mandatory serial dependency graph.

Offline copying/hashing warms host file caches. Fresh COW branches, NBD mounts
and guests do not make this an empty-node S3 test. There is no encrypted Reader,
mapping tree, object storage, regional ingress, cached-new cohort or occupied
production-width measurement. This two-core test host and historical runsc pin
do not establish current production parity. Local timings cannot pass the 2s gate.

## Changes and failed attempts retained

1. Initial unit `s0-wrapper-demand-1VMg5U` fails before work begins: placing the
   whole runner in a private network prevents its existing readonly PostgreSQL
   baseline guard from connecting. No image build, NBD, guest or command occurs.
2. Qualified unit `s0-wrapper-demand-qualified-1VMg5U` keeps the control guard on
   the host network and isolates each diagnostic child instead. Both complete
   wrappers build. Node ordinary and wrapper both fail during runsc create:
   `cgroup.subtree_control` returns EBUSY because the runner occupies the parent
   cgroup. Neither starts procd. The wrapper also reports one cleanup error;
   the runner stops before Coding rather than continuing after failed cleanup.
   Exact later inspection finds no loop, mount or NBD attachment and unchanged
   original runtime/files/rows. Both identical runsc logs and all 34 reports remain.
3. Runtime-qualified unit `s0-wrapper-demand-runtime-1VMg5U` moves the supervisor
   into a leaf cgroup and creates guest cgroups as its siblings, leaving the
   delegated parent empty. It waits for the exact loop's backing-file reference
   to disappear before unmounting XFS. It reuses the already built readonly
   wrappers after allocated-span/content identity verification; no rebuild. That
   guard rejects Node before any lane starts: the old sparse-geometry digest is
   unstable. Its cgroup/loop corrections are not yet exercised by a guest.
4. Canonical unit `s0-wrapper-demand-canonical-1VMg5U` first checks the exact
   readonly XFS wrapper tree and complete embedded EROFS SHA against the retained
   source. The isolated file-backend identity becomes logical size plus ordered
   nonzero 4KiB block addresses/contents. Zero holes and allocated zero blocks
   are the same virtual-disk bytes, not changes to guest file-hole semantics.
   All four disk images must reproduce logical identity after the lanes. It does
   not reinterpret the old failed digest as valid or change product checksums.
   All lanes use the same corrected harness and fresh work/guest identities.
   All four create/start and per-lane cleanup operations pass, but procd exits
   before HTTP readiness: the diagnostic Assignment omitted the required
   `EnvVars[SANDBOX0_SANDBOX_ID]`. All four 10s HTTP waits fail; no command runs.
   Every logical disk passes before/after identity checks. The outer staging
   unmount then reports EBUSY; later inspection verifies all loops/NBD and owned
   mounts absent across process mount tables. Do not mark that cleanup successful.
5. Validated unit `s0-wrapper-demand-validated-1VMg5U` fixes that exact assignment
   input. A new test invokes the actual procd AssignmentFromEnv loader: it first
   reproduces the rejection against canonical sources, then passes with the fix.
   It reuses the fully checked embedded-content evidence only after all four
   current logical disk hashes match the canonical after-hashes, and verifies
   logical contents again after execution. No repeated inner mount/tree/hash
   scan, no image rebuild. It records owned file handles before outer unmount.

The cgroup create failure is a harness error, not an ordinary/wrapper speed result.
The original cleanup error does not prove its precise asynchronous substage;
the new exact-reference wait must still pass actual cleanup checks. Do not discard
either failed task, expand timeouts or relabel partial mount traces as startup.

The fingerprint investigation finds Node SEEK_DATA coverage changing from
250,417,152 bytes/25 spans to 315,404,288 bytes/36 spans, with unchanged inode,
length, mtime and ctime. FIEMAP identifies a 65,007,616-byte unwritten extent.
This is consistent with the cache-sensitive mechanism in upstream Linux 6.8:
ext4 delegates seeking to iomap, whose unwritten-extent handling consults the
page cache. See [ext4 seek dispatch](https://github.com/torvalds/linux/blob/v6.8/fs/ext4/file.c)
and [iomap SEEK_DATA/SEEK_HOLE](https://github.com/torvalds/linux/blob/v6.8/fs/iomap/seek.c).
The old digest also included raw interval boundaries, so it was not a stable
byte identity. This evidence alone is not a full before/after byte-integrity
proof; the fourth attempt independently validates embedded content and uses
canonical logical-byte identity. New tests cover holes, preallocated zeros,
read-cache population, written zeros and a real nonzero change. Source files
and all failed identities stay retained.

## Result: do not advance this measured wrapper

Four independent authenticated procd instances complete real `node -v`, all
returning `v22.23.2`, with successful per-lane runsc/mount/loop/NBD cleanup. The
validated order is Node ordinary, Node wrapper, Coding wrapper, Coding ordinary.
There is only one matched pair per image, not a latency distribution.

| Image/layout | Unique base-read MiB | Nonzero-containing unique MiB | Base calls | NBD calls |
| --- | ---: | ---: | ---: | ---: |
| Node ordinary | 89.004 | 88.410 | 23,392 | 889 |
| Node wrapper | 143.852 | 143.289 | 36,997 | 1,389 |
| Coding ordinary | 89.844 | 89.309 | 23,577 | 926 |
| Coding wrapper | 147.059 | 146.516 | 37,843 | 1,421 |

The wrapper increases unique base reads by **61.6% / 63.7%**, or 54.848/57.215MiB.
This is not predominantly zero-filled unwritten space: excluding all-zero reads
still leaves 54.879/57.207MiB more nonzero-containing data. The observer uses
returned-byte SHA to classify all-zero reads; it does not count only nonzero
individual bytes or equate these blocks with encrypted objects/GETs.

Reads started during `node_v` dominate the increase: ordinary 67.652/68.480MiB
versus wrapper 116.539/119.703MiB. Mount-phase unique bytes drop from about
3.19/3.13MiB to 1.22/1.32MiB, but that narrow saving does not represent the complete
workload. Phase-specific unions can overlap; do not add them into unique totals.
The trace does not yet distinguish mandatory executable/library demand from
speculative readahead, or identify which nested-layer policy causes the increase.

| Image/layout | Local attach-to-authenticated-ready ms | Real command API ms | Local combined ms |
| --- | ---: | ---: | ---: |
| Node ordinary | 315.569 | 952.174 | 1,267.744 |
| Node wrapper | 228.984 | 1,091.989 | 1,320.974 |
| Coding ordinary | 228.109 | 1,088.790 | 1,316.901 |
| Coding wrapper | 236.694 | 865.346 | 1,102.041 |

These cache-affected local-file timings include the observer but exclude
regional ingress, manager/PG claim, encrypted mapping/header/data service and
real ctld launch authority. They are **not new 2s passes**. Coding's lower local
duration despite more bytes is another reason not to select a layout from a
single cached file-backed timing. Base ReadAt interval unions range 879–1,141ms;
those intervals include local filesystem/scheduling waits, may overlap other
work or cross the command boundary, and are not a pure disk-time attribution.
The independent request-ID-correlated OSS evidence remains in
[CORRELATED-READER.md](CORRELATED-READER.md); this screen neither replaces it nor
exonerates hardware under real occupancy.

Decision: do not proceed with encrypted publication or format adoption of this
exact wrapper solely on compact metadata/mount savings. It fails the observed
whole-workload read-volume screen. This does not disprove every possible compact
filesystem layout. Any reopening needs an identified mechanism that removes
the added demand under unchanged requirements, not another timeout/cache/
readahead sweep or repeated compatibility work without a credible net-cost gain.

## Verification and closure

160 remote JSON reports across five distinct attempts verify by SHA/size. Keep
all failed tasks, identities, guest logs and the actual-loader red test. The final
five Go race tests pass; vet/build pass. Five evidence tests/20 assertions pass.
All four logical disk identities match the canonical before/after and validated
before/after checks. Original A/B image metadata and runtime binaries/configs,
service PIDs, node boot, database rows and Nomad job state remain unchanged.

The validated task still exits failed because the **outer staging volume unmount
returns EBUSY after all workloads and content checks**. Its pre-unmount owned
cgroup contains only the Ruby supervisor and no open staging-file FDs. This does
not exclude cwd, mappings or transient kernel references; the exact cause is
unresolved. Later independent checks verify all owned mounts absent across
process mount tables, all loops absent and all64 NBD devices detached. Do not
rewrite the task as successful, use lazy unmount, or run another workload merely
to hide this teardown failure. The original filesystem/runtime terminal-proof
contracts are not qualified by this diagnostic.

SSH is closed and its owned master terminates. Compute stop uses the remote
skill's workspace Makefile lifecycle only; `/data` and failed artifacts remain.
The interim Stopping receipt is retained. A fresh 10:38:27 UTC cloud read confirms
Stopped/StopCharging/no public IP, after the same stop call completes. This is
recorded in the sealed lifecycle evidence. No Kind/bootstrap/local e2e, production change,
merge, tag, object upload or material deletion. All1,353 frozen product files
and193 dirty-worktree entries remain preserved; only experiment notes change.

No new regional startup sample exists.
Encrypted durable representation, image/worker authentication, populated upper
histories, source sparse/hardlink semantics, complete rebase Apply/health and
terminal absence contracts remain open. The generic carrier, claim-time tenant
RootFS, disposable workers, encryption/checksums/fencing, regional claim plus
immediate real command, empty-node/cached-new and actual occupied-width 1s/2s
requirements are unchanged.

## Follow-up attribution

[D-DEMAND-PROVENANCE](DEMAND-PROVENANCE.md) verifies that nearly all added volume
already exists at NBD, then composes readonly outer-XFS mapping with retained
EROFS file-body geometry. Extra Node/procd body reads explain71–74% of the NBD
increase;61,988 observed blocks independently match retained bytes. It also
qualifies the measurement adapter: the diagnostic's generic file ReaderAt does
not take the candidate-only concrete-Reader clean-span fast path, which is absent
from main. No new guest/S3/regional run or format adoption follows. Nested
buffering/readahead remains a causal hypothesis requiring per-layer observation.
