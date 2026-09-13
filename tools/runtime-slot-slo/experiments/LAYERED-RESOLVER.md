# D-LAYERED-RESOLVER: resolve actual data through held layer roots

2026-09-12. Evidence: `/tmp/sandbox0-layered-resolver.6LZwmH`.
Diagnostic Go overlays only; no product source, deployed runtime or format change.

## Result and scope

The qualified prototype resolves every regular-file path in both retained real
EROFS-lower/XFS-upper OverlayFS trees. It keeps guest-visible identity separate
from the held underlying data file and only projects upper extents into writable
physical attribution. This closes a native regular-file resolver prerequisite,
not full rebase, image-format, authenticated worker or startup acceptance.

| Actual tree | Regular paths | Lower paths | Upper paths | Selected payload comparisons |
| --- | ---: | ---: | ---: | ---: |
| Node | 5,775 | 5,771 | 4 | 7 |
| Coding | 132,932 | 132,928 | 4 | 7 |

Each selected merged/direct pair has identical content and matches the previous
sealed mutation result: Node ELF, procd, copied-up hardlink member, unchanged
lower peer, renamed file, upper sparse file and its hardlink. All seven selected
merged devices in each image differ from their actual underlying device. Upper
inode generations are available from the direct XFS descriptor; merged
generations are unavailable and are not fabricated.

The upper sparse file remains 32,768 bytes with logical data `[0,4096)`, and its
new upper hardlink shares the actual inode/generation. Both old renamed paths
remain whiteouted: resolving the merged path returns ENOENT without lower
fallthrough. The earlier existing-lower-hardlink write-propagation failure and
source ELF sparse-hole differences are NOT repaired or waived here.

The metadata/extent scan reads no file payload. Seven selected paths per tree
receive full payload comparisons; this is not a fresh full-tree payload hash.
Original immutable lower images and source upper images are separately hashed
before and after, and fresh working clones match their source upper hash before
mounting. The scanner excludes 1,522/17,834 non-regular entries, including the
root/directory/symlink entries; it is explicitly not a complete rebase Manifest.
No unsupported regular path is silently skipped.

Logical data totals by path are 252,178,156/4,893,858,784 bytes. They count hardlink
aliases, not unique data or disk usage. Observed offline scans take 520.295ms and
15,088.933ms after image verification and selected reads. These are cache-affected
preparation costs, NOT claim latency, cold node-v, or production-width results.
No full-tree scan is added to claim.

## Implementation and recorded failures

The Go overlay reuses current `rootfsrebase` secure-root, FIEMAP and inode-version
code. Existing path wrappers still apply the original unsupported-FIEMAP mask.
Only diagnostic fd-based helpers support the known lower inline/non-aligned
logical coverage. No lower physical offset enters writable branch accounting;
no fake LBA, empty-data projection, serialized field or Apply conversion is added.

Held roots require observed device/inode/mount-id/filesystem matches. Path opens
reuse BENEATH/NO_MAGICLINKS/NO_SYMLINKS, reject non-regular objects before opening
data, then reopen only the already-held numeric O_PATH descriptor internally.
Child mount crossings, root replacement, wrong roots, symlinks/FIFOs, opaque
fallback, mismatched size/FIEMAP and out-of-geometry upper extents fail closed.
Lower physical roots must be XFS or EROFS; upper must be XFS. The real test client
also requires the merged root to be OverlayFS.

The change/effect sequence is preserved rather than overwriting failures:

1. First local race run fails: requiring merged regular-file `st_dev` to equal
   the merged directory device repeats the invalid physical-identity assumption.
   Local temporary roots also reside on host OverlayFS, not a physical XFS layer.
2. Correct this distinction: only physical lower/upper roots require uniform
   device identity; merged still requires its exact mount id and filesystem.
   Add explicit physical-profile rejection. XFS-dependent local cases skip
   transparently and must run remotely, rather than being counted as coverage.
3. Recognize the already-observed directory `trusted.overlay.uuid` only with its
   bounded 16-byte representation. It and origin metadata never route data.
4. First real-mount attempt rejects BOTH images at selected procd because an
   ancestor has `trusted.overlay.impure`; neither whole-tree scan starts.
   Preserve the exit-1 unit, binaries, sources, receipts and successful cleanup.
   Ten top-level XFS mechanism tests pass in that attempt.
5. Qualify only directory `trusted.overlay.impure=y`, with malformed-value and
   regular-file rejection tests. Use a separate unit, binaries and fresh clones.
   Both complete regular trees now pass; eleven top-level XFS mechanism tests
   pass. The sole remote skip is the deliberately inapplicable non-physical-host
   profile test (the remote temporary filesystem really is XFS).

Linux v6.8 copy-up marks parents impure when they contain copied-up entries;
readdir uses that flag for inode-number translation. The lookup path independently
handles redirect, metacopy, opaque and whiteout state. This supports the narrow
qualification, not ignoring every overlay attribute. Unknown attributes,
redirect/metacopy and malformed recognized values continue to reject. Sources:
[copy-up](https://raw.githubusercontent.com/torvalds/linux/v6.8/fs/overlayfs/copy_up.c),
[readdir](https://github.com/torvalds/linux/blob/v6.8/fs/overlayfs/readdir.c),
[lookup](https://raw.githubusercontent.com/torvalds/linux/v6.8/fs/overlayfs/namei.c).
These are upstream reference semantics, not an audit of every Ubuntu patch.

Local race results are retained separately: initial 28 pass events, 13 fail
events, one skip; profile-corrected 21 pass/15 skip; UUID-qualified 21 pass/16
skip; impure-qualified 22 pass/18 skip. Events include subtests and parent events,
so these are not counts of independent fully exercised tests. Actual remote XFS
tests cover the skipped resolver mechanisms, including a real child bind mount.
The earlier privileged whiteout Apply result remains historical; this turn does
not rerun it or cover the other outstanding NBD/RustFS/gVisor tests.

## Authority, machine and lifecycle boundaries

These root fingerprints are local experimental observations, NOT authenticated
ctld/WorkerRequest inputs or a durable nested-image binding. The private owned
namespace provides a quiescent fixture; holding root/file descriptors and checking
roots before/after does not prove the absence of concurrent file/path mutations.
Do not claim a general racing-writer-safe scanner from these tests.

The machine/S3 hypothesis remains supported by the separate
[request-ID-correlated investigation](CORRELATED-READER.md): large exact client
waits coincide with OSS response waits even with sub-millisecond smoothed RTT.
That does not establish an OSS defect, eliminate limited transport effects, or
prove occupied production hardware health. This resolver test adds no S3 request
experiment, hardware sweep, replacement startup baseline or causal latency claim.

Use only the remote skill's Makefile lifecycle, not obsolete Kind/bootstrap.
Resolve the 64GiB staging disk by serial and UUID before mounting; `/dev/nvme0n1`
is the system disk, while the validated staging device is `/dev/nvme1n1` in this
boot. Two isolated native loop-backed filesystems are diagnostic fixtures, not
an adopted two-NBD runtime architecture. All original images remain unmodified.

The first unit ends failed/exit-code/1 and the qualified unit inactive/success/0;
both have MainPID 0. All owned mounts and loop devices are gone. Before/after
13 original runtime files, service PIDs, boot identity, job index 70668/two groups,
formal/source/original database row counts 2072/2072/263 and 64 detached NBD devices
match. All 52 first-attempt and 54 qualified-attempt reports transfer with exact
hashes. The owned SSH master closes explicitly. `/data` and all clones remain.
Keep `final-cloud.json`, which actually observed Stopping, unchanged. The same
owned stop call finishes at 09:35:21 UTC; the fresh `stopped-cloud.json` read at
09:36:16 UTC confirms Stopped/StopCharging/no public IP. No diagnostic owner is
left running. Eight evidence-validator tests with ten assertions also pass.

Main refs were fetched this turn and remain core `0f092204`, infra `af04ea978`.
The diagnostic host is still 2 CPU/8GiB with historical runtime pins, not the
current production machine/runsc profile. Both unit memory peaks include full
image hashing and offline preparation; they are not per-sandbox density costs.
All 1,353 frozen product files and the prior 138-artifact evidence set verify.
No production rollout, merge/tag, RootFS publication, guest command or deletion.

## Remaining decision

Do not repeat these complete image scans as another baseline. The native
regular-file resolver is qualified only for this exact supported mount profile.
Next integration must retain exact worker/image authority and separate logical
coverage from writable attribution throughout full Manifest/Diff/Apply and health
proofs. Source sparse allocation, existing lower hardlinks, gVisor/inode/watch
behavior and complete durable wrapper/recovery/absence still need resolution.

Actual new-layout mandatory reads and complete net cold/cached-new costs are
unmeasured. A layout change remains an unchosen candidate, not a latency result.
The generic warm carrier, claim-time tenant root, stateless workers, encryption,
checksums, readiness and writer fencing requirements are unchanged. Original
populated-root/upper-history, occupied actual production width, regional-ingress
claim plus immediate real `node -v`, preferred 1s/accepted 2s gates remain OPEN.
