# D-COMPACT-BASE-FEASIBILITY: real images and rebase compatibility

2026-09-12. Evidence: `/tmp/sandbox0-compact-base-feasibility.EyagT5`.
This advances [METADATA-SUFFICIENCY.md](METADATA-SUFFICIENCY.md) from an
opportunity/source screen to actual complete filesystem images. No runtime format
is adopted, and there is no new startup, claim or guest-command result.

## Decision

Both complete public test-image trees can be represented by an uncompressed
EROFS image while preserving the checked file contents, metadata and links.
Current rebase cannot consume that representation: both EROFS scans fail on
inline/non-aligned extents, while both matched XFS controls pass on the same
kernel and unmodified scanner.

Do not remove the FIEMAP checks or silently drop lower extents. Two diagnostic
tests against the existing implementation show why neither is sufficient:

- Identical numeric offsets on different devices are still both intersected
  with branch dirty LBAs. The device/inode key deduplicates hardlinks; it does
  not translate physical address spaces.
- Clearing extents also clears logical data coverage. An added 4096-byte file
  with no extents yields zero data-read bytes in `Diff`; supplying its data
  extent yields 4096. Missing physical attribution cannot mean an all-hole file.

These demonstrate contracts a new representation must satisfy, not defects in
the current single-XFS model. The next design needs separate logical file-data
coverage and exact writable-layer physical attribution. Unknown or mismatched
provenance must fail closed. A read-only base is not permission to discard data,
invent an offset, or bypass generation binding.

## Actual source, format and fidelity

Reuse the retained A images from sealed D-XFS-REAL-VOLUME, not another OCI pull
or rejected alignment experiment. Reverify the exact scanner binary and both
complete source trees before building. The source OCI digests and fixed Node/
procd bytes match the diagnostic image family used by the retained startup work:

- Node source `docker.io/library/node@sha256:4d676821dff059fd00d277ee4261ef34ea712317fed0737c03941481b5760c96`.
- Coding source `docker.io/sandbox0ai/otemplates@sha256:4861af110b573e4bf23ffb537ac7acd74696383b25af51815838e6d0a767cfa6`.
- Node ELF SHA256 `3517c2df0b2f8cd7f422b4b8450ef81c6889f08eb03e281d6de9079b15e6a327`;
  procd SHA256 `29dedac3bce92b6a9a3d507889110927ede8727f87186376524e577617c69f2d`.

The source and compact trees independently match the original whole-tree hashes.
Checks cover every regular file's bytes, mode/owner, nanosecond mtime, symlinks,
hardlink relationships and xattrs. The root directory, excluded by the retained
tree-hash convention, is separately checked for mode/owner/nanosecond mtime.
Do not extend those checks to untested hole, inode/device, file-watch or gVisor
semantics.

| Actual image | Entries excluding root | Unique regular-file bytes | Plain EROFS image bytes | Current XFS / EROFS rebase scan |
| --- | ---: | ---: | ---: | --- |
| Node | 7293 | 248363154 | 249196544 | pass / reject |
| Coding | 150762 | 4489374053 | 4506877952 | pass / reject |

The EROFS inode counts 7292/150755 independently agree with entries plus root,
minus repeated regular-file hardlinks. Superblock block counts times 4096 agree
with the exact image lengths. Both `fsck.erofs` checks pass; this is supplemented
by full content hashes, not treated as their replacement.

Image SHA256 values:

- Node: `3abdf3904770f495d1baa34c0a0753918e11be8bf8eacbcf79990289ddf074ee`.
- Coding: `a902d2345927a44e2f33cd4afe2cbee4509b265ca0b8c299cb45fd0e84a5a10d`.

Use extracted Ubuntu noble `erofs-utils 1.7.1-1build2`, no system installation
or package-index update. The 110570-byte package SHA256 is
`ce1a7283a3ffc47bc832dfd77c4fba6a327f3928f1c90c64fe14e251251cfc3a`.
Build with `--quiet --preserve-mtime -L s0-compact-test`, without compression,
chunking, extra devices, inode preloading or a parameter sweep. Plain images and
regular-file loop mounting are documented upstream; newer native file-backed
mount features are not assumed. [EROFS build/mount documentation](https://erofs.docs.kernel.org/en/latest/mkfs.html).

## The actual compatibility failures

The binary links the frozen, unchanged `pkg/rootfsrebase` implementation. Git
confirms that package has no difference from current main. Native kernel is
`6.8.0-124-generic`; no patched kernel or filesystem module was installed.

| Compact tree | First rejected file | Logical extent offset | Flags |
| --- | --- | ---: | --- |
| Node | `etc/adduser.conf` | 0 | `0x301` |
| Coding | `etc/R/Makeconf` | 12288 | `0x301` |

Those flags combine LAST, NOT_ALIGNED and DATA_INLINE. Current `Scan` deliberately
rejects the latter two. A bounded, read-only follow-up runs the SAME binary on
the original XFS trees: scans succeed with 7294/150763 nodes including root.
The follow-up does not rebuild either image or replay a startup baseline.

Simply accepting these flags still leaves a lower-image physical address that is
not a writable branch LBA. Simply erasing extents instead loses the coverage
needed by `fullSourceData`, `allocatedRanges` and sparse/hole-aware `Diff`.
A future adapter must retain logical coverage while binding dirty attribution to
the exact writer device/layer. Immutable-base identity, copy-up, whiteouts,
rename/hardlink behavior, hole punch, truncate/grow and old/source/target rebase
must be tested together before lifecycle integration.

## Costs this trial does not establish

These are standalone read-only images, not the proposed complete single-NBD XFS
wrapper. Writable upper/work, outer filesystem, loop/mount, descriptors, mapping,
encryption and retention costs are NOT included. Source XFS allocation is
333643776/5471035392 bytes; comparing it with only the new lower image is not a
total-RootFS saving or a cold-start speedup.

File-level `st_blocks` sums differ in meaning across filesystems and can count
packed/shared storage repeatedly. For example, Node's EROFS file sum is263286784
bytes, above the actual249196544-byte image. Do not use that sum to claim either
physical expansion or savings. Full-tree hashes also do not establish identical
sparse-hole layout; that remains part of future rebase qualification.

Observed build times are0.25s Node and129.45s Coding, with builder RSS peaks
18304/315392KiB. These are offline preparation after full-source verification;
cached reads are expected. They are neither claim latency nor per-sandbox runtime
memory. The whole diagnostic unit reports3.9G peak memory including preparation/
page-cache charges, not occupied-density acceptance. No preparation is moved to
claim time by this proposal.

There are zero new claims, guest commands or startup latency samples. No RootFS
object download/publication is part of the trial. The original cold/cached-new
new-identity, populated-large-root, populated writable-history, actual occupied
width and regional-ingress-to-procd plus immediate `node -v` gates remain open.
Coding has4.49GB file contents inside a sparse1TiB XFS source, not a populated1TiB
root. The original warm pool has one ready carrier; it was not repaired or used
as a full-path trial.

## Environment, failure record and preservation

Use the remote-test skill's Makefile lifecycle only, never its obsolete Kind/
bootstrap flow. Initial cloud read confirms the authorized Singapore test ECS
is Stopped/StopCharging,2CPU/8GiB. The system disk has only6.6GB free. Resolve
the retained64GiB staging disk by serial `t4n1qmzpkzn19uxs6o0p` and filesystem UUID
`59c7f761-4479-46df-b2ef-8ea7d7bce30b`; it is now `/dev/nvme1n1`, not the old
record's `/dev/nvme0n1`. Never reuse the historical device path, which now names
the system disk.

First inventory the staging filesystem read-only without journal recovery.
Build in a new owned subdirectory inside a private mount namespace; source XFS
and new EROFS inspection devices are explicitly read-only. XFS inspection uses
`norecovery`, so it does not replay a source log. [Linux XFS mount documentation](https://docs.kernel.org/admin-guide/xfs.html).

The initial package-preparation helper incorrectly required the literal executable
name in help output and exited1 AFTER successful download/extraction. Preserve
that failed receipt. A read-only check proves the help exits0 and libraries are
present; a new qualification checks the actual help contract. No package
download/extraction, image build or live unit was restarted for this correction.

One build task completes successfully, followed by one matched read-only scanner
control. Same live handles are polled while running; no replay on missing partial
output. All loop devices and temporary mounts detach. A/B source image inode/
size/mtime/ctime/allocation snapshots are unchanged; both A source content trees
were freshly reverified. Before/after boot, original13files, service PIDs,
job70668/two groups, database rows2072/2072/263 and64detached NBD devices match.

Seventy-two reports transfer with independent SHA256/size checks. Final host
inspection confirms no loops, both mount roots detached, no system erofs-utils
installation, build unit inactive/success/0/MainPID0, and scanner control exited0.
SSH closes explicitly; the owned master exits255 after that closure, not a trial
failure. Preserve the interim cloud Stopping receipt. Fresh08:07:55UTC observation
confirms Stopped/StopCharging/no publicIP. The64GiB disk and two new images are
retained for the next exact-input experiment; nothing was deleted.

Two Go race contract tests and eight Ruby tests/nine assertions pass. All1353
frozen product files remain unchanged. Both main refs are freshly fetched and
remain sandbox0`0f092204`/infra`af04ea978`. No runtime deployment, production
change, merge or tag. Only experiment notes change in the repository.

Next: qualify the logical-coverage versus writable-physical-attribution model
without weakening existing checks, then evaluate actual required reads and full
net cost of a complete durable embedding. Reuse these exact images; do not repeat
the source pull, format build, ordinary baseline or stopped packet experiments.
