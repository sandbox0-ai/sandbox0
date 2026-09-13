# D-LAYERED-MOUNT-PROVENANCE: real mount evidence

2026-09-12. Evidence: `/tmp/sandbox0-layered-mount.LyQ6f5`.
Follow-up to [LAYERED-REBASE-MODEL.md](LAYERED-REBASE-MODEL.md). Both retained
complete EROFS images are mounted with isolated XFS uppers. Selected native
operations are measured, not the complete filesystem contract or a sandbox SLO.

## Decision

Do not implement physical layer attribution by comparing a merged file's
`st_dev`/`st_ino` to lower/upper device IDs. Both actual images disprove that
shortcut. The previous transient model required independently asserted physical
provenance; its inputs cannot be populated directly from merged stat results.
Its conservative device validation would reject these observations, not make
them safe by accepting synthetic expected IDs.

Continue only with exact underlying-layer resolution bound to offline branch
authority and immutable base identity. Keep logical data coverage distinct from
physical extents and guest-visible identity. This is a rebase/offline requirement,
not permission to add full-tree scanning to claim. No format is admitted and no
runtime scanner or safety check is relaxed in this experiment.

## Copy-up: stable stat identity, different data source

Main explicitly uses `index=off,metacopy=off,redirect_dir=off,xino=off`. Use those
options, plus `noexec` for this non-executing diagnostic. Three isolated loop
devices per case mount the original XFS read-only, EROFS read-only and a new
512MiB XFS upper. The whole lower image remains present; no selected-files-only
replacement is used for either primary case.

For the Node case's `etc/adduser.conf`:

| View and operation | Device / inode | FIEMAP data address |
| --- | --- | --- |
| Direct EROFS lower | 1793 / 7497472 | physical0, length3040, flags0x301 |
| Merged before copy-up | 58 / 7497472 | same lower extent |
| Merged after chmod and write | 58 / 7497472 | physical335597568, length4096, flags1 |
| Direct XFS upper after write | 1794 / 655489 | same upper extent |

The merged identity stays unchanged while its FIEMAP switches to upper data.
The Coding case's `etc/R/Makeconf` repeats this behavior: merged device58 and
inode135143514 remain stable; its old lower data plus inline tail become an
upper extent at335642624, length16384. These device/inode/address numbers are
local observations, never portable identities or candidate layout constants.

Both chmod operations preserve content; both subsequent writes modify only
merged/upper content. Lower hashes remain unchanged. FIEMAP results match exact
direct-layer observations before and after; errors or missing maps are not
treated as successful equality. The diagnostic retains raw flags but does not
feed them to branch dirty attribution or remove production FIEMAP rejection.

Merged `FS_IOC_GETVERSION` remains unavailable after these copy-ups, while the
direct XFS upper returns a generation. Do not turn that error into a trusted
zero generation. `statx` mount IDs and filesystem magic are recorded; merged
mount identity is not an underlying data-device proof either.

This is consistent with upstream Linux6.8: `ovl_map_dev_ino` can expose an
anonymous per-layer device, `ovl_getattr` can preserve the copy-up origin's
identity, while `ovl_fiemap` consults the current real data inode. Upstream source
explains the observed mechanism; it is not a claim that the Ubuntu kernel binary
was rebuilt or its distribution patch set exhaustively audited.
[Linux6.8 OverlayFS inode implementation](https://raw.githubusercontent.com/torvalds/linux/v6.8/fs/overlayfs/inode.c).

## Hardlinks: separate existing behavior from format regression

The selected real pairs are Node `usr/bin/gunzip` / `usr/bin/uncompress` and
Coding `usr/bin/bunzip2` / `usr/bin/bzcat`. Before copy-up each pair shares a
lower inode and content. Writing the first merged name does not modify its peer;
the peer and both direct lower names retain the original bytes. Therefore the
primary experiment does not pass a hardlink-preservation gate.

After observing that result, run ONE focused same-XFS control with exact Node
image bytes. Its lower/upper/work directories share a new XFS filesystem, lower
is read-only bind-mounted as in main, and all four mount options are identical.
The hardlink break reproduces there too. This control identifies behavior that
also exists in the current-layout mount configuration; it is not an EROFS-only
regression or a production/gVisor reproduction. It is a tiny semantic control,
not a replacement for the full primary images or a startup baseline.

Official documentation describes hardlink breakage with index disabled and the
separate identity behavior of same-filesystem versus cross-filesystem layers.
No mount-option sweep, index enablement or production fix is made here.
[OverlayFS documentation](https://docs.kernel.org/filesystems/overlayfs.html).

Newly created hardlinks entirely in the upper do share inode/content after a
write in both primary cases. Keep this distinct from existing lower hardlinks
that undergo copy-up.

## Sparse layout and whiteouts

All selected original-XFS, EROFS and merged file content hashes match before
mutation, including the exact fixed Node/procd bytes. However SEEK_DATA/SEEK_HOLE
on both real images finds a representation difference:

| File | Original XFS hole bytes | Plain EROFS / merged hole bytes |
| --- | ---: | ---: |
| Node ELF,124836408bytes | 1183744 | 0 |
| procd,24463545bytes | 4096 | 0 |

Thus earlier whole-tree byte/metadata/link hashes did not prove sparse-layout
equivalence. Do not silently extend that earlier fidelity result. Whether and
how a new format preserves the required allocation/rebase contract remains to
be resolved; equal zero-byte reads are not a substitute for that decision.

Controlled new upper files behave as requested in both cases: create a64KiB
file with16KiB data, punch `[4096,8192)`, shrink to8KiB, then grow to32KiB. Exact
SEEK ranges and FIEMAP are retained; growth leaves only the first4KiB allocated.
These selected cases do not exhaust hole/truncation behavior for arbitrary files.

Renaming the modified lower-origin file hides its old merged name, retains the
lower bytes, preserves modified bytes at the new name and creates an upper0/0
character-device whiteout. The unchanged main rebase test
`TestApplyRecreatesOverlayWhiteoutAndOpaqueDirectory` also runs and passes on the
remote host, closing its local CAP_MKNOD skip. The other13 prior NBD/RustFS skip
events are not claimed as covered by this experiment.

## Environment, preservation and limits

The remote-test skill supplies only the workspace Makefile lifecycle. No old
Kind/bootstrap/deployment path runs; user-required experiment notes override
the skill's generic no-summary-docs rule. Fresh main refs remain sandbox0
`0f09220460581bfc1fdc331f34ebc85bf38381e7` and infra
`af04ea978f62b6eceaa78da98840227505ca764b`. Frozen1353product files are unchanged.

Host: Ubuntu24.04, kernel6.8.0-124-generic,2CPU/8GiB. Resolve the64GiB staging
disk by serial`t4n1qmzpkzn19uxs6o0p` and UUID
`59c7f761-4479-46df-b2ef-8ea7d7bce30b`, currently`/dev/nvme1n1`. The historical
`/dev/nvme0n1` name now denotes the system disk and is not a valid target.

One primary unit completes, then one explicitly separate focused control exits0.
All loops and temporary mounts detach. Original13files, service PIDs, job70668/
two groups, database histories2072/2072/263 and64detached NBD devices are preserved.
Both compact image SHA256 values remain exact; all four retained original XFS
image stat/allocation records are unchanged. No source pull or image rebuild.

81 reports transfer with SHA256/size verification. SSH closes explicitly; its
owned master exits255 after closure. Keep the interim `stopped-cloud.json`
receipt, which actually says Stopping. The SAME Makefile stop call completes;
fresh `final-cloud.json` at08:57:08UTC proves Stopped/StopCharging/no publicIP.
No repeated stop/start operation is substituted for waiting on that live handle.

New upper images are retained in the owned trial directory. Their512MiB logical
size and67,584,000/67,633,152allocated bytes are not per-sandbox RAM or a complete
durable wrapper measurement. The unit's3.8GiB peak includes offline scans/hash
reads and caches, not occupied sandbox density. Preparation is cache-affecting;
there is no cold-cache, claim, guest execution or startup timing claim here.

Initial native vet failed on local Stat_t.Nlink's uint32 type; explicit conversion
fixes cross-architecture compilation. Preserve `BUILD-FAILURE.md`. A final probe
build adds failure-path partial-result capture before staging; the unmodified
rebase test binary is not rebuilt for that observer-only addition. No remote
test replay was needed for either preparation change.

The machine/S3 hypothesis remains covered by existing correlated request
evidence, not retested or exonerated by these native mounts. Complete encrypted
durable embedding, underlying-layer scanner/Apply proofs, actual required reads,
net cold/cached-new costs, populated root and upper histories, occupied actual
production width, and regional ingress claim plus immediate real `node -v` all
remain open. Preserve generic warm carriers and claim-time RootFS binding; do
not add root prewarming, increase timeouts or weaken readiness/terminal proof.
