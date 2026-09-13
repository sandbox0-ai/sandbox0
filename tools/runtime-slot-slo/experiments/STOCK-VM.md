# D-STOCK-VM: complete producer succeeds, sparse copy-up still amplifies writes

2026-09-13. Evidence: `/tmp/sandbox0-stock-vm.WrELv4/evidence.json`.
The preceding [stock-profile inspection](STOCK-PROFILE.md) was progress. This
turn executes the selected full producer and stock kernel, closing the gap
between source inspection and actual filesystem behavior. It produces no
regional startup sample and does not explain the current default runtime's
slow startup by attributing an unadopted candidate's defect to it.

## Executed boundary

- Host: original2CPU8GiB Singapore ECS, Ubuntu kernel6.8.0-124-generic,
  boot54cdff68-6efc-4de5-9400-5002f58f72fa. No KVM device or VMX/SVM support.
- Producer: exact signed vendor erofs-utils1.9.2 x86_64 executable, run with
  the existing Ubuntu userspace libraries. This is not a complete vendor OS
  or evidence about packages installed in production.
- Guest: exact signed vendor kernel6.6.102-7.alnx4.x86_64 plus seven matching
  modules, booted once using QEMU8.2.2 TCG, one virtual CPU and1GiB RAM. The
  static Go PID1 probe mounts XFS, EROFS and OverlayFS and performs real
  chmod-triggered copy-up. No virtual network or tenant credentials.
- Host kernel and package database unchanged. QEMU and its20 dependency
  packages are downloaded and unpacked only into the owned experiment tree.
  The `_apt` directory-access warning is retained; no package installation or
  post-install scripts run. An explicit owned SeaBIOS path is added during
  preflight, before the first launch; the initial manifest is retained.

The [direct-kernel boot mechanism](https://www.qemu.org/docs/master/system/linuxboot.html)
allows testing the vendor kernel without replacing the host kernel. TCG
execution time is not a Sandbox0 latency measurement or production-width proof.

One new512MiB XFS fixture on the existing staging disk contains the same four
source byte/allocation controls as the previous native counterexample. No old
RootFS image is reused for writes. Full mkfs and fsck succeed for both plain
and4KiB chunked images; the VM mounts and reads both, performs copy-up, and
compares against a readonly bind whose lower/upper/work are on the same XFS.
All68 complete payload observations across the three paths match size, mode
and SHA256. The old1.7.1 sparse/tail EIO is absent in this executed combination.

## Actual allocation after copy-up

All source files have mode0600; chmod0600 is used consistently with the prior
fixture. Copy-up is proved by the real upper file observations, not inferred
from a mode-value change. Metacopy is explicitly off.

| File | Source allocation | Plain EROFS to XFS | Chunked EROFS to XFS | Same-XFS control |
| --- | ---: | ---: | ---: | ---: |
|16MiB all-hole |0 |16MiB |0 |0 |
|16MiB allocated-zero with two data islands |16MiB |16MiB |3.875MiB |16MiB |
|16MiB sparse with two4KiB data islands |8KiB |16MiB |3.875MiB |8KiB |
|12345-byte short-tail file |4KiB |16KiB |16KiB |4KiB |

The chunked lower's SEEK and FIEMAP expose the sparse file's exact two4KiB
logical data ranges. After copy-up, the upper has two1MiB initialized extents
plus two960KiB unwritten extents:2MiB initialized and1.875MiB unwritten,
4,063,232 allocated bytes total. Relative to the original8KiB, those are256x
initialized-extent and496x allocated-sector ratios. These are filesystem
extent/allocation observations, not measured S3 traffic or exact host write-I/O
byte counters. They confirm the vendor OverlayFS source concern about1MiB
splice requests crossing holes, with additional XFS unwritten allocation.

Two measurement distinctions matter:

- EROFS `st_blocks` reports rounded logical size even for the hole-only inode;
  it is not per-file physical object occupancy. The chunk image is44KiB and
  the plain image48MiB+16KiB for this zero-heavy synthetic fixture. That ratio
  cannot be extrapolated to populated user roots or language runtime images.
- SEEK_DATA over XFS unwritten extents can depend on page-cache contents. The
  collected upper SEEK range includes these extents after prior payload reads.
  Keep FIEMAP initialized/unwritten extents separate; do not relabel the whole
  allocated amount as initialized data or a cold-cache observation.

The newer producer also converts allocated zero blocks into holes, as predicted
by its source. Equal payload hashes therefore do not imply equal allocation or
SEEK behavior. The same-XFS control changes source FIEMAP SHARED flags through
reflink; all other source identity, logical state and physical extent fields
remain unchanged. The verifier masks only that expected flag, not other drift.

## Baseline and decision

Fresh main0f092204 `pkg/rootfssession/runtime_linux.go:MountOverlay` places
lower, upper and work under one XFS root, with the same index/metacopy/redirect/
xino options. Its artifact builder enables XFS reflink. Thus the same-XFS
control tests a real property of the current layout, not an invented ideal
baseline. It remains a small kernel/filesystem fixture, not a full NBD/S3/
gVisor lifecycle test of the deployed product.

Do not adopt this cross-filesystem EROFS-lower/XFS-upper candidate merely
because the producer and kernel now read sparse files correctly. It loses the
cheap same-filesystem copy-up property and fails the sparse allocation/cost
comparison. Do not replay the same kernel/tool combination or promote a
metadata-only workaround as proof that first-write costs are fixed. Any next
layout proposal must preserve that capability or explicitly prove bounded
write/copy-up cost before full-image lifecycle and regional startup testing.
The existing XFS-based bounded header/mapping cache and independent-read work
remain untouched.

## Completion, preservation and remaining goal

The single systemd campaign exits0 and its VM powers off normally. All58 exported
files, including the serial console, are checksummed. The probe's two
local mechanism tests pass; evidence verification passes2 tests/18 assertions,
including13 deliberately invalid semantic/scope reports. These verifier tests
do not substitute for the actual remote execution.

All149 prior sealed stock-profile artifacts and1359 frozen product files verify
unchanged. Original service processes/binaries, job index73058/two groups and
trial database row count2121 remain unchanged. The final scan covers222 host
mount tables, with no owned mounts/loops/VM and all64NBDs detached. New fixtures
and tools are retained, the staging disk is unmounted, and owned SSH is closed.
The remote skill lifecycle stops compute; the independent03:22:00UTC cloud
receipt confirms Stopped/StopCharging, original2CPU8192MiB and no public IP.
Earlier Stopping observations remain recorded, not overwritten as successes.

One remote start and one kernel boot; zero claims, literal node-v commands or
regional startup samples. No local e2e, production rollout, merge or tag; no
timeout/cache/machine enlargement or user RootFS prewarm. Preferred1s/accepted2s,
cold/cached-node new identities, immediate real command, populated/history-
bearing large roots and actually occupied production-width gates remain open.
