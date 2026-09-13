# D-STOCK-PROFILE: production-pinned vendor kernel has the seek backport

2026-09-13. Evidence: `/tmp/sandbox0-stock-profile.vhypbh/evidence.json`.
This follows [SPARSE-PRODUCER.md](SPARSE-PRODUCER.md). The preceding user-question
turn only explained prefetch timing and is classified as no progress. This
experiment changes the next qualification target; it is not a startup trial.

## Correct the environment comparison

Fresh `sandbox0-infra/origin/main` is
`1bdd57f2c41144b6da60ec7f07ab295eedca0ca7`. Its
`regions/ali-ue1-nomad.tfvars` pins
`aliyun_4_x64_20G_alibase_20260801.vhd`, not the retained Ubuntu24.04/Linux6.8
test-node profile. The official
[image release notes](https://www.alibabacloud.com/help/en/alinux/product-overview/alibaba-cloud-linux-4-image-release-notes)
identify that image as Alibaba Cloud Linux4.0.5 with kernel6.6.102-7.alnx4.
This is a configured-image/source mapping, not a fresh observation of the
running production kernel or its installed package set.

The exact signed
[vendor source RPM](https://mirrors.aliyun.com/alinux/4.0/updates/source/Packages/kernel-6.6.102-7.alnx4.src.rpm)
already contains EROFS `erofs_file_llseek`, calling `iomap_seek_data` and
`iomap_seek_hole`. Its changelog explicitly includes the seek-support backport.
Therefore neither upstream6.6 nor the Ubuntu6.8 counterexample establishes that
this vendor kernel lacks the feature. An Ubuntu kernel upgrade is not the next
assumed solution.

## Check the shipped binary, not just source strings

The exact
[x86_64 kernel RPM](https://mirrors.aliyun.com/alinux/4.0/updates/x86_64/os/Packages/kernel-6.6.102-7.alnx4.x86_64.rpm)
matches the current repository metadata SHA256
`5abaeb054b6377590e4f1324553ad4d3ffaa12edaece8b8e8e456250a0331510`.
Its source-RPM identity matches the inspected source. Extracted `erofs.ko` has
the expected x86_64 architecture and `6.6.102-7.alnx4.x86_64` vermagic. The
ELF relocation at the actual file-operations llseek slot points to
`erofs_file_llseek`; both iomap seek calls fall inside that function's symbol
range. This is stronger than finding unused function names in the module.

The packaged config enables EROFS and OverlayFS as modules. It is not byte-
identical to the source config: compiler/tool capability values are resolved
during packaging. The full diff is retained; the relevant filesystem settings
are checked on the packaged config itself.

All four selected kernel/producer source and binary RPM signatures validate
with key23f834c6 from the
[official public key](https://mirrors.aliyun.com/alinux/4.0/RPM-GPG-KEY-ALINUX-4),
imported only into this experiment's private RPM database. No host trust store
or installed package changes. The original system-database `NOKEY` failure
(exit4) is retained. Repository compressed/open checksums, sizes and selected
binary hashes also match. The initial Ruby XML parser is unavailable; that
exit1 remains recorded, and a Go standard-library XML verifier succeeds.

## Remaining producer and copy-up qualification

The signed repository candidate `erofs-utils-1.9.2-1.alnx4.x86_64` predates the
pinned image. Its source updates minimum extents at chunk transitions and
includes allocated-zero-to-hole conversion. It is not the previous1.7.1 plus
one-fix experiment, and it has not been executed in this turn. The repository
also supplies1.9.4 dated September2, after the image release; its source RPM was
downloaded but is excluded from this selected profile, not inspected/executed,
and not claimed to be installed in production. No installed producer version
has been inferred from a release-note table or repository availability.

The vendor OverlayFS copy-up still uses SEEK_DATA followed by up-to1MiB splice
requests, without restricting each request to SEEK_HOLE. Source inspection
therefore leaves a concrete concern that short data extents can copy adjacent
holes as zeros. The amount actually allocated depends on execution and has
not been measured here. Do not turn seek support into a claim that all sparse
allocation is preserved.

Next: execute a complete producer/image/read/copy-up fixture using this exact
stock kernel, preferably isolated from the existing remote host runtime.
Include all-hole, allocated-zero, sparse islands and short-tail controls; check
full payload hashes, seek ranges, allocation and errors. Then full-image
RootFS lifecycle/cost qualification and regional startup acceptance are still
required. No automatic production or test-host kernel upgrade is authorized by
this source result.

## Preservation and scope

All240 artifacts in the preceding sealed producer experiment and all1359 frozen
product files verify unchanged. The diagnostic worktree still uses XFS/Overlay;
EROFS has not been integrated into the default mount path. The verifier passes
3 tests/17 assertions, including14 rejected evidence/scope mutations. These
tests validate the report, not runtime filesystem behavior or cold-start speed.

No remote starts, kernel boots, claims, guest commands or startup samples. No
local e2e, package installation, production mutation, merge or tag. This turn
does not freshly observe cloud power state. No user RootFS prefetch, enlarged
timeout/cache or machine change. The original cold-node and cached-node new-
identity claim, literal node-v,1s/2s, populated/history-bearing large-root and
actually occupied production-width gates all remain open.
