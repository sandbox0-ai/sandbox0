# D-METADATA-SUFFICIENCY: compact-base opportunity and compatibility screen

2026-09-12. Local reanalysis and source audit, not a new startup experiment.
Evidence: `/tmp/sandbox0-metadata-sufficiency.k8FywM`.

## Decision

A filesystem-metadata payload-only improvement has almost no demonstrated cold
Coding headroom. A wider mechanism removing the associated mapping dependency
as well has more opportunity, but must remove most of that exposure before added
costs. A compact read-only base is a hypothesis worth screening, not a chosen
format, an implementation authorization, or a proven 2s solution. Do not restart
the rejected directory-neighbor/packet groupings or repeat the ordinary baseline.

The exact OSS correlation in [CORRELATED-READER.md](CORRELATED-READER.md) remains
the machine/S3 evidence. It shows material response/service waits, not proof of
an OSS defect or that production machines are healthy under occupancy. Its
serialized Reader totals and provider quantiles are not substituted for runtime
latencies below. A filesystem change must reduce dependent remote operations,
not merely shrink inode bytes or hide waits in the first user command.

## Retained real-runtime observations

All sixteen sandbox identities and all thirty-two complete phase observations
from [CRITICAL-PATH-BUDGET.md](CRITICAL-PATH-BUDGET.md) reproduce exactly. Each
sample immediately executes the real `node -v`, with the recorded successful
exit and output. This table describes the historical fixture, not fresh timing.

| Cohort / image, four samples each | Claim ms | First command ms | Claim through command ms |
| --- | ---: | ---: | ---: |
| Empty node / Coding | 1657.635–1675.950 | 989.994–1008.274 | 2665.827–2665.969 |
| Empty node / Node | 879.527–894.554 | 1122.712–1137.694 | 2017.228–2017.311 |
| Cached node, new identities / Coding | 703.085–718.481 | 562.145–577.526 | 1280.625–1280.653 |
| Cached node, new identities / Node | 538.638–556.588 | 266.299–284.240 | 822.914–822.956 |

Cold combined misses are still 8/8; cached-new misses remain 0/8. No improvement
or additional pass is claimed this turn. The fixture was instrumented private
regional TLS, 16CPU/64GiB, eight synchronized claims, unoccupied memory, sparse
Coding root and historical runtime/source pins. It is not populated-large-root
or occupied production-width acceptance.

## New calculation: subtract overlapping waits before claiming opportunity

Target only requests whose every 4KiB unit belongs to the exact retained XFS
inode/directory inventory: 995 Node units and 23344 Coding units. This is not all
filesystem metadata and does not distinguish lower from writable upper. Mixed
requests stay outside the target; other requests are not declared file-data-only.

For each exact sandbox and phase:

- Narrow target is these requests' payload source-wait union. Other waits retain
  their mapping waits and all source waits of other requests of that sandbox.
- Wider target includes both mapping and payload waits of those same requests.
  It does not include constructor, CPU, admission or unrelated request costs.
- Exclusive exposure is `union(target) minus union(other)`. Verify independently
  that `union(all waits) = exclusive exposure + union(other)`.
- Claim and command windows are disjoint; only then add their interval measures.
  Never multiply shared source cost by followers or add overlapping categories.

| Cold Coding combined opportunity | Payload only | Mapping plus payload |
| --- | ---: | ---: |
| Target wait union, ms | 705.055–707.062 | 903.040–919.871 |
| Overlap with other waits, ms | 41.725–44.683 | 41.725–44.683 |
| Exclusive wait exposure, ms | 661.674–663.330 | 858.356–875.198 |
| Combined minus entire target, other costs fixed, ms | 1958.816–1960.914 | 1746.072–1762.838 |
| Combined minus exclusive exposure, other costs fixed, ms | 2002.639–2004.269 | 1790.745–1807.521 |
| Fraction of exclusive exposure needed for 2s | 100.40–100.65% | 76.09–77.58% |

The uncorrected payload-only arithmetic needs 94.18–94.46% removal and leaves
only 39.086–41.184ms headroom before any new reads, decode, devices or mounts.
The overlap-corrected interval screen leaves all four samples slightly over 2s.
Even the uncorrected zero-payload claim remains 1423.883–1444.236ms, missing the
preferred 1s claim target.

The wider screen has 192.479–209.255ms headroom only if the entire exclusive
target becomes free. This does NOT establish that a compact filesystem can
remove 76–78%, let alone all of it. Required upper metadata and replacement
filesystem reads remain. Node's cold command has only 60.860–76.986ms of this
metadata-payload exposure, versus 974.071–976.494ms in no-inventory requests;
Coding-only metadata success cannot establish the general command requirement.

These are fixed-other opportunity screens, NOT causal savings or architectural
lower bounds. Some other waits can be speculative read-ahead; their overlap is
not proof they block the user process. Conversely target waits need not all lie
on its critical path. A new format changes requests/dependencies. The retained
trace cannot identify mandatory guest faults or reconstruct missing sub-stage
timestamps. Do not use the 2–4ms residual to claim a universal impossibility.

## Compact immutable base: source-contract audit, not a format decision

Freshly fetched authorities remain sandbox0 main `0f09220460581bfc1fdc331f34ebc85bf38381e7`
and infra main `af04ea978f62b6eceaa78da98840227505ca764b`. Source audit retains
main Git blobs, main SHA256, frozen-candidate SHA256 and matching line anchors.
The dirty diagnostic branch is not substituted for main architecture authority.

Current main and candidate both have one XFS filesystem containing lower, upper
and work; the lower is bind-mounted read-only, and OverlayFS uses `xino=off`,
`index=off`, `metacopy=off`, `redirect_dir=off`. One NBD session/branch and one
block-map generation encompass the durable filesystem. Device allocation and
terminal journals are exact-identity, not arbitrary mount discovery.

EROFS is an example candidate because it has compact 32/64-byte inodes, direct
inode addressing and compact directories. Those documented properties motivate
a hypothesis; they do not establish fewer OSS operations, support on the deployed
kernel, or 2s readiness. [Linux EROFS documentation](https://docs.kernel.org/filesystems/erofs.html).

Two possible embeddings must not be conflated:

| Untested embedding | Authority and device implication | Main additional risks |
| --- | --- | --- |
| Independent immutable base and writable XFS upper | Explicit atomic generation binding both durable references; separate unshared NBD attachments normally consume two slots | Descriptor/GC/retention integration, exact two-device ownership, mount/fence recovery, density and rebase |
| Immutable base image file inside the existing XFS wrapper, read-only loop mount at lower | Could retain one block-map generation and one NBD; loop is disposable derived state, not another durable authority | Additional loop/mount proof, XFS-file-to-image block translation, cache/read amplification and rebase |

The second is feasible enough to audit conceptually because regular-file loop
mounts are documented. Plain images are supported; double compression is not a
requirement. No image was built or mounted, no tool was installed, and native
new-kernel file-backed mount support is not assumed. [EROFS build and mount
documentation](https://erofs.docs.kernel.org/en/latest/mkfs.html).

Two NBD devices are therefore NOT an inevitable cost of EROFS. Conversely a
one-NBD wrapper is not free: extra block translation, buffered pages and loop
resources must be measured at occupancy, and it retains outer XFS mount work.
Do not assume all tenants share one lower to conceal the diverse-root cold case.

The lower may differ from the upper filesystem, but upper/work must share a
filesystem. Under `xino=off`, moving layers to different filesystems can change
inode/device properties, so unchanged guest and file-watch behavior requires
tests, not only a mount-success check. [Linux OverlayFS documentation](https://docs.kernel.org/filesystems/overlayfs.html).

### Concrete rebase and terminal contracts that prevent a mount-only patch

- `pkg/rootfsrebase/manifest.go` defines FIEMAP physical offsets relative to the
  exact backing block device. `DirtyFileRanges` intersects those offsets directly
  with branch dirty LBAs; its device/inode key deduplicates hardlinks but does not
  translate a different device's physical addresses into the branch namespace.
- `pkg/rootfssession/rebase.go` scans old/source merged trees and mounts three
  isolated branch roles. Supplying lower-image-relative offsets as NBD offsets
  can misattribute dirty file ranges. Each proposed embedding needs explicit
  upper/lower provenance and correct translation or exclusion before rebase can
  remain safe. This is an incompatibility risk in the proposal, not a newly
  demonstrated defect in the current single-XFS implementation.
- Current runtime journal and absence inspection cover NBD, XFS, lower bind and
  merged mount. An extra loop or separate base mount requires durable intent,
  exact binding, restart adoption, cleanup ordering and plugin-independent
  physical absence proof before resource or writer release.
- Snapshot/fork/pause must still publish one atomic durable generation; dirty
  upper state cannot remain node-only. Import, template-from-sandbox, restore,
  rebase and GC must agree on base identity and retention. A new encoding needs
  an explicit admitted format/compatibility contract, not a silent relabel.
- Imported base compactness says nothing about a heavily populated writable
  upper or long-lived snapshot. Do not replace the root-size requirement with
  only a clean-import or sparse-logical-size result.

## Next bounded decision and verification

Do not implement either embedding on the strength of inode size alone. First
resolve its exact block-address provenance and terminal ownership model. Then
measure actual complete metadata/data placement, mandatory lookup/read behavior,
mapping/encryption/decode and additional mount/device costs for both images.
The existing XFS NBD request stream cannot simply be relabeled as an EROFS trace.
Reject before runtime integration if those costs do not leave useful full-path
headroom or regress ordinary data efficiency, cached-new behavior or density.

This is distinct from the rejected block-neighbor packets: the hypothesis changes
filesystem metadata representation itself. It remains unproven and does not
re-admit previous candidates or authorize a parallel storage authority/FUSE path.

Seven helper tests / 1513 assertions pass, including a seeded independent bitmap
oracle for interval overlaps. All 32 full prior phase objects reproduce exactly;
12 prior budget artifacts, 164 trace artifacts and 1353 frozen product files
verify. Only experiment notes change. No remote/cloud query, startup run,
runtime patch, production rollout, merge, tag, data write or deletion this turn.
Last remote receipt remains historical 07:11:37UTC Stopped/StopCharging; it is
not a fresh cloud observation. All original acceptance gates remain open.
