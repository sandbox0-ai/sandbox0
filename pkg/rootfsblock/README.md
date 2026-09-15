# RootFS block-map formats

The default durable importer remains on format one. Manager's
`rootfs_importer.format_generation: 2` opts new image imports and claims into
directly addressed compressed ranges after all manager, ctld and driver readers
have been upgraded. This is not a completed production rollout or cold-start
acceptance result. Existing sources retain their committed format even when the
new-image policy changes. The new-image policy does not admit the
private diagnostic format 10005 or depend on its object namespaces and overlays.

## Address and integrity domains

The descriptor and each mapping page are immutable, checksum-bound inputs.
Envelope encryption remains owned by `pkg/objectstore`; compression is applied
before that envelope. Range offsets address the decrypted object payload, not
ciphertext offsets.

| Field | Meaning |
| --- | --- |
| `ObjectRange.Key` | Complete immutable object key; published keys hash stored bytes before envelope encryption. |
| `Offset` | Physical range start within the decrypted object payload. |
| `Length` | Exact decoded range length. |
| `Checksum` | SHA-256 of the complete decoded range. |
| `Encoding` | Empty for raw bytes, or `zstd`. |
| `EncodedLength` | Physical range length for zstd; strictly positive and smaller than `Length`. Zero for raw bytes. |
| `MappingEntry.DataOffset` | Block-aligned offset within a decoded data range. The logical view spans `BlockCount * 4096` bytes. |

Mappings carry physical addresses directly. A first demanded data range needs
no separate pack index or whole-pack download. Object inventory references
continue to hash and count complete **stored** objects; they must not count
decoded range sizes as physical storage.

## Versioning

Version-one descriptors, mapping bytes, default build options, and publication
identities remain unchanged. Its ranges are raw and have no decoded view offset.
Zero `BuildOptions.FormatVersion` selects this contract for standalone builders
and remains omitted from legacy canonical import operation inputs. Durable
imports derive an omitted builder format from `format_generation`: generation
two selects format two, including after scanning existing PostgreSQL columns.
An explicitly conflicting builder format is rejected. There is no second
persisted format authority.

Format two uses descriptor version two and mapping locator/page version two.
The existing 32-byte mapping header retains its magic and records version two
in bytes 8..9. Its entries retain the original 64-byte prefix and append:

| Entry-relative offset | Field |
| --- | --- |
| 64 | Codec: zero for raw, one for zstd. |
| 65..67 | Reserved, must be zero. |
| 68..71 | Encoded length, big-endian uint32. |
| 72..75 | Decoded data view offset, big-endian uint32. |
| 76..79 | Reserved, must be zero. |
| 80 onward | Object key, using the existing prefix's key length. |

Root and child mapping versions must match the descriptor. Legacy pages cannot
carry compressed entries, and internal-page entries cannot select data views.
Raw/encoded lengths, view alignment, page coverage, ordering, reserved bits,
checksums, and bounded zstd output are all validated before use.

Format-two new data ranges default to at most 64KiB. Mapping payloads retain
their 8MiB decoded bound. Writers use independent zstd frames with a 64KiB
window and keep incompressible input raw. Decoders have a bounded window and an
exactly capped output; the decoded checksum remains mandatory after envelope
authentication. Bulk demand can fetch adjacent raw and compressed ranges in
one bounded physical GET and verify each decoded range independently.

## Lifecycle and admission

Incremental publication inherits the base format and rejects an implicit
upgrade/downgrade. A batch contains only one format. Splitting a compressed
range reuses its original physical locator and full decoded checksum, adjusting
only `DataOffset` and logical coverage. Adding a decoded offset to a compressed
physical offset would corrupt the branch. Changed-block comparison accounts
for these decoded views without reading data objects.

Composite tails, incremental roots, and batch materialization preserve the
version-two descriptor. Import operation inputs, ready-artifact attestations,
and node handoff must match compressed format two to format generation two;
compressed bytes cannot masquerade as a legacy runtime assignment.

Incremental and batch materialization copy only affected mapping paths. They
retain untouched child locators, split overflowing pages, create paths for
previously sparse gaps, and remove emptied children. Raw partial ranges still
need their bytes to compute new checksums; compressed decoded views and fully
replaced ranges need no old data download. New raw data ranges stop at edited
leaf boundaries while continuing to share bounded data packs. Both publication
paths use the same tree editor; batches pack edited pages across members at
each dependency level and share one bounded verified read cache.

`MaxEditedMappingEntries` bounds touched base entries plus dirty blocks in one
edit. It is not a limit on the total extent count of the durable generation.
Published page payloads and processed level entries are released as parents
consume their locators. Per-operation limits and shared caching do not by
themselves prove an aggregate node RSS or production concurrency budget.

Full-image builds stream mapping leaves and retain at most one fanout of child
locators per tree level. Completed mapping payloads are released after their
locators enter the frontier; the importer no longer accumulates one entry for
every nonzero range. A full frontier is held until a successor proves it is not
the root, preserving the original page grouping, sparse coverage, checksums,
object keys, descriptor and retry identity in both formats. Data and mapping
PUTs may interleave, but each still crosses the existing durable import journal
before publication. An interrupted map PUT/completion can replay the same
immutable objects without returning a ready descriptor prematurely.

The durable OCI importer calls `BuildMaterializedFileGeneration` only after
unmounting and opening its exclusively owned XFS image read-only. Linux
`SEEK_DATA`/`SEEK_HOLE` can omit full canonical units proven to be file holes,
using one current extent rather than a list proportional to the logical size.
Units touching data retain their exact original global or file-relative
boundaries, including zeros at their edges. Object bytes, mapping pages,
descriptors, journal entries and retry identities are unchanged in both formats.
The input's position is restored, and detected size/mtime/ctime changes or I/O
errors discard the result. The caller must still exclude all writers; metadata
checks are not a lock or a snapshot. Unsupported hole discovery falls back to
the ordinary full read. The ordinary `ReaderAt` entry points never trust sparse
extent hints, and no live node reader, readiness gate or timeout is changed.
This bounds sparse import scanning, not populated-image cost or startup latency.

Each sequential full/incremental/batch builder owns and closes one reusable
range encoder. There is no new global codec cache or per-range codec instance.
Sparse zero ranges reuse their input buffer. Live mapping memory is bounded by
fanout and depth, in addition to bounded pack/codec buffers; the final inventory
still retains one reference per distinct immutable object. That inventory,
concurrent builds, OCI extraction and XFS staging are separate memory/disk
costs, so the streaming bound is not constant total import memory or a node RSS
guarantee.

The existing node-shared `ReadCache` owns source admission for both formats.
Source slots cover transport, decoding, checksum verification, and cache
admission. Cache identity is decoded checksum/length, so raw and compressed
copies of identical content may safely share verified bytes. Decoded subviews
do not get separate cache entries that undercount a retained whole range.

Recently used decoded mapping pages can occupy a protected segment of at most
one eighth of the same cache budget, capped at 16MiB (within the default 128MiB
total). Protection does not allocate another cache or preload tenant data.
Displaced mapping pages can borrow ordinary LRU space; pages larger than the
protected budget use ordinary LRU eviction. Encoded buffers, decoded entries and
cache overhead remain charged once, and every parent/child binding is checked
even on a hit. This prevents bulk data scans from evicting a fitting mapping
working set, but does not make a mixed-image data working set fit in memory or
guarantee claim/command latency.

### Node disk tier

`NewReadCacheWithDisk` optionally adds a node-owned disk tier below the memory
LRU and above the object source. All session readers on that node share it.
Keys use the decoded SHA-256 and length, so immutable bytes can be reused across
object locations, generations, and raw/compressed encodings. Descriptor and
parent/child validation still runs; writable branches and composite-tail
overrides never enter this cache.

The directory contains plaintext and must be dedicated host-private storage,
outside every sandbox mount. It is restricted to mode 0700, holds a single-owner
lock, and rejects symlink paths. Every disk hit verifies length and checksum;
missing, corrupted, or unreadable entries fall back to the authenticated object
source. A disk error cannot turn invalid data into a successful read.

In ctld's `nomad_runtime` configuration:

```yaml
nomad_runtime:
    read_cache_bytes: 134217728
    read_cache_directory: /var/lib/sandbox0/ctld/read-cache-v1
    read_disk_cache_bytes: 8589934592
```

Memory defaults to 128MiB when omitted. Disk caching is opt-in: its directory and
positive byte budget must be supplied together. The disk budget counts payload
bytes, not filesystem metadata or allocation rounding. At most 131072 ranges
are indexed, independently bounding metadata even with tiny mapping entries.
Eviction follows process-local LRU order; restart reconstructs bounded occupancy
without persisting per-hit recency. This disposable tier is never a durability
source for user writes.

Verified fills use a non-blocking queue capped at 32 entries and 8MiB including
the active write. Duplicate pending fills share one queue entry. Saturation or
disk failure skips cache fills without failing the demand read. Atomic rename
publishes completed files; no cache fsync extends the claim path. On process
restart, completed entries are reusable and an incomplete staging file is
discarded. Host crashes may lose cache data; checksum validation handles torn
files on their next use. `Close` drains accepted fills and releases ownership.
`ReadCache.Stats` reports hits, misses, writes, errors, dropped fills, occupancy,
and queued bytes. Local disk reuse is distinct from a fresh node with no cache;
neither a new sandbox nor clearing guest page cache establishes the latter.

## Remaining acceptance

An explicit `BuildMaterializedGenerationWithLayout` entry point can partition
immutable image bytes into file-relative complete units without changing their
physical filesystem placement. A `DataRangeLayout` stores compact preferred
spans, rejects overlaps and binds the image size and unit (at most 64KiB).
Each build owns a sequential cursor; residual bytes return to global boundaries.
The existing codec, bounded packs, streamed map builder and inventory are reused.
Initial raw and compressed entries always reference complete payloads, so this
does not relax raw-view checks or add another cache. One layout retains at most
16MiB of span storage, not constant total import memory.

The ordinary builder's publication bytes and default import format remain unchanged.
The optional `rootfsartifact.XFSBuilder.BuildWithDataRanges` entry point discovers
complete file-relative units in a read-only, exclusively owned 4KiB-block XFS
image. Root-confined batched traversal does not follow symlinks or cross devices.
Eligible hardlinks are deduplicated by inode; extents with any flag other than
LAST (including shared or unwritten extents) retain ordinary global segmentation.
The scanner never replaces their raw bytes with synthetic zeros. Unexpected
overlap, malformed geometry, more than 10,000,000 entries or 1,048,576 extents,
more than 1,048,576 tracked hardlink identities or preferred spans, directory depth
above 256 or paths above 4096 bytes are errors, not successful partial plans.
These limits bound discovery state, not total importer memory or file size.
The image is cleanly unmounted and verified before a plan is returned for raw
publication. The standalone collector requires the caller to enforce that same
exclusive ownership and unmount boundary.
This raw-read boundary follows the [Linux FIEMAP contract](https://docs.kernel.org/filesystems/fiemap.html).

The opt-in `rootfs_importer.data_layout_policy: xfs-file-ranges-v1` policy binds
format 2 / 64KiB to the durable operation, chooses `BuildWithBoundedDataRanges`,
and emits a version-2 attestation including the policy and any budget fallback.
On a typed budget error it discards the entire plan and uses the ordinary block
builder, preserving input support and explicitly recording `scan-budget-exceeded`.
Unsafe metadata and I/O/cancellation errors never trigger this fallback. Source
selection matches the exact policy; digest-bound existing generations remain
unaffected. A PostgreSQL fence rejects old-worker publication of a legacy result
for a new-policy operation. Empty policy retains old operation and attestation
bytes and global-grid selection. All managers must be upgraded before enablement.

Complete-image cost and startup validation remain required before activation.
A descriptor alone is not an attestation that a particular import policy was
applied, and an attested fallback is not an optimization hit. Selected-window sharing results do not
establish actual startup cache-hit rate or a latency guarantee.

Default importer/claim activation, runtime XFS/OverlayFS lifecycle acceptance,
regional-ingress cold/cached-node measurements, first `node -v`, and
production-width density acceptance remain required before rollout. A pure
range/codec test is not a sandbox startup measurement.

The path-copy regression includes a 1TiB logical model with 16,777,216 mapped
64KiB extents and real checksums for generated mapping pages. Two disjoint
updates read only five mapping pages. Repeated data shares one compressed
object: this is not a populated XFS image or a remote startup/RSS measurement.
The streamed-map scale regression constructs all 16,777,216 extents and publishes
real mapping pages while reusing a synthetic data locator. It is still not a
populated XFS image or a remote startup measurement. Changed-block comparison
has separate traversal bounds. Populated import memory, mapping depth,
privileged lifecycle behavior, and production-width startup must still be
covered before enabling this format as the production default.
