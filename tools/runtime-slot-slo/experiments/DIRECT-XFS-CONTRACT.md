# D-DIRECT-XFS-CONTRACT: direct branch projection

2026-09-13. Native filesystem prerequisite, not a startup result or layout
adoption. Evidence: `/tmp/sandbox0-direct-xfs.DKqRyi/evidence.json`, SHA256
`1b3cb1bc4461a2986567cd26621b15d49226f75913bc3ece85fc4668378a3a90`.

## Question and decision

The actual current implementation mounts one block-COW-backed XFS containing
lower/upper/work, then exposes an OverlayFS merged tree. Can an isolated branch
instead expose its XFS lower tree directly while retaining the existing block
generation and file-aware rebase primitives? This is a different hypothesis from
the rejected EROFS wrapper, larger caches, fragment workers and broad ELF prefetch.
It may avoid the second mount, upper/lower lookup work and file-level copy-up.
There is no measured cold-start saving yet. Historical Overlay mount time alone
(116-118ms in one prior fixture) is not sufficient evidence for the full 2s gate.

The bounded native prerequisite passes. It justifies a matched real-root read
cost experiment, not a production mount change. All original requirements remain:
tenant-neutral warm carriers, claim-time RootFS selection, no tenant prewarm,
10s public requests/no POST retries, actual first literal `node -v`, distinct
cold and cached NEW identities, populated/history roots and occupied node width.

## Actual scope and frozen code

- Refetched sandbox0 main `0f09220460581bfc1fdc331f34ebc85bf38381e7` and infra main
  `1bdd57f2c41144b6da60ec7f07ab295eedca0ca7`. Main has the same XFS/Overlay projection
  and generic Scan/DirtyFileRanges/Diff/Apply path; the worktree is not substituted
  for the main architecture authority.
- The diagnostic adds only a temporary Go test via an overlay. All 1,362 product
  files and the complete file set match the previous frozen inventory before
  and after. No service binary, runtime default, artifact or API is replaced.
- Dedicated remote ECS remains 2CPU/8GiB, kernel `6.8.0-124-generic`, private test
  mount namespace. Explicit nbd61/62/63 are outside ctld's configured nbd0..15.
  No gVisor guest, manager claim, regional request or command runs in this test.
- The fixture is a newly generated 512MiB XFS image with small real files and a
  64MiB sparse file. It is not a populated large-root or performance sample.
  Actual compressed-format2 Reader, envelope encryption/RSA, Branch, kernel NBD,
  XFS and incremental publication run against an encrypted **MemoryStore**.
  That proves the exercised block reconstruction, not remote S3 durability or
  response latency. Retained source image files are not wired into the Readers.

## Verified behavior

Three independent branch identities initially reference the same immutable
mapping root. Each has a separate WAL, device, XFS mount and empty read/header
cache. The diagnostic bind exposes lower as writable XFS with nosuid/nodev/noatime
mount requests; it does not mount OverlayFS. It rejects nonempty upper/work and
symlinked layout directories before binding. These are fixture guards, not a
production format admission or a complete security audit.

The native test executes and checks:

- An in-place 4KiB update, user xattr, hardlink rename, addition and deletion.
- A 4KiB sparse hole punch, an independent 4KiB write at 32MiB, and truncation
  from 64MiB to 48MiB. Bytes, logical size and bounded allocated blocks agree.
- Generic three-way rebase through the **unchanged** production Scan,
  DirtyFileRanges, Diff and Apply implementations. An independent target prefix
  and target-only file survive; source changes and hardlink identity are applied.
  This fixture deliberately restores the target data file's old mtime so its
  byte-only update does not create a conflicting metadata edit.
- The independent old branch still has the original payload and deleted file,
  and lacks the source addition. No mutable lower is shared across branches.
- After clean source unmount, its exact WAL is reopened and checkpointed;
  incremental immutable publication produces a different mapping head. A NEW
  branch identity with a NEW empty WAL and new caches reconstructs the modified
  filesystem from that head. This is not the manager's named snapshot/fork API,
  a cross-process durable-store restart, or a running-guest checkpoint test.

The exact diff has six changes. Source payload is 4,096 + 4,096 + 11 = **8,203
bytes**, independently matched to Apply's source read count. Old and target reads
are 8,195 bytes each; target writes are 8,203 bytes and hole punching 4,096 bytes.
The rename refers to the modified hardlink, without a second full payload copy.
An independent JSON verifier reproduces the diff digest and the expected sparse
offsets and dispositions. Do not interpret this as an 8,203-byte startup budget.

Native tests pass with zero skips. Source/fresh-published attachments observe
116/110 kernel reads, 26/35 Reader range calls and 2/3 unique key unwraps; each
key unwrap occurs once per attachment. These are synthetic transport counters,
not HTTP GETs or a before/after speed comparison. The 0.56s test duration includes
fixture work and is explicitly **not** readiness or claim latency.

Local guard race tests pass three repetitions; rootfsblock, rootfsrebase and
rootfssession unit suites and vet pass. Local privileged tests are not run.

## Exclusions and next gate

An existing Overlay generation may contain modifications, whiteouts, work state
and merged-tree semantics in upper. Binding lower directly would lose that
state. Empty upper alone also cannot grant production conversion authority.
Any eventual format needs an authenticated layout discriminator, import and
materialization rules, old-upper migration, compatibility class handling,
journal recovery/absence proof and all lifecycle/security checks. No format2
descriptor is silently reinterpreted by the product in this experiment.

Next measure the **same** complete real ordinary XFS artifacts with only their
projection changed in a strictly isolated diagnostic runtime: actual encrypted
OSS/Reader/COW, stock runsc/procd, immediate literal `node -v`, cold and cached
NEW identities, unchanged caches, timeouts and concurrency. Do not use a readonly
guest, zero-cost local backing or absent upper data to claim end-to-end success.
Both Node and Coding must be measured; full regional and density gates follow
only if the net read/latency effect qualifies. Existing history artifacts remain
ineligible for a direct-lower bypass, not silently excluded from the final goal.

## Cleanup and evidence

The systemd unit exits successfully. Independent host-wide mount inspection,
loop inspection and all64 NBD checks prove test resources absent. All original
service PIDs/hashes, configs, job77379, source inventories and DB histories remain
unchanged; retained test-history DB stays2174 rows. The final original pool reports
**one** ready carrier, not two; no job repair or healthy two-carrier assertion.
All new scratch inputs/WALs/reports and previous `/data` caches are retained.

The skill's audited lifecycle targets start/stop only the diagnostic ECS; no
legacy Kind deployment is used. Fresh cloud receipt at11:48:23UTC confirms
2CPU/8GiB, `Stopped/StopCharging`. SSH master closes intentionally (its process
exit255 is retained separately from successful control-close and test exit0).
Eight remote report exports and52 local artifacts are checksummed. No production,
merge, tag, local e2e or performance/default adoption. The full goal stays open.
