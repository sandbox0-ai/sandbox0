# D-LAYERED-TARGET: complete native layered target merge, not startup admission

2026-09-13. Diagnostic evidence: `/tmp/sandbox0-layered-target.4OzjM0`.
Go compile overlays only; product `pkg/rootfsrebase` remains identical to
`origin/main` at `0f09220460581bfc1fdc331f34ebc85bf38381e7`.

## Result and relevance

[LAYERED-APPLY.md](LAYERED-APPLY.md) covered logical source data but retained the
legacy target scanner. This experiment fills that target gap on the complete
retained Node and Coding EROFS trees, with three independent XFS upper clones
per image and a shared immutable lower. It does not replay the layout ABBA or
claim another latency improvement from [CODING-LOOP-NET.md](CODING-LOOP-NET.md).

The repaired diagnostic reuses the actual Diff/Apply core. A complete scanner
includes directories, symlinks and other nonregular entries, checks their held
merged mount identity, and resolves regular data identity before xattr-cache
lookup. User-visible metadata, including link count, remains from the merged
view. Immutable logical data coverage is separate from writable physical
extents; immutable lower extents never become upper dirty-LBA addresses.
Both initial and final target scans use the same complete interpretation.

| Native fixture | Paths before | Paths after | Regular / directory / symlink / FIFO before | Source payload read and written |
| --- | ---: | ---: | --- | ---: |
| Node | 7,301 | 7,303 | 5,776 / 1,009 / 515 / 1 | 8,198 B |
| Coding | 150,770 | 150,772 | 132,933 / 15,892 / 1,944 / 1 | 8,203 B |

For each full tree, controlled source and target edits in separate 4KiB ranges
of a1GiB logical sparse file both survive. The target has at most16KiB allocated
for that file; a separate newly added sparse hardlink pair shares an actual
inode and its payload matches. A real write to the retained lower
`etc/debian_version` copies it up and reaches the target with correct bytes.
Two added hardlink paths explain the exact final path-count increase.

An independent all-path rescan matches Apply's final manifest digest. A second
overlapping source edit is rejected in preflight; target manifest digest and
the previously independent target payload remain unchanged. The complete tree
is scanned, but only selected changed payloads are compared; this is not a
full-content hash of every writable target file or a populated1GiB root test.

## Changes, failures and correction

Keep all three campaigns and their exact binaries, scripts and outputs:

| Campaign | Actual outcome | Next change |
| --- | --- | --- |
| Initial | ARM64 test binary cannot load on x86_64 ECS; all three invocations exit255 before tests execute | Explicit `CGO_ENABLED=0 GOOS=linux GOARCH=amd64`, ELF architecture check, new owned clones |
| Qualified | Both complete tree scans reach Apply, which fails trying to open a directory as writable file data; socket mechanism fixture also exceeds AF_UNIX pathname length | Reproduce directory bug against unmodified main Diff; fix metadata classification in overlays; bind test socket through a held directory FD |
| Repaired | Both native full-tree merges/conflict checks pass; native mechanisms pass | Retain evidence; no additional replay or production adoption |

The directory failure is a pre-existing main logic defect, not proof that
EROFS changes file data: `metadataDelta` treats a directory's filesystem-owned
`st_size` change as a user file length change, and Apply attempts to open the
directory for truncation. The minimal test against **unmodified main Diff**
fails exactly once. A regular-file size control passes. The correction includes
`size` only for regular files, preserving actual directory mode/owner/mtime
changes and regular-file truncation semantics. It is consistent with the
existing whole-node comparison, which already excludes directory size.

The isolated default-policy correction passes60 test events with3 explicit
whiteout skips across three race rounds. No product file is patched: the
original main source and the proposed correction are retained separately in
`metadata-fixed/`. The repaired layered overlay uses the same correction.
The failed operational Apply targets are retained and never retried in place.

The long-socket-path failure is a test harness issue, not a filesystem failure.
The socket stays in the exact XFS temporary directory and remains in the full
scanner coverage; a held `/proc/self/fd/<fd>/socket` pathname avoids the Unix
socket path-length limit. It is not skipped or moved to a different filesystem.

## Tests and exact scope

Final rootfsrebase/rootfssession `-race -count=3` passes630 test events with99
explicit skip events, including subtest parents. Every passing key occurs
three times. The33 unique local skip conditions include unavailable privileged
XFS/mount/NBD/whiteout fixtures and RustFS endpoints; they are not integration
passes. Native XFS mechanism execution separately passes37 events and skips
one non-XFS-host profile case because TMPDIR is already XFS. Child file and
directory bind-mount rejection, nonregular traversal, xattr-cache ordering,
target conflict coverage and legacy-proof rejection are exercised.

The native full Node/Coding cases take6.12s/158.07s to perform fixture setup,
multiple complete metadata scans, Apply and adversarial checks. These are
**offline test durations, not claim or first-command latency**. This scanner
is not wired into the claim hot path. Runtime request budgets stay10s.

Source/target coverage callbacks and layer identity remain private diagnostic
policies. Any nondefault policy is barred from issuing the old ApplyResult
health proof, even if the caller omits its experimental flag. No invented
WorkerRequest, artifact digest, trusted lineage or writer authority is used.
This is a native **same-base** three-way fixture, not authenticated rebase
between distinct immutable artifacts or a manager generation publication.

## Preservation and remaining gates

The existing retained EROFS images and original XFS uppers are hashed before
and after every campaign. Historical ordinary XFS images retain exact inode,
size, mtime, ctime and allocation metadata. All18 working upper clones and all
diagnostic reports are preserved. No original data, Nomad job, binary or
database row is changed. The latest physical absence check examines227 process
mount tables; all owned mounts and loops are absent and all64 NBDs detached.
Job index73058, original process identities and2CPU/8GiB shape are preserved.
Shutdown is tracked by the single `stop-once` process and independent cloud
instance receipts; a `Stopping` observation is retained, not treated as stopped.
The independent final observation at `2026-09-13T01:59:44.706532344Z` confirms
`Stopped / StopCharging / ecs.g9i.large / 2CPU / 8192MiB`, with no public IP.
The owned SSH master is explicitly closed before stopping compute.

The offline evidence verifier checks export checksums, preserved input/runtime
identities, every campaign's failures, native results and final cloud state.
Its two tests pass12 assertions, including nine deliberate corruptions of
checksum, cleanup/runtime identity, native scope, claim/path count, conflict status,
digest or process exit. Raw exports and the final evidence are retained with a
SHA256 artifact inventory; no failure is replaced by a later passing record.

All1359 frozen product files remain unchanged. No production rollout, PR,
merge or tag occurs. The experiment records **zero claims, guest commands or
startup samples**. There is no tenant prewarm, command training, S3 publication,
image import, timeout increase, cache increase or machine resize.

This is progress toward the candidate layout's data correctness, not adoption.
Still open: authenticated different-artifact binding and durable lifecycle;
the previously observed sparse-hole representation loss and existing lower
hardlink copy-up behavior; gVisor identity/watch behavior; complete
snapshot/fork/restore/rebase/recovery/GC qualification; genuinely populated,
history-bearing large roots; cold-node and cached-new identities; regional
ingress to authenticated readiness and immediate literal `node -v`; actual
occupied production-width acceptance at the original preferred1s/accepted2s
gate. The compact layout's cached-command regression gate remains open too.
