# D-LAYERED-APPLY: logical source coverage through real Apply I/O

2026-09-13. Evidence: `/tmp/sandbox0-layered-apply.XlDmWp`.
Diagnostic compile overlays only; no production source or runtime change.

## Result and reason for this step

[CODING-LOOP-NET.md](CODING-LOOP-NET.md) establishes a local cold-read opportunity,
not permission to adopt the compact representation. Existing layered Diff and
regular-file resolver experiments were already complete. This experiment fills
an additional Apply prerequisite instead of repeating those scans or the layout
ABBA: logical source coverage now reaches validation, conflict comparisons and
actual file creation/copy, not only Diff planning.

The actual main Apply implementation is reused by a private policy hook in a
Go compile overlay. Exported `Apply` always uses the zero/default policy; its
Manifest/WorkerRequest/ApplyResult versions and legacy extent/proof semantics
remain unchanged. The existing layered model validates asserted source/old
request, role, base, lineage, coverage, device and block geometry. Its Diff policy
constructor is shared with Apply rather than independently reconstructed.

The added real-I/O cases prove these bounded properties:

- A1GiB logical lower-source file with only4KiB data and no physical extents
  copies exactly4KiB into an actual sparse target. Target size remains1GiB,
  allocated bytes4096, and the payload matches. Unmodified exported Apply
  rejects the separate-coverage request before creating a target file.
- An added lower file conflicting with target content is rejected before
  target mutation. An already-identical addition converges with zero writes.
- Tested lower-to-upper copy-up with colliding inode numbers preserves actual
  target hardlinks and copies the4KiB payload once; the old data region is a hole.
- A removed lower file cannot hide a target-content conflict behind an unrelated
  upper file with the same numeric inode. The target file remains intact.
- Disjoint sparse user/new-base changes both survive, with only4KiB source data
  read/written. Cancellation and five invalid authority/coverage/geometry inputs
  leave the target untouched.

These are actual temporary-file operations combined with **asserted** layer,
device and lineage observations. They are not native EROFS/gVisor mounts, an
authenticated scanner, or a populated1GiB startup test. Synthetic upper-device
offsets appear only in these model fixtures; no invented lower LBA is introduced
into production or the logical data copy.

## Changes and counterexample retained

The overlay moves allocation queries at every relevant Apply site: source-data
validation, whole-node equality, byte-range comparison union and file creation.
Replacement, source hardlink validation and the inode-survival removal shortcut
also consult the same layer identity scope used by Diff.

The first package race run passes72 test events, including subtest parents,
and skips the CAP_MKNOD whiteout test. Additional adversarial testing then
exposes a guard defect in the new diagnostic prototype:

| Input | Initial prototype | Qualified prototype |
| --- | --- | --- |
| Non-default coverage callback, omitted `layered` flag | Could produce an old v1 health proof | Rejects any non-default callback |
| Non-default identity callback, omitted flag | Same unsafe guard shape; not reached after first red assertion | Explicitly tested and rejected |

Preserve `proof-red.json` and the exact three-file `red/` snapshot. This is a
new prototype defect, not a production proof bypass: the exported entry accepts
no experimental callback, and no production implementation was modified.
The correction inspects active coverage/identity callbacks as well as the flag.
Diagnostic execution returns a private report, not ApplyResult or WorkerResult;
its input digest binds asserted request/views/dirty geometry, not authenticated
mount authority. Conversion/JSON interpretation cannot pass legacy validation.

Final `-race -count=3` for rootfsrebase/rootfssession passes588 test events,
including subtest parents, with42 explicit skip events. All passing test keys
appear three times. There are10 new top-level Apply tests,45 pass events including
five rejection subtests across the three rounds. Vet and test-binary build pass.
The session package takes270.862s for the three rounds; this is test execution
time, not sandbox cold-start latency. The same live handle was observed until
completion, not restarted for lack of partial captured output.

The42 skips are the same14 unique conditions repeated three times: one whiteout
case lacks CAP_MKNOD, eleven NBD/XFS cases/subcases lack a dedicated privileged
NBD device, and two RustFS cases lack an external test endpoint. These are not
privileged/integration passes. No local e2e or remote environment was started to
hide the missing coverage. The earlier remote whiteout result is separate
historical evidence, not a new run of this overlay.

## What remains before a real layered rebase

The target still uses the current `Scan` and extent interpretation. A real EROFS
target is therefore **not supported by this prototype**. Full target resolver/
logical coverage, non-regular entries, immutable nested-image binding, held-root
and quiesced-writer authority, final target verification and durable publication
must be integrated and tested together. No old proof check may be removed to
make the candidate pass.

Existing source sparse-hole representation differences, lower hardlink copy-up
behavior, gVisor identity/watch semantics, complete wrapper recovery/absence and
snapshot/fork/restore/rebase/GC contracts remain open. The temporary-file tests
do not settle any of those format decisions. They only prove that the reused
Apply core can carry separate source coverage without dropping payloads or
skipping the tested conflicts.

All original regional preferred1s/accepted2s readiness plus immediate literal
`node -v`, cold/cached-new identities, populated/history-bearing roots and actual
occupied production width gates remain open. There are zero new claims,
guest commands, physical mounts, remote object-store requests or startup samples in this experiment.
No startup speedup is claimed. Generic carriers, claim-time tenant selection,
stateless workers and unchanged request timeouts remain requirements.

The offline evidence validator passes2tests/13assertions, including five
negative mutations that reject hidden test failure, missing proof-guard coverage,
lost red evidence, failed vet or changed tested source.

All1359 frozen runtime source files verify; rootfsrebase still matches current
main0f092204. Existing user changes elsewhere are preserved. Only requested
experiment notes change in the repository. No deployment, production mutation,
merge, tag or data deletion. No cloud/remote operation occurred; the last
independent test-machine observation remains the prior00:54:36UTC
Stopped/StopCharging receipt, not a fresh cloud-state assertion.
