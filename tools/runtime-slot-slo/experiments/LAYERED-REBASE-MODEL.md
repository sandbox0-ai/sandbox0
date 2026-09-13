# D-LAYERED-REBASE-MODEL: logical coverage and writer attribution

2026-09-12. Local evidence: `/tmp/sandbox0-layered-rebase.OeoO1h`.
This is an isolated algorithm prerequisite following the two real images in
[COMPACT-BASE-FEASIBILITY.md](COMPACT-BASE-FEASIBILITY.md), not a selected format,
runtime implementation, authenticated mount scanner or cold-start result.

## Result and implementation boundary

A Go overlay reuses the existing `Diff` implementation with explicit logical
coverage and inode-identity scope callbacks. Its default path retains the legacy
single-filesystem interpretation. No fields are added to `Node`, Manifest v1,
WorkerRequest or ApplyResult; all 1353 frozen product files remain unchanged.

The diagnostic layered input carries known data/hole intervals separately from
physical extents. Only a private writable-device projection reaches existing
`DirtyFileRanges`; the logical diff never receives that projection. Lower inline
offsets therefore cannot become writer LBAs, while a new lower file can still
contribute real source data without inventing a physical extent.

Regular-file identity comparisons distinguish lower and upper layers. This
covers the tested copy-up collision, prevents a lower-to-upper inode-number
collision from becoming a rename, and retains tested upper hardlink/rename
behavior. Unknown non-regular stable identities are rejected; directory-layer
provenance is not silently guessed.

Inputs validate the existing WorkerRequest/digest, asserted expected role,
mount/boot/device records, base artifact, lineage, coverage, extent flags,
hardlink consistency and branch geometry. These records are **asserted local
test inputs, not authenticated kernel or ctld proof**. The existing base artifact
contract does not yet bind an EROFS image nested inside a durable wrapper.
There is no scanner, new authority, wire protocol or durable publication here.

The output is a private diagnostic plan with no Apply/WorkerResult conversion.
Tests confirm legacy Apply validation still rejects both separate-coverage data
and cross-layer replacement plans. Do not remove those rejections: Apply also
uses extent-derived allocation, replacement checks and `inodeSurvives`, and its
health proof only binds the existing manifests. A Diff-only adapter is not a
complete or safe format migration.

## Reproducible tests and failure record

The baseline package has 18 passing tests and one privileged whiteout test
skipped. The initial overlay has 51 passing Go events, including subtest parents,
and the same skip. Its tested cases include:

- Added sparse lower data with no physical map: 4096 source bytes retained;
  unmodified legacy Diff on that hypothetical input reports zero.
- Identical lower/upper physical offsets: only the writable file is dirty.
- Copy-up with equal inode/generation numbers, including unknown generation:
  replacement preserves data coverage and hardlink source reads occur once.
- Same-layer upper rename versus cross-layer numeric identity collision.
- Already-resolved merged deletion. This is not a kernel whiteout test.
- Missing/unknown coverage, unsupported flags, device/request/base mismatches,
  overflow, out-of-file ranges and contradictory hardlink observations reject.
- An independent deterministic 256-case cell-state oracle verifies data/hole
  changes, growth and truncation. Its integer states distinguish holes from
  allocated data; it is not an EROFS sparse-layout or filesystem I/O proof.

Additional adversarial tests deliberately expose six missing guards in the
initial prototype. Preserve `adversarial-red.json`, its exact source hashes and
the `red/` source snapshot; do not rewrite it as a passing run.

| Counterexample | Initial result | Corrected prototype |
| --- | --- | --- |
| Wrong dirty block size | accepted | reject against request descriptor |
| Dirty LBA outside branch | accepted | reject outside exact geometry |
| Dirty list over request limit | accepted | reject before attribution |
| An old upper device reused as source lower | accepted | reject cross-role alias |
| Unscoped directory stable identity | could infer rename | reject unsupported provenance |
| Lower inode changes coverage while renamed | path-only check missed it | compare immutable observations by inode |

These are candidate-model defects, not evidence of equivalent bugs under the
current single-XFS production contract. The correction also bounds upper
physical extents to the validated branch size.

`adversarial-green.json` records 58 pass events and one skip. Final `-race` tests
of `pkg/rootfsrebase` and `pkg/rootfssession` both pass: 181 pass events including
subtest parents, with 14 explicit skip events. Never call that privileged or
end-to-end acceptance. Skips cover:

- One whiteout test: local environment lacks CAP_MKNOD.
- Eleven privileged NBD/XFS tests or subtests: no dedicated
  `SANDBOX0_PRIVILEGED_NBD_DEVICE` was supplied.
- Two RustFS integration tests: no `SANDBOX0_RUSTFS_ENDPOINT` was supplied.

No capability escalation, local e2e, remote device allocation or external test
endpoint was introduced to turn these skips into passes. The complete raw test
events and final source hashes are retained. Baseline/first-overlay receipts
predate automatic per-run hash capture; the preserved `v1/` source snapshot was
taken before the adversarial edits. Do not describe those first receipts as
having contemporaneous automatic input hashes.

The first evidence-seal attempt fails its equality check because it excluded
the top-level analysis timestamp but not the nested audit timestamp. A read-only
comparison isolates that sole substantive-field-container difference to
`preservation.recorded_at`. Preserve `SEAL-FAILURE.md`; exclude only the two
validated timestamps on comparison, retain all hashes/results and verify that
a changed product-file count still fails equality. No experiment/test rerun or
analysis rewrite is needed for this bookkeeping correction.

## What this does not prove, and the next experiment

No claim, guest command, required-read trace or latency sample was collected.
The existing machine/OSS evidence remains relevant; this prototype does not
exonerate either or turn image-byte savings into a startup speedup. RootFS is
still only selected at claim, without tenant root prewarming or a persistent
worker dependency. All original regional-ingress-to-procd, immediate `node -v`,
cold/cached-new identity, populated root/upper-history and occupied-width gates
remain open.

Next use the retained exact images to qualify actual merged-mount provenance,
copy-up, inode/device, sparse, whiteout and hardlink behavior. Cover the skipped
privileged cases in the authorized remote environment. Then evaluate a complete
durable embedding, including outer filesystem, loop/lower mounts, encryption,
mapping, extra cache/memory and terminal absence/recovery costs. Measure the new
representation's own required reads; do not relabel historical XFS traces.

The compact format is still unchosen. Stop or redesign if provenance cannot be
bound without weakening safety, semantics regress, or total cold/occupied costs
do not support the original gate. Do not advance merely because this local
model passes.

## Preservation

Reverify all 134 prior compact-image artifacts, original trace evidence and all
1353 product files/path inventory. Main refs checked, not fetched in this local
turn: sandbox0 `0f09220460581bfc1fdc331f34ebc85bf38381e7`; infra
`af04ea978f62b6eceaa78da98840227505ca764b`. The rebase package still matches main.
Only experiment notes change in the repository; no rollout, merge, tag or
deletion. No remote/cloud operation occurred. The last observed test-instance
state is the prior 08:07:55UTC Stopped/StopCharging receipt, not a fresh claim
about remote state. Retained images and the staging disk were not accessed.
