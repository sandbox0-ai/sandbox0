# D-PERSISTED-CONFIG: repair the persisted recovery contract

2026-09-13 Asia/Beijing. Local implementation following D-POOL-BOOT, not a new
startup performance result or production rollout.

## Change and observed effect

The new normal regression first fails with `decode recovered task config: EOF`
after Nomad LocalState persistence; the direct-memory control passes. The fix
explicitly saves normalized `DriverConfig` in opaque driver state and advances
task handles from version1 to2. Recovery no longer reads the private Nomad raw
configuration field that disappears during persistence.

Missing command/security class, unknown or legacy handle version, malformed
state, task/allocation/node/namespace/netns-path mismatch, and unexpected derived
container/bundle/root paths all reject recovery before exposing a task. Recovery
does not apply StartTask defaults. Newer node-local lifecycle state can update
phase/claim, but must match the validated immutable identity and configuration;
corrupt or conflicting local state cannot silently fall back to an older phase.

Existing regional boot/netns-incarnation checks and one-shot carrier policy are
unchanged. Version1 handles deliberately require normal carrier replacement;
this is not an in-place legacy migration or a promise of process continuity.
The already-implemented manager post-terminal scheduling fix is preserved, not
duplicated. Public driver and self-hosted operational notes describe this boundary.

## Verification

- Full independent driver module `go test -mod=readonly -race -count=1 ./...`
  passes on final source: driver, rootfsbuilder, copyup workload and RootFS
  workload packages; packages without tests remain explicitly separate.
- Recovery tests repeat five times under race: codec round-trip and heartbeat,
  exact idempotency,23 invalid configuration/identity cases,10 conflicting local
  state cases, changed boot and same-path changed netns, corrupt opaque state,
  newer matching lifecycle state, privileged configuration and snapshot isolation.
- An additional actual Nomad BoltStateDB write/close/reopen/read regression
  recovers the same handle and resumes heartbeat. Its dedicated three repeated
  runs and the final suite pass. No Nomad daemon, guest or local e2e is launched.
- Both manager runtimeslotnomad and runtimeslotreconciler race suites pass.
  Final driver-module vet and builds pass. Go module requirements/sums are unchanged.
- One initial guard assertion incorrectly expected fake runsc Version to record
  an operation. The fake does not record Version; actual tracked operations are
  empty. The corrected assertion requires zero operations. Both failed receipts
  remain alongside the successful results; no product behavior was relaxed.

Tests use the driver module's existing Nomad1.11.1 dependency. The remote daemon
is1.11.3 and has not run this candidate. Boot/netns tests use an exact-registration
authority double; they do not replace real remote authority/refill qualification.

## Candidate and provenance

The previous1353-file inventory is historical after this intentional change.
The new inventory has1355 product files: five changed existing files, two new
recovery source/test files, and1348 unrelated existing files unchanged. Dirty
status moves193→198; user changes in the already-dirty documentation remain.
No commit, merge, tag, API regeneration or other repository change is made.

The initial native arm64 debug build is retained only as a local compile check.
Use the separately built **linux/amd64** binary for the remote worker. It matches
the previous remote candidate's Go1.25.5, CGO0, trimpath and stripped build flags;
there is no diagnostic overlay.

- Artifact root: `/tmp/sandbox0-persisted-config.5erDgE`.
- Current source: `verified-candidate.source.json`, SHA256
  `3aba811aa5a207c143f7e23ac32d338db9545449d372e6dfd18bb0f7459c200b`.
- Remote-target binary: `nomad-driver-sandbox0-linux-amd64`, SHA256
  `ed647ff9627156b43a212e4882c0cd920dda418b14435e22d8debdfab8630899`.
- `evidence.json` records local qualification; `evidence-final.json` adds the
  correctly targeted remote artifact and supersedes its native-build handoff.

## Remaining gates

No remote operation, object request, claim, command, prewarm, timeout change or
performance sample occurred. The last full regional cold combined maxima remain
Node2.221s/Coding2.668s; do not credit this recovery fix with a measured speedup.
Populated large roots/upper histories and actually occupied machine width remain
unqualified. All original architecture, durable-authority and1s/2s gates remain.

Next remotely verify two distinct paths with the candidate manager refill fix
and the new driver: same-boot Nomad client restart must recover persisted handles;
a new boot must reject old incarnation binding and replenish with new allocations.
Retain original environment backups, source/artifact hashes and all failed
observations. Only then resume regional cold/cached-new first-command comparisons.
Do not use the old frozen1353-file auditor as a current-candidate verifier, or
repeat the rejected object-size-only S3 sweep.
