# D-SESSION-OWNER-RESET: preserve ownership before optimizing activation

2026-09-13. Evidence: `/tmp/sandbox0-session-owner.M6Ksu5/evidence.json`.
The previous session-scope experiment was progress. This turn repairs an
ownership prerequisite found while evaluating bounded activation, not a
performance improvement or a session-format migration.

## Reproduced failure

The manager sets `ResetCopiedSessionState` when an exact runtime assignment
uses copied RootFS session state. However, FileStore.BindSandbox used to adopt
an absent `sandbox-id` marker immediately. Supervisor.Activate then saw the
store as already bound and skipped copied-state reset.

The retained unchanged-source tests show three failures:

- An ownerless copied stopped session and its idempotency key remain visible.
- An ownerless copied running session is recovered as a new attempt. The unit
  fixture executes only `/bin/true`; it is not a guest or `node -v` workload.
- Corrupt ownerless copied state is decoded instead of discarded, failing
  activation even though the assignment explicitly requires clearing it.

An independent controller test against the original source reproduces both
readiness consequences: valid copied sessions remain exposed at PhaseReady;
corrupt copied state prevents successful activation.

## Repair and preserved behavior

Keep one shared binding implementation. Ordinary BindSandbox permits legacy
adoption. Supervisor activation passes a policy disallowing that adoption when
the authoritative copied-state reset instruction is set. Missing ownership
then remains unpublished until ResetForSandbox has removed copied state.

| Existing owner | Explicit copied reset | Behavior |
| --- | --- | --- |
| Missing | No | Adopt legacy sessions in place |
| Missing | Yes | Clear copied state before publishing target owner |
| Foreign | No | Fail closed, preserve original state and owner |
| Foreign | Yes | Clear copied state before publishing target owner |
| Exact target | Either | Preserve sessions and idempotency keys |

The exact-owner case is important for retries: a repeated reset-bearing
assignment must not discard sessions already created by its target. Ownership
read errors still fail closed without deleting or decoding records. An absent
state directory can be recreated, including in the reset path.

No new storage authority, background recovery, early-readiness publication,
cache budget, timeout, prefetch or external API field is introduced. The stale
portal-restore comment beside activation is replaced with the actual ownership
ordering rule. Public supervised-session documentation now explains the legacy
and reset distinction.

## Verification and provenance

- Preserve the initial failing session test run and the independent failing
  original-source controller overlay run. Do not label their exit1 results as
  harness failures or omit the bad behavior.
- Fixed full session, runtimecontroller, procdconfig and runtimecontrol unit
  suites pass57 test events before the added controller regression. With that
  regression included, race count3 passes180 test events with zero failures or
  skips. Cases cover stopped/running/corrupt copied state, idempotency, exact
  owner retry, legacy adoption, missing directory and owner-read failure.
- Vet passes for session, runtimecontroller and procd main. The first procd
  build fails at VCS status lookup; the recorder correctly refuses completion.
  Retain both failures. A separately recorded build uses observed infra/main's
  `-buildvcs=false` and stripped flags and produces a static linux/amd64 binary.
  No binary is deployed or inserted into a RootFS.
- The five-file product delta is two implementation files, two new regression
  test files and one documentation file. Of the previous1359-file inventory,
  exactly three existing files changed and1356 remain identical. The new1361-
  file inventory is `/tmp/sandbox0-session-owner.M6Ksu5/source-after.json`, SHA
  `4195da42fb07166936a228b04153f1501ed46dbcc52cbe21bcce4d5f7e1cf3e3`.
  Preserve the old source inventory and all previous evidence unchanged.
- No remote start, cloud operation, RootFS import, regional claim/command
  sample, production rollout, PR, merge or tag. Existing remote artifacts and
  latency samples still identify the previous build, not this new procd.

## Remaining work

This repair does not remove directory enumeration, per-session state saves,
open-journal retention or recursive history removal. The size-independent
activation issue and previous empty-history cold misses both remain open.
Before an indexed/lazy/reset design is considered, it must preserve the owner
matrix above without making copied source records executable or visible as
target sessions.

The user was asked asynchronously to confirm whether the accepted hard2s
boundary is authenticated claim command-ready or claim plus first `node -v`.
No reply was received during this experiment. Do not silently change existing
ledger gates; retain claim, readiness, command-only and combined observations
and every1s/2s miss. No goal completion is claimed.
