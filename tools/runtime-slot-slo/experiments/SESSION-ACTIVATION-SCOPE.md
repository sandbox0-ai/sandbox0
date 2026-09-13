# D-SESSION-ACTIVATION-SCOPE: history-dependent procd activation work

2026-09-13. Evidence: `/tmp/sandbox0-session-activation.aUmkfG/evidence.json`.
No runtime implementation change or remote workload. The preceding goal turn
only answered the prefetch question and supplied no new performance evidence.

## Finding

Even stopped sessions add synchronous work before procd listens. For each
retained session, activation loads its state, opens its journal, updates its
runtime generation/time and atomically replaces its state file. It retains an
open active-journal descriptor. A copied-store reset instead synchronously
removes all old session directories before rebinding the store.

The actual Linux filesystem observations are:

| Stored stopped sessions | State files opened / replaced on same-owner activation | Journals opened / descriptors retained | Retained files deleted on copied-store activation |
| --- | --- | --- | --- |
| 0 | 0 / 0 | 0 / 0 | 0 |
| 1 | 1 / 1 | 1 / 1 | 3 |
| 32 | 32 / 32 | 32 / 32 | 96 |
| 128 | 128 / 128 | 128 / 128 | 384 |

Each fixture session contains a valid state file, journal manifest and active
segment with one real event. All sessions are stopped; their command is never
executed. Same-owner recovery changes state generation to 2 while leaving
journal bytes unchanged. The copied-store arm exposes zero source sessions and
deletes every retained fixture file before activation returns. It does not
open old state files or journals to recover them.

These are file-operation counts, not S3 GETs, payload bytes or remote latency.
In particular, recursive unlink does not imply reading journal payloads.

## Execution and boundaries

- Execute the unchanged FileStore, Journal and Supervisor on local Linux/arm64
  through a Go overlay adding only a diagnostic test. No implementation overlay.
- Watch all existing fixture directories with Linux inotify after preparation;
  drain preparation events and end the observation immediately after Activate.
  Reject queue overflow, malformed events and unknown watch descriptors. Count
  distinct state opens, atomic replacements and retained-file deletions.
- Independently check activation logs, live `/proc/self/fd` journal targets,
  resulting state fields and complete journal bytes after the observation.
  Verification reads and Supervisor.Close writes are outside the counted window.
- Eight normal subcases pass. Race count3 passes all24 subcases/27 test events,
  with no failures or skips. A separate result checker requires both complete
  matrices and every expected operation count.
- All1359 frozen product files are unchanged. Session and runtimecontroller
  source has no diff against the observed origin/main commit0f092204; no new
  fetch is represented as having occurred in this experiment.
- No cloud start, claim, guest command, S3 request, local Sandbox0 e2e, occupied
  workload, format/default change, prewarm, timeout/cache increase or production
  action. Generated fixtures are isolated test temporary directories.

Timing fields are retained only as `local_warm_fixture_elapsed_ns`; these small
newly-created fixtures are neither cold-node samples nor large-root acceptance.
Do not substitute their durations for authenticated regional readiness or add
them to a historical sample from another environment.

## Why this changes the next action

This is a distinct scale dimension from the previously optimized lazy reading
of a single journal's retained payload. Bounded work *per journal* does not make
activation independent of the number of journals. Session lifetime in the
current documentation includes stopped sessions until explicit deletion or
sandbox lifetime termination; they cannot simply be discarded as an optimization.

The readiness dependency is visible in the current source:

1. `manager/cmd/procd/main.go` waits for runtimeController.Activate before
   constructing and starting the HTTP server.
2. `manager/procd/pkg/runtimecontroller/controller.go` waits for session
   activation before PhaseReady.
3. `manager/procd/pkg/session/supervisor.go` loads/opens all stored sessions and
   then calls recoverPersistedSession for each. Even stopped records are saved.
4. `manager/procd/pkg/session/store.go` implements state-file Sync/rename and
   per-session recursive removal for copied-store reset.

Before changing this path, qualify whether an ownership-safe indexed or lazy
session representation can bound activation work while preserving idempotency
keys, cursor integrity, runtime-generation/attempt fencing, stopped-session
visibility and fork isolation. Do not just background the existing loop, drop
records, expose readiness early or introduce a second durable authority.

This finding does **not** explain existing empty-session cold misses, validate a
performance improvement, or reopen rejected prefetch/cache/layout experiments.
Those misses remain an independent requirement. Any future history-bearing
remote comparison must still include empty-node versus cached-node NEW runtime
identities, literal first `node -v`, full regional clocks and occupied width.
All original1s/2s and populated-root gates remain open.
