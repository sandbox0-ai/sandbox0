# D-RUNTIME-HTTP: stopped before the first claim

Date: 2026-09-12. Diagnostic preparation, NOT a startup measurement or acceptance.

## Unchanged question and requirements

Determine connection acquisition, request-to-first-byte and body-read costs on
actual owned slow reads in complete claim plus immediate `node -v`. The previous
fixed-range helper cannot establish the cause of the historical runtime delay.
Keep ordinary RootFS layout and the frozen product unchanged while calibrating
observation OFF/ON/ON/OFF on four fresh boots, eight lanes per cohort, followed
by cached-node NEW identities. The planned maximum was 64 claims, with no retry.

Generic tenant-neutral carriers, claim-time RootFS binding, disposable workers,
PostgreSQL/S3 durability, encryption/checksums, fencing, isolation, 10s request
timeouts and the 1s target/2s fallback are unchanged. Claim, real command and
combined latency remain separate; all misses must be retained. Populated-large
roots and occupied production-width density remain required acceptance gates.

## Preparation completed

- No product edits. Frozen 1353-file source inventory and 164 old trace artifacts
  reverified; existing worktree changes preserved. Main refs remain 0f092204
  (runtime) and ceb895c (infra).
- Temporary HTTP decorator forwards the existing client, credentials, retryer
  and pool. Bounded logs contain attempt IDs, hashed object/connection identities,
  ranges, phase timestamps and status flags, not credentials or raw URLs.
- Full objectstore/rootfsblock race suites pass, plus three HTTP-specific tests.
  The first parser used unsupported `go tool trace -d=1`; corrected diagnostic
  tests use pinned Go 1.25.5 `-d=parsed`. Failed receipts remain.
- Historical diagnostic 16CPU/64GiB/eight-carrier configuration was restored
  temporarily for comparison, NOT hardware expansion as an optimization.
  The historical runsc, private regional TLS and sparse Coding artifact are
  still not production/occupied-memory/populated-large acceptance.
- New isolated database `s0_runtime_http_qqq4jd` cloned unchanged source data.
  No imports replayed or source rows deleted. Original ordinary artifacts match.
- After a fresh boot, eight carriers were ready, no physical RootFS runtime
  remained, and target OSS outgoing packets and NBD I/O counters were zero.

## Failure and stopping decision

The first OFF-arm harness exited during preflight, before any claim or command.
Its unchanged trace-space check requires 8,589,934,592 free bytes. A subsequent
read-only check found 7,263,281,152 bytes available (about 6.76GiB). The concurrent
external observer also failed while assuming a service cgroup has `cpu.max`.
That leaf exposes `cpu.stat` and pressure files but lacks the CPU controller;
its immediate parent has `cpu.max = max 100000`. This is an observer assumption
failure, NOT evidence that CPU throttling caused the old cold-start delay.

The first disk-report helper accidentally overwrote its output variable with a
PID. Its `disk` field is invalid. The separate `disk-status.remote.json` supplies
the exact disk observation above; the error is retained, not silently repaired.

Stop this campaign at its first failed preflight. Do not reduce the guard,
erase old artifacts, replace the failed cohort, or run the other three cycles.
Actual counts: **0 claims, 0 commands, 0 HTTP runtime traces, 0 completed cycles**.
No medians, maxima, misses, overhead bound or performance improvement can be
inferred. The full-runtime attribution question remains unanswered.

## Follow-up admission, not another tuning sweep

Before another campaign, validate the complete observer against the actual
service cgroup hierarchy without changing controllers or quotas. Missing leaf
`cpu.max` must be recorded explicitly alongside ancestor constraints; it must
not be fabricated as a leaf quota or discarded from the observation. Check the
whole campaign's disk budget before resize/install, and arrange bounded capture
storage without deleting retained evidence or prewarming tenant data. Reuse the
already tested diagnostic build where possible. These are harness prerequisites,
not a new RootFS mechanism or justification for cache/pool/timeout tuning.

Ten allowlisted partial receipts were exported and individually hashed. Private
configs, DB dump and credentials remain remote. Original binaries/configs/service
processes and two-carrier job were independently verified restored; source data
is unchanged, no physical runtime remains, and all 64 NBD devices are idle. SSH
was closed and its handle reached terminal state. Cloud verification at
2026-09-11T23:42:17.847299063Z confirms original 2CPU/8GiB, Stopped/StopCharging.
The remote-test preservation/lifecycle workflow kept `/data` and the isolated DB.
No production rollout, merge or tag. Investigation remains incomplete.

Evidence root: `/tmp/sandbox0-runtime-http.qqQ4Jd`; 138 artifacts sealed.
Evidence SHA256: `983a15917b384febf792e31a19342d6d8248bcf665c1d2468a9afdbc90954523`.
Index SHA256: `2a5b8734c61d4075ceced915a15a03281670be4670e160b8c14842eaa111952a`.
