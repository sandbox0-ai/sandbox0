# Cold-start investigation: convergence decision

2026-09-13. This decision supersedes the earlier open-ended optimization loop,
not the architecture or the contents of historical evidence.

## Latest user direction

The user asked to converge and explicitly accepted the current performance as
reasonable, even somewhat above2s. Stop adding performance candidates or remote
tuning campaigns merely to force the observed total below2s. Continue only
delivery cleanup, necessary regression, and an accurately scoped PR handoff.
New performance exploration needs a new user request, not an automatic goal
continuation interpreting the old hard-threshold plan as unfinished tuning.

Generic tenant-neutral carriers, claim-time RootFS binding, disposable nodes,
durable PostgreSQL/S3 authority, encryption, writer fencing and authenticated
readiness stay intact. Do not introduce pre-claim tenant RootFS warming, increase
the10s HTTP budgets, or retry claim/command POSTs to manufacture a passing sample.

## Report the observed result, not a universal guarantee

The latest retained private-regional diagnostic, [READER-LIVE.md](READER-LIVE.md),
contains32 successful first `node -v` commands. Across its observed/unobserved
arms, cold claim-plus-command ranges2.128–2.692s; cached-node NEW identities
range1.532–1.608s. Readiness/claim and command-only remain separately reported.
Use those ranges with the experiment's limits, not a generic claim that every
root starts in2s. Both arms used a temporary V2 Reader variant, not the unchanged
product Reader now being frozen for review; these numbers are NOT a measured
release-binary promise. One external sampler failed; it is not a green acceptance
run. The ordinary baseline arms in [INDEXED-LIVE.md](INDEXED-LIVE.md) separately
retain cold combined maxima2.008/2.068s for Node and2.614/2.480s for Coding; they
also predate final guest parity. Never combine variants into one claimed result.

The subsequent direct-XFS experiment has a narrower node-local boundary and
cannot replace regional measurements. It fails its net-cost screen and remains
rejected. Neither test establishes populated-large-root or occupied-production-
width performance, or current guest/release parity. Preserve those limitations
without restarting an indefinite optimization loop. Historical1s/2s misses stay
in the evidence; user acceptance does not rewrite them into passes.

## Delivery scope

| Area | Convergence treatment |
| --- | --- |
| Correctness fixes | Retain for review with their reproductions and regressions: persisted Nomad task configuration, exact recovery identity, copied-session owner/reset ordering, Reader lifetime and terminal-resource boundaries. |
| Existing demand-read implementation | Freeze the current candidate for regression and review: bounded immutable header/mapping caches, independent safe source admission and ordinary bounded read coalescing. No further budget/concurrency sweep. |
| New import formats/layout policies | Do not silently activate them. Current configuration keeps format1/legacy import selection by default; opt-in format2/geometry/layout/mapping policies and their schema changes need explicit compatibility review in the PR. |
| Rejected or unqualified prototypes | Do not copy temporary inline/joint/EROFS/direct-XFS/ELF-prefetch variants into the product. Indexed-window/history-fragment temporary variants are not approved defaults merely because a few samples improve. |
| Diagnostics | Preserve frozen raw evidence and the experiment ledger; separate benchmark tools and provenance from production changes. Instrumented timings are not uninstrumented guarantees. |
| Remaining acceptance coverage | Keep populated/history, actual occupied width and current binary parity explicit. Do not claim they passed, and do not use them as a reason for more unsolicited performance exploration. |

This is a scope decision, not approval of every dirty file as merge-ready.
Worktree `perf/rootfs-demand-read` contains a large accumulated change set;
no destructive reset, removal of user-owned edits, merge, tag or production
rollout follows this document. The frozen source inventory has1362files at
base `0f09220460581bfc1fdc331f34ebc85bf38381e7`, inventory SHA256
`8c7ebcef841b6ec05e6b2e4ebeb7971012f93fccd3b02e678c787c273afbd546`.

## Finite closeout

1. Check the retained source and independent driver module with race-enabled
   regression. Skip privileged/external integration only where its explicit
   opt-in is absent; record skips as missing coverage, not passes. No local e2e.
2. Review the accumulated diff against the scope above and separate its logical
   changes for one reviewable PR. Do not bundle `/tmp` prototypes into it or
   change production defaults to match a favorable diagnostic fixture.
3. Hand off exact tests, operational compatibility and observed cold/cached
   readiness/command distributions. Do not label the universal hard-bound goal
   achieved or auto-merge/deploy from the user's convergence comment.

The remote diagnostic machine was restored to2CPU/8GiB Stopped/StopCharging at
2026-09-13T12:20:38.507602969Z; no remote operation is needed for this decision.
All `/data` evidence/caches remain preserved. Closeout regression evidence is
under `/tmp/sandbox0-cold-convergence.AVqslf`; results are not assumed before the
actual test processes finish.

## Closeout regression result

The21 selected core packages ran with race, count1, readonly modules and bounded
build parallelism. Twenty packages pass. The original overall suite exits1:
2208PASS events, four FAIL events (two top-level XFS tests plus two subtests),
and166explicit skips. This first run remains failed, not retroactively green.
Independent driver module:113PASS, zeroFAIL, two privileged payload/corpus skips;
three packages without tests are separately reported as package skips.

The failed XFS tests use mocked FIEMAP but real directory/file metadata. A fresh
probe reproduces local `/tmp` directory device30 versus regular-file device31.
The production same-device guard correctly rejects that fixture before overlap/
mutation assertions. An isolated `/dev/shm` fixture reports device36 for both.
Keeping all production/test source identical, rerun only the affected package
with test-process TMPDIR on that same-device filesystem (build executables still
use ordinary executable temporary storage): race count3 passes186events, zero
failures, six explicit privileged skips. Every previously failing test runs and
passes three times. No guard is weakened, test skipped to hide failure, privileged
mount performed, or runtime replayed. Owned temporary probe directories are
removed only after empty checks; all experiment evidence remains retained.

Verification rechecks the entire1362-file product inventory and path set.
Database, privileged, external-store and opt-in scale/soak skips remain missing
coverage. This is scoped local regression, not full CI, regional acceptance or
merge readiness. Raw `core.json`, `driver.json`, `xfs-fixture.json` and their
checksums in `verification.json` preserve the exact commands and outcomes.
