# D-EXEC-ADVICE: pinned guest hints and conditional ELF-read cost

2026-09-13. Local source/geometry diagnosis only. Evidence root:
`/tmp/sandbox0-exec-advice.nu3eoS`. No new sandbox, command, cloud query, S3
operation, runtime implementation or latency sample. All original requirements
in [STRUCTURED-OPTIMIZATION.md](STRUCTURED-OPTIMIZATION.md) remain unchanged.

## Decision

Reject the specific proposal to reduce first-command blocking by issuing a
guest `fadvise(WILLNEED)`, `madvise(WILLNEED)` or `readahead` hint when an actual
exec request arrives. At the tested stock runsc pin these interfaces do not
issue the proposed reads. A successful hint return is not evidence of prefetch.

Do not substitute unconditional whole-ELF reads without a new bounded net-cost
case. The retained scanned Node has107,122,688bytes of page-aligned initialized
PT_LOAD coverage. That is not its necessary startup working set, and the
historical read-overlap comparison below lacks exact cross-artifact executable
identity. No actual-read candidate or remote comparison is admitted here.

## Exact upstream source, not just the support table

The current tested runtime is stock `release-20260817.0`, binary SHA256
`048b89aada69dc3333422e139d6e9d02f8ab06bda52398060e0fbdacca00074c`.
The annotated tag object is`c46e5e46d3e6c01640e2237756a9099f6f917699`;
its dereferenced commit is`50e1502a95d36ad2faf2c7ef33b8bf21fe975293`.
Retain all four downloaded source files with exact SHA256 checks.

| Guest interface | Pinned implementation | Consequence for this hypothesis |
| --- | --- | --- |
| `fadvise64`, including WILLNEED | Validates descriptor/advice and returns success, without issuing a hint or read. | Adding a successful call does not initiate prefetch. |
| `madvise(MADV_WILLNEED)` | The selected advice clause returns success without an effect. Other advice branches have different semantics. | Do not infer a read from this return, or classify all madvise operations as no-ops. |
| `readahead` | Validates arguments and returns EINVAL; no hint is issued. The amd64 table nevertheless labels it supported. | Support-table labels alone are insufficient. |

Primary sources: pinned
[Fadvise64](https://github.com/google/gvisor/blob/50e1502a95d36ad2faf2c7ef33b8bf21fe975293/pkg/sentry/syscalls/linux/sys_file.go#L897),
[Madvise](https://github.com/google/gvisor/blob/50e1502a95d36ad2faf2c7ef33b8bf21fe975293/pkg/sentry/syscalls/linux/sys_mmap.go#L212),
[Readahead](https://github.com/google/gvisor/blob/50e1502a95d36ad2faf2c7ef33b8bf21fe975293/pkg/sentry/syscalls/linux/sys_read_write.go#L558)
and [amd64 table](https://github.com/google/gvisor/blob/50e1502a95d36ad2faf2c7ef33b8bf21fe975293/pkg/sentry/syscalls/linux/linux64.go#L232).
The local AST audit binds file hashes, actual function bodies and those routes;
it is not a guest syscall test or a general interprocedural proof engine.

This does not reject every way to cause actual reads under stock gVisor.
For example, the separate Mmap handler sets PlatformEffectCommit for
MAP_POPULATE. Its downstream behavior, resource cost and usefulness are not
qualified by the three findings above. No modified runsc is proposed.

## Static segment geometry and its important binding limit

D-NODE-IDENTITY previously hashed the exact124,836,408-byte Node ELF in both
scanned roots as`3517c2df0b2f8cd7f422b4b8450ef81c6889f08eb03e281d6de9079b15e6a327`.
Its five PT_LOAD records match, but the roots' dependency file contents differ.
This turn revalidates its52sealed artifacts and consumes those metadata records;
it does not reread a Node binary or collect a loader trace.

The generic static plan rounds initialized file ranges to4KiB pages, clips EOF,
excludes zero-file-byte BSS, and unions overlap. It yields`[0,49168384)` and
`[50331648,108285952)`, totaling107,122,688bytes. These offsets derive from ELF
metadata, not a command-specific profile; there is no runtime prefetch code.

The separate D-NODE-RA-COVERAGE record contains65,314,816bytes of inode-relative
base-read coverage, unioned across two guests per cohort. It includes kernel
read-ahead, not just pages required by the application. The two input families
belong to DIFFERENT immutable artifacts:

- ELF scan: Node`f19e6d66...`, Coding`9d11df8f...`.
- Historical read offsets: Coding`37de2f09...`, inode79717, same recorded size.
- Current regional runs use still other Node/Coding artifact bindings.

No exact Node content-hash bridge from that historical trace artifact to the
scanned ELF was established in this turn. Equal file size/path is insufficient.
The original `analysis.json` omitted this provenance caveat; retain it, and use
`analysis-v2.json` with explicit cross-artifact identity=false as canonical.

The conditional byte-set comparison is arithmetically unchanged: intersection
63,401,984bytes; plan outside the historical union43,720,704bytes; historical
union outside the plan1,912,832bytes; fixed union109,035,520bytes. The plan is
1.6401times the old union, but these are NOT measured additional current I/O,
S3 bytes, cache/RSS charges, necessary working set or predicted latency savings.
The approximately42MiB figure mentioned during investigation is corrected to
this conditional scope, not silently promoted into a performance result.

## Bounds before any actual-read candidate

An actual-read mechanism would need its own case, not a retry of these hints:

- Exact executable/dependency binding before historical overlaps are used as a
  prediction; then a generic trigger after the real exec request, with no
  pre-claim tenant reads or workload-specific offsets.
- Bounded source operations, temporary memory and node-wide concurrency. A
  context cancellation or descriptor close cannot be assumed to interrupt a
  blocked backing read; prove ownership and cleanup before runtime admission.
- Unchanged PATH/CWD, permissions, symlink and executable replacement semantics.
  Do not add a second authority for which file the command executes.
- Net critical-path benefit after charging extra reads, verification/decode,
  cache displacement and contention for both image families. Whole-file work
  may scale with executable size even if it ignores unrelated RootFS size.
- Only a supported candidate proceeds to paired/reversed regional empty-node
  and cached-new measurements, then populated histories and real occupied width.

## Verification, failures and status

Seven local AST tests pass, including altered/missing sources, an inserted
read call, changed advice behavior, successful readahead route and malformed/
missing/duplicate functions. Vet passes. Geometry tests pass10tests/921assertions,
including300independent byte-set cases, BSS, overlaps, EOF and invalid bounds.
They validate the diagnostic computation, not runtime cancellation or a speedup.

Reverify1359unchanged candidate source files and237sealed prior-turn artifacts.
Only experiment documentation changes in the product worktree. Preserve the
prior dirty worktree. Initial upstream lookups confused the annotated tag with
its commit and guessed a nonexistent sys_read.go path; the actual function is
in sys_read_write.go. An API-listing syntax error and a failed JSON parse are
also local retrieval failures, not runtime or storage failures. The initial
source copy added a trailing blank line; correct it and require the original
downloaded hashes before admitting the audit.

Machine/S3 evidence is unchanged: request-ID-bound service delays in
[CORRELATED-READER.md](CORRELATED-READER.md) and cached-new success under actual
CPU occupancy in [RESIDENT-SIGNAL.md](RESIDENT-SIGNAL.md) reject a simple
CPU-only explanation, but do not prove an OSS fault or exonerate occupied
hardware. This syscall finding is a rejected optimization mechanism, not the
root cause of all cold-start latency.

Prior actual empty-node combined2.221/2.668s and initial occupied-fixture
Coding2.375s misses remain. No timeout/cache/hardware change, tenant prewarm,
local e2e, production operation, merge or tag. Remote state was not queried;
the last observation is the preceding2026-09-12T19:43:26.407334688Z receipt:
Stopped/StopCharging, original2CPU/8GiB with restored fixture and retained data.
The goal remains open; no universal1s/2s or populated-root/density result follows.
