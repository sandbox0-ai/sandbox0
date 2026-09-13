# D-EXEC-PREFETCH: bounded generic execution-time real reads

2026-09-13. Local prototype qualification, not remote latency acceptance.
Evidence: `/tmp/sandbox0-exec-prefetch.vRfMxE`. All requirements in
[STRUCTURED-OPTIMIZATION.md](STRUCTURED-OPTIMIZATION.md) remain unchanged.

## Decision and implementation

The file-coordinate gate in [CURRENT-EXTENTS.md](CURRENT-EXTENTS.md) is complete.
Implement one experimental policy through a six-file temporary Go overlay; do
not repeat identity/layout scans, old carrier recovery or broad read tracing.
No product runtime file or default is modified. No new claim, guest command,
cloud operation, S3 read/write, runtime deployment, merge or tag occurs here.

The real CMD constructor still resolves the original `exec.Cmd.Path`, arguments,
working directory and environment. After creating its direct/PTY runner, and
before starting that same command, an exact command environment opt-in
`S0_EXEC_PREFETCH=elf-v1` permits bounded reads of its main ELF. Other values
are a zero-I/O off path. There is no Node-specific profile, guessed dynamic-
library resolution, template prewarm or claim-time RootFS rebinding.

- One active optional job per procd, no waiting queue; busy commands skip it.
- Four independent ReadAt workers, each with1MiB scratch; total reserved read
  requests are capped at128MiB, including ELF/program headers and partial/error
  attempts. Parse at most128 program headers; do not parse section tables.
- Accept bounded ELF64 little-endian EXEC/DYN loads for x86_64/AArch64. Union
  initialized4KiB file-page ranges, clip EOF, skip zero-file-byte BSS. Unsupported,
  malformed, unreadable or over-budget plans do not replace normal exec errors.
- Pin with O_PATH, reject nonregular/pseudo files before payload open, reopen
  through a held verified proc-fd directory with O_NOATIME and CLOEXEC, and check
  device/inode/size. Never fall back to an atime-changing or privileged open.
  This is an optional read of a pinned inode, not an authority proof that a
  subsequently mutable exec pathname still resolves to that inode.
- Wait for completion/close, subject to a fixed500ms optional-stage wait cap.
  The cap cancels only optional work, not the original exec context. All pre-exec
  waiting is charged to external first-command wall time; process StartTime is
  not the end-to-end timer. Public10s budgets are unchanged.
- Cancellation/expiry can return while an opener, read or Close is blocked.
  Keep the admission token, descriptor and scratch ownership until all physical
  work returns. Already-in-flight work can overlap the subsequently launched
  command and must still be charged in later cost/cleanup measurements.

The128MiB cap is not an NBD/S3-byte cap: kernel read-ahead, encryption/compression
and cache behavior remain distinct. FourMiB is live read scratch, not a hard
RSS bound; Go heap retention and guest/host page cache also matter. A Go timer
is not a hard real-time guarantee during scheduler starvation. Pending outcomes
do not have final read accounting; their zero-value counters must not be reported
as proof of zero I/O or complete cleanup.

## Changes, failures and verification

1. Initial code review corrected overflow in chunk-end arithmetic near MaxInt64
   and avoided filepath.Join cleaning symlink/`..` traversal into another path.
   Both are now regression-tested, not performance experiments.
2. A hook before runner creation would let Stop miss the CMD cancellation callback.
   Place it after runner creation, outside the reaper lock and before exec.
   Actual direct and PTY CMD tests stop while the hook is waiting and verify no
   process is launched. Four blocked readers retain ownership until the last
   returns;100 competing canceled-job callers cannot admit more optional work.
3. Source inspection found that HTTP creation calls CMD.Start before attaching
   request cancellation to the completion wait. The CMD context is created from
   Background. Therefore internal-context cancellation tests do NOT prove HTTP
   cancellation reaches this new stage. Add the500ms optional wait cap, without
   changing existing public API/command timeouts. A blocked-opener test proves
   the original real `/bin/true` still runs after expiry while ownership remains.
4. Initial focused tests pass62 test/subtest events, repeated three times186;
   the existing process/CMD/reaper suites pass156. After the cap and ownership
   tests, the final focused suite passes66 per repetition,198 over three race
   repetitions; final full suites pass160. The credential-helper entry point is
   skipped only in the parent process; both dropped-UID subprocess cases actually
   run and pass (execute-only owner and readable nonowner/O_NOATIME denial).
5. Native Linux tests prove nonzero bounded real reads, unchanged atime, CLOEXEC,
   no leaked temporary FDs, prompt FIFO/device rejection, exact budgets, malformed
   ELF rejection, overlap/EOF handling and independence of four readers. Actual
   CMD on/off tests cover argv/env/CWD/PATH, stdout/stderr, exit status, reaping,
   stdin/no-stdin, PTY, start failures and prelaunch Stop. These are arm64 Linux
   subprocess unit tests, NOT stock-gVisor or end-to-end tests.
6. Selected adjacent context/session/HTTP-handler/procd tests pass17 with race;
   retain their explicit regex in `adjacent.json` (not whole-suite coverage).
   Vet passes for both native-tested logic and linux/amd64 target compilation.
7. First procd build fails obtaining VCS status. Retain `build-prefetch.json`.
   Explicit `-buildvcs=false` with separate source/hash receipts fixes building;
   do not infer that the failed build changed runtime behavior. The initial
   stripped pair builds successfully but is not selected for remote comparison.
   Inspection of the old qualified build confirms plain, not stripped, flags;
   preserve both pairs and build a fresh plain matched pair with Go1.25.5,
   CGO_ENABLED=0, linux/amd64 and trimpath. No test failure is discarded.

Final plain experimental procd:24,507,865bytes, SHA256
`8eec5c8a9cbdc53fc77ae2dc82596a76f0df3aaccc40ced3ead934a55023cda5`.
Fresh plain non-overlay control:24,473,208bytes, SHA256
`0c737562c549c8668ef226934c11b9f1f116a1f59b3f53f52a30dea070ade98d`.
These are new local artifacts; no byte equivalence to the historical installed
procd is claimed. The primary causal on/off trial should use the SAME experimental
binary and RootFS per image, differing only in the explicit command opt-in, so
binary layout or reimported procd placement is not the alleged treatment effect.

## Next remote gate; no speedup yet

First validate the exact stock20260817 runtime's O_PATH/Fstatfs/proc-fd/O_NOATIME
behavior and observe actual nonzero work. Native success and unsuccessful web
source fetches do not establish guest support; an optional skip is not prefetch.
Do not remove the safety flags just to obtain a successful-looking trial.

Then use matching observer-free on/off execution with literal `node -v` and full
regional request timing. Retain claim/readiness, command and combined times;
record cap/busy/skip outcomes and account for physical pending work separately.
Use cold nodes versus cached-node NEW identities; charge actual NBD/S3 bytes,
CPU, memory and teardown. Reject the candidate if its extra reads/concurrency
are not beneficial or violate high-density resource bounds. The previous43.7MB
extra Node file coverage is a known risk, not predicted S3 transfer or speedup.

True occupied-width and populated/history-bearing RootFS acceptance remain open,
as do every original1s/2s regional gate. The previous current-pin cold combined
maxima2.221/2.668s are still misses, not replaced by local unit-test durations.
S3 service waiting and machine contention remain separate measured dimensions;
no larger machine, cache allowance or timeout is accepted as the optimization.

All1359 inventoried product files and59 prior sealed artifacts are verified.
Only this experiment record and the ledger change in the worktree; all existing
user modifications are preserved. No local e2e or remote compute was started.
The most recent inherited cloud observation remains Stopped/StopCharging at
2CPU8GiB; it is not a new cloud query in this turn. Preserve all evidence/data.
