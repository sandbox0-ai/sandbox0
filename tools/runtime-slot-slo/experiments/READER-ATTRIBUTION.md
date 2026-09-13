# D-READER-ATTRIBUTION: qualify per-Reader diagnostic accounting

2026-09-13. Temporary diagnostic overlay, no product-default change or new
startup sample. Evidence root: `/tmp/sandbox0-reader-attribution.W5e5Zc`.

## Why this is the next measurement

D-DEMAND-COVERAGE proved that ordinary Node/Coding immutable views cannot enter
the history-fragment helper. D-DEMAND-LIVE's mixed-cohort Coding improvement
therefore needs direct/indirect attribution, not another unchanged ABBA claim.
This diagnostic counts each Reader's work and binds it to its actual branch;
it does not introduce a new optimization or explain the cold gap by itself.

The overlay retains the V2 candidate and existing cache, source admission,
singleflight, checksum, lifetime and10s request-budget contracts. It adds no
object fetch, prewarm, network listener, global Reader registry or per-read log.
Only explicit `S0_DEMAND_OBSERVATION_DIR` in this diagnostic build enables it.

## Accounting and lifecycle

- Per Reader: requested/returned read bytes, fragment attempts versus dispatched
  batches/units, source calls/bytes, and range/mapping/coalesced/group flights.
- Flight leaders are callers whose actual singleflight closure executes. The
  shared flag cannot identify followers because it is also true for leaders
  with waiters. Eight-reader test proves1source call charged to the leader and
  seven followers charged no source calls. Cache rechecks can yield a leader
  without any source call; those counters remain separate.
- Source bytes are range bytes before envelope encryption, not HTTP attempts,
  ciphertext, wire traffic or minimum guest demand. Shared source work is not
  multiplied by the number of waiting sandboxes.
- Reader construction counts root-map work; successful branch open binds exact
  RootFS/generation/writer epoch and base head. Root-map/tail checksums remain
  separate from that head. Failed opens after observer creation have distinct
  report reasons; earlier validation/context rejection creates no observer.
- Branch close emits one exclusive0600JSON report, capped at64KiB. Repeated
  close does not overwrite. Conflicting reuse is invalid; report failures and
  post-report reuse emit fixed nonsecret warnings. No object key/payload/error
  string/credential is exported. Prepare a private fresh directory per boot/arm.
- Fixed counter struct is472bytes, excluding referenced strings, stack and
  serialization; this is not a measured RSS bound. No Reader registry keeps
  retired Readers alive. Write/fsync occurs during cleanup and can affect later
  machine state. Snapshot quiescence is not physical-absence/readiness proof.
- Durations are overlapping lifetime sums, including constructor and cleanup,
  not claim/command subphase times or additive critical-path costs. Exact
  external request windows and authenticated authority checks remain required.

## Tests and builds

Observer-specific tests pass45parent/subtest events over three race repeats,
zero skips. Coverage includes ordinary/history distinction, real shared-flight
ownership, cache hits, cancellation, short/long bodies, errors/panics, branch
binding, duplicate close, output collision, privacy and disabled/invalid config.
The full rootfsblock/rootfssession/nomadruntime overlay race run passes1252events;
18privileged/external/large-model/soak skips remain skips. Vet passes.

The local native ARM64 dynamic build is retained only as local build evidence.
Static amd64/CGO-disabled builds are the remote candidates:

| Build | SHA256 |
| --- | --- |
| Observed candidate | `f4042deb54140914e037374e530199548a4f052495b24f04444b16acd217e6bd` |
| Unobserved candidate control | `e5d0bf5d80bc1dce926a2eff6ee959b7f0ae5a21654b6162ceff6a2dd12aebaa` |

The control reproduces the exact D-DEMAND-LIVE candidate, not that campaign's
older baseline implementation. Nothing has been staged or installed remotely.

## Observer cost is not zero

A clean native ARM64 ABCCBA screen compares uninstrumented candidate, observed
binary with collection disabled, and collection enabled. Each mode has eight
samples per GOMAXPROCS1/8. These are sequential fully cached4KiB reads, not eight
simultaneous Readers, encrypted cold I/O or remote runtime calibration.

| GOMAXPROCS | Uninstrumented median ns | Disabled median ns | Enabled median ns |
| --- | ---: | ---: | ---: |
| 1 | 434.15 | 435.35 | 800.45 |
| 8 | 472.95 | 484.95 | 784.40 |

All48clean samples retain0bytes/0allocations per read and produce no per-read
report files. Disabled ranges overlap control; do not claim equivalence from
medians alone. Enabled adds about0.31–0.37microseconds, a material66–84percent
increase on this extremely short path. Do not extrapolate it into a precise
remote correction or label instrumented timings observer-free.

The initial48samples are also retained. Their first control might overlap vet,
so that entire screen is excluded from the conclusion. Only after the original
test/build/vet handles and first screen completed was the clean screen run.
No runtime experiment, claim or cloud action was replayed.

## Decision and remaining gates

The counter mechanism is locally qualified for diagnostic use; remote overhead
calibration and complete per-Reader generation attribution are still required.
Do not adopt it as product instrumentation or a performance fix from these tests.
Use a matching observer-free control, distinguish lifetime counts from request
phases, and reject missing/conflicted/live reports rather than interpreting them
as zero. Preserve every cold/cached-new miss and immediate literal node-v result.

All1362product source files/inventory are unchanged. Only temporary diagnostics
and requested records change. No local e2e, remote/cloud call, production change,
merge or tag. The previous stopped-instance receipt is historical, not freshly
queried here. Generic carriers, claim-time RootFS binding, no tenant prewarm and
all original populated-root/occupied-width/regional1s/2s gates remain intact.
