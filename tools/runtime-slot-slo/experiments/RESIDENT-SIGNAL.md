# D-RESIDENT-SIGNAL: real finite occupied-node matrix

2026-09-13. Local evidence: `/tmp/sandbox0-resident-signal.RcBrWF`; retained
remote root: `/data/sandbox0-resident-signal-RcBrWF`; isolated database:
`s0_resident_signal_rcbrwf`. This completes the previously invalid resident
fixture and obtains six actual controlled windows. It does not close the cold
startup goal or establish a production-width acceptance result.

## One scoped change

Ordinary non-PTY HTTP CMD creation supplies no stdin pipe. Add explicit
`--control=signals` to the finite diagnostic helper, using USR1/load and
USR2/release through the existing exact-context signal endpoint. Register the
handlers before publishing idle, preserve the same single-use state machine,
and retain fixed45s lifetime,60s async TTL,40s matrix budget and strict live
owner/PID/lease/cgroup evidence. Default stdin mode still stops at EOF. TERM/INT,
expiry, invalid transitions and output failure stop the helper. No product
runtime, public CMD semantics, HTTP budget, RootFS layout or timing gate changes.

Tests now launch real no-stdin subprocesses through procd's actual DirectRunner,
not a pipe-backed mock. They cover default EOF, signal lifecycle, delivered
duplicate/out-of-order controls, expiry, INT/TERM and blocked/failed output.
The helper's22top-level tests and private adapter's33top-level tests pass three
race repetitions; vet, the complete SLO unit suite and linux-amd64 builds pass.
Ruby validators pass45distinct tests/133assertions (the subprocess suite also
reruns35of those tests). These local tests are not runtime SLO evidence.

The corrected prior base-config constructor is preserved. The private adapter
uses generated SignalContextRequest, exact owned context routes, exclusive
mutation intents, explicit signaled acknowledgement and subsequent guest phase
acknowledgement. Signals can coalesce: do not replay ambiguous sends or treat an
HTTP acknowledgement as proof of sustained load.

## Frozen remote scope

Same16CPU/64GiB Singapore test shape, qualified manager/ctld/driver/stock runsc
pins, two frozen artifacts, eight generic single-use carriers,1750m/1792MiB
leases and private regional TLS ingress. Materialization remains enabled;
background maintenance/squash is disabled. No OCI import, RootFS prewarm, cache
knob, hardware sweep or timeout increase. All1358prior source files are checked
before editing; only the helper implementation/tests and its diagnostic README
change, plus one new subprocess test file. All other candidate source is kept.

Seven resident Node claims and their preparation precede two retained initial
targets. Then the same seven helper identities move idle -> loaded -> released,
with Node/Coding target order alternated as previously frozen. Each loaded
helper touches1536MiB and runs two CPU workers. Every target is a new sandbox
identity and immediately executes literal `node -v`.

This is one-wide admission with seven existing residents at eight active
carriers, not eight simultaneous new claims. Later phases naturally reuse node
caches populated by earlier measured claims. They are not globally cold and
their ordering prevents attributing the full timing difference to CPU load or
memory release alone. CPU-busy10.5GiB allocation on64GiB is not memory-reclaim
acceptance. The large Coding root remains sparse rather than a populated
large-root/upper-history acceptance case.

## Actual results

All eight target commands return201, stopped, exit0 and exact `v22.23.2\n`.
All six controlled windows pass the remote independent guest/cgroup verifier
and a separate local replay of the downloaded raw evidence.
The matrix completes collection but exits1 because the initial Coding combined
sample still misses2s. No initial sample is discarded as warmup.

| Phase | Case | Claim wall | Authenticated readiness | Literal node-v | Claim through node-v |
| --- | --- | ---: | ---: | ---: | ---: |
| Initial encounter | Node | 0.147049s | 0.144896s | 1.158798s | 1.305883s |
| Initial encounter | Coding | 1.389479s | 1.387012s | 0.985558s | 2.375093s |
| Idle, cached-new | Node | 0.313091s | 0.310255s | 0.218503s | 0.531625s |
| Idle, cached-new | Coding | 0.387674s | 0.385622s | 0.418502s | 0.806205s |
| Loaded, cached-new | Coding | 0.186256s | 0.182647s | 0.083647s | 0.269936s |
| Loaded, cached-new | Node | 0.362600s | 0.358107s | 0.320243s | 0.682875s |
| Released, cached-new | Node | 0.138760s | 0.136439s | 0.041858s | 0.180648s |
| Released, cached-new | Coding | 0.134196s | 0.132008s | 0.041226s | 0.175450s |

One startup1s miss, zero startup2s misses, two combined1s misses and one
combined2s miss remain. These eight individual samples are not a percentile or
a hard guarantee. The147ms Node claim is followed by1.159s first execution;
reporting only claim would hide most of the business-visible first-command cost.

The seven resident cgroups use12.22–12.24CPU cores in the two loaded windows;
whole-host busy is84.45–85.80%, with zero steal. Total resident memory charge is
11.735–11.741GB (decimal), falls to418–421MB after release, and swap/OOM/max events
remain zero. Loaded host MemAvailable still exceeds51.57GB, explicitly excluding
a claim of memory-reclaim pressure. These measurements establish real occupancy,
not just live allocations or guest-reported bytes. Host iowait is not an S3
service-latency measurement.

## Interpretation and remaining work

The same machine can serve cached-new targets below1s even with independently
verified CPU-busy residents. That makes a simple "insufficient CPU explains all
cold latency" diagnosis unsupported by this trial. It does not prove CPU never
matters, nor isolate cache warming from load in this fixed ordered matrix.
Likewise, cache-sensitive first execution does not establish that the S3 service
is faulty: application read dependencies, required round trips and service time
are different costs. Do not turn this diagnostic into another machine/S3 knob
sweep or claim a new runtime speedup; runtime source and pins did not change.

Keep first-touch command cost as the unresolved structural target. Preserve the
prior truly empty-node combined2.221/2.668s misses separately, along with the
generic pool, claim-time tenant RootFS, stateless compute, encryption/durability,
populated histories and true occupied hardware-width requirements. Neither
cached success nor changing the measurement boundary closes the1s/2s goal.

## Evidence and lifecycle

The completed trial retains all15known sandbox IDs, attempts each owned DELETE
once, reports zero unknown claims/cleanup errors and reaches zero leases with
eight replacement carriers. The isolated database retains2087rows, including
the2072historical source rows. Original source databases/artifacts are guarded.

The previous sealed146-file bundle remains unchanged. Its remote archival gap
is filled by a separately hashed23-file annex containing extensionless HTTP
intents and unit logs; the old seal is not rewritten. The current collector also
includes these artifacts. A local download basename check initially rejected
`.intent.json` after downloading only cases.json; retain that failure, correct
the filename allowlist and resume by verifying existing exact hashes without
overwriting different files. This did not rerun any remote request.

An early preflight observed Nomad not yet listening after boot and made no
test-root/database mutation. A later read independently confirmed their absence
before preflight was attempted again. Installation/restoration service waits
are separate from request timing and are never used to replay a mutation.
Final independent physical-absence, restored runtime/job, compute stop and shape
restoration receipts accompany this experiment's evidence seal.

Original binaries, configs and two-group job are restored, with2ready at job
index72102; all64NBD devices are detached and writer/runsc/mount/branch/cgroup
absence is independently checked. The diagnostic unit is terminal with PID0.
Four owned dropins are moved into the retained `removed/` directory. No source
history, database, backup or failure log is deleted. The local download contains
95verified current result/log/intent/restoration files plus the23-file old-run
annex. Compute is stopped and restored to the original2CPU/8GiB specification;
the final cloud receipt provides its observation time. No production, PR merge,
tag or local e2e operation.
