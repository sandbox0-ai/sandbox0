# Cold-start optimization: requirements and decision gates

Updated: 2026-09-12. Investigation plan, not a new public SLO or production approval.
The user requested a structural optimization method and explicitly preserved the
fundamental requirements. This plan changes experiment selection, not architecture
or acceptance boundaries.

## Requirements that cannot be traded away

- Keep Nomad-only, stock gVisor, generic tenant-neutral warm carriers. No guest or
  tenant RootFS is prepared on a node before claim identifies the RootFS.
- Keep RootFS and compute separable: PostgreSQL/S3 remain durable authorities;
  workers are disposable. Empty target-node caches are a required cohort.
- Measure authenticated regional ingress through procd readiness and claim
  response. Keep the 1s target and 2s accepted fallback; retain every miss.
- Immediately execute real `node -v`. Report claim, command-only and combined
  latency separately, including combined 2s misses. Moving work from claim into
  the first command is not an improvement of the complete path. Arbitrary user
  process initialization is not made identical to the platform readiness SLO.
- Cover cached-node NEW sandbox identities separately, populated large roots
  separately from sparse logical size, and actual occupied-node CPU/memory at
  real configured width. Lower width or unoccupied leases are not density proof.
- Keep the 10s request timeout, authentication/readiness proof, encryption,
  independent checksums, writer fencing, isolation and durable cleanup unchanged.
  Larger caches or hardware cannot silently replace a cold-start solution.
- The size-independence objective must not be narrowed to convenient images.
  Finite passing samples do not prove a universal hard bound for every root or
  network condition; remaining limitations and observed failures stay explicit.

## What the latest evidence actually establishes

`D-PROJECTED-DEMAND` compared ordinary, old-inline and projected-inline layouts
under fixed encrypted helper workloads. These are not sandbox startup samples.

| Coding mixed reads | Ordinary | Old inline | Projected inline |
| --- | ---: | ---: | ---: |
| Mean elapsed, ms | 602.700 | 964.165 | 866.731 |
| Mapping + data GETs | 16 + 45 | 23 + 69 | 23 + 49 |
| Returned ciphertext, bytes | 6,972,673 | 6,733,404 | 7,665,441 |
| Mean CPU, ms | 349.887 | 450.109 | 470.564 |

Inlining trades fewer metadata-only reads for bigger mapping pages and fragmented
views of retained compressed frames. Projection repairs some batching, but adds
physical coverage and does not remove the mapping/decode costs. Even all-cache-hit
mixed reads retain a CPU-side penalty. These effects are demonstrated; their
individual contributions to whole-startup wall time are not yet quantified.

The two projected samples are slower than both ordinary samples for each image.
Keep ordinary as this workload's control. Reject current inline/projected runtime
admission; do not promote a local assembly fix just because its counters improve.

`D-MAPPING-TRACE` is a separate instrumented eight-wide runtime observation:
Coding cold claim was 1.642-1.662s and combined about 2.666s. It used private
regional TLS, historical runsc and a sparse large root without occupied-memory
acceptance. Do not combine its durations with helper timings or present it as
production parity. `D-CONSTRUCTOR-AUDIT` found about 64ms maximum root-mapping
call time; even optimistic removal with everything else fixed leaves Coding
combined above 2.6s. Root mapping alone is not a sufficient solution.

## Stage 1: establish the causal budget before choosing a mechanism

Reuse sealed traces and prior rejected experiments first. Produce one per-sample
dependency timeline for ingress/transaction, RootFS setup, runsc create/start,
authenticated procd readiness, and first-command execution. Under these spans,
identify mandatory reads, speculative reads, dependent remote round trips,
transferred/decoded bytes, CPU work and admission/scheduler waits.

Do not add overlapping phase maxima, multiply shared singleflight delays by lane
count, equate aggregate GET count with sequential round trips, or subtract cached
timing from cold timing as an exact causal attribution. Where ownership/dependency
is missing, label it unknown and add only the missing observation.

For each candidate, state the measurable critical-path cost it could remove, an
optimistic savings estimate with its assumptions, and the remaining gap to 1s/2s.
Use this to rank structural changes before CPU/allocation micro-optimizations.
Counterfactual perfect-cache or zero-wait replay is diagnostic only, never a
replacement for empty-node acceptance. No new broad tracing run is admitted if
existing evidence already answers the question.

## Stage 2: select a structural change with a falsifiable cost model

Candidates must address a measured mechanism: whole-root-dependent work, serial
read dependencies, mapping/data representation coupling, read amplification, or
occupied-node contention. Do not assume the current inline design must be saved.

The model must predict together: critical-path round trips, mapping/data bytes,
mandatory/speculative decoding, per-read temporary memory and node-wide retained
memory under concurrency. A lower GET count with higher bytes/CPU is a tradeoff,
not an automatic win. Preserve independent authentication and exact ownership.

Import-time organization may be investigated only as a general durable format
operation, not a second state authority or command-specific startup profile. It
does not authorize fetching tenant data onto workers before claim. Any format
candidate must include incremental COW, import cost and arbitrary-file behavior.

## Stage 3: one-variable verification, then complete-path admission

1. Freeze baseline artifacts, software, hardware, cache state, occupied load,
   workload and observer settings. Predeclare predicted change and reject rule.
2. Verify bytes, corruption/partial-read behavior, cancellation, isolation and
   bounded memory locally. Helper benchmarks can reject a candidate cheaply;
   they cannot approve a runtime or substitute for a first real command.
3. Only an adequately supported candidate proceeds to paired/reversed remote
   full-path comparisons with empty-node and cached-new cohorts. Separate mixed
   image contention and identical-image sharing; validate actual occupied width
   and populated-large roots before acceptance. Verify current main/infra pins.
4. Record sample counts, distribution, maxima, all 1s/2s misses, bytes, CPU,
   memory/reclaim and resource cleanup. Never hide a failure in an average or
   erase it with a later good sample. Insufficient/confounded evidence remains
   inconclusive, regardless of how attractive a local percentage looks.

If prediction fails, revise the causal model or reject the variant. Do not append
another tuning sweep to the same design without new evidence. Keep one active
hypothesis and a documented stopping condition.

## Immediate next deliverable

Priority update after the user's machine/S3 hypothesis: D-HOST-S3 performs two
bounded original-object probes without product/layout changes. The first has
83 reads / 92 HTTP attempts and concurrent host counters; the second isolates
the SAME envelope's RSA unwrap at 5.199–5.438 ms (16 calls) after one extra GET.
Raw transport delays and local cold-header CPU both exist. This short 2-core
probe has no CPU throttling/steal or memory pressure but does not explain the
earlier 16-core occupied-runtime question. All 93 HTTP attempts succeed; no SLO
acceptance was run. See [HOST-S3.md](HOST-S3.md).

D-READER-COST now completes the original full-Reader component correlation:
14373ReadAt checks,547exact source calls and both shared-cache states match;
581HTTPsuccess and57actual crypto calls (cold50Coding/7Node, cached-new0).
In its serialized cold Reader time, HTTP intervals occupy88.525%, crypto outside
HTTP3.296%; do not mistake that for live startup or counterfactual savings.
Post-write waits reach175ms with regular20ms host samples, negligible enclosing
pressure and no TCP retransmission; no whole-runtime pause explains that wait.
The next history audit, D-ARRIVAL-PATH, corrects the proposed kernel re-probe:
sealed September 9 evidence already correlates all 1186 GETs to kernel arrival
on a real claim + node-v run. Its cold write-to-callback mean is18.573ms, versus
0.156ms from earliest relevant arrival to callback. It uses older artifacts and
does not retrospectively attribute D-READER-COST's175ms sample, but without a
changed mechanism it is sufficient to reject repeating broad kernel/TLS tracing.
The genuinely missing distinction is network versus request-ID-bound OSS service
time. See [ARRIVAL-PATH.md](ARRIVAL-PATH.md).

D-ARRIVAL-PATH's new boot has two NEW ready carriers without a job/configuration
change. The preceding boot's two failures were allocation-registration409unique
constraint conflicts, not storage timeouts. Old slots are terminal/unclaimed;
new slots bind this boot. This removes an immediate fixture-readiness obstacle,
not the current-version, width, populated-root or occupied-node acceptance gaps.
Bucket logging is disabled and the exact-bucket SLS access-log policy query
returns zero rules. Existing service-log availability must be established before
rerunning requests solely for service correlation. No production diagnosis is
inferred from this persistent two-core test fixture's reboot behavior.
Do not repeat either the three-object microbenchmark or the full uninstrumented
Reader replay, or blindly change hardware, cryptography, concurrency or timeouts.
Combined codec cache-hit binding,
cancellation and concurrent guards remain required; this priority change does
not admit them. The successive boot observations are D-HOST-S3ready2,
D-READER-COSTready0, D-ARRIVAL-PATHready2. All retain unchanged job/configuration;
the later automatic replacement does not retroactively repair an earlier run.

D-OSS-SERVICE adds actual provider processing evidence: one unchanged original
Reader schedule produces581successful GETs, but all client request-ID validations
fail and the diagnostic remains failed. The matching14second test-bucket log
window has581unique GETs with exactly the same object-hash/response-byte
multiplicities. OSS server processing median/P95/max is3/25/68ms; provider response
time is7/32/71ms. These are window-population statistics, not per-attempt service
correlation, RTT, exclusive critical-path time or startup improvement. Local CPU
is78.654%idle without observed throttling/steal/memory PSI, but this does not
exonerate hardware at occupied width. See [OSS-SERVICE.md](OSS-SERVICE.md).

One exact-test-bucket logging policy was enabled with existing service authority,
then disabled and verified before its original20minute deadline. The managed
regional logstore retains data7days and has two shards; disabled policy and
resources remain for audit. The latest boot has ready1, not a healthy full-runtime
acceptance fixture. Next validate a SINGLE bounded actual OSS/S3 response-header
and provider-log correlation before any larger replay; reuse existing dependency
traces to quantify mandatory serial remote waits. Keep the581request failure,
fixed timeout/caches, original artifacts and all acceptance gaps explicit.

D-OSS-ID closes the single-request observation gate: actual S3-compatible OSS
returns `x-amz-request-id`, not `x-oss-request-id`. One SHA-bound1KiB request maps
to exactly one service log; client post-write20.971ms, provider response20ms and
server processing7ms. One fresh-connection smoke sample is not a distribution,
RTT decomposition or startup result. Its exact-bucket logging rule is disabled
early; compute isStopped/StopCharging. See [OSS-ID.md](OSS-ID.md).

Reuse D-HTTP-DEPENDENCY rather than repeating the dependency/arrival trace. Its
existing payload-wait coverage screen already supplies the structural target.
Candidate review now reproduces and fixes an original-parent binding bypass on
memoized selector hits; see [COMBINED-PARENT-BINDING.md](COMBINED-PARENT-BINDING.md).
The temporary candidate validates checksum/length/encoding on every hit and
charges the retained binding to the same cache. This does not qualify cancellation
or concurrency, and changed cache charges need complete-stream revalidation once
those guards stabilize. Next finish these guards and actual encrypted-source/
decode net cost for BOTH images, not another baseline-only or tuning sweep.

D-COMBINED-LIFETIME now qualifies the temporary candidate's bounded shared
selector/packet lifetimes and concurrent reads. An already-canceled selector
still doing GET/parse/publication is reproduced and corrected; live followers
survive another caller's cancellation, abandoned unresponsive work stays counted,
and packet sharing requires the complete recipient contract. Ordinary Reader
singleflight semantics are unchanged. Both images pass eight independent local
concurrent reads; this is not occupied runtime width. The new 57,492-read matrix
passes with identical ordinary controls and fixed cache budgets. Candidate source
windows remain355→330 cold and192→167 cached-new, while encoded bytes increase
741,750/925,820. No net performance improvement is inferred. Next actual encrypted
source/decode/CPU/memory net cost, not another baseline or timeout/cache sweep.
See [COMBINED-LIFETIME.md](COMBINED-LIFETIME.md).

D-COMBINED-TRANSPORT now completes the actual encrypted-source component
comparison, with the unchanged corrected candidate and fixed O/C/C/O order.
All57,492ReadAt outputs,64constructors,full cache/source schedules and2206actual
HTTP responses verify. Cold gains vary greatly; cached-new reverses for BOTH
images in the second pair. The preregistered net screen does not pass: no runtime
admission. Identical ordinary cold shapes vary6418→3856ms Reader, chiefly in
HTTP post-write waits; this is not an OSS-defect or machine-exoneration proof.
Candidate cached-new first-touches eight new Coding encrypted frame groups,
adding41.38–41.88ms exclusive header processing, not header-cache eviction.
No request/cache/timeout/hardware sweep or unchanged repeat is justified.
Compute isStopped/StopCharging. See [COMBINED-TRANSPORT.md](COMBINED-TRANSPORT.md).

D-COMBINED-FIRST-TOUCH closes the eight-object cache-cause question with passive
observation of the actual unchanged bounded cache. All14,373ReadAt outputs,
16constructors,330/167source sequences and cache snapshots match. Each target
data entry is explicitly evicted while its protected mapping leaf survives;
three last evictions come from Node reads sharing the mixed cache. Five switch
from their own joint packet, two have no bounded joint, and one was supplied by
another trigger's packet. Only five prior headers are reusable, about26ms of
exclusive cost; simply refetching those old joints adds220,164encoded and
988,720decoded bytes and changes four data publication orders. Do not implement
that shortcut. This is not occupied-width or startup evidence. No remote command
this turn. See [COMBINED-FIRST-TOUCH.md](COMBINED-FIRST-TOUCH.md).

Completed screens, without new runtime admission:

- [CRITICAL-PATH-BUDGET.md](CRITICAL-PATH-BUDGET.md): matched stage and read-wait
  exposures; index-only and ensure-only removal do not independently close the
  combined-path gap under the explicit fixed-other-cost assumption.
- [FRAME-GROUP-FEASIBILITY.md](FRAME-GROUP-FEASIBILITY.md): whole-frame data-only
  groups preserve ordinary bulk geometry, but demonstrated coverage does not
  justify them as the standalone 2s solution.
- [JOINT-DELIVERY-FEASIBILITY.md](JOINT-DELIVERY-FEASIBILITY.md): wrapping old leaves
  indiscriminately exceeds the packet budget; a trigger-leaf-only capsule retains
  more complete groups without speculative foreign mapping pages. Selection must
  happen before the target leaf, and its current modeled parent records are large.
- [JOINT-SELECTOR-CODEC.md](JOINT-SELECTOR-CODEC.md): actual bounded selector encoding
  and size-matched encrypted-store tests show one current-tree parent and one cold
  provider read for both images. Dense synthetic concentration adds a hierarchy
  level; a manifest-only address is explicitly NOT a full-object content identity.
- [JOINT-BYTE-COST.md](JOINT-BYTE-COST.md): complete original source bytes, real
  full-packet addresses and actual parent envelopes validate. A first net model
  omitted ordinary grouped mapping reads and is rejected; its correction matches
  actual Reader baselines. Unconditional packets amplify Node costs; only the
  leaf-miss policy remains eligible for causal screening, not runtime admission.
- [JOINT-CRITICAL-PATH.md](JOINT-CRITICAL-PATH.md): all16 complete streams replayed
  with actual original bytes/Reader at128MiB; every request's source range
  schedule matches. Node never activates leaf-miss packets, while Coding's
  favorable free-packet screen still misses2s once retained ordinary stages are
  kept. Stop this standalone packet branch. An exact owned slow data read already
  overlaps header and ciphertext calls; missing HTTP sub-phases now matter.

- [SOURCE-HTTP-ATTRIBUTION.md](SOURCE-HTTP-ATTRIBUTION.md):78 fixed original
  encrypted reads,98 successful HTTP attempts, no retries. Fresh-provider data
  reads have49.187ms median versus12.247ms with the provider reused but a fresh
  encryption header. The slowest ciphertext attempt spends16.699ms acquiring a
  connection and69.873ms waiting from request write to first byte, then0.128ms
  through body consumption. This is component evidence, not historical startup
  attribution. The candidate ALREADY reuses one provider per ctld lifetime and
  prepares credentials before accepting work; another provider cache is not a fix.

- [RUNTIME-HTTP-PREFLIGHT.md](RUNTIME-HTTP-PREFLIGHT.md): the bounded full-runtime
  diagnostic build passed objectstore/rootfsblock race tests, but the first OFF
  arm stopped before any claim: free disk was below its predeclared trace budget
  and the external sampler assumed a nonexistent leaf `cpu.max`. Zero startup
  samples, commands or runtime HTTP traces; no overhead calibration or runtime
  causal result. Preserve the failed campaign. Before another run, validate the
  sampler against actual cgroup ancestry and check disk budget before installation.

- [HTTP-CALIBRATION.md](HTTP-CALIBRATION.md): the new campaign fixed and validated
  those observer prerequisites before installation, then completed all64claims
  and64realcommands across four fresh-boot OFF/ON/ON/OFF controls. Cold combined
  latency exceeds2s in24of32samples; cached-new combined misses0of32. Coding still
  misses with tracing OFF. Of1298HTTP attempts,1292reuse connections; selected
  slow post-write waits predominantly show callback goroutine Waiting, not long
  GC or runnable delay. Network/kernel/service causality is not distinguished.
  Per-image calibration rejects traced cached-Node command absolute timings;
  aggregate controls must not conceal that exception. No production acceptance.

- [HTTP-DEPENDENCY.md](HTTP-DEPENDENCY.md): all1282startup-NBD HTTP attempts linked
  through explicit source/flight operations;5128subphase maps independently
  verified over82046interval bins. Only4of16unowned attempts are startup mapping
  constructors;12are post-command. A generous all-image connection-only zero-cost
  screen still leaves Coding combined2.52–2.69s. Its single-outstanding payload
  post-write exposure spans both claim/command; a candidate targeting only those
  intervals would need48–68% coverage reduction with other costs fixed. This is
  not a full guest/kernel dependency proof or guaranteed savings.

- [COMBINED-DELIVERY.md](COMBINED-DELIVERY.md): the first unified ideal-cache
  screen covers all14,373retained requests. Coding source windows are284ordinary,
  275joint-only,273data-only and269combined; adding independent reductions would
  incorrectly predict264. Even with all new delivery free, only160–178ms of
  Coding combined2s headroom remains. This survives necessary coverage screening,
  not actual-cost/runtime admission. Complete manifest bounds reject five whole
  Coding groups. An actual original-tree/decoder audit identifies31missing
  immutable frames totaling234,751encoded bytes; exact parent-bound locators are
  ready. No source bytes may be fabricated or groups dropped for missing fixtures.

- [COMBINED-BYTES.md](COMBINED-BYTES.md): the31exact missing frames are now
  retrieved and verified; all1850actual complete data groups and both combined
  selectors validate with both current encryption algorithms. Actual envelope
  costs raise per-stream stored bytes by554650Coding/181802Node versus ordinary,
  while source windows stay269/228. This corrects previously omitted costs, not
  a startup speedup. Shared128MiB mixed-root eviction is still unmodeled. Remote
  warm readiness failed at0before export; it was not repaired or used for claims.
  Object-only before/after state verifies; test compute isStopped/StopCharging.

- [COMBINED-CACHE.md](COMBINED-CACHE.md): both prototype and corrected matrices
  use the actual Reader/shared128MiB cache, not a rewritten LRU. Each completes
  57492output comparisons and16exact original source schedules. Shared cold
  windows355->330with741750additional encoded bytes; reused-cache new identities
  192->167with925820additional bytes. A targeted guard exposed a selector reload
  on an otherwise fully cached demand; corrected cached-mapping-first dispatch
  changes that from1source call to0. Rerunning the full matrix preserves every
  observed source/counter result. Retained-cache accounting stays bounded, not an
  RSS/occupied-density or timing proof. This completes the cache prerequisite;
  next is matched actual encrypted transport and decode/CPU net cost, not a sweep.

Finish ONE unified conditional delivery model: joint mapping/data on mapping
miss plus whole-frame data grouping on small payload miss, with unchanged ordinary
bulk behavior. This is a wider-coverage hypothesis, not re-admission of either
standalone rejected branch. Recompute interactions using complete streams, actual
encoded selectors/bytes and bounded shared cache; never add the two prior modeled
savings or assume their independent cache outcomes. Reject before deployment if
the net mechanism cannot close the gap without worsening Node or density costs.
The missing-source, actual full-group/selector/encryption and bounded shared-cache
prerequisites are complete for the current diagnostic images. Actual encrypted
net-cost screening is now complete but inconsistent, not passed. Do not repeat
these exports, ideal-cache screens or unchanged remote replay. Keep the current
candidate out of runtime. The recorded first-touch member/cache decisions are
now audited; per-trigger old-packet reuse has only about26ms favorable header
credit and adds bytes/order changes. [COMBINED-PACKED.md](COMBINED-PACKED.md) has
now qualified complete physical placement of ALL3660unchanged representations,
not just the eight hot objects. Actual five64MiB-bounded packs and both parents
validate with both crypto algorithms. New stored parents8409/220557bytes fit
existing bounds. In the SAME49diagnostic source calls, headers33->7 but provider
ranges49->52 and returned ciphertext increases735112bytes across AES cohorts.
These are local provider counts, not OSS HTTP or a new full shared-cache replay.
Logical payload source windows are unchanged; nonzero-offset first touches and
cipher alignment move costs. Crediting only the26removed headers against prior
measured arms gives92–96ms cold/41ms cached-new; reversed cached-new Reader still
differs+449.487ms with everything else fixed and added costs ignored. HTTP
variability remains, so this is not a universal regression or startup bound.
Stop advancing the unchanged combined candidate solely through header packing.
Do not implement its new metadata/cache/lifetime or launch another remote sweep
without a materially wider dependency reduction or demonstrated net-cost cause.
A smaller envelope count cannot replace the payload-wait coverage requirement.
Machine/S3 attribution has advanced in
[CORRELATED-READER.md](CORRELATED-READER.md): one fixed ordinary Reader schedule
now matches ALL581client IDs to exact OSS object/status/byte records. Of121cold
post-write waits>=20ms,116also have OSSresponse>=20ms and62processing>=20ms.
RTT estimates are generally sub-millisecond; host80%idle without throttle/memory
pressure. Large exact delays are not explained solely by local unwrap or physical
RTT. Two retransmission-counter increments also remain; no blanket host/network
exoneration or OSS-internal causal split follows. The2CPUhelper still does not
establish occupied production-width health or current-runtime acceptance.
This correlation prerequisite is COMPLETE, not grounds for another ordinary
baseline or pool/hardware sweep. Reuse the sealed real-startup dependency
evidence to choose a materially wider mandatory-read reduction and explicitly
account for service-response waits, rather than projecting helper quantiles or
parallel service sums directly onto the guest/kernel critical path.
Include both-image extra bytes, decode/verification/CPU and retained/re-read costs;
reject a proposal whose fixed-other-cost bound cannot close the remaining gap.
[METADATA-SUFFICIENCY.md](METADATA-SUFFICIENCY.md) now adds a non-overlapping
request-class screen across all32retained phase windows. Coding metadata-payload
union705–707ms has42–45ms overlap with other source waits; exclusive662–663ms
does not quite cover its666ms combined gap in the fixed-other calculation.
Including these requests' mapping dependencies yields858–875ms exclusive
exposure, but closing2s needs76–78%removal before new costs. These are opportunity
screens, not causal critical-path bounds or new startup results. Do not restart
metadata-only packet grouping based on uncorrected wait sums.
A compact immutable filesystem base is an UNCHOSEN wider hypothesis, not an
approved implementation. Audit either independent base/upper or a single-XFS
wrapper with immutable lower image; do not assume two NBDs are inevitable or
one-NBD wrapping has no extra cost. Main's exact branch-LBA/FIEMAP rebase contract,
new loop/lower mount absence proofs, format admission, inode/device behavior and
populated writable-history semantics must be resolved before runtime work.
Only then test actual complete representation/net cost without relabeling the
old XFS request stream as a new filesystem trace. No product change or remote
operation was made for this source/interval audit; all original gates remain.
[COMPACT-BASE-FEASIBILITY.md](COMPACT-BASE-FEASIBILITY.md) subsequently builds
both complete plainEROFSimages from exact retained sources. Full byte/metadata/
link/xattr trees and separate root metadata match; no source pull/rebuild replay.
The current scanner actually rejects inline/non-aligned extents on bothimages;
matched same-kernel/binary XFScontrols pass. Current-code tests also show that
rawdifferentdevice offsets alias and droppingextents destroys logical file-data
coverage. Therefore qualify separate logical coverage and exact writable physical
attribution before adapting rebase; neither acceptingflags nor emptyingextents is
safe. Preserve immutablebase/generation binding,copyup,whiteout,hole,truncate,
rename/hardlink and terminalproof contracts. Actual sizes249196544/4506877952bytes
do not include a writable wrapper or any runtime/OSS cost. Retained source
builds/scans are preparatory and cacheaffected, not latency/density evidence.
Reuse the two exact outputimages; do not repeat the source export/build or
substitute oldXFSrequests for futurefilesystem-demand evidence. The test machine
is freshlyStopped/StopCharging and oldsources/runtimestate are preserved; no
runtimeformat has been admitted and all original fullpath gates remain open.
Before any future full-path trial, resolve remote warm readiness and verify
current pins. Neither the two-core helper's RSS nor local concurrent tests prove
occupied-node density or a size-independent startup bound.
[LAYERED-REBASE-MODEL.md](LAYERED-REBASE-MODEL.md) advances the logical/physical
separation into an isolated Go overlay of the existing Diff algorithm. Tested
copy-up and rename/hardlink identities distinguish regular lower/upper layers;
logical lower coverage no longer requires a fabricated physical address. Six
adversarial guard gaps are retained as red tests and then corrected. Existing
Apply and proof contracts remain unchanged and reject unsupported plans. This
is not an authenticated mount scanner, durable image binding or layered Apply;
directory provenance and actual filesystem semantics remain open. Two-package
race passes with14explicit privileged/integration skips, not full acceptance.
Next qualify actual merged-mount provenance/semantics on the retained images,
then measure the complete representation's own reads and net cold/density cost.
No startup or remote operation was performed for the local model; existing OSS
response-path and host evidence is neither replaced nor contradicted.
[LAYERED-MOUNT-PROVENANCE.md](LAYERED-MOUNT-PROVENANCE.md) then tests selected
native operations in BOTH complete retained images. Copy-up preserves merged
stat identity while FIEMAP switches to direct upper data; merged pseudo-devices
do not equal real lower/upper devices, and GETVERSION remains unavailable.
Therefore do not populate the model using merged stat as physical provenance:
resolve the actual data layer independently and bind it to exact offline branch
authority. No synthetic relabeling, relaxed flags or full-tree claim-time scan.
Existing hardlink writes fail in the current-options same-XFS focused control
as well as both candidate trees, so this is not an EROFS-only regression. Source
ELF sparse holes become allocated zeros in the plain candidate despite equal
bytes. These are open contracts, not a passed filesystem compatibility gate.
Upper sparse/new-hardlink/whiteout cases and the previously skipped privileged
whiteout Apply test pass; other NBD/RustFS/gVisor/proof gates remain untested.
No startup or actual new-format required-read result exists. Do not advance the
format without resolving these boundaries and measuring a complete embedding's
own net cold/occupied costs. The test host is verifiedStopped/StopCharging after
all owned tasks/mounts/loops terminate; retained images remain available.
[LAYERED-RESOLVER.md](LAYERED-RESOLVER.md) now qualifies a diagnostic held-root
resolver across all 5,775/132,932 actual regular paths, including copied-up upper
files, without using merged stat identity as physical authority. Only upper
extents enter writable attribution; lower inline extents retain logical data.
The first actual run failed on impure directory metadata, which was narrowly
qualified after source inspection; failed receipts remain. Exact XFS safety tests
and selected payload comparisons pass. Do not repeat full-image scans or count
their cached offline duration as startup. This is not a complete Manifest/Apply,
authenticated immutable-image binding, racing-writer proof or format adoption.
Preserve the open sparse/hardlink/gVisor contracts and require the complete
embedding's actual mandatory reads and net costs before any original SLO claim.
Account for work in BOTH claim and first command; more GETs alone do not establish
a longer serial critical path. Keep unknown header-channel/guest/kernel edges and
host I/O wait explicit.
[WRAPPER-DEMAND.md](WRAPPER-DEMAND.md) now measures the COMPLETE native one-XFS
wrapper through real NBD/runsc/procd and node-v, against fresh ordinary controls.
The exact measured wrapper increases unique base bytes61.6%/63.7%; excluding
all-zero reads preserves the increase. Most extra volume appears in first-command
reads, overwhelming narrow mount savings. Do not advance this wrapper to an
encrypted publication/format rollout on a metadata-only argument. Actual
mandatory/speculative provenance and remote object dependencies remain unknown;
the local-file timing is cache/observer affected, not a regional2s result.
Preserve all five harness versions/failures and four successful command traces.
The actual-loader red test prevents repeating the assignment mistake; logical
zero canonicalization fixes only a cache-sensitive diagnostic image fingerprint.
Outer staging unmount still failsEBUSY despite successful per-lane cleanup;
independent post-exit mount/loop/NBD absence is verified, not a successful unit.
Do not repeat full image construction or compatibility scans for the same layout.
Reopening requires an identified mechanism to remove the added native demand and
evidence of net gain, while keeping encrypted authority, original cold/cached-new,
populated histories, occupied width and fullpath command-ready gates unchanged.
No further broad capture or timeout/cache/pool/concurrency sweep is justified by
these results. Do not infer serial header/data fetches when evidence proves
overlap, or reopen the stopped packet branch without materially wider dependency
coverage or another demonstrated cause.
Current packets still lack durable import/COW/publication/retry/inventory/GC.
No new production source changes, rollout, merge or tag are authorized by this
plan. The original full-path, size-independence and density gates remain unchanged.

[DEMAND-PROVENANCE.md](DEMAND-PROVENANCE.md) narrows the complete-wrapper result:
NBD itself requests the extra55–57MiB;4KiB base fallback adds very little volume.
Exact file-offset composition and61,988 retained block checks attribute71–74%
of the increment to the identical Node/procd bodies, not just filesystem metadata.
Both ordinary images read identical Node/procd ranges despite different root
sizes. Separate this measured demand expansion from the independently observed
OSS response tail and unqualified occupied-machine costs. The diagnostic's generic
ReaderAt hides the frozen candidate's trusted Reader fast path; main still has
the block path. Synthetic adapter GET counts are not a new optimization result.
The next specific question is where loop/inner-file demand expands into outer
XFS/NBD reads, with actual DIO/device identity recorded. No kernel-option sweep,
full-tree rebuild, claim scan, compatibility repeat or format adoption follows
from that hypothesis. Preserve the current no-go until a measured structural
remedy removes the added demand without weakening the original contracts.

[LAYER-DEMAND.md](LAYER-DEMAND.md) provides the missing layer observation on one
fresh retained-wrapper guest: actual loopDIO=0 and22,447 buffered XFS reads;
known inner91,402,240bytes versus149,987,328 outer embedded-file bytes, with
59,768,832bytes outside known inner demand after accounting for sparse holes.
All extra bytes are covered by worker readahead BIOs. Backed Node/procd offsets
are identical to the ordinary control. Keep the45 early setup completions that
predate exact-loop issue filtering; conservative completion/XFS unions prevent
overclaiming, and the first-command issue/completion stream is complete.
The next justified causal comparison is bypassing the intermediary buffered
file path with exact mode/alignment validation, not a global readahead sweep.
At that point no DIO result existed. Removing all candidate-added reads only reopens
the read-volume screen; require encrypted cold/cached-new net benefit and all
unchanged RootFS/authority/production-width/regional command gates before adoption.

[LOOP-DIO.md](LOOP-DIO.md) now supplies that causal ABBA: actual DIO 0/1/1/0,
unchanged 512-byte sectors and 4096KiB readahead, identical inner executable
offsets, with extra outer-file bytes 57/0/0/57MiB. The direct wrapper's NBD demand
is only 2.43% below the previous ordinary control, not a proven S3 speedup.
Advance to one exact encrypted block-COW/S3 net-cost comparison with cold and
cached-new identities; do not repeat native ABBA, image rebuilds or broad option
sweeps. Preserve the generic file-adapter versus concrete Reader boundary when
using these traces. Net benefit and all full semantics/authority/occupied-width/
regional claim plus real-command gates remain open; no format adoption follows.
The initial analyzer failure and non-atomic setup filter gap are retained:
complete NBD and post-mount loop proofs do not mean a complete setup trace.

[LOOP-NET.md](LOOP-NET.md) completes the next encrypted storage-to-command
comparison. O/W/W/O cold local combined times are 2154/1348/1359/1355ms: the
reverse ordinary control removes the apparent large latency gain. The wrapper
reduces mean HTTP attempts22.35% but ciphertext bytes onlyabout1.3%; cached-new
guests all need zero object requests and complete locally in273–289ms. Preserve
the first ordinary >2s sample and do not call narrower local results regional
acceptance. No repeatable net latency advantage or format adoption follows.
Source staging is detached before guests; actual encrypted Reader, COW/NBD,
authenticated procd and new identities are verified. First publication failure
and harness credential/receipt fixes remain separate retained evidence.
Machine CPU scheduling and remote-object timing remain relevant: sampled CPU/I/O
pressure is present without steal/throttle/OOM, while ordinary client ciphertext
P95 changes43->21ms at nearly identical volume. Client acquire-to-close is not
pure OSS processing. Require response-wait versus node CPU attribution with
provider-cache limits; do not replay the same layout ABBA, SLS baseline, image
rebuild or broad machine/cache/RA/concurrency sweep. Original claim-time RootFS,
stateless shared pool, durable semantics, populated histories, occupied width
and regional first-command gates remain unchanged.

[COMMAND-COST.md](COMMAND-COST.md) supplies the missing real-command HTTP/CPU
split on ordinary layout. Cold self CPU stays857–890ms while elapsed time changes
2323->1343->1318ms and first-byte P95 changes42->15->18ms at similar bytes. About99%
of requests reuse connections. A first-byte wait is not pure OSS processing and
cannot be added to CPU time. Middle cold profiles attribute330ms sampled CPU to
decode/verify and110ms to the frame path; exclude50ms of diagnostic NBD hashes.
GC STW is2.304ms, not the second-scale delay. No production-width/regional proof.
This admits a bounded per-read frame scratch/in-place-AEAD cost experiment with
all existing security, ownership, range/partial-read and retry semantics, identical
object traffic and measured CPU/allocation benefit. The110ms whole-path sample
is not a promise of110ms savings or full gate closure. Avoid global pooling,
integrity-check removal, layout replay and broad machine/cache/RA/timeout sweeps.
Response-wait variation and occupied-machine effects remain independently open.

[FRAME-SCRATCH.md](FRAME-SCRATCH.md) completes that bounded experiment. The
same-machine control/candidate/candidate/control has cold combined 2138/1391/
1415/1273ms; reverse control again catches up. Read-process allocation falls
about19% (517-519MB -> 420-421MB), but late CPU is nearly equal and exact object
request multisets differ. This is a retained allocation candidate, not an adopted
startup optimization. All eight real commands and cleanup pass; no regional or
occupied-width acceptance follows. Do not repeat scratch tuning, layout ABBA or
a broad S3/machine baseline. Return to the full regional claim + immediate-command
budget and existing actual-runtime traces, with populated histories and occupied
physical width still required. If server correlation is needed there, reuse the
already qualified dual-header request-ID observer instead of the newer observer's
incomplete x-oss-only capture. Fundamental architecture and all gates stay fixed.

[REGIONAL-PARITY.md](REGIONAL-PARITY.md) qualifies the next full-path inputs:
the optimized worktree is based on current main, but retained complete runtime
trials use runsc20260810 versus the current infra20260817 pin. The official
binary, matching version-only shared compatibility class, current regional
harness and existing staged runtime artifacts are now verified. No guest has
run with the new pin, and no version-caused latency improvement is inferred.
Proceed to the actual fixed-width regional cold/cached-new comparison with
current pin/class and exact restore guards. Do not repeat this preparation or
use it to close populated-root/upper-history, occupied-width or latency gates.

[REGIONAL-RUN.md](REGIONAL-RUN.md) now completes that current-pin full-path trial.
All16 authenticated claims and real commands succeed, but all eight cold combined
samples miss2s: Node2.221s/Coding2.668s maxima. Cached-new maxima1.029/1.673s
remain distinct and are not proof of warm-cache immunity. The cold host has12.6%
average busy CPU,0 steal, no lease throttling and58.8GiB minimum available memory;
I/O pressure is substantial. This argues against aggregate CPU/memory exhaustion
in this unoccupied fixture, not against single-thread limits or all machine effects.
NBD-backed iowait cannot distinguish S3 processing, network and client dependency
waits. Do not repeat pin qualification or adopt a version-caused improvement.
Reuse this exact current regional budget. The follow-up below supersedes the
initial suggestion to trace cached Coding merely because its command is slower.
Keep populated histories, occupied width and every1s/2s gate open.

[FIRST-TOUCH.md](FIRST-TOUCH.md) independently replays2082 cold source-member
cache flags from the existing full regional trace. There is no claim-stage
eviction reload; only three relevant repeated source calls before command
completion expose24.145ms. Generously crediting entire affected NBD requests and
transitive followers still leaves Coding about2.560s under fixed other costs.
Even crediting mixed first-touch/known-content operations leaves about2.171s.
These are coverage screens, not measured speedups or universal lower bounds.
Cached Coding607-796ms variation predates the current pin; do not rediscover the
known cached payload-reload behavior in another full trace. Cache retention may
help cached-new guests but lacks recorded coverage as the cold-path main fix.
Admit a new candidate only with identified first-touch payload dependencies or
remote-response cost changes and quantified net CPU/memory/request costs. Reuse
HTTP-DEPENDENCY and existing server correlation, without another broad S3/machine/
layout/header/cache sweep. Zero new runtime samples or product changes; all
original full-path, populated-history, occupied-width and authority gates remain.

[PACK-RESPONSE.md](PACK-RESPONSE.md) tests one distinct remote response variable:
total encrypted object1MiB/64MiB, with identical target plaintext, exact range and
65616cipher bytes. Twelve balanced pairs show first-target post-write medians
7.731/7.250ms, only6small-object wins and disagreeing order strata. The predeclared
50%/5ms/9-pair screen fails: no size-only pack change, sweep or real-image reimport
is admitted. All48 target calls actually reach OSS; repeated-source medians fall
to4.932/4.607ms and all paired repeats improve. Do not attribute this to a caller
payload cache or promote it to empty-node startup. Fresh publication/header reads
can warm provider storage; network/client/order effects and pure server time stay
unseparated. Original2CPU fixture is not a regional/occupied-width sample. Its
pre-existing zero-ready-carrier boot state is retained, not repaired or called
healthy. No product/runtime change;24 synthetic encrypted objects and all receipts
are retained, original state preserved and compute stopped. All original gates
remain open; require a different identified first-touch mechanism before another
remote response experiment.

[POOL-BOOT.md](POOL-BOOT.md) separates a recovery failure from read performance.
The old original-fixture boot fails persisted task-config decode, attempts new
task identities within old allocations, gets409, and loses client artifacts to
disk-pressure GC before PG terminal publication. The next ordinary boot places
new ready carriers without manual repair. Current candidate/main independently
reproduce the EOF across Nomad's actual persistence codec, while direct-memory
recovery tests pass. Persisted normalized driver configuration is a distinct
missing contract; do not duplicate the existing post-terminal refill fix or
weaken exact-incarnation checks. This newly concrete regression is the next
narrow implementation lane before candidate reboot qualification. Filesystem96%
use is verified but no disk-latency causality is established. Prior pool-pin was
read-only, and this anomaly does not explain the2.668s ready-carrier sample.
No startup samples, S3 requests or product changes in this diagnosis; retained
failed observations and all acceptance gaps remain, with compute stopped.

[PERSISTED-CONFIG.md](PERSISTED-CONFIG.md) implements the newly reproduced
recovery defect, rather than another read-performance tuning lane. Version2
opaque handles retain normalized configuration; strict legacy/missing/state
binding rejection replaces reliance on Nomad's private non-persisted field.
Codec and actual BoltStateDB close/reopen recovery pass, with full driver race
and manager refill regressions. This local result does not validate remote
Nomad1.11.3 restart, boot replacement or a startup speedup. Next qualify same-boot
handle recovery and changed-boot new-allocation refill using the linux/amd64
artifact. The new1355-file inventory supersedes the historical1353-file candidate
only for current-source checks; preserve the old measurements and all full-path,
populated-history and occupied-width gaps. No new remote or object activity.

[RECOVERY-REMOTE.md](RECOVERY-REMOTE.md) now qualifies that recovery candidate on
actual Nomad1.11.3: same-boot restart keeps both original allocation/slot IDs and
fresh heartbeats; one ordinary new boot rejects missing old namespace bindings,
terminalizes old slots and automatically replaces both allocations with no job
mutation. This two-carrier/2CPU warm-only result is not a regional startup or
occupied-density result. An invalid test-only materializer disable and early
submission guard failures are retained and corrected, not labeled product bugs.
Host95–96% disk pressure is observed, without disk-latency/S3 causality. Preserve
the existing2.221/2.668s cold combined misses and all original requirements; do
not rerun recovery diagnosis or the rejected object-size-only S3 tuning lane.

[READ-HEDGE.md](READ-HEDGE.md) screens a new response-tail mechanism without a
remote run: delay10ms, one assumed7ms duplicate response, fixed20%token budget
and at most two modeled duplicate flights. Its bounded optimistic owned credit
leaves cold Coding2.345–2.527s; even free overlapping constructors cannot close2s.
Removing the budget adds60–63%cold GETs and still misses the slower cycle. Neither
modeled timing nor duplicate independence is a runtime result; all extra-load,
cancellation, guest-dependency and occupied-width limits remain. Reject this
specific hedge policy before code/remote tuning. Preserve Node/cached-new results
and their timing exclusions, current real2.221/2.668s misses, and every original
architecture, RootFS-history and density gate. No product or remote mutation.

[RESIDENT-PREFLIGHT.md](RESIDENT-PREFLIGHT.md) prepares the next distinct current-
candidate occupancy check, not another empty-node tuning sweep. A bounded native
helper and33synthetic evidence-validator tests reject false memory/CPU occupancy,
stale/expired guests and missing/aliased cgroups. Normal/race/signal/vet/build
checks pass; no remote sample exists yet. Policy arithmetic rejects the initial
7GiB/1750m assumption under the frozen baseline, before changing any test node.
Keep eight1792MiB/1750m leases and proposed seven1536MiB resident allocations;
this is CPU-busy, one-wide cached-new admission, not64GiB reclaim or eight new
claims. Historical D-OCCUPIED remains intact. Next wire and test exact live
transport/sampling and cleanup, then run the bounded same-identity idle/load/
release comparison. All original cold, populated-history, command and1s/2s
requirements remain unchanged; do not call local fixture preparation an SLO gain.

[RESIDENT-CONTROL.md](RESIDENT-CONTROL.md) completes the local one-shot resident
HTTP adapter, pinned-cgroup sampler and single target-window composition. Current
OpenAPI corrects the prior narrative input-field assumption before any remote
request. Exact live/lease/boot/incarnation checks and21top-level race-tested
cases reject replay, identity aliasing, stale output and invalid counter/physical
bindings; pure SQL checks do not claim a database qualification. Preserve both
mock setup failures and unchanged budgets. Full phase orchestration, real host
sampling/claims, independent remote absence and restoration remain next; this
local component result does not close any startup, occupied-memory, populated-
history or first-command gate and introduces no product runtime change.

[RESIDENT-MATRIX.md](RESIDENT-MATRIX.md) now composes the bounded full diagnostic
CLI and its independently canceled owned-ID cleanup, with31top-level Go tests
passing three race repetitions and45Ruby tests/133assertions. Preserve two real
initial encounters before starting finite helpers, then same-identity idle/load/
release comparisons; this prevents silently discarding first-touch cost or
calling it isolated CPU contention. Correct only pre-target synchronous transition
heartbeat gaps and retry verified pending states only. Collection success cannot
mask1s/2s timing misses. This is local fixture progress, not a remote result or
permission to change runtime budgets. Next qualify the isolated remote lifecycle
and run this exact matrix once; all cold/populated-history/occupied-width and
full-path requirements remain. Do not reopen the rejected machine/S3 tuning lanes.

[RESIDENT-REMOTE.md](RESIDENT-REMOTE.md) supplies real current-pin partial results:
seven resident claims plus two initial literal-node-v targets. Node claim159ms
still precedes1.109s command execution; Coding combined2.632s still misses2s.
No controlled load window passes. The private constructor initially combined a
none workload with a command gate; its corrected actual-config regression leaves
public validation unchanged. More importantly, ordinary non-PTY CMD creation
does not open stdin, so the helper's EOF behavior invalidates the load fixture.
Do not infer live occupancy from a successful context creation or pipe-backed
unit tests. Next adapt the finite helper to supported context signals, preserving
same-process proofs and every original hard requirement. Real known-ID cleanup,
physical absence and original runtime/job restoration are now exercised. This
does not establish machine/S3 causality, populated-history or production SLOs.

[RESIDENT-SIGNAL.md](RESIDENT-SIGNAL.md) repairs the finite test fixture through
the existing exact-context signal API and exercises the actual no-stdin procd
runner locally. A real matched-node matrix now admits six idle/load/release
windows with the same seven resident identities. Initial Node/Coding combined
1.306/2.375s remain visible; loaded cached-new0.683/0.270s does not close the cold
2s miss. Runtime pins, HTTP budgets and RootFS semantics did not change. The
ordered test proves cached targets can succeed under verified CPU occupancy,
not that CPU never matters or S3 is faulty, and it cannot separate accumulated
cache warming from the complete phase timing delta. Preserve original empty-
node, populated-history and true occupied-width gates. Return to the necessary
first-command read path rather than restarting rejected machine/S3 tuning lanes.

[EXEC-ADVICE.md](EXEC-ADVICE.md) rejects a new generic exec-time hint hypothesis
against the exact current stock pin: fadvise/madvise WILLNEED do nothing and
readahead returnsEINVAL. Do not trust its supported-table label or infer a read
from successful advice. Whole initialized Node ELF segments cover107,122,688bytes;
the historical read-overlap screen uses a different artifact without an exact
executable-hash bridge. Canonical v2 corrects that missing caveat, so the roughly
42MiB outside the old union is conditional geometry, not extra current I/O or
predicted savings. Ten geometry tests/921assertions and seven AST tests/vet pass,
with no product/remote activity or runtime improvement. Keep this hint lane
closed. Any actual-read mechanism needs same-file inputs, bounded ownership/
cancellation/concurrency, unchanged exec semantics and a net critical-path cost
case before remote admission. Existing machine/S3 evidence, real cold misses and
all populated-root, occupied-width and original architecture gates remain open.

[NODE-BINDING.md](NODE-BINDING.md) now hashes actual current format2 Node/Coding
files through a bounded remote read-only inspector. Node binaries and ELF
metadata are identical to each other and the old scan; seven dependency hash
sets differ, so the images remain confounded as a RootFS-size-only comparison.
Exact PostgreSQL metadata additionally identifies the old read-offset source as
retired format10005/mapping5, not current format2. Its historical executable
bridge is unverified and its byte union must not drive a current cost prediction.
No old decoder, runtime change or latency result is admitted. Preserve preflight
failures and this boot's original carrier0; isolated file inspection is not
healthy claim readiness. Both current scans finish with independent physical
cleanup and original files/processes/data preserved. Use current-format/source-
bound evidence for the next actual-read mechanism, keeping all original cold,
first-command, populated-history, density and1s/2s gates unchanged.
