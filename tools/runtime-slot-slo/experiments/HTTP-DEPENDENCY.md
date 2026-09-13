# D-HTTP-DEPENDENCY: owned waiting and a structural savings target

2026-09-12. Offline analysis of sealed full-path traces, not a new performance
sample, runtime implementation or acceptance pass. Fundamental requirements in
[STRUCTURED-OPTIMIZATION.md](STRUCTURED-OPTIMIZATION.md) remain unchanged.

## Decision

Do not prioritize connection-pool/establishment tuning or another mapping/header
cache as the standalone solution. Existing caches, provider reuse and independent
header/data reads are present in the actual frozen implementation. The remaining
large observed opportunity spans block-payload delivery in BOTH claim and command.

The new analysis attaches HTTP subphases through explicit source operations and
shared-flight dependencies. It does NOT recover a complete guest/kernel critical
path. In particular, an outstanding NBD read can include kernel read-ahead; even
single-outstanding exposure is not automatically mandatory guest blocking or
guaranteed removable time. Keep that distinction before admitting a candidate.

## Input verification and correction

Reverified all323D-HTTP-CALIBRATION artifacts,1353frozen product files/inventory,
and164preceding D-MAPPING-TRACE artifacts. Existing193worktree entries preserved.
No cloud query/start, new claim/command, RootFS read/write, product edit, local
e2e, production rollout, merge or tag in this turn. Fresh main refs are recorded
in the final evidence; historical measured software is not promoted to current
production parity.

The previous report's annotation of16unowned requests as constructors was wrong:

| Scope across all eight captures | HTTP attempts | New connections | Ciphertext body bytes |
| --- | ---: | ---: | ---: |
| Startup with explicit NBD ownership | 1282 | 2 | 170151974 |
| Startup mapping constructors without NBD ownership | 4 | 4 | 14660 |
| After every first command completed | 12 | 0 | 262464 |

Thus1286attempts belong to the startup windows;1298remains the correct whole-
capture total. Twelve post-command attempts cannot be charged to startup. The
original traces and sealed reports remain intact; repository annotations are
corrected explicitly. Original latency samples and observer exceptions do not
change. All cached-Node command absolute trace timings retain the prior
representativeness rejection.

## Method and independent validation

1. Attach each HTTP attempt to a complete `source.io` region on its calling
   goroutine or the explicitly captured creating-goroutine stack. Require full
   temporal containment and exact NBD owner agreement, not nearby timestamps.
2. Follow nested synchronous flight leaders and uniquely matched shared-flight
   calls, binding key, goroutine and exact operation start/end. Repeated loads
   of the same content hash are different operations.
3. Clip each connection, post-write-to-first-byte and body interval through
   those edges and intersect with recipient NBD Waiting/Syscall states.
4. Union per exact sandbox and claim/command window. Count ALL its NBD tasks
   when extracting single-outstanding periods, including reads without HTTP.
   Never multiply shared source costs by the number of recipients.

All1298source associations validate;1282startup-NBD attempts yield64phase windows
across32traced sandbox identities. Eight tests/17assertions cover overlap,
transitive flights, distinct repeated operations, clipping and independent reads.
A separate pointwise oracle follows ALL enclosing leaders rather than the main
nearest-parent interval algorithm: all5128HTTP subphase maps match across82046
interval bins. This validates bookkeeping, not unrecorded dependency edges.

The encryption-header cache has its own waiter channels without exact per-flight
identity in these traces. Extra recipients through those channels are deliberately
unmodeled. Unowned constructors are excluded from per-NBD exposures. HTTP phases
can overlap one another; their columns must not be added as disjoint costs.

## Cold Coding: where the owned exposure lies

Milliseconds, four samples per row. Mapping and payload can overlap across NBD
requests. The table reports exposure unions, NOT predicted savings or stage CPU.

| Cycle / phase | All post-write exposure | Single-outstanding post-write | Single-outstanding mapping part | Single-outstanding payload part |
| --- | ---: | ---: | ---: | ---: |
| 1 / claim | 1092.788–1096.092 | 838.415–925.169 | 272.954–329.236 | 565.460–596.364 |
| 1 / command | 856.280–863.282 | 626.101–633.126 | 83.300–90.305 | 542.800–542.834 |
| 2 / claim | 1059.274–1070.419 | 847.724–909.298 | 237.849–254.647 | 609.875–655.038 |
| 2 / command | 810.027–825.242 | 604.452–620.393 | 79.628–95.449 | 524.636–525.815 |

All these Coding post-write intervals come from reused connections. In the same
single-outstanding scopes, connection acquisition exposure is0.268–0.378ms in
claim and0.266–2.607ms in command. The recorded primary ctld GOMAXPROCS is16
through all four active captures (396metric records), not1. This is not proof
that all cores were utilized or that other CPU/contention costs are absent.

Node must remain part of candidate admission: its cold-command payload post-write
exposure is908.660–963.965ms overall,262.154–284.575ms single-outstanding. A
Coding-only metadata shortcut that fragments ordinary Node data delivery is not
an acceptable solution. Full cached-new and Node results remain in summary JSON.

## Quantitative screen, with all other costs held fixed

For a generous connection-only screen, credit EVERY observed acquisition
interval during each sample, including the other image and unowned constructors.
This avoids relying on incomplete recipient ownership. The union is64.910ms in
cycle1 and47.354ms in cycle2. Even if all of it vanished, cold Coding combined
would remain2688.732–2688.858ms and2520.789–2520.907ms respectively. It is not
a true architectural lower bound, but does not justify a pool-only solution.

The observed combined deficit is753.642–753.768ms in cycle1 and568.143–568.261ms
in cycle2. If a candidate claims savings ONLY from the recorded single-outstanding
payload post-write intervals, and all other durations stay fixed, it needs48.15%
to68.00% of that coverage just to close2s. Newly introduced I/O, mapping, decode,
cache pressure and cancellation work must also be charged. This is a target for
testing a mechanism, not an assertion that the saving is achievable.

Zeroing just the owned mapping post-write intervals leaves2188.393–2327.628ms
combined. This narrower screen excludes root/CPU/unmodeled waits; use the prior
complete mapping/constructor screens for their larger scopes. Do not add the
different screens as if their costs were independent.

## Next admissible hypothesis

The prior leaf-miss joint packets and whole-frame data groups remain rejected
as standalone fixes. A materially different possibility is conditional combined
delivery: joint mapping/data only on a mapping miss, whole-frame data grouping
on an actual small payload miss after mapping is already available, and ordinary
bulk reads unchanged. It must not split original frames or add an unbounded index.

Before any implementation or remote run, build ONE unified source/cache model
from the retained full-image directory graph and original bytes. Recompute both
policies' interaction over complete streams; do not add their separate historical
savings or assume independent cache hits. Include actual selectors/dictionaries,
encrypted bytes, frame verification, bounded128MiB mixed-root cache and new source
work. Stop this combination too if its net coverage cannot close the combined
gap without regressing Node. This is a new coverage hypothesis, not re-admission
of either unchanged stopped variant or permission to tune group sizes from traces.

No pre-claim tenant data, larger cache/timeout, weaker checksum/authentication,
RootFS visibility change or warm guest is authorized by this hypothesis. Storage
publication/COW/fencing/GC and occupied-size/density/full-command gates remain
required before any production admission.

## Evidence and reporting corrections

Root: `/tmp/sandbox0-http-dependency.DjIBlO`; canonical outputs are
`v1-analysis.json`, `summary-v2.json`, `validation.json` and `gomaxprocs.json`.
The initial summary accidentally allowed the standby calibration offset to
replace the primary offset. Its source/result are retained; primary-only v2
produces exactly the same numerical results. The main per-lane dependency
analysis was unaffected. Do not reuse the initial summary implementation.

Remote was not queried this turn. Last verified state remains the previous
2026-09-12T00:40:25.305309375UTC Stopped/StopCharging, original2CPU/8GiB, retained
data and restored configuration. All original goal gates remain open.

Final audit confirms freshly fetched sandbox0 main0f092204 and infra mainceb895c,
with all product/source evidence unchanged. All30artifacts sealed; evidence SHA256:
`c165634c903eedcdef2270a0b806924647e11b7b348785d661801de626a8ee13`.
Artifact-index SHA256:
`0afde4302e8ed61c5356eaf18f92d35594496a56d752b2b0c0f01ff8ae5dc52e`.
