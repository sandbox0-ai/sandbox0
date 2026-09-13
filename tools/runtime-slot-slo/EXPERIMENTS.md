# Cold-start experiment log

Updated: 2026-09-13. This is an investigation decision log, not a production
acceptance report or a change to the public API/SLO contract.

## Objective and measurement rules

- Keep generic, tenant-neutral warm carriers. Bind the tenant RootFS at claim;
  do not prewarm the target RootFS on the node.
- Optimize authenticated regional-ingress-to-procd readiness and the complete
  claim response toward 1s; the user accepts a 2s fallback investigation gate.
  Preserve canonical 1s misses in the original reports.
- Immediately execute a real `node -v` in each target sandbox. Record claim,
  command-only, and claim-to-command completion separately. Do not turn an
  arbitrary user process's startup time into the platform readiness contract.
- Keep the explicit 10s public request budget, readiness proof, durability,
  isolation, and cleanup checks unchanged. No claim/command POST retries.
- Separate the first target-node-cache-cold cohort from cached-node NEW sandbox
  identities. A reused sandbox is not a cached-new claim. Same-artifact lanes
  in a cold cohort may share in-flight reads; they are not independent cold nodes.
- Report actual occupied CPU/memory and reclaim, not just allocated leases.
  Sparse logical size is not populated RootFS size. Eight claims on the test
  node are not proof of production hardware/policy or high-density acceptance.

## Current reference baseline: B-UTC8

Keep the previous eight-way run as the reference for the next comparable trial.
This selection does not roll back or deploy production, and does not imply that
all supported RootFS sizes satisfy a hard bound.

| Input | Frozen reference |
| --- | --- |
| Evidence | `/tmp/sandbox0-utc-width8.H0tW4j/evidence.json` |
| Evidence SHA-256 | `e994d8cbbc6ad9c569f3ced82c0b63aef94acc42413fe545018dcf8628b609ea` |
| Source main | `0f09220460581bfc1fdc331f34ebc85bf38381e7` plus the explicitly hashed diagnostic/runtime artifacts, not an arbitrary build of the dirty worktree |
| Test node | 16 vCPU / 64GiB; 8 standard carriers; 10 batches of 8 NEW sandboxes |
| Per-claim lease | 1750m CPU / 1792MiB; formal policy 1GiB/CPU, max 4GiB |
| Runtime | Nomad 1.11.3; stock runsc `release-20260810.0`; UTC-logging procd |
| runsc SHA-256 | `670bcd3cbc103f00d8bb5098edc370f32397ee4c134231436bafa659bb3c068e` |
| procd SHA-256 | `29dedac3bce92b6a9a3d507889110927ede8727f87186376524e577617c69f2d` |
| ctld SHA-256 | `55fa1ef7ab8f619614daef8e896fef96a4dab4681ea9531060287c3e8ec8d85d` |
| manager SHA-256 | `6d81e008b8ce1ab26c6ec43418bc05bbd7cf71db3a0c813faa49f405a8aed80a` |
| driver SHA-256 | `1ab4e86cbfc587bd37c804c45883ef5e26bac21eb12c23b677a39d29192d09a5` |
| Read policy | Shared decoded cache 128MiB; source admission 8; coalescing window 1MiB; NBD max request 128KiB; observed readahead 4096KiB |
| RootFS cases | `mixed-xe3dad-node22`: 16GiB logical, about 256MiB nonzero; `cold-coding-agent-v060`: 1TiB sparse logical, about 4.68GiB nonzero |
| Node22 artifact | `sha256:f19e6d6672006966013d69e019d6fc69e1ecc042a844509ffcc3c9a6ea0cd4b1` |
| coding-agent artifact | `sha256:9d11df8f4db7b1a54774d39b366a24c3967afacfefb10255060f2b2afdf2a11c` |
| Workload / timeout | First command `node -v`, expected `v22.23.2\n`; 10s public timeout |
| Instrumentation | No runtime phase instrumentation; external read-only sysfs sampling |

All values below are observed maxima in seconds, not guarantees. Maxima in
different columns can come from different samples; do not add the maxima.

| Run | Cache cohort | n | Claim | Node command only | Claim + Node | Combined >2s |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| B-UTC8 | First cold cohort | 8 | 1.212635 | 1.565378 | 2.774360 | 8/8 |
| B-UTC8 | Cached-node new identities | 72 | 0.796459 | 1.140862 | 1.920306 | 0/72 |
| R-RUNSC17 | First cold cohort | 8 | 1.437780 | 1.690186 | 3.102282 | 4/8 |
| R-RUNSC17 | Cached-node new identities | 72 | 0.918416 | 1.560684 | 2.461699 | 36/72 |

Both runs had 80/80 claims numerically below 2s. B-UTC8 is the better observed
eight-way latency reference, but its cold combined path still misses 2s, and
neither run exercises meaningful resident-memory reclaim. Retain the earlier
occupied-node cold claim of 2.105s; later passing samples do not erase it.

## Latest changes and decisions

| ID | Change / question | Verified observation | Decision / missing evidence |
| --- | --- | --- | --- |
| P-UTC | Use UTC for procd's own logging, without changing guest `TZ` | Implementation and fresh-process/race tests retained; three matching durable artifacts prepared. Preparation itself made no claim/command measurements. | Retain candidate and its exact procd hash. Do not claim the entire later speedup is causally attributable to logging. |
| D-OCCUPIED | UTC procd with 6 CPU-busy, memory-touched residents plus 2 new targets | Observed about 13.162 CPU cores and 52.07GiB memory. Target cold max claim 1.603528s, command 1.782619s, combined 3.386202s; cached combined max 2.414180s. Memory PSI was only 1 microsecond. | Useful occupied-node evidence, not sustained reclaim or eight simultaneous new claims. No full acceptance. |
| B-UTC8 | Eight synchronized new targets, without resident-pressure fixture | See frozen table above; all 80 claim/readiness observations below 2s, cached combined max 1.920306s. | Preserve as reference, not production approval. |
| F-RECLAIM | Attempt resident-memory/reclaim fixture | Six 4GiB residents OOMed: 512MiB `/tmp` files were tmpfs and counted in addition to 3584MiB buffers. Follow-up attempted a 1GiB RootFS write/fsync workload, larger than the verified 512MiB per-writer dirty-tail budget, and stalled before the intended memory allocation. Eight preparation claims, zero target acceptance claims. | Invalid latency fixture. Do not repeat it. Use committed clean immutable resident data; separately investigate pressure/fence/cleanup ordering. Dirty-pressure auto-pause causality remains an inference. |
| R-RUNSC17 | Align stock runsc from Aug 10 to the infra-main Aug 17 pin; matching compatibility digest; other measured binaries/policy unchanged | Same 80-target matrix, but fresh boot and uncontrolled provider cache. Cached claim max rose 0.796459s -> 0.918416s; cached command max 1.140862s -> 1.560684s. All 40 combined misses belong to coding-agent. NBD reads stayed about 731MiB per eight-claim/command cohort. | Observed latency deterioration, NOT an optimization success. Cross-boot, non-randomized data do not prove a causal runsc regression. Pin parity is distinct from performance improvement. Keep B-UTC8 as comparison reference. |
| A-LEDGER | Centralize prior experiments and freeze decisions | 154 historical status/locator records imported with source-record hashes. Recent results cross-checked against their raw evidence. | No runtime change or new performance result. Use this log before scheduling another trial. |

Evidence locators: P-UTC `/tmp/sandbox0-procd-utc.SUiqqW`; D-OCCUPIED
`/tmp/sandbox0-utc-density.GpsBQW`; F-RECLAIM
`/tmp/sandbox0-utc-reclaim.zWOSEB`; R-RUNSC17
`/tmp/sandbox0-runsc-parity.iPQB1g` (evidence SHA-256
`710c120a5ab4c02931518e5130b88fdaa05bb22e58109e5453ed64d5ff6774b7`).
Raw artifacts are local/private evidence, not published files in this repository.

## Already tested: do not restart these hypotheses without new evidence

These decisions apply to the tested variants, not to every possible design in
the same family. Search the ID in [history.jsonl](experiments/history.jsonl) for
the original status, evidence path, and available checksum verification.

| Direction | Existing result | Reopen only for |
| --- | --- | --- |
| Increase source admission 8 -> 16 | Initial pair showed modest gains but cold combined still >3s; reversed occupied-node trials exist. No established general fix or default promotion. | A new measured contention mechanism and matched cold/occupied comparison; inspect `observer_free_source_admission8_vs16_paired_validation` and `occupied_node_reversed_source_admission16_vs8_actual_load` first. |
| Increase NBD request cap to 1MiB | Matched reverse-order comparison completed; decision was to retain 128KiB. | A falsifiable new mechanism, not another request-size sweep. See `cold_reverse_nbd_cap_matched_preflight_and_connection_budget_probe`. |
| Disable readahead | Fewer bytes, more small requests; all 18 cold claims in the disabled arm exceeded 2s. | A different demand-batching design; do not repeat unchanged 0KiB. See `kernel_readahead_off_matched_cold_regression`. |
| Reduce readahead 4096 -> 128KiB | Roughly halved block bytes and reduced sampled RSS, but mixed latency tradeoffs; matched cold and occupied trials already exist. | A specific uncovered workload/pressure condition. See `partial_readahead_matched_cold_comparison` and `partial_readahead_occupied_node_comparison`. |
| Increase decoded cache 128 -> 512MiB | Cached Node max 956.081ms -> 160.332ms, but uncached combined remained about 2.97s; sampled combined ctld RSS increased about 334MB. | An explicit cache/density budget decision, not a stateless-cold solution. See `bounded_cache_capacity_ab_separates_cached_gain_from_uncached_limit`. |
| Always-encoded data cache | Working-set CPU comparison rejected the prototype. | A materially different representation with bounded CPU/memory cost. See `encoded_cache_representation_rejected_after_working_set_cpu_comparison`. |
| Independent demand-read workers / reusable suffix buffers | Correctness and forward/reverse remote comparisons exist; no adoption-ready result. Sequential baseline retained. | New measured allocation/contention evidence, not tuning from one favorable tail. See `bounded_independent_demand_read_reversed_remote_ab` and `bounded_suffix_reuse_observer_free_remote_ab`. |
| Node file layout / page faults / XFS readahead | Previous artifact's Node/library extents, fault observations and large-folio/readahead byte coverage were already analyzed. | Only the missing delta to the current artifact/executable identity; do not redo the whole forensic chain. See `generic_current_node_file_layout_forensics_run`, `generic_current_node_fault_demand_forensics_run`, and `generic_current_node_readahead_coverage_run`. |
| HTTP/provider first-byte wait | Correlated traces and kernel-packet timing exist; prior waits were predominantly before data arrival, not post-arrival Go/TLS work. Not proof of pure OSS server time. | Current-version evidence that changes the causal model. See `remote_kernel_tcp_first_byte_arrival_attribution` and `correlated_command_source_wait_stack_and_family_audit`. |

## Next experiment admission

Before starting compute or modifying a test runtime, append a planned entry with:

1. A unique ID, the exact baseline ID, one falsifiable hypothesis, and the one
   independent variable. If more must change, list the confounders explicitly.
2. Source/binary/config/template hashes; actual hardware, carrier width, lease
   policy, storage/encryption geometry, cache state, and observer configuration.
3. Existing related IDs and the specific new evidence that justifies repetition.
   A previously proposed `next_work` item is not evidence that it remains useful.
4. Expected directional effect, safety bounds, sample/cohort plan, and a decision
   rule fixed before seeing results. Do not redefine success around a fast case.
5. Rollback targets and an exact live operation handle. An observation timeout
   does not justify replaying an operation that may still be running.

After the trial, append the result under the same ID: actual change, successful /
failed / unattempted counts, cold and cached-new claim/readiness/command/combined
distributions, occupied CPU/memory/PSI/OOM, read-byte/request scope, cleanup,
immutable evidence hashes, and one of these decisions:

- **Retain candidate**: reproducible benefit with preserved safety; state which
  required gates are still missing. This is not production approval.
- **Reject variant**: regression, unacceptable cost, or disproved mechanism;
  restore the reference and state what would justify reopening.
- **Inconclusive**: confounded or insufficient comparison; do not call it a gain.
- **Invalid fixture**: failed prerequisites; never count it as latency acceptance.
- **Diagnostic only**: changed understanding, not runtime performance.

Do not overwrite failures or promote a new reference merely because its version
is newer. A claim that a candidate is faster needs a comparable baseline;
production adoption additionally needs the full requested cold-cache,
populated-RootFS and actual high-density validation.

The latest crossover did not establish a stable runsc regression. D-NODE-IDENTITY
proves identical Node bytes but different dependency candidates. D-NODE-GEOMETRY
now proves that these identical files have disjoint decoded block-cache keys
because their file-relative block boundaries differ. This identifies a sharing
limitation, not its latency cost. Reuse the existing read/transport work; any
alignment/chunking candidate must preserve authenticated geometry and account
for import/incremental-COW and small-file costs. No unchanged crossover or
blanket tuning sweep is justified. Neither universal 2s startup nor production
release is approved by this log.

D-XFS-STRIPE admits a diagnostic allocation-alignment candidate to real-artifact
preparation: synthetic cross-layout sharing improved without a small-file
allocation increase. This does not change B-UTC8 or establish a startup result.

D-XFS-REAL-VOLUME subsequently rejects this specific mkfs-stripe candidate:
both real source trees were preserved, but complete Node/procd range-key
intersection remained zero. Do not advance this policy to durable publication
or startup benchmarking, or extrapolate the earlier synthetic result. A new
alignment/chunking mechanism needs explicit file-relative boundary evidence and
storage/incremental-COW cost accounting, not an unchanged stripe/AG sweep.

D-FILE-RANGES subsequently admits a different, publication-only mechanism:
complete file-relative units restored shared decoded keys on all four retained
real layouts, with bounded additional entries and encoded bytes in identical
target-containing windows. No allocator setting or performance baseline changed.
Next verify a generic full-image import planner with explicit provenance,
hardlink/shared-extent safety and complete-image costs; do not repeat this probe
or equate its full-file key intersection with actual startup-cache hit rate.

## History maintenance

### I-RANGE-LAYOUT (2026-09-10, completed implementation step; activation pending)

- **Admission:** D-FILE-RANGES passed the selected-window mechanism gate.
  Replace its temporary duplicate publisher with an explicit bounded layout
  input to the existing streamed block builder. Preserve B-UTC8; no latency
  inference or activation follows from this implementation step.
- **Implementation boundary:** immutable compact preferred spans, not one
  record per 64KiB block; full block-aligned payloads only. Validate size/range
  binding, bounds and overlap before I/O/publication. Normalize adjacent spans
  without mutating the caller. Keep a per-build cursor, existing codec, pack
  publisher, mapping frontier and inventory; do not add a second publisher.
- **Identity / activation guard:** the existing entry point must remain byte-
  identical with no layout. A new explicit format-two entry point will not be
  wired into manager/importer configuration until immutable operation and
  ready-artifact provenance plus exact selection are implemented. Existing
  operations record range size but not the proposed segmentation policy; do
  not reuse a ready artifact by size alone as evidence of the new policy.
- **Tests / decision:** reject malformed/unbound layouts before writes, preserve
  old zero-layout output/objects, verify all bytes over random gaps and offsets,
  sparse buffers, pack/page boundaries, huge logical-span compactness, shared
  cache/coalescing, encryption and incremental-COW contracts. Run relevant unit
  and race suites, not local e2e. Failures remain recorded. This step cannot
  establish full-image import cost, privileged XFS scanning, durable readiness
  or cold/cached-new/high-density command acceptance.
- **Workspace:** branch `perf/rootfs-demand-read`, main/HEAD `0f09220460581bfc1fdc331f34ebc85bf38381e7`.
  Preserve all existing dirty changes; snapshot overlapping builder/docs before
  editing. Evidence root `/tmp/sandbox0-range-layout.GJyxxY`. No remote compute,
  object-store/DB mutation, rollout, merge or tag is authorized by this entry.
- **Implemented:** `DataRangeLayout` now owns at most 1,048,576 compact spans
  (16MiB of span storage) and a separate cursor per build. The explicit
  `BuildMaterializedGenerationWithLayout` delegates to the existing streamed
  builder, codec, pack publisher, map frontier and inventory. No alternate
  production publisher, cache, timeout, readiness or runtime path was added.
  Default entry-point behavior and durable importer selection remain unchanged.
- **Verified:** baseline block tests passed before editing. Ten new top-level
  tests cover layout normalization/ownership, bounds/overlap, pre-I/O binding,
  byte-identical empty plans at 4/16/64KiB, 24 deterministic randomized coverage
  cases, sparse residuals, pack/page boundaries, int64-limit arithmetic,
  concurrent builders, cancellation/short reads, cache/coalescing, encrypted
  raw/compressed ranges, corruption and incremental COW. Focused race tests
  passed. Ordinary AND complete race suites passed for `rootfsblock`,
  `rootfsartifact`, `rootfsimporter`, and `manager/pkg/rootfsimportworker`.
  The independent Nomad driver module's `go test ./...` passed separately.
  Privileged/environment-gated tests were not enabled; no local e2e ran.
- **Differential proof:** reuse the sealed D-FILE-RANGES publisher as a temporary
  test oracle only. With identical synthetic inputs and object prefixes, the
  new shared publisher produced byte-identical descriptors, all stored data/map
  objects, inventories and counters. The accepted cache/coalescing/COW/encryption
  cases also passed through the new entry point under race detection. This is
  not a repeat of real-image latency testing or proof of full-image costs.
- **Decision / remaining work:** retain the core implementation. Generic XFS
  traversal, hardlink/shared-extent policy, import/attestation provenance and
  exact artifact selection remain pending. No durable importer call site selects
  the new entry point. Complete-image cost and privileged validation must precede
  encrypted durable publication and the original cold/cached-new/high-density
  claim plus immediate `node -v` acceptance. Keep B-UTC8 and all earlier misses.
- **Scanner constraint audit for the next step:** kernel FIEMAP documentation
  permits extent ends beyond the requested interval and warns against raw
  extent-data reads while the filesystem is mounted. Collect bounded metadata,
  cleanly unmount, then stream immutable image bytes; unknown/encoded/unwritten
  extent data must not be treated as ordinary file payload. Do not synthesize
  zeros into raw image bytes from a file's unwritten flag. Use root-confined
  traversal with explicit regular-file and same-filesystem checks, not process-
  global chroot in an active-active manager. Go 1.25.5 `os.Root` documentation
  explicitly does not prohibit mount/device traversal by itself.
  Primary reference: https://docs.kernel.org/filesystems/fiemap.html.

### D-FILE-RANGES (2026-09-10, completed; mechanism retained for further validation)

- **Baseline / new mechanism:** retain B-UTC8 for performance. Reuse the four
  immutable D-XFS-REAL-VOLUME image layouts. That experiment rejected physical
  allocation hints, not file-relative publication boundaries. Linux v6.8
  `xfs_bmap_compute_alignments`, `xfs_bmap_btalloc_at_eof`, and
  `xfs_alloc_fix_len` distinguish logical extent-size hints from conditional
  physical stripe alignment; no new stripe/AG/hint sweep is admitted.
- **Hypothesis / single variable:** partition the same image bytes differently
  at import: only complete file-relative 64KiB units contained in one recorded
  allocated extent become preferred ranges. Keep partial ends, holes and other
  bytes in bounded residual ranges. Every initial mapping still references a
  complete raw or compressed payload with DataOffset=0, retaining the current
  format, checksum, coalescing and incremental-COW contracts. Do not change XFS,
  file bytes, the codec, runtime cache, concurrency, readiness or timeouts.
- **Bounded diagnostic:** generic range planner unit tests, then read-only
  reconstruction of the pinned Node/procd files from all four retained images.
  Compare canonical global-grid and preferred-range maps over the SAME union
  of target-containing 64KiB image windows; bytes outside these windows are
  intentionally absent from the diagnostic map. This is not a published RootFS
  or a full-image storage-cost estimate. Reuse production pack/map/reader/COW
  code via a temporary test overlay; never execute guest binaries.
- **Decision rule fixed before execution:** all reconstructed file hashes must
  match the prior evidence, and all roundtrip/coalescing/COW/corruption tests
  must pass. Candidate shared complete keys must cover >=99% of each target's
  allocated complete-file-unit bytes across both image-size pairs; aggregate
  range count and encoded data+mapping bytes in each identical window set must
  grow <=1%. Otherwise reject or mark the fixture invalid, preserving failures.
  Passing admits only a generic full-image importer/provenance/cost experiment,
  not adoption or a latency claim. No new target-specific production path.
- **Scope / cache / cleanup:** original 2vCPU/8GiB remote test node only; no
  claims, guest commands, OSS reads/writes, DB changes, runtime rollout or local
  e2e. Resolve the retained staging disk by serial/size/UUID and mount ext4
  read-only without journal recovery; never format or create another disk.
  New diagnostic files live in a unique root on the system disk. Verify frozen
  source, original files/processes/job/rows, idle writers/NBD before and after;
  do not reschedule a carrier merely for this offline test. Unmount the owned
  staging mount, close the exact terminal session and stop compute afterward.
- **Inputs / evidence:** previous analysis SHA-256
  `e1262a21c687ba98fdfd04457805207dfce67f4ac8dba017a0e913f19fd91eb5`;
  fixture inputs and source hashes are frozen before remote execution under
  `/tmp/sandbox0-file-ranges.fLsoby`. Node/procd bytes and sparse logical sizes
  retain the previous experiment's identities; no populated-1TiB, first-command
  working-set, cold latency or high-density claim is made by this diagnostic.
- **Result:** all four real-image subtests and both partitions per image passed.
  Raw image reconstruction and current Reader reconstruction matched each
  pinned Node/procd SHA; every selected image-window byte also matched. On BOTH
  XFS A/B layout pairs, Node shared complete decoded keys increased from 0 to
  1883/1883 and procd from 0 to 372/372. These counts cover 123,404,288 and
  24,379,392 bytes respectively; partial units/holes are not claimed as shared.
- **Measured local publication costs:** every arm increased 2263 -> 2269 data
  entries (+0.265%). Encoded data-plus-map bytes changed by +11,284 (a-node),
  +46,586 (b-node), -25,896 (b-coding), and +50,218 (a-coding); maximum increase
  0.0816%. Mapping bytes alone increased by 658, 805, 1205, and 1335 bytes.
  These are equal selected-window comparisons, NOT full-image cost, encrypted
  storage/wire bytes, unique startup demand, or actual cache-hit measurements.
- **Cost comparison caveat:** diagnostic object prefixes encode `false` versus
  `true` and differ by one byte. Reported mapping-byte costs include that
  identity-encoding difference; they are not a pure decomposition of planner
  overhead. Decoded data checksums and sharing counts exclude object prefixes.
  Use equal-length identities in the next full-image cost experiment.
- **Contract tests:** local race tests and remote tests passed for arbitrary
  4KiB physical starts, raw/compressed payloads, one coalesced bulk source read,
  cross-layout reuse of the existing decoded cache, 16KiB encryption frames,
  corruption rejection without exposing bytes, and three 4KiB writes spanning
  two data ranges. Compressed unchanged fragments required zero source-data
  reads; raw fragments read only the two touched ranges. Only dirty payloads
  were published and the base stayed unchanged. These focused tests are not a
  full privileged/durable checkpoint acceptance suite.
- **Decision:** retain the mechanism, not a runtime change. A generic bounded
  full-image planner, ambiguous/shared-extent handling, importer/attestation
  provenance and exact artifact selection remain unimplemented. Full-image
  storage/COW evaluation must precede durable encrypted publication and matched
  cold/cached-new regional claim + immediate `node -v` at actual width/density.
- **Evidence:** remote fixture SHA-256
  `59ac9f5c417bdf61b6851b84c99e2258be06bfdbd0c91a53fc7ca0657794dfd3`;
  analysis `/tmp/sandbox0-file-ranges.fLsoby/analysis.json`, SHA-256
  `63a7663c61d48f191b82d330cd3f223e9c44febba7f42472b3bd2c8a475fef5b`.
  All 12 exported members verified locally, including full raw key lists, with
  independently checked intersections and costs. Frozen source was unchanged.
- **Cleanup guard correction:** the first helper rejected its precondition
  before any unmount intent/execution because Ruby symbol keys were compared
  directly to JSON string keys. An independent read proved canonical states
  identical and no unmount attempt. Keep that failure and helper; the corrected
  helper normalizes keys and is separately recorded. No test was rerun.
- **Physical cleanup:** original 13 files, service processes, Nomad job index
  64101, and database row counts 2072/263 preserved; all 64 NBD devices idle.
  No XFS/loop/NBD attachment or carrier reschedule occurred. Read-only staging
  mount removed, diagnostic process absent, and frozen remote source inventory
  reverified after the test. Existing staging disk and old images remain intact.
- **Final environment:** owned terminal session terminated after planned SSH
  closure. Fresh cloud observation at 14:01:31 UTC verified original
  `ecs.g9i.large` 2vCPU/8GiB, `Stopped` / `StopCharging`. No production mutation,
  merge, tag, object-store publication or database mutation occurred.

### D-XFS-REAL-VOLUME (2026-09-10, completed; candidate rejected)

- **Admission / previous turn:** D-XFS-REAL is terminal with zero OCI/image
  attempts because the test host has only 14.06GiB free on its 128GiB system
  disk. This follow-up supplies space; it does not change the candidate or
  reinterpret that failed fixture as a performance result.
- **Independent staging:** provision exactly one tagged 64GiB pay-as-you-go
  ESSD PL0 disk in the existing test instance's zone, using a recorded unique
  client token. Resolve cloud disk identity to an exact new host device by
  serial and size; require no filesystem, partition, mount or old signature
  before formatting it. Use a new ext4 staging filesystem mounted only at
  `/data/sandbox0-xfs-staging-G3Q8sb`; never format/resize the original system
  disk or remove historical data. Do not change fstab or runtime data paths.
  Record disk ID, filesystem UUID, price and ownership for reuse by this active
  diagnosis; the disk is retained with evidence, its storage charge continues
  while compute is stopped. Initial read-only quote was CNY 0.026752/hour;
  recheck before provisioning. Original CPU/memory, runtime and job stay fixed.
- **Unchanged experiment:** the D-XFS-REAL pinned OCI/procd bytes, verified
  importer, canonical XFSBuilder, 16GiB Node / sparse 1TiB coding logical sizes,
  A-node/B-node/B-coding/A-coding order, 24GiB free guard and 20-minute bound
  remain unchanged. Only B inserts `mkfs.xfs -d su=64k,sw=1`. All four arms use
  the same new staging filesystem. No old arm completed, so there is no
  cross-volume performance comparison. No claim/command/startup result is
  inferred from offline preparation.
- **Unchanged gates:** all four whole-tree identities must match their exact
  verified source including file bytes, mode/owner, mtime, links and xattrs;
  Node/procd hashes remain fixed. B cross-image complete 64KiB Node-key overlap
  must be at least 99% and improve deficient A overlap. Unique small-file
  allocation increase must be at most 1% in each matched pair. Report procd
  sharing, sparse extents and local image allocation separately. A pass only
  admits isolated durable artifact publication and later exact-digest regional
  cold/cached-new claim plus immediate `node -v`, actual density and durability
  acceptance. Do not promote B-UTC8 or sweep stripe settings on failure.
- **Operation / cleanup:** new local root `/tmp/sandbox0-xfs-real-volume.G3Q8sb`,
  new remote trial root under the staging mount. One guarded provision/attach
  sequence and one exact trial unit, no replay on an observation timeout.
  Detach trial loop mounts, unmount the staging filesystem after evidence
  export, preserve the reusable disk and its recorded identity, close owned
  sessions and stop compute. No production changes, local e2e, runtime source
  edits, public timeout change, OSS publication or sandbox/import DB mutations.
- **Result:** all four real-image builds passed `xfs_repair -n`, full-tree
  identity comparison, fixed Node/procd hashes, and detached inspection mounts.
  Both source trees also matched their post-pair rescan. Node has 7,293 entries
  and 5,773 regular-file paths; coding has 150,762 entries and 132,930 regular-file
  paths. Coding contains 4,489,374,053 unique regular-file bytes, not 1TiB of
  populated data. The exact trial unit exited successfully; zero claims, guest
  commands or startup latency samples were issued.
- **Predeclared sharing gate failed:** A's Node complete-range sets contained
  1,882 / 1,882 keys and shared zero. B had 1,883 / 1,882 keys and also shared
  zero (0%, below 99%). Procd likewise shared zero for A (372 / 371 keys) and
  B (371 / 371 keys). These are verified SHA-256 identities of complete 64KiB
  logical-image ranges bound to actual file bytes by FIEMAP, not runtime cache
  hit measurements or a startup working-set trace.
- **Concrete counterexample:** B-node's complete Node ranges start at
  file-relative residue 32KiB modulo 64KiB; B-coding's residues are 28KiB and
  20KiB. Procd residues are 40KiB versus 36KiB. Thus the stripe setting did not
  establish common file-relative boundaries. Node retained its exact
  124,836,408-byte SHA and 1,183,744 sparse-hole bytes; procd retained its exact
  24,463,545-byte SHA and 4,096 sparse-hole bytes across all four images.
- **Cost / actual geometry:** small-file allocation stayed 49,545,216 bytes
  for each Node arm (5,532 unique small files), and 1,027,059,712 bytes for each
  coding arm (128,915 unique small files). B's local backing-image allocation
  increased by 630,784 bytes for Node and 1,130,496 bytes for coding. This is
  local sparse-image allocation, not OSS storage cost. `mkfs.xfs` did report
  `sunit=16, swidth=16` filesystem blocks for B, so the option was applied.
  Its derived AG count also changed: A=4 for both sizes, B=16 for Node and 32
  for coding. The one changed command-line input therefore bundles these
  derived layout effects; it is not proof of an isolated stripe-only effect.
- **Decision:** reject this allocation-policy candidate for the requested
  sharing mechanism. Preserve the earlier synthetic pass and this real-file
  counterexample without relabeling either as startup acceptance. No durable
  candidate artifact is published and no expensive cold/occupied startup trial
  is admitted for this rejected variant. Future work must establish explicit
  file-relative chunk/alignment behavior, including sparse files and small-file
  or incremental-COW cost, before returning to full regional claim + `node -v`.
- **Reusable staging resource:** disk `d-t4n1qmzpkzn19uxs6o0p`, name
  `sandbox0-cold-start-staging-G3Q8sb`, 64GiB ESSD PL0, filesystem UUID
  `59c7f761-4479-46df-b2ef-8ea7d7bce30b`. During this boot it resolved by serial
  to `/dev/nvme1n1`; never assume that device name on a later boot. It remains
  attached to the test instance but the staging filesystem is unmounted after
  export; no fstab change. Reuse this exact disk rather than creating another.
  Preserved public OCI trees and generated images live under its `trial/`
  directory. The recorded quote is CNY 0.026752/hour while retained, including
  when compute is stopped. No old data or system-disk contents were removed.
- **Restoration:** original CPU/memory, 13 files, service processes, job index,
  and database sandbox counts 2072/263 stayed fixed during the trial. All owned
  loop mounts and staging mount were detached; all 64 NBD devices stayed idle.
  A single exact-head ForceReschedule evaluation handled the known original
  carrier registration conflict before measurement. The new disk and session/
  final stopped-compute receipts are recorded separately from performance data.

### D-XFS-REAL (2026-09-10, preparation incomplete)

- **Admission:** D-XFS-STRIPE passed synthetic layout sharing and small-file
  allocation gates. The follow-up source audit confirms an explicit new object
  prefix produces a distinct durable import operation, but source-OCI ready
  lookup selects the newest matching artifact rather than an allocation policy.
  Future durable/performance A/B must use exact result artifact digests and
  isolate candidate publication from existing template source selection.
- **One variable / hypothesis:** use the unchanged canonical XFSBuilder with
  either default mkfs arguments (A) or only `-d su=64k,sw=1` (B). Does the
  synthetic sharing gain survive actual pinned Node22/coding source trees,
  without changing file content/metadata or increasing small-file allocation?
- **Fixed inputs:** the B-UTC8 Node22 and coding OCI digests, linux/amd64,
  UTC procd SHA `29dedac3bce92b6a9a3d507889110927ede8727f87186376524e577617c69f2d`,
  and 16GiB / 1TiB logical sizes. These are sparse logical image sizes, not
  fully populated capacity. Unpack each OCI through the canonical verified
  importer once, then use that exact source tree for its A/B pair. Capture
  full-tree content/mode/owner/link/xattr identities before and after copying,
  plus Node/procd hashes, sparse layout, complete 64KiB file-bound range keys,
  and allocated bytes. Natural filesystem UUID/time/layout variation is retained
  and is not a second software variable; no import-time or startup speed claim.
- **Predeclared plan / decision:** build in A-node/B-node/B-coding/A-coding
  order, one image per arm. Candidate cross-image Node complete-range key
  overlap must reach at least 99% and improve a deficient A overlap. All four
  trees must match their own verified source identity; Node must match the
  previously verified SHA and procd its pinned SHA. Aggregate allocated bytes
  for unique regular files of at most 64KiB must increase by no more than 1%
  within either matched pair. Otherwise reject or classify inconclusive; do not
  sweep neighboring stripe sizes. A pass admits durable artifact construction,
  not adoption or startup acceptance. Procd sharing and total image allocation
  are separately reported, not substituted for the Node gate.
- **Scope / safety:** original 2CPU/8GiB test node, private mount namespace,
  exclusive owned loop images under `/data/sandbox0-xfs-real-K4mah0`, at least
  24GiB free staging space. No NBD attachment, target encrypted artifact read,
  OSS writes, import/sandbox database changes, runtime/default changes or guest
  command execution. Allow one bounded preparation run (20 minutes) with no
  automatic replay; this is not a change to public HTTP or importer timeouts.
  Retain generated images as evidence, detach owned mounts/loops, preserve
  original services/files/job/data, export evidence and stop compute. Any later
  startup test needs fresh target-node caches, regional ingress, immediate
  `node -v`, cached-new identities and the real density/policy gates.
- **Evidence root:** `/tmp/sandbox0-xfs-real.K4mah0`.
- **Observed result:** the single exact systemd run terminated before either
  OCI unpack or any image build: `insufficient staging space`. A fresh `df`
  showed 15,094,738,944 bytes available (about 14.06GiB), below the predeclared
  24GiB guard. The instance has only its 128GiB system disk; host block/mount
  enumeration and Aliyun `DescribeDisks` found no separate attached data disk.
  Zero of four images attempted, zero claims, commands or latency samples.
  This is an invalid preparation fixture, not a failed alignment/performance
  result; D-XFS-STRIPE remains a candidate, not a promoted baseline.
- **Completed preparation work:** two local diagnostic unit tests passed;
  the unchanged frozen 1301-file source built the trusted diagnostic for remote
  amd64 successfully, binary SHA
  `fb427469ef08922ad90674aa9a03615a087508a4fb8eb9d40ac6ff2f4f6e2498`.
  Full-tree fingerprint and real-image A/B code is prepared, but its actual
  file-tree/geometry checks have not run and are not counted as passing.
- **Preparation failures retained:** initial verified member extraction
  completed, then a read-only Nomad query timed out before stage receipts.
  Members were independently re-hashed and missing receipts recorded; no
  extraction replay. A rebooted original warm carrier then exhibited the known
  allocation registration conflict. One exact-head guarded ForceReschedule
  evaluation restored ready=2 without changing job spec or service processes.
- **Cleanup / next action:** original 13 files, service processes, job index
  and database sandbox row counts 2072/263 remained unchanged; no owned loop
  attachment or NBD use, no OCI target or encrypted artifact access, and no
  imported/published RootFS. The run is authoritatively terminal. No existing
  data was removed, no disk was created/resized, and the space guard was not
  lowered. Before a new uniquely identified attempt, supply independently
  identified staging storage with enough free space; preserve this failure and
  retain the same four-arm plan and gates. A read-only quote for a 64GiB ESSD
  PL0 staging disk is recorded as preparation information, not a provisioned
  resource or a change to runtime storage. Final session/cloud receipts verify
  shutdown separately from the execution analysis.

### D-XFS-STRIPE (2026-09-10, completed)

- **Baseline / new mechanism:** D-NODE-GEOMETRY proves an 8KiB alignment
  difference and disjoint keys for identical current Node files. The current
  `XFSBuilder` uses default allocation geometry followed by sparse-preserving
  `cp -a`. No historical stripe-alignment experiment was located.
- **One variable:** a diagnostic CommandRunner adds `-d su=64k,sw=1` only to
  new-image `mkfs.xfs`; the product builder, copy, mount and repair sequence is
  otherwise unchanged. This is not a filesystem block-size change or an
  inherited per-file extent-size hint. No runtime/format/default change.
- **Question:** can this allocation policy produce stable 64KiB file-relative
  block identities across two different surrounding layouts without increasing
  small-file allocations? Upstream documentation/source describes conditional
  stripe alignment, not an unconditional whole-file guarantee; measure the
  actual remote kernel/tools and do not infer results from that documentation.
- **Plan:** four isolated 1GiB images in A0/B0/B1/A1 order; A is the current
  builder and B adds the stripe option, 0/1 are fixed different surrounding
  layouts containing byte-identical deterministic targets; neighboring payload
  prefixes are fixed at 24KiB / 32KiB before execution. Include a dense
  multi-MiB file, a 124,836,408-byte synthetic file with the observed Node sparse
  holes, threshold-size files and many small files. These are generated fixtures,
  not either tenant artifact or executable Node.
- **Predeclared decision:** candidate full-range identity overlap across layouts
  must reach at least 99% for both multi-MiB targets, improve a deficient baseline,
  preserve all file hashes/holes/modes/links and copy-on-write clone bytes, and
  increase aggregate small-file allocated bytes by no more than 1% in either
  matched layout. Otherwise reject or mark unproven, retaining every result.
  A pass admits further real-artifact testing, not adoption or startup success.
- **Safety / scope:** original 2CPU/8GiB test shape, separate private mount
  namespace, exclusive new image paths, loop mounts only; the fixture issues no
  NBD/OSS or sandbox/import database mutations,
  no target RootFS access, claims or guest commands. Require at least 8GiB free
  staging space; cap the fixture run at ten minutes. Preserve failed receipts
  and any images retained by the existing builder's cleanup contract;
  remove owned mounts/loop attachments, verify original services/files/job/data,
  export evidence and stop compute. No local e2e. Future cold acceptance requires
  a fresh boot, regional ingress, actual width and immediate `node -v`.
- **Contract audit:** fixed data-range geometry participates in durable import
  identity and provenance. Changing allocation policy does not require changing
  authenticated block geometry, but shipping it still needs explicit import
  policy/cache identity handling so existing artifacts are not silently relabeled.
  The fixture's filesystem reflink test is not a block-COW checkpoint/reattach
  acceptance test. Root: `/tmp/sandbox0-xfs-alignment.7vYugr`.
- **Result:** all four images built, passed the builder's `xfs_repair -n`,
  preserved 264 target file hashes/modes per image, links, exact sparse-hole
  counts and isolated reflink-clone 4KiB-write checks. Candidate images passed
  the predeclared overlap and small-file allocation gates. Zero claims, guest
  commands or startup latency samples; no target artifact was accessed.
- **Measured full-range sharing:** the dense 4MiB target shared 0 of 63/64
  complete ranges between A0/A1, versus 64/64 between B0/B1. The synthetic
  Node-shaped target shared 0/1883 versus 1884/1884. These are SHA-256 identities
  of complete 64KiB logical-image ranges, with each range checked against the
  file bytes via FIEMAP; not measured runtime cache hits or published objects.
- **Cost:** each arm's 256 small files occupied 5,242,880 bytes. All six
  threshold files also retained the same allocated bytes. The synthetic
  Node-shaped file retained 1,183,744 sparse-hole bytes. Candidate local sparse
  image allocation increased by 237,568 bytes (232KiB) in each matched layout;
  this is backing-image allocation after the clone check, not OSS storage cost.
- **Important limit:** B0/B1's sparse target had full-range file-relative
  offsets at both 0 and 32KiB modulo 64KiB. Equal keys in these layouts do not
  prove universal file-relative alignment or arbitrary fragmentation behavior.
- **Exact environment:** Linux `6.8.0-124-generic`, xfsprogs `6.6.0`, GNU cp
  `9.4`, original 2CPU/8GiB. Canonical XFSBuilder source SHA
  `93a875565550932eccc46fe25317ced59e5660b2b5f365e86316d2116b817c24`;
  only the diagnostic CommandRunner inserted the stripe arguments. Test binary
  SHA `b8429f7f988e83e337ae7362de9fd8f34080b1bca36f41f853cda7eac5dc4c47`.
- **Preparation / cleanup:** a rebooted original carrier failed with the known
  allocation registration conflict. One guarded ForceReschedule evaluation
  restored two ready carriers; no job-spec mutation or service restart. Original
  service processes, 13 files, job and database row counts (2072 / 263) were
  preserved. All owned loop mounts detached and all 64 NBD devices stayed idle.
  Generated fixture images are retained under the isolated remote root; no
  existing `/data` contents were deleted. Final session/cloud receipts are sealed
  separately from the analysis.
- **Decision:** retain the diagnostic candidate, not a new performance baseline
  or default. Next prepare explicitly identified real-artifact variants with
  fixed executable/dependency/procd bytes; verify storage geometry, space/read
  amplification and applicable durability before matched regional claim plus
  immediate `node -v` tests. Preserve both cold and cached-new cohorts and real
  density gates. No source/default, timeout, production, merge or tag change.
- **Evidence:** `/tmp/sandbox0-xfs-alignment.7vYugr/analysis.json`, SHA-256
  `9e195a3fb68648b903129adb54da9e3f33ca4d5955b91133c63985acf4318825`.
- **Design references, not measurement substitutes:** upstream
  [mkfs.xfs manual source](https://kernel.googlesource.com/pub/scm/linux/kernel/git/dgc/xfsprogs-dev/+/refs/tags/v5.7.0-rc1/man/man8/mkfs.xfs.8)
  and [XFS allocation source](https://kernel.googlesource.com/pub/scm/linux/kernel/git/torvalds/linux/+/5b10ff013e8a57f8845615ac2cc37edf7f6eef05/fs/xfs/xfs_iomap.c).

### D-NODE-GEOMETRY (2026-09-10, completed)

- **New evidence / question:** D-NODE-IDENTITY proves identical current Node
  bytes. The current reader already uses checksum/decoded-length cache keys;
  the importer splits by logical-device 64KiB boundaries, not file boundaries.
  Test whether different XFS file alignment prevents these identical executables
  from sharing decoded range identities. This is not another cache-size sweep.
- **Related work:** the old file-layout analysis used a different descriptor;
  P-UTC compared resident-versus-target keys, not target-versus-target Node
  extents. Neither establishes this current executable's range-key overlap.
- **Plan / inputs:** inspect only the two exact B-UTC8 immutable descriptors,
  using isolated read-only/no-recovery NBD31 mounts and a trusted chrooted file
  layout scanner. Read verified mapping entries for Node extents, not Node file
  contents or arbitrary guest executables. Keep the original 2CPU/8GiB shape,
  runtime binaries/config/job and databases. No claim or latency samples.
- **Decision rule:** compare full 64KiB ranges wholly within each file, their
  file-relative offsets and checksum/length identities. Report boundary ranges
  separately. Low overlap plus differing file-relative boundaries establishes
  a limitation of current cross-image block sharing, not its latency cost or
  proof that a new chunking design would meet the cold-start gate. If boundaries
  and keys match, reject this hypothesis; do not change cache policy either way.
- **Safety:** reuse authenticated readers; verify parent bindings, cap layouts
  at 1024 extents and mapped entries at 8192, retain 128MiB diagnostic cache and
  3-minute/512MiB requested-ciphertext-range limits per artifact. Never use the
  ctld cache or execute guest bytes. SDK retry bytes remain outside that limit's
  measurement scope. Export failures and exact handles; remove owned mounts/NBD,
  verify original state, close owned sessions and stop compute. Fresh boot is
  required before any subsequent cold-start acceptance.
- **Initial diagnostic failure retained:** the first Node scan rejected a valid
  sparse file because the new validator incorrectly required contiguous file
  extents. The coding-agent scan was not attempted in that run; 649,609
  ciphertext bytes were returned and physical cleanup passed. This is a tool
  assumption failure, not a runtime/latency regression.
- **Correction based on evidence:** a separate raw FIEMAP scan proved four
  supported extents and identical 1,183,744-byte sparse holes in both files.
  Only then did a mapping-only follow-up accept nonoverlapping sparse gaps.
  It reused the exact hashed raw reports, excluded holes from data accounting,
  and rejected all data-pack source keys. Original and corrected diagnostic
  unit tests passed; no product reader/importer/cache implementation changed.
- **Finding:** each current Node has 1,883 full 64KiB ranges, representing
  123,404,288 decoded bytes, plus eight boundary fragments. The two full-range
  key sets have **zero intersection**; including boundary ranges still gives
  zero shared keys. File starts are at device offsets 24KiB versus 32KiB modulo
  64KiB; full-range file-relative starts are 40KiB versus 32KiB modulo 64KiB.
  Thus byte-identical Node files are split at different file-relative boundaries
  and cannot share these decoded blocks through the current content-key cache.
- **Scope / decision:** diagnostic only; confirmed cross-image sharing
  limitation, not measured startup demand, eviction, OSS wire bytes or latency
  attribution. The union of full-file ranges must not be called the command
  working set. Keep B-UTC8, singleflight and current runtime defaults. Zero
  claims/guest commands/latency samples; cold and high-density gates remain open.
- **Read scope:** raw layout scans returned 633,205 / 810,293 ciphertext bytes
  in 21 / 32 wrapper calls. The mapping-only follow-up returned 133,079 /
  138,885 bytes in four calls each. Layout mounting can read surrounding
  filesystem metadata/data blocks; it did not execute or explicitly hash Node.
  Mapping-only source reads were restricted to mapping objects.
- **Preparation recovery:** initial readiness checks failed during boot; after
  convergence, one old unclaimed carrier remained failed with allocation
  registration conflict. One guarded ForceReschedule evaluation restored two
  ready carriers. No job-spec mutation, service restart, claim or DB editing.
- **Cleanup verified before export:** original service processes, 13 files,
  job specification and row counts (2072 formal / 263 original) preserved;
  both private mounts removed and all 64 NBD devices detached. No rejected
  object/block mutations in successful scans. Cloud/session closure is indexed
  in the final sealed evidence, separately from this analysis.
- **Evidence:** `/tmp/sandbox0-node-geometry.nds5a4/analysis.json`, SHA-256
  `d2329658fdad71e8f7bd083d4f3cea568616bbf448ea3b533f4d9d3a9ee416e8`.
  Failed, raw-layout and mapping-follow-up archives are retained separately.
- **Next admissible work:** evaluate a generic import-time alignment/chunking
  design against incremental COW, authenticated geometry and small-file space
  costs before testing a candidate. Freeze executable/dependency bytes and
  compare both cold and cached-new claim plus immediate command. Cross-image
  deduplication alone cannot establish unique-RootFS stateless-cold performance.

### R-RUNSC-CROSSOVER (2026-09-10, completed)

- **Baseline / related work:** B-UTC8 and R-RUNSC17 above. The 154-record history
  contains a pin-parity run, but no Aug-10/Aug-17 runtime crossover. The new
  evidence is the same-width command-tail deterioration after pin alignment.
- **Hypothesis:** the Aug-17 runsc version reproducibly increases coding-agent
  first-command latency under the unchanged eight-way workload.
- **Independent variable:** stock runsc Aug 10 (A, SHA `670bcd3cbc103f00d8bb5098edc370f32397ee4c134231436bafa659bb3c068e`)
  versus Aug 17 (B, SHA `048b89aada69dc3333422e139d6e9d02f8ab06bda52398060e0fbdacca00074c`).
  The matching runtime compatibility catalog changes only `runsc_version`;
  the shared protocol must verify each registered digest before claims.
- **Fixed inputs:** B-UTC8's manager/ctld/driver/procd hashes, both artifact
  digests, 16vCPU/64GiB shape, 8 standard carriers, 1750m/1792MiB leases,
  128MiB decoded cache, source admission 8, 1MiB coalescing, 128KiB NBD cap,
  4096KiB observed readahead, and 10s timeout. No runtime instrumentation,
  production changes, cache tuning, retry, or target-RootFS prewarm.
- **Plan:** one boot, A1/B1/B2/A2 order; identical service/cache reset before
  every arm (including B1 -> B2); two batches of eight new claims per arm,
  each followed immediately by `node -v`; 64 target claims total. Keep all
  failed/unattempted samples. Report each arm and template separately.
- **Cache labels / limits:** only A1's first cohort can establish fresh-node
  cold scope. Later first cohorts reset ctld process caches but may retain
  host/provider caches. Every arm's second cohort is cached-node NEW identity.
  Provider cache and nonlinear time variation remain uncontrolled. This is
  a version-association diagnostic, not independent cold-node or density proof.
- **Decision rule fixed before execution:** compare coding-agent command-only
  medians and maxima in the second cohort of each arm. A reproducible slowdown
  requires both B arms to exceed both A arms by at least 10% and 100ms on both
  statistics, with no functional/config/preflight failure. Otherwise label
  the version-regression hypothesis unconfirmed/inconclusive; do not keep
  repeating the same crossover without new evidence. Even a positive result
  remains version-associated, not proof of an isolated kernel/OSS mechanism.
- **Safety / restore:** reject changed prerequisites before claims; preserve
  every issued operation's intent and live handle; never replay on observation
  timeout. Require physical lease/writer/NBD cleanup between arms. Restore the
  original 13 files, original two-carrier job, 2vCPU/8GiB shape and stopped compute
  at the end. If a prerequisite fails, retain the partial evidence and do not
  silently replace the failed arm. Artifact root: `/tmp/sandbox0-runsc-crossover.lpEm3B`.
- **Interruption / preparation observations:** B2's first readiness observation
  found the just-submitted allocations pending; all eight became ready without
  another submission or service restart. Its compatibility proof completed at
  09:04:56 UTC, but the SSH response was lost and the provider session terminated
  at 09:05:43. Observation resumed at 09:55:41. Read-only recovery proved the same
  boot/configuration, completed compatibility proof, physical absence and row
  count 2040; no B2 claim had been issued. The proof was not replayed. This
  approximately 50-minute interruption is an additional time/order confound;
  retain it explicitly, not as a clean uninterrupted crossover.
- **Status:** completed. 64/64 new claims and 64/64 immediate `node -v` commands succeeded; all four arms passed physical cleanup. No runtime source change, timeout increase, target prewarm, or baseline promotion.
- **Decision:** Inconclusive for the version-regression hypothesis: the predefined rule did not pass. Do not repeat the unchanged crossover without new evidence.
- **Boundary:** only A1 first cohort is fresh-node cold. Later first cohorts reset processes only. No meaningful resident-memory reclaim, fully populated 1TiB image, or production-hardware/policy acceptance was tested.
- **Evidence:** `/tmp/sandbox0-runsc-crossover.lpEm3B/remote-analysis.json`, SHA-256 `7a31724131251ae0902e2c9fe372f874d46f6e8915cdcc37873580464cc33c8a`. This immutable analysis is indexed separately from the later restoration/cloud/evidence seal, avoiding a circular log hash.
- **Baseline recovery:** the initial two-carrier readiness prerequisite failed after boot. Read-only evidence isolated one registration-conflict allocation; one guarded reschedule restored readiness before staging or claims. The failed check and recovery receipts are retained. No claim sample was replaced.
- **Restoration observation:** a Nomad job GET timed out before the restore submission intent existed. A separate read proved the job still stopped and no submission intent/result; the original two-carrier job was then submitted once and verified ready. Public claim/command requests were never retried and their 10s budget was unchanged.
- **Final environment:** original 13 files and two ready carriers verified; all 64 NBD devices detached; original database history preserved. The first cloud downsizing request returned no success; its failure was retained and the same request was resolved with its original documented idempotency token. A fresh cloud read at 10:14:11 UTC verified the original 2vCPU/8GiB shape, `Stopped` and `StopCharging`. Both owned terminal sessions ended. No production mutation, merge or tag.

Observed maxima in seconds; column maxima need not belong to the same sample:

| Arm | Cache scope | n | Claim | Node command only | Claim + Node | Combined >2s |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| A1 | First node-cold | 8 | 1.206061 | 1.454058 | 2.646841 | 4/8 |
| A1 | Cached-new | 8 | 0.736030 | 1.425297 | 2.147768 | 4/8 |
| B1 | Process-cache reset only | 8 | 0.982005 | 1.514128 | 2.478323 | 4/8 |
| B1 | Cached-new | 8 | 0.675859 | 1.035343 | 1.701810 | 0/8 |
| B2 | Process-cache reset only | 8 | 1.084825 | 1.501518 | 2.584043 | 8/8 |
| B2 | Cached-new | 8 | 0.742659 | 1.352039 | 2.092247 | 4/8 |
| A2 | Process-cache reset only | 8 | 1.280776 | 1.611584 | 2.863002 | 4/8 |
| A2 | Cached-new | 8 | 0.797487 | 1.174443 | 1.969561 | 0/8 |

Predeclared primary comparison: coding-agent cached-new command only, milliseconds:

| Arm | n | p50 | Max |
| --- | ---: | ---: | ---: |
| A1 | 4 | 1420.854 | 1425.297 |
| B1 | 4 | 1026.972 | 1035.343 |
| B2 | 4 | 1349.646 | 1352.039 |
| A2 | 4 | 1172.859 | 1174.443 |

All eight claims overlapped in every cohort, with exact active lease identities and budgets. All 64 post-command lease snapshots had zero OOM/OOM-kill counters. Externally bracketed NBD reads were 731.362–731.506MiB per eight-claim-plus-command cohort; these are not OSS bytes, command-only bytes or unique application demand. Per-arm sampled combined ctld RSS peaks ranged 789.3–918.6MiB, not full-node memory or a resident/reclaim-pressure proof.

Both B arms must exceed both A arms by at least 10% and 100ms on both statistics. Version-regression hypothesis unconfirmed by the predefined rule; do not rerun the unchanged crossover without new evidence.
Keep B-UTC8 as the historical reference, not a promise that its observed tail reproduces on every boot. Preserve all cold/occupied misses. The next admissible work must fill a specific missing current-artifact identity or demand-read mechanism; do not repeat the old layout/fault/readahead chain.

### D-NODE-IDENTITY (2026-09-10, completed)

- **Related work / new gap:** R-RUNSC-CROSSOVER did not reproduce the proposed
  stable version slowdown. Reuse the old `37de2f` artifact's layout/fault/RA
  analysis; it did not establish the current `f19e6d` versus `9d11df` executable
  and library identity. Their attestations name different source OCI digests,
  despite identical `node -v` stdout. No repeated latency sweep is admitted.
- **Question / decision:** hash the current immutable Node executables, ELF
  interpreter and dependency candidates, resolving in-image symlinks. Different
  bytes disprove the assumption of identical command payloads; matching bytes
  remove that difference but do not establish equal filesystem layout or I/O.
  Static dependency candidates are not a trace of actual loader choices.
- **Inputs:** the exact two B-UTC8/R-RUNSC-CROSSOVER artifact/descriptor/procd
  digests, encrypted regional test OSS and read-only formal PostgreSQL metadata.
  Reuse existing reader/encryption APIs and verify the frozen remote source.
- **Scope:** one bounded read-only inspection per artifact, on the original
  2vCPU/8GiB test shape. Zero claim/command samples. No runtime rollout, service
  configuration change, public timeout change, object/DB mutation or production
  action. A private read-only NBD/mount rejects writes and journal recovery;
  trusted diagnostic code hashes/parses files without executing guest binaries.
- **Cache / resource bounds:** separate diagnostic reader/cache, not ctld's
  shared cache; at most 512MiB provider reads and 3 minutes per artifact, 128MiB
  decoded cache and fixed system-file allowlist. This intentionally reads target
  bytes for diagnosis and is never cold-start acceptance; a future latency test
  needs a fresh boot and explicit cache proof.
- **Cleanup:** verify idle leases/writers and free exact NBD before attaching;
  detach only the owned mount/device, preserve original files/job/data, export
  evidence and stop compute. Preserve failures and live handles, never replay an
  uncertain operation. Root: `/tmp/sandbox0-node-identity.VxqyyU`.
- **Status:** completed. Both exact immutable artifacts inspected successfully; local file/ELF unit tests and all 30 exported member checksums verified. No claim or guest command was executed, and no latency improvement is claimed.
- **Finding:** Node is exactly the same 124,836,408-byte ELF in both artifacts: SHA-256 `3517c2df0b2f8cd7f422b4b8450ef81c6889f08eb03e281d6de9079b15e6a327`. The small image has `/usr/local/bin/node`; coding-agent has `/usr/bin/node`. PT_LOAD segments, interpreter name and DT_NEEDED list match.
- **Dependency distinction:** all seven standard-directory dependency candidates have different bytes. Each name resolved to one canonical candidate per image, with absolute symlinks confined inside the image. This is a static scan, not a trace proving actual loader selection or startup demand. No claim that library differences caused the measured delay.
- **Decision:** diagnostic only. Eliminate Node executable/build differences for these paths. Keep B-UTC8; do not change runsc, caches, timeouts or libraries from this result. A size-only comparison must also freeze dependency identities; consult the existing size/layout history before admitting a new experiment.
- **Inspection I/O:** isolated readers returned 56,477,647 and 58,962,093 ciphertext bytes in 176 and 201 wrapper range calls for Node22 and coding-agent respectively, within the planned bounds. These counts include full-file hashing and metadata, not first-command reads or necessarily HTTP attempt counts. They do not prove cold-start performance.
- **Budget measurement scope:** the 512MiB limit is enforced on requested ciphertext ranges at the diagnostic wrapper boundary. SDK retry/transport bytes were not separately metered; do not describe that limit as a verified wire-byte total.
- **Preparation recovery:** one original unclaimed carrier failed registration after reboot (`runtime_slots_cluster_id_allocation_id_key`). One guarded ForceReschedule evaluation was issued only after proving exactly one failed head and one healthy head; two ready carriers were subsequently observed. No job-spec change, service restart or RootFS access was needed for recovery.
- **Cleanup:** both private read-only/no-recovery mounts and the owned NBD attachment removed; rejected write-operation counters zero; all 64 NBD devices detached. Original service processes/files/job specification and database row counts (2072 formal, 263 original) preserved. Owned terminal session ended; original 2vCPU/8GiB instance verified `Stopped` / `StopCharging`.
- **Evidence:** `/tmp/sandbox0-node-identity.VxqyyU/analysis.json`, SHA-256 `e1c36b2154f6838cec6ee46a769bc1d97cb8be5d5ecb13e565f9eef288be8b02`. Full hashes and raw scan reports are in the immutable analysis/archive; abbreviated dependency hashes below are for readability.

| Dependency | Node22 SHA-256 prefix | coding-agent SHA-256 prefix | Bytes identity |
| --- | --- | --- | --- |
| ld-linux-x86-64.so.2 | 02bcda52c1a5 | 1cd555ac46b7 | Different |
| libc.so.6 | 6b4a45352fd0 | d8db8739a163 | Different |
| libdl.so.2 | 95d521b2c39f | 850b46fd4f44 | Different |
| libgcc_s.so.1 | 2bd1552c4779 | d93224d2b0da | Different |
| libm.so.6 | 7f2ca87f652f | 1b87a1a50b49 | Different |
| libpthread.so.0 | b93a680da8a0 | 92dcea07a5b1 | Different |
| libstdc++.so.6 | e7848e32af49 | 1fd75fe70354 | Different |

[history.jsonl](experiments/history.jsonl) retains the searchable snapshot of all
154 historical status records discovered in the prior aggregate manifest, followed
by individually indexed new experiments.
[history-source.json](experiments/history-source.json) records its hash and
verification scope. This index does not mean every old test was re-run or that
all claims in its recorded statuses were re-proven. Entries without located
evidence or an expected checksum are explicitly marked, not silently trusted.

Append future plans/results and decisions here; append their immutable evidence
locator/status to the history index. Record when old evidence is superseded and
why, without changing the old raw result. Keep tokens, private keys, signed
transport URLs and full private configuration out of this repository.

### I-XFS-SCAN (2026-09-10, implementation and privileged verification completed)

- Implement bounded, root-confined, read-only XFS FIEMAP discovery for the
  canonical `DataRangeLayout` builder added in I-RANGE-LAYOUT. Keep the existing
  XFS builder and importer defaults unchanged; do not activate publication policy.
- Deduplicate hardlinks; exclude flagged/shared/unwritten extents from preferred
  segmentation without dropping original image bytes. Reject malformed geometry,
  unexpected overlap, device crossings and limit exhaustion. No filename rules.
- Validate metadata only while XFS is read-only; unmount and verify before raw
  image reads. Unit tests cover bounds, traversal, pagination and cleanup. A
  separate opt-in privileged test must exercise real XFS and full-image readback
  through the canonical publisher, on the original 2CPU/8GiB remote test shape.
- No claims, guest commands, runtime replacement, DB/object-store writes, timeout
  changes or production mutations. This is correctness validation, not startup
  acceptance or production-width evidence. Preserve failures and stop compute.
- Evidence root: `/tmp/sandbox0-xfs-scanner.08WD1o`.
- **Implementation:** optional `XFSBuilder.BuildWithDataRanges` remounts the
  exclusively owned image read-only, collects bounded metadata, then cleanly
  unmounts and runs `xfs_repair -n` before returning a plan. The existing `Build`
  command sequence and all importer call sites remain unchanged. No deployment
  configuration, artifact format or public timeout changed.
- **Local verification:** complete rootfsartifact, rootfsblock, rootfsimporter
  and rootfsimportworker unit suites passed; rootfsartifact race suite passed;
  Linux/amd64 test binary and Darwin/arm64 package compiled. No local e2e.
- **Preserved failure:** ordinary local fixture directories report device 30,
  regular files device 31. Initial traversal tests therefore correctly rejected
  a device crossing. Independent `stat` confirmed the discrepancy. Running the
  test process with `TMPDIR=/dev/shm` provided same-filesystem fixtures; all tests
  passed without relaxing the production guard. Both failed attempts are kept.
- **Remote verification:** all 16 top-level rootfsartifact tests passed on the
  original 2CPU/8GiB test node, including both opt-in privileged XFS tests. The new
  test verified writable-XFS rejection, actual read-only enforcement, hardlink
  deduplication, reflink/shared and unwritten extent exclusion, clean unmount and
  full 314,572,800-byte image equality through the canonical block builder/reader.
  Eight entries yielded seven regular paths, five unique scanned files, one
  skipped hardlink, six extents, three flagged extents (two shared), three
  preferred spans and 393,216 preferred bytes. The test transport retained 32
  published objects / 811,593 bytes in memory; these are not real-image cost or
  encrypted OSS/startup measurements.
- **Remote state:** original service processes/files/configs, Nomad job modify
  index 64101, formal/original sandbox row counts 2072/263, empty physical leases
  and 64 idle NBD devices remained unchanged. Private test image/mounts were
  cleaned; retained real-image staging disk stayed unmounted. One 10,411,489-byte
  test-binary upload was slow through the CLI tunnel; no upload/test replay. This
  transport wait is excluded from all startup interpretation.
- **Decision / missing gates:** retain the optional scanner, not an enabled
  import policy. Resolve scanner budgets before activation: its 1,048,576-entry
  cap is below OCI import's default 10,000,000 `MaxFiles`. Require explicit
  operation/attestation policy provenance, exact ready-artifact selection and
  full real-image publication cost validation, then fresh-node claim plus first
  `node -v`, cached-new identities and occupied production-width/reclaim tests.
  No new latency samples were taken; B-UTC8 and all prior misses remain intact.
- **Final cleanup:** owned CLI terminal observed `Terminated`; the original
  stop request completed and a fresh cloud read at 15:01:41 UTC confirmed
  `Stopped` / `StopCharging`, still 2CPU/8GiB. The earlier `stopped.cloud.json`
  filename is only an observation label: its body correctly records `Stopping`;
  `stopped-final.cloud.json` is the terminal cloud proof. No stop request replay.
- **Evidence:** `/tmp/sandbox0-xfs-scanner.08WD1o/evidence.json`, SHA-256
  `c27cb7040b7cf643ac1d7f2c4216e80180cc26bad96e9b0fe5aa1cdbe462fc37`.

### I-IMPORT-LAYOUT (2026-09-10 UTC, implementation and full-image cost verified)

- Connect generic file-relative segmentation to an explicit versioned image
  import policy, retaining byte-identical legacy operation and attestation
  identities. Bind requested policy and actual fallback outcome in attestations.
- Persist policy in import operations and artifact metadata; source lookups must
  select the exact policy, while digest-bound snapshots/generations keep their
  committed artifact. Fence mixed-version worker publication in PostgreSQL.
- Keep scanner metadata bounded without shrinking ordinary OCI input support:
  distinguish budget errors from invalid/unsafe metadata, discard the complete
  preferred plan on budget exhaustion, publish original global segmentation,
  and attest that fallback explicitly. Never call fallback an optimization hit.
- Verify policy identity/selection, legacy stability, scanner fallback isolation,
  journal-before-PUT and final publication fencing. Then inspect full real-image
  publication costs on the remote test environment; use equal-length prefixes.
- No production rollout/merge, timeout increase, tenant prewarm or claim samples
  during implementation/cost checks. Full cold claim plus first `node -v` and
  density/reclaim acceptance remain the actual goal, not these correctness tests.
- Evidence root: `/tmp/sandbox0-import-layout.S9kuYo`; preserve failed attempts.
- **Implementation:** explicit opt-in
  `xfs-file-ranges-v1` is wired through config, discovery, durable operation,
  builder, version-2 attestation, ready metadata and exact source selection.
  Empty policy preserves legacy identities; digest-bound sources ignore mutable
  import selection. PostgreSQL migration 54 fences policy/attestation mutation,
  old-worker Ready publication, and unsafe downgrade. Scanner entry budget now
  matches OCI's default 10,000,000; depth/path/extents/hardlinks/spans remain
  bounded, with typed budget-only whole-image fallback, not partial success.
- **Preserved implementation failure:** first remote PostgreSQL suite failed
  because SELECT returned 25 columns while the scanner expected 26. SELECT and
  UPDATE RETURNING now share one column list, with a regression test. The failed
  source/binary/results remain separate from revision 2; no baseline DB changed.
- **Verified before cost completion:** local related package unit/race and the
  independent driver module pass. Remote real-XFS builder/importer preferred and
  257-directory budget-fallback cases reconstruct and mount full 300MiB images.
  The OCI unpacker in those tests is a trusted fixture, not a registry import.
  PostgreSQL targeted revision-2 tests pass; the full sandboxstore suite passes
  163 top-level tests, with the opt-in fork-depth/fanout scale test skipped.
  A final local-only wire-contract test independently confirms exact legacy
  attestation bytes/digests for format 1 and 2; importer/store race tests pass.
- **Cost scope:** canonical full-image encoding to a checksum-verifying discard
  sink, not S3, encryption, durable Ready publication, claim or command latency.
  Both modes use identical `rootfs/layout-cost-equal` prefixes and ordinary
  contiguous 64KiB reads over the complete logical image. No sparse shortcut.
  Canonical Node image: global 99,657,215 bytes / 11 objects; file-relative
  99,720,264 bytes / 11 objects (+0.06327%), scanner 7,293 entries / 242 preferred
  spans, no fallback. At this interim snapshot, sparse logical-1TiB Coding costs
  were still running. The first
  Coding global pass overlaps the PostgreSQL full suite; build wall times are
  diagnostic only and are not controlled performance comparisons.
- **Final complete-image costs:** Coding global 1,707,367,431 bytes / 153
  objects; file-relative 1,708,720,176 bytes / 156 objects (+1,352,745 bytes,
  +0.07923%). Both have 76 data packs; only mapping pages increase, 77 -> 80.
  The scanner visited 150,762 entries, found 4,191 preferred spans covering
  3,667,656,704 bytes, and did not fall back. Both Coding passes read every
  logical byte of 1,099,511,627,776 bytes. The retained sparse image occupies
  5,471,035,392 host bytes; this is not a populated 1TiB RootFS.
- **Timing boundary:** Node scan 0.112s, Coding scan 2.417s occur once during
  import, never during claim. Full offline encoding took Node 14.29s / 14.76s
  and Coding 885.00s / 815.56s (global / file-relative); different cache state
  and DB-suite overlap prevent a latency comparison. These are not startup
  numbers, nor encrypted S3 upload timings. Complete encoded-byte overhead is
  small in these two fixtures; actual cold-read benefit is still unmeasured.
- **Final implementation checks:** 18 remote rootfsartifact and 32 importer
  top-level tests passed. The old separately gated privileged importer/RustFS
  tests were skipped; the new real-XFS preferred and budget-fallback importer
  test did run. Final local importer/store race and Darwin/arm64 compile pass.
  All remote binaries prove Go 1.25.5; `/usr/bin/go` outside the module reports
  launcher 1.22.2 and is not the selected build toolchain.
- **Cleanup / decision:** the exact newly created test DB `s0_layout_s9kuyo`
  was dropped after session absence was checked; its synthetic fixtures are
  reproducible from tests. Owned XFS mounts and the read-only retained disk
  mount are absent. Frozen/failed/corrected source inventories, original
  binaries/configurations, 2072/263 baseline DB rows, job index 64101 and 64
  idle NBD devices were verified unchanged. Source changes and failed/passing
  receipts are retained. Keep policy opt-in and disabled in running services;
  proceed next to real durable encrypted import and regional cold claim plus
  first `node -v`, cached-new identities, then occupied production-width/reclaim.
  This step adds zero claims or guest commands and does not establish the 2s
  goal. The original single stop request completed; fresh cloud reads confirm
  `Stopped` / `StopCharging`, still the original 2CPU/8GiB shape. The owned CLI
  terminal is `Terminated`. No lifecycle request was replayed.
- **Sealed evidence:** `/tmp/sandbox0-import-layout.S9kuYo/evidence.json`,
  SHA-256 `0eaa69e951f06122d523f152089d0a480f3fb8dc7260deca3972a8389740af01`.

### D-LAYOUT-READY (2026-09-10 UTC, Node Ready; Coding failed)

- Previous turn made implementation/cost progress, not startup acceptance.
  Verify the sealed I-IMPORT-LAYOUT artifacts and current source before use.
- Produce real pinned Node and Coding OCI imports using the verified
  `xfs-file-ranges-v1` implementation, exact UTC procd bytes, format 2 / 64KiB
  ranges and the existing 16KiB application-encrypted OSS store. Do not synthesize
  ready metadata from the offline discard-sink cost probe.
- Use a separately named test PostgreSQL database and unique object prefixes;
  retain the resulting operations/journal/artifacts for subsequent real runtime
  acceptance. Prefer a consistent read-only dump of the idle formal fixture so
  its existing API/template context is available later. Migrate only the clone;
  original databases, service configurations and installed binaries stay intact.
- Resolve retained staging disk identity and space before writing only a new
  owned work directory. Preserve old trial images/trees. Keep existing importer
  and public claim budgets, preserve failures, and inspect exact operation state
  before any retry. No repeated cost sweep and no production mutation.
- Audit policy/fallback, journal publication, encrypted-object integrity and
  descriptor/readback evidence. Importing or auditing on this host is not a cold
  runtime test: a clean subsequent worker boot/cache audit remains mandatory
  before cold claim plus immediate `node -v`; cached-new and occupied full-width
  acceptance remain part of the unchanged goal.
- Evidence root: `/tmp/sandbox0-layout-ready.J2BgC6`.
- **Node result:** the first real pinned OCI attempt reached durable Ready in
  25.293s. All 11 published objects (99,606,503 encoded plaintext bytes) passed
  authenticated decryption, exact length and SHA-256 readback. A read-only
  diagnostic guard observed committed per-operation intent before every actual
  encrypted PUT. Attestation v2 binds `xfs-file-ranges-v1` and 64KiB ranges,
  with no fallback. Existing artifacts and original services remain unchanged.
  This is an instrumented offline import, not a claim latency measurement.
- **Coding in-flight observation:** the first attempt progressed through real
  OCI decompression, XFS copy, read-only inspection/repair and encrypted object
  publication. During a later interval, read counters increased while the
  published object count remained 35. This matches the canonical builder's
  complete logical-address scan, including sparse zero ranges. Record this as
  offline import cost, not as a new claim-path regression. No timeout change,
  import replay, product-code edit or startup benchmark has occurred in this
  step so far.
- **Coding final result (failure retained):** the first attempt ended at the
  existing 15m build-budget boundary after 903.368s, reporting `lease_uncertain`,
  not `build_timeout`. It published 153 encrypted objects totaling 1,708,591,018
  encoded plaintext bytes, but did not publish a Ready descriptor/artifact.
  Those partial objects were not readback-audited. The sparse 1TiB image occupied
  5,471,019,008 host bytes during encoding; this is not a populated 1TiB image.
  At 17:04:43 UTC the process read counter was 819,504,443,679 bytes and the
  object count was still 120, following earlier intervals of unchanged object
  counts despite increasing reads. The counter also includes OCI work and is
  not an exact encoding cursor. The canonical builder explicitly reads every
  logical range, including holes. No completed Coding cost or startup result is
  inferred from its partial publication inventory.
- **Failure classification follow-up:** the existing renewal context inherits
  the build deadline; a deadline-cancelled in-flight renewal can take precedence
  over `build_timeout` and leave the operation conservatively `building` until
  expiry. A local diagnostic-overlay unit test reproduced that mechanism five
  times under `-race`, without product edits or local e2e. The sanitized remote
  result does not expose the underlying renewal error, so this is a mechanism
  consistent with the boundary timing, not proof of the exact remote error or
  a reason to weaken lease fencing. No timeout was increased.
- **Recovery without replay:** after the exact failed lease expired, invoked
  the existing PostgreSQL garbage reconciler once in the clone, without calling
  `Worker.RunOnce` or any importer. Exactly one expired lease was recovered;
  no terminal operations were purged or objects enqueued for deletion. Coding
  is now `pending`, attempt count 1, with no lease or result artifact. Existing
  Ready artifacts, including the newly verified Node artifact, are byte-identical
  to their pre-Coding inventory. Retain the journal and partial objects; do not
  start an import-enabled manager against this clone and accidentally replay it.
- **Cleanup:** both import processes ended, owned OCI/XFS scratch is empty,
  no owned XFS mount remains, and the retained disk was cleanly unmounted. Old
  trial-image inode/size/mtime/ctime, original binaries/configurations/services,
  baseline 2072/263 sandbox rows, job index 64101, 64 idle NBD devices, and both
  preserved remote source inventories match the preflight. The clone
  `s0_layout_ready_j2bgc6`, its private dump/config, the verified Node artifact,
  and the failed Coding operation/journal are retained. No material data was
  deleted; importer-owned temporary staging was cleaned by the importer.
- **Decision:** this step establishes one real durable encrypted Ready artifact
  and one bounded failed import, not the startup goal. Do not repeat the unchanged
  1TiB import or raise its budget. The usable Node artifact can advance fresh-boot
  regional cold claim plus immediate `node -v` and cached-new identity checks;
  isolating the pending Coding operation from background workers is mandatory.
  Separately investigate the bounded sparse-scan import cost before another
  Coding attempt. Generic warm carriers, claim-time RootFS binding, public 10s
  timeout and opt-in/default-disabled policy remain unchanged. This step adds
  zero claims, zero guest commands and zero production changes; occupied actual
  production-width/reclaim acceptance is still outstanding.
- **Compute closure:** the single stop request completed successfully; fresh
  Aliyun reads confirm `Stopped` / `StopCharging`, still the original
  `ecs.g9i.large` 2CPU/8GiB shape. The owned CLI terminal is `Terminated`.
  No lifecycle request was replayed. Diagnostic evidence is retained under
  `/tmp/sandbox0-layout-ready.J2BgC6` for the next controlled step.
- **Sealed evidence:** `/tmp/sandbox0-layout-ready.J2BgC6/evidence.json`,
  SHA-256 `89e670fff89c4add559652ff6d60d0baf91f4b3b9e07a3d519836c9a27e8bb8b`.

### B-LAYOUT-NODE (2026-09-10 UTC, measured; cold combined gate missed)

- Previous D-LAYOUT-READY is progress: one verified real encrypted Node Ready
  artifact and one preserved Coding failure, not startup acceptance. Its 71
  sealed artifacts and unchanged 1326-file source inventory were revalidated.
- Advance the usable Node artifact through real authenticated regional ingress,
  manager/ctld/stock runsc/procd, then immediate `node -v`. Use fresh target-node
  cache and cached-node new identities as separate cohorts; no tenant RootFS
  prewarm or public 10s timeout change. This single-image stage does not prove
  cross-image cache sharing, large populated RootFS, or the full 2s objective.
- Target eight generic carriers on the existing test ECS temporarily resized
  from 2CPU/8GiB to 16CPU/64GiB. Fresh Aliyun resource-modification results offer
  `ecs.g9i.4xlarge`, not production `u2a/u1` shapes; label it diagnostic hardware,
  not production-width/occupied-reclaim acceptance. Preserve disk/network shape
  and restore the original stopped compute specification afterward.
- Current manager requires its importer; simply setting importer disabled would
  fail startup. Create a separate test DB copy, isolate other template discovery
  inputs and the pending Coding operation there, and audit GC/maintenance so
  original shared objects and prior evidence remain intact. Preserve the
  original D-LAYOUT-READY database and failed operation unchanged. Do not alter
  the claim/readiness boundary to accommodate the fixture.
- Keep installed service backups, configuration hashes, source versions,
  actual RootFS binding, resource leases, NBD/cache/transport evidence, latency
  samples, failures and cleanup proof. The independent driver module and stock
  runtime contract must remain consistent. No production changes or merge.
- Evidence root: `/tmp/sandbox0-layout-node.A7tAqI`.
- **Preparation / isolation:** created private test DB `s0_layout_node_a7taqi`
  from the retained import DB. Only in this copy, made the two non-Node22
  templates non-ready and the pending Coding operation abandoned, preserving
  all rows, descriptors and journals. Original Coding stays pending attempt 1
  in `s0_layout_ready_j2bgc6`. The first fixture transaction omitted the
  required `abandoned_at`; PostgreSQL rejected it and rolled back both template
  changes. Installation refused the missing completion receipt before replacing
  files. The corrected transaction was run only after exact rollback/state
  checks; failed receipts remain. This is fixture SQL, not product code.
- **Test configuration difference:** retained the required importer and
  materializer with their original budgets, using 30-day terminal retention
  for historical fixture records. Disabled only the background RootFS maintenance
  controller in the new test config so a DB copy cannot delete original shared
  objects. This differs from the ordinary running maintenance configuration;
  do not use the result as production acceptance or a controlled improvement
  comparison against B-UTC8. One image, idle memory, eight concurrent leases
  and the real claim/command path are the scope of this stage.
- **Deployment:** same-source manager, ctld and independent Nomad driver built
  with Go 1.25.5 and installed with explicit original backups. Gateways, stock
  runsc, class catalog, public timeout and embedded UTC procd are unchanged.
  Full service/hash/config/capacity validation passed, eight generic carriers
  became ready, and pre-reboot ctld TCP443 and NBD read counters were zero.
- **Cold transition:** issued one graceful ECS reboot. The old tunnel closed;
  an early reconnect was attempted while cloud state was still `Stopping` and
  failed before any remote command. A local missing-master receipt was also
  mistakenly submitted to `apply_patch` and rejected without changes. Provider
  reads confirmed both owned connections terminal (`InstanceNotRunning`), then
  the existing instance returned to `Running`; no reboot or claim was replayed.
- **Harness correction, zero startup samples:** the first diagnostic process
  exited before any claim because the mixed-case loader correctly requires at
  least two images. Verified zero claim rows added, zero TCP443 tenant packets,
  zero NBD I/O, and no active leases on the same fresh boot. Archived that
  failed harness, logs and receipts recoverably under `failed-harness-v1/`.
  Switched only the diagnostic caller to the existing single-template path;
  retained the product mixed-case validator. Rebuilt and launched a distinct
  v2 unit with its own intent; cold preflight must pass again before any POST.
- **Actual result:** two synchronized width-eight cohorts, 16 distinct sandbox
  identities and 16 successful first `node -v` commands (`v22.23.2`, exit 0).
  Exact active leases were verified against the new Node Ready artifact
  `sha256:ffb281591fb7f5ad6468d6912f24dcf1196aaef8ff82884cedcaad0ee5c205ad`.
  The fresh-boot target-node cache proof passed again before the first POST:
  zero ctld TCP443 traffic, zero NBD I/O and no branch mounts. Concurrent cold
  lanes may share in-flight loads; these are not eight separately cold nodes.
  Public request timeout remains 10s, with no claim or command POST retries.

  | Cohort | Samples | Maximum regional claim | Maximum command-only | Maximum claim-to-command completion | Combined >2s |
  | --- | ---: | ---: | ---: | ---: | ---: |
  | Fresh target-node cache | 8 | 0.849864s | 1.312495s | 2.158426s | 8/8 |
  | Cached node, new identities | 8 | 0.247946s | 0.108997s | 0.339679s | 0/8 |

  Maxima are independently selected samples, not additive. Combined latency is
  the original `first_command_duration_ns` (claim start through command finish),
  not that field plus claim. All 16 claim samples were below 1s, but the full
  cold combined 2s gate failed; functional process exit 0 is not SLO acceptance.
- **Correlated claim diagnosis:** cold RootFS ensure 278.696–317.350ms,
  runsc create 70.260–76.986ms, runsc start 122.161–125.869ms, and subsequent
  procd probe 253.175–278.039ms. Cached-new RootFS ensure 41.300–50.409ms,
  runsc start 28.444–42.631ms, procd probe 33.665–72.791ms. These are
  same-request phase records; node subphases are nested, not extra wall time.
  The cold command window has sampled aggregate completed NBD read bounds of
  566,820,864–567,541,760 bytes across eight lanes. This is neither object-store
  download volume nor minimal application demand. The 100ms observer is too
  coarse to tightly bound the ~100ms cached command window. Do not attribute
  the cold gap to a specific source/CPU stage without command-path evidence.
- **Density boundary:** all eight physical leases were 1750m CPU/1792MiB;
  total leased 14 CPU/14GiB, but observed guest cgroup memory was only about
  0.92GB and host available memory never fell below 59,733,762,048 bytes.
  No OOM/max events; this is not occupied-memory or reclaim acceptance. All
  attached NBD queues retained 128KiB max request / 4096KiB readahead, with eight
  simultaneous attachments observed. No queue/cache/resource tuning this step.
- **Interpretation / next evidence:** new file-relative Ready artifacts work
  through the real regional command path, with a preserved cold failure and
  clear cached-new contrast. This does not prove the layout policy caused an
  improvement over B-UTC8: image mix and maintenance configuration differ.
  Before another tuning sweep, use the cold command read window and existing
  producer evidence to select one bounded changed hypothesis; do not repeat the
  unchanged benchmark. Coding import's full-logical scan, mixed-image cache
  behavior, populated large RootFS and occupied production hardware remain open.
- **Restoration verifier correction:** original binaries and the saved two-carrier
  job were restored once. The first read-only verifier wrongly expected the
  retained formal fixture's config paths/DB instead of the actual pre-install
  `/etc/sandbox0` config hashes and `sandbox0` DB. Exact process/config inspection
  confirmed the original paths and binary hashes; corrected the verifier's
  authority, not the restored runtime. Preserve the failed verification/export
  receipts; do not reinstall or resubmit the job to satisfy a mistaken check.
  A second read-only check hashed only the first physical line of PostgreSQL's
  multiline `json_agg`; inspection showed all nine Ready artifacts and Coding
  still pending attempt 1. Corrected collection to the same all-line
  strip-and-join normalization used in the original pre-install checksum.
- **Verified closure:** all 16 test identities are terminal, zero active leases,
  empty test branches/import scratch, all 64 NBD devices idle. Original running
  binaries/config hashes and saved two-carrier job semantics were verified;
  original `sandbox0` DB has 263 sandbox rows, formal DB 2072, and the isolated
  test copy 2088. The prior import DB's complete nine-artifact checksum and
  Coding pending attempt 1 are unchanged. Both retained remote source inventories
  and the local 1326-file source inventory are unchanged. Evidence exported
  without private configs/dumps, all three owned CLI connections terminal, one
  stop and one resize-back request completed. Fresh Aliyun read confirms
  `Stopped / StopCharging`, original `ecs.g9i.large` 2CPU/8GiB. No production
  rollout, merge or tag. Evidence: `/tmp/sandbox0-layout-node.A7tAqI/evidence.json`.

### D-PLATFORM-PROCD (2026-09-10 UTC, candidate preparation)

- B-LAYOUT-NODE is progress, not completion. Its 125 artifacts, source inventory
  and ledger checksums were verified before this entry. Retain its cold combined
  miss (2.158426s) and the old B-UTC8 reference; do not replay unchanged tuning.
- New hypothesis: fixed platform procd bytes currently incur demand-read cost
  through the tenant RootFS. With file-relative complete ranges, known platform
  bytes can populate the existing checksum/length-keyed cache before a tenant
  is selected. This requires neither reading a tenant image nor changing the
  authenticated readiness boundary. First real `node -v` remains mandatory.
- A temporary same-digest read-only `/procd` OCI mount passed four component
  tests three times under the race detector, but was rejected before any remote
  use: it changes writable RootFS visibility and would need a new persistence
  and compatibility contract. The diagnostic files/results remain preserved.
  Stock gVisor's [filesystem documentation](https://gvisor.dev/docs/user_guide/filesystem/)
  was checked; no DirectFS, exclusive-bind or stock runsc setting was changed.
- Selected diagnostic: on ctld's existing node ReadCache initialization, read
  only the digest-pinned public platform procd file (32MiB upper bound), verify
  the whole file before insertion, and seed complete 64KiB units. Do not pad the
  partial tail, pin data, add another cache, alter mapping/content keys, or
  increase the existing 128MiB LRU budget. RootFS and OCI remain unchanged;
  modified content must miss the old cache or use its exact unchanged fragments.
- Local overlay tests cover raw/compressed data, different RootFS offsets,
  COW updates and old generations, partial tails, buffer ownership, disabled/
  undersized cache, ordinary eviction, concurrent readers and bad file/digest
  inputs. Four top-level seed tests passed three race-enabled repetitions.
  No product source file is edited; the candidate lives only in the owned Go
  build overlay under `/tmp/sandbox0-platform-procd.YmgqYi`.
- Preparation failure retained: the first local OCI-overlay serializer had a
  missing Ruby brace and exited before generating files; its dependent formatter
  could not find `handle.go`. Corrected locally; no remote action occurred then.
- Cold scope must be explicit: fresh boot and zero tenant object/NBD reads, but
  the fixed platform procd content cache is intentionally seeded. Never label
  this as a completely empty decoded cache or tenant RootFS prewarm. Record
  startup seed count/bytes, actual runtime bindings and first-command timings.
- Remote admission: original ECS freshly verified `Stopped / StopCharging`,
  `ecs.g9i.large` 2CPU/8GiB. Current modification inventory offers diagnostic
  g9i 16CPU, not production u2a/u1 hardware. Use the same eight-carrier isolated
  Node fixture/config differences as B-LAYOUT-NODE, keep public 10s timeout,
  preserve original services/DBs, then restore original stopped specification.
  A single candidate run is feasibility evidence, not causal or production
  acceptance; the full mixed-image/occupied/reclaim goal remains open.
- Pre-run verification: race-enabled `pkg/rootfsblock` and `pkg/rootfssession`
  regression produced 270 top-level pass records and 15 skip records (including
  privileged subcases and large fixture/model tests). This is not privileged
  integration acceptance. The four seed tests passed again after adding the
  startup seed report. The remote Go 1.25.5 build preserved the prior manager
  and driver binary hashes exactly; only candidate ctld changed to
  `7c095b8657cc423c8b1dc9da8705d32db07a99e5e8451c14850d9131f6993ceb`.
  The isolated copy `s0_platform_procd_ymgqyi` keeps only Node22 claimable and
  disables object maintenance as in B-LAYOUT-NODE. The retained source import
  DB, its nine artifact records and pending Coding attempt 1 remain untouched.
  Eight generic carriers became ready before any claim. A single graceful ECS
  reboot was requested at 18:37:17 UTC to establish the tenant-data-cold scope;
  reboot/service preparation time is not included in sandbox claim latency.
- Fresh-boot proof: `00467751-1d5c-48e3-8bd6-1f79edc52374`, eight ready
  carriers, 2072 sandbox rows, zero active leases, zero ctld TCP443 outgoing
  packets and zero configured NBD I/O before claim. The active ctld startup
  record shows 373 complete fixed-platform units retained, charged at
  24,543,027 bytes within the unchanged 134,217,728-byte LRU. The fixed file
  is 24,463,545 bytes, whole-file SHA-256 pinned before insertion; the partial
  tail is not seeded. This is explicitly platform-seeded/tenant-data-cold,
  not an empty decoded cache. The one planned two-cohort harness was launched
  only after this proof; concurrent lanes may share in-flight reads.
- **Measured result / stop decision:** all 16 unique claims and immediate
  `node -v` commands completed with exact `v22.23.2` stdout and exit 0, with
  no POST retries. All eight first-cohort combined samples still exceed 2s.
  The candidate stays diagnostic-only; do not promote it as a cold-start fix.

  | Scope (8 samples each) | Regional claim max | Command-only max | Claim-start to command-finish max | Combined >2s |
  | --- | ---: | ---: | ---: | ---: |
  | Prior B-LAYOUT-NODE fresh target-cache cold | 0.849864s | 1.312495s | 2.158426s | 8/8 |
  | Current fresh tenant-data cold, fixed procd seeded | 0.765284s | 1.368354s | 2.139798s | 8/8 |
  | Current cached-node, new sandbox identities | 0.211965s | 0.088716s | 0.296151s | 0/8 |

  Each maximum is selected independently, so columns are not additive.
  Combined time is the original `first_command_duration_ns`, not claim plus
  that field. The observed cold combined maximum decreased only 18.628ms
  (0.863%); these sequential different-boot cohorts are not randomized causal
  proof. No claim or command failure/missing sample was excluded.
- **Correlated phases, not an invented cause:** cold procd probe was
  102.639–122.201ms versus prior 253.175–278.039ms; runsc start was
  61.926–66.338ms versus 122.161–125.869ms. However RootFS Ensure was
  406.792–445.956ms versus 278.696–317.350ms, and first-command latency did
  not improve. Node timings are nested within claim time. The first-command
  window still spans sampled aggregate completed NBD reads of
  567,181,312–568,086,528 bytes across eight lanes (prior
  566,820,864–567,541,760). These are sector-based bounds, not S3 download
  bytes, source GET counts or minimal application demand. We did not collect
  per-command source/CPU critical-path spans, so do not assign the remaining
  wall-clock gap to an individual storage or CPU stage from aggregate wait time.
- **Density boundary unchanged:** eight exact 1750m/1792MiB leases, 14 CPU /
  14GiB total leased but only 921,128,960 bytes observed guest cgroup memory
  in the cold cohort. Minimum observed host available memory was
  59,847,766,016 bytes; no OOM/max events. This is not occupied-memory/reclaim
  acceptance. Observers saw eight attached devices, unchanged 128KiB requests
  and 4096KiB readahead, and no observer errors.
- **Next evidence, not another tuning sweep:** preserve both cold failures;
  do not repeat the unchanged fixed-platform/cache/queue candidate. The primary
  observed gap remains first-command cold work. Any next candidate needs
  bounded command critical-path source/CPU evidence and a predicted effect.
  Do not retry the unchanged pending Coding importer: its full-logical sparse
  scan first needs publication-equivalence and fail-closed validation. Mixed
  images, large populated RootFS and occupied production hardware remain open.
- Restoration exception retained: after original services/files were restored,
  the pre-submission Nomad job GET timed out (no intent and no PUT had occurred).
  The early final verifier correctly refused incomplete restoration. A separate
  read then proved both restore submission markers absent and the warm job still
  `Stop=true` at index 65267. Proceed only with the one still-pending original
  job submission; do not replay a possibly successful write or alter timeouts.
- **Verified closure:** original job submitted once and restored at index 65279
  with two ready carriers. Running original binary/config hashes, original
  database counts, both retained source inventories and the nine-artifact source
  import checksum were verified; Coding remains pending attempt 1. All 16 test
  identities are terminal, zero active leases, empty branches/import scratch,
  all 64 NBD devices idle, retained staging disk unmounted. Private configs and
  DB dump remain remote-only in the isolated experiment root. Both owned CLI
  connections are terminal. One stop and one resize-back request completed;
  fresh Aliyun read confirms `Stopped / StopCharging`, original
  `ecs.g9i.large` 2CPU/8GiB. The 1326-file local product source inventory is
  unchanged (`1fa1e9e4a816fd2415aa1978211977d97810f010b810d14b663ab5e18c72d2b7`),
  as are the staged diagnostic source hashes. No production change, PR merge,
  tag, timeout increase or tenant RootFS prewarm. Evidence:
  `/tmp/sandbox0-platform-procd.YmgqYi/evidence.json`. The goal remains open.

### I-SPARSE-PUBLICATION (2026-09-10 UTC, correctness verified; real import pending)

- D-PLATFORM-PROCD is completed diagnostic progress, not goal completion. Its
  124-artifact seal and 167-record ledger were verified before this entry.
  Historical command HTTP/flight/fault evidence already exists; do not replay
  the same attribution exercise merely because the next-work line suggests it.
- The concrete full-scope blocker is D-LAYOUT-READY's failed Coding import:
  the canonical publisher scans the entire sparse 1TiB logical image, despite
  only about 5.47GB host allocation. Fix that read amplification before another
  Coding attempt so mixed-image startup acceptance can proceed. Import time is
  separate from sandbox startup; the previous cold command failures remain.
- Add an explicit owned read-only file publication path, using only host
  filesystem-reported holes, with O(1) extent state and original canonical
  segment boundaries. Do not trust tenant hints, change live block readers,
  change the storage format or skip allocated zero extents by assumption.
  Preserve the ordinary ReaderAt full-scan path as the comparison and fallback.
  [Linux lseek semantics](https://www.man7.org/linux/man-pages/man2/lseek.2.html)
  permit conservative all-data reporting; unsupported probing must fall back,
  while genuine I/O errors, invalid offsets and source mutation must fail.
- Decision gate: byte-for-byte identical objects, descriptor and references
  for format one/two, global/shifted layouts, random holes and boundary cases;
  cancellation, short reads, publication errors and mutation must preserve
  failure. Sparse 1TiB tests are not populated-data or startup benchmarks.
  No timeout increase, tenant RootFS prewarm, production change or local e2e.
  Evidence root: `/tmp/sandbox0-command-path-delta.c7QB1c`.
- Focused race gate passed seven top-level tests without skips: exact objects /
  descriptor / inventory for deterministic random sparse extents and both
  formats, shifted boundaries, malformed seek responses, unsupported fallback,
  source/publisher/cancellation errors, read-only FD validation and file mutation.
  Actual sparse 1TiB local files required 131,072 bytes / four seeks for two
  endpoint data units, and zero bytes / one seek for all holes. These are sparse
  storage models, not populated RootFS or claim latency. The first compile
  attempt failed only on an inferred `int` in a near-MaxInt64 test; corrected
  to `int64` before any tests ran.
- Manager worker/discovery/claim regression passed 90 top-level tests; one
  RustFS-dependent test skipped. The first complete four-package race run ended,
  but its tool transport elided 192,056 bytes, so its receipt was rejected and
  no passing result is claimed. Preserve this capture failure. Rerun only the
  local unit suite with checksummed zlib-base64 stdout; no runtime/import replay.
- Remote admission: CLI confirms the isolated instance is stopped in original
  2CPU/8GiB shape. Run only the selected unit tests and actual XFS full-image
  equivalence / importer reconstruction tests in an owned Go overlay and
  temporary mount directory. Preserve the 1325-file remote source inventory,
  original running services/configurations, two-carrier job index 65279 and
  database counts. No service rollout, new claims, guest commands, retained disk
  mount, database mutation or regional object publication is part of this step.
  Full local race recapture may run concurrently; it is not yet recorded green.
  Require nine selected top-level remote passes and no skips, prove temporary
  mount absence and original state, then stop the unchanged 2CPU instance.
- Full local race recapture is preserved, including its failure: 319 top-level
  passes, 20 skip records, and two existing artifact-traversal tests failed on
  the local virtual `/tmp` reporting device 30 for directories and 31 for regular
  files. Both test and scanner files are unchanged from the pre-edit inventory;
  the existing cross-filesystem guard correctly refuses that fixture. Do not
  remove the guard or recategorize the failed run as passing. Re-running the
  artifact package with only the test process's TMPDIR on an owned native
  `/dev/shm` directory produced 16 passes and two privileged skips; the two
  failing tests now pass. The same compiled source is used. The empty native
  temporary root was removed. Darwin/arm64 test-binary cross-compilation passed;
  that binary was not executed and does not prove non-Linux runtime acceptance.
- **Remote correctness gate passed:** one test unit on original 2CPU/8GiB,
  nine selected top-level race passes, zero skips/failures. An actual
  314,572,800-byte XFS image with hardlinks, shared/unwritten extents, a sparse
  file, small data and external symlinks produced exactly identical descriptors,
  inventories and all 32 immutable objects (811,603 bytes) with full versus
  hole-skipping publication. Both preferred layout and typed budget fallback
  imports reconstructed a mountable XFS image with matching files; preferred
  publication was two objects / 18,768 bytes, fallback two / 30,746 bytes.
  These different policies need not have the same bytes. The two actual sparse
  1TiB file models also passed on the remote host (128KiB/four seeks and
  zero bytes/one seek), still not populated-data or latency acceptance.
- Read-only cleanup verifier correction retained: the first verifier assumed
  the inherited command helper returned stdout; it returns nil. The export
  correctly refused unverified state. Changed only the verifier to capture
  and validate `findmnt` stdout explicitly; no test or runtime was replayed.
  Verification then proved empty test scratch, no owned mounts, 64 idle NBDs,
  unchanged original running files/configs, job index 65279, 2072/263 database
  counts and the retained 1325-file source inventory. No service was installed,
  no database or regional object was written, and no claim/guest command ran.
- **Decision:** retain the sparse publication implementation as a correctness-
  verified import improvement. It changes no publication format, attestation,
  live-read/cache/readiness or timeout contract. It has not yet measured a real
  Coding OCI import under the unchanged 15m budget. The next step is one
  explicitly isolated changed-code import, with complete durable object and
  Ready auditing, before mixed-image cold/cached-new and occupied-node startup
  validation. Do not substitute import timings for claim or command timings.
- Closure: the single stop request completed at 19:19:08 UTC. A fresh Aliyun
  read at 19:25:21 UTC confirms `Stopped / StopCharging`, unchanged original
  `ecs.g9i.large` 2CPU/8GiB. The sole owned CLI terminal connection is
  `Terminated` with an end time; the SSH master was closed successfully.
  No resize, production change, PR merge or tag occurred. Preserve the full
  local failed run and its passing native-filesystem follow-up in the evidence
  bundle; do not call the original full invocation green. Evidence:
  `/tmp/sandbox0-command-path-delta.c7QB1c/evidence.json`. The cold-start goal
  remains open.

### D-SPARSE-CODING (2026-09-10 UTC, real Coding Ready and full object audit verified)

- Previous turn was progress: I-SPARSE-PUBLICATION implemented the explicit
  immutable local file path and passed byte-equivalence/privileged XFS gates.
  Its 62-artifact seal and 168-record history were verified before this entry.
- Run one new Coding import from the same pinned real OCI image, logical 1TiB,
  format two, 64KiB file-relative policy, encrypted OSS and exact UTC procd.
  Keep the production worker's 15m build budget, 2m lease and 30s renewal.
  Use a new isolated database cloned from the idle formal baseline and a new
  object prefix. The old failed Coding operation remains pending attempt 1 in
  its retained database; never replay or alter it as part of this comparison.
- Stage the correctness-verified product delta through a Go overlay against
  the unchanged remote source. No service/runtime rollout, tenant prewarm,
  claim, guest command, production mutation or local e2e is part of this import
  step. Observe process I/O, committed pre-PUT journal and Ready transaction;
  authenticate and hash every newly published object before acceptance.
- Baseline is the preserved 903.368s failure, not a successful 15m import. The
  test image is still sparse logical capacity, not 1TiB populated data. Import
  time is not regional claim or first-command latency. Evidence root:
  `/tmp/sandbox0-sparse-coding.vRYYZl`.
- **Result: Ready in 322.786799595s**, one worker run and one durable attempt,
  with the original 15m budget unchanged. The real pinned Coding image produced
  artifact `sha256:3392066be6e26546909b842347aee3b136a6fdf9df56f2ceba09508958048a31`
  in database `s0_sparse_coding_vryyzl` (OID 298346). All 156 immutable objects
  were journaled before their one successful PUT, then authenticated/decrypted
  and fully hashed on readback: 80 mapping pages / 3,441,128 bytes and 76 data
  packs / 1,705,554,754 bytes, 1,708,995,882 total encrypted-store plaintext
  bytes. Ready proof is attestation v2, `xfs-file-ranges-v1`, 64KiB, no fallback;
  descriptor-only reading also verified the XFS superblock. The test plus audit
  completed successfully at 19:40:41 UTC; audit time is outside the import time.
- The actual materialized file had logical size 1,099,511,627,776 bytes and
  allocated size 5,471,047,680 bytes. Whole-worker `rchar` delta was
  20,166,764,772 bytes, versus the prior failure's observed >819GB process
  counter before its timeout. This includes OCI unpack/XFS work and is not a
  publisher-only or storage-device-read count. At 19:40:15 UTC, the open image
  seek position was already 825,531,633,664 bytes while process `rchar` was
  19,047,707,946 bytes and 124 objects were published: direct evidence that
  logical holes are jumped rather than read linearly. Do not present this as
  a repeated statistical speedup or as populated-1TiB acceptance.
- Observed stages were OCI unpack, file copy, XFS repair, then publication;
  no claims or guest commands ran. Diagnostic additions were two process-I/O
  snapshots and one first-PUT walk of the owned work tree to record file size
  and allocation; that overhead is included, not subtracted. This turn changed
  no product source beyond the already verified sparse implementation.
- Exceptions preserved: strict host-key lookup initially failed for the new
  public IP; reconnect used the original trusted host-key alias, with strict
  checking still enabled. A broad process-argument observation exposed a
  temporary CLI URL; that owned session was immediately closed and its
  termination verified, then replaced. No URL is retained in evidence. The
  first observation has an explicit summary-only receipt. Cleanup initially
  compared symbol-keyed Ruby state with string-keyed JSON and refused to
  proceed; export also refused. Only the verifier's key normalization changed;
  no import, runtime or database operation was replayed.
- Cleanup verified the original service binaries/configurations and PIDs,
  2072/263 original database rows, job index 65279, 64 idle NBD devices, the
  original 1325-file import source and the old pending Coding attempt 1 plus
  its nine Ready-artifact checksum. Owned scratch is empty; the retained disk
  is unmounted and earlier trial images unchanged. The new Ready database,
  object prefix and private configuration/dump are retained for mixed-image
  acceptance; private files were not exported. Compute stop is in progress.
- **Decision:** keep the sparse publication fix. Real Coding import no longer
  blocks the next cold/cached-new mixed-image claim plus immediate `node -v`
  tests. Claim, command-only and combined timings still need measurement,
  followed by actual occupied production-width and large populated RootFS
  coverage. The 2s startup goal is not achieved by this import result.
- Closure: the one stop request completed at 19:44:10 UTC; fresh Aliyun read
  at 19:44:20 UTC confirms `Stopped / StopCharging` in unchanged original
  `ecs.g9i.large` 2CPU/8GiB shape. All three owned CLI sessions are terminated
  with end times. No resize, service rollout, production change, merge or tag.
  Evidence is `/tmp/sandbox0-sparse-coding.vRYYZl/evidence.json`; the goal
  remains active. Next work must reuse the verified Ready artifact, not repeat
  this already successful import merely to obtain another passing run.

### B-LAYOUT-MIXED (2026-09-10 UTC, mixed-image cold command SLO misses preserved)

- Previous goal turn was progress: D-SPARSE-CODING published Ready in 322.787s
  under the unchanged 15m budget and authenticated/hashed all 156 objects.
  Its 73-artifact seal and 169-record history were verified before this entry.
  Reuse the proven Coding artifact, not another OCI import.
- Create a private clone of the successful Coding database and copy exactly
  the verified Node Ready artifact's five-table metadata closure, preserving
  descriptors, attestations, object checksums, journal state and timestamps.
  The old failed Coding operation stays untouched in its original database.
  Disable discovery only for the unused Node24 template in the new copy;
  retain Node22 and Coding templates. Disable background deletion in the
  isolated copy so retained source objects cannot be collected by the trial.
- Build the current correctness-verified product source through an owned Go
  overlay; no platform-procd cache seeding or other rejected diagnostic tuning.
  Install only on the isolated remote host with verified backups and exact
  rollback targets. Use eight generic carriers, 1750m/1792Mi per claim, same
  128KiB NBD request cap and 4096KiB readahead, and the unchanged 10s public
  request budget. Do not initialize a guest before claim.
- Verify a fresh boot, zero ctld object-storage traffic and zero NBD counters
  before one 8-way mixed cold cohort (four Node22/four Coding). Then delete
  those identities, allow automatic carrier replacement and run one 8-way
  cached-node new-identity cohort. Execute exactly one immediate `node -v`
  per claim, expecting the pinned v22.23.2 in both images. No claim/command
  POST retries; retain functional and latency failures. Report authenticated
  regional claim, command-only, and original claim-to-command-completion time
  separately, including per-image results and actual resource/RootFS binding.
- Fresh Aliyun availability/dry-run/price checks admit only the existing
  `ecs.g9i.large` -> `ecs.g9i.4xlarge` test-instance resize. Diagnostic shape
  is 16CPU/64GiB with eight carriers; this is not the production machine type
  and not an occupied-memory/reclaim test. One 1TiB logical sparse image is
  still not a 1TiB populated-data case. Full acceptance remains open.
  Evidence root: `/tmp/sandbox0-mixed-layout.UY0fMG`.
- **Measured result:** 16 unique claims and 16 immediate `node -v` commands
  succeeded without POST retries. Each cohort contained four samples per
  image. All cold claims met the fallback 2s readiness threshold but missed
  1s; all eight cold claim-to-command-completion samples exceeded 2s. Both
  cached-new images met 2s in this finite cohort. Marginal maxima below are
  not additive: the combined column is the original continuous timer.

  | Image / node-cache state | Regional claim max | Command-only max | Combined max | Combined >2s |
  | --- | ---: | ---: | ---: | ---: |
  | Node22 / fresh cold | 1.225243s | 1.292285263s | 2.522537776s | 4/4 |
  | Coding / fresh cold | 1.926517s | 1.218692357s | 3.155506002s | 4/4 |
  | Node22 / cached, new identities | 0.717853s | 0.505003447s | 1.217882164s | 0/4 |
  | Coding / cached, new identities | 0.849419s | 0.589797694s | 1.424366844s | 0/4 |

- The real Coding Ready metadata was cloned into `s0_mixed_layout_uy0fmg`
  (OID 299880). Node's Ready artifact and its five-table closure were copied
  exactly, without import or object writes. The prior failed Coding operation
  was not copied. A fresh boot `d906dd93-8e34-4d37-be2f-e7a10161c4c4`
  had zero ctld TCP443 traffic and zero NBD counters before the first cohort;
  no platform seed or tenant prewarm was used. Simultaneous lanes may share
  in-flight reads. Provider-side origin caches were not reset.
- Correlated claim phases locate the dominant time on the node, not in
  metadata or carrier acquisition. Cold Node's `node_claim` was
  746.117-782.200ms and `procd_probe` 390.855-403.458ms; Coding's were
  1241.106-1273.076ms and 600.750-612.476ms. Inside `node_claim`, RootFS
  preparation took 478.054-516.915ms / 640.821-669.992ms respectively,
  runsc create 66.078-77.988ms / 301.972-306.179ms, and runsc start
  174.626-175.441ms / 285.022-287.458ms. These nested marginal ranges
  must not be added to their enclosing phase. Metadata was 0.995-2.573ms
  and slot acquisition 6.826-12.674ms across the cold cohort.
- Cached-new still spent 359.729-369.902ms in Node `node_claim` and
  500.032-518.553ms in Coding `node_claim`; `procd_probe` remained
  309.292-318.441ms / 287.613-305.054ms respectively. Guest lease CPU
  counters after the commands reported zero throttled periods/time in all
  16 cases; OOM, max-limit and reclaim/refault counters were also zero.
  Actual concurrent leases were 14CPU/14GiB, but observed guest memory was
  only about 880MiB total and host available memory never fell below
  59,737,554,944 bytes. This does not cover occupied-node memory pressure.
- Sampled NBD completed-read bounds were 286,104,064-315,034,112 bytes
  over the cold claim window and 630,540,288-647,120,896 bytes over the
  first-command window. These cohort windows overlap across images; do not
  add them or call them GET traffic/minimum executable bytes. Whole-trial
  ctld matched incoming IP counters were 86,473,795 bytes and can undercount
  traffic without early socket association. No command-internal trace was
  collected in this trial, so cache eviction and object dependency causes
  remain hypotheses, not established explanations for the mixed-image cost.
- The harness completed at 20:10:33 UTC, preserving all SLO misses. All
  16 trial identities were deleted through the public lifecycle, automatic
  replacement returned eight generic ready carriers, and verification found
  no active leases, writers, NBD attachments or import scratch. No retained
  source image/database/object was deleted. The 1331-file product inventory
  was unchanged; no timeout, cache, queue or fanout tuning was performed.
- **Decision:** keep the correctness-verified sparse publication fix, but do
  not claim it solved cold command execution. Mixed-image results are not a
  controlled regression comparison against the prior Node-only or legacy
  layout runs. Next attribution should explain the remaining mixed-image
  node/readiness and first-command dependencies before changing parameters;
  do not repeat unchanged cache/readahead/fanout/platform-seed trials.
  Full occupied production-type/width and large populated RootFS acceptance
  remain open. Original runtime/configuration restoration is in progress.
- Restoration verified at 20:14:56 UTC: original binary/configuration hashes,
  exact original job semantics (new modify index 65724), two ready carriers,
  the original 263/2072-row databases, both retained source artifact sets and
  the 1325/1301-file remote source inventories are intact. All 64 NBD devices
  are idle, owned branch/import scratch is empty, and the retained disk is
  unmounted. The new metadata database and raw evidence remain available.
- Exceptions: the planned graceful reboot terminated the original SSH
  transport; the owned replacement session retained strict host-key checking.
  A local supplemental phase-analysis script initially had a syntax error;
  it was corrected before evidence generation, reading the same checksummed
  exports without rerunning a claim, command or remote mutation. Installation
  and restoration each took about 116s, including graceful service stops;
  this setup/teardown time is not sandbox startup latency. Both owned CLI
  sessions have terminal status and end times. One cloud stop is pending;
  no production mutation, PR merge or tag was made.
- Closure: the one stop completed at 20:17:17 UTC. A fresh Aliyun read at
  20:18:05 UTC verifies `Stopped / StopCharging` and restored original
  `ecs.g9i.large` 2CPU/8GiB shape after the one guarded resize-back request.
  Evidence is `/tmp/sandbox0-mixed-layout.UY0fMG/evidence.json`, including
  raw samples, authenticated phase logs and per-image supplemental analysis.
  No compute, SSH terminal, test guest or writer remains active for this
  trial. The sparse import improvement is retained; the startup goal remains
  active with the explicit acceptance gaps and next-attribution gate above.

### D-SHARED-WINDOWS (2026-09-10 UTC, concurrent file-relative sharing probe)

- Previous turn was progress: B-LAYOUT-MIXED established cold/cached-new
  mixed-image timings and correlated node phases without CPU throttling or
  reclaim in its samples. Its 117-artifact seal and 170-entry history were
  verified before this entry. The remote host remains stopped.
- New falsifiable mechanism: the accepted file-relative 64KiB publisher
  shares decoded content identities across image offsets, but runtime bulk
  coalescing still groups entries by absolute logical 1MiB windows. Distinct
  window membership could create two simultaneous source loads for common
  content. Existing layout contract tests prove sequential cached reuse,
  not concurrent cold flight sharing for unequal window boundaries.
- This differs from the older physical-source overlap probe: the current
  file-relative publisher deliberately changes cross-image content identity.
  It does not justify another unchanged cache/admission/readahead sweep.
  Use the unchanged current production builder/Reader through a temporary
  unit-test overlay, eight synchronized Readers over two equal-byte files.
  Change only physical file offsets within each raw/compressed stratum;
  control with equivalent-window offsets and exact 4KiB demands.
- Before any source load can finish, wait for all fixture goroutines to block,
  then count actual source loads and covered checksum/length identities.
  Verify every delivered byte, the existing source bound, and zero additional
  data loads for subsequent new Readers. No throughput/latency inference from
  this barrier fixture; real published-artifact overlap and its runtime cost
  would still require separate evidence if the mechanism is confirmed.
- No runtime/source policy change, cloud start/resize, guest, object-store,
  database mutation or local e2e. Public timeout and all architecture contracts
  remain unchanged. Evidence root: `/tmp/sandbox0-shared-windows.Ay0qNg`.
- **Result: mechanism confirmed, runtime attribution unproven.** All 12
  offset/demand/representation combinations passed normally and under race
  detection. For 128KiB demand and eight Readers, equal/equivalent windows
  shared one source load. Offsets 12,288 and 110,592 bytes created unequal
  windows and two concurrent loads covering 29 decoded 64KiB units, only
  15 unique: 917,504 bytes of duplicate decoded coverage. Raw source bytes
  were 1,900,544 versus 983,040 for the equivalent-window control. The highly
  compressible fixture read 435 versus 225 stored bytes. These are synthetic
  content/range counts before encryption, not real-object wire-byte savings.
- Every 4KiB-demand control shared one exact source load, including unequal
  file offsets. Subsequent new Reader instances issued zero extra data loads;
  all requested bytes matched and source admission returned to zero without
  exceeding its existing bound. This confirms a concurrent bulk-membership
  gap, not broken sequential caching. It does not prove actual cache eviction,
  explain the cached-new B-LAYOUT-MIXED delay, or measure a command speedup.
- Existing related layout, coalescing, cache and shared-admission regressions
  passed in a separate race run: 361 test items including subtests. Raw JSON
  output was exported completely with a byte count and SHA-256. Product source
  remains the same 1331 files; the only new code is a temporary unit overlay.
- Exceptions are retained: the initial virtual filename ended in
  `_windows_test.go`, so Go correctly excluded it on Linux and reported no
  tests. That exit-zero result was rejected, the virtual filename corrected,
  and the expected 12 fixture reports explicitly checked. A separate control
  run's tool output was truncated; its partial text remains an incomplete
  observation, not passing evidence. The subsequent race run used checksummed
  compressed output. Neither exception caused a remote test/mutation replay.
- **Decision:** diagnostic only. Before considering a content-based bulk
  grouping change, map the CURRENT published Node/Coding ranges and actual
  read requests by decoded content and ordered window identity. The old
  physical-overlap trace used different content segmentation and cannot
  establish the new cross-object content-overlap rate. Preserve its important
  negative result: independent small reads sometimes completed earlier than
  covering bulk reads, so unconditional small-to-bulk joining remains rejected.
  Real overlap on the readiness/first-command path, bounded verification and
  cancellation behavior, then end-to-end cold/cached-new/occupied acceptance
  are required; no cache, queue, fanout, timeout or prewarm policy was changed.
- Fresh Aliyun read at 20:28:10 UTC confirms the original 2CPU/8GiB instance
  remains `Stopped / StopCharging`. This turn started no compute or sessions,
  ran no claim/guest command, and made no remote/production mutation, merge or
  tag. Full populated-RootFS and actual occupied production-width acceptance
  remain open. Evidence: `/tmp/sandbox0-shared-windows.Ay0qNg/evidence.json`.

### D-MIXED-READ-TRACE (2026-09-10 UTC, current-artifact read attribution)

- Previous turn was progress: D-SHARED-WINDOWS proved a concurrent bulk
  membership gap in a deterministic fixture, not its real startup impact.
  Its 19-artifact seal and 171-entry history were verified first. Baseline
  source remains main/HEAD `0f09220460581bfc1fdc331f34ebc85bf38381e7` plus
  the same 1331-file candidate inventory used by B-LAYOUT-MIXED.
- Hypothesis: current file-relative Node/Coding artifacts can issue distinct
  bulk flights covering identical content during the real claim/first-command
  path. The older physical-object overlap trace cannot measure cross-object
  content overlap after this segmentation change. Measure before changing
  coalescing; retain the prior rejection of unconditional small-to-bulk joins.
- Independent variable is bounded ctld diagnostic instrumentation only.
  Reuse B-LAYOUT-MIXED manager/driver binaries and both already Ready artifacts
  exactly; clone the successful Coding database and copy Node's verified
  five-table closure again into a new private database, without OCI import or
  object writes. No template prewarm, platform seed, cache/window/admission,
  queue, gVisor/procd, timeout or public contract change.
- First validate trace correlation locally with the actual Reader/NBD code,
  not local e2e. Emit only branch/root/content/hashed-object identities, exact
  source spans, bulk-window members, cache insertion/eviction/baseline state,
  and trace task/region scheduling metadata. Each trace is bounded to 256MiB
  and 90s; overflow, automatic stop, missing clocks or ambiguous ownership
  invalidates attribution. Do not reinterpret profiler timings as observer-free
  startup acceptance, and do not subtract its overhead from measured samples.
- Then, only on the isolated g9i test host resized to 16CPU/64GiB, use eight
  generic carriers and 1750m/1792MiB leases. Verify fresh boot and zero tenant
  transport/NBD counters before four Node/four Coding cold claims, each with
  one immediate `node -v`; repeat only as a cached-node NEW-identity cohort.
  Preserve all failures and SLO misses, with the unchanged 10s request budget
  and no claim/command POST retries. Trace cleanup so tasks terminate, but
  distinguish it from claim and first-command windows in the analysis.
- Keep original binaries/configurations, two-carrier job index 65724, retained
  source databases and files, idle NBD/writer proof and exact rollback targets.
  Stop/restore only owned test resources, shrink to original 2CPU/8GiB, and
  stop compute afterward. This is diagnostic hardware, not production-type or
  occupied-memory/populated-RootFS acceptance. Evidence root:
  `/tmp/sandbox0-mixed-read-trace.EGhNWT`.

### D-MIXED-READ-TRACE — measured result and closure

- Kept the same 1331-file product candidate, both Ready artifact descriptors,
  UTC procd and B-LAYOUT-MIXED manager/driver bytes. Built only a temporary
  ctld trace overlay; no source policy, public timeout, cache size, read-ahead,
  window, admission or tenant prewarm change. This is instrumented diagnostic
  evidence, not observer-free production acceptance.
- Local trace tests: five passing items with race checking, including actual
  NBD/shared-flight correlation and raw/compressed shifted-window fixtures.
  The full Reader race controls passed 825 items (186 top-level, two skips).
  The first long window record was truncated by Go trace's 1024-byte string
  limit; retained that failure and split records into bounded members. The
  corrected parser recovers exactly 917504 concurrent cross-object decoded
  bytes in both synthetic fixtures before any remote claim was issued.
- One fresh-boot, eight-way mixed cohort and one cached-node new-identity
  cohort completed: four Node plus four Coding per cohort, 1750m/1792MiB
  actual leases each. Cold boot had zero ctld object transport and NBD I/O.
  All 16 unique claims and 16 immediate `node -v` commands succeeded with
  exact `v22.23.2` output. No claim/command POST retry; timeout remained 10s.

| Cohort / image | Claim max (s) | Command-only max (s) | Combined max (s) |
| --- | ---: | ---: | ---: |
| Cold / Node | 0.873303 | 1.180507 | 2.056793 |
| Cold / Coding | 1.907962 | 1.124074 | 3.031169 |
| Cached-new / Node | 0.586568 | 0.543994 | 1.117155 |
| Cached-new / Coding | 0.628669 | 0.069556 | 0.698108 |

- Claim is authenticated regional Server-Timing; combined is the original
  client claim-start-to-first-command-completion timer, not a sum of maxima.
  All cold claims passed 2s, but all eight cold combined samples exceeded 2s.
  Cached-new samples passed both. Do not infer a causal latency gain relative
  to B-LAYOUT-MIXED from a diagnostic observer and different finite samples.
- Four complete trace files passed byte/SHA256, clock, balanced-region,
  terminal-task and unique-flight-leader checks. Cold primary: 7202 NBD reads,
  4429 flight calls / 466 leaders / 3963 waiter edges; cached-new: 7195 reads,
  1139 / 158 / 981. No ambiguous joins. Standby had no RootFS reads. Captures
  stayed below 256MiB/90s, with no automatic stop or registry overflow.
- **Reject the proposed real-runtime explanation:** neither cohort showed
  cross-object concurrent content repetition. Cold source-attempt decoded
  coverage was 137375745 bytes, with 1884160 active-repeat bytes (~1.37%).
  This does not support implementing shifted-window joining as the current
  seconds-scale bottleneck fix. Keep independent small-demand progress.
- **Cache churn is now observed, not inferred from cache size:** exact event
  replay matches final cache snapshots; 199 cold and 520 cached-new eviction
  events. The complete 1818-entry cold-final cache exactly equals the next
  cohort's starting cache. Cached-new Coding first command has zero source
  attempts and takes 65.85–69.56ms. Node first command has 50 source attempts
  and takes 527.34–543.99ms; 134 unique units / 8200192 decoded bytes were
  resident at cohort start, subsequently evicted during that cohort, then
  requested again by the command. This identifies claim/command cache churn
  without proving a replacement policy or justifying another cache-size sweep.
- Cold same-sandbox NBD source/shared-wait interval unions overlap about
  1.116–1.123s of Node's 1.167–1.181s command interval and 1.032–1.046s of
  Coding's 1.106–1.124s. Cached Coding has no such wait, whereas cached Node
  has 0.403–0.438s. These are clipped concurrent NBD wait unions, not an
  application critical path, exact CPU time or additive latency components.
  Source metrics count bounded source.io attempts before encryption, not
  confirmed successful provider GETs, HTTP retries, delivered bytes or wire.
- Retained operational exceptions: one initial Nomad read timeout during
  boot; receipt recovery from the completed private DB operation (no replay);
  two SSH banner failures during the single requested reboot, confirmed by
  Aliyun terminal status as InstanceNotRunning; a successful read-only Cloud
  Assistant boot check; premature local parsing before SCP completion, which
  failed without emitting attribution and was repeated only after full byte
  and hash verification. No remote experiment was repeated to replace failures.
- All test sandboxes/writers/mounts/resources were cleaned and the original
  binaries, configurations and two-carrier job restored (index 66148). The
  original 263/2072-row authorities, source artifact databases, retained old
  failed import and source inventories were preserved. The new private DB
  `s0_mixed_trace_eghnwt` retains 2088 rows and remote-only private dump/configs.
  Four owned cloud terminal connections are terminal. Instance is restored
  to ecs.g9i.large (2CPU/8GiB), Stopped/StopCharging, verified 21:15:41 UTC.
- No production mutation, PR merge, tag or local e2e. Eight idle diagnostic
  carriers on g9i are not occupied production-type/width acceptance; the 1TiB
  Coding RootFS is sparse, not a populated 1TiB image. The baseline runsc is
  August 10, not production August 17 parity. Universal 1s/2s acceptance is
  still unproven. Next work must change a supported hypothesis: identify the
  necessary cold-read dependency/working set and distinguish required bytes
  from kernel speculation; use the observed cache churn to evaluate bounded
  policy changes, without repeating rejected knob sweeps.
- Evidence, raw traces, per-sandbox wait unions, failures and exact closure:
  `/tmp/sandbox0-mixed-read-trace.EGhNWT/evidence.json`,
  `attribution-summary.json`, `runtime-attribution.json` and `artifact-index.json`.

### D-READ-DEPENDENCIES — current read-chain diagnostic plan

- D-MIXED-READ-TRACE is verified progress: all 193 artifacts and the current
  1331-file candidate inventory were checked before this entry. No new remote
  test is admitted merely because the preceding entry suggests more tracing.
- Reuse the complete current-artifact NBD/source/flight traces. Determine
  whether a single NBD demand serializes independent data reads, or whether
  its dependency is mapping-then-data and the longer sequence arises between
  separate kernel/guest requests. This changes whether the already-rejected
  independent-worker family is worth reconsidering for the current layout.
- Resolve shared-flight leaders recursively, distinguish mapping and data,
  retain exact writer/API intervals, and refuse ambiguous ownership or cycles.
  A chronological sequence across separate NBD requests is not a proven
  application dependency; do not report it as a command critical path.
- This is local analysis only, no local e2e, node startup, tenant prewarm,
  timeout/policy/source change or new performance sample. Preserve earlier
  file/fault/readahead and HTTP first-byte findings without repeating them.
  Evidence root: `/tmp/sandbox0-read-dependencies.cJvoWu`.

### D-SHORT-DEMAND-LOCAL — bounded candidate admission

- The intervening user-feedback turn only restated status (no progress). This
  continuation rechecked HEAD, origin, dirty status and the same 1331-file
  baseline; no product source or remote state was changed.
- Current dependency analysis identifies 27 owned serial multi-data reads in
  the claim/first-command windows; all use physically adjacent spans in one
  object. Of these, 24 have demands below 128KiB and individual units at most
  64KiB. Three existing bulk-window cases are deliberately unchanged. The
  recorded shared-source fanout is not a counterfactual application speedup;
  physical adjacency alone does not prove same-leaf logical eligibility.
- Test one temporary Go-overlay candidate: merge only full authenticated units
  actually intersecting a short demand, using the already verified leaf and
  existing raw/encoded coalesced verification. No whole-window speculation,
  extra mapping fetch, worker, suffix reservation, cache expansion, source
  admission change, timeout increase or tenant prewarm. Tails, partial units,
  holes, object/physical/leaf boundaries and coarse legacy units stop batching.
- Unlike the rejected independent-worker/suffix family, this changes the
  number of same-request source operations, not thread concurrency. Keep
  single-unit progress and bulk canonical identities. Admission to remote
  testing requires local integrity/prefix/cancellation/sharing/bounds controls
  and a measured allocation/request-volume comparison with the exact baseline.
  This is local unit/benchmark work, not local e2e or an end-to-end SLO sample.
  Root: `/tmp/sandbox0-read-dependencies.cJvoWu` (`short-intent.json`).

### D-READ-DEPENDENCIES / D-SHORT-DEMAND-LOCAL — result and closure

- Exact shared-flight graph analysis and its four tests/nine assertions passed.
  The 27 owned serial multi-data requests are physically adjacent; 24 short
  candidates use at most64KiB units. Every cold candidate source chain has
  four observed dependent readers/sandboxes, not just its one producer. No
  application dependency is inferred between separate NBD requests.
- The distinct temporary batching overlay reduces single-reader two/three-unit
  source calls to one with identical requested bytes. Existing bulk windows,
  source admission8, cache128MiB, timeout10s and independent single-unit reads
  are unchanged. No product source, worker, prewarm or remote deployment change.
- Full `pkg/rootfsblock` race tests passed877 items (194 top-level), two skips:
  the sixteen-million-extent scale model and isolated RustFS integration.
  New49-item targeted checks cover shifted raw/mixed-encoded units, integrity,
  partial-error prefix and untouched suffix, cancellation, same-demand sharing,
  exact-small progress, tail/legacy/leaf/hole/physical boundaries and ownership.
  The first boundary fixture failed legacy-format validation; retained its
  original source/receipt and corrected the fixture, not the candidate code.
- **Do not adopt or remotely test the unchanged merge-only candidate.** In an
  eight-reader overlapping-envelope fixture, calls fall3->2 but raw source
  bytes rise196608->327680 (+66.67%); mixed encoded65566->131117 (+99.98%).
  Both sides reproduce across three deterministic race runs. Initial active
  source slots rise1->2. This is a synthetic concurrency counterexample, not
  a replay of production traffic or proof of a measured startup regression.
- Normal-cache allocation benchmarks (local arm64,100 iterations x3/cell)
  show raw two/three-unit allocation+82.72%/+80.78%, mixed+91.20%/+52.48%.
  Raw no-cache cases allocate less; single-unit cases stay essentially equal.
  Allocation is not retained RSS. Benchmark timing is not OSS RTT or startup;
  the first baseline benchmark overlapped a short race test.
- Cold Coding's identified candidate source intervals have wall-clock unions
  of10.646ms during claim and117.835ms during command; Node31.968/170.761ms.
  These exclude some dependencies and are not causal critical-path bounds or
  predicted savings. Do not claim this candidate closes the observed2s gap.
- Keep the prior1331-file product baseline. Reconsider only after demonstrating
  bounded allocation and evidence that the real read-path benefit justifies
  overlapping-demand duplicate content, without blocking independent small
  reads. Do not repeat this exact overlay or the rejected worker/suffix and
  cache/window/admission sweeps simply because request counts look better.
- `git diff --check` passes. No cloud API, remote claim/command, local e2e,
  production mutation, merge or tag this continuation. Remote state was not
  reobserved; its last verified stopped-state receipt remains in the previous
  experiment. The preceding cached-new cohort passed; cold combined and actual
  occupied production-width/production-runsc/populated-large-RootFS acceptance
  remain unfulfilled.
  Complete evidence: `/tmp/sandbox0-read-dependencies.cJvoWu/evidence.json`.

### D-COLD-MAP-PATH — current startup-stage mapping attribution

- Previous turn was progress: the64-artifact seal,173 history records and
  unchanged1331-file candidate were reverified. Do not repeat the rejected
  short-demand merge, independent-worker/suffix or parameter sweep variants.
- Reuse current mixed traces plus per-sandbox claim-phase records to determine
  where Coding's extra cold-claim time encounters mapping and data reads.
  Existing phase durations already show a distributed difference: RootFS
  preparation, runsc create/start and procd proof all differ from Node.
  The earlier XFS BPF result used a different16GiB artifact; it cannot prove
  current kernel attribution from a similar512-byte request sequence alone.
- Bound stage positions using client start/completion, measured planner/driver
  durations and residuals, and logger timestamp as an upper bound only.
  `CompleteSandboxClaim` runs after planner timing and before the log, so
  naively subtracting durations from that log would fabricate exact timestamps.
  Preserve inner/outer attribution and explicit clock-sensitivity assumptions.
- The older grouped-mapping prototype did remove metadata calls in one remote
  pair, but used a different indexed codec and an extra128MiB encoded cache.
  Do not silently port its representation/budget or claim its611ms result as
  a matched gain on the current artifact. No new runtime candidate is admitted
  by this plan; current evidence must identify the relevant dependency first.
- Offline analysis only: no new claim, command, local e2e, source/policy change,
  cloud mutation or production action. Root:
  `/tmp/sandbox0-cold-map-path.388Ogw`.

- Analysis now places at least3/3/7 distinct cold Coding mapping source attempts
  in runsc create/start/procd proof even with a10ms timing-margin sensitivity;
  the corresponding Node stages have none. This justifies a compatibility-only
  probe of existing v2 locators: nonadjacent child mapping pages packed as
  independent compressed ranges in one immutable object, read by the UNCHANGED
  Reader and incremental path copier. No grouping/read-ahead implementation,
  import policy activation, new format or latency claim follows from this probe.

### D-COLD-MAP-PATH — measured attribution and compatibility result

- Kept all1331 product files unchanged. Five bound-model tests/719 assertions
  passed; the16 current mixed samples reuse complete immutable traces, not
  new latency observations. Post-planner DB/log work is not silently included
  in planner time. Full artifacts retain0/1/10ms clock-margin sensitivity,
  driver/RPC residuals and conditional inner/outer stage windows.
- Coding's extra claim time is not just mounting. RootFS ensure is609-638ms,
  create278-281ms, start388ms, proof525-548ms; Node287-323/86-90/105/278-295ms.
  These marginal ranges must not be added as if they were one sample.
- With1ms clock margin, Coding create/start/proof have3/3/7 distinct mapping
  source attempts and about86/157-160/151-162ms of mapping-call wait unions.
  Corresponding Node phases have no mapping source attempt. All four Coding
  readers retain at least3/3/7 in the10ms-margin inner windows. Coding's ensure
  has6-8 mapping attempts and183-229ms mapping wait, versus Node3/97-104ms.
  This identifies metadata waits throughout launch, not pure runsc CPU cost.
- Cold first command has12 mapping attempts/341-357ms mapping wait for Coding
  versus2/37-39ms for Node. Cached-new has no mapping source attempt in these
  stages; data-flight waits still remain. Concurrent waits are not additive
  latency, a causal critical path, or a promised optimization speedup.
- The512-byte middle-device search lies inside RootFS ensure in the current
  analysis. Old BPF identified XFS log-head search on a different artifact;
  do not call that exact current-kernel proof or change log/recovery semantics.
- Existing v2 locators/codec passed a new compatibility-only fixture for
  nonadjacent child pages packed as independent raw/zstd ranges in one object.
  Unmodified Reader and incremental path copying preserve bytes/holes, exact
  offsets, parent binding, corruption isolation, unchanged packed children
  after root rebalancing, and readability of the old generation. Targeted
  fixture plus existing controls passed25 items (11 top-level) with race.
  Initial failed expectations about no-cache retention/root depth were
  corrected in a separately preserved v2 fixture, not in product code.
- **Next admission:** bounded metadata delivery in CURRENT v2, with canonical
  group identity, independent checksum/parent verification and existing shared
  cache/source limits. Generic import selection/provenance, inventory/GC,
  encrypted transport, differing-parent concurrency and complete-image costs
  must be verified before activation. Do not restore the old indexed codec
  or extra128MiB encoded cache, or treat its611ms single-lane result as a
  matched current gain. Current Reader still fetches each packed page exactly;
  compatibility alone provides no performance improvement.
- `git diff --check` passes. No cloud call, remote claim/command, local e2e,
  product deployment, production mutation, merge or tag. No remote state was
  reobserved. Full2s combined, populated-large-RootFS, occupied production-width
  and runsc-parity gates remain open. Evidence:
  `/tmp/sandbox0-cold-map-path.388Ogw/evidence.json`.

### D-MAPPING-GROUP-LOCAL — bounded current-v2 metadata delivery experiment

- The acknowledgement turn made no progress. Reverified the previous32-artifact
  seal,174 history entries and unchanged1331-file product inventory before acting.
- Test-only overlay: group physically contiguous same-object mapping siblings
  known from an authenticated v2 parent, including logically nonadjacent pages.
  Bound to1MiB stored/4MiB decoded/32 pages, sharing the existing128MiB cache and
  eight source slots through decoding. Canonical identity includes every locator;
  differing-parent subsets may duplicate content and must be measured explicitly.
- Keep ordinary root/data reads and caller-side parent validation. Each page is
  checksum-verified independently and gets an owned decoded buffer; no hidden
  retained group buffer, second cache or worker. Unusable geometry/insufficient
  cache falls back to exact demand. Transport/demand errors propagate, not retry.
- Test real-sized1024-entry metadata, shared/different parents, cache budgets,
  corruption, cancellation, admission and allocations before remote admission.
  This is not an XFS/populated-image benchmark or a claim/command latency sample.
  No production/publisher activation. Root:
  `/tmp/sandbox0-mapping-group.eaH6w2`.

- Initial local unit/race checks passed, but both AES-GCM and ChaCha20 encrypted
  probes exposed a real candidate regression: a corrupt OPTIONAL trailing frame
  makes the grouped source read fail even when the demanded first page is healthy.
  The exact baseline passes. Preserve the failed v1 overlay/receipts; v2 adds one
  exact-demand fallback after releasing a failed group transport's source slot.
  Cancellation/deadline does not fallback. This does not retry claim/command POSTs.
  Healthy grouped transport reduced provider GETs4->1 and requested ciphertext
  ranges214276->165064 bytes in this1024-entry-page fixture; not startup evidence.

### D-MAPPING-GROUP-LOCAL — result and next admission

- Preserved all1331 product files; candidate remains a temporary current-v2
  Reader overlay. Same-object contiguous groups are bounded1MiB stored/4MiB
  decoded/32 pages, within the existing128MiB cache/eight shared source slots.
  No old indexed codec, extra encoded cache, background worker or tenant prewarm.
- Four1024-entry pages, all demanded: source calls4->1 without extra stored
  bytes; eight same-parent readers share source/decode work. Real AES-GCM and
  ChaCha20 wrappers with current16KiB frames reduce healthy provider calls4->1
  and requested cipher ranges214276->165064 bytes (22.97% less). These are
  local metadata fixtures, NOT remote latency or claim/command measurements.
- Corrected a genuine encrypted-neighbor isolation regression in separately
  retained v2: group transport failure releases its slot before one exact demand;
  cancellation/deadline does not fallback, demanded corruption still fails.
  Initial v1 failures for both algorithms remain recorded, not overwritten.
- Three targeted race repetitions pass. Full rootfsblock race suite passes873
  items/198 top-level tests with2 explicitly large-scale tests skipped. The
  64-independent-reader fixture admits8 loads and queues56, then drains all;
  hot cache hits and independent small data reads preserve progress. This is
  source-admission validation, not occupied production-width startup acceptance.
- Three100-iteration allocation repetitions per cell: compressed whole-group
  sequential allocations fall9.08%, same-parent eight-reader allocations7.83%.
  Raw equivalents increase28.92%/31.47%. Single-page demand reads roughly4x
  bytes and allocates5.11x raw/3.61x compressed. Different parent subsets still
  make2 calls but read3x raw/2.993x encoded bytes; allocations rise245.59%/139.95%.
  These costs are real constraints, not omitted outliers. Small/disabled cache
  falls back to exact demand. Local arm64 ns/op is not a remote speed claim.
- **Decision:** retain repaired Reader only as a feasibility candidate. Current
  publisher still stores separate mapping objects. Next implement/evaluate
  bounded generic streaming group publication and immutable import provenance,
  complete-image demand coverage and demand-only decoded-page population; verify
  physical inventory/GC/snapshot/fork semantics before remote admission. Do not
  deploy unchanged eager-all sibling grouping or repeat rejected tuning sweeps.
- No cloud calls, remote claims/commands, local e2e, production mutation, merge
  or tag. Public timeout remains10s. Remote state was not reobserved. Cold2s
  combined, populated-large-RootFS, occupied production width and runsc-parity
  gates remain open. Full evidence:
  `/tmp/sandbox0-mapping-group.eaH6w2/evidence.json`.

### D-MAPPING-PUBLISH-LOCAL — demand decode and streaming publication

- Previous turn was progress; reverified its74-artifact seal,175 history records
  and unchanged1331-file product inventory before acting.
- Temporary overlay will reuse current streaming publisher, not hand-author
  benchmark-only packed children. Opt-in contiguous-mapping-v1 groups are bounded
  to4 pages/240KiB stored/1MiB decoded and preserve empty-policy publication.
  The stored bound leaves frame headroom under the current256KiB crypto prefix.
- Replace eager decoded MappingPage population with demand-only parsing of shared
  verified mapping payloads, retaining existing source/cache budgets and v2 exact
  fallback for transport errors. Measure partial/subset cases, not just all pages.
- Validate real publisher output, per-level frontier/pending bounds, physical
  object inventory, failures/cancellation and incremental old-generation reads.
  BuildOptions enters operation hashing, but current DB persists flattened option
  fields and attestation lacks grouping provenance; do not enable this policy
  before those integration gaps are addressed. No production activation here.
  Root: `/tmp/sandbox0-mapping-publish.PBECYC`.

### D-MAPPING-PUBLISH-LOCAL — result and durable integration boundary

- Product inventory stays1331 files. Implemented a temporary demand-only mapping
  parser and an opt-in contiguous-mapping-v1 policy in the actual streaming
  materialized-generation publisher. No benchmark-command-specific selection,
  extra cache/worker, tenant prewarm or production activation.
- The publisher bounds pending groups per level to4 pages/240KiB stored/1MiB
  decoded. Roots/final partial parents and oversized pages may stay individual.
  Tests cover fanouts2/3/4/7/31/1024, stored/decoded bounds, sparse/full equivalence,
  deterministic retry, failure/cancellation, whole-object checksums/inventory,
  exact reachable offsets and incremental preservation of old grouped objects.
  All72 empty-policy golden descriptor/reference identities stay unchanged.
- Actual publisher output for a generated20975616-byte block image, through
  AES-GCM/current16KiB frames and the Reader, reduced mapping provider GETs7->3
  and requested cipher ranges204016->134304 bytes (34.17% less). Full data readback
  matches and both sides parse7 demanded pages. This is a generated block image,
  NOT a normal OCI/XFS import, populated-large RootFS or startup measurement.
- Optional mapping bytes are verified but only demanded pages are parsed and
  protected as mappings. Unparsed cached pages queue for bounded parsing without
  new I/O; parsed hot pages bypass saturation.64 distinct grouped readers retain
  the8-source limit and parse128 roots/demanded pages instead of192 eager pages.
- Three100-iteration local arm64 repetitions: compressed single-page allocations
  fall47.64% versus eager grouping; different-parent subset allocations38.18%.
  They STILL exceed exact legacy by88.97%/48.34%, respectively. Source bytes are
  unchanged from eager grouping (roughly4x/3x exact demand). Raw variants improve
  parsing cost too, but retain larger allocation overhead. Full-group compressed
  allocations remain8.55% below legacy sequentially and7.78% below at8readers.
  Unparsed optional payloads occupy ordinary LRU space and may be evicted under
  real mixed-image pressure; that tradeoff needs remote validation.
- Full race suites pass896 rootfsblock items/206 top-level and75 importer items/
  33 top-level. Two explicit scale and three privileged tests skip; none fail.
  Actual encrypted publisher delivery also passes its separate3-item probe.
- **Next admission:** canonical hashing now binds the optional build policy, but
  current PostgreSQL INSERT/select/scanner omit it and whole-spec readback would
  conflict. ReadyArtifact attestation/selection lacks the provenance as well.
  Wire schema migration, old pending-operation preservation, fenced worker and
  versioned attestation/Ready selection before enabling policy. Validate policy
  scope for shared incremental/batch BuildOptions, physical GC and OCI/XFS import.
  Detailed source map: `/tmp/sandbox0-mapping-publish.PBECYC/integration-next.md`.
- No cloud/remote calls, claim/command samples, local e2e, production mutations,
  merge or tag. Public timeout remains10s and remote state was not reobserved.
  Real cold/cached-new `claim + node -v`, occupied production width, populated
  large RootFS, production runsc parity and the2s combined gate remain open.
  Evidence: `/tmp/sandbox0-mapping-publish.PBECYC/evidence.json`.

### D-MAPPING-DURABLE — durable integration plan

- Previous turn was progress; reverified its60-artifact seal,176 history entries
  and unchanged1331-file product inventory before code edits.
- Integrate the tested demand-only Reader/streaming publisher into this worktree.
  Add persisted mapping policy, versioned Ready proof and source-selection filter;
  preserve empty-policy old operations/attestations and fence mismatched old workers.
  The immutable policy remains an import input, not runtime compatibility.
- New SQL migration must cooperate with existing layout-proof triggers, protect
  policy-bearing rows on rollback and be checked in an isolated test database.
  Shared incremental/batch options must reject unsupported policy use explicitly.
- Keep configuration opt-in and disabled. No production rollout/merge/tag and no
  changed public timeout or tenant prewarm. Local unit/race checks first; remote
  schema/importer validation requires fresh CLI/environment checks. Root:
  `/tmp/sandbox0-mapping-durable.v3D98r`.

### D-MAPPING-DURABLE — integration and isolated database result

- Integrated the sealed demand-only Reader and bounded streaming publisher into
  this worktree. Persisted `contiguous-mapping-v1` in durable import options,
  threaded it through worker/discovery/claim selection, and added version-3 Ready
  proofs and migration55. Empty-policy identities remain unchanged; the setting
  defaults off and is not part of generic carrier compatibility.
- PostgreSQL rejects mismatched old-worker Ready publication, policy/proof
  mutation and rollback with policy-bearing rows. Version3 can coexist with the
  file-relative data-layout policy. Incremental/batch and trusted mutation builds
  reject this import-only option; digest-bound existing sources are unaffected.
- Final local unit and service race suites passed. Full rootfsblock race passed
  900 items /208 top-level tests, with2 explicitly gated tests skipped. The
  independent Nomad driver module passed69 items /50 top-level tests; privileged
  corpus checks were skipped. Dependent runtime packages compiled. Final tested
  product inventory is1353 files and remained unchanged across verification.
- Fresh remote DB `s0_mapping_durable_v3d98r` (OID302911) ran18 top-level tests
  with no skips: mapping/layout/geometry publication and migrations, old-worker
  fences, lease/CAS/GC, initial generation, snapshot and fork. These encoded
  descriptor fixtures do not prove normal OCI/XFS grouped artifact lifecycle.
- Preserved failures: obsolete26-column assertion; new test fixture missing size
  and wrong field; full race interrupted at180s total budget before passing in
  183.306s with600s test budget. Product request timeout remains10s. Slow CLI
  tunnel upload was intentionally stopped, its partial retained, then identical
  binary uploaded through verified direct SSH in3.953s. No test was resubmitted
  because an observation timed out.
- Original remote binaries/config and historical sandbox row counts
  263/2072/2088 were unchanged. Owned CLI connection terminated; ECS restored to
  original2CPU `ecs.g9i.large`, Stopped/StopCharging. The private DB, receipts and
  partial upload remain for audit; no production rollout, merge or tag occurred.
- No new claim/command samples or latency win claimed. Partial-group byte
  amplification and ordinary-LRU eviction still require real-workload checking.
  Next: normal OCI/XFS grouped import and encrypted inventory/lifecycle, then
  fresh-node vs cached-new identity claim plus immediate `node -v`, populated
  large RootFS and actual occupied production width with matching runsc.
  The regional-ingress2s combined gate remains open. Evidence:
  `/tmp/sandbox0-mapping-durable.v3D98r/evidence.json`.

### D-MAPPING-OCI — real immutable artifact plan

- Previous goal turn was progress. Reverified its70-artifact seal and unchanged
  1353-file candidate. Production architecture remains based on main; no rollout.
- Import the same digest-pinned Node22 and Coding sources through the normal
  durable worker, OCI unpacker, XFS builder and encrypted OSS publisher. Add only
  `contiguous-mapping-v1` to the existing file-relative format2 import policy.
- Verify committed pre-PUT journal, applied Ready proof, authenticated object
  inventory and complete mapping locators before attempting cold claim/command.
  Use an isolated new DB/prefix/work directory and preserve all prior data.
- Keep2CPU for artifact preparation; this is not a production-width benchmark.
  The existing1TiB Coding case is sparse (~5GiB populated), not a populated-large
  result. No tenant RootFS prewarm or request timeout change. Root:
  `/tmp/sandbox0-mapping-oci.UQO1aV`.

### D-MAPPING-OCI — real encrypted artifact result

- Both pinned OCI sources completed one normal durable import each with
  `xfs-file-ranges-v1` plus `contiguous-mapping-v1`, no fallback and version-3 Ready
  proofs. The importer/XFS/publisher product code is unchanged from the1353-file
  candidate sealed in D-MAPPING-DURABLE. No request timeout or tenant prewarm.
- Node22:6 mapping pages /3 mapping objects instead of6;8 total objects,
  99,617,955 plaintext bytes. Ready artifact:
  `sha256:2b5208c97335f4e25312150a1d980ed6039efe1884139129efd79f00a7f1b155`.
- Coding:80 mapping pages /21 mapping objects instead of80;97 total objects,
  1,709,513,457 plaintext bytes. Ready artifact:
  `sha256:21a2f3bac82e85bf353d9d625165c79c9d915f5c107fe2f36c349cc84e806ceb`.
  Its1TiB logical disk contains5,471,080,448 allocated bytes, not1TiB populated.
- Every PUT had committed journal intent. All105 physical objects were fetched,
  authenticated/decrypted and SHA-256 checked. Production Reader traversal
  verified the full86-page mapping forest and exact physical locator coverage
  for4,341 /80,781 data extents, without orphan bytes, gaps or overlaps. Node has
  one4-page group; Coding has twenty groups, max4pages/object. Grouped mapping
  objects stayed below177,145 stored bytes in these real artifacts.
- Whole-tree traversal used3 /21 provider GETs, but crypto headers were already
  cached by the preceding object audit. This is not a fresh-node/startup count.
  Normal XFS rebuild metadata and object prefixes differ, so import durations
  (13.874 /313.078s) and small payload-size changes are not causal latency wins.
- Existing DB artifacts/rows, runtime binaries/config and retained trial images
  were preserved. Both operation leases released cleanly, no import retries or
  garbage deletions. Own XFS scratch files/mounts are gone; retained disk unmounted.
  New DB `s0_mapping_oci_uqo1av`, immutable objects and evidence remain for reuse.
- Built manager/ctld/driver without diagnostic overlays and staged exact hashes
  under the owned remote root; not installed. Initial helper failures (NVMe
  enumeration guard, Ruby replacement escaping, wrong driver package path and
  JSON field case) are retained. Slow old-SSH-master transfer was interrupted
  only after observing its queue/window and partial file, retained separately,
  then completed on a fresh connection; final complete hashes passed.
- Coding completed at23:51:35UTC; the next goal continuation observed it at
  03:21:48UTC. That observation gap is not import/runtime latency and did not
  trigger a replay. Both SSH masters closed; ECS stopped/StopCharging on its
  original2CPU shape at03:32:02UTC. No production mutation, merge or tag.
- No new claim or guest-command sample. Next use these exact Ready artifacts and
  staged binaries for matched fresh-node/cached-new regional-ingress `claim +
  node -v`; do not reimport identical inputs. Grouped artifact lifecycle, occupied
  production width, populated-large RootFS and stock runsc parity remain open.
  Evidence: `/tmp/sandbox0-mapping-oci.UQO1aV/evidence.json`.

## D-MAPPING-COLD — cold Node improved; cold Coding combined still over2s

- Previous conversation acknowledgement added no evidence. Revalidated all1353
  product files against the pure candidate inventory before continuing.
- Reuse both D-MAPPING-OCI Ready artifacts and its three staged binaries; no
  imports repeated. Clone source `s0_mapping_oci_uqo1av` into isolated
  `s0_mapping_cold_gx3kuv`, preserve original services and source databases.
- Match B-LAYOUT-MIXED:16CPU/64GiB test node,8 concurrent generic carriers,
  four Node and four Coding claims after reboot, then8 cached-node new IDs.
  Keep10s request budget and immediately execute `node -v`. Record trusted
  regional claim timing, full client claim wall, command-only and combined wall.
- Keep baseline stock runsc release-20260810.0 to isolate this candidate.
  Infra main pins release-20260817.0; production parity remains a separate gate.
  Eight CPU-lease-wide claims are not occupied production memory density;
  sparse1TiB Coding has about5GiB allocated, not1TiB populated.
- Plan and receipts: `/tmp/sandbox0-mapping-cold.GX3kuV`. Restore original
  binaries/config/job,2CPU shape and StopCharging after measurement.

Outcome (2026-09-11):

| Cache regime / image | Max regional claim | Max first `node -v` only | Max claim-start to command-complete |
| --- | ---: | ---: | ---: |
| Fresh-node cold / Node22 | 0.799s | 0.961s | 1.767s |
| Fresh-node cold / Coding | 1.746s | 1.073s | 2.825s |
| Cached-node new ID / Node22 | 0.553s | 0.422s | 0.954s |
| Cached-node new ID / Coding | 0.615s | 0.058s | 0.674s |

- Each row contains4 samples;16 unique claims,16 exact `v22.23.2` successes,
  zero claim/command retries. Maxima are marginal and must not be added.
  Client claim wall maxima:0.813/1.760/0.556/0.618s respectively.
- Every observed claim was under2s; four cold Coding combined samples were
  over2s. This small cohort is not a hard bound or production acceptance.
- B-LAYOUT-MIXED cold combined maxima were2.523s Node /3.156s Coding. Current
  observations improve about30.0% /10.5%; not randomized causal proof. Both
  tests use the same width/runsc/harness contract, but rebuilt XFS metadata and
  object prefixes differ. No new product change or import in this experiment.
- Actual new boot, zero pre-claim ctld TCP443 outgoing packets and zero NBD
  I/O verified no node target prewarming. Concurrent cold lanes may share
  in-flight immutable loads. Both exact new grouped RootFS digests were proved
  against actual writer/resource bindings;8 leases consumed14,000m CPU.
- Boundary is verified TLS into the real regional gateway using a private
  hosts override to127.0.0.1 on the test node. Gateway/manager/ctld/procd are
  included; public Internet/NLB and production cross-host networking are not.
  This is not a narrower ctld-only timer, but not a production ingress result.
- Coding cold claim:RootFS ensure611–637ms; stock runsc create251–254ms and
  start250–251ms; procd probe544–553ms. Metadata and lease acquisition are
  milliseconds. First command adds1065–1073ms. Ranges are not additive; exact
  per-sandbox phase records are retained. All sampled lease CPU throttling
  counters were zero, which does not rule out ctld/host CPU contention.
- ctld RSS peak451,399,680bytes versus919,556,096 in the old matched run;
  still128MiB read cache,8 source admission,128KiB NBD max sectors and4MiB
  read-ahead. This is an observation, not total runtime memory or a density win.
- Cleanup proved no active leases/RootFS/runsc/cgroups/NBD attachments,8 ready
  replacements, no imports. Restored original binaries/configs and two-carrier
  job (index66843), source DBs unchanged. Retained isolated DB has2088 rows.
  ECS is Stopped/StopCharging on original2CPU/8GiB at03:59:16UTC.
- Recorded helper issues:reconnect failures during the same reboot, early
  finalization observing still-running cleanup, oversized tool-output export,
  and analysis before download completion. No workload or mutation replay;
  explicit artifact download then full size/SHA verification resolved export.
- Next correlate this exact Coding artifact's mounted-XFS demand with encrypted
  header/mapping/data reads and CPU/wait during RootFS ensure, runsc, procd probe
  and `node -v`. Do not repeat cache/read-ahead/fanout changes without evidence.
  Production stock-runsc parity, occupied width/reclaim, populated-large images
  and grouped artifact pause/resume/snapshot/fork remain unverified.
- Evidence: `/tmp/sandbox0-mapping-cold.GX3kuV/evidence.json`; raw measurements,
  per-claim logs, analysis and checksum manifest are retained alongside it.

### D-MAPPING-TRACE — attribute the unchanged grouped-artifact candidate

Plan (2026-09-11):

- Keep D-MAPPING-COLD's 1,353 product files, artifacts, stock runsc, 128MiB
  cache, eight source admissions, 128KiB NBD requests and 4MiB read-ahead.
  No public timeout change, tenant RootFS prewarming or new image import.
- Add temporary Go overlay instrumentation only to ctld. Trace mapping-group
  leaders/waiters, immutable range/cache identities, encrypted header/provider
  reads and scheduler states. Never log keys, credentials or payload bytes.
- Two eight-wide mixed cohorts on the isolated 16CPU/64GiB test machine:
  fresh reboot, then cached-node new sandbox identities; immediate `node -v`.
  Include authenticated regional gateway timing but disclose the private
  loopback hosts binding. This is diagnostic, not production SLO acceptance.
- Trace bounded to 90s/256MiB per process/cohort; require 8GiB disk available,
  authenticated writer/branch identity joins, complete traces and verified
  artifact downloads. Preserve partial failures; never replay claim POSTs.
- Restore original services/binaries/configs/two-carrier job; stop compute
  and restore the original 2CPU/8GiB shape after evidence collection.
- Working evidence: `/tmp/sandbox0-mapping-trace.FqwUIZ/plan.json`.

Outcome (2026-09-11; diagnostic, not a performance acceptance run):

| Cache regime / image | Max regional claim | Max first `node -v` only | Max claim-start to command-complete |
| --- | ---: | ---: | ---: |
| Fresh-node cold / Node22 | 0.879s | 1.138s | 2.017s |
| Fresh-node cold / Coding | 1.662s | 1.008s | 2.666s |
| Cached-node new ID / Node22 | 0.554s | 0.284s | 0.823s |
| Cached-node new ID / Coding | 0.716s | 0.578s | 1.281s |

- Four samples per row,16 distinct identities and16 exact successful node
  commands; no claim/command retry. Marginal maxima are not additive.
  All claims were below2s, but this small instrumented sample is not a hard
  guarantee. D-MAPPING-COLD remains the observer-free comparison; changing
  completion order and trace overhead preclude causal timing comparisons.
- Four complete bounded traces,25,115,483bytes, with no overflow or automatic
  stop. Go clock calibration drift stayed within11.602us. Primary traces
  contain7,187/7,186 NBD reads and3,926/1,418 uniquely matched shared-flight
  waiter edges. Standby performed no RootFS reads.
- Coding cold claim still spends605–644ms in RootFS ensure,227–231ms in runsc
  create,219–220ms in runsc start, and498–520ms in procd probe. These are
  per-phase ranges, not additive maxima; correlated raw samples are retained.
- Same-sandbox source/shared/cipher wait interval unions cover1.344–1.365s
  during cold Coding claim and0.928–0.944s during its first command. Source
  admission wait is at most12.6ms/1.2ms in those windows. These unions are
  concurrent NBD waits, not an exact process critical path or summed CPU time.
- Cold observed430 underlying provider calls across57 objects (p50 acquisition
  13.4ms,p95 41.1ms,max116ms); cached-new observed214 calls across35 objects.
  Provider-get ends when the body is acquired, not after complete delivery;
  calls do not count SDK HTTP retries or prove successful bytes on the wire.
- Cached-new had zero mapping-object and zero header-loading provider calls.
  Immutable mapping/header caching is working. Cold57 key unwrap regions
  accumulated277.938ms in Go Running state; frame AEAD27.549ms. These are
  scheduler wall-state observations, not exact CPU samples. Initial header
  loads can include bounded payload prefixes; do not call all their bytes
  pure header overhead.
- Cache replay exactly matches final snapshots:244 cold and769 cached-new
  eviction events. Coding's cached-new command reloaded208 distinct units
  (12,427,264 decoded bytes,about11.85MiB) that were resident at cohort start
  and evicted before the source attempt. Node insertions triggered58 of those
  victims (3,596,288bytes); Coding insertions triggered150. This demonstrates
  within-cohort cache churn, not a causal explanation of another run's timing.
- The last-eviction trigger was a coalesced read for169 of those208 units.
  That does not yet prove the inserted neighbors were unnecessary: classify
  demand versus later reuse before choosing a bounded admission/retention
  policy. Do not infer an optimal larger cache or repeat512MiB blindly.
- No active cross-object duplicate-content source load was observed. Cold
  source coverage144,511,406 decoded bytes includes mapping and speculative
  neighbors; it is not minimum application demand. ctld RSS peak443,281,408
  bytes is diagnostic-process RSS, not total sandbox density cost.
- All1,353 product files stayed identical. Temporary ctld instrumentation
  used Go overlays only; the encrypted header/mapping/data behavior and
  security contracts were unchanged. No image import or prewarm occurred.
- Original binaries/configs/two-carrier job restored (index67254), all leases,
  runtime processes, mounts and64 NBD devices empty; source databases unchanged.
  Isolated test DB retained2088rows. ECS restored to2CPU/8GiB and verified
  Stopped/StopCharging at04:36:52UTC. No production rollout,merge or tag.
- Preparation output exceeded the tool display budget; the complete remote
  file was retained and downloaded with size/SHA verification. SSH reconnect
  failures belonged to one graceful reboot. No mutation or benchmark replay.
- Remaining acceptance gaps:production runsc/network and current-main parity,
  occupied density/reclaim, populated-large images, repeated observer-free
  cold runs, and grouped-artifact storage lifecycle behavior. The Coding
  fixture is sparse1TiB logical,not populated1TiB; ingress uses private loopback.
- Evidence: `/tmp/sandbox0-mapping-trace.FqwUIZ/evidence.json`; exact cache
  eviction attribution: `/tmp/sandbox0-mapping-trace.FqwUIZ/cache-cause.json`.

### D-DATA-REUSE — demand recency, not speculative window recency

Plan and result (2026-09-11; local candidate, no new runtime latency result):

- Previous turn was progress. Revalidated all164 sealed D-MAPPING-TRACE
  artifacts and all1,353 baseline product files before editing. Main refs remain
  sandbox0 d3541bd and sandbox0-infra 0df7cde; this work stays on the existing
  historical diagnosis candidate and does not establish current-main parity.
- Replayed coalesced window members against the exact same-artifact logical
  NBD request intervals and cache insertion/eviction events. Cold Node/Coding
  fetched coverage contains12,562,944/5,986,816 decoded bytes with no matching
  later same-artifact NBD request by cohort end. These are coverage sums, not
  unique live memory, minimum application demand or globally unused bytes:
  kernel read-ahead, dirty-branch reads and unknown cross-artifact aliases limit
  this attribution. Much other neighboring data was already requested by
  concurrent readers, so disabling all coalescing is not justified.
- Found a narrower concrete recency issue: window inspection used `cache.get`
  for every neighbor, falsely promoting cached edges/interiors. Publication
  also refreshed independently filled speculative neighbors. Conversely, a
  caller consuming multiple entries from one retained span did not mark all
  of those actual entries as recent.
- One independent variable: recency follows the recipient's actual bounded
  read demand, not incidental whole-window inspection. Added non-touching
  immutable peeks, recency-neutral racing coalesced publication, and one locked
  demand-only promotion pass per shared-flight recipient. Raw and encoded
  coalescing use the same semantics. No checksum, parent binding, encryption,
  corruption fallback, resource lease, data layout or public API change.
- No added cache queue/tier/budget, source worker, readahead tuning, timeout
  increase or tenant RootFS prewarm. Newly verified entries still have the
  existing LRU admission policy; this is not a complete scan-resistant cache.
  Cache128MiB, mapping protection16MiB, window1MiB and source admission8 remain.
- Added four regression tests, each in raw and mixed encoded/raw form. They
  cover demand-span recency, non-touching cached neighbors with bounded
  eviction, a concurrent publication race, and different shared-flight
  recipients. All8 subcases fail semantically against three reconstructed
  baseline files whose SHA256 exactly matches the old product inventory;
  there is no baseline compile failure. Candidate tests pass.
- Full `pkg/rootfsblock` unit suite passed. Relevant race tests repeated5 times
  passed. `pkg/objectstore`, `pkg/rootfsobjectstore`, `pkg/rootfssession`,
  `pkg/rootfsimporter`, `pkg/nomadruntime`, and the independent Nomad driver
  module tests passed. No local e2e or privileged runtime acceptance was run.
- Existing deterministic18-reader pressure test has identical request counts,
  encoded/decoded coverage and accounted cache bytes before/after. The sparse
  bulk pattern still issues144 GETs in each round and covers150,994,944 decoded
  bytes. Dense/small fitting working sets still issue zero second-round GETs.
  Test durations ran concurrently and are not a CPU/latency comparison. This
  candidate has proven recency semantics, not proven startup speedup.
- Six product paths changed relative to the previous inventory: three Reader
  files, one new regression test, RootFS package README and self-hosted
  observability docs. Other1,348 product paths preserved; total1,354 files.
  `candidate.source.json` stores a hash-verified base-plus-delta inventory.
- Aliyun CLI freshly confirmed the isolated ECS remains Stopped/StopCharging
  at2CPU/8GiB. No test machine start, remote claim, deployment, production
  mutation, merge or tag in this experiment.
- Next admission: build a ctld-only candidate and run observer-free matched
  fresh-node/cached-new, eight-wide mixed claim plus `node -v`, including
  reversed order against the retained baseline. Do not grow this into a new
  cache tier or repeat larger-cache tuning without measured benefit; revert
  if the extra recency work regresses latency without demonstrated benefit.
- Evidence: `/tmp/sandbox0-data-reuse.gpCrvx/evidence.json`; exact baseline
  overlay, test receipts and replay details are retained beside it. The full
  production cold/occupied/populated-size/lifecycle goal remains incomplete.

### B-RECENCY-ABBA — non-instrumented recency candidate comparison

Initial plan (2026-09-11; completed results below):

- Prior D-DATA-REUSE was progress. Revalidated its21 sealed artifacts and the
  1,354-file candidate inventory before building; no additional product edit.
- Single variable is ctld consumer-demand cache recency. A uses retained pure
  ctld a4b260df; B uses newly built, non-instrumented ctld e32a7c8b. Manager,
  driver, stock runsc release-20260810.0, UTC procd and both exact grouped
  artifacts stay fixed. No import or template prewarming.
- Order A1/B1/B2/A2. Each arm uses an independent DB clone of the same Ready
  source and a fresh node boot, then two8-wide mixed cohorts with distinct
  identities and immediate `node -v`. Total64 claims/commands if all arms pass.
- Keep128MiB cache,1MiB coalescing, source admission8,128KiB NBD cap,4MiB
  read-ahead and public10s timeout. Verify cold zero-object/NBD counters,
  exact executable hashes, actual leases and cleanup before accepting samples.
- Isolated16CPU/64GiB host only. Regional TLS uses the existing private
  loopback hosts binding; no production NLB/cross-host or occupied-memory
  acceptance claim. Four arms are not sufficient to establish a hard bound.
- Preserve all partial results, no POST replay on observer timeout. Restore
  original binaries/configs/two-carrier job between arms; preserve databases
  and artifacts. Finally restore2CPU/8GiB and Stopped/StopCharging.
- Working evidence: `/tmp/sandbox0-recency-ab.XKeNPF/plan.json`.

Result (2026-09-11; ABBA complete, recency-only candidate reverted):

- Four fresh boots, four isolated clones,64 distinct sandbox identities and64
  successful immediate `node -v` commands. All64 regional claim measurements
  were below2s; 16 combined claim-plus-command samples exceeded2s.
  No claim/command replay, new import, user RootFS prewarm or timeout increase.
- Each arm checked zero pre-claim object traffic and NBD counters, exact
  process hashes, source artifact digests, physical CPU/memory leases, and
  terminal cleanup. All1,354 candidate files stayed exact through testing.
  After the no-win decision, the six recency paths were archived and reverted;
  every one of1,353 final product files matches the prior baseline inventory.
  Unrelated changes are preserved; RootFS unit and focused race tests passed.
- The binary was not instrumented with Go tracing. Identical external memory,
  block-I/O and lifecycle observers were present in every arm; observer-free
  in the original plan meant no diagnostic binary, not zero measurement overhead.

Per-group maxima in seconds; claim is the regional ingress readiness timer.
Command-only excludes claim; combined uses the actual claim-start-to-command-end
timer. Marginal maxima need not belong to the same individual sample.

| Cache regime | Image | Arm | Regional claim | Command only | Combined |
|---|---|---|---:|---:|---:|
| Cold | Coding | A1 | 1.546721 | 1.004596 | 2.556174 |
| Cold | Coding | B1 | 1.673104 | 0.978821 | 2.649172 |
| Cold | Coding | B2 | 1.597729 | 1.064215 | 2.664994 |
| Cold | Coding | A2 | 1.578674 | 0.936118 | 2.508443 |
| Cold | Node22 | A1 | 0.827446 | 0.998096 | 1.834343 |
| Cold | Node22 | B1 | 0.873425 | 1.053358 | 1.938606 |
| Cold | Node22 | B2 | 0.834990 | 1.150009 | 1.986235 |
| Cold | Node22 | A2 | 0.890468 | 1.033084 | 1.920630 |
| Cached-new | Coding | A1 | 0.774584 | 0.758669 | 1.527622 |
| Cached-new | Coding | B1 | 0.758524 | 0.607058 | 1.367629 |
| Cached-new | Coding | B2 | 0.831009 | 0.740571 | 1.562505 |
| Cached-new | Coding | A2 | 0.696386 | 0.632639 | 1.315547 |
| Cached-new | Node22 | A1 | 0.582278 | 0.489018 | 1.049282 |
| Cached-new | Node22 | B1 | 0.565123 | 0.413027 | 0.960346 |
| Cached-new | Node22 | B2 | 0.668304 | 0.351181 | 1.007435 |
| Cached-new | Node22 | A2 | 0.517654 | 0.401539 | 0.898873 |

- Decision: The recency-only candidate did not establish a startup benefit. Fresh-cold combined maxima were higher in both paired rounds for both images (Coding +3.64%/+6.24%, Node22 +5.68%/+3.42%); cached-new combined results improved in the first pair and regressed in the second. Two correlated paired rounds do not establish a statistical or causal bound, but do not justify promoting this added cache policy. All64 claims and commands succeeded; all claims were below2s, while16 cold Coding combined results exceeded2s.
- Disposition: Reverted only the six D-DATA-REUSE product paths to the exact1,353-file prior baseline. Preserved previous mapping/header/source-read improvements and all unrelated work. Archived the six candidate files, including its removed regression test, plus both binaries and full measurements; no production rollout or merge.
- Next: Do not repeat the recency-only or larger-cache sweep. Retain the measured cold RootFS and first-command source/shared-wait diagnosis; require a demonstrated reduction in demand-path remote bytes or waits before another implementation. Production/current-main parity, occupied density, populated-large images and storage lifecycle remain separate acceptance gaps. No extra cache tier, increased timeout or tenant RootFS prewarm is justified by this result.
- Limitations: two paired rounds only; synchronized lanes are correlated.
  Private-loopback regional TLS is not public NLB/cross-host acceptance;
  width8 on16CPU/64GiB is not occupied-memory production-width acceptance.
  Coding is sparse1TiB logical with about5GiB allocated, not populated1TiB.
  Stock runsc20260810 and the historical candidate do not prove production
  runsc20260817/current-main parity or grouped-artifact lifecycle acceptance.
- Transport: the new public IP was unreachable directly. Existing local
  network proxy restored SSH while keeping host-key verification. B2 install
  lost its SSH receipt after remote completion; read-only receipt/hash checks
  proved success and zero claims before continuing. A later B2 observation
  disconnect did not interrupt the detached remote harness. Neither action
  nor benchmark was replayed. Both transport failures remain in the record.
- All four arms restored original binaries/configs/two-carrier job and proved
  no active leases/branches/NBD attachments. Test databases and artifacts
  remain preserved. Final Aliyun state is2CPU/8GiB,Stopped/StopCharging.
- The cold-start goal remains incomplete. No production mutation, merge or tag.
- Evidence: `/tmp/sandbox0-recency-ab.XKeNPF/evidence.json`; detailed paired
  deltas: `/tmp/sandbox0-recency-ab.XKeNPF/comparison.json`.

### D-CIPHER-OVERLAP — request-range concurrency audit

Plan (2026-09-11; diagnostic only):

- Previous ABBA turn was progress: recency-only candidate rejected/reverted,
  all64 samples and exact restoration retained. Revalidate307 sealed artifacts
  and the1353-file baseline before any next implementation.
- Reuse the grouped-artifact trace to quantify same-object ciphertext request
  overlap, separating concurrent acquisition from serial reloads. Decoded
  source singleflight does not itself prove absence of frame-range overlap.
- Provider-get end is response acquisition, not complete body transfer. Keep
  guaranteed acquisition overlap separate from any conservative containing
  source/header/parallel-take upper bound; do not infer wire savings or latency.
- No product edit, new remote claim, prewarming, cache tier, admission increase
  or timeout change. Admit transient sharing only for material measured
  concurrent overlap with bounded memory and full authentication/cancellation
  semantics. Spatial repetition after eviction is not sufficient evidence.
- Working evidence: `/tmp/sandbox0-cipher-overlap.w7kh4O/plan.json`.

Result (2026-09-11; diagnostic only, no runtime change):

- Current grouped-artifact trace: cold430 provider acquisitions over57 objects;
  cached-new214 over35 objects. The standby issued zero. Concurrent acquisition
  overlap covered2,561,228/60,853,679 requested ciphertext bytes (4.21%) cold and
  1,509,168/24,327,132 (6.20%) cached-new; respectively24 and6 calls were fully
  geometrically covered by prior still-acquiring ranges.
- Most fully covered calls were Node22. Cold Coding had only2 claim and4 first-
  command calls. For14 of30 fully covered calls, the newer independent call
  acquired its response before all required covering older calls had acquired
  theirs. This warns against unconditional joining; it is not body-completion
  timing, proof of a regression, or a predicted latency saving.
- Requested ranges are not delivered wire bytes or SDK retry counts. Source/
  header scope end is not independently observed body completion; cancellation
  and fallback can weaken that inferred bound. The decision uses observed
  provider-acquisition overlap only. Concurrent duration sums are not latency.
- Decision: do not admit a ciphertext cache or unconditional range-sharing
  variant from this evidence. Preserve safe independent reads. Next attribute
  current-artifact NBD demand intervals to mapping/data dependencies to locate
  the remaining Coding claim/command serial waits, not another cache sweep.
- Seven synthetic analysis checks passed. Reverified all307 ABBA artifacts and
  exact1353-file restored product baseline. The original verification handle
  was missing on continuation; only its read-only validation was rerun. No
  remote mutation, deployment, claim/command replay or new latency sample.
- Aliyun CLI confirmed test ECS Stopped/StopCharging,2CPU/8GiB. No timeout
  increase, tenant RootFS prewarm, production mutation, merge or tag. The cold
  combined2s, production parity, occupied density, populated-size and lifecycle
  acceptance gaps remain open.
- Evidence: `/tmp/sandbox0-cipher-overlap.w7kh4O/evidence.json`.

### D-DEMAND-CHAIN — current-artifact single-outstanding NBD attribution

Plan (2026-09-11; read-only trace analysis):

- D-CIPHER-OVERLAP yielded evidence against broad ciphertext sharing. Preserve
  the exact1353-file product baseline and reuse only hash-bound grouped-artifact
  traces. No new runtime, cloud mutation or performance sample.
- Hypothesis: remaining Coding delay is dominated by upstream-serial NBD
  demand, including mapping-then-data waits, not insufficient admission width.
- Partition each sandbox claim and first-command window by zero/one/multiple
  outstanding NBD requests. For exactly one, classify its observed wait by
  mapping/data scope and request size. Do not sum concurrent intervals or call
  NBD concurrency an application dependency graph or predicted speedup.
- This is the missing current-artifact delta to earlier file/fault/readahead
  forensics, not a rerun of those older layouts. Require exact current-artifact
  file/metadata attribution before admitting a publication/layout change.
- Evidence root: `/tmp/sandbox0-demand-chain.dMNqnh/plan.json`.

Result (2026-09-11; diagnostic only, runtime baseline unchanged):

- Exact partition of32 windows (16 claim/command pairs) from the retained
  diagnostic trace passed accounting checks. The independently tested sweep
  has107 synthetic checks, including100 discrete-oracle concurrency cases.
- In all four cold Coding samples, exactly one NBD request was outstanding
  for1.091-1.221s during claim and0.745-0.764s during first-command. Those are
  per-sandbox observations: other sandboxes can still contend on shared reads.
- Within that single-request time, small-request mapping waits were
  372-399ms in claim and123-140ms in first-command; small mapped-payload waits
  were701-812ms and578-581ms respectively. Bulk-payload single-request waits
  were0ms in claim and30-33ms in command. These are marginal per-sample ranges,
  not additive maxima or predicted removable latency.
- Cold Coding had17-20 claim reads and6-8 command reads with at least1ms of
  both mapping and payload wait. Cached-new had zero such mapping waits while
  small-payload waits remained. This supports upstream-serial demand as the
  next investigation target, not an admission-width increase or cache sweep.
- Mapping versus payload classification comes from recorded Reader scopes.
  A payload is a RootFS block; file data versus XFS metadata is NOT yet known.
  Single outstanding NBD is not proof of an application dependency graph or
  that later demands can safely be predicted/prefetched.
- Extracted61 distinct small ranges from cold Coding lane0 with at least1ms
  single-request mapping/payload wait:816,128 logical bytes. They cover29 claim
  and32 command observations; representative claim waits389.871ms mapping plus
  796.631ms payload, command140.159ms plus579.669ms. This is an inspection input,
  never a production prefetch list or startup-training acceptance artifact.
- Decision: preserve the best measured baseline, no new implementation from
  this analysis. Next inspect those exact immutable ranges in a bounded,
  independent-cache remote reader to distinguish XFS metadata and file data;
  require current-artifact binding before any publication/delivery design.
- Reverified all prior cipher-overlap and mapping-trace sealed artifacts and
  exact1353-file product inventory. Fresh Aliyun CLI confirms original2CPU/8GiB,
  Stopped/StopCharging. No new claim, runtime edit, prewarming, timeout change,
  production mutation, merge or tag; no local e2e.
- The instrumented values here do not supersede the non-instrumented ABBA A
  baseline or prove2s cold combined, occupied production-width, populated-large
  RootFS, current-main/runsc parity or storage lifecycle acceptance.
- Evidence: `/tmp/sandbox0-demand-chain.dMNqnh/evidence.json`; exact next-read
  targets: `/tmp/sandbox0-demand-chain.dMNqnh/targets.json`.

### D-BLOCK-KIND — exact immutable slow-demand classification

Plan (2026-09-11; remote read-only diagnostic):

- Prior D-DEMAND-CHAIN was progress. Reverified its11 sealed artifacts and the
  exact1353-file product baseline. Inspect61 current Coding ranges (816,128
  logical bytes) plus the superblock, not an older layout or a new SLO trial.
- Original2CPU/8GiB test ECS; standalone authenticated Reader with independent
  128MiB cache,128MiB ciphertext reservation budget,10MiB per provider call and
  3min diagnostic deadline. No NBD/mount/guest or shared ctld cache access.
- Verify exact Ready artifact21a2f3 and descriptoraff13a against the retained
  source DB. Classify supported XFS structures using CRC/UUID/address binding;
  unknown stays unknown. No tenant bytes/names or credentials in summaries.
- No product/runtime/config/job change, DB/object write, claim, command,
  timeout increase or performance sample. These addresses are diagnostic
  targets, never a production prefetch list. No rollout, merge or tag.
- Preserve original processes/files/job/data and all failures. Export receipts,
  stop compute; observe the same live handle after transport interruption,
  never replay a possibly running inspection. Root: `/tmp/sandbox0-block-kind.w1ndbl`.

Result (2026-09-11; exact remote immutable inspection complete):

- Inspected all61 selected ranges plus the superblock using the unchanged
  authenticated Reader. Exact source DB Ready artifact21a2f3/descriptoraff13a
  and source row count2072 were verified before and after. No NBD, mount,
  guest, claim/command, object write or service/config/job replacement.
- 33 complete ranges match supported CRC/UUID/address-bound XFS metadata:
  24 inode-cluster ranges and9 directory/DA-tree ranges. One range contains a
  verified superblock sector plus unknown bytes;27 remain unknown. Matching
  structure content is not a complete XFS validator or allocation-ownership
  proof. Unknown is not equivalent to regular-file data.
- Correlation to the retained representative cold Coding lane (not a new
  latency measurement):12 fully recognized claim ranges correspond to76.986ms
  mapping wait plus262.426ms payload wait;21 first-command ranges correspond
  to131.622ms mapping wait plus442.730ms payload wait,574.352ms total. These are
  disjoint single-outstanding NBD wait intervals, not predicted removable time
  or an application critical-path proof. Do not prorate partial ranges by bytes.
- The61 ranges span816,128 logical bytes. Inspection additionally read4,096
  superblock bytes and returned4,903,817 ciphertext bytes in112 provider-range
  calls, reserving5,015,669 bytes. This includes mapping/header loading in an
  independent128MiB cache; not original startup wire traffic, SDK retry counts
  or a latency comparison. Remote request budget remained128MiB/3min.
- Decision: admit investigation of generic, bounded filesystem-metadata
  grouping/delivery costs, not a runtime candidate. Assess complete-image
  metadata inventory, objects/GETs, encoded/decoded amplification, import cost
  and incremental-COW/lifecycle/GC before activation. Derive ownership from
  filesystem authority, never magic detection or these61 trace addresses.
  Preserve independently verified demanded bytes, bounded cache/admission,
  small-read fallback, generic warm carriers and claim-time RootFS selection.
- Diagnostic unit/race tests, independent CRC32C oracle, all9 exported member
  hashes and post-run exact1353-file product inventory passed. Temporary
  inspector SHA256 eff546c57737d2b281db1144aa0437235be52a1673d2361ec0a13837eeeabb1a;
  no product source edit, timeout increase, tenant prewarm or local e2e.
- Retained failures: initial strict SSH lookup lacked the recycled-IP entry;
  existing independently verified host-key alias restored access with strict
  checking. Old preflight helper rejected the new DB name; explicit readonly
  psql fixed the diagnostic without changing its allowlist. Initial final-check
  falsely compared Ruby symbol keys to JSON string keys; normalized comparison
  proved identical PIDs/hashes. No inspection or mutation was replayed.
- Original service processes/files, two-group job modify index68814, formal
  and source DB rows2072 and original rows263 stayed unchanged;64 NBD devices
  stayed detached. Original ready-carrier count was0 before and after this
  boot; no claim was attempted or readiness repair performed. This is not a
  ready-runtime restoration claim. Inspector terminated successfully, compute
  is2CPU/8GiB Stopped/StopCharging and SSH ended after shutdown. All/data retained.
- Production/current-main/runsc parity, occupied density, populated-large
  RootFS, full storage lifecycle and cold combined2s remain unproved. No
  production mutation, PR merge, tag, baseline promotion or performance gain.
- Recognition follows the Linux v6.8 [XFS inode/superblock definitions](https://raw.githubusercontent.com/torvalds/linux/v6.8/fs/xfs/libxfs/xfs_format.h)
  and [directory definitions](https://raw.githubusercontent.com/torvalds/linux/v6.8/fs/xfs/libxfs/xfs_da_format.h).
  Attempted xfs_cksum.h links were unavailable and are not cited as consulted
  evidence; independent bitwise CRC tests and real record checks are retained.
- Evidence: `/tmp/sandbox0-block-kind.w1ndbl/evidence.json`; qualifications and
  complete unknown-range records remain in `qualification.json`/`analysis.json`.

### D-META-INVENTORY — filesystem-authoritative metadata inventory

Plan (2026-09-11; diagnostic preparation):

- D-BLOCK-KIND is progress; all44 sealed members and exact1353-file product
  baseline reverified. New hypothesis: inode allocation and directory fork
  APIs can produce a generic bounded metadata inventory without command
  traces, tenant prewarm, mkfs changes or timeout growth. The existing FIEMAP
  preferred-span scanner skips small files and directories; its complement
  must not be called metadata. Prior bootstrap profiles/old-artifact file
  forensics are not complete current-image ownership inventories.
- Validate native INUMBERS/GETBMAPX semantics and limits before compute.
  Exact current Node2b5208/8a28dd and Coding21a2f3/aff13a only. Original
  test2CPU/8GiB; independent128MiB Reader cache,512MiB ciphertext budget per
  image,10MiB/provider call,10min diagnostic deadline,2GiB memory/150% CPU.
  Private read-only no-recovery NBD31 mount, no guest/claim/DB/object write.
- Inventory all allocated-inode-containing4KiB blocks and all reachable
  directory fork extents, including indexes beyond i_size; name uncovered
  classes explicitly. Cost gate uses class-separated groups capped at1MiB
  decoded metadata with independently readable4KiB units. The61 trace
  addresses are posthoc coverage evidence only, never inventory inputs.
- Preserve original services/configs/job/data; record exact binary/input and
  unit handle before launch, observe rather than replay after interruption,
  detach only owned NBD/mount and stop compute. Original ready0 is not an
  acceptance fixture. No performance gain, candidate promotion or full2s
  claim follows from inventory. Plan: `/tmp/sandbox0-meta-inventory.r3ySnM/PLAN.md`.

Result (2026-09-11; both exact immutable images inspected once):

- Kernel INUMBERS plus directory GETBMAPX completed for both current artifacts.
  Node:7,297 allocated inodes,1,010 reachable directories,915 inode-containing
  blocks and80 directory blocks,4,075,520 bytes total. Coding:150,760 allocated
  inodes,15,893 directories,18,847 inode-containing blocks and4,497 directory
  blocks,95,617,024 bytes total. These are complete named classes, not all XFS
  metadata. Attribute/symlink/AG/log/extent-tree and unallocated-inode-only
  blocks remain explicitly outside this inventory.
- Found8 Node and113 Coding directory extents beyond i_size. An EOF-limited
  directory mapping would miss these. Both authenticated superblocks report
  rmapbt enabled; no mkfs feature change was made. This can justify a future
  GETFSMAP investigation if a concrete design needs wider ownership coverage;
  this run did not call GETFSMAP or attest a new remote kernel version. UAPI
  and pagination reference: Linux v6.8 xfs_fs.h/xfs_ioctl.c/xfs_itable.c and
  xfs_bmap_util.c, retained as exact local reference snapshots.
- Predeclared class-separated1MiB decoded-cap groups with independently
  compressed4KiB units: Node5 groups/444,291 encoded payload bytes; Coding92
  groups/10,733,371 bytes. These estimates exclude mapping/encryption framing,
  publication, extra locators, COW and lifecycle/GC. They are not wire cost,
  a new on-disk format or an approved runtime delivery policy.
- Actual complete-image scan: Node0.538s/78 provider calls/1,969,865 ciphertext
  bytes; Coding23.408s/2,271 calls/56,736,507 bytes. Subsequent cost reads share
  only the probe's private128MiB cache. Coding total scan+cost returned
  129,666,117 ciphertext bytes across5,219 calls; cost pass21.588s included
  552.778ms codec wall time. This remote Reader diagnostic is not an importer
  local-file CPU benchmark or a claim/first-command performance sample.
- Posthoc independent coverage of the old61 small-demand ranges:30 fully
  covered,3 partial,28 uncovered. All21 previously recognized first-command
  metadata ranges are covered, preserving their old574.352ms disjoint wait
  attribution without calling it removable time. Trace offsets never entered
  the scanner. An independent interval oracle checked all61 coverage results.
- Selected command's245,760 bytes of metadata touch15 groups totaling
  15,273,984 decoded bytes and1,968,061 encoded payload bytes. Eager whole-group
  decoding would amplify by62.15x. Claim+command touch24 groups/24,313,856
  decoded bytes;14 command groups were not touched by selected claim ranges.
  This excludes other demands, other sandboxes, eviction and writer deltas.
  Independent4KiB compression permits demand-only decoding; the62.15x is not
  asserted as unavoidable for encoded-only delivery. No GET or latency win
  was measured, and no group-size sweep or eager metadata preload is admitted.
- Decision: diagnostic only. Reuse this inventory to gate a bounded encoded
  grouping/demand-only authenticated decoding design with current locator,
  transport/mapping and high-density costs. Do not repeat an unchanged scan,
  hardcode the old demand addresses, add a cache-size/timeout sweep or promote
  this cost model into a runtime candidate. Storage authority/lifecycle and
  cold/cached-new node-v AB remain required before adoption.
- Temporary binary8c71d3dd5a793607c81e4e5d93d5b014faab5baa6088083ad05aa4cf4941315e;
  29 unit/race checks/subchecks passed. Local traversal fixture initially
  failed because the workspace filesystem reports different devices for dirs
  and files; using one tmpfs fixture passed with cross-device rejection intact.
  Early live-unit export was refused; later oversized terminal export was
  recovered with lossless packed transport and17 original member hashes.
  Local posthoc script syntax/phase-label errors were corrected before output.
  No remote inspection, transfer or mutation was replayed after a timeout.
- Original2CPU/8GiB services/process PIDs/hashes/configs, job68814/two groups,
  source/formal rows2072 and original rows263 preserved. This boot had ready2
  before/after; no claim or readiness repair.64 NBD detached, owned mount
  absent, inspector terminal success, ECS Stopped/StopCharging, SSH terminal.
  All/data retained. Largest sampled running-unit MemoryPeak was503,783,424
  bytes; terminal peak was unavailable, so this is not the final cgroup peak.
- Exact1353-file product baseline and previous44 sealed artifacts preserved.
  No product/runtime edit, tenant prewarm, increased timeout, local e2e,
  production mutation, merge/tag or baseline promotion. Cold combined2s,
  current-main/runsc parity, populated-large RootFS, occupied production-width
  and full storage lifecycle acceptance remain unproved.
- Evidence: `/tmp/sandbox0-meta-inventory.r3ySnM/evidence.json`; see
  `analysis.json`, `coverage.json` and `qualification.json` for exact bounds.

### D-META-DEMAND — complete retained demand locality gate

Plan (2026-09-11; offline diagnostic):

- D-META-INVENTORY is progress. Reuse its full two-image named-class inventory
  and fixed1MiB/independent4KiB group cost policy; do not rescan images or tune
  group size. Evaluate all retained cold/cached-new16 identities and32 real
  claim/node-v windows, not only the61 slow Coding ranges.
- Current Reader checksums/decodes the entire ObjectRange. Independent4KiB
  metadata needs explicit publication/authentication geometry; changing cache
  alone is insufficient. Old always-encoded/indexed-codec variants are not
  reactivated. No product/on-disk/runtime/cache/timeout/mkfs change.
- Verify source/image/trace hashes and clocks; count unbound requests and
  writer-subrange ambiguity. Reader-root traces omit exact clean subranges,
  so NBD/inventory intersections are potential base metadata demand, not exact
  source GETs or proven immutable-byte attribution. Report conditional bounds
  and full phase/cohort results, with an independent interval oracle.
- Model ideal encoded retention explicitly; it is not a mixed-data128MiB
  cache replay or a latency prediction. Require authenticated group locators,
  demanded-block verification, bounded admission/fallback and full COW/GC
  integration before implementation. No compute launch, tenant prewarm, local
  e2e, production mutation, merge/tag or reduced startup acceptance scope.
- Plan and artifacts: `/tmp/sandbox0-meta-demand.PvArGT`.

Result (2026-09-11; complete retained-trace locality and decode-cost gate):

- Verified all14,373 NBD requests across cold/cached-new streams against the
  exact current artifact roots and an independent inventory interval oracle.
  All had one observed current Reader root; standby streams had no reads.
  All16 identities and32 claim/real node-v windows succeeded in the retained
  run; no windowed read crossed its phase end. This is historical instrumented
  private-loopback width8 evidence, not a new public or occupied-width sample.
- Each Coding first command intersects339,968 bytes in32 fully inventoried
  NBD requests and18 fixed physical-order groups. The four concurrent cold
  commands actually caused22 metadata-associated payload source attempts,
  196,338 stored-range bytes and1,441,792 decoded member bytes. Source attempts
  are not provider GETs or wire bytes; member ranges can include neighboring
  uninventoried bytes beyond the NBD demand.
- Disjoint historical payload decode/verification Running spans for those
  requests total0.317–0.978ms per Coding command. Corresponding per-sandbox
  single-outstanding metadata payload waits are442.508–443.777ms; mapping
  waits112.934–131.622ms. Mapping decode Running spans are0–1.856ms. Running
  spans are not exact CPU cycles and exclude other cipher/runtime work; the
  wait analysis keeps all other NBD reads in the concurrency partition.
  These observations reject decode-only work as the main latency mechanism,
  not demand-only decoding as a safety/cost requirement for future grouping.
- Conditional group model: Coding claim touches13 groups; command touches18,
  with27 in the union. Even ideal retention leaves14 newly touched command
  groups/1,838,795 encoded payload bytes. Whole touched command groups decode
  to18,419,712 bytes, versus339,968 potentially demanded4KiB bytes. Node has5
  claim groups and4 command groups entirely among them. All two-image touched
  groups total3,908,879 encoded payload bytes in the ideal model.
- This is not a measured A/B: hypothetical group payload and historical
  source ranges have different policies; no future GET-count, cache-hit or
  latency claim follows. Ideal retention ignores eviction, admission timing
  and in-flight duplication. Reader-root logs do not specify clean subranges;
  one base call can coexist with a dirty prefix/suffix, so the NBD/inventory
  intersections remain potential demand, not byte-exact base attribution.
- Current ObjectRange/DataOffset require whole-range decoded validation. A
  cache-only edit cannot manufacture independently verifiable4KiB compressed
  units. New grouping still needs authenticated location/index geometry,
  mapping cache/transport overhead, fallback and complete publish/COW/GC
  integration. Reviewed historical indexed-codec and always-encoded-cache
  families remain unactivated; no old-version result was promoted.
- Decision: diagnostic progress; do not implement fixed physical-order
  whole-group delivery or a decoder-only candidate on this evidence. Next
  investigate filesystem-defined directory-block/child-inode dependency
  locality and full-image duplication/index costs. A new graph probe must
  collect missing filesystem relationships, not rerun the unchanged inventory
  or learn grouping from command paths/trace addresses. Stronger locality and
  safety/cost gates precede publication/reader implementation and real cold/
  cached-new command A/B. No parameter sweep or tenant prewarm is admitted.
- Four model tests/412 assertions and14,373 independent interval checks
  passed. Exact1353-file product baseline and all66 preceding sealed members
  preserved. No product/runtime/on-disk/cache/timeout/mkfs edit, remote compute
  launch, claim/guest command, local e2e, production mutation, merge or tag.
  ECS re-observed2CPU/8GiB Stopped/StopCharging. Cold combined2s, current-main/
  runsc parity, populated-large, occupied production-width and full storage
  lifecycle acceptance remain unproved.
- Evidence: `/tmp/sandbox0-meta-demand.PvArGT/evidence.json`; full windows in
  `analysis.json`, exact decode/wait scopes in `decode-attribution.json` and
  exclusions/next admission in `qualification.json`.

### D-DIRECTORY-GRAPH — filesystem-defined dependency locality

Plan (2026-09-11):

- Previous goal turn was feedback/status only (no progress). Collect the
  missing directory/parent/child inode relations and per-directory extent
  ownership from the same immutable images; do not repeat counts alone.
- Hypothetical group triggered only by an owned external directory block:
  own/parent/child inode blocks plus all that directory's data-fork blocks,
  deduplicated,256-block/1MiB decoded cap. Overflow and no-trigger shortform
  directories use exact demand; no trace-trained groups or size sweep.
- Measure every directory, duplicated independently compressed4KiB payload,
  explicit hypothetical index costs, then replay all32 retained windows as
  conditional ideal-retention bounds. No actual GET/latency benefit implied.
- Reuse isolated RO NBD31,2CPU/8GiB,128MiB private cache,512MiB per-image
  cipher reservation. Preserve original services/DB/job and stop compute.
  No runtime/storage-format change, claims, tenant prewarm, timeout increase,
  local e2e or production mutation. Full plan/artifacts:
  `/tmp/sandbox0-directory-graph.HKuxn3`.

Result (2026-09-11; directory graph and request-cost gate complete):

- Captured every reachable directory's inode, parent, child inode identities
  and owned data-fork extents, without paths or regular-file payload reads.
  All earlier inventory counts/spans and independent4KiB encoded costs match
  exactly. New per-unit size/checksum table permits overlap cost accounting.
  Remote kernel6.8.0-124-generic/x86_64, same immutable format2 artifacts.
- Node:1010 directories,65 eligible groups,945 no external trigger, no
  overflow;964 member occurrences/826 unique blocks,65,583 duplicated
  encoded payload bytes,49,632 hypothetical index bytes.
  Coding:15,893 directories,4214 eligible,11,676 no external trigger,3
  overflow with exact fallback;32,145 member occurrences/21,314 unique
  blocks,4,834,400 duplicated encoded payload bytes,1,748,848 hypothetical
  index bytes. Full group payload14,551,793 bytes plus1,015,978 bytes in
  metadata not covered by any eligible group. Index convention48-byte
  member/32-byte header/16-byte trigger excludes real location/authentication,
  framing, fallback index and lifecycle costs; not a proposed accepted format.
- Replayed all14,373 exact-root-bound retained NBD reads and32 claim/node-v
  windows with an independent interval oracle. Directory membership and
  trigger choice use filesystem relations only. Ideal standalone Coding
  command:79 newly demanded metadata units,39 previously group-delivered,
  8 new groups plus32 exact4KiB units,238,812 encoded payload bytes. Node
  command:49 new units,25 preprovided,4 groups plus20 exact units,47,514 bytes.
  These are conditional models, not new remote claims or source GET counts.
- Request gate changes the next action: standalone ideal Coding command
  has15/32 fully inventoried requests already available but17 still needing
  delivery. Historical single-outstanding payload waits in these classes
  are142.197–143.911ms and299.627–300.533ms respectively. All corresponding
  Coding claim payload waits remain in new-delivery requests (the4 ideal-hit
  claim requests had zero such wait). These classify old observed waits;
  they are not removable-time or future-latency predictions.
- Even ideal cross-sandbox sharing gives8 group +32 unbatched4KiB fallback
  transfer units for the cold Coding commands, versus22 historical payload
  source attempts/196,338 stored-range bytes. The policies differ and neither
  count is provider GETs. This flags fragmentation risk: do not replace the
  existing larger authenticated range fallback with serial4KiB reads.
  Ideal cached-new zero transfers ignore eviction, flight timing and mixed
  regular-file cache pressure; this is not a measured cache-hit result.
- Decision: retain graph/locality evidence, but do not admit external-dir-
  trigger-only grouping as a runtime candidate. Next use the retained graph
  to evaluate deterministic bounded union of ALL directory records in an
  inode-containing block, including no-external-block directories. No choosing
  one owner by command path/trace, no group-size sweep or unchanged remote
  scan. Early dependency delivery and preserved fallback transport granularity
  must both pass before authenticated publication/reader implementation.
- RO probe completed once per image, zero block/object mutation attempts,
  no claims/guest commands. Node scan0.514s/cost0.027s; Coding scan23.097s/
  cost22.948s,129,666,117 total ciphertext bytes. These are offline diagnostic
  full-image costs, not startup latencies. Largest sampled unit MemoryPeak
  518,303,744 bytes; final peak unavailable. Coding process maxRSS307,052KiB.
- Local composition initially refused a duplicate helper destination; fixed
  by verifying the existing identical helper. Packed terminal export was
  truncated by tool output; byte-preserving SCP recovered17 exact originals.
  Early analysis rejected a partially downloaded file; complete-transfer
  guard added. Stop request overlapped the last transfer, which completed
  successfully and all17 hashes verified before SSH termination. No probe,
  transfer or cloud mutation replay; original data retained.
- Original ready2 before/after, original process PIDs/hashes/configs, job68814/
  two groups, source/formal2072 and original263 rows preserved.64 NBD devices
  detached, owned mount absent, unit inactive/dead/success/PID0, SSH terminal.
  ECS confirmed Stopped/StopCharging/2CPU/8GiB. Exact1353-file product baseline
  and83 preceding sealed members preserved. Go unit/race tests and5 model
  tests/420 assertions passed; no runtime/cache/timeout/mkfs edit or promotion.
- Cold claim+node-v2s, current-main/runsc parity, populated-large RootFS,
  occupied production-width and publish/COW/GC acceptance remain unproved.
  Evidence: `/tmp/sandbox0-directory-graph.HKuxn3/evidence.json`; exact windows
  in `analysis.json`/`request-gate.json`, decision bounds in `qualification.json`.

### D-INODE-TRIGGER — earlier demand activation without owner guessing

Plan (2026-09-11; offline, previous turn was progress):

- Reuse all directory/inode relationships, including no-external-block
  directories. Actual demand of an inode-containing block activates the
  deterministic union of ALL directories whose own inode is in that block;
  same256-block/1MiB decoded cap, overflow exact fallback, no size sweep.
- Consider activation on actual demand even if the trigger bytes arrived in
  a prior group; skip fully available groups. Never recurse from speculative
  delivery. Report added group work separately from demand availability.
- Measure full-image costs and all32 historical windows. Preserve the
  existing whole authenticated range fallback/coalescing contract;4KiB unit
  fallback arithmetic is not a runtime design or GET/latency prediction.
- No remote launch/scan/claim, tenant prewarm, timeout/cache-size increase or
  product mutation. Plan and artifacts: `/tmp/sandbox0-inode-trigger.kcZCij`.

Transport follow-up plan (before execution, same experiment):

- Initial ideal Coding command model improves demand misses17 ->9, but
  requires25 group transfers. Add one deterministic transport cost gate:
  trigger-sorted64MiB payload packs, same-demand adjacent activated groups
  only,1MiB stored/1024 members/32 groups per window. No gap filling,
  additional activation, group-size sweep or changed fallback granularity.
- Report original-timeline lead from supplying trigger read start/end to
  first later demand; instantaneous model availability is not observed I/O
  completion. Index/framing and real latency remain unmeasured.

Result (2026-09-11; earlier trigger + adjacent transport prototype gate):

- Verified all77 preceding sealed members and the exact1353-file product
  baseline. Reused full graph and independent4KiB costs; no remote scan,
  artifact publication, guest command, claim or product edit. Local main
  refs were inspected: sandbox0 d3541bd and infra0df7cde. That sandbox0/main
  descriptor supports format1 only; the historical candidate's format2 and
  this model are not current-main/runsc or production deployment parity.
- Full-image deterministic inode unions: Node454 triggers, all1010 directory
  records admitted,304 multi-directory triggers (max6 records/block), largest
  group122 blocks. Coding9591 candidate triggers/9588 admitted;3 overflow
  triggers contain7 directory records;4185 multi-directory triggers (max8),
  largest candidate363 and admitted220 blocks. No owner was chosen by trace,
  file path or command.10,045 independent full-image union checks passed.
- Overlap is a real cost: Node1,271,144 total grouped payload bytes,
  826,853 duplicates and150,144 hypothetical index bytes. Coding26,097,038
  grouped payload bytes,15,619,990 duplicates,3,091,776 hypothetical index
  bytes;493 metadata blocks/256,323 encoded bytes are outside admitted groups.
  These use the earlier payload-only independent4KiB codec and48/32/16-byte
  index accounting, excluding real authenticated locations/framing/fallback
  index and lifecycle overhead. Not an accepted wire format or memory budget.
- All14,373 retained NBD reads and32 claim/real node-v windows replayed with
  exact-root checks and independent intersection oracle. Actual demand may
  activate a previously group-delivered inode trigger, but speculative bytes
  never recursively activate groups; fully available groups are skipped.
- Standalone ideal Coding command:54/79 first-demand units previously
  group-delivered (external-dir model39); requested bytes available before
  23/32 requests, leaving9 (previous17) needing delivery.25 new groups and
  zero residual4KiB model units cost328,604 encoded bytes. Two requests with
  already-available demand activate3 further speculative groups; blocking
  those would reduce the no-new-delivery count to21, not23. No async arrival
  or end-to-end latency improvement follows from this ideal model.
- Same-demand adjacent pack transport: Coding command25 groups ->11 payload
  windows, max126,641 bytes/235 member occurrences/4 groups per window and
  at most1 window per original request;2 windows are speculative on already
  available demand. Payload is unchanged, no omitted neighbor activated.
  Coding claim41 groups ->17 windows plus8 residual units; Node claim48 ->13
  windows plus9 units, command18 ->9 windows plus1 unit. The same ordinal
  request stream under ideal cross-sandbox retention has the same cohort
  window totals. Cached-new zero in that ideal shared model is not a measured
  cache hit.11 model windows are NOT11 provider GETs; previous22 source
  attempts/196,338 bytes have a different policy and are not an A/B control.
- Original-timeline lead is not unlimited: the first standalone Coding
  command has11/54 preprovided first-demand units less than1ms after the
  supplying original trigger read ended. Its claim has8 demands before the
  supplying original read ended. These are historical timing opportunities,
  not completion of a future group fetch; do not assume async data is ready.
- Current Reader fallback resolves and validates whole ObjectRanges, keeps
  activePayload through the original request and coalesces bulk reads. A
  prototype must use complete-assist-hit or whole-original-ReadAt fallback,
  not residual4KiB calls. Preserve tail/dirty overrides and checksum semantics.
  DataOffset does not create independently authenticated compressed units.
  A versioned root-bound index plus full immutable publication/reachability
  is required; no locator-from-key shortcut or unversioned side authority.
- Decision: admit early inode activation + adjacent transport for an isolated
  implementation experiment, NOT runtime activation or SLO acceptance. Next
  implement/test the generic authenticated index/transport and whole-fallback
  contract, including actual index/framing, cache/admission/concurrency costs;
  do not repeat inventory or tune sizes. `PROTOTYPE-CONTRACT.md` records the
  authority, fallback, async and lifecycle boundaries before real cold/
  cached-new end-to-end A/B. Missing complete clean-base subranges and index
  costs prevent stronger predictions from these traces.
- 7 model tests/1231 assertions,5 transport tests/27,792 assertions and450
  transport interval checks passed. ECS re-observed Stopped/StopCharging at
  2CPU/8GiB; compute was never started. No tenant prewarm, timeout/cache-size
  increase, local e2e, production mutation, merge/tag or baseline promotion.
  Cold combined2s, populated-large, occupied production width and complete
  COW/fork/rebase/GC acceptance remain unproved.
- Evidence: `/tmp/sandbox0-inode-trigger.kcZCij/evidence.json`; complete windows
  in `analysis.json`, `request-gate.json`, `transport-gate.json`; decision and
  exclusions in `qualification.json`.

### D-ASSIST-PROTOTYPE — authenticated group and whole-fallback overlay

Plan (2026-09-11; previous turn was progress):

- Implement the admitted inode-demand/adjacent-group mechanism in a temporary
  Go overlay using actual rootfsblock cache/admission/codec and encrypted
  store. No production descriptor/config or product-file mutation.
- Explicit diagnostic version/root-bound paged index, publisher reads member
  bytes from the real immutable Reader, all generated objects inventoried.
  This is not current format1/2 or a generally publishable RootFS artifact.
- Complete-hit or original full ReadAt fallback; preserve tails/dirty data.
  Cached triggers still activate on actual demand; no recursive speculation.
  Opportunistic async work only within immediately available existing source
  slots; no unbounded queue or increased parallelism/cache/timeout.
- Test codec/index corruption, root binding, fallback source granularity,
  encrypted cost, async/cancellation and bounded state before remote claims.
  Plan/artifacts: `/tmp/sandbox0-assist-prototype.LIlwFL`.

Result (2026-09-11; isolated implementation progress, runtime gate NOT passed):

- Implemented the diagnostic prototype as four temporary Go compiler-overlay
  files. Actual immutable Reader bytes feed a versioned root-bound paged
  index and independently encoded4KiB units. Reuses the existing ReadCache,
  range codec, eight source slots and encrypted store. No product source,
  production format/configuration or remote artifact was changed.
- Implemented complete assist hit or one original whole ReadAt fallback,
  demand-only decoding, actual-demand activation of already-cached inode
  triggers, no recursive activation, and bounded adjacent active-group
  transport. Index bounds are256KiB/32 entries/depth6/64 visited pages; group
  transport bounds are1MiB stored/1024 members/32 groups. These are prototype
  algorithm bounds, not a cold-start latency or process RSS guarantee.
- Preserved all test receipts: v1 passed11 top-level race tests; v2 passed14
  after adding demanded-checksum/malformed-frame, cross-root eviction and
  commit-then-error publication-attempt coverage. v3 safety extension passed
  16 but FAILED1. Do not report the latest suite as green.
- New reproducible failure: a speculative worker in generation B joins the
  cache singleflight led by generation A. Canceling B cannot leave the
  blocking singleflight.Group.Do, so B.Close waits for A's unrelated source
  read. The test observes the actual WaitGroup waiter stack before canceling;
  Close is still pending after200ms of fixture time. Cleanup unblocks A and
  all workers exit. The failure is retained and UNFIXED. This concerns the
  isolated prototype, not a diagnosis of deployed runtime latency.
- Actual AES-GCM index/data fixture: one index load and one group window,
  two provider calls,133,160 requested cipher bytes versus1,632 returned;
  later member demand adds zero provider calls and the two demands decode
  four units total. These tiny in-memory synthetic costs are NOT full-image
  index/framing measurements, remote GET latency or predicted SLO savings.
  Independent encrypted-index and encrypted-data byte corruption fall back
  successfully; a valid compressed1MiB frame is rejected at the4KiB unit
  output bound. These safety checks passed, but do not negate cancellation.
- Whole fallback16KiB/128KiB preserves one original read and its normal
  payload source-range granularity; dirty branch overrides and partial EOF
  passed. Cache accounting/eviction and distinct-root identity tests passed.
  Complete already-decoded hits bypass admission, but encoded-only hits still
  need a slot and may wait behind occupied slots: the stronger no-wait cached
  demand contract remains unmet and must not be advertised as implemented.
- Root-label equality is not a Merkle proof for arbitrary assist data. The
  trusted publisher reads member bytes through the base Reader; production
  use still needs authoritative descriptor binding, durable publication
  intents, reachability/GC and lifecycle/upgrade integration. The diagnostic
  descriptor/reference kinds are intentionally rejected by existing format
  validators. Attempted object references are only an in-memory diagnostic
  result, not a durable import journal.
- Decision: no runtime activation or remote latency A/B yet. Next repair
  leader-owned admission and independently cancelable non-owner waits while
  preserving shared cache/coalescing, bounded speculation and cleanup. Do not
  detach unaccounted workers or remove cross-generation coalescing to hide the
  failure. Resolve the encoded-cache wait boundary as part of this gate.
- Exact1353-file product baseline and all26 previous sealed members verified.
  No remote compute started/status queried, no claims/guest commands, no
  tenant prewarm, runtime timeout/cache/concurrency increase, local e2e,
  production mutation, merge/tag or baseline promotion. Full package suite
  was not run. No new cold/cached-new claim+node-v latency result; cold2s,
  occupied production width, populated-large and full lifecycle remain open.
- Evidence: `/tmp/sandbox0-assist-prototype.LIlwFL/evidence.json`; successful
  and failed receipts `test-v1.json`, `test-v2.json`, `test-v3-safety.json`;
  remaining boundaries and next action in `qualification.json`.

### D-ASSIST-OWNERSHIP — independently cancelable shared assist reads

Plan (2026-09-11; previous goal turn was progress):

- Repair the isolated overlay's reproduced shared-flight follower cancellation.
  Keep shared content/coalescing and the existing eight source admission slots.
  Reference-count the already-owned slot across the caller and its candidate
  leader callback; canceled followers leave promptly, while actual I/O retains
  its owner/slot until completion. No detached unaccounted work or second cache.
- Verify ownership races, shutdown, source-slot release, coalescing, busy-slot
  speculation and multiple generations. Separately resolve the encoded-only
  cache-hit wait boundary without eager group decoding or unbounded work.
- No remote claim, product change, new timeout/cache/concurrency setting or SLO
  claim. Plan and artifacts: `/tmp/sandbox0-assist-ownership.Hjho1A`.

Result (2026-09-11; ownership/cache-hit gates fixed, isolated real-image cost probe only):

- Preserved all17 preceding sealed artifacts and the exact1353-file product
  baseline. Seven Go compiler overlays implement this experiment; the ordinary
  Reader copy differs by two diagnostic ReadCache fields only. No product
  source, accepted storage format, service configuration or remote state changed.
- Fixed the previous shared-flight follower cancellation failure. One
  cache-owned transient record/shared completion signal represents each actual
  admitted assist transfer; canceled followers leave no stored waiter list.
  Caller and leader reference-count one existing source permit. The leader's
  lifecycle job is registered before scheduling and held until actual I/O and
  verification unwind. Close waits for its own work, not another generation.
  A canceled leader does not permanently disable a healthy surviving reader.
- The initial DoChan draft was rejected BEFORE testing: pinned x/sync v0.19.0
  retains a subscriber channel until leader completion even after that waiter
  cancels. It is preserved as superseded-dochan.go.txt, not a measured runtime
  candidate. The final transient table replaces assist-only flight handling;
  ordinary Reader flights are unchanged. No second assist content cache or
  duplicate durable state was introduced.
- Ownership tests:256 canceled followers still share one physical group read;
  64 distinct logical owners remain within eight source slots; same-owner
  shutdown, surviving follower retry and queued-demand cancellation pass.
  A deliberately stalled transfer retains its slot after caller cancellation
  until it actually unwinds. All transfer records/slots release on completion.
  These are synchronized in-memory tests, not production-width/RSS results.
- Recorded a separate red test: complete encoded-only4KiB and128KiB hits still
  waited behind occupied source slots. Fixed with one cache-wide cancellable
  CPU-only demand decoder gate, never held across I/O. Both original cases
  now pass with all eight source slots occupied and zero new source reads.
  Already-decoded hits keep their bypass; speculative neighbors are not eagerly
  decoded.64 cached-demand logical owners plus cancellation of one waiter
  verify gate ownership and zero network work for the63 surviving demands.
- Tradeoff: one additional bounded decoder/staging job can coexist with source
  I/O, and encoded-hit CPU work is serialized. No source-width/cache-budget
  increase occurred, but high-density throughput and RSS are still unmeasured.
  This removes network-slot waiting for complete encoded hits; it is not a
  zero-CPU-queue-time guarantee. Existing fallback reads already in progress
  retain the normal Reader lifecycle contract, not a new global cancellation API.
- Test receipts remain separate:22 ownership tests passed; encoded-hit red
  test failed as expected;23 tests passed after that fix; final25 assist tests
  passed. Nine critical concurrency tests repeated20 times produced180 passes
  without race failures. Corruption, whole16KiB/128KiB fallback, dirty branch,
  EOF, distinct-root eviction and real encrypted fixture checks remain green.
- Full-package attempt hit the90s test-harness deadline in the unchanged large
  mapping-stream model after132 top-level passes. Its stack and unsuccessful
  receipt are preserved. The first shard planner rejected an incorrect
  assumption about printed subcase PASS records; no shard Go tests started
  until corrected. Seven subsequent shards covered every remaining test name
  and all six frontier subcases with unchanged fixtures and the same90s limit.
  Verified inventory:235 top-level tests,233 unique passes,2 explicit opt-in
  skips (external RustFS integration and16-million-extent stream-scale model).
  No test-size reduction, sandbox timeout increase or synthetic-large-image
  claim. The earlier whole-package timeout is not retroactively a green run.
- Decision: isolated real-RootFS index/encryption cost probe is now appropriate;
  general runtime activation and SLO acceptance are NOT. Next use actual
  immutable Node/Coding source bytes and complete generic plans, measure real
  index/framing, requested/returned cipher bytes, cache isolation and costs.
  Do not repeat graph discovery or promote a trace-picked/pinned manifest.
  Authoritative descriptor/publication intents/reachability/GC and lifecycle
  integration still precede a deployable runtime candidate.
- No remote instance start/status query, claim, guest command, tenant prewarm,
  local e2e, production mutation, merge/tag, current-main parity claim or baseline
  promotion. No new cold/cached-new end-to-end result. Cold2s, occupied physical
  production width, populated-large images and full lifecycle remain unproved.
- Evidence: `/tmp/sandbox0-assist-ownership.Hjho1A/evidence.json`; exact test-name
  and subcase accounting in coverage.json; result boundaries in qualification.json.

### D-ASSIST-REAL-COST — real immutable RootFS and isolated OSS fixture

Plan (2026-09-11; previous goal turn was progress):

- Use the tested overlay with complete generic inode plans and actual immutable
  Node/Coding source bytes. No graph rescan, paths selected for publication,
  tenant prewarm, NBD mount, claim, guest command or product mutation.
- Source RootFS/DB remain read-only. For real encrypted network cost, create
  only journaled conditional objects under the fresh test-bucket prefix
  rootfs/diagnostic-assist-wJWgfq/{node,coding}/. No template/artifact registration.
  Archive ciphertext, verify exact identity and delete only owned fixture keys
  after measurements; preserve cleanup receipts and original objects/services.
- Empty application/crypto caches versus shared-cache new Reader instances;
  normal versus assist whole-request paths, fixed cache/admission settings.
  Historical fully inventoried metadata request shapes are NOT dirty-state,
  gVisor/kernel or ingress replay and are not claim/command SLO timings.
- Audit the full authenticated index and4KiB SHA oracle. Record true OSS calls,
  requested/returned cipher bytes, caller/drain time and CPU/peak-RSS scope.
  Existing2CPU/8GiB test instance is not occupied production width. Stop compute
  when done. Plan/artifacts: `/tmp/sandbox0-assist-real-cost.wJWgfq`.

Result (2026-09-11; real OSS cost measured, mixed regression, NOT runtime admission):

- Built the complete generic index from actual immutable Node/Coding bytes,
  without selecting publication groups by command paths. Full index audit
  verified454/9588 groups and2674/54824 member references against the retained
  4KiB SHA oracle. Actual stored index bytes121,375/2,540,777; decoded JSON
  bytes530,709/11,125,093. Stored grouped payload1,271,144/26,097,038 exactly
  matches the earlier independent codec cost. No artifact/template registration.
- Empty application caches, one fixed-order sample each: Coding metadata loop
  plus async drain1,298.132ms ->969.516ms; Node214.773ms ->354.531ms. Coding
  wrapper source calls73 ->64 and returned ciphertext3,563,632 ->1,861,799 bytes;
  Node28 ->39 calls and690,353 ->900,141 bytes. The larger case improves while
  the smaller case regresses; this is not a generally successful candidate.
- Shared-cache new Reader repeats have ZERO provider reads for both families.
  Baseline loop+drain0.531ms/0.672ms versus assist13.973ms/16.709ms (Node/Coding).
  Assist CPU25,551us/29,932us versus baseline555us/700us. These are NOT cached
  new sandbox claims. Coding empty-cache index transport is29 calls/478.967ms
  summed duration versus group transport23 calls/188.199ms. Overlapping sums
  are not removable critical-path wall time.
- Source inspection: cached raw index bytes are still parsed, member-validated
  and canonically re-marshaled on every lookup. Repeated parsing is a plausible
  CPU/allocation contributor, NOT a profiled attribution. Next isolate/profile
  this work before a parsed-page cache, keeping any parsed state inside existing
  total/protected-mapping accounting and eviction. No second cache, pinned full
  index, image-specific tuning, source-width/cache increase or timeout change.
- Interpretation limits:36 Node and51 Coding historical metadata request shapes
  are applied to immutable base bytes, not historical dirty state. No kernel,
  NBD, gVisor, ingress, new sandbox, claim or real node-v execution. Fresh
  application/crypto caches do not attest a cold node; raw OSS connections are
  reused. Provider-wrapper calls do not enumerate SDK-internal HTTP retries.
  One fixed baseline-then-assist run is neither replicated A/B nor SLO evidence.
- Cumulative process peak RSS39,248KiB/505,916KiB includes full build/audit and
  cannot be assigned to the reader. Coding remains sparse1TiB logical/~5GiB
  allocated, not populated1TiB. Existing2CPU/8GiB diagnostic host is not occupied
  production width. Ready carriers were0 before AND after; no repair attempted.
- Test-v2 passed26 assist and3 probe top-level race tests. Its source hashes
  exactly match the built and remotely executed binary. Guard syntax failure
  was caught locally before transfer/launch; corrected cleanup readback bounds
  and absence checks were tested before build. observe-01 output truncation is
  retained as an observation limitation; complete observe-02 and the verified
  archive preserve the same terminal execution. The probe was not restarted.
- All329 owned fixture objects (30,338,160 encrypted bytes) were verified,
  deleted and confirmed absent. Their ciphertext is recoverable from local
  export and retained remote diagnostic archives. Verified1,025 exported files;
  original files/PIDs, source rows, Nomad job and64 detached NBDs unchanged.
  Remote test lifecycle skill used only for preserve/start/stop, not its legacy
  Kind deployment path. Final Aliyun receipt confirms Stopped/StopCharging.
- Exact1,353-file product baseline and33/77/26 prior sealed members preserved.
  No product activation, tenant prewarm, production mutation, local e2e,
  merge/tag or baseline promotion. Authoritative descriptor/publication/GC and
  lifecycle integration still precede runtime admission. Regional-ingress
  claim+first real node-v, cold2s, occupied production width and populated-large
  image acceptance remain OPEN. Next perform the isolated CPU attribution,
  then matched replicated remote measurements only if justified.
- Evidence: `/tmp/sandbox0-assist-real-cost.wJWgfq/evidence.json`; reproducible
  aggregation in analyze.rb/analysis.json, verification.json, qualification.json.

### D-ASSIST-INDEX-CPU — actual archived index CPU attribution

Plan (2026-09-11; previous goal turn was progress):

- Profile unchanged lookup on the same whole historical request shapes using
  full actual Node/Coding indexes from retained ciphertext archives. Decrypt
  only on the original remote host; no key export, OSS access/object writes,
  graph scan, claim, NBD mount, guest command or runtime mutation.
- Verify full index/member oracle before timing. Repeat sequential lookup200
  times with zero measured archive reads; separately measure full decode,
  strict JSON parse, validation and canonical marshal on all pages10 times.
  Full residency/parsed fixture state isolates CPU only; it is NOT a proposed
  pinned/unaccounted runtime cache or the real asynchronous ReadAt schedule.
- Existing2CPU/8GiB host, bounded diagnostic resource limits, private network
  and network syscall denial. Preserve remote state, export profiles/results,
  stop compute. No new cold-start/SLO acceptance or promotion.
  Artifacts: `/tmp/sandbox0-assist-index-cpu.AUsDHa`.

Result (2026-09-11; repeated index decode confirmed as isolated CPU hotspot):

- The exact full16/311-page Node/Coding indexes were decrypted from retained
  ciphertext archives and audited against454/9588 generic groups and2674/54824
  immutable member references. No key export or OSS client construction.
  Both measured workloads performed ZERO archive reads.
- 200 repetitions of unchanged sequential lookup over36/51 historical request
  shapes cost24.537/39.138ms wall and26.684/40.204ms CPU per sequence. Cumulative
  allocation churn is9.705MB/15.841MB and25,455/42,023 allocations per sequence,
  not retained heap/RSS. No real ReadAt/fill/async schedule was replayed.
- CPU samples: decodeAssistPage4.80s/90.06% of Node5.33s;7.74s/96.27% of
  Coding8.04s. JSON Decode55.91%/57.21%, page validation25.89%/29.60%, JSON
  Marshal7.69%/8.96%. Nested cumulative percentages are not additive categories.
  Repeated parsing/validation is now a measured hotspot in isolated lookup,
  not merely source-based speculation or a claim-wide attribution.
- Independent all-page component checks agree: Coding311-page full decode
  159.958ms, strict JSON parse94.844ms, validate49.908ms, canonical marshal
  16.752ms per pass (ten repetitions each). Node16-page full decode7.973ms.
  Components are independent timings, not a decomposed wall-time sum.
- Decision: implement verified parsed-page reuse in the EXISTING ReadCache
  entry/total/protected-mapping budget, with conservative retained-structure
  accounting and eviction. Preserve root/parent/range checks, checksum/canonical
  validation before insertion, corruption fallback, cross-root isolation and
  cancellation. Do not optimize away validation, add a second cache, pin the
  full index, increase budget/concurrency/timeout or introduce image rules.
  Then repeat matched CPU and replicated OSS comparisons. Cold index network
  waiting remains a separate unresolved cost; no predicted claim savings.
- Final28 assist and1 archive race tests passed; tested sources match the
  built/executed binary. All previous prototype index/reader/ownership source
  and the exact1,353-file product baseline remain unchanged. Existing1,082
  sealed predecessor files verified. No product implementation change this turn.
- Remote execution succeeded and18 exported files plus manifest were verified.
  Source rows/files/PIDs, Nomad job and64 detached NBDs unchanged; original
  ready count is2 before/after on this boot (prior boot was0), without a repair
  or a claim-capability assertion. No new objects, claims or guest commands.
- Harness limitation retained: systemd-run records requested network/mount
  restrictions, but post-exit systemctl show exposes unloaded/default values.
  Those properties do NOT attest live child confinement. Archive-only code
  and zero timed reads establish CPU I/O scope; any future confinement audit
  must capture live/self-observed properties. No probe was restarted.
- First cloud query during shutdown reports Stopping despite its filename;
  retain it and use the later terminal stopped-final receipt. Remote-test skill
  applied only to preserve/start/stop. No Kind deployment or production action.
- This is not full ReadAt, cold/cached new claim+node-v, high-density or2s
  acceptance. Full-index residency and parsed component fixtures are only CPU
  isolation, not a runtime cache policy. Authoritative publish/GC/lifecycle and
  populated-large/occupied production-width acceptance remain incomplete.
- Evidence: `/tmp/sandbox0-assist-index-cpu.AUsDHa/evidence.json`; raw profiles
  under remote-artifacts, normalized metrics/profile tables in profile-analysis.json.

### D-ASSIST-PARSED-CACHE — reuse canonical parsed pages within existing budget

Plan (2026-09-11; previous goal turn was progress):

- Address measured repeated decode using one optional parsed assist value in
  the existing ReadCache entry, not another content cache. Charge raw capacity,
  parsed slice capacities/strings/structures together; share current total and
  protected-mapping budgets and eviction. No image-specific/pinned cache.
- Preserve per-use root/logical-size/parent/range checks and canonical/checksum
  validation before caching. Reuse current independently cancelable shared
  flights and owned source permits. Test mixed-cache pressure, eviction, typed
  identity, corruption, parallel owners and cancellation before remote timing.
- If local gates pass, same-boot offline actual-index CPU baseline/candidate
  ABBA with identical instrumentation and200 sequential request-shape repeats.
  Capture effective child confinement in-process. No claim/guest command/OSS
  writes or production admission; actual network and end-to-end gates remain.
  Artifacts: `/tmp/sandbox0-assist-parsed-cache.oFqbmu`.

Result (2026-09-12; parsed-page implementation and isolated CPU gate passed):

- Added an immutable typed assist page to the SAME ReadCache entry as raw
  bytes. Raw capacity, parsed slice capacities/strings/structures are charged
  together; existing total/protected-mapping budgets and eviction are reused.
  Per-use root/logical-size/parent-level/range checks remain. Canonical and
  checksum validation precedes caching; shared decode work retains the existing
  owner/source permit through completion with independently cancelable waits.
- Verified duplicate insertion, actual retained raw capacity, mutually exclusive
  page types, mixed ordinary mapping/data pressure, eviction with held immutable
  references, zero/oversized cache budgets, wrong root/parent and malformed
  canonical bytes.64 concurrent owners share one decode; canceled followers
  leave promptly and a canceled leader does not poison a survivor.
- Same-boot actual-index offline A/B/B/A passed for Node and Coding. A is the
  PREVIOUS ASSIST with cached raw index bytes, NOT ordinary Reader without
  assist; B adds parsed-page reuse. Identical200 sequential whole-request-shape
  repetitions and10 all-page component repetitions. No actual ReadAt/fill/
  asynchronous claim schedule is represented by these CPU measurements.
- Mean per-sequence wall: Node23.323ms ->0.0913ms, Coding37.577ms ->0.1966ms.
  CPU25.253ms ->0.0987ms and38.550ms ->0.2009ms, respectively. Both repetitions
  agree; CPU falls99.61%/99.48% ONLY in this resident lookup workload. Allocation
  churn9.716MB ->24.749KB and15.835MB ->45.599KB per sequence, not retained RSS.
- Charged cache: Node534,917 ->1,163,601 bytes (+628,684); Coding11,206,886
  ->12,938,582 (+1,731,696). Entry counts remain16/311. Candidate protected
  charges972,011/2,690,081 bytes; total128MiB/protected16MiB unchanged. This is
  evidence of extra accounted metadata, not proof of multi-root physical density.
- All eight samples verify the original full index/member oracle and zero
  archive reads during timing/zero OSS calls. In-process evidence now proves
  private network namespace, denied INET socket with EPERM, NoNewPrivs/Seccomp,
  read-only archive/key mounts and writable diagnostic output mount.
- Two premeasurement harness failures retained: default seccomp deny killed
  the intentional socket probe with SIGSYS; adding errno return then exposed an
  observer requiring an exact child mount instead of inherited read-only root.
  Separate self-inspection proved effective restrictions. Corrected only errno
  behavior and most-specific mount observation, added a mount-rule test, used
  fresh unit/receipt identities and v2 binary names, and checked all measurement
  outputs absent before launch. Cache/workload code never changed during these
  harness corrections. Neither failed attempt is counted as a successful sample.
- 45 related and58 broader read-path tests passed;3 final harness tests passed.
  16 critical tests repeated20 times yielded320 passes. These are selected race
  suites, not a full-package or privileged runtime acceptance claim. Exact
  1,353-file product baseline and77 prior sealed artifacts preserved; compiler
  overlays only. No production code activation, merge/tag or baseline promotion.
- 72 remote exported files verified, including failed receipts and final eight
  profiles/results. Source rows/files/PIDs, Nomad job and64 detached NBDs remain
  unchanged. Original ready count1 before/after on this boot; not a repair or
  claim-capability assertion. Remote-test skill used only for preserve/start/
  stop. No OSS/RootFS object writes, graph rescan, guest commands or claims.
- Decision: proceed to matched replicated real-OSS whole ReadAt comparisons,
  retaining ordinary Reader and previous-assist references as needed. Include
  Node and Coding, empty application/crypto and shared-cache new Reader cases,
  full caller+async drain, actual source calls/bytes, CPU and cache charges.
  Do not translate isolated CPU savings into cold claim or first node-v savings.
- Cold index network cost and ordinary-Reader versus assist regressions remain
  open. Authoritative descriptor/publication/GC/lifecycle integration precedes
  any runtime admission. No regional-ingress claim+node-v, populated-large,
  occupied production-width or1s/2s acceptance was proved. No tenant prewarm,
  runtime cache/source-width/timeout increase, local e2e or production mutation.
- Evidence: `/tmp/sandbox0-assist-parsed-cache.oFqbmu/evidence.json`; paired
  metrics/cache costs in analysis.json, safety gates in verification.json and
  qualified failures/next action in qualification.json. The early cloud receipt
  named stopped-final actually says Stopping; terminal-stopped is the final proof.

### D-ASSIST-OSS-PAIRED — Matched real-OSS whole-read gate (2026-09-12)

- Isolate parsed-page reuse from generic assist network costs. Preserve ordinary
  Reader and raw-page assist controls. Node order O/A/B/B/A/O; Coding B/O/A/A/O.
  Each of twelve fresh processes measures empty application/crypto caches then
  a new Reader sharing that cache, with the exact prior immutable whole requests.
- One full generic fixture per image, no trace-selected publication. Fresh exact
  diagnostic prefix, conditional ownership journal, bounded writes, child reads
  only, exact ciphertext readback and post-cleanup absence. No source mutation.
- Measure caller+async drain, CPU, ranges/bytes and cache charges. No cache-size,
  source-width or timeout changes. This does not measure a new sandbox identity,
  node/kernel/gVisor/ingress, provider-cold, populated-large or production width.
  Artifact root: `/tmp/sandbox0-assist-oss-paired.IRbsn7`.

Result (2026-09-12; real-OSS CPU gain confirmed, generic assist not admitted):

- All12 fresh processes and24 cohorts completed on one unchanged2CPU8GiB
  boot, with exact immutable block verification, fresh application/crypto cache
  per process and zero provider reads in every shared-cache new Reader cohort.
  These are NOT new sandbox identities, claim/node-v or end-to-end measurements.
- Mean caller loop (including Reader open/SHA) plus async drain, empty cache:
  Node ordinary372.357ms, raw assist507.172ms, parsed assist502.521ms.
  Parsed does not establish a stable cold win over raw (overlapping ranges),
  and remains34.96% slower than ordinary. Both parsed samples exceed both
  ordinary samples. Coding ordinary1062.510ms, raw1017.603ms, parsed936.321ms:
  parsed7.99% below raw/11.88% below ordinary in these two mirrored repeats.
- Shared means: Node ordinary0.626/raw12.886/parsed1.733ms; Coding ordinary
  0.628/raw16.340/parsed1.651ms. Parsed reduces the assist penalty substantially
  but still exceeds ordinary. Empty CPU raw->parsed175.277->130.218ms Node,
  348.556->283.512ms Coding; shared24.466->2.912 and30.691->3.127ms.
- Node actual provider calls28 ordinary vs39 raw/parsed, returned ciphertext
  690,353 vs900,089 bytes (+30.38%). Coding73 vs64/65, bytes3,563,632 vs
  1,861,719/1,878,123 (parsed47.30% below ordinary). Index traversal itself
  still costs10 Node and29 Coding range calls. CPU savings do not remove them.
- Parsed Coding consistently adds one non-overlapping16,404-byte assist-data
  range; group windows23->24, grouped stored bytes607,882->613,086, async
  starts42->46. Cold hits/fallbacks46/5 and demanded decode units126 unchanged.
  Additional background activation is plausible, but per-callback causality was
  not instrumented. This extra work must remain visible for density evaluation.
- Parsed minus raw charged cache628,684bytes Node/1,733,921bytes Coding, the
  latter including changed background coverage. Budgets128MiB total/16MiB
  protected unchanged. No physical multi-root or production-width conclusion.
- Full generic fixture audit passed454/9,588 groups and16/311 pages.329 fresh
  conditional objects (30,336,789 ciphertext bytes) were exact-SHA read back,
  deleted and absence verified; archives retained. Source RootFS/DB unchanged.
- Raw35/candidate43 related race tests plus5 harness tests per binary passed;
  tested/built/executed hashes match.1,114 exported files verified. Only isolated
  compiler overlays and this ledger changed; no product activation/merge/tag.
- Unlike D-ASSIST-REAL-COST, every sample pair uses a fresh provider process;
  included Reader open costs134..181ms. The older probe reused the builder's
  provider/connection. Do not interpret cross-experiment totals as a runtime
  regression or infer uncached OSS service/kernel state. No p95/hard SLO claim.
- Decision: keep parsed-cache result; do NOT globally admit generic assist.
  Next design gate must reduce extra cold index traversal/network work using
  generic root-bound import metadata/cost eligibility, not image/trace-specific
  tuning, more cache/source width or higher timeouts. Avoid another CPU-only
  loop. Authoritative descriptor/publish/GC/lifecycle integration and actual
  regional-ingress claim+node-v/empty-node/occupied-width gates remain open.
- Remote-test skill used only for preservation/start/stop. Existing files/PIDs,
  job68814, source2,072/original263 rows and64 detached NBDs unchanged; ready1
  before/after. The early terminal-stopped.cloud receipt captured Stopping;
  stopped-confirmed.cloud after lifecycle completion is the required final proof.
- Evidence: `/tmp/sandbox0-assist-oss-paired.IRbsn7/evidence.json`; all samples,
  actual I/O and fixture cleanup are in analysis.json and the retained export.

### D-ASSIST-INDEX-GROUP — Generic adjacent index publication (2026-09-12)

- Previous turn is progress: parsed assist still made10/29 cold index range
  calls and regressed Node versus ordinary Reader. Test fewer transport rounds,
  not another CPU-only loop. Repack the complete generic index using existing
  mapping publication bounds (4pages/240KiB stored/1MiB decoded), preserving
  all leaf/member/data bindings and exact parent-owned child locators.
- Reuse ordinary mapping-group planning, existing assist flight ownership and
  source permits. Authenticate co-read sibling bytes before cache insertion;
  canonical parsing remains demand-only. No pre-claim fetch, whole-index pinning,
  bigger cache/timeout/source width, image-name or trace-derived layout.
- Gate packing/corruption/cancellation/fallback locally, then compare ordinary,
  prior parsed assist, grouped parsed assist in12 mirrored fresh processes,
  empty application/crypto cache followed by shared-cache new Reader. Same
  data groups, two complete index layouts. Include full caller+async drain and
  actual bytes/calls. No runtime admission or end-to-end claim/node-v assertion.
  Artifact root: `/tmp/sandbox0-assist-index-group.qtcUD4`.

Result (2026-09-12; fewer calls, but no generic cold-start admission):

- Repacked the full exact generic index, preserving454/9,588 groups,
  2,674/54,824 member bindings,16/311 pages and original data payload bytes.
  Grouped physical index objects5/79 vs16/311 original. Reused existing mapping
  publisher bounds and mapping-group planner, with owned assist source permits,
  independently cancelable flights and checksum-before-cache group delivery.
  Canonical parsed page validation and all per-use root/parent checks remain.
- Final baseline43/grouped49 related race tests and5 harness tests each passed.
  Six new packing/corruption/cancellation/zero-cache tests repeated20 times give
  120 passes. Before building, linked explicit repack caller cancellation to its
  independent base Reader lifetime and tested cancellation during blocked I/O.
  Earlier passing local receipts preserved; final tested/built/executed hashes
  match. No runtime source activation or claim behavior changes.
- All12 mirrored fresh processes/24 cohorts completed. Here baseline means
  PREVIOUS PARSED ASSIST, not the raw-byte repeated-JSON-decode prototype.
  Empty-cache mean caller+open/SHA+async drain, Node: ordinary349.556ms,
  baseline595.302ms, grouped451.345ms. Grouped wins both baseline samples but
  still loses to both ordinary samples. CPU131.116->101.434ms, ordinary62.859ms.
- Coding means ordinary1084.895ms, baseline890.825ms, grouped856.571ms.
  Grouped729.691/983.451ms straddles baseline852.577/929.072ms: no stable
  latency improvement proved. CPU289.593->249.635ms, ordinary300.251ms.
  Do not promote a small mean improvement amid this range into a hard bound.
- Index provider calls Node10->5/Coding29->20; total39->34 and65->56.
  Assist-data and original-rootfs calls/bytes are identical between layouts.
  Ordinary totals28/73. Index bytes88,323->125,234 and262,667->624,964;
  total900,082->936,993 (+4.10%) and1,878,094->2,240,391 (+19.29%).
  This is extra sibling download despite fewer calls. Ordinary bytes690,353/
  3,563,632 remain the correct control, not just the slower assist version.
- Node assist-data19 plus original fallback10 calls already total29 before
  any index call, versus ordinary28. Eliminating index calls alone gives no
  request-count advantage in this captured cold workload; this is not a formal
  latency impossibility proof. Do not continue by increasing grouping width.
- Charged cache grows191,990bytes Node/1,672,854bytes Coding, within unchanged
  128MiB total/16MiB protected budgets. Shared means baseline->grouped Node
  1.165->1.290ms/Coding2.205->1.984ms; ordinary0.652/0.623ms. All shared
  cohorts have zero provider calls; no physical-density or p95 claim follows.
- 413 conditional fixture objects (33,063,279 ciphertext bytes) were exact-SHA
  read back, deleted and absence verified; recoverable ciphertext archives
  retained.1,370 remote exported files verified. Original services/files/PIDs,
  job68814, source2,072/original263 rows and64 detached NBDs unchanged; ready1
  before/after. Compute stopped after export, with final cloud confirmation.
- Decision: retain diagnostic evidence, no generic grouped-assist admission.
  Next assess bounded metadata physical layout through the existing authoritative
  RootFS mapping, avoiding a second index traversal. Candidate MappingEntry has
  DataOffset views, but current DataRangeLayout only handles contiguous spans;
  noncontiguous topology-aware metadata placement is not implemented/enabled.
  Require complete generic topology, byte identity, single mapping authority,
  bounded mapping/transfer/decode/cache cost and ordinary Reader comparison.
- No image/trace-specific placement, user RootFS prewarm, larger cache/source
  width, timeout increase or local e2e. Remote skill supplied preservation and
  start/stop only, never Kind deployment. No product baseline promotion,
  production mutation, merge or tag. Current-main format parity is not proven.
- This remains whole immutable metadata ReadAt, not all reads, dirty branch,
  NBD/gVisor, new sandbox identity, real node-v or regional ingress. Sparse1TiB
  Coding/~5GiB allocated is not populated1TiB;2CPU8GiB is not occupied production
  width.1s/2s end-to-end gates remain open. Evidence:
  `/tmp/sandbox0-assist-index-group.qtcUD4/evidence.json`.

### D-METADATA-CLUSTERS — Ordinary mapping placement gate (2026-09-12)

- Previous turn is progress: grouped auxiliary index cost is measured and not
  generically admitted. This round removes that second traversal from the design:
  an isolated import-layout builder uses only ordinary format-two RootFS mapping
  and unmodified Reader. Product baseline1353 files remains byte-identical.
- Verify prior1433-file grouped-index seal,77-file complete directory-graph seal
  and exact historical request input. Plan from ALL named-class metadata blocks
  and immutable directory-star edges, never paths/image names/runtime requests.
  Node995 units/1,010 directories/1,537 edges; Coding23,344 units/15,893 directories/
  33,704 edges. All3 Coding directories over the old256-member limit are retained.
  Named classes are inode-containing and directory-data-fork blocks, NOT all XFS
  metadata. Include external directory extents beyond i_size.
- Deterministic weighted-edge merging gives disjoint at-most16-block/64KiB
  clusters. Singletons retain original placement. Authenticated source bytes are
  relocated through existing path-copy edits; encoded objects use DataOffset
  views, raw incompressible blocks retain complete independent checksums. Shared
  mapping publication retains4page/240KiB stored/1MiB decoded bounds. No custom
  Reader, runtime auxiliary index, RootFS prewarm, larger cache/source width or
  runtime timeout. Diagnostic descriptor still depends on original reachable
  objects: durable dependency inventory/registration/GC integration is NOT done.
- First unit run8pass/1fail: incremental validator correctly rejected the import
  mapping policy. Fix isolated builder normalization, not the product validator.
  Then9 passed. Canonicalize consecutive encoded views within inherited leaf
  boundaries; add complete-range and cross-leaf tests, giving final10 passed.
  Final stress20x10=200 passes and10 race passes. Earlier9-test stress/race
  receipts retained; selectors were expanded to include the new contiguous case.
- Full-byte ordinary Reader tests cover cache0/default/shared-new-Reader,
  partial/EOF, holes/unchanged entries, raw fallback, dirty branch, composite tail
  materialization, malformed/over-budget topology, source corruption/failure,
  cancellation and publication rejection. New object checksums/inventory match.
- Measured SYNTHETIC tradeoff, not network/startup latency: fixed128KiB read on
  alternating logical groups costs ordinary1 data-source call vs clustered32
  with cache0, or1 vs2 with default128MiB cache. Partial views cannot use ordinary
  bulk coalescing. Canonical contiguous groups recover1 call; cross-leaf byte
  consistency passes. Canonicalization does not remove noncontiguous-view costs.
- Frozen full-graph model: Node644 moved blocks,249..644 new views; Coding13,338
  moved blocks,6,464..13,338 new views. Lower bound assumes all clusters encode
  and no inherited leaf split; upper includes per-block raw fallback. Neither
  includes splitting inherited unchanged ranges: these are NOT net tree growth.
  Ordinary mapping byte volume mattered in the prior OSS control, so actual
  mapping I/O must be measured before claiming fewer directory-data reads wins.
- Historical36/51 whole metadata requests are modeled only AFTER planning.
  Node touches70 partition members (18 multiblock clusters), Coding92 (27).
  Modeled moved-cluster decode897,024/1,486,848bytes; singleton demanded bytes
  212,992/266,240. These counts are NOT source calls: singleton physical ranges,
  actual codec sizes/checksum dedup, encryption frames, coalescing, mapping
  fetches and cache competition are excluded. Do not compare to an assumed
  globally aligned64KiB baseline. No trace-derived graph tuning after results.
- Five-package regression358pass/2fail/11skip: rootfsblock218/0/3,
  rootfsartifact14/2/2, importer36/0/3, session87/0/3, objectstore3/0/0.
  Both XFS traversal/overlap-mutation failures reproduce WITHOUT the overlay:
  local stat device31 differs from expected30 before intended assertions. Do not
  suppress them or claim the broader suite is green; baseline failure receipts
  and initial builder failure are retained. No unrelated product fix performed.
- Decision: byte-correct isolated layout prototype, NOT runtime admitted.
  Freeze planner and audit both exact real roots' mapping growth, actual codec
  and encrypted I/O costs using bounded, journaled remote fixtures before full
  startup trials. If that cost gate is negative, stop this candidate instead of
  widening groups/cache/timeouts. No current-main format parity or durable
  publication integration proved. No remote compute started in this round.
- This round ran0 claims/0 guest commands; model is not NBD/gVisor or regional
  ingress. Empty-node/new-identity cached claim+real node-v, occupied production
  width, populated-large-root and1s/2s end-to-end gates remain open. Coding is
  sparse1TiB/~5GiB allocated, not populated1TiB. No production/merge/tag changes.
  Full changes, source snapshots, model and test receipts:
  `/tmp/sandbox0-metadata-clusters.LI7ED6/evidence.json`.

### D-METADATA-OSS — Frozen layout on actual encrypted roots (2026-09-12)

- Previous turn is progress: topology model and byte-correct prototype exposed
  fragmentation risk. Freeze its planner, weights,16-block limit, canonical
  builder and source SHA; do not tune against the observed request shapes.
  Use the original995/23,344 full-inventory block oracles and exact root bindings.
- The isolated harness builds a new ordinary format-two descriptor through
  existing mapping, audits EVERY reachable mapping page/parent binding, then
  verifies every inventoried4KiB unit through the UNMODIFIED ordinary Reader.
  Existing data and maps remain read-only. No auxiliary index, registration,
  tenant prewarm, runtime/cache/source budget change or timeout increase.
- Final19 related race tests plus6 harness race tests passed, including exact
  mutation scopes, immutable cleanup, mirrored sample identity, full oracle and
  ordinary mapping audit/cancellation. The initial harness4-test receipt remains;
  final tested/built/executed source hashes match. The previous two broader XFS
  baseline failures are not asserted fixed or covered by this narrower test run.
- New mapping views match model lower bounds: Node249/Coding6,464; all65/1,254
  multiblock clusters compressed. Singletons351/10,006 retained. Actual net data
  view growth is286/8,543 because inherited unchanged ranges also split.
  Partial views0->339/10,285; full tree pages6->9 and80->132; mapping objects
  3->5 and21->43. Mapping decoded bytes830,273->886,840 and15,608,461->17,301,753;
  unique stored mapping-range bytes185,581->195,877 and3,442,721->3,678,300.
  These are locator/page geometry, not complete packed-object or provider bytes.
- Diagnostic build costs0.847s Node/18.696s Coding, only import/audit work, NEVER
  claim time. New complete objects4/29; plaintext348,416/6,946,729bytes. All995/
  23,344 metadata units exactly match the retained SHA oracle; no byte mismatch.
- Eight mirrored fresh processes,16 cohorts: Node ordinary/layout/layout/ordinary;
  Coding layout/ordinary/ordinary/layout. Each has empty application/crypto cache
  then shared-cache NEW READER. No builder provider/cache reuse in timed children.
  Reader-open and whole request+SHA costs included; host/provider cache emptiness
  and new sandbox identities are NOT implied by a new process.
- Empty-cache whole metadata ReadAt Node ordinary386.371/352.487ms
  (mean369.429), layout476.669/455.555ms (466.112,+26.17%). Coding ordinary
  984.100/901.365ms (942.733), layout1175.520/1087.338ms (1131.429,+20.02%).
  Both layout samples are slower than BOTH ordinary samples for each root.
  Two samples are not p95 or a hard bound; this is enough to reject this gate,
  not to claim an end-to-end startup regression or universal impossibility.
- Actual provider calls Node28->39 and Coding73->87; cipher bytes690,353->
  839,537 (+21.61%) and3,563,632->2,874,118 (-19.35%). Coding's lower total bytes
  do NOT mean less data amplification: mapping bytes2,589,181->1,549,881 while
  data bytes974,451->1,324,237. Data calls57->69, mapping16->18. Node data calls
  26->36, mapping2->3; data bytes513,590->695,058. Removing the extra index does
  not remove this fixed layout's increased data-source work.
- Mean CPU Node59.930->74.988ms, Coding291.474->269.656ms. Charged cache Node
  3,219,747->3,392,993bytes; Coding20,486,661->16,446,350bytes. Shared new-Reader
  means ordinary/layout Node0.618/0.706ms, Coding0.559/0.729ms, all0 provider
  calls/bytes. No bigger or independently pinned cache was introduced.
- Decision: REJECT this directory-star metadata placement; no runtime admission.
  Do not widen groups, retune graph weights from traces or deploy it for startup
  timing. Return to authoritative regional-ingress claim followed by real node-v
  evidence on the unchanged best baseline, isolating remaining mandatory serial
  reads/runtime phases. A metadata-only win would still not prove the objective.
- Original services/files/PIDs, job68814/source2,072/original263 rows preserved;
  ready1 before/after,64 detached NBDs, physical empty.33 conditional fixture
  objects totaling7,331,187 ciphertext bytes exact-SHA read back, deleted and
  absence verified; recovery ciphertext archives retained.197 exported files
  verified before compute stop. Skill supplied preservation/start/stop only;
  no Kind, production rollout, merge, tag or product-source changes. Final
  cloud-stopped receipt is required for closure, not just an SSH disconnect.
- Still0 claims/0 guest commands. Fixed2CPU8GiB is not occupied production width;
  Coding sparse1TiB/~5GiB allocated is not populated1TiB. Empty-node and
  node-cache/new-identity claim+node-v, actual width and1s/2s gates remain open.
  Evidence and exact source/IO/cleanup records:
  `/tmp/sandbox0-metadata-oss.qdMqH6/evidence.json`.

### D-CONSTRUCTOR-AUDIT — Bound the direct root-page cost (2026-09-12)

- Diagnostic only: reanalyze sealed D-MAPPING-TRACE before proposing an inline
  root-page claim grant. No remote start, new claim/command, tenant RootFS read,
  source change, timeout increase or benchmark-baseline promotion. All 164
  indexed historical files and all 1,353 current candidate source files match
  their frozen hashes; the exact product path inventory is also unchanged.
- Both ctld processes were alive, but standby b recorded zero NBD tasks, reads
  and source attempts in BOTH cold and cached-new windows. Primary a recorded
  7,187/7,186 reads and 396/214 source attempts. This excludes concurrent A/B
  read caches in these traces, not every possible whole-lifecycle HA failure.
- Match each root load by descriptor digest, hashed object key, exact offset,
  encoded size and decoded member checksum/length. Each image has ONE cold
  source attempt, shared by four pre-device mapping callers, and zero cached-new
  root source attempts. Do not multiply the shared wall time by four.
- Node root page: 485 encoded / 982 decoded bytes; source 64.227ms; complete
  mapping call maximum 64.286ms (four callers 50.041–64.286ms). Coding: 5,177 /
  15,200 bytes; source 63.918ms; mapping maximum 64.120ms (33.672–64.120ms).
  Exact source attempts are enclosed by mapping regions and three matching
  shared-flight waiter edges. No exact mapping-goroutine-to-sandbox identity
  is invented. Six other unowned source attempts all start after every first
  command completed; exclude them from constructor cost.
- Optimistic arithmetic, NOT a benchmark or causal bound: subtract each image's
  largest full root-mapping interval from every cold combined sample while
  holding everything else fixed. Coding remains 2.601707–2.601849s, versus
  measured 2.665827–2.665969s. Node would be 1.952943–1.953026s versus measured
  2.017228–2.017311s. The Node-only arithmetic does not admit a generic fix.
  Network initialization can move to a later mandatory source read; actual
  savings may differ. No claim or command speedup was measured this turn.
- Decision: do not prioritize a new inline-root claim contract as the sole
  combined-gate solution. Its direct observed cost is about 64ms, not the
  remaining approximately 666ms Coding miss. Do not rule out a bounded root-page
  optimization as one future component if independently justified. Reuse the
  existing post-mount demand-chain evidence instead of another root-page-only
  runtime trial or the rejected metadata grouping/cache parameter sweeps.
- Four local analysis-helper tests / 16 assertions pass; git diff check passes.
  This is not a product regression test run. Current-main/runsc parity, actual
  occupied width, populated large RootFS, public ingress and the complete
  cold/cached-new 1s/2s acceptance remain open. Private-regional instrumented
  16CPU/64GiB traces are not new production-width measurements.
- Evidence: `/tmp/sandbox0-constructor-audit.loVxKU/evidence.json`, SHA-256
  `0888da5ca100ad0ec0f9013d01468da39f9c3d5afc943037fa1f93e7d5c8051b`.

### D-INLINE-DATA-COST — Full-map inline-payload feasibility (2026-09-12, planned)

- Previous turn is progress: root-page direct cost is about64ms, not a sole
  solution for the Coding combined miss. This probe examines metadata payload
  placement in the authoritative leaf itself, not a second index or a promise
  that shrinking any RootFS stage establishes the full gate.
- Node22.23.2 source excludes an intervening hypothesis: --version returns
  before extra-CA/OpenSSL/V8 initialization. Do not remove TLS trust settings or
  describe CA parsing as the measured node-v bottleneck. Source:
  https://github.com/nodejs/node/blob/v22.23.2/src/node.cc#L1063.
- Unlike the rejected generic_inline_index_feasibility_run, actual data bytes
  would accompany the mapping page, eliminating a subsequent payload object
  request. Unlike D-METADATA-OSS, separate cluster-data fetches would not remain.
  A new representation is NOT admitted by this hypothesis; page bloat, splits,
  cache charging, partial views, path-copy and authenticated format/version
  compatibility must be tested before any runtime use.
- First export complete original mapping geometry using the ordinary Reader
  and exact existing roots. Reuse the full995/23344-unit metadata size/SHA oracle;
  never select units by observed command/path/latency. Model80-byte hypothetical
  inline records plus stored payload, external fragment/key costs and1024-entry
  fanout. Raw model bytes are not compressed page bytes or actual provider I/O.
- Local diagnostic Go overlay only; no product source, root format, cache,
  parallelism, timeout or readiness change. Remote read-only scope: original
  prefix only,512MiB requested ciphertext,10MiB/range,20000 calls, no object
  Create/Put/Delete/Head/List. No NBD/mount/runtime/DB mutation. Preserve original
  files/PIDs/job/rows and64 detached devices, export verified results, stop compute.
- Fetched sandbox0 main0f09220460581bfc1fdc331f34ebc85bf38381e7 and infra main
  ceb895c; the latter only refreshed image pins and still documents runsc17.
  Candidate remains1353 dirty-worktree files and historical roots, not main
  runtime artifacts. Skill contributes preservation/lifecycle only; no Kind.
- Root: `/tmp/sandbox0-inline-data-cost.NPPrGU`. Exact test/build/input hashes
  precede remote launch. No new claim/command or startup acceptance yet.

Completed result:

- The ordinary Reader authenticated every original mapping page and child
  binding: Node6 pages/5 leaves/4341 data views; Coding80/79/80781. Mapping-only
  provider reads were6/80 calls and237,705/4,436,211 returned ciphertext bytes.
  This scan is not a claim, command, cold-node or production-width benchmark.
- Frozen whole-image inventory:995 Node and23,344 Coding metadata4KiB units.
  Hypothetical inline records plus independently encoded payloads increase Node
  leaf entries4341->5297, leaves5->6, uncompressed leaf bytes829,291->1,345,765
  (+62.28%). Coding entries80781->103826, leaves79->102, leaf bytes15,593,261->
  28,137,181 (+80.44%). Maximum modeled leaf368,644/492,478 bytes stays below
  8MiB; this does not establish compressed/group size, actual wire validity,
  cache cost or provider traffic for a new representation.
- Of78/2197 original source ranges touched by metadata,39/1746 remain referenced
  by external bytes. They contain371/16,396 selected units (37.29%/70.24%).
  Inline-only reads might remove a data GET, but mixed reads can still fetch the
  original range and duplicate those bytes. Do not count entire original packed
  objects as reclaimable or infer a latency win from fewer logical references.
- Decision: allow only a bounded byte-correct codec/cost probe next, not runtime
  admission. Keep the same complete metadata selection, test actual encoded
  mapping/group/crypto costs and cache charging, including mixed external views.
  Explicit version, parent and checksum rejection plus Reader/path-copy,
  materializer, GC, inventory and mixed-version contracts must precede activation.
  No trace-selected metadata, parameter sweep, extra cache or tenant prewarming.
- Eight focused Go race tests and8 model tests/16 assertions pass. Test/build
  source hashes match; all1353 product files and the full path inventory remain
  unchanged. No product implementation or new root format was introduced. Older
  broader XFS device-test failures are not reclassified as fixed.
- Read-only remote checks preserved original files/PIDs, ready1, job68814,
  source2072/original263 rows and64 detached NBDs; no object/DB/runtime mutation.
  Fifteen exported files and the41,482,240-byte archive were checksum verified.
  Probe unit terminated successfully; cloud confirmed Stopped/StopCharging at
  2026-09-11T18:18:09Z and SSH master is terminal. Skill supplied environment
  preservation/lifecycle only; no Kind or production operations.
- Zero new claims/guest commands; no measured startup improvement. Full regional
  ingress claim followed immediately by node-v, empty-node vs cached-new
  identities, occupied real width, populated-large roots and1s/2s gates stay open.
  Evidence: `/tmp/sandbox0-inline-data-cost.NPPrGU/evidence.json`, SHA-256
  `cabdc5ea9b31a6585e4a6220c62c60691dfb17ad93c8403b110a27200a986d54`.

### D-INLINE-CODEC — Actual inline wire cost (2026-09-12, planned)

- Previous turn is progress: full-map model sealed; actual compression and
  mixed-view costs remain unknown. Isolated diagnostic codec only, experimental
  version65534 rejected by production Reader, no durable format activation.
- Reuse existing mapping validation, bounded range zstd/checksum and publication
  group bounds. Test malformed lengths/flags/checksums, parent binding, truncated
  payloads, raw fallback and mixed external decoded views. Preserve all995/23344
  metadata units from the same exact original images; no demand-based selection.
- Measure actual compressed leaf sizes and same-limit grouping, including
  retained slice/key/decoded-entry charges. Real encrypted-memory tests distinguish
  inline-only data GET removal from mixed reads still fetching the old range;
  neither memory transport nor whole-image codec time is startup latency.
- Remote read-only original prefixes,512MiB/20000calls/10MiB-range per image;
  no NBD, mount, object, DB or runtime mutation. Preserve environment and stop
  compute after checked export. Skill supplies preservation/lifecycle, not Kind.
- Root: `/tmp/sandbox0-inline-codec.DGzvQW`. Product source remains unchanged;
  goal and complete cold/cached-new claim+node-v acceptance remain open.

Completed result:

- All995/23344 original metadata4KiB units read through the ordinary authenticated
  Reader and verified against the immutable SHA oracle. Independently encoded
  payload totals exactly reproduce the previous444,291/10,733,371-byte oracle.
  Every new leaf passes bounded encode/decode/re-encode equality, including
  inline checksums. Original leaf encoding exactly reproduces original stored
  totals. No new root/internal tree or durable format was published.
- Actual compressed leaves Node185,096->599,202 bytes (3.237x), Coding3,437,544->
  13,308,474 (3.872x). Uncompressed bytes and entry/page counts exactly match the
  prior model. Same fixed publication limits yield total leaf groups2->3 and
  20->68; Coding has11 oversized singleton pages, maximum340,975 stored bytes.
  Group counts are layout accounting, NOT observed provider-call counts.
- Retained leaf allocation charge Node2,176,956->3,031,739 (+39.27%), Coding
  40,827,822->61,008,327 (+49.43%). Inline charge includes decoded entries,
  keys/checksums, wire backing buffer and payload-slice backing array; inline
  payload slices alias the wire. No independently retained decoded payload cache
  is counted. This is neither actual eviction behavior nor peak node memory.
- AES-GCM/RSA and ChaCha20/RSA focused encrypted-memory tests use real16KiB
  frames and current bounded immutable-header/range cache policies. Inline-only
  reads issue0 data GETs; mixed reads still issue a data GET, like ordinary.
  AES example: ordinary62,621 returned ciphertext bytes, inline-only670,
  inline-mixed62,689. Each map has1 provider GET across cold then shared-cache
  helper calls. These synthetic transport helpers are NOT production Reader
  integration, new sandbox identities, network samples or startup latency.
- Apply the frozen historical whole-metadata requests ONLY AFTER the complete
  layout is encoded; no request/phase/latency selects the inline units. Node36
  requests require3->4 distinct pages and1->3 groups, stored grouped bytes
  174,410->599,202. Coding51 requests require20->21 pages and15->20 groups,
  2,567,720->4,303,423 stored bytes. Do not assume every startup downloads all
  13.31MB, or equate full-image growth with startup regression.
- Demand-group accounting assumes no eviction/fallback and the existing grouping
  policy, carrying group state from historical claim to first-command phase.
  It excludes root/internal pages, crypto amplification and mixed external reads.
  Fewer serial payload requests could still offset larger mapping reads; only an
  actual net demand-I/O comparison can decide. No latency improvement or universal
  rejection is established here. No runtime admission, threshold sweep, extra
  cache, prewarm, timeout change or production rollout.
- Twelve focused Go race tests pass, including2048-unit repaging, malformed
  records, checksum/parent rejection and raw-partial-view refusal. Initial focused
  tests also passed before adding full-cost coverage. Five-second fuzz smoke
  passed52,509 executions but started from invalid seeds; not exhaustive parser
  coverage. Broader historical XFS device failures remain outside this run.
- All1353 product-source files/inventory unchanged. Remote preserved original
  files/PIDs, ready1, job68814, source2072/original263 rows and64 detached NBDs.
  Source collection86/2334 provider reads and2,030,807/57,374,921 returned cipher
  bytes are probe-building costs, not transport measurements of the new format.
  Sixteen files and17,643,520-byte archive verified. Unit succeeded; ECS freshly
  confirmed Stopped/StopCharging2026-09-11T18:41:24Z; SSH master terminal.
- Next gate: compare complete ordinary vs diagnostic inline demand reads with
  identical metadata selection, actual crypto/group reads and bounded cache
  charging, including mixed-view duplication. Reader/path-copy/materializer/GC,
  inventory/mixed-version and regional-ingress cold/cached-new claim+node-v,
  occupied real width and populated-large-root1s/2s acceptance remain open.
  Evidence: `/tmp/sandbox0-inline-codec.DGzvQW/evidence.json`, SHA-256
  `9b94748bb44a4ba7938f2ac138eae23a13b8d5005aa0a10f00f04147d6d89a36`.

### D-INLINE-DEMAND — Actual encrypted inline demand reads (2026-09-12, planned)

- Previous turn is progress: actual leaf compression and historical request
  coverage quantified growth, not net latency. Add explicit diagnostic-only
  Reader via local Go overlays; ordinary Reader and durable mapping encoder
  reject experimental pages, including cache hits and shared page-flight results.
- Reuse existing mapping groups, crypto, source admission and128MiB LRU. Inline
  payloads alias authenticated mapping bytes; parsed map overhead is charged.
  No independent encoded cache, pinning, trace-selected metadata or tuning sweep.
- Publish only unique experiment-owned encrypted mapping objects, never register
  RootFS artifacts or change original descriptors. Every995/23344 metadata unit
  must match its immutable SHA oracle. Original roots remain pinned and verified.
- Two fixed workloads: exact historical whole-metadata requests, and their
  deduplicated covering128KiB aligned ranges to expose mixed inline/external
  reads. The latter's full-block SHA oracle is generated from the original
  authenticated Reader AFTER metadata selection; it never chooses inline units.
- Node ordinary/layout/layout/ordinary; Coding layout/ordinary/ordinary/layout.
  Fresh process per family sample; separate empty application/crypto cache then
  shared-cache new Reader for each workload. No host/provider-cold or new-sandbox
  interpretation. Full request+open+SHA time, provider I/O and cache charge count.
- Unit/race coverage includes mixed bulk reads, small/disabled caches, concurrent
  readers, cancellation, encrypted corruption and cached/flight format rejection.
  Initial compile test used a wrong object-kind constant; corrected before tests.
  Diagnostic source hooks do not change product files or production binaries.
- Skill supplies preservation/lifecycle, not Kind. Journal conditional writes,
  retain ciphertext archives, verify exact identity before owned-object deletion
  and verify absence; preserve original files/PIDs/DB/NBD and stop compute.
- Root: `/tmp/sandbox0-inline-demand.O4DlTj`. No startup acceptance yet; complete
  regional-ingress claim+node-v, empty-node/cached-new, occupied width and
  populated-large-root1s/2s gates stay open.

Completed result:

- Initial remote attempt failed BEFORE publication/timing: the retained full
  metadata inventory enumerates classes, not sorted logical offsets. Node first
  inversion12,885,983,232->53,248; Coding825,707,778,048->131,072. All995/23344
  offsets are unique. The strict builder correctly rejected this order. Zero
  owned objects, cleanup complete, original state preserved;17 files exported.
  Do not treat that failed attempt as a timing sample or silently erase it.
- Independent retry `/tmp/sandbox0-inline-demand-retry.vWdCNb` sorts a COPY of all
  original offset/checksum pairs; no selection or changed policy. Added a real
  complete-inventory regression test.31 focused race tests pass, including
  experimental wire rejection after cached/page-flight delivery, mixed/unaligned
  bulk reads, zero/tiny/bounded caches, concurrent Readers, crypto corruption,
  mixed-version tree rejection and exact owned-object cleanup. Initial compile
  typo and subsequent coverage additions remain recorded; no product files changed.
- All995/23344 inline units byte-verified through the diagnostic Reader. Root and
  every leaf are authenticated; no RootFS artifact registered. New metadata flags
  and payload index exist only in Go overlays. Ordinary encoding rejects them;
  the ordinary comparison uses the same hook binary with diagnostic opt-in OFF.
- Eight fresh family processes,32 cohorts: two fixed workloads, each with empty
  application/crypto cache then shared-cache NEW READER. NodeABBA, CodingBAAB.
  36/51 exact historical whole-metadata requests and19/34 covering128KiB mixed
  requests. Complete request+Reader-open+SHA time is counted. Fixed workload order
  means the mixed workload can reuse process/network state, but not crypto/range
  cache state. Neither workload proves empty-host/provider or new-sandbox startup.
- Empty-cache metadata means Node388.247->258.524ms (-33.41%); Coding847.431->
  726.703ms (-14.25%). Two individual inline samples are below both ordinary
  samples for each root. Provider GETs28->4 and73->23. Returned cipher Node
  690,353->603,868; Coding3,563,632->4,334,822. Fewer calls can offset MORE bytes,
  but this isolated metadata gain does not establish the startup objective.
- Mixed means Node156.794->309.829ms (+97.60%), Coding628.379->1047.484ms
  (+66.70%). BOTH inline samples exceed BOTH ordinary samples for each root.
  GETs12->28 and61->92, despite slightly LOWER total cipher1,953,461->1,937,658
  and6,972,673->6,733,403. This is a verified rejection of the current gate,
  not an end-to-end startup regression or universal rejection of inline data.
- Exact provider-event split: mixed Node mapping2->4 calls, data10->24;
  Coding mapping16->23, data45->69. Node data bytes1,776,698->1,333,790 and
  Coding4,383,492->2,398,581. More/smaller data GETs identify a specific issue
  to investigate alongside `fullDataRange` eligibility: partial external views
  cannot use the existing whole-range bulk coalescer. This is not an exact
  application critical-path attribution or proof that one change recovers all loss.
- All shared-cache cohorts have0 provider GETs/bytes, but inline reads are slower:
  metadata Node0.735->2.309ms, Coding0.624->2.893ms; mixed2.321->7.196ms and
  3.640->10.223ms. Cold mixed CPU means76.943->118.922ms and374.788->499.433ms.
  Current parser verifies all embedded payload checksums eagerly and demand reads
  decode again; no lazy-validation optimization was inserted into this comparison.
- Charged cache falls: metadata Node3,219,747->2,663,370 and Coding20,486,661->
  16,167,181 bytes; mixed10,104,855->6,279,008 and42,602,377->23,155,478. Lower
  retained bytes do NOT imply faster reads. Existing128MiB total budget, source
  admission and crypto policy remain fixed; parsed inline map overhead is charged.
- Decision: REJECT runtime admission for this implementation. Next isolate bulk
  planning over partial external views and retained original ranges. Any generic
  coalescing change must preserve inline overrides, independent source checksums,
  holes, unaligned reads, cancellation and bounded memory. Keep the same metadata
  set/codec/cache/thresholds; do not rerun unchanged or tune them from these samples.
- 73 exact new mapping objects (4Node+69Coding) totaling13,993,873 ciphertext bytes
  read back by SHA/size, deleted, and absence verified; ciphertext archives retained
  for recovery.332 retry files and35,092,480-byte archive verified. Original
  files/PIDs, ready1, job68814, source2072/original263 rows and64 detached NBDs
  preserved across both attempts. ECS freshly Stopped/StopCharging19:10:36UTC;
  shared SSH master terminal. No production operations or new claims/commands.
- All1353 product files/inventory unchanged.31 tests are focused, not a broad
  product or privileged runtime regression claim. Real regional-ingress claim+
  immediate node-v, empty-node/cached-new identities, occupied actual width,
  populated-large roots, production-main/runsc parity and1s/2s gates remain open.
  Evidence: `/tmp/sandbox0-inline-demand-retry.vWdCNb/evidence.json`, SHA-256
  `476c7ead023a8e85caf5727a56214ec36e6dd0c60a8a30ef37fb551a50bfea11`.

### D-PARTIAL-PROJECTION — Partial-view coalescing correctness and complete geometry (2026-09-12)

- Previous turn is progress: encrypted metadata demand improved, but mixed data
  GETs increased10->24 and45->69. Current inline runtime admission remains
  REJECTED. Isolate the missing partial-view bulk path before any remote rerun.
- Local diagnostic overlays only. Keep all995/23344 independently inventoried
  inline units, codec,128MiB shared LRU, admission,128KiB demand threshold and
  1MiB coalescing limits unchanged. No tenant RootFS prewarm, timeout change,
  object publication, remote start, claim, command or production mutation.
- Separate exact immutable physical source frames from logical mapped views.
  Deduplicate identical ObjectRange values, join only contiguous physical offsets
  in one object, verify each decoded source using the existing loader, then
  assemble logical DataOffset views and inline overrides into a caller buffer.
  Never cache projected bytes under an original source checksum. Existing tail
  overrides remain authoritative. Ordinary full-range fast path is unchanged.
- Planner stays within the authenticated leaf and aligned1MiB logical window;
  source decoded bytes, stored span and caller output each stay at most1MiB.
  Tiny/disabled caches clip the window to demand. Holes, noncontiguous sources,
  backwards physical order, conflicting immutable locators and crossing views
  stop planning. A group transport failure falls back to exact demanded views;
  a corrupt neighbor cannot hide a separately healthy demanded prefix.
- Whole original geometry is reconstructed from SHA-verified exported mapping
  profiles, then split using ALL sorted metadata units and repaged1024 entries.
  Every reconstructed leaf start/span/entry/inline count must match the prior
  actual encrypted builder output. Hashed object keys preserve equality only;
  inline markers in this static audit are not authenticated payloads or Reader
  input. No workload selects layout. Entry/request intersections are NOT GETs.
- Additional caller assembly is bounded but is not a node-level peak-memory
  proof; eager inline validation and repeated decoding are unchanged. Reduced
  planning fragmentation does not establish remote latency or startup benefit.
- Initial new ordinary-alias test omitted NewReader's cache-size argument and
  failed compilation; the captured failure is retained. Corrected before the
  focused retry. An earlier patch transaction with two operations on one path
  was rejected atomically; it did not change files. No failed attempt is timing.
- Root: `/tmp/sandbox0-partial-plans.yyFMSb`. Complete package race verification
  and artifact sealing follow; regional-ingress claim+immediate node-v, empty
  node/cached-new identities, occupied actual width, populated-large roots,
  production-main/runsc parity and1s/2s startup gates remain open.

Completed result:

- All230 top-level package tests passed with race detection;2 environment-gated
  tests skipped (one-TiB fully mapped model and RustFS ten-thousand-generation
  scale test). All9 new projection/geometry tests passed. This is package-level
  regression coverage, not privileged runtime or startup acceptance. The whole
  race suite took347.492s; test execution time is NOT a sandbox latency sample.
- Byte fixtures cover inline and tail precedence, ordinary compressed partial
  aliases with reordered DataOffsets, zero/tiny/128MiB caches, corrupt adjacent
  frames, transport fallback, cancellation and concurrent roots with different
  inline overrides sharing original physical frames. The32-block mixed fixture
  uses one local data GET. Original-root readers retain the original bytes.
- Complete geometry matches the prior actual builder: original4341/80781 views
  become5297/103826 entries,5->6 and79->102 leaves, with all995/23344 inline units.
  Every new leaf's start/span/entry/inline counts match; source locator bounds and
  every admitted plan's logical/physical continuity and1MiB limits are checked.
- Under the unchanged128MiB policy, fixed mixed workloads intersect22Node and
  64Coding external entries (each counted per request intersection). Old planner
  eligibility3->14 and13->56;11Node and43Coding newly eligible intersections,
  of which11/41 are partial views. Remaining8/8 intersections still fall back.
  These are NOT actual provider counts and cannot be subtracted from last turn's
  GETs. They establish coverage of the fragmentation hypothesis, not net speedup.
- Whole-geometry synthetic128KiB demands add22Node and1788Coding eligible
  entries, including22/1743 partial entries. No previously eligible ordinary or
  inline target loses eligibility in either0-byte or128MiB policy. Ordinary
  full-entry dispatch still uses the old fast path; conservative fallback remains.
- Decision: pass local correctness/geometry gate, not runtime admission. Next
  compare ordinary, prior-inline and projected-inline actual encrypted demand
  reads using identical frozen layout, workloads, cache and thresholds. Count
  mapping/data GETs, ciphertext bytes, CPU, elapsed time and cache cost together.
  Do not enlarge caches/windows, change metadata selection or rerun unchanged.
- All1353 product files and inventory unchanged;393 prior experiment artifacts
  and frozen overlay inputs reverified.22 current artifacts sealed. No remote
  start/query, object mutation, claim or command this turn. Last known remote
  checkpoint is the prior turn's Stopped/StopCharging19:10:36UTC, not a fresh
  remote observation. No production changes. All whole-startup gates remain open.
  Evidence: `/tmp/sandbox0-partial-plans.yyFMSb/evidence.json`, SHA-256
  `5aeb964616b5386a8a1b5f7f938533f7eed8d12b694dec4fb49fd58a99371383`.
  Artifact index SHA-256:
  `6aa626d2b3087dbff04b34079e0192e242592725a7ffdf930cb03d9d520b512f`.

### D-PROJECTED-DEMAND — Three-way encrypted demand-read comparison (2026-09-12, planned)

- Previous turn is progress: byte-correct partial projection and complete mapping
  geometry qualified a remote net-benefit test, not runtime admission. Fresh
  main remains0f092204; infra mainceb895c. Protect the193 existing dirty entries.
- Two binaries use the identical harness, frozen previous vs projected Reader
  overlays, with explicit binary/family matching and per-process binary SHA.
  One new encrypted inline layout per image is shared by BOTH inline families;
  ordinary reads retain the original immutable descriptor. All metadata units,
  codec,128MiB cache, source admission,128KiB demand and1MiB window stay fixed.
- Node ordinary/layout/projected/projected/layout/ordinary; Coding
  projected/layout/ordinary/ordinary/layout/projected.
  Twelve fresh family processes,48 cohorts: historical metadata then fixed
  covering128KiB mixed reads, each with separate empty application caches then
  new Reader sharing caches. No host/provider-cold, new sandbox, guest or ingress
  interpretation. Count open+whole-read+SHA, provider mapping/data reads, cipher
  bytes, CPU and cache charge; process setup is outside the measurement.
- Keep exact full995/23344-unit and mixed-block SHA oracles. Only conditional,
  uniquely owned experimental mapping publications are allowed; journal and
  archive ciphertext before write, verify exact SHA/size before cleanup, then
  verify absence. Original RootFS objects, binaries, PIDs, DB/job/NBD stay intact.
- Read remote skill for preservation/lifecycle; use workspace Makefile only for
  verified Singapore ECS lifecycle, never historical Kind deployment. Fresh
  preflight: bootcd687286-eb31-4fe0-8f96-9c706196cda3,2CPU8GiB,ready1,
  job68814,2072 source/263 original rows,64 detached NBDs, physical runtime empty.
- Test harness and both overlay test commands pass; binaries previouse36fec6e
  and projected94154a03. Initial local artifact-copy patch exceeded tool output
  size and was rejected without writes; the large immutable JSON now stays at
  its verified source path and is copied directly during remote staging.
- Root `/tmp/sandbox0-projected-demand.kJjdYn`. No runtime rollout/claim/command.
  Regional-ingress claim+immediate node-v, empty-node/cached-new identities,
  occupied real width, populated-large roots and1s/2s acceptance remain open.

Completed result:

- Twelve fresh processes,48 cohorts complete with byte checks and no sample
  publication/prohibited operation. Both inline families share each image's
  exact build-receipt SHA and encrypted mapping objects; binary SHA/implementation
  matches the assigned family. Full995/23344-unit inventory verified.54 focused
  race-test executions across the two builds (31 distinct top-level tests) pass.
- Mixed empty-application-cache means ordinary/prior-inline/projected:
  Node125.540/275.545/269.213ms; Coding602.700/964.165/866.731ms. Both projected
  samples are slower than BOTH ordinary samples for each root. Coding projected
  samples849.034/884.429ms are below both prior-inline941.273/987.057ms (-10.11%
  mean). Node246.929/291.497ms overlaps prior-inline267.575/283.516ms; its -2.30%
  mean is NOT robust speedup evidence. These are helper reads, not startup times.
- Exact mixed mapping/data GETs ordinary/prior-inline/projected:
  Node2+10 /4+24 /4+19 (total12/28/23); Coding16+45 /23+69 /23+49
  (61/92/72). Partial projection removes5/20 data GETs, proving an actual
  transport effect. It does not remove the extra mapping cost or all fragmentation.
- Mixed returned ciphertext Node1,953,461/1,937,658/1,904,850 bytes;
  Coding6,972,673/6,733,404/7,665,441. Coding projected fetches932,037 MORE
  ciphertext bytes than prior-inline despite20 fewer GETs. CPU means Node
  71.449/108.196/114.633ms and Coding349.887/450.109/470.564ms. Counts alone
  are not the objective; no cache/window/timeout tuning was inserted.
- Exact DATA ciphertext coverage, prior-inline->projected: Node repeated
  returned bytes115,458->49,632, union1,218,332->1,251,350; Coding repeated
  380,756->66,823, union2,017,825->3,263,795. Coding adds1,246,757 bytes not
  covered by prior-inline and omits787; net union growth1,245,970. The extra
  transfer is expanded coverage, not increased duplicate download. This is
  encrypted provider interval accounting, NOT demanded/decompressed bytes or
  application critical-path attribution.
- Metadata-only means ordinary/prior-inline/projected remain favorable:
  Node346.120/220.130/228.558ms, Coding825.419/614.656/630.129ms. Both inline
  paths have identical GET/byte/cache counts on metadata-only reads; projection
  is not invoked for these small inline demands. Small time differences there
  are not evidence of a projection benefit or regression.
- All24 shared-cache cohorts make0 GETs. Mixed means ordinary/prior-inline/
  projected Node2.039/6.181/5.988ms and Coding4.172/9.456/9.454ms. This retained
  CPU-side penalty remains even with provider I/O absent; eager inline validation,
  demand decoding and view assembly need explicit accounting before another run.
  No claim is made that one of these explains the entire measured difference.
- Mixed charged cache ordinary/prior-inline/projected: Node10,104,855/
  6,279,008/6,673,802 bytes; Coding42,602,377/23,155,478/30,694,480. Budget
  stays128MiB. Lower retained cache than ordinary does not establish net speedup,
  and per-read assembly bounds are not occupied-node RSS proof.
- Decision: REJECT current inline/projected combination for runtime admission.
  Partial-view eligibility is a real issue but is not a sufficient fix. No
  unchanged network rerun or tuning sweep. Next use frozen mapping geometry and
  provider intervals to explain remaining boundaries/expanded coverage, and
  separate mandatory from speculative inline decode and mapping amplification
  before selecting another implementation. All real startup requirements remain.
- 73 experiment-owned mapping objects (4Node+69Coding) read back by exact size/
  SHA, deleted and absence-verified;13,993,874-byte ciphertext recovery archives
  retained.373 remote files,51,015,680-byte export verified. Original files/PIDs,
  ready1,job68814,2072 source/263 original rows,64 detached NBDs and physical
  absence preserved. All1353 product files/inventory unchanged. ECS freshly
  Stopped/StopCharging2026-09-11T19:53:17.357560Z; shared SSH session terminal.
  Skill supplied preservation/lifecycle, not Kind or production deployment.
  Evidence: `/tmp/sandbox0-projected-demand.kJjdYn/evidence.json`, SHA-256
  `dc04ac64cbd6114c728f5cbd2644699ae865d5bb5196b09acab1292116cc22a4`.
  All427 local/exported artifacts sealed; artifact index SHA-256:
  `e9eb538f3f8540e6a49178919fade404fdeeb8e4eab99e61ae6e778c447305a9`.

### D-PROJECTION-BOUNDS — Bound caller assembly, then reassess structural priorities (2026-09-12)

- Prior experiment made diagnostic progress, not startup progress: fewer data
  GETs did not overcome mapping/CPU/coverage costs. Investigate only unnecessary
  caller assembly in the frozen projected Reader; preserve planner, physical
  source reads, independent checksums, eager page validation and cache policy.
- Local overlay only. The previous path assembled the complete valid logical
  window, up to1MiB, for a128KiB demand and could decode out-of-demand inline
  values again. Clip the caller view to demand and the current target entry;
  keep original verified source payloads and their cache identities unchanged.
- Twelve paired byte fixtures cover0/1KiB/128MiB budgets and aligned/unaligned
  demands. Old/new caller SHA, exact source GET sequence and cache charge match
  in all pairs. For the highly compressible1MiB fixture, offset0/128KiB demand
  with128MiB budget: assembly1,048,576->126,976 bytes; demand-side inline decodes
  18->3, removing14 out-of-demand calls plus a repeated already-read block.
  Both use the same3 source GETs and1,069,515-byte charged cache.
- All26 selected top-level race tests pass, including tail/EOF, corrupt-neighbor
  healthy-prefix behavior, cancellation, shared-source isolation, diagnostic
  format rejection and the existing complete frozen geometry regression. This
  is not the complete package suite or privileged runtime acceptance. An initial
  new test passed nil where its constructor requires a cache and failed setup;
  corrected the fixture only. The reconstructed failure note is retained.
- No provider/latency measurement: counters exclude eager page validation and
  do not quantify each cost's share of real startup. Identical IO means this
  fix does not address expanded network coverage. No runtime admission.
- User requested structural optimization with unchanged fundamental requirements.
  Defer the proposed additional static-boundary audit and any inline remote run.
  Next use existing end-to-end traces to rank causal costs, achievable savings
  and unresolved dependencies before selecting another mechanism. Requirements,
  cost model and experiment gates are recorded in
  [STRUCTURED-OPTIMIZATION.md](experiments/STRUCTURED-OPTIMIZATION.md).
- All1353 product files/inventory,427 prior artifacts,164 trace artifacts and
  inherited overlay inputs reverified. No remote query/start, object mutation,
  claim, command or production change. Remote last known Stopped/StopCharging
  remains prior turn19:53:17UTC, not a fresh observation. All startup gates open.
  Evidence: `/tmp/sandbox0-projection-bounds.wOCtSc/evidence.json`, SHA-256
  `8cf53f01eccb12a9c4677d788ab5dbd89f7c9d2cf3e3bdafcffafdcf5d57b6de`.
  All17 current artifacts sealed; artifact index SHA-256:
  `4086b9be8feb24a4004e7984c6687e12950ecb92b8000e516e1ea6c406efdb90`.

### D-CRITICAL-BUDGET — Matched whole-startup cost and sufficiency screen (2026-09-12)

- Previous turn is progress: local assembly verification changed the next action
  to structural end-to-end budgeting. Reuse sealed actual16-sandbox claim/node-v
  samples, not inline helper timings. No new runtime or benchmark operation.
- Freshly fetched origin/main remains sandbox00f092204, infraceb895c. Older
  local main branches are not authoritative substitutions. The measured runtime
  is still the explicitly frozen diagnostic candidate, not production parity.
- Exact per-sandbox joins preserve the claim/node duration hierarchy and all
  client claim/command timestamps. Per-stage timestamps were not exported; do
  not invent a precise absolute phase timeline from duration-only logs.
- Cold Coding combined is2665.827-2665.969ms, requiring665.827-665.969ms savings
  for2s. Matched optimistic zero-cost arithmetic with everything else fixed:
  RootFS ensure alone leaves2021.815-2060.965ms; all observed mapping-index
  calls including root leave2010.991-2039.636ms. Both still miss in all4 samples.
  This is a sufficiency screen, not a causal bound or permission to remove
  checks; future downstream effects require a separate model. Categories overlap.
- Independently reproduced all32 historical wait unions. New mapping-index vs
  block-payload classification uses enclosing Reader regions; interval overlaps
  remain explicit rather than adding concurrent waits. Coding cold command
  payload waits802.032-804.293ms; wholly inventoried-metadata requests account
  for472.085-473.341ms exposure, requests with no inventoried metadata329.437-
  331.191ms. Node command no-inventory payload exposure974.071-976.494ms warns
  against a Coding-metadata-only solution that degrades ordinary bulk reads.
- Complete995Node/23344Coding unit inventory is exact-artifact bound; unknown
  units are not declared regular files. NBD includes kernel read-ahead; these
  exposures are not a mandatory-fault DAG or promised removable time. The first
  analysis rejected a false4KiB-alignment assumption; corrected for sub-block
  request intersections and retained a reconstructed failure note.
- Cold3920 NBD waiter edges include3235 cross-sandbox edges; cached-new1418
  includes1220. Physical leaders must not be multiplied by followers. All16
  cached-new phase windows have zero mapping/header waits while payload waits
  remain. Source-width growth does not remove dependencies on the same flight.
- Five analysis-helper tests/323 assertions pass, along with real-data stage,
  sample, interval and counterfactual-arithmetic checks. No product test suite
  or new startup result is claimed. All1353 product files/inventory,164 trace
  artifacts and17 previous-experiment artifacts reverified unchanged.
- Decision and ranked next evidence:
  [CRITICAL-PATH-BUDGET.md](experiments/CRITICAL-PATH-BUDGET.md).
  Prioritize general filesystem-metadata payload delivery while preserving
  ordinary mixed-read efficiency; no unchanged inline/cache/admission/readahead
  sweep. True guest/kernel dependencies, ensure sub-stages, runsc/procd CPU,
  occupied real width, populated-large roots and complete1s/2s acceptance remain.
- No remote query/start, claim, command, object mutation or production change.
  Last known ECS Stopped/StopCharging remains19:53:17UTC from the remote turn.
  Evidence: `/tmp/sandbox0-critical-budget.4tfaKf/evidence.json`, SHA-256
  `8bd91678351c969e952bdc045f88ce334aa7214805477ff0649f9d01c5acc1c5`.
  All12 artifacts sealed; artifact index SHA-256:
  `80aa72c676c57a41c35f3dad30a9b2f2df8c40f03e43f0433765242e89c515f3`.

### D-FRAME-GROUPS — Whole-frame topology and delivery sufficiency screen (2026-09-12)

- Previous turn is progress: matched budgets require a block-payload mechanism,
  not index-only/ensure-only tuning. Model whole original checksum/length frames
  grouped from every directory/inode relation, without moving4KiB units, splitting
  mapping views or adding an independently traversed index. No product code.
- Reconstruct all1010Node/15893Coding directories and454/9591 inode triggers,
  including the3 earlier4KiB overflows. Exact capped-plan equality verifies the
  prior complete input. Fold all triggers sharing a source frame; admit only
  complete unions at most1MiB decoded/stored, never truncate. Small misses only;
  cached hits/bulk requests do not activate groups or recursive speculation.
- Same-leaf-only membership has inadequate coverage:3Node/37Coding admissible
  groups. Cross-leaf references require an explicit dictionary in the parent
  representation. Complete model yields61/1794 groups,3/38 budget fallbacks;
  original4341/80781 entries remain unchanged. No new partial/split entries.
- Modeled added uncompressed page records37,098/1,258,462 bytes (max added
  page13,381/86,530), including explicit foreign refs and256-byte group headers.
  Exact original encoded frame payload summed per trigger2,703,730/87,494,560
  bytes. These are different cost scopes, not actual encoded pages/ciphertext
  or new stored objects. No wire format, cache budget or density proof follows.
- Ideal empty private-cache metadata sequences: Node23->13 modeled source
  windows and159,337->294,195 stored-frame bytes; Coding37->21 and298,960->
  867,013. Pure128KiB mixed source-event sequences remain EXACTLY identical to
  ordinary for both roots (8/26 windows). This is not equal total mapping/CPU
  performance; earlier small reads can change later bulk cache contents.
- Replay all14,373 retained request shapes across16 sandbox traces. Every
  family/sample starts empty with instantaneous unbounded retention, including
  shapes labeled historical cached-new; this is NOT cached-node acceptance.
  Complete Coding shape266->250 windows,44,760,300->45,456,449 stored bytes,
  ideal payload residency110,759,936->115,806,208. Node236->225 windows,
  43,870,919->44,041,113 bytes, residency106,373,120->107,356,160. Mixed-root
  retained payload alone exceeds the real128MiB shared cache; eviction is omitted.
- Correlate affected requests with OLD block-payload waits, excluding mapping:
  cold Coding313.216-314.652ms, Node138.387-149.292ms. No increased-window request
  appears in these ideal streams. Even deleting all those Coding old waits with
  other costs fixed leaves2351.176-2352.754ms combined, before added dictionary,
  bytes/decode and real cache competition. This is an optimistic coverage screen,
  not a causal bound or predicted latency; other dependency effects need proof.
- Decision: do NOT implement/remote-test this frame-group-only variant as the
  standalone2s solution. Preserve its whole-frame/bulk invariants. Next examine
  joint mapping-page/complete-frame delivery to address both serial lookups, with
  bounded hierarchical selection and explicit hash-DAG/parent/COW/GC semantics;
  no flat full-RootFS index preload, unchanged inline rerun or cache/width sweep.
  Full reasoning: [FRAME-GROUP-FEASIBILITY.md](experiments/FRAME-GROUP-FEASIBILITY.md).
- Seven model tests/31 assertions pass; initial String/Symbol expectation failure
  corrected without model changes and retained. Verified1353 product files and
  inventory,164 trace artifacts,77 graph,393 prior input and12 budget artifacts.
  No actual RootFS reader test, runtime change, object mutation, remote query/
  start, claim or command. Last known ECS stopped remains prior remote19:53:17UTC.
  All original cold/cached-new, occupied real width, populated-large and1s/2s
  acceptance requirements stay open. No production, merge or tag changes.
- Evidence: `/tmp/sandbox0-frame-groups.ZNyWRh/evidence.json`, SHA-256
  `136164721a578fbbc89a644233f4e20ad2f72c4423c45ad3e192dbb0e13cc9fd`.
  All13 artifacts sealed; artifact index SHA-256:
  `1863ba62239a1765faa807de68d5d5c174c3abe30ab68dc1bfe41f1baeb27694`.

### D-JOINT-DELIVERY — Early selector and complete-packet geometry (2026-09-12)

- Previous turn is progress: frame-group-only coverage is insufficient to justify
  a standalone 2s implementation. This turn screens joint mapping/data delivery,
  not another payload-only GET sweep. The packet must be selected before fetching
  its authoritative target leaf; a plan inside that leaf is too late to eliminate
  the mapping-then-data dependency. No product source or remote operations.
- Predeclare 1MiB decoded/stored per complete packet, including modeled 64-byte
  envelope/80-byte member records. Preserve all original mapping entries and full
  frames. Whole original leaf plus all inventoried frames in it admits 0/3 Node
  and 15/60 Coding leaves; maximum decoded charge 2,853,248/9,179,168 bytes.
  Directory frame union plus ALL referenced leaves admits only 33/61 and743/1794.
- Correctness refinement: foreign mapping pages are not needed merely to prefill
  independently verified original checksum/length cache entries. Future logical
  reads still require CURRENT parent/leaf authority. Keeping only the trigger leaf
  with the SAME complete frame union admits59/61 Node and1751/1794 Coding. Earlier
  frame-only3/38 over-budget groups remain excluded; no member truncation or limit
  growth. These counts are geometry, not request coverage or latency.
- Minimal admitted maximum decoded charge983,152/1,022,144 bytes; stored charge
  147,777/235,258. Original encoded page+frame content summed per trigger5,110,803/
  159,816,009 bytes; modeled packet records40,576/1,137,184. These include repeated
  content across triggers, are not node cache or actual encrypted object sizes.
- Explicit parent selector convention272 bytes/trigger gives16,048/476,272 bytes
  before encoding, versus original decoded roots982/15,200. Locator key bound160
  bytes is an explicit model assumption, not a restriction silently imposed on
  existing objects. No flat complete-index preload or free extra selector read
  accepted. Current codec rejects appendices/version mismatches; format evolution,
  hierarchical byte bounds and real encoded costs remain implementation gates.
- Document hash-DAG ordering, exact current parent binding, immutable cache-only
  prefill, branch tail priority, changed-leaf hint drop/rebuild, existing PG/S3
  retry/reference/GC integration and ordinary bulk/source/memory bounds. These are
  design obligations, not completed codec/Reader/COW/security tests.
- Six model tests/5602 assertions pass. Initial two-variant geometry preserved;
  initial source reconstructed by reversing only the minimal addition, matching
  its recorded hash exactly. Tests verify unchanged original entries and complete
  real groups, both record-inclusive limits, deduplication and fixed cost counts.
  Reverified1353 product files/inventory,164 traces,13 prior frame-group artifacts,
  77 graph artifacts and393 prior inputs. No product test or startup pass claimed.
- Decision: reject indiscriminate old-leaf wrapping; retain the minimal packet
  ONLY for one bounded-selector/encoded-size/dependency screen before a Reader
  implementation or remote trial. Full reasoning and updated immediate plan:
  [JOINT-DELIVERY-FEASIBILITY.md](experiments/JOINT-DELIVERY-FEASIBILITY.md),
  [STRUCTURED-OPTIMIZATION.md](experiments/STRUCTURED-OPTIMIZATION.md).
  All fundamental1s/2s, empty-node/cached-new, populated-large, real first command
  and occupied actual-width requirements remain open. No production/merge/tag.
- No fresh remote query. Last known stopped state remains the prior remote
  19:53:17UTC observation. Evidence: `/tmp/sandbox0-joint-delivery.ecG6HA/evidence.json`,
  SHA-256 `bc896ced401158b6e737ae5148b0605584b77f37bdda7dc8d57ccae6ce26a0f0`.
  All12 artifacts sealed; artifact index SHA-256:
  `e0e3242e250419c8da93e1af1cc0d1f6b78d7d91c058fbb468dbdfd05f165630`.

### D-JOINT-SELECTOR — Actual early-selector codec and transport geometry (2026-09-12)

- Previous turn is progress: the minimal trigger-leaf packet has bounded geometry
  but early selection is unqualified. This turn implements a standalone diagnostic
  manifest/selector codec, byte-bound packing, negative tests and actual unchanged
  encrypted-store size fixtures. No runtime/product source change or cloud calls.
- Retain all59Node/1751Coding admitted complete groups. Canonical64-byte manifest
  header/80-byte members preserve original full-frame checksum/encoding/lengths.
  Selector shares a bounded prefix and uses128-byte records with full leaf,
  manifest and trigger hashes. Actual current-setting zstd selector bytes:
  Node7675 raw/4686 stored, Coding224253 raw/130695 stored. These are diagnostic
  manifest identities, NOT final full-content packet hashes or a durable format.
- Explicit128-byte parent envelope plus sealed original core lengths calculates
  Node8785 decoded/5299 stored, Coding239581 decoded/136000 stored. Original cores
  are982/485 and15200/5177. No actual complete parent/core/packet bytes constructed.
  Real packet hashes will require selector re-encoding and size remeasurement.
- Conservative1MiB decoded/240KiB stored/1024-child parent packing accounts actual
  selector compression and maximum legal1104-byte ordinary child records. Both
  images fit one parent with no dropped plans. Synthetic64leaves*1024plans gives
  22leaf-parent groups and2levels versus1ordinary level; do not omit that extra
  dependency. A single8200-plan leaf falls back whole, not by membership truncation.
- Product EncryptingImmutable size fixtures use16KiB chunks,256KiB prefix/parallel
  budgets, existing8MiB/1024-entry header cache, ephemeral in-memory RSA2048 and
  both AEADs. Eight cases, cold and header-hit passes: each needs one underlying
  read. Coding candidate cold requests148660/returns136653-136654 cipher bytes,
  versus baseline17428/5670-5671. It avoids a second read in this geometry but adds
  about131KB upfront; no remote RTT, eight-wide sharing or RootFS byte proof.
- Three local ARM64 benchmark repetitions: fresh selector decode+checks+parse
  Node46.601-48.221us/about50292B allocated, Coding0.871-0.990ms/about951518B.
  Parsed lookups27.50-32.68/48.90-56.85ns with0alloc. Allocation churn is NOT peak,
  retained cache or node-wide memory; these are not remote startup CPU estimates.
- Additional identity counterexample: two valid60-byte zstd encodings with
  different equal-size skippable frames decode identically and produce the SAME
  diagnostic manifest, yet have DIFFERENT encoded-byte hashes. Reject publishing
  manifest-addressed packets as a substitute for existing full-byte content
  identity. Preserve immutable collision/retry/object inventory/PG/S3 semantics.
- Ten top-level tests and seeds pass under race; separate identity-scope race test
  passes. Initial ../prefix validation defect fixed with negative test retained.
  Initial unguided mutation runs retained as such; separately instrumented guided
  fuzz passes249948selector/238067manifest executions. Finite fuzz is not exhaustive.
- Decision: early selection/transport bounds are feasible for these images, but
  net startup gain remains unproven. Next obtain actual original encoded bytes,
  construct full packets/real content hashes and compare net joint mapping+data
  I/O/CPU/cache/bulk costs INCLUDING the larger first parent. Do not claim runtime
  admission from codec counters or another payload-only sweep. Full reasoning:
  [JOINT-SELECTOR-CODEC.md](experiments/JOINT-SELECTOR-CODEC.md).
- Verified1353 product files/inventory,164trace artifacts and12prior joint artifacts.
  No remote start/query, new claim/command, object mutation, production/merge/tag.
  Last known remote stopped remains prior19:53:17UTC. All1s/2s, cold/cached-new,
  populated-large, occupied actual-width, real first-command and cleanup gates open.
- Evidence: `/tmp/sandbox0-joint-selector.x1Xc4S/evidence.json`, SHA-256
  `d5fd5deecd58c4b077add1d3f43d638580c253c6eb87bf0d376d621f8ff2eb1e`.
  All58 artifacts sealed; artifact index SHA-256:
  `5357a3f5edaf8ac2cda0f1e8de73a573505fb6fccbfaaf721037ff4b71f15ead`.

### D-JOINT-BYTES — Real packet bytes and corrected net-cost screen (2026-09-12)

- Previous turn is progress: a diagnostic selector was bounded, but manifest
  identity did not bind complete packet bytes. This turn reads exact original
  source ranges on the Singapore test ECS and constructs complete local packets.
  No product code change, production operation, deployment, merge or tag.
- Original 2CPU/8GiB, no resize. Source helper has a key whitelist, read/request
  bounds and denied write/list/head operations. All 86 original mapping pages and
  5,668 selected full data frames independently verified by the current decoder;
  111,394,995 encoded bytes including maps. All selected original source views
  match sealed profiles. Workload-selected fixtures do not define layout groups.
- Construct all 59Node/1,751Coding packets with unchanged original compressed
  bytes. Full encoded packet SHA256 replaces manifest-only addressing in a
  distinct diagnostic selector. Actual root/selector parent bytes and every
  current child hash validate. Largest packets stored/decoded:
  147,777/983,152 and235,258/1,022,144, within1MiB limits.
- Actual complete parents stored/decoded: Node5,297/8,783;
  Coding135,946/239,579. All packets together add5,151,379/160,953,193 stored
  bytes; source-range duplication remains real storage cost. No S3 publication,
  PG references, COW/path-copy, retry/inventory/GC implementation or admission.
- Real-byte in-memory transport fixtures use unchanged product encryption,
  both AEADs,16KiB chunks, existing256KiB limits and ephemeral RSA. All24
  cold/header-hit reads require one underlying source call. No cloud latency or
  startup inference. Largest Coding packet cold AES requests247,084 cipher bytes.
- Important failed model retained: first net model omitted ordinary grouped
  mapping delivery. Actual Reader disproved Node3/131,056 versus1/174,410 and
  Coding20/867,599 versus15/2,567,720 mapping reads/bytes. Do NOT use that model's
  candidate result. Corrected model matches all four actual baseline source
  counts/bytes and exact mapping key/offset/length schedules.
- Corrected metadata screen, ordinary / unconditional / leaf-miss-only:
  Node source events25/15/17, encodedbytes334,232/696,515/339,908;
  Coding53/40/42 and2,871,857/3,172,638/2,714,065. These candidate costs are
  offline modeled, not actual source/CPU/latency measurements. Reject unconditional
  packets as the next candidate because of amplification; leaf-miss-only remains
  eligible for causal screening, not runtime admission or a2s claim.
- Pure128KiB ordinary non-root source schedule stays identical. Larger parent
  still adds4,812/130,769 encoded bytes. Do not label this zero-cost fallback.
  No shared-cache/concurrency/occupied-node or dirty-tail state is simulated.
- Nine top-level Go tests pass under race across export, packet validation/
  construction, original Reader and actual transport stages. No local e2e.
  Source preservation rechecked; helper exited0, inactive/dead/MainPID0, original
  processes/configuration/Nomad job/PG counts/physical empty state unchanged.
  Archive140,359,680bytes,5,776files individually verified. /data preserved.
  SSH master explicitly closed; test ECS freshly Stopped/StopCharging at
  2026-09-11T22:05:45.722992032Z, original2CPU/8GiB.
- Next: apply corrected grouped mapping to complete retained claim+command
  streams and owned wait exposure BEFORE another candidate runtime. Check the
  remaining combined2s gap, then actual candidate Reader CPU/I/O/cache bounds
  only if causally justified. All fundamental1s/2s, no prewarming, empty/cached-new,
  populated-large, immediate command, occupied real width and cleanup gates open.
  Details: [JOINT-BYTE-COST.md](experiments/JOINT-BYTE-COST.md).
  Evidence root: `/tmp/sandbox0-joint-bytes.Ntq5cM`.
- Final cloud recheck at2026-09-11T22:17:29.455355000Z confirms
  Stopped/StopCharging,2CPU/8GiB. All7,650 artifacts sealed. Evidence SHA-256:
  `08545817fcda78f3e040252f380827ef2903fe121882a3a949435cf936fa7667`;
  artifact-index SHA-256:
  `c70d2aac18628c3fbe537c8142f890df20c6ef01a04bb3b08654c759e41d4759`.

### D-JOINT-CRITICAL — Full-stream qualification and owned slow-source boundary (2026-09-12)

- Previous turn is progress: complete packets are byte-correct, but only a
  leaf-miss delivery strategy remains eligible for causal screening. This turn
  completes that screen without product edits or cloud calls. Fresh main refs
  remain0f092204/ceb895c;1353 product files,164 trace files and7650 previous
  byte artifacts reverified. Existing193 dirty/untracked entries preserved.
- Reproduce eight prior helper families, then all14,373 reads from16 exact
  claim+first-command streams. Actual original Reader uses verified original
  bytes and a fresh PRIVATE unchanged128MiB cache for every stream. Every request
  matches the model's exact source key/offset/length sequence.1784 distinct
  immutable request outputs agree across occurrences; no missing fixture/fallback
  source failure. This is not observed shared-cache/dirty-tail/NBD runtime state.
- Actual baseline cache peaks Node109,008,107 andCoding134,155,105 bytes, below
  134,217,728. No RSS/occupied-density claim and no enlarged cache. Candidate
  retention/eviction remains modeled. Historical cached-new labels identify
  input cohorts, not the fresh private cache in this test.
- Full Node stream:239 source events unchanged, ZERO packet activations,
  +4812 parent bytes. Full Coding:284->275 events, seven packets,
  47,683,268->47,795,524 encodedbytes,123,996,429->124,733,960 decoded charge.
  Its14 changed reads include seven still-required packets, four ideal no-source
  reads and three still requiring ordinary source operations. Early skipped
  sibling mapping delivery creates later mapping misses; keep those costs.
- Old waits on ALL changed Coding requests total666.710–681.001ms, but treating
  every replacement/fallback request as free misleadingly approaches2s. In a
  more mechanism-specific favorable screen, new packets cost ZERO, ideal hits
  remove old waits, retained ordinary mapping/data stages stay, and the larger
  root costs no more than before: cold Coding still2,017.337–2,022.497ms combined
  and1,218.408–1,236.513ms claim. Even zeroing entire replaced NBD reads leaves
  2,012.938–2,013.898ms. These are fixed-other-cost counterfactuals, NOT universal
  lower bounds, causal speedups or new startup samples.
- Decision: stop current leaf-miss joint packets as the independent1s/2s
  implementation candidate. Retain byte work; no durable-format implementation,
  parameter sweep or candidate rollout without a materially new premise.
- Exact ownership: Coding waiter1103's mapping/payload waits55.366/116.689ms
  both depend on leader1102 in another sandbox. Owner data content5490bytes
  takes116.067ms. Header provider1,024cipherbytes takes74.771ms; independent
  range17,401bytes takes115.950ms; overlap74.718ms. Header/data already run in
  parallel. Do not sum followers or mistake header wait for serial network I/O.
  Provider-get ends at response body acquisition, not completed body delivery.
- Next: bounded read-only HTTP-attempt/connection/DNS/TLS/first-byte/body/retry
  attribution on exact original ranges, separating header and connection state.
  Existing trace does not show those sub-phases; no network/credential/CPU cause
  is asserted yet. No production operation, timeout/cache/width increase or
  user-rootfs prewarming. Full end-to-end/density/size/security gates stay open.
- Tests: one Go race test covers16 full streams; nine Ruby tests/26 assertions
  pass. Initial verifier Entry.object/Entry.Object schema typo retained/fixed;
  only local verifier rerun. No source export, Reader test or remote replay.
  No local e2e. Last known remote remains previous22:17:29UTC Stopped/StopCharging,
  not freshly observed this turn. Details:
  [JOINT-CRITICAL-PATH.md](experiments/JOINT-CRITICAL-PATH.md).
  Evidence root: `/tmp/sandbox0-joint-critical.GZLjTa`.
- All27 artifacts sealed. Evidence SHA-256:
  `3f839b634958bb93c7d943611c114ff84b1e8dfe733c59840d665ea317dcb1e6`;
  artifact-index SHA-256:
  `cbde5c5c9f608c402ab51ba9b910ed38a2dfb0d9df192ecb6a7204835582df57`.

### D-SOURCE-HTTP — Fixed-range connection/first-byte/body attribution (2026-09-12)

- Keep all fundamental architecture and1s/2s/full-command/empty-node/density
  requirements. No product changes. Fresh main refs remain0f092204/ceb895c;
 1353 product files,164 prior trace files and193 existing worktree entries
  preserved. Temporary GetObject HTTP-client decorator forwards the original
  client, credentials, retryer and transport; no increased limits or timeouts.
- Select three exact original Coding ranges from verified sealed bytes: root
 5177bytes,read1102's175438-byte mapping group and5490-byte data frame at5850541.
  Six rounds each: fresh provider/header, reused provider/fresh header, reused
  provider/header. Three additional batches of eight independent cold-header
  reads are component diagnostics, NOT eight sandboxes or production width.
- All78 encrypted reads match original encoded and decoded checksums. All98
  HTTP attempts return206/HTTP1.1; no SDK/transport retry, body failure or denied
  operation. Each has exactly one GotConn/WroteRequest trace. Requested ciphertext
 5,625,932bytes. Four local Go race tests passed before remote execution; no e2e.
- Sequential encrypted-read median/max ms (six each), fresh / provider-reused
  header-cold / provider-and-header-reused: root43.939/69.639,
 10.163/11.863,4.526/5.259; mapping42.248/51.490,12.149/12.528,6.035/7.139;
  data49.187/88.286,12.247/19.668,6.308/12.940. Preparation and independent
  content verification are separate. Every sample retained; no replacement run.
- Slowest data HTTP attempt63 reads the exact original cipher offset5856255 /
 17401bytes. Its86.654ms until body acquisition includes16.699ms connection
  acquisition and69.873ms written-request-to-first-byte; subsequent body drain
 0.128ms. Header overlaps data. This narrows the boundary, NOT server/network/CPU
  causal attribution. Fresh/reused fixed order and provider-side state can confound
  group differences;2CPU/8GiB helper is not the historical16CPU workload.
- Frozen candidate source already uses one provider for the ctld runtime and
  shares it across Readers; credentials initialize before runtime capacity.
  Helper credential preparation112.671–125.456ms is NOT a per-claim charge or
  proof that the historical116ms source wait is credential initialization.
  No new provider cache or larger connection pool is justified by these results.
- Next: ordinary-layout full claim+command control with bounded HTTP attribution
  and contemporaneous CPU/queue pressure; calibrate observer overhead on identical
  configured hardware/load. Determine new/reused connection ownership of actual
  slow critical-path attempts. Do not reopen stopped format branches or infer
  startup success from these component timings. All acceptance gates remain open.
- Helper exit0,inactive/dead/MainPID0; original services/files,PG counts,Nomad
  job68814/two groups,empty runtime and64detached NBD verified before/after.
  Ten exported receipts individually verified. SSH explicitly closed/terminal;
  /data preserved. Cloud fresh2026-09-11T22:59:28.901488594Z confirms
  Stopped/StopCharging,original2CPU/8GiB. No source writes,claims,guest commands,
  production rollout,merge or tag. Details:
  [SOURCE-HTTP-ATTRIBUTION.md](experiments/SOURCE-HTTP-ATTRIBUTION.md).
- Evidence root `/tmp/sandbox0-source-http.0a3MTm`;54 indexed artifacts sealed.
  Evidence SHA256:
  `b5ae9e7bcf5db59bfed1f935ebb0c08cc6d6d9325bd67c212d7d517f3529e9a0`;
  artifact-index SHA256:
  `de6c2b791c08cb7814ffe7a2251a194bc6b5bc3ffb74f4f55e575396f77c6028`.

### D-RUNTIME-HTTP — Full-runtime observer preparation; zero claims (2026-09-12)

- Fundamental architecture and acceptance requirements unchanged. Temporary
  bounded HTTP tracing forwards existing provider/client/credentials/retries;
  no product, cache, timeout, admission, RootFS format or security changes.
  Full objectstore/rootfsblock race suites and three HTTP-specific tests pass.
  Failed parser/build/bootstrap preparation attempts retained in attempts.md.
- Planned four fresh-boot OFF/ON/ON/OFF cycles, each eight-wide cold plus cached
  NEW identities and immediate real node-v, maximum64claims/no retries. Historical
 16CPU/64GiB restored only as the diagnostic fixture, not an optimization or
  production-width/occupied-memory/populated-large proof. New isolated clone
  s0_runtime_http_qqq4jd preserves original2072rows and exact ordinary artifacts.
- First fresh-boot cold proof passed: ready8,emptyphysicalruntime,zero targetOSS
  outgoing packets and zero NBD I/O. First OFF-arm unit then failed PRECLAIM:
  required8,589,934,592 free bytes, observed7,263,281,152 (6.76GiB). Its external
  sampler also assumed leaf cpu.max existed; actual leaf has cpu.stat/pressure,
  no cpu.max, while immediate parent is max100000. This does NOT establish CPU
  throttling or disk space as the historical startup cause. Initial disk-helper
  output-variable bug retained; separate read-only receipt gives correct df.
- Stop campaign after first failed preflight; no guard reduction, data deletion,
  replacement cohort or remaining-cycle launch. Actual0claims/0commands/0traces,
 0completedcycles; no performance sample, miss distribution, observer calibration
  or full-runtime causal result. Next prerequisite: locally validate observer
  against real cgroup ancestry and check bounded capture storage before install.
-1353source files and164oldtrace artifacts reverified; existing193worktree entries
  preserved. Ten allowlisted partial receipts exported and individually hashed;
  no configs/privateDBdump/keys exported. Original services/files/configs/two-group
  job restored, sourceDB/artifacts unchanged, testDBretained2072rows, physicalempty,
 64NBDidle. Restore verification23:38:58UTC; originaljobindex69204. Failedunit
  terminal/MainPID0/exit1. SSH explicitly closed/terminal; all owned handles done.
- Cloud fresh2026-09-11T23:42:17.847299063Z confirms Stopped/StopCharging,original
 2CPU/8GiB. /data preserved. No production rollout,merge or tag. All startup,
  size-independence and occupied-density gates remain open; goal NOT complete.
  Details: [RUNTIME-HTTP-PREFLIGHT.md](experiments/RUNTIME-HTTP-PREFLIGHT.md).
- Evidence root `/tmp/sandbox0-runtime-http.qqQ4Jd`;138indexed artifacts sealed.
  Evidence SHA256:
  `983a15917b384febf792e31a19342d6d8248bcf665c1d2468a9afdbc90954523`;
  artifact-index SHA256:
  `2a5b8734c61d4075ceced915a15a03281670be4670e160b8c14842eaa111952a`.

### D-HTTP-CALIBRATION — Full-path HTTP attribution and OFF/ON controls (2026-09-12)

- Fundamental requirements unchanged; no product changes. The previous failed
  D-RUNTIME-HTTP remains failed/zero claims. New campaign validates real cgroup
  ancestry and3968MiB bounded storage budget before installation. Eight Ruby
  tests/16assertions and full objectstore/rootfsblock race suites pass. No e2e.
- Four fresh-boot OFF/ON/ON/OFF cycles, each eight-wide cold then cached-node NEW
  identities, four Node22/four Coding, immediate real node-v.64claims/64commands
  functionally succeed,64unique identities, no POST retries/replacement cohorts;
  all cold/cleanup proofs pass. Historical16CPU/64GiB/privateTLS/oldrunsc/sparse
  Coding/unoccupied-memory fixture is NOT production acceptance or a hardware fix.
- Cold claim0.718–1.629s, command1.017–1.207s, combined1.852–2.754s. Of32cold:
  claim16>1s/0>2s, command32>1s/0>2s, combined32>1s/24>2s. Coding combined fails
  all16, including OFF2.672/2.634s maxima. Cached-new claim0.515–1.030s (one1s
  miss), command0.375–0.796s, combined0.988–1.633s (28>1s,zero>2s). Keep all misses.
- Mixed aggregate calibration does not trigger the rejection rule, but per-image
  cached Node COMMAND does: OFFmax449.812/378.296ms vs ON484.410/504.501ms. Reject
  those traced absolute timings as representative; two repetitions cannot prove
  universal observer overhead. Other metrics are not thereby proven unbiased.
- Eight valid complete traces,51,531,521bytes,28,761NBD reads,zero ambiguous flight
  edges.1298HTTP attempts all206/HTTP1.1/no errors;6new/1292reused connections,
 170,429,098cipherbody bytes.1282exact NBD-owned attempts overlap owner claim or
  command;16attempts unowned. D-HTTP-DEPENDENCY later corrects their annotation:
 4startup mapping constructors and12post-command, not16constructors. Slow reused50–67ms post-write waits
  mostly show response-callback Waiting,not longGC/runnable delay. No proof of
  network versus server cause. Body tails can reach43.551ms. Do not sum shared
  followers or equate HTTP count with serial latency.
-100ms bracketing observer retains ancestor quotas (no finite ctld quota), CPU,
  memory and I/O PSI. No observed memory PSI increment; actual leased memory
  occupancy only0.92GB. Host I/O waits remain an unresolved boundary, not a proven
  physical-disk bottleneck. Next use existing full dependency traces to justify
  sufficient combined-path savings; no additional broad capture/parameter sweep.
-124allowlisted exported artifacts verified; no configs/privateDBdump/credentials
  exported. Original files/configs/processes/two-carrierjob70668 restored00:36:17UTC;
  sourceDB2072/263 and artifacts unchanged, newDB2136retained, physicalempty/NBD64idle.
  SSH explicitly closed/terminal. Authoritative cloud00:40:25.305309375UTC confirms
  original2CPU/8192MiB,Stopped/StopCharging. /data retained; all owned handles done.
  Final audit1353product+164priortrace+138failedcampaign+54fixedHTTPfiles passes.
  Details: [HTTP-CALIBRATION.md](experiments/HTTP-CALIBRATION.md).
- Evidence root `/tmp/sandbox0-http-calibration.myJt7P`;323indexed artifacts sealed.
  Evidence SHA256:
  `044dab092235d0462c4bd6a33f2903b22591c09a186a966915f0224a013b0b85`;
  artifact-index SHA256:
  `c2d495c681cba6726d8d4b7ca6d6ce33eb07154055157c2f858515d5f86d6976`.

### D-HTTP-DEPENDENCY — Exact source/flight waiting and savings target (2026-09-12)

- Previous turn is progress. Reverify323sealed calibration artifacts,1353product
  files and164priortrace artifacts; preserve193existing worktree entries. No
  product edit, remote query/start, RootFS operation, new claim/command or e2e.
- Correct prior scope annotation:1298whole-capture HTTP attempts comprise1282
  startup-NBD,4startup unowned mapping constructors,12post-command. Startup1286
  actualcipherbody170,166,634bytes; post-command262,464bytes. Raw predecessor
  evidence unchanged. All prior samples/misses and cached-Node observer exception
  retained; no claim that unowned cleanup was constructor cost.
- Attach complete HTTP operations by source.io calling/creating-stack ancestry,
  then propagate connection/post-write/body intervals through exact nested and
  uniquely matched shared-flight operations. Intersect with recipient NBD blocking
  states; union per exact sandbox and phase. Include all reads when determining
  single-outstanding intervals. No inferred temporal-only or cross-flight edge.
- Eight tests/17assertions pass. Independent pointwise all-ancestor oracle matches
  all5128subphase recipient maps across82046interval bins and64phase windows.
  This validates explicit bookkeeping, not missing encrypted-header channel or
  guest/kernel fault/read-ahead edges. Overlapping HTTP categories are not additive.
- Cold Coding single-outstanding post-write exposure:claim838.415–925.169ms,
  command604.452–633.126ms, all reused connections. Payload component alone is
  claim565.460–655.038ms andcommand524.636–542.834ms. Connection exposure in the
  same scopes is onlyclaim0.268–0.378ms andcommand0.266–2.607ms. GOMAXPROCS is16
  in396metrics across all four primary captures; no setting changed.
- Even crediting every image's and constructor's connection acquisition for free,
  fixed-other-cost Coding combined remains2.521–2.689s. Its2s gap is568–754ms;
  targeting only recorded single-outstanding payload post-write exposure needs
 48–68% coverage reduction before charging new I/O/mapping/CPU/cache costs. This
  is a candidate target, NOT a realized saving, true lower bound or acceptance.
- Next one offline hypothesis: joint mapping/data on mapping miss, whole-frame
  groups on small payload miss, ordinary bulk unchanged. Require a unified full-
  stream byte/cache/source model; separate historical candidate savings cannot be
  added. Standalone frame/leaf-packet branches stay rejected. No runtime admission
  unless net modeled coverage and both-image/memory costs justify it.
- Initial summary used a standby clock offset for the global connection screen;
  preserve its source/result, correct only primary-lane selection in summary-v2.
  All numerical results remain identical; per-lane dependency analysis unaffected.
  Remote last-known state is prior00:40:25UTCStopped/StopCharging2CPU/8GiB, not
  freshly queried this turn. No production rollout/merge/tag. Goal remains active.
  Details: [HTTP-DEPENDENCY.md](experiments/HTTP-DEPENDENCY.md).
  Evidence root: `/tmp/sandbox0-http-dependency.DjIBlO`.
- Fresh main refs0f092204/ceb895c; final source/input audit and document checks
  pass. All30artifacts sealed. Evidence SHA256:
  `c165634c903eedcdef2270a0b806924647e11b7b348785d661801de626a8ee13`;
  artifact-index SHA256:
  `0afde4302e8ed61c5356eaf18f92d35594496a56d752b2b0c0f01ff8ae5dc52e`.

### D-COMBINED-DELIVERY — Unified policy interaction and exact byte prerequisites (2026-09-12)

- Previous goal turn was a requirements restatement, not new evidence. This turn
  executes the next finite necessary-coverage screen without weakening the goal.
  One ideal private checksum/length cache governs joint-on-mapping-miss and
  whole-frame grouping on small payload miss with mapping already present.
  Keep ordinary bulk dispatch; no recursive/cache-hit speculation or view split.
- All14373immutable retained request shapes across16exact identities replayed.
  Original and joint-only controls match full reference schedules and sealed
  counters. Four toggles use identical semantics: Coding windows284ordinary,
  275joint-only,273data-only,269combined. Independent addition incorrectly gives
  264; joint/data-group interactions lose five of those predicted source-window
  reductions. Node239/239/228/228. Counts are NOT HTTP round trips or latency.
- With all replacement delivery free but remaining ordinary mapping/data waits
  retained, historical cold Coding combined would be1821.894–1839.482ms; only
  160.518–178.106ms of2s margin before new transport/index/verification/cache cost.
  Node1785.234–1797.294ms under the same generous screen. Existing modeled stored
  bytes already grow477811Coding/175006Node per stream BEFORE new data-group
  manifests/selectors/encryption. Coding has one request with added source work.
  This survives necessary coverage only; no candidate trial or speedup claim.
- Complete topology still covers1010Node/15893Coding directories. Prospective
  64-byte manifest/80-byte member accounting rejects five WHOLE Coding groups
  under unchanged1MiBdecoded/stored caps:61Node/1789Coding admitted. No truncation
  or trace-based selection. These fallbacks do not change retained source streams,
  but still apply to arbitrary reads. The prospective encoding is not actual wire.
- Original-tree Go audit reconstructs ALL4341Node/80781Coding entries, verifies
  every parent/child and existing required frame through the unchanged bounded
  product decoder. Required77Node/2122Coding frame indices;77/2091available and
  verified. Missing31Coding frames total234751encoded bytes. Exact validated
  locators are retained; no synthetic bytes or availability-based group removal.
  The initial35missing indices include four removed only by whole-group budgets.
- Six model tests/12assertions and the original-tree Go race audit pass. An
  exploratory query failed on Entry.object vs actual Entry.Object; retained and
  corrected with a new read-only locator query, not another export/runtime run.
  No local e2e/full product suite/candidate Reader test. Reverified1353product,
  164trace and8202indexed predecessor artifacts. Fresh main refs0f092204/ceb895c6;
  preserve193preexisting worktree entries and all earlier experiment evidence.
- Next: bounded read-only retrieval of31exact immutable source ranges, then finish
  actual full-group/selector/encryption and unchanged128MiB shared mixed-root
  cache/eviction accounting. No new import/claim/trace campaign needed for this
  input gap. Do not restart a stopped standalone branch or add independent savings.
  All regional ingress/procd+realcommand, empty/cached-new, populated-large,
  occupied actual width, storage lifecycle and1s/2s gates remain open.
- This turn: no remote query/start, object transfer/mutation, runtime/product
  change, claim/command, production operation, merge or tag. Remote stopped state
  was not freshly queried. Details:
  [COMBINED-DELIVERY.md](experiments/COMBINED-DELIVERY.md).
  Evidence root: `/tmp/sandbox0-combined-delivery.Kbsusf`.
- Final source/input and whitespace audits pass; all owned handles terminal.
  All22artifacts sealed. Evidence SHA256:
  `878d3406727df5efaadfe8834722e9d60129c5f02d69711864be20969efeae04`;
  artifact-index SHA256:
  `a6d913ec4fae63158f703adf702244b177ef4ee77c71cfc3d189e5814cca7cc0`.

### D-COMBINED-BYTES — Actual complete objects and corrected costs (2026-09-12)

- Preserve fundamental requirements and the same unified conditional hypothesis.
  Bounded read-only export obtains31exact Coding original frames,234751encoded
  bytes, through existing encryption and the unchanged product bounded decoder.
  Every transferred file/result SHA/size verifies; no fabricated/recompressed or
  split original frames, new import, claim, command or product implementation.
- Preserve three prerequisite/harness failures: expected warm-ready2was0before
  staging; original ARM dynamic binary exited255before Go on x86_64; separate
  native binary exited1with omitted existing AWS profile environment and no
  completed files. Object-only prerequisites explicitly amended, runtime gate
  remainsFAILED. Correct architecture and exact existing profile on a new unit;
  keep failed binaries, receipts and empty output. No original job/config repair.
  All349allocations were historical terminal records, not349new failures.
- Successful export44provider calls,764453requested/749345returned cipherbytes;
  earlier failed native launch emitted no counters, so all-attempt totals/retries
  are unknown. No object writes/HEAD/list or key/profile exports. Before/after
  original binaries/configs, DB rows, job70668, empty runtime and64idle NBD match.
- Actual data manifest64+80permember, combined selector192perrecord. All61Node/
  1789Coding complete groups match manifest-inclusive budgets and retain59/1751
  existing verified joint records. Largest decoded853072/984304bytes below1MiB;
  wrapped parent stored7805/206609bytes below240KiB. Both current trees fit one
  parent, not proof arbitrary populated-large roots need no added hierarchy.
- Both supported encryption algorithms actually encrypt/decrypt all1850groups
  and two parents on local fixtures:7408cold/header-cached equality checks, one
  provider call each. Existing header-cache/chunk/prefix bounds unchanged. This
  is neither remote HTTP latency nor cached-node runtime acceptance; keys and
  ciphertext bodies ephemeral, verified plaintext artifacts retained.
- All14373requests/16streams preserve exact prior combined source schedules.
  Ordinary/combined windows284/269Coding,239/228Node; actual additional stored
  bytes554650Coding/181802Node, previously omitted76839/6796. Additional decoded
  charge4266807/1982295includes diagnostic verify-all members and manifests, not
  future cache-aware CPU. Do not turn ideal private-cache byte counts into actual
  shared-cache or startup performance; no achieved2s pass this turn.
- Two exporter tests, two codec bounds/integrity tests and completeconstruction
  pass Go race. Reverified1353frozen product,164trace,8202predecessor and22previous
  artifacts; preserve193worktree entries, refs0f092204/ceb895c6. Only temporary
  diagnostics and explicitly requested experiment docs changed. No local e2e or
  full product/candidate Reader/storage lifecycle suite.
- Skill used for Makefile lifecycle only, no historical Kind/bootstrap/deploy.
  One start/stop, no resize; retain/data. Final cloud2026-09-12T01:52:01.558996250Z
  Stopped/StopCharging,ecs.g9i.large,2CPU/8GiB. SSH closed, warmready remained0.
  Before future claims verify a healthy current fixture; do not reinterpret this
  readiness failure as cause of earlier cold misses. No production/rollout/merge/tag.
- Next: the SAME model with unchanged128MiB shared mixed-root cache, current
  mapping protection/exact charges/content identity and evicted repeat reads.
  No repeated missing-frame fetch, independent-savings addition or parameter
  sweep. All true ingress/procd+realcommand, size/density and lifecycle gates open.
  Details: [COMBINED-BYTES.md](experiments/COMBINED-BYTES.md).
  Evidence root: `/tmp/sandbox0-combined-bytes.CwzhqX`. Goal remains active.
- Final artifact/source/whitespace verification passes; all owned handles
  terminal. All1961artifacts sealed. Evidence SHA256:
  `702c2c91e866af6b2420683e2d5e1212c9cf503d3e9ffecc4eccbb1a560b0369`;
  artifact-index SHA256:
  `647a77b4616167e497e725993c1c304644351ddce245b06562551aa925775d79`.

### D-COMBINED-CACHE — Real shared cache and cache-hit guard correction (2026-09-12)

- Previous turn is progress: actual immutable bytes/combined codec now available.
  Keep original requirements. No product edit, cloud query/start, source export,
  import, claim, guest command, production operation, rollout, merge or tag.
- Temporary Go overlay reuses actual Reader and its shared128MiB cache, existing
  16MiBmaximum mapping protection/source slots/bulk windows/content identities.
  Combined encoded parent and parsed selector capacity occupy ordinary unprotected
  entries in the SAME cache, not a new budget/side cache. Whole packets are not
  retained; only independently owned and completely verified original members.
- Each prototype matrix runs57492ReadAt calls over14373retained shapes: original/
  candidate private plus original/candidate shared batches. Every output matches
  sealed original hashes; all16private original controls match exact source calls.
  Shared batch is fourNode/fourCoding identities in recorded-start SERIAL order,
  followed by eightNEWReaders using the exact retained cache. Not recorded overlap,
  concurrent candidate singleflight, live dirty branches or occupied actual width.
- First matrix passes but a targeted128MiB guard FAILS: after selector eviction
  with ordinary mapping/data hot, ordinary makes0source calls and candidate1.
  Preserve that failure and exact v1source. Fix diagnostic dispatch to consult
  cached canonical mapping/data BEFORE selector lookup, retaining normal page
  flights/parsing/accounting and exact parent validation. Same guard now0/0;
  do not mistake the original resident-selector cache-hit test for this boundary.
- Corrected matrix reruns fully. All32private/fourshared result rows, source
  schedules, packet activations and counters equal first prototype for this fixed
  corpus. Both remain separate; no assertion of all-interleaving/LRU-order parity.
  Private ordinary/combined Coding284/269windows,2/50evictions; Node239/228,0/0.
- Corrected shared cold:355->330windows,53571464->54313214encoded plaintextbytes,
  238->317evictions. Reused-cache NEW identities:192->167windows,
  20248020->21173840bytes,778->876batch evictions. Candidate cold16data groups+
  7joint packets; next batch24data groups/no joint. Selector builds2then0, combined
  selector cache charge545348bytes. All cache-accounted peaks<=134217728bytes.
- Exact repeated physical-key/range bytes cold832997->827986; cached-new
  20248020->20700293plus473547new physical-rangebytes. New physical ranges are
  NOT necessarily new logical content. Packet verify-all decoded11839264cold/
  11976656cached-new incl manifests, with1966080/2818048originalbytes already
  cached. Source windows are not HTTP counts or sequential round trips.
- Four guard cases, corrected selector-eviction hit test, complete matrix and
  twelve existing mapping/cache/coalescing tests pass under race detection.
  Interval analyzer3tests/404assertions passes independent per-byte union oracle.
  Initial unused-import compile failure and later nonmatching patch anchor are
  preserved/corrected; no prior failed result was overwritten or relabeled green.
- Final checks preserve1353frozen product files,164trace,8202predecessor+22delivery+
  1961actual-byte artifacts and193existing worktree entries, refs0f092204/ceb895c6.
  Reverse only named instrumentation and gofmt: exact original Reader recovered.
  Actual cache reuse verified, not a guessed LRU. No local e2e/full product suite.
- Bounded-cache prerequisite complete for this two-image diagnostic. Next one
  gate: matched actual encrypted transport plus decode/CPU/allocation net cost
  on the same corrected full requests/bytes and fixed cache/admission settings.
  Do not repeat cache sweeps or add independent savings. Window reduction alone
  admits no runtime trial. Full2s, large-populated, occupied-density, ciphertext/
  RSS, durable import/COW/publication/GC and lifecycle gates remain open.
  Remote stopped/ready0are prior observations, not freshly queried this turn.
  Details: [COMBINED-CACHE.md](experiments/COMBINED-CACHE.md).
  Evidence root: `/tmp/sandbox0-combined-cache.4hue2k`. Goal remains active.
- Final source/artifact/whitespace checks pass, all owned handles terminal.
  All47artifacts sealed. Evidence SHA256:
  `76e5fa5797b2c3a764c624a08322e70307547538ffa73cd9101bede92554e16e`;
  artifact-index SHA256:
  `b7f9303e1f010286c1c685463047099f6dde74f8484988027b9fbc613f8b17dc`.
  Memo qualification uses pinned original/wrapped-root pairs; independent
  mismatched-parent cache-hit, concurrent candidate and cancellation guards
  remain required before new-format product/runtime admission.

### D-HOST-S3 — Fixed-object host/storage and RSA attribution (2026-09-12)

- User machine/S3 hypothesis changes priority before more combined-layout work.
  No product edits, resize, cache/concurrency/timeout changes, object writes,
  claims, guest commands, rollout or production operations. Fetch current main:
  sandbox0 stays0f092204; infra ceb895c->af04ea9 (template digest change only).
- Existing Singapore test ECS remains2CPU/8GiB, separate from prior16CPU/64GiB
  runtime samples. Same3original Coding objects/ranges, one reused provider,
  8sequential paired rounds. Raw uses exact observed ciphertext ranges/hashes;
  encrypted reads independently verify encoded+decoded content. Cached-header
  geometry and repeated remote-service state are explicitly NOT empty-node SLO.
- 83logicalreads/92HTTPsuccess,0retries/rejects,90connectionsreused; requested/
  returned5400473/4993508cipherbytes. Root raw/fresh-header/cached medians
  5.648/10.773/4.630ms, mapping-group5.711/13.196/7.085ms. Preserve seeds
  root55.289/mapping35.073/data43.937ms. Data raw2window reads are sequential;
  encrypted cold windows overlap, so no false per-request/end-to-end comparison.
- 47hostsamples/909.632ms:71.823%CPUidle,0steal/throttle,0memoryPSI/events,
  43.462msCPU-somepressure,hostIOsome/full3.180/2.505ms,0TCPretrans/timeouts.
  One host-wideTCPAbortOnClose not attributed to OSS. Fullcgroupancestry captured;
  missingrootlimitfiles retained. Samplingmedian0.575ms/max1.723ms, gapmax20.994ms.
  This is not occupied-density evidence or proof that host/network/S3 are healthy
  in all earlier/later runs. Post-write waits alone still cannot identify S3.
- New finding: cold-root lastHTTPclose->readend median5.595ms vs cached0.002ms.
  Targeted second probe downloads SAMEroot ciphertext once, verifies priorSHA,
  keeps envelope/keys only in remote memory, decrypts SAMEwrappedkey16times via
  unchanged productionRSA-OAEP. Allpass, wall5.199–5.438ms/median5.271ms;
  processCPUmedian5.284ms includes host observer. Extra rawGET itself70.455ms,
  including17.844msconnection and52.420mspost-write; no layout/decrypt in thatGET.
  Total93HTTPsuccess. No weak-key/algorithm change, header bypass or plaintext leak.
- Sealed ordinary Reader sourcekey cardinality50Coding/7Node, also union50/7
  across8streams. Not liveheader misses or serialcritical path: never multiply
  counts by5ms or lanecount to claim savings. Envelope missCPU now explicitly
  belongs in structuralcost model alongside sourcewindows/bytes/decode.
- Preflight initiallyfailed twice on inheritedready0; read-only inspection finds
  rebootready2, originaljobmodifyindex70668/config/PIDs/source rows unchanged,
  physicalempty/NBD64detached. No warmjob repair. First native buildmissingnew
  sourcefiles fails; retainbuild.json, correctedbuild-v2 matches testedsource.
  Go race5testsfirstprobe/6followup; analyzer3tests796assertions pass.
- Both remoteunits terminalinactive/success/0; intentionalSSHmasterclose returns
  255, not probefailure. Skill uses workspaceMakefile lifecycle only, retain/data,
  no Kind/bootstrap/deploy/local e2e/merge/tag. No claimed2s pass or goalcompletion.
- Next originalfullReader/claim+realnode-v with exact headerCPU+HTTP+host causal
  attribution. No rerun of this three-object baseline or parameter sweep.
  Combinedproduct/cancellation/concurrent guards and allsize/density/lifecycle
  gates remainopen. Details: [HOST-S3.md](experiments/HOST-S3.md).
  Evidence root: `/tmp/sandbox0-combined-net.3HqrAF`.
- Final cloud2026-09-12T03:19:18.723349375Z confirmsStopped/StopCharging,
  2CPU/8GiB/no publicIP. Finalfrozen1353productfiles/164tracefiles verify,193
  existingstatusentries preserved andgitdiff--checkpasses. Evidence/index seals
  live in the evidence root; no required startup gate was markedcomplete.

### D-READER-COST — Actual original Reader, HTTP and crypto overlap (2026-09-12)

- Previous turn is progress; now execute all14373retained request shapes with
  actual ordinary Reader and encryptedOSS, same128MiBsharedcache/16MiBmapping
  protection/8sourceslots/8MiB1024headercache. Same2serialized8identity mixed-root
  schedules, secondbatchNEWReaders. Oneprovider; no prewarm/PUT/import/claim/guest,
  product changes, hardware/cache/concurrency/timeout sweep or SLO admission.
- Temporary context-bound timer wraps existingobjectAEAD (validation/RSA/AEAD),
  not key payloads. Removing one overlaycall recovers exact frozen source.
  HTTP/source/ReadAt spans carry ownership; union/intersection avoids double count.
  14373outputSHAchecks+16constructors,547per-ReadAt source ranges andbothfinalcache
  statesmatch.581HTTPsuccess,577reusedconnections;57coldcrypto(50Coding/7Node),
  0cached-newcrypto. No per-range or per-lane repeatedRSA; reusealreadyworks.
- SERIAL aggregateconstructor+ReadAt7674.903/2027.743ms, notclaim times. HTTPunion
  6794.207/1796.192ms (88.525/88.581%). Coldcryptounion307.297ms overlapsHTTP
  54.302ms, leaving252.995ms (3.296%) outsideHTTP. Othercoldsource69.743ms,
  Readeroutsidesource557.959ms. TestSHA460.581/458.861msseparate; don'tcompare
  fullbatchwall8199.890/2570.130msto2sSLO ordivideby8toinventper-sandboxtiming.
- Source355/192windows,53571464/20248020encodedplaintextbytes; returnedcipher
  59104366/23408734bytes. Alloriginalsharedcacheevictedrereadsretained. Cold
  objectAEADmedian5.265ms/range5.091–6.929ms; its standalone optimization is not
  principal remedy. No crypto weakening or unsupported exactspeedup claim.
- Coldpost-write median/max12.728/175.016ms; cached6.793/56.268ms. A1024byte
  headerwaits142.642ms thenbody0.085ms, so bulkthroughputaloneinsufficient.
  546hostsamples/10.897s:81.729%CPUidle,0steal/throttle/memoryPSI,0TCPretrans/
  timeouts. GlobalCPU-somepressure512.358ms,probe200.743ms; notzero contention.
  HostTCPAbortOnData/Close+3/+1unattributed. RSSmax324368KiB,notdensitybound.
- Duringlongest175mswait8samplescomplete/maxgap20.717ms; enclosing180.574ms
  windowCPU-some134ushost/309usprobe,0IO/memoryPSIorTCPretrans/timeouts. Whole
  runtimepausefortheentirewaitruledout,notallper-goroutine/kernel/networkeffects.
  No proxyenvironmentonoriginalservices/systemd. Exactkernelarrival/service
  processingtime/requestIDsnotcaptured,soS3servicealone remainsunproven.
- Inputprepfirstmistookbatchstubsforfullstreams,failedbeforeoutputs; corrected
  lookupchecksfullpopulation. EightGo racetests and3intervaltests606assertions
  passindependentper-unitunion/intersectionoracle. Staticlinux/amd64 verified.
  NativeNomadCLIlistingfails; inheritedauthenticatedread-onlyhelper succeeds.
- Currentbootready0,jobmodifyindex70668. Historical277failed/74completeallocs,
  not277newfailures; two preceding-bootcarriers DriverFailure/NotRestarting. Event
  known_flags scanJSONfieldnamesandarenotactualtimeoutdiagnosis. No repair;
  fullruntimeacceptancerequireshealthycurrentfixture. Originalconfig/PIDs/source
  rows/physicalempty/NBD64detachmatchbeforeafter. Refs0f092204/af04ea9fetched.
- Oneprobeunitterminalinactive/success/0; intentionalSSHclose255. SkillMakefile
  lifecycleonly;preserve/data,noKind/bootstrap/deploy/local e2e/production/merge/
  tag. Nextmissingconnection/kernelarrivalandrequest-IDserviceevidenceonoriginal
  claim+realnode-v,notanotherunmodifiedhelperreplay. All2s/size/density/durability
  gatesandcombinedprototypeguardsgenuinelyopen; goalactive.
  Details: [READER-COST.md](experiments/READER-COST.md).
  Evidence root: `/tmp/sandbox0-reader-cost.I5N7UF`.
- Eight existingcrypto/singleflight/cancellation/corruption/AAD/parallel-read
  regressiontestsalso pass withoverlayunderrace. Finalcloud
  2026-09-12T03:39:10.526463750Z Stopped/StopCharging,2CPU/8GiB/nopublicIP;
  /data preserved. Final1353frozenproductfiles/164tracefilesandgitdiff--checkpass,
  193existingstatusentriespreserved. Noobserver-offcalibration or SLOpassclaimed.

### D-ARRIVAL-PATH — Reuse arrival evidence; distinguish fixture and OSS (2026-09-12)

- Read-only follow-up to the machine/S3 hypothesis. No product/default changes,
  new claim, guest command, RootFS object read/write, import or layout experiment.
  New evidence root: `/tmp/sandbox0-arrival-path.acMT7K`.
- Reverified 207 sealed packet-correlated files and 19 source-wait audit files.
  The September 9 actual claim+node-v run already places most GET first-byte
  delay before kernel arrival: cold mean18.573ms write-to-callback versus0.156ms
  earliest-arrival-to-callback. All1186GETs mapped; no packet loss. Historical
  cold combined2.159–2.989s,18/18miss2s; cached1.149–1.384s,0/18miss. Older
  source/artifact versions and unoccupied width do not attribute this week's
  175ms Reader outlier or satisfy production acceptance. Cancel the proposed
  repeat broad kernel trace unless a changed mechanism warrants it.
- New boot174f2ed1… automatically creates two NEW unclaimed ready allocations;
  job remains system/two groups/restart0/modify70668. Old07dad576… and3fea784a…
  are terminal, previous-boot and unbound in PostgreSQL; their actual errors
  were registration409 on the allocation unique constraint, not S3 timeout.
  New9546ce02… and951b3aa7… bind this boot. No manual repair, forced evaluation,
  job re-registration, database deletion or relaxation of single-use identity.
  Refill behavior during the preceding boot remains a separate question.
- Exact test bucket is Singapore/Standard/LRS; the preceding OSS probe uses
  its internal endpoint and the test ECS is in Singapore. Bucket log shipping
  is disabled. Exact-bucket SLS access-log policy lookup returns0of0rules. A
  separate legacy Singapore OSS-log project lookup returnsProjectNotExist after
  fixing CLI parameters and a duplicated-project endpoint. All failed attempts
  are retained; local CLI/TLS errors are not OSS performance samples.
  No logging service/configuration was created or enabled.
- Service time still needs request-ID correlation using OSS `server_cost_time`
  and `response_time`. Current observer does not retain those IDs. Do not equate
  post-write time with pure OSS processing or use lightly occupied2CPU results
  to exonerate hardware at real occupied production width. Next establish
  bounded service-log observation before another correlation-only runtime run.
- Before/after original binaries/configuration/PIDs/job and sandbox row counts
  2072/2072/263 match; physical runtime empty, all64NBDdetached. No local e2e or
  production/merge/tag changes. Skill uses workspace Makefile lifecycle only;
  `/data` preserved. Final cloud03:58:44.867428750UTC isStopped/StopCharging,
  2CPU/8GiB/no publicIP. Intentional SSH close255 follows successful control exit.
- Frozen1353product/164trace files reverify;193existingstatusentries preserved,
  `git diff --check` passes. Requirements and all actual2s/size/density/durability
  acceptance gates remain open; goal active. Details and corrections to the
  proposed next experiment: [ARRIVAL-PATH.md](experiments/ARRIVAL-PATH.md).

### D-OSS-SERVICE — Provider window evidence; ID correlation failed (2026-09-12)

- One original full-Reader replay, no product change:14373ReadAt checks,
  16constructors,547source calls and two fixed cache states validate;581HTTP206,
  57crypto operations,577reused connections. Serialized two eight-identity
  schedules are not claims or occupied width. All581client ID validations fail;
  terminal unit remains failed/exit1. No replay or success relabeling.
- Exact-bucket04:25:20–04:25:34UTC service logs contain581unique successful GETs,
  82,513,100response bytes; object SHA256/response-byte multiplicities match the
  client population. OSS processing median/P95/max3/25/68ms, response7/32/71ms.
  This is window correspondence, not exact per-attempt correlation, network RTT
  or startup savings. No actual response-header presence/length or alternate
  S3 request-ID header was retained; synthetic header tests missed that gap.
- Cold Reader interval6286.661ms includes5384.857ms HTTP and274.656ms exclusive
  crypto. Cached-new1758.829ms includes1529.133ms HTTP/no crypto. These are helper
  totals, not2s gate results. Lower times than the preceding unchanged workload
  are variation, not a measured code improvement. All size/density gates open.
- Host78.654%CPU idle; no steal/throttle/memory PSI. CPU and small host IO
  contention remain. One global retransmission, zero TCP timeouts; the longest
  71.530ms wait has regular host samples and no enclosing retransmit/IO/memory
  pressure. No different-machine or observer-off test, no production inference.
- Scoped policy s0-cold-service-svgqq9 used existing SLS/role, only the exact
  test bucket, no IAM/account-wide/production changes. Enabled once; original
  04:30:13UTC deadline unchanged. Disable verified04:26:45.690962500UTC. Managed
  regional logstore/two shards/7day retention and disabled policy remain for
  audit; data retention is not automatic resource deletion or zero-cost proof.
- Setup mistakes preserved: eventual policy readback/default empty scope fields,
  raw GetLogs routing/SQL projection, object-only guard omitting ready1, and
  relative-require launcher via stdin failing before intent. Absolute wrapper
  verified unchanged staged launcher; exactly one actual probe. No carrier repair.
- Current boot871e46f2… remains ready1; original configuration/PIDs/binaries,
  job70668/two groups and rows2072/2072/263 unchanged, runtime empty/NBD64detached.
  No RootFS writes, claims or guest commands. Skill Makefile lifecycle only;
  preserve/data, noKind/local e2e/production/merge/tag. Final cloud
  04:30:08.485154375UTC Stopped/StopCharging,2CPU/8GiB/no publicIP.
- Eleven diagnostic Go race tests passed pre-run;8analysis tests/616assertions
  pass. Next single-request OSS/S3 header+service-log smoke test before any larger
  replay, then existing runtime-dependency evidence to rank removable serial
  waits. Keep crypto/proofs/claim-time binding/10s timeout unchanged. Goal active.
  Details: [OSS-SERVICE.md](experiments/OSS-SERVICE.md).
  Evidence root: `/tmp/sandbox0-oss-service.svgQQ9`.

### D-OSS-ID — Fix observation before another full replay (2026-09-12)

- One original immutable1KiB ciphertext header through the actual S3-compatible
  provider; SHA matches prior attempt9. Exactly oneHTTP206, no rejected/extra
  network attempts, writes, claims or commands. It returns valid24hex
  x-amz-request-id and no x-oss-request-id, explaining the observer-name gap.
  Previous581lost IDs remain uncorrelated; no historical failure is relabeled.
- ID6AA4D92CF4F7BE35316C0BD1 matches exactly one GetObject service record, same
  object SHA/status/1024bytes. Client fresh connection13.919ms, post-write20.971ms,
  full HTTP35.092ms; OSS response20ms/server processing7ms. Distinct boundaries,
  one sample, no RTT decomposition/distribution/startup improvement claim.
- Five diagnostic Go race tests pass. Actual unit inactive/success/0. Existing
  SLS assets reused; unique exact-bucket rule s0-cold-id-tgsr7d disabled at
  04:46:58.601605469UTC, before original04:55:16deadline. No IAM/new logstore or
  retention/shard change. Disabled rule and existing7day/two-shard assets retained.
  A policy-list local shape guard first mishandles data+statistics arrays, then
  is corrected before any write. Log readiness is0then1; same-ID query succeeds.
- Fresh boot3714c164… ready1, so no runtime acceptance. Original config/PIDs/
  binaries/job70668/two groups/rows2072/2072/263 unchanged, physical runtime empty,
  all64NBDdetached. Skill Makefile lifecycle only, preserve/data, no Kind/e2e/
  production/merge/tag. Final cloud04:51:14.557638750UTC Stopped/StopCharging,
  2CPU/8GiB/no publicIP. Details: [OSS-ID.md](experiments/OSS-ID.md).

### D-COMBINED-PARENT-BINDING — Cached selector authority guard (2026-09-12)

- Reuse existing dependency evidence, do not recapture:30sealed artifacts still
  support the historical48.15–68.00%payload-wait exposure coverage target under
  fixed-other-cost assumptions. This is not guaranteed mandatory-path savings.
- New negative test finds a real diagnostic-prototype gap: cold mismatched
  Node-wrapped/Coding-original parent rejects, but cached selector accepts the
  same mismatch after both valid roots are resident. Original failure retained.
- Temporary delivery fix records original checksum/decoded+stored lengths/
  encoding with memoized selector and checks every hit. Bound strings/struct
  are charged to SAME cache. Physical key/offset remain separate from immutable
  content identity; same-content relocated hits still have zero source reads.
- Same two tests fail-before/pass-after; expanded5top-level/8subtest regression
  passes under race, including corrupt-packet/no-partial-publish and fallback.
  No concurrent-caller/lifetime or performance acceptance implied. Parent binding
  correctness is improved; no product/runtime/format publication has changed.
- Added retained charges mean the older full matrix is not proof for this code.
  Finish cancellation/concurrency guards, revalidate complete-stream cache, then
  measure net actual encrypted transport/decode/CPU cost for both images before
  any full-runtime trial. All1s/2s/populated-root/occupied-density/durable-lifecycle
  gates remain open; no timeout/cache/tenant-prewarm concession. Goal active.
  Evidence root: `/tmp/sandbox0-combined-bind.8xh6cW`.
  Details: [COMBINED-PARENT-BINDING.md](experiments/COMBINED-PARENT-BINDING.md).

### D-COMBINED-LIFETIME — Bounded shared work before net-cost admission (2026-09-12)

- Local-only temporary overlay. The predecessor's background selector behavior
  performs GET/parse/commit after caller cancellation; adapter-based negative
  control retains1/1/1 and nil error. Identical test passes with zero work after
  the fix. No deployed-product exploit or startup improvement is claimed.
- Independent caller waits, final-recipient cancellation, fixed10s load bound,
  at most8live diagnostic loaders and SAME8source slots as ordinary Readers.
  Canceled uncooperative work stays counted until actual exit; no overlapping
  replacement. Complete verification precedes bounded publication; no canceled
  ordinary fallback. Packet sharing includes the complete recipient contract.
- Seven flight tests and6real-fixture tests pass under race; both Node/Coding
  have8independent concurrent verified packet reads. Existing5parent/fallback,
  13ordinary lifetime/cache/admission tests and same128MiB eviction/residency
  guard pass. An initial weak leaf-cache length assertion is corrected to the
  exact child's checksum+length; original test and receipt remain for audit.
- New57,492ReadAt full matrix passes; ordinary private/shared rows exactly
  match the predecessor. No budget expansion or packet failures. Cold source
  windows355→330 with+741,750encoded bytes/+79evictions; cached-new192→167 with
  +925,820bytes/+98evictions. Candidate window/byte deltas remain unchanged.
  Parent-bound selectors charge545,594bytes, +246 over prior unbound entries.
- These are encoded-byte Reader requests and serialized historical schedules,
  not HTTP counts, startup latency, physical RSS or occupied production-width
  evidence. Ordinary Reader shared-lifetime semantics remain unchanged. Next
  net actual encrypted transport/decode/CPU/memory cost for both images; do not
  count fewer requests as a measured speedup or repeat a baseline-only sweep.
- No remote/cloud/logging/production operation, product file change, merge/tag,
  new claim or guest command. All1s/2s/populated-root/density/durable-lifecycle
  gates remain open. Evidence: `/tmp/sandbox0-combined-lifetime.H5k5Kw`.
  Details: [COMBINED-LIFETIME.md](experiments/COMBINED-LIFETIME.md).

### D-COMBINED-TRANSPORT — Actual encrypted net cost does not consistently pass (2026-09-12)

- Fixed ordinary/candidate/candidate/ordinary, fresh Reader+header caches per
  arm, cached-new Reader identities within each arm, one shared provider and
  credentials. Unchanged128MiB/16MiB Reader,8MiB/1024header,8source and10srequest
  budgets. Complete57,492ReadAt+64constructors and exact source/cache snapshots
  verify;2206successful globally unique-ID HTTP attempts. Serialized component
  schedules, ZERO new claims/guest commands, not occupied width or startup SLO.
- Actual encrypted33new diagnostic objects/1,984,949encoded plaintext bytes are
  conditionally uploaded and verified by a separate process before measurement.
  Exactprefix `diagnostics/combined-transport-szlb74/` retained for audit; no
  existing RootFS/head/key changes. OSSservercache is uncontrolled and object
  upload/history differs. The fixed order is not statistical acceptance.
- Cold Reader pair deltas candidate-minus-ordinary -2415.644/-264.998ms; cached-
  new -331.583/+490.870ms. The latter reverses for BOTH Node and Coding. Verdict
  no consistent both-image gain, no runtime admission; do not erase the negative
  pair through tuning/retry. No guaranteed candidate regression is inferred.
- Ordinary cold controls alone vary6418.111→3856.041ms with identical HTTP
  shapes/bytes. HTTPunion changes-2522.882ms and post-write union5186.431→2767.142ms;
  connection union only39.439→23.704ms. Substantial HTTP-boundary variability,
  not provider-service/network/scheduler causal separation or an OSS defect.
- Source windows355→330/192→167 differ from HTTP389→355/192→167. Additional encoded
  plaintext741,750/925,820bytes differs from actual wire22,445/137,660bytes.
  Candidate first-header operations68vs57cold and8vs0cached-new. Eight new Coding
  frame groups have NO prior source touch, adding41.383–41.880ms exclusive header
  crypto; not header eviction and not fixed by larger header cache. Both-image
  header/pack costs and remaining critical-path coverage require a structural
  feasibility bound before any further implementation or runtime trial.
- Test fixture2CPU/8GiB/ready1; idle73.34–80.98%,no observed steal/throttle/memory
  PSI/OOM,nonzero CPU pressure. One process retains earlier reports/heap, so RSS
  maxima325516–370736KiB are not isolated per-sandbox or density acceptance.
- Retain initial local ChaCha-constant compile failure. Corrected Go race12top+
  9subtests pass; Ruby7tests/29assertions pass, including a tighter guard for an
  earlier overly broad nanosecond-test tolerance. All1353productfiles unchanged.
- Bothremoteunits inactive/success/0,all22reports transferred/hash-verified;
  originalconfig/PIDs/binaries/job70668/rows2072/2072/263 unchanged,physicalempty,
  all64NBDdetached. Skill Makefile lifecycle only,preserve/data,noKind/logging/
  IAM/production/merge/tag. Final05:57:19UTC Stopped/StopCharging/no publicIP;
  earlierStoppingreceipt remains. All1s/2s/large-root/density/durable gates open.
  Evidence: `/tmp/sandbox0-combined-transport.szlb74`.
  Details: [COMBINED-TRANSPORT.md](experiments/COMBINED-TRANSPORT.md).

### D-COMBINED-FIRST-TOUCH — Actual cache causes behind eight new headers (2026-09-12)

- Local-only passive journal on the unchanged candidate/actual128MiB cache and
  16MiB protected mapping. Full14,373ReadAt+16constructors pass;330cold/167cached-
  new source schedules and complete cache snapshots exactly match prior actual
  encrypted replay. Journal41events within1024cap; no LRU/charge/source changes.
- Eight trigger entries are admitted, explicitly evicted, then loaded through
  new data-only objects while their original mapping leaves remain protected.
  Five last evictions occur in Coding cold reads; three in Node cached-new
  reads sharing the cache. This is measured local cache behavior, not physical
  occupied-width scheduling/latency. Larger encrypted-header cache is not a fix.
- Five data groups previously used their OWN joint packet. Two have no joint
  because decoded size would exceed1MiB (1,063,184/1,116,432bytes); one was filled
  by another trigger's packet and its own joint was never read. Preserve all
  fallbacks and complete members; no fixture/hot-key-specific format selection.
- Exact actual decoders verify all8data/6availablejoint objects and identical
  encoded+decoded original member sets. The initial positional-equality test
  fails and is retained: five available pairs differ in publication order,
  including four of the five reusable pairs. The corrected test records the
  difference instead of treating it as an equivalent LRU transformation.
- Freeing only the five reusable header operations credits26.255/25.811ms in
  prior candidate arms1/2, removes zero source windows, and leaves the reverse
  cached-new pair+465.059ms overall (Node+153.934/Coding+311.125). This is a
  fixed-other-cost screen with prior HTTP variation, not achieved startup or a
  statistical rejection of every pack design. Whole-old-joint reuse would add
  220,164encoded plaintext/988,720decoded bytes before cache/order effects.
- Do not implement the old-joint shortcut. Next evaluate complete-topology
  authenticated pack ranges preserving dispatch/member semantics, including
  selector/offset/cipher alignment, possible extra nonzero-offset header HTTP,
  mixed-cache/decode/memory and durable lifecycle costs. No header-only tuning
  or reopening rejected always-encoded-cache/platform-seed sweeps.
- Go race observer3subtests/member binding/fullreplay and independent8-target
  authenticated measured-parent/selector binding pass; localreplay22.00s is
  not startup time. Ruby3tests/8assertions pass. All1353productfiles unchanged.
  No remote/cloud/production/newclaim/guestcommand/merge/tag; latest stopped
  observation remains previous05:57:19UTC receipt, not a new cloud query.
  Evidence: `/tmp/sandbox0-combined-first-touch.cXbbDJ`.
  Details: [COMBINED-FIRST-TOUCH.md](experiments/COMBINED-FIRST-TOUCH.md).

### D-COMBINED-PACKED — Complete physical packs and actual range costs (2026-09-12)

- Local-only: pack ALL 3,660 unchanged combined objects / 256,866,056 bytes by
  original logical order, data then joint, within the existing64MiB limit. Node
  one pack, Coding four; no hot-object selection, padding or changed members.
- Exact old parents plus bounded compressed placement tables fit240KiB stored/
  1MiB decoded: Node8,409/14,101bytes, Coding220,557/387,863. Independent packet
  identity and original-parent/current-child validation remain. New metadata
  shared-cache charging/lifetime is NOT qualified by the previous cache matrix.
- Both actual crypto algorithms verify all3,662packed ranges including parents.
  Fresh wrappers replay the SAME49diagnostic-object calls from the497-call prior
  schedule, cold25/cached-new24;448ordinary calls omitted, not a full replay.
- AES cold header operations25->7 but provider ranges25->28 and ciphertext
  +394,336bytes; cached-new headers8->0, ranges24unchanged, ciphertext+340,776.
  ChaCha has identical counts,+394,359/+340,784bytes. Three nonzero-offset Coding
  first touches need header+payload ranges. Actual provider geometry is NOT OSS
  HTTP/latency. Source dependencies remain; larger-object cipher alignment and
  loss of standalone EOF increase transferred bytes.
- Exact removed headers in prior measured arms credit only92.104/96.259ms cold,
  41.880/41.383ms cached-new outside HTTP. Fixed-other-cost reversed cached pair
  still+449.487ms even ignoring all added costs. Prior HTTP variability remains;
  no universal regression or startup bound follows. Stop header-only advancement
  of the inconsistent combined candidate; require wider demonstrated dependency/
  net-cost coverage before cache/lifetime/remote/runtime work.
- Go race28guards, two complete encryption algorithms, two actual-original
  wrong-offset/checksum subtests pass; Ruby5tests/10assertions pass. Product1353
  files unchanged, no remote/cloud/claim/command/production/merge/tag. Preserve
  all prior data/results. The last stopped receipt is prior05:57:19UTC, not a
  fresh observation. Original SLO/large-root/occupied-width/durable gates open.
  Evidence: `/tmp/sandbox0-combined-packed.AldIJg`.
  Details: [COMBINED-PACKED.md](experiments/COMBINED-PACKED.md).

### D-CORRELATED-READER — Exact OSS/client/socket attribution (2026-09-12)

- Previous three slow retained IDs have no SLS rows; no retrospective correlation.
  One NEW ordinary full Reader schedule, unchanged input/algorithms/caches/10s:
  all14373ReadAt+16constructors,547source shapes/cache states,57crypto and581HTTP
  succeed. All581actualS3IDs now match exact objectSHA/status206/returned bytes.
  No missing/duplicate/mismatched rows; prior failedIDcapture remains failed.
- Cold post-write median12.607/P9534.585/max97.942ms; exact OSS processing7/28/69
  and response13/36/98ms. Cached-new6.729/13.027/48.322ms; processing3/5/14,
  response6/12/48. Of121cold waits>=20ms,116have OSSresponse>=20ms and62processing
  >=20ms. All3cached waits>=20ms have matching response>=20ms,none processing.
  Distinct timing boundaries: no exact network subtraction or startup sum.
- TCP_INFO1740/1743samples available;3body-close misses explicit. RTT median
  0.528/P950.801/max1.116ms. Four connections,577reused requests. Two cumulative
  retransmit increments coincide with IDs567/572,post-write13.239/19.050ms versus
  provider response5/7ms. Retain limited transport effects, not blanket network
  exoneration. TCP sampler union7.390ms; no observer-off latency acceptance.
- Exact examples: Coding97.942ms wait/OSS98response/61processing/0.497RTT;
  Node73.503/73/69/0.706. Stronger evidence of service/response-path contribution,
  not proof of OSS defect/internal queue cause. Local key unwrap/network distance
  alone cannot explain these exact waits. No product/layout fix newly admitted.
- Host2CPU/8GiB,80.060%idle,no steal/throttle/memoryPSI/OOM,nonzero CPUpressure.
  Serialized Reader unions7005.886/1845.815ms are NOT per-sandbox startup/2s
  samples. Source pins unchanged; fixture remains historical and not production
  width/parity. Original large-root/occupied-width/real-command gates remain.
- Exact-test-bucket policy only,existing service/role/7day-two-shard logstore;
  no IAM/production changes. Disable07:07:47UTC before original07:25:35deadline,
  fresh readback disabled. Sole unit inactive/success/0,ten reports verified,
  source/process/job/DB/physical-empty/NBD64unchanged. Skill Makefile lifecycle
  only;SSHclosed,/data preserved,final07:11:37UTCStopped/StopCharging. No replay.
- Go race14tests,Ruby7tests/15assertions pass;1353productfiles unchanged. Finish
  this correlation prerequisite and do not repeat another broad ordinary baseline.
  Next mechanism needs materially wider mandatory-remote-dependency coverage.
  Evidence: `/tmp/sandbox0-correlated-reader.T03MNR`.
  Details: [CORRELATED-READER.md](experiments/CORRELATED-READER.md).

### D-METADATA-SUFFICIENCY — Overlap-corrected budget and compact-base audit (2026-09-12)

- Local only; all16exact runtime identities/32phase objects reproduce. Cold
  Coding combined2665.827–2665.969ms remains unchanged, gap665.827–665.969ms.
  Its inventoried-metadata payload union705.055–707.062ms overlaps other waits
  by41.725–44.683ms. Exclusive exposure661.674–663.330ms leaves2002.639–2004.269ms
  with everything else fixed. This is an interval screen, NOT a causal lower
  bound: required versus speculative guest/kernel work remains unidentified.
- Metadata mapping+payload exclusive exposure858.356–875.198ms is wider but
  requires76.09–77.58% removal before new costs; perfect elimination leaves
  only192.479–209.255ms headroom. Payload-only claim still1.424–1.444s. No new
  pass, format implementation or re-admission of stopped packet/grouping paths.
- Audit compact readonly base against MAIN one-XFS/one-NBD/block-generation
  contracts. Independent base+upper devices and an immutable image file inside
  one XFS wrapper are distinct candidates; two NBDs are not inevitable. Wrapper
  retains authority but adds loop/proof/translation/cache costs. Both require
  exact rebase FIEMAP-to-branch address provenance, terminal recovery/absence,
  admitted format and guest inode/device compatibility. No build/install/mount.
- EROFS documented compactness motivates a hypothesis, not fewer measured OSS
  dependencies or a2s promise. Base-only wins do not cover populated upper
  histories. Preserve ordinary-data/cached-new behavior and occupied density.
  Next is a bounded address/ownership and actual-net-cost screen, not a baseline
  replay, timeout/cache/hardware/concurrency sweep or direct runtime rollout.
- Source refs freshly fetched: sandbox0main0f092204, inframainaf04ea978. Seven
  helpertests/1513assertions pass;12priorbudget/164trace/1353productfiles verify.
  No remote/cloud operation, claim/command, productchange, production/merge/tag
  or deletion. Last stopped07:11:37receipt is historical, not newly observed.
  Evidence: `/tmp/sandbox0-metadata-sufficiency.k8FywM`.
  Details: [METADATA-SUFFICIENCY.md](experiments/METADATA-SUFFICIENCY.md).

### D-COMPACT-BASE-FEASIBILITY — Complete images and rebase controls (2026-09-12)

- Build ONE plainEROFS image per retained actual source, not another OCI pull or
  startup baseline. Node7293entries/248363154filebytes ->249196544imagebytes;
  Coding150762entries/4489374053filebytes ->4506877952imagebytes. Both full
  content/mode/owner/mtime/links/xattrs hashes and separate root metadata match.
  fsck passes; block/inode totals independently reconcile. No wrapper or runtime
  integration, OSS publication, claim, guest command or new latency sample.
- Existing unmodified rebaseScan rejects both: Nodeetc/adduser.conf at0 and
  Codingetc/R/Makeconf at12288, flags0x301inline/not-aligned. Same-kernel/binary
  XFScontrols pass7294/150763nodes. Do not disable flags. Additional current-code
  tests show rawdifferentdevice LBAs collide and clearingextents loses logical
  data coverage. Next model must separate file-data coverage from exact writable
  block attribution, including rebase/copyup/hole/rename/hardlink semantics.
- Do not turn compact lower bytes into fullRootFS savings or startup latency:
  outerXFS/upper/work/device/mapping/crypto costs absent; per-file st_blocks can
  doublecount packedspace. Offline builds0.25/129.45s after source verification
  are cached-preparation observations, not claim timings.3.9Gunit peak is not
  sandboxRSS/density. Coding remains4.49GBdata/sparse1TiB, not populated1TiB.
- Testdisk resolved by serial/UUID: now/dev/nvme1n1, oldname now systemdisk.
  Private namespace; exactread-onlysource/EROFSloops; preserve oldA/Bimages and
  newoutputs. Extract only erofs-utils1.7.1-1build2,no systempackage/indexchange.
  Retain first erroneoushelpguard exit1; inspection/qualification corrected it
  without redownload, extraction or experiment replay.
- One buildunit and one readonlycontrol finish;72reports hash/sizeverify. All
  loops/mounts gone, original13files/PIDs/job70668/rows/NBD64unchanged. Skill
  Makefile lifecycleonly,SSHclosed; retainStoppingreceipt,fresh08:07:55UTCStopped/
  StopCharging.2Go racecontracts,8Rubytests/9assertions,1353productfilesunchanged.
  Main0f092204/infraaf04ea978freshlyfetched. No production/merge/tag/deletion.
  Original1s/2s,realcommand,populatedroot/upper,occupiedwidthgates remain open.
  Evidence: `/tmp/sandbox0-compact-base-feasibility.EyagT5`.
  Details: [COMPACT-BASE-FEASIBILITY.md](experiments/COMPACT-BASE-FEASIBILITY.md).

### D-LAYERED-REBASE-MODEL — Separate coverage, identity and dirty attribution (2026-09-12)

- Diagnostic Go overlay reuses existing Diff with explicit logical coverage and
  layer-scoped regular inode identity. Only a private upper physical projection
  reaches DirtyFileRanges. No fake lower LBAs, loss of lower source data, new
  Manifest/worker fields, scanner changes or legacy Apply/proof conversion.
- Tested lower/upper address collision, copy-up inode collision, upper rename,
  cross-layer false rename, hardlinks, merged deletion, malformed provenance and
  a256-case independent data/hole/growth/truncate oracle. Baseline18pass/1skip;
  first overlay51pass-events/1skip. These are synthetic model checks, not actual
  merged-mount/EROFS/sparse/watch proof or claimed startup improvements.
- Retain intentional adversarial-red failure: six initial guard gaps cover
  wrong blocksize/outside LBA/excess count, cross-role device alias, unscoped
  directory identity and renamed lower coverage mutation. Corrected model
  rejects all; green58pass-events/1skip. Two-package race passes181events with
  14explicit skips (whiteout CAP_MKNOD,11NBD cases,2RustFS), including parent
  pass events. No privileged or end-to-end acceptance is claimed.
- Expected mount/base/request observations are asserted test inputs, NOT
  authenticated provenance or a nested-image durable binding. Legacy Apply
  continues to reject unsupported plans. Full rebase apply/proofs, actual
  copyup/whiteout/sparse/inode semantics, complete embedding/netcost, populated
  upper histories and occupied actual-width1s/2s+real-command gates remain open.
- Prior134artifacts/1353productfiles and original trace verify. Main refschecked
  unchanged, not freshlyfetched. No remote operation; lastStopped08:07:55receipt
  is historical. No productchange, formatadoption, deployment, merge/tag/deletion,
  timeout/cache/hardware/concurrency sweep or newlatencysample.
  Evidence: `/tmp/sandbox0-layered-rebase.OeoO1h`.
  Details: [LAYERED-REBASE-MODEL.md](experiments/LAYERED-REBASE-MODEL.md).

### D-LAYERED-MOUNT-PROVENANCE — Actual copy-up invalidates stat attribution (2026-09-12)

- Reuse both complete retained EROFS images, with readonly originalXFS/EROFS
  loops and isolated512MiB XFSuppers. Same main mount options, native selected
  operations only. No guest binary execution, claim, formatadoption or startup
  sample. No source pull, complete image rebuild, timeout/cache/concurrency sweep.
- Both smallfile copyups retain mergeddevice58/origininode while FIEMAP changes
  to directupper offsets. Directlowerdevice1793 andupper1794 differ from the
  mergedpseudo identity. MergedGETVERSION remains unavailable; directupperworks.
  Reject merged-stat physical attribution; previousmodel needs independently
  resolved real-layer inputs, not syntheticdevice relabeling. Keep flags strict.
- Existing real lower hardlink pairs fail write propagation in bothimages.
  ONE focused exact-byte same-XFS control with identical options reproduces it:
  not an EROFS-only regression or production/gVisor evidence. New upper hardlinks
  work. No index/mountoptions changed to conceal the observed behavior.
- SourceELFholes1183744bytes(Node)/4096(procd) become0in plainEROFS/merged despite
  equal content hashes. Earlier bytefidelity did not cover sparse allocation.
  Controlled upperpunch/shrink/grow ranges match; rename creates0/0whiteout.
  Unmodifiedmain privilegedwhiteout/opaque Applytest now actuallypassesremotely;
  the13other priorNBD/RustFS skips remain uncovered.
- Mainunit and separatecontrol terminal, allmounts/loopsdetached;13originalfiles/
  servicePIDs/job70668/rows2072/2072/263/NBD64 andretainedimages unchanged.81reports
  transferverified;1353productfiles unchanged. SSHclosed; keep interimStopping
  receipt, same stopcall finishes, fresh08:57:08UTCStopped/StopCharging. Retain
  /data andnewuppers; no production/rollout/merge/tag/deletion.
- Next resolve exact underlying-layer data/identity in offline rebase, bind
  immutablebase andApply/healthproof, and settle sparse/hardlink contracts. Do
  not put fulltree scans onclaim or infer2s from this prerequisite. Complete
  durableembedding/netcost/actualrequiredreads and alloriginal cold/cached-new,
  populatedupper, occupiedwidth, regionalclaim+realnode-v gates stayopen.
  Evidence: `/tmp/sandbox0-layered-mount.LyQ6f5`.
  Details: [LAYERED-MOUNT-PROVENANCE.md](experiments/LAYERED-MOUNT-PROVENANCE.md).

### D-LAYERED-RESOLVER — Held data-layer roots on complete real trees (2026-09-12)

- Diagnostic Go overlay reuses secure-root/FIEMAP/generation primitives without
  changing product files, legacy flags, Manifest/Apply or worker proof formats.
  Guest identity stays separate from actual data identity; only upper physical
  extents can reach writable attribution. No full-tree scan enters claim.
- Preserve first local failure (invalid merged-device assumption/host OverlayFS
  fixture), explicit local XFS skips, and first real-mount failure on both roots'
  `trusted.overlay.impure`. Check upstream copy-up/readdir/lookup semantics, then
  qualify only exact directory `impure=y`, retaining malformed/unsupported guards.
  First/qualified units and fresh clones are separate; no overwritten failure.
- Qualified full regular trees pass Node 5,775 (5,771 lower/4 upper), Coding
  132,932 (132,928 lower/4 upper), plus seven sealed payload comparisons each.
  Upper sparse/hardlink and whiteout checks pass. Non-regular entries are outside
  this resolver, not a complete Manifest. Existing lower-hardlink and source
  sparse-hole discrepancies remain unresolved. Local observations are not
  authenticated/quiescent-worker proofs; concurrent mutation is not qualified.
- Remote XFS mechanisms pass ten then eleven top-level tests; only the
  inapplicable non-physical-host case skips. Preserve all local race outcomes and
  skips. Offline metadata scans 520.295/15,088.933ms follow verification reads:
  not empty-node startup, density or new <=2s acceptance. No guest/claim samples.
- 106 reports verify; original images, runtime files/PIDs/rows/job/NBD state
  unchanged. Both tasks terminal, mounts/loops gone, SSH closed, `/data` retained.
  Same stop call finishes; fresh 09:36:16 UTC confirms Stopped/StopCharging. Keep
  the earlier Stopping receipt. Eight helper tests/ten assertions pass.
  Main refs freshly fetched unchanged; 1,353 frozen product files preserved.
  No production/merge/tag/deletion, S3 replay or timeout/cache/hardware sweep.
- Native regular-file resolver prerequisite is complete for this exact profile;
  no format adoption. Exact worker/image binding, full rebase Apply/health/absence,
  guest semantics, complete durable embedding/net cost and all original
  cold/cached-new, populated upper, occupied-width claim+node-v gates stay open.
  Evidence: `/tmp/sandbox0-layered-resolver.6LZwmH`.
  Details: [LAYERED-RESOLVER.md](experiments/LAYERED-RESOLVER.md).

### D-WRAPPER-DEMAND — Complete native wrapper reads (2026-09-12)

- Build one complete single-XFS wrapper around each retained plain EROFS image;
  no source export or EROFS rebuild. Compare actual existing COW/NBD/stock-runsc/
  procd reads with ordinary controls. NBD63 is outside ctld's nbd0..15 pool;
  existing readahead, request/readiness deadlines, crypto and production remain
  unchanged. File-backed diagnostic, not encrypted Reader/S3/regional acceptance.
- Preserve successive harness failures: whole-runner network isolation prevents
  readonly PG guard; populated parent cgroup prevents create; sparse-geometry
  digest changes with unwritten/page-cache extent visibility; missing assignment
  sandbox EnvVars causes procd to exit. Each correction has separate artifacts.
  The actual procd loader reproduces the last failure locally before the fix.
  Canonical logical-byte hashes and exact embedded SHA checks replace only the
  invalid diagnostic sparse-topology hash, not any production/object checksum.
- Four validated authenticated guests execute real node-v successfully. Unique
  base reads increase Node89.004->143.852MiB, Coding89.844->147.059MiB, +61.6/+63.7%.
  Nonzero-containing data shows the same amplification. The first command adds
  roughly49–51MiB in its own phase; mount-phase savings do not cover it. Required
  versus speculative reads and actual encrypted object/GET dependence are unknown.
  Local combined times1.10–1.32s are cache/observer affected, not new2s passes.
- Do not advance this exact wrapper to encrypted publication/adoption from these
  results; demand amplification needs an identified wider remedy first. No new
  timeout/cache/readahead/hardware sweep or duplicate compatibility campaign.
- 160 reports and all four before/after logical-image checks verify. Five Go race
  tests plus vet/build pass; five evidence tests/20assertions pass. Keep the
  validated unit failed: outer staging unmount returnsEBUSY after all per-lane
  cleanups, though later all owned mounts/loops/NBD are verified absent. No open
  staging FDs in the last owned supervisor observation; kernel-reference cause
  remains unresolved. SSH closes; compute stop is tracked with a fresh cloud
  read at10:38:27UTC confirmingStopped/StopCharging/noIP, preserving the interim
  Stoppingreceipt and all `/data` artifacts.
- All1,353 frozen product files, original runtime/files/PIDs/rows/job and193dirty
  worktree entries preserved. No production/merge/tag/data deletion/object write.
  Original empty-node/cached-new, populated-root/upper, occupied production-width
  regional claim+real command and1s/2s gates remain open.
  Evidence: `/tmp/sandbox0-wrapper-demand.1VMg5U`.
  Details: [WRAPPER-DEMAND.md](experiments/WRAPPER-DEMAND.md).

### D-DEMAND-PROVENANCE — Attribute demand before tuning service latency (2026-09-12)

- Reanalyze sealed traces, no new startup campaign. Wrapper NBD unique bytes add
  57,499,648/59,918,848 versus base57,511,936/59,994,112. All NBD bytes lie within
  base union;4KiB fallback is not the main volume amplifier. Speculation and
  timing-dependent demand are still not separated from mandatory application I/O.
- Ordinary Node/procd file offsets and hashes match exactly across both images:
  62.289/18.000MiB. One readonly remote metadata session composes outer-XFS bmap
  with sealed EROFS body FIEMAP. Wrapper becomes97.746/23.324MiB in BOTH images;
  those files explain74.37%/71.37% of total NBD growth.61,988 targeted body blocks
  (253,902,848bytes) independently match original trace hashes. Inline tails and
  holes excluded; residual is not all metadata. No image rebuild/full-tree scan.
- Correct main/candidate boundary: concrete-Reader clean-span Branch path exists
  only in frozen candidate, absent main0f092204. Native observedFile uses generic
  fallback.12 Go overlay mechanism cases plus clean-span/holes/corruption tests
  pass race; source-side observer retains the trusted Reader type. No weakened
  guard or synthetic GET-count speedup claim. Demand-only recency was already
  ABBA-rejected; do not repeat that cache sweep.
- Nested loop buffering/readahead is a source-supported hypothesis, not proven
  cause or DIO fix. Next evidence must identify actual loop/inner-to-outer demand
  and device state. Exact wrapper remains unchosen/no-go. Existing correlated
  OSS tails and machine occupancy limits remain separate, not erased by local
  demand attribution. No new S3 baseline, timeout/cache/hardware/width change.
- Preserve initial postboot Nomad-refused guard; successful map operation passes
  its own unchanged guard first. Two private ro/noload ext4 collections unmount
  successfully; no XFS mount, loop/NBD/guest, object/database mutation. Original
  runtime/files/PIDs/rows/job preserved within boot; final232 process mount tables
  show no owned mount, loops absent/NBD64detached. SSHclosed; same stopcall ends,
  fresh11:06:59UTCStopped/StopCharging/noIP. Prior EBUSY failure is not relabeled.
- Six offline tests/221assertions pass;1,353product files and193dirty entries
  preserved. No production/rollout/merge/tag/deletion. No new regional sample;
  original empty-node/cached-new, populated upper/root, actual occupied width and
  regional claim+immediate real node-v1s/2s gates remain open.
  Evidence: `/tmp/sandbox0-demand-provenance.CLqEfh`.
  Details: [DEMAND-PROVENANCE.md](experiments/DEMAND-PROVENANCE.md).

### D-LAYER-DEMAND — Observe the intermediary buffered-file expansion (2026-09-12)

- One retained Node wrapper, fresh COW/gVisor/procd and real node-v; no source
  export/rebuild/object request, timeout/cache/RA/hardware/width change. Source
  staging readonly/noload, temporary work outside it. Fresh serial/UUID resolution
  selects nvme0n1 this boot, not the previous boot's nvme1n1.
- Actual loop7:0 hasDIO=0 and4096KiB readahead, matching unchanged NBD43:63.
  Private exact-device/inode trace records26,416events without loss; all1,388NBD
  issues match later completions and the Go NBD union exactly150,721,024bytes.
  Global trace configuration unchanged; private instance removed.
- Known inner loop-completion/XFS-entry union91,402,240bytes; projected outer
  image reads149,987,328. After outer zero holes,59,768,832bytes (57MiB) lie outside
  all known inner requests, all covered by worker RA BIOs.22,447XFS entries are
  buffered;0direct. Inner backed Node/procd file offsets equal ordinary exactly.
  Thus the observed candidate's data growth is in the second buffered-image
  read path, not larger executable demand or S3 alone. No per-BIO parent claim.
- Preserve alias-discovery failure and two offline analyzer failures. Tracefs
  aliases share exact identity;45loop setup completions predate issue filtering.
  Temporal matching rejects future-issue explanations and conservative inner
  unions include missing setup requests. Command103issues/103completions match;
  its outer-only51,429,376bytes are RA-covered. Per-phase sets are not additive.
- Seven Go race tests/vet/build and four offline tests/11assertions pass.27remote
  reports verify. Unit/workload/content checks and teardown succeed; original
  files/PIDs/rows/job preserved, all owned mounts/loops/NBD/trace absent. SSHclosed;
  same stopcall ends and fresh11:31:31UTC confirmsStopped/StopCharging/noIP. Keep
  older EBUSY failure unchanged.1,353product files/193dirty entries preserved.
- Next: one exact-mode/alignment causal test bypassing the buffered intermediary,
  not a global RA sweep. No DIO/net encrypted benefit or format adoption proven.
  Returning to ordinary volume alone is insufficient. Local228.803ms readiness
  plus275.608ms command is cache/observer affected, not regional2s acceptance.
  No production/merge/tag/deletion; original cold/cached-new, populated histories,
  occupied width, durable/semantic proofs and regionalclaim+node-v gates stayopen.
  Evidence: `/tmp/sandbox0-layer-demand.eZhQVJ`.
  Details: [LAYER-DEMAND.md](experiments/LAYER-DEMAND.md).

### D-LOOP-DIO — Causal intermediary direct-I/O ABBA (2026-09-12)

- Four fresh Node-wrapper COW/runsc/procd identities, buffered/direct/direct/
  buffered. Same retained image, machine, 512-byte sectors, 4096KiB readahead,
  request/readiness deadlines and real node-v. Actual loop DIO 0/1/1/0 stays
  unchanged through each command; XFS statx alignment is 512/512 bytes.
- NBD unique bytes 150,721,024 / 90,952,192 / 90,952,192 / 150,721,024. Extra
  outer-file reads 59,768,832 / 0 / 0 / 59,768,832: the second expansion is
  removed, and returns in the reverse control. Known inner bytes remain
  91,402,240; backed Node/procd offset sets exactly equal ordinary controls.
  Outer iomap readahead callbacks 226/0/0/226, inner callback shapes unchanged.
  Shared immutable base checks cover 22,234 equal 4KiB offsets across all trials.
- Preserve the first analyzer's failed full-loop completion assertion. Separate
  filter writes expose some setup issues without completions. Full-prefix loop
  completeness is not achieved; all NBD issues/completions and post-mount loop
  issues/completions match with no recorded errors. All 58,736 recorded trace
  events survive without loss. No guest rerun or replacement of failed evidence.
- Nine Go race tests/vet/build and eight offline tests/22 assertions pass. Four
  real node-v commands succeed, unit and cleanup succeed, 36 reports verify.
  Source staging readonly/noload; all owned resources absent, 236 mount tables
  checked, original runtime/rows/job and 1,353 product files preserved. SSH
  closes; fresh 12:01:22UTC confirms Stopped/StopCharging/no IP. No production,
  merge, tag, object publication, data deletion or local e2e.
- This is an unchosen candidate's native demand mechanism, not regional/S3
  timing. Local combined 472/339/346/463ms is cache/observer affected. Direct
  wrapper volume is only 2.43% below ordinary; adoption still needs net encrypted
  cold/cached-new benefit and every original semantics, authority, populated-root,
  occupied-width and regional claim plus immediate node-v gate. Next is that
  encrypted net-cost comparison, not another native baseline or timeout sweep.
  Evidence: `/tmp/sandbox0-loop-dio.as6ioN`.
  Details: [LOOP-DIO.md](experiments/LOOP-DIO.md).

### D-LOOP-NET — Real encrypted OSS/guest net-cost comparison (2026-09-12)

- O/W/W/O pairs with empty node caches then cached-new COW/runsc/procd identities.
  Exact retained Node wrapper, DIO=1, unchanged geometry/timeouts and concrete
  Reader-to-Branch path. Original optimized ordinary artifact is the control.
  Source staging is detached before guests; immutable misses require encrypted
  OSS. No local-file fallback, regional claim or occupied-width acceptance.
- All eight authenticated readiness proofs and real node-v commands succeed.
  Cold local operation-to-command is 2153.593 / 1348.447 / 1359.223 / 1354.945ms;
  cached-new is 287.909 / 273.245 / 288.641 / 289.342ms. The reverse ordinary
  control catches up: no repeatable net latency advantage is established.
  Preserve the first local >2s result; zero new regional samples.
- Cold HTTP attempts 265/207/203/263, cached-new zero. Mean count decreases
  22.35%, but ciphertext volume only about1.3%, with encoded volume slightly
  higher in the wrapper. Ordinary controls have almost identical bytes but
  client ciphertext P95 drops42.883->20.528ms. Do not assign the first-control
  gap to layout. Client acquire-to-close timing is not pure OSS server timing.
- Host 500ms samples show CPU PSI and I/O wait, no steal/throttle/OOM. This does
  not exonerate machine/scheduler cost, identify a physical disk fault, or prove
  S3 is the sole cause. Next attribution must distinguish response wait from
  CPU/scheduler work, not repeat the layout/RA/cache/hardware sweep.
- Retain the failed initial publication: missing original AWS provider selection
  and an independent command/descriptor filename collision. No guest ran there.
  Qualified runner restores verified nonsecret provider selection in a fresh
  prefix/root. Six encrypted objects/98,925,660 encoded plaintext bytes retained;
  no manager/PG generation publication. First19 and qualified63 reports preserved.
- Both builds pass12 Go race tests/vet/build; six offline tests/50assertions
  pass. Qualified unit succeeds;231 mount tables checked, all owned resources
  absent, original network/runtime/rows/job and1353product files preserved.
  SSH closes; fresh12:39:38UTC confirmsStopped/StopCharging/noIP. No production,
  merge/tag/deletion/local e2e. Candidate not adopted; all original full-path,
  cold/cached-new, populated-history, semantic/authority and density gates remain.
  Evidence: `/tmp/sandbox0-loop-net-qualified.GuLqwt`; first attempt
  `/tmp/sandbox0-loop-net.XEArUb`. Details: [LOOP-NET.md](experiments/LOOP-NET.md).

### D-COMMAND-COST — Real-command HTTP/CPU attribution (2026-09-12)

- Three ordinary-layout pairs, each empty node cache then cached-new guest;
  only middle pair adds CPU/Go trace to lightweight HTTP callbacks. Same exact
  optimized Node descriptor, concrete Reader/COW/NBD/runsc/procd, source staging
  detached, no object publication or regional claim. All six authenticated
  proofs/node-v commands and owned cleanup succeed.
- Cold local combined2322.850/1342.559/1318.042ms, self CPU857.271/890.474/
  866.646ms; cached-new255.523/259.767/291.998ms and zero source requests.
  Preserve first local >1s ready/>2s combined. Zero new regional acceptance.
- HTTP265/262/264, about99% reused connections; all791 match raw source IDs,
  key hashes, exact ranges and bytes. Same93,223,424 unique NBD bytes per guest.
  Write-to-first-byte P95=42.213/15.320/17.629ms, versus similar transferred
  volume and self CPU. Response-wait variation, not doubled CPU work, accompanies
  most of the extra second. This is not pure OSS server timing or a CPU residual.
- Middle cold CPU samples880ms: decode/verify330ms, frame path110ms, TLS80ms,
  RSA10ms, GC-worker50ms. Categories can overlap. Exclude diagnostic NBD hashes
  (50ms cold/70ms cached) from product cost. Cold runnable P95.740ms; GC STW total
  2.304ms. Parallel wait sums are not critical path; host/CPU/S3 remain distinct.
- Next admissible candidate: bounded per-read frame-buffer reuse/in-place AEAD,
  preserving all authentication, checksum, range, retry and partial-read rules.
  Require same requests/bytes and actual CPU/allocation improvement; not a
  global buffer pool, a format adoption or a claim this alone closes the SLO.
- 19 Go race tests/vet/build; 7 offline tests/1636 assertions pass. Retain offline
  old-toolchain and verbose-output-bound failures, without guest replay.39remote
  artifacts verified;236mount tables checked; original services/PG/job/network,
  1353product files/193dirty entries preserved. Fresh13:00:30UTC confirmsStopped/
  StopCharging/noIP. No production/merge/tag/deletion/local e2e. Original cold,
  cached-new, populated-history, semantic/authority, density and regional gates
  stay open. Details: [COMMAND-COST.md](experiments/COMMAND-COST.md).
  Evidence: `/tmp/sandbox0-command-cost.wjKOrB`.

### D-FRAME-SCRATCH — Allocation benefit without proven startup speedup (2026-09-12)

- Isolated compile overlay reuses per-read ciphertext scratch and opens AEAD
  in-place. No format, authentication/checksum, cache, concurrency or timeout
  changes. Frozen product files remain unchanged.
- Same-machine ordinary control/candidate/candidate/control, each cold then
  cached-new identity. Eight authenticated procd proofs and real node-v succeed.
  Cold combined 2138.183 / 1391.363 / 1415.499 / 1272.784ms; the late control
  is faster. Preserve O1 >2s and zero regional acceptance samples.
- Cold cumulative process allocation falls from 517-519MB to 420-421MB, about
  19%; this is neither RSS nor density proof. CPU is 937.087 / 898.031 /
  888.035 / 894.609ms, so no repeatable whole-command CPU gain is established.
- Same 93,223,424 unique NBD bytes but different exact request multisets.
  HTTP attempts 265/265/264/268, all 1062 successful and exactly source-bound;
  write-to-first-byte P95 39.155/20.927/18.261/17.311ms. Cached-new has zero
  source requests and 274-295ms combined. Do not credit response variation to code.
- Both AEADs and exact frozen-loop differential tests pass; entire objectstore,
  rootfsblock and rootfsobjectstore race suites pass. Remote synthetic AES 1MiB
  read allocates 2.238MB -> 27.7KB, but that microbenchmark gain is not startup.
  Both harness variants pass race/vet/build; 7 offline tests / 42 assertions pass.
- All 42 remote reports and cleanup retained; stop workflow used, no production,
  object publication, merge/tag/deletion/local e2e. Candidate retained but not
  adopted as a latency fix; stop this tuning lane and return to full regional,
  populated-history and occupied-width causal/admission gates.
  Details: [FRAME-SCRATCH.md](experiments/FRAME-SCRATCH.md).
  Evidence: `/tmp/sandbox0-frame-scratch.81ED3t`.

### D-REGIONAL-PARITY-PREP — Qualify current stock pin and class (2026-09-12)

- Return from frame allocation tuning to full regional comparison. Confirmed
  worktree HEAD equals current main; the1353-file dirty optimized candidate is
  unchanged. Existing full traces still use runsc20260810, infra pins20260817.
- Downloaded exact official20260817 generation; both SHA256/SHA512 verified.
  Staged independently and ran only --version on remote amd64. Original host
  runsc remains unchanged. Built fixed current regional harness; no guest sample.
- Inspected live CONFIG_PATH and catalog; built a local version-only catalog
  and verified distinct canonical digests through actual manager/shared packages.
  No resource/tenant fields introduced. Existing three pure candidate runtime
  binaries verified against manifest; no imports or source rewrites repeated.
- Complete regional harness/runtime-slot/nomadclaim race suites pass. Retain
  read-only old-config-path and observation-timestamp equality mistakes; final
  check proves all authority fields unchanged, excluding only sampling time.
- Zero claims/commands/performance samples, no service or database/object write,
  no production/merge/tag/local e2e. Read-only/version-check ECS session uses
  original2CPU shape and stop workflow. Next is the actual current-pin full
  regional cold/cached-new comparison, not another helper or broad baseline.
  Populated-history, occupied width and all1s/2s gates stay open.
  Details: [REGIONAL-PARITY.md](experiments/REGIONAL-PARITY.md).
  Evidence and exact execution handoff: `/tmp/sandbox0-regional-parity.HpFSNd`.

### D-REGIONAL-RUN — Actual current-pin regional cold/cache trial (2026-09-12)

- Reused the frozen candidate and both Ready artifacts; current official runsc
  and matching pinned-path catalog,16CPU/64GiB and eight generic carriers.
  Fresh boot plus zero NBD/RootFS outgoing traffic proves no target-node prewarm.
  Then eight cached-node new identities;10s requests and zero POST retries.
- All16 claims/commands and cleanup succeed. Node/Coding cold combined maxima
  are2.221/2.668s: all eight cold samples miss2s. Cached-new maxima1.029/1.673s
  pass2s but not1s. Claim maxima0.903/1.579s cold and0.605/0.877s cached.
  Preserve per-sample clocks, all1s/2s misses and exact stdout/exit proofs.
- Read-only host counters show12.6% cold average busy CPU,0 steal, no lease
  throttling or memory exhaustion, but substantial I/O wait. NBD-backed iowait
  does not isolate S3 service latency. Single-thread and occupied-node effects
  remain open; no broad hardware/S3 sweep or claimed version-caused speedup.
- Retain temporary-catalog path rejection and install the qualified version-only
  catalog with an exact backup/restore intent. Other preparation/early-observation
  errors remain separate; none is a startup sample or retried claim.
- No product changes/import replay/prewarm/production/merge/tag/local e2e.
  Current-pin full-path prerequisite is measured; cold combined, populated-root/
  upper-history and occupied-width gates remain open. Original services/catalog/
  job/shape restoration and StopCharging receipts accompany the retained evidence.
  Details: [REGIONAL-RUN.md](experiments/REGIONAL-RUN.md).
  Evidence: `/tmp/sandbox0-regional-run.qKMiRv`.

### D-FIRST-TOUCH — Cache retention lacks cold-path coverage (2026-09-12)

- Reuse the sealed historical full regional trace, not a new runtime trial.
  Independent cache replay matches2082 source-member flags across396 attempts.
  No claim-stage eviction reload; only three relevant repeated source calls
  precede command completion, exposing24.145ms versus Coding's~666ms deficit.
- Deliberately crediting whole related NBD requests plus transitive followers
  across both images leaves2.560s with other costs fixed. Even crediting mixed
  known/first-touch reads leaves2.171s. These are conservative coverage screens,
  not measured speedups or universal lower bounds; changed scheduling/CPU/request
  geometry remains unmodeled. Six post-command calls receive no startup credit.
- Correct the previous next-action premise: cached Coding607-796ms variation
  predates the new stock pin. Prior traces already prove payload eviction/reload;
  they do not attribute each newer sample. No redundant cached tracing, larger
  cache or compressed-retention candidate is justified as the main cold fix.
- Machine and remote response costs remain distinct. Current unoccupied-host
  counters do not show aggregate CPU/memory exhaustion; NBD iowait is not S3
  service timing. Reuse existing HTTP dependency/server-correlation evidence to
  admit a specific first-touch mechanism with enough net full-path coverage.
- Six tests/210 assertions and48 independent interval checks pass. Zero new
  claims/commands, remote starts, object writes or product changes. Frozen1353
  product files/193dirty entries preserved; read-only cloud check confirms original
  stopped2CPU/8GiB shape. No production/merge/tag/local e2e. All original1s/2s,
  populated-history, occupied-width and durable-authority gates remain open.
  Details: [FIRST-TOUCH.md](experiments/FIRST-TOUCH.md).
  Evidence: `/tmp/sandbox0-first-touch.6eS2Bm`.

### D-PACK-RESPONSE — Object-size response-cost hypothesis (planned 2026-09-12)

- One new variable: total encrypted immutable object size1MiB versus64MiB;
  identical plaintext target at256KiB,64KiB long, same16KiB encryption geometry
  and provider. Twelve synthetic pairs, balanced order and reversed repeat reads.
  Envelopes/first frames are prepared separately and never overlap target payload.
  This is first-touch payload with cached headers, not a cold claim or SLO trial.
- Existing first-touch response evidence admits the question; prior frame/group
  variants changed request dependencies and bytes, not this isolated variable.
  No import replay, format change, target RootFS prewarm or larger cache/timeout.
- Require at least50%/5ms median post-write improvement,9/12 pair wins and both
  order strata agreeing, with exact authenticated bytes and one request per target,
  before admitting further RootFS work. Otherwise stop this size-only lane.
  Provider cache/recent writes and unique object placement remain confounders.
- Original2CPU/8GiB test host only; at most24 create-only synthetic encrypted
  objects/780MiB plaintext in an owned diagnostic prefix. Preserve source objects,
  runtime/PG/Nomad state and all failures; retain artifacts and stop compute.
  No new sandbox acceptance follows from this diagnostic, even if it is faster.
  Exact plan/harness: `/tmp/sandbox0-pack-response.HWZGyw/PLAN.md`.

### D-PACK-RESPONSE — Result: no consistent small-object advantage (2026-09-12)

- 12 synthetic1MiB/64MiB pairs, identical target plaintext/offset/length and
  actual cipher range263278-328893. All72 encrypted reads succeed, including48
  first/repeated target reads. Provider attempts24PUT/24HEAD/72GET, no retries;
  every target has one reused-connection request,65616bytes and exact verification.
- First-target post-write medians are7.731ms small/7.250ms large; P90 nearly
  equal. Small wins6/12 pairs, and order-stratum medians disagree. The fixed
  50%/5ms/9-pair admission rule fails. Preserve large's35.819ms outlier, but do
  not infer a size-caused tail or reimport/repack the real images from it.
- Repeated-source medians4.932/4.607ms; every paired repeat is faster despite
  real OSS requests and identical bytes. There is no caller payload cache.
  Provider state/order/network/client effects remain unseparated; fresh writes
  and disjoint header preparation do not establish provider-side cold storage.
- Retain initial SSH failure and rejected two-ready-carrier fixture assumption.
  Actual original ready=0 is explicitly frozen before/after this storage-only
  experiment; no Nomad repair or runtime-health claim. Original services, files,
  job71527, histories and64 detached NBDs stay unchanged. No guest or claim ran.
- Eight Go race tests/vet/build, four analyzer tests/14 assertions and48 endpoint
  checks pass;67remote artifacts verified. Frozen1353 product files/193dirty
  entries preserved. Owned unit/PIDs/scratch absent, SSH closed, stop workflow
  used.24 generated encrypted diagnostic objects retained (818907216stored bytes).
  No production, format adoption, timeout increase, merge/tag/deletion/local e2e.
  Stop this size-only lane; all original full-path/populated/occupied gates remain.
  Details: [PACK-RESPONSE.md](experiments/PACK-RESPONSE.md).
  Evidence: `/tmp/sandbox0-pack-response.HWZGyw`.

### D-POOL-BOOT — Diagnose zero-ready pool after reboot (planned 2026-09-12)

- Prior storage-only run observed job71527 and original services intact but zero
  ready carriers, versus two ready at the preceding regional restoration. This
  is a new recovery-path observation, not proof of the measured2.668s cold cause.
- Read current source and old/current boot service logs, Nomad allocation/
  replacement state and PG slot/capacity projections on original2CPU/8GiB host.
  No pool reconstruction, claim, object operations, service/job/config changes,
  resize, production rollout or local e2e. Stop compute after read-only diagnosis.
- Exact-incarnation registration must stay fail closed across boot changes.
  Determine whether rejection and replacement work, and distinguish original
  fixture binaries/configuration from current main before proposing a change.
  Preserve all original full-path, RootFS, occupied-width and authority gates.
  Plan/receipts: `/tmp/sandbox0-pool-boot.cr1zqD`.

### D-POOL-BOOT — Persisted-handle recovery defect reproduced (2026-09-12)

- Old boot: two recovery EOFs precede same-allocation registration409 failures.
  Disk-pressure client GC precedes PG terminal publication; no later alloc-stop
  evaluation appears before the next ordinary boot places two new ready carriers.
  No manual pool repair. This is not evidence of S3-caused recovery failure.
- Current driver/main decode a private Nomad TaskConfig field omitted by actual
  LocalState persistence. Temporary same-codec unit overlay reproduces EOF three
  times under race; direct-memory recovery control passes three times. No fix or
  reboot acceptance claimed. Distinguish this new persistence defect from the
  already-implemented post-terminal refill notification fix; do not redo the latter.
- Filesystem reports96% use and14% inode use, triggering automatic Nomad GC;
  no ENOSPC or physical-disk latency causality demonstrated. Prior regional
  pool-pin checks were read-only, not postboot reconstruction. Ready-carrier
  cold combined2.221/2.668s still needs explanation and remains above2s.
- Zero claims/commands/object operations or product/runtime/config changes.
  Both full startup log windows retained; intentionally bounded tail and earlier
  observation failures remain explicit. Frozen1353files/193dirty entries and
  original histories/artifacts preserved. SSH closed, ECSStopped/StopCharging,
  original2CPU/8GiB. No production/merge/tag/deletion/local e2e.
- Next narrow lane: versioned persisted driver configuration and exact-identity
  recovery regression, then candidate refill qualification. All original
  architecture, RootFS, cold/cached-new, populated/occupied and1s/2s gates stay.
  Details: [POOL-BOOT.md](experiments/POOL-BOOT.md).
  Evidence: `/tmp/sandbox0-pool-boot.cr1zqD`.

### D-PERSISTED-CONFIG — Versioned recovery fix (2026-09-13)

- Added a normal failing regression across Nomad LocalState persistence, then
  persisted normalized driver configuration in version2 opaque handles. Same
  persisted recovery/heartbeat now passes. Legacy/unknown/missing state fails
  closed instead of reconstructing default security inputs.
- Immutable task/allocation/node/namespace/netns/path inputs must match; newer
  local lifecycle state cannot replace identity/configuration. Regional boot/
  namespace rejection, one-shot carriers and the existing manager refill fix stay.
- Full independent driver race suite, five-repeat recovery guards, actual Nomad
  BoltStateDB close/reopen recovery, manager refill race suites and vet/build pass.
  Retain initial EOF failure and corrected fake-Version assertion failure.
- Five existing product files changed and two recovery files added:1353→1355,
  with1348 unrelated existing files preserved and dirty193→198. New source
  inventory and linux/amd64 artifact are separately hashed. Native arm64 compile
  output is retained but is not the remote artifact. Docs describe legacy rollout.
- No remote operation/claim/command/object request, prewarm, timeout change,
  production rollout, commit, merge, tag or local e2e. No measured latency gain;
  cold combined2.221/2.668s and populated/occupied/full-path gates remain open.
  Next qualify actual same-boot persistence and new-boot replacement remotely.
  Details: [PERSISTED-CONFIG.md](experiments/PERSISTED-CONFIG.md).
  Evidence: `/tmp/sandbox0-persisted-config.5erDgE`.

### D-RECOVERY-REMOTE — Actual Nomad persistence and boot qualification (2026-09-13)

- Remote Nomad1.11.3 with the exact version2 amd64 driver: same-boot restart
  preserves both allocation/slot IDs and resumes fresh authenticated heartbeats.
  Both `warm-slot-recovered` events are retained in the complete restart window.
- One ordinary ECS reboot rejects missing old namespace bindings, terminalizes
  old slots and automatically creates two new ready allocations. JobModifyIndex
  71629/version371 remain unchanged throughout both events; no manual refill.
  Keep old-allocation409 and no-restart events, not just the final ready count.
- Isolated2072-row database and node state; original2CPU/8GiB, two warm carriers,
  no guest/claim/command/import/prewarm. No product source change. Test setup
  mistakenly disabled the mandatory materializer: retain the failure and both
  pre-mutation submission guard failures. Correct only the owned config after
  proving object-work queues empty; no application timeout increase.
- Host95–96% disk usage triggers historical allocation GC, but is not proof of
  disk latency causing cold startup. This is recovery-only, not an S3 or regional
  startup measurement. Cold combined2.221/2.668s, populated histories, occupied
  width and all original architecture/authority/full-path gates remain open.
- Original binaries/catalog/configuration and job semantics restored, with two
  ready original-fixture carriers. All64 NBD devices detached, source histories
  and artifact bindings preserved;33 sanitized documents downloaded and hashed.
  Isolated DB/files retained, owned SSH closed, ECSStopped/StopCharging at2CPU/
  8GiB. No production/merge/tag/deletion/local e2e.
  Details: [RECOVERY-REMOTE.md](experiments/RECOVERY-REMOTE.md).
  Evidence: `/tmp/sandbox0-recovery-remote.ev9NvC`.

### D-READ-HEDGE — Reject the bounded delayed-duplicate GET premise (2026-09-13)

- New hypothesis: one duplicate for an already-issued slow immutable GET, no
  prewarm or longer timeout. Screen fixed10ms delay and assumed7ms duplicate
  response with a20%token budget and at most two modeled duplicate flights.
  Use exact retained source/flight recipients, both images and both cache cohorts.
- Bounded cold requests increase58/424 and60/425; Coding owned waiting credit
  223–227ms leaves hypothetical combined2.345–2.527s. Granting all unowned
  constructor tails still leaves2.329–2.490s. No runtime admission.
- Removing the budget costs60–63%extra cold GETs. Under the same assumption the
  slower cycle still remains2.208s; the faster modeled cycle is not a measured
  pass. Instantaneous-response and perfectly correlated controls expose the
  unproven independence/response assumptions. No parameter sweep or deployment.
- Nine tests/23assertions and512independent interval checks pass. Preserve the
  original cached-Node exclusion-label error; v2 fixes its32modeled flags with
  unchanged numerical results. Reverify1355current product files and446prior
  artifacts; no product/remote/cloud/claim/command/object activity. Original
  current-pin2.221/2.668s, populated-history, occupied-width and full-path gates
  remain open. Details: [READ-HEDGE.md](experiments/READ-HEDGE.md).
  Evidence: `/tmp/sandbox0-read-hedge.cfUMAs`.

### D-RESIDENT-PREFLIGHT — Bounded occupied fixture preparation (2026-09-13)

- Added a finite native guest load helper with page-touch acknowledgement,
  single-use load/release control, bounded lifetime and EOF/signal/error cleanup.
  Normal and three-repeat race tests, small subprocess signal tests, vet and
  linux/amd64 build pass. No runtime or RootFS implementation change.
- Pure host/guest evidence checks pass33tests/38assertions and reject missing,
  stale, aliased, expired or insufficient actual occupancy. They are not yet a
  remote sampler/runner and do not establish runtime cleanup or startup latency.
- Reject the initial7GiB/1750m proposal: actual policy code with baseline1GiB/CPU,
  max4GiB rejects it and would derive7000m. Keep1792MiB/1750m leases; proposed
  seven1536MiB residents are10.5GiB allocation, not64GiB memory-reclaim proof.
  Seven residents plus one target is explicitly one-wide cached-new admission.
- Correct the initial duplicate D-OCCUPIED label; retain the historical six-
  resident/two-target results. Reverify114prior artifacts and1355source files.
  Only diagnostic README/helper files and separately tracked experiment notes
  change. No remote/cloud/S3/claim/command, prewarm, timeout increase or local e2e.
- Next wire/test authenticated one-shot control, exact PG/cgroup sampling and
  cleanup before compute starts. Current actual cold combined2.221/2.668s and
  all populated-history, empty-node, occupied/full-path and original gates remain.
  Details: [RESIDENT-PREFLIGHT.md](experiments/RESIDENT-PREFLIGHT.md).
  Evidence: `/tmp/sandbox0-occupied.VpIIc2`.

### D-RESIDENT-CONTROL — One-shot transport and target-window sampling (2026-09-13)

- Added private authenticated resident transport using generated input `data`
  with an explicit newline, fixed async TTL/lifetime and10s HTTP budget. No
  application POST replay after ambiguous outcomes; owned file absence/hash
  readback, exact guest context/owner and clean live evidence are required.
- Pinned-inode cgroup sampler and single-window composition capture same-host
  monotonic brackets, pre/post lease and live-process identity, CPU/memory/swap/
  OOM/pressure/host counters and failed-target cleanup identity. Added a guarded
  isolated read-only PG query adapter; no actual DB connection occurred.
- Final21top-level tests pass three race repetitions, vet and both architecture
  test builds; three pure query/guard tests pass26assertions. Retain the initial
  invalid mock cleanup poll and later stuck mock-server cleanup timeout. Fix
  fixtures only; no public timeout or product validation was relaxed.
- Verify22prior artifacts and1358unchanged candidate files. Local TLS/counter
  fixtures and target callbacks are not remote runtime/physical absence or SLO
  evidence. No cloud/remote/S3/claim/guest command/prewarm/runtime change/local e2e.
- Next complete the bounded seven-resident/one-target phase matrix, measured
  admission and independent cleanup, then current-candidate remote execution.
  Preserve current2.221/2.668s cold combined misses and all original requirements.
  Details: [RESIDENT-CONTROL.md](experiments/RESIDENT-CONTROL.md).
  Evidence: `/tmp/sandbox0-resident-control.aIJpy6`.

### D-RESIDENT-MATRIX — Complete local phase matrix and explicit latency gates (2026-09-13)

- Composed seven resident preparations, retained initial Node/Coding encounters,
  bounded same-identity idle/load/release phases and six controlled targets.
  Initial cache encounters are not discarded warmups or globally empty samples.
- Cleanup survives cancellation, retains unknown identities and attempts each
  owned DELETE once; lost responses use read-only observation. Exact authority
  checks distinguish verified pending refill from invalid or unowned state.
- Correct synchronous pre-target load/release heartbeat gaps without weakening
  stable-state/window checks. Pin cases/helper inputs before claims. Separate
  collection completion from per-target1s/2s readiness and literal-node-v timing;
  retain misses and exit nonzero on a failed2s diagnostic screen.
- Final31top-level Go tests pass three race repetitions, vet and actual native/
  linux-amd64 CLI builds. Ruby fixtures pass45tests/133assertions, including the
  subprocess verifier. Native startup opt-in rejection is checked. No remote/
  cloud/S3/database/guest activity or runtime/timeout/prewarm change.
- Reverify39prior artifacts and1358unchanged product files. Remote isolation,
  install/restore and actual occupancy execution remain next; existing cold
  combined2.221/2.668s misses and all original requirements remain open.
  Details: [RESIDENT-MATRIX.md](experiments/RESIDENT-MATRIX.md).
  Evidence: `/tmp/sandbox0-resident-matrix.HLWoaJ`.

### D-RESIDENT-REMOTE — Actual initial commands, invalid occupied phase (2026-09-13)

- Verified and installed current pins on the matched16CPU64GiB test node with
  eight generic ready carriers and isolated metadata; no OCI import/prewarm.
  Seven parallel resident claims succeed in0.856–0.861s. Initial Node/Coding
  claim walls are0.159/1.569s; literal-node-v takes1.109/1.063s; combined clocks
  are1.268/2.632s. Both commands return exact stdout/exit0; Coding still misses2s.
- First fix a private no-workload/positive-command-gate configuration error,
  proven to have issued zero claims. Retain its old terminal unit/binary/log,
  add the actual-constructor regression, and preserve common validation plus
  the2s executable gate. Three race repetitions, vet and cross-build pass.
- The actual run stops before controlled windows: ordinary non-PTY CMD create
  has no stdin pipe, so the EOF-sensitive helper cannot sustain input-driven
  load. Local pipe-backed mocks missed this contract. No occupied matrix or
  CPU/S3 causality is admitted. Next use the existing signal API with explicit
  finite helper mode; do not change product CMD semantics or weaken liveness.
- All nine known sandboxes are cleaned once, unknown/cleanup errors are zero,
  and physical absence is proved. Restore original binaries/configs/two-group
  job and2ready; retain2081isolated rows and all source history. Stop/shape
  restoration evidence accompanies the sealed artifact. No production action.
- Preserve1358product files and46prior artifacts. Original cold2.221/2.668s,
  populated histories, true occupied width and every original gate remain open.
  Details: [RESIDENT-REMOTE.md](experiments/RESIDENT-REMOTE.md).
  Evidence: `/tmp/sandbox0-resident-remote.VGhaLd`.

### D-RESIDENT-SIGNAL — Actual same-resident idle/load/release windows (2026-09-13)

- Correct only the diagnostic helper's no-stdin contract with explicit finite
  signal control through the existing context API. Default EOF behavior, real
  CMD semantics,10s HTTP budgets and1s/2s gates remain unchanged. Real DirectRunner
  no-stdin subprocess tests now cover lifecycle and cleanup; helper22/adapter33
  top-level tests pass three race repetitions, with vet/build/SLO unit checks.
- The matched16CPU64GiB run completes15claims and eight literal-node-v targets,
  including six independently verified controlled windows on the same seven
  residents. Initial Node/Coding combined1.306/2.375s are retained; loaded cached-
  new results are0.683/0.270s, released0.181/0.175s. Initial Coding still fails2s;
  collection success does not turn the diagnostic into a passing SLO.
- Runtime pins did not change. Cached-new success under verified CPU load is
  not proof of globally cold performance or S3 health, and ordered cache warming
  prevents claiming that loading/releasing memory caused the whole difference.
  This is one-wide admission at eight active carriers, not eight new claims or
  64GiB memory-reclaim acceptance. Populated-root/history requirements stay open.
- All15known IDs are deleted once, zero unknown/cleanup errors, zero active
  leases; retain2087isolated rows, restore original runtime/job and stop/restore
  compute shape with independent proof. Preserve the old seal and recover its
  23missing intent/log artifacts into a new annex. No production action/prewarm.
  Details: [RESIDENT-SIGNAL.md](experiments/RESIDENT-SIGNAL.md).
  Evidence: `/tmp/sandbox0-resident-signal.RcBrWF`.

### D-EXEC-ADVICE — Reject unsupported guest hints, bound the ELF comparison (2026-09-13)

- Exact stock release-20260817.0 source shows fadvise(WILLNEED) and
  madvise(WILLNEED) ignore advice; readahead returnsEINVAL despite its supported
  table label. Bind all four source hashes and actual amd64 routes. No guest
  hint implementation or remote run is admitted; do not generalize to real reads.
- Scanned Node initialized PT_LOAD coverage is107,122,688bytes. Its overlap
  with a retained65,314,816-byte read union is only CROSS-ARTIFACT geometry:
  exact executable identity across these inputs is unverified. Preserve v1's
  omitted binding caveat and correct canonical v2; the43,720,704-byte difference
  is not measured extra current I/O or a latency prediction.
- Seven AST tests/vet and10geometry tests/921assertions pass. Verify1359unchanged
  candidate files,237prior sealed artifacts and52identity artifacts. Only
  experiment notes change; no cloud/S3/claim/command/product runtime activity.
- A real-read proposal still needs same-file binding, generic exec-time trigger,
  bounded lifetime/concurrency, unchanged command semantics and net cost before
  remote admission. Preserve actual cold2.221/2.668s and initial Coding2.375s
  misses, machine/S3 attribution limits and every original requirement.
  Details: [EXEC-ADVICE.md](experiments/EXEC-ADVICE.md).
  Evidence: `/tmp/sandbox0-exec-advice.nu3eoS`.

### D-NODE-BINDING — Exact current files, retired historical trace (2026-09-13)

- Read-only scans bind Node in both current regional format2 artifacts to the
  same124,836,408-byte SHA256 and ELF metadata. All seven standard dependency
  candidates differ across images, so these are not RootFS-size-only controls.
  This is current-artifact identity evidence, not library-causality or speedup.
- Exact metadata locates the historical read-union artifact in a different
  database with retired format10005/mapping5. Do not use its65MB read union or
  the42MiB conditional difference as current-cost evidence. The historical Node
  hash bridge remains unverified; no obsolete decoder is revived.
- Two supported scans use622bounded underlying ciphertext range calls, return
  120,366,504bytes and attempt zero mutations. These are full-file inspections,
  not HTTP-attempt counts or startup demand. Zero claims/commands/latency samples.
- Preserve initial zero-ready, wrong-database and old-allowlist failures. Only
  independent read-only inspection is admitted with physical absence; original
  carrier0is not healthy runtime readiness. Original processes/files/job and
  data remain unchanged; unit terminal, mounts removed and all64NBD detached.
- Seven inspector/guard tests and two real-session/fake-runtime lifetime tests
  pass three race repetitions; vet/build pass.1359product files and23prior sealed
  artifacts are preserved. Current mounted Reader transport uses node lifetime;
  no cross-claim cancellation fix or new runtime API is justified here.
  Details: [NODE-BINDING.md](experiments/NODE-BINDING.md).
  Evidence: `/tmp/sandbox0-node-binding.KImNPR`.

### D-CURRENT-READ-SCOPE / D-POOL-HEALTH — Current read coordinates; close legacy recovery loop (2026-09-13)

- Reuse current-format D-MAPPING-TRACE, not the retired10005 Node read union.
  Bind all14373 existing NBD reads through exact active leases/artifact digests
  to16 old measured sandbox identities and32 phase groups. Both cohorts have
  claim volume unions22.285/22.351MB and first-command70.939/71.807MB for
  Node/Coding. These are NBD volume reads, not file demand, S3 bytes or new tests.
- Independent512-byte-sector oracle agrees with interval unions; four tests /
  eight assertions pass. Verify13 trace inputs,65 prior sealed files and1359
  unchanged product files. Historical runsc/observer and sparse-root limitations
  remain. Exact current file-extent translation is still needed before an ELF
  real-read cost case; do not repeat this schedule or broad tracing.
- Previous default-fixture zero-ready state is the already-fixed OLD-driver EOF
  then409/terminal path. This boot automatically has2new ready carriers with
  job72102 and files/processes unchanged; no manual fix or candidate installation.
  Close this diagnosis. Reuse D-RECOVERY-REMOTE; old readiness must not be an
  unrelated prerequisite for an independently safe current-candidate install.
  Timed claims still require healthy exact candidate carriers/capacity/width.
- No new claims/commands/RootFS reads, product edits or speedup. Preserve all
  data and64detachedNBDs; SSH closed, computeStopped/StopCharging at2CPU8GiB.
  All full-path1s/2s, populated-root and occupied-width gates remain open.
  Details: [CURRENT-READ-SCOPE.md](experiments/CURRENT-READ-SCOPE.md).
  Evidence: `/tmp/sandbox0-pool-health.E5nSZs`.

### D-CURRENT-EXTENTS — Current file mapping closes the ELF cost-input gap (2026-09-13)

- Read only FIEMAP for16 already-identified files in the two exact current
  format2 images; no repeated full hashes, old layouts, guest execution or traces.
  Reuse existing14373 NBD reads and current-file SHA/ELF identities. All256
  file/phase projections match an independent512-byte-cell oracle.
- Node observed union65,314,816B; initialized allocated ELF plan107,102,208B;
  overlap63,401,984B and extra43,700,224B, identical across both image/cohort pairs.
  All seven dependency candidates are fully present in the observed read union.
  These are speculative-inclusive file bytes, not S3 cost or required working set.
- Do not equate extra file bytes with extra network bytes or automatic regression.
  Geometry now supports a bounded generic exec-time real-read prototype/cost
  test, not default activation. Preserve actual exec resolution/stdio/signals/
  cancellation and account for all preparation in external first-command time.
  No more identity/layout/legacy-recovery or broad trace repetitions are needed.
- Seven Go tests including15 adversarial cases pass3race repetitions, vet/build
  pass; four projection tests/seven assertions and256 oracle groups pass. Actual
  metadata reads68underlying ciphertext calls /2,842,378 returned bytes. Both
  scans clean up, unit terminal,64NBD detached, original data/files/job preserved.
- Old baseline ready1 is recorded, not repaired or timed. SSH closed; compute
  Stopped/StopCharging at2CPU8GiB. Zero new claims/commands/product edits/speedup;
  all full-path1s/2s, populated-root and occupied-width gates remain open.
  Details: [CURRENT-EXTENTS.md](experiments/CURRENT-EXTENTS.md).
  Evidence: `/tmp/sandbox0-current-extents.0exACR`.

### D-EXEC-PREFETCH — Bounded generic real-read prototype, local qualification (2026-09-13)

- Implement a private six-file Go overlay only: exact command opt-in, real
  resolved main ELF, one active job/procd, four1MiB readers,128MiB reserved reads
  including bounded metadata. No profile, library-path guessing or tenant prewarm.
  Native pinned O_PATH/proc-fd/O_NOATIME opening rejects unsafe/unreadable files;
  stock-gVisor compatibility remains unverified. Off performs no RootFS reads.
- Place the hook after runner creation so Stop can cancel it. Correct high-offset
  overflow and symlink/`..` handling. HTTP cancellation does not reach CMD creation;
  cap optional waiting at500ms, leaving actual exec context/public10s unchanged.
  Blocked opener/read/Close retains admission and resources after caller returns;
  do not present pending zero counters as zero I/O or cleanup proof.
- Final focused race tests pass66 events x3; full process/CMD/reaper pass160;
  selected adjacent context/session/HTTP/procd tests pass17, vet/build pass.
  Real subprocess tests cover argv/env/CWD/PATH, stdio/PTY, exit/reaping, unsafe
  files and canceled prelaunch. These are local units, not startup timings.
- Preserve first VCS-stamping build failure and successful stripped pair. Old
  qualified flags are plain; produce a fresh matched plain amd64 pair. Experimental
  SHA8eec5c8a..., control0c737562.... Prefer the same experimental binary/RootFS for
  causal remote on/off so importer/binary-layout differences are not treatment.
- No cloud start, new claim/guest command, deployment or runtime-default change.
  Verify1359 unchanged product files and59 prior sealed files. Next: exact-stock
  compatibility/nonzero-work gate, then cold/cached-new literal node-v on/off cost
  trial. No repeated layout/old-pool/broad-trace loop; all regional1s/2s and true
  occupied-width/populated-root gates remain open. No speedup claimed.
  Details: [EXEC-PREFETCH.md](experiments/EXEC-PREFETCH.md).
  Evidence: `/tmp/sandbox0-exec-prefetch.vRfMxE`.

### D-PREFETCH-GUEST — Actual stock reads; noatime mount precondition (2026-09-13)

- Two isolated stock20260817 guest runs use the unchanged bounded-read prototype
  and same probe binary. Both read8,388,728B incl. headers/10calls and verify real
  bytes. Ordinary host mount FAILS atime preservation on guest AND host despite
  O_NOATIME. Do not silently relax the safe-open policy or call this a pass.
- Current XFS/Overlay implementation already uses MS_NOATIME. With only a private
  host-bind noatime mount added, the identical test PASSES, with guest/host atime
  unchanged and no pending job. This qualifies the specific file-access boundary
  under that precondition, not arbitrary mounts, a new claim or startup latency.
- Preserve two harness failures: brief --help is not the full flags listing;
  runsc empty list is JSON null, not always[]. No failed guest is restarted.
  Both exact units/runtime roots/processes/mounts/cgroups are terminal/absent;
  64NBD detached, old processes/files/job72102 and263/2072 rows unchanged.
- Guest-generated fixture is warm, not S3-backed;2CPU8GiB is not production-width.
  Zero regional samples, real Node commands, artifact imports or default changes.
  Next: current durable-importer candidate templates, verified actual noatime
  RootFS, same-binary on/off literal node-v cold/cached-new cost trial. General
  safety outside the verified mount condition and all1s/2s/density gates stay open.
- Preserve/data, both fixtures and failures. SSH closed; remote skill requires
  compute shutdown and final CLI confirmation. No old-pool/layout/trace loop.
  Details: [PREFETCH-GUEST.md](experiments/PREFETCH-GUEST.md).
  Evidence: `/tmp/sandbox0-prefetch-guest.uSVgcF`.

### D-PREFETCH-IMPORT — Real candidate artifacts for command-triggered on/off (2026-09-13)

- Current durable importer creates Node/Coding artifacts with the SAME qualified
  experimental procd8eec5c8a..., preserving OCI pins, procd.v3, format2,64KiB
  file-range and contiguous-mapping policies. Independent DB
  `s0_prefetch_import_rasjys`, owned S3 prefixes; source/original data unchanged.
- Both tests pass:8/96 journaled publications, EVERY object authenticated and
  hashed,99,657,145/1,706,485,817 plaintext bytes. Ready attestation and XFS
  superblock validate. Import workers13.632/268.991s are NOT startup timings.
  Coding1TiB logical/5,471,105,024B allocated is not a populated1TiB test.
- Retain initial unmounted-staging failure and corrected local ext4-versus-XFS
  guard. Reuse exact existing64GiB disk, no format/expansion/history deletion.
  Both units finish once; publication-ready while audit runs is not terminal.
  Original job72102/processes/configs preserved;64NBD detached, no owned loops/
  mounts/cgroups/children, staging normally unmounted, data retained, compute
  shutdown verified through the remote skill lifecycle.
- User asks when prefetch is possible: only AFTER a command arrives and its
  real executable is resolved, before process start. It is bounded main-ELF
  parallel reading, not future-command/tenant-root prediction, not full-root
  fetching, and not a claim-readiness fix. Exact working set is unknown; extra
  I/O can regress latency. All waiting and physically pending reads stay in
  full-path time/resource accounting; no HTTP timeout increase.
- Zero claims, guest commands or regional samples. Keep same procd/artifact
  for causal on/off; command-level env is supported by API but still needs
  diagnostic harness forwarding/tests. Do not reimport or repeat closed layout/
  guest-hint/legacy-pool gates. All1s/2s, populated-root and occupied-width gates
  remain open; no speedup/default promotion claimed.
  Details: [PREFETCH-IMPORT.md](experiments/PREFETCH-IMPORT.md).
  Evidence: `/tmp/sandbox0-prefetch-import.RAsJyS`.

### D-PREFETCH-TRIAL — Command-triggered ELF on/off fails activation (2026-09-13)

- Same experimental procd and exact per-image imported RootFS, command-level
  opt-in only; literal node-v, regional claim/readiness/command/combined clocks,
  unchanged10s HTTP budgets. Add/test only diagnostic env forwarding, exact
  response confirmation and requested-key-only reporting; no runtime default.
- Complete16-claim arms, eight-wide cold then cached-node NEW identities on
  separate boots: cold combined Node off2.055/on1.638s, Coding off2.514/on2.873s;
  cached-new Node off0.911/on1.356s, Coding off0.636/on1.573s. Four samples/cell,
  Coding cold4/4 misses2s both arms. Mixed results do not earn activation or p99.
- Complete-arm NBD reads1,534,084,608B off versus2,193,003,520B on (+42.95%),
  including cleanup, NOT S3 payload. Wire counters also rise but include overhead
  and can undercount INPUT. Phase CPU13-21%, zero observed lease throttling/OOM;
  no saturation signal here, not exclusion of S3/occupied-node contention.
- Retain two incomplete eight-command pilots and one mount-only diagnostic:
  49claims/48commands total,32 complete-comparison claims. Correct post-command
  observer's private mount namespace, then WAL-root versus actual mount-root
  assumption. Final exact eight-lease/artifact/XFS/Overlay/guest noatime proof
  passes, both full units finish and physical resources clear. Do not hide pilots.
- Full tool race count3 passes243 test events; vet/build pass. Final observer
  fixture6tests/12assertions passes; earlier5tests/7assertions missed root selection.
  Preserve original runtime/data, restore original two-carrier job and compute
  shape, stop via remote skill lifecycle with independent cloud verification.
- Reject this fixed broad-ELF plan, no repeated trial/import/layout/pool loop.
  It cannot predict demand or fix claim readiness. Same16CPU64GiB test shape is
  not occupied-width proof; sparse1TiB is not populated/history-bearing1TiB.
  All1s/2s requirements remain open; no production promotion, merge or tag.
  Details: [PREFETCH-TRIAL.md](experiments/PREFETCH-TRIAL.md).
  Evidence: `/tmp/sandbox0-prefetch-trial.SeM8xy`.

### D-CLAIM-SUBSTAGES — Recover existing exact-sample setup timing (planned 2026-09-13)

- D-PREFETCH-TRIAL is progress and rejects broad main-ELF prefetch activation.
  Do not replay it. Recover only missing historical manager/driver/slow-Ensure
  logs for its32 complete-comparison samples, using sealed boot/allocation IDs.
- Falsifiable question: is the remaining slow claim attributable to XFS mount,
  local journal/device setup or writer authority? Existing aggregate CPU/wire
  counters cannot answer that. Preserve censored Ensure logs below500ms as
  missing, not zero. Durations are nested; do not add maxima or infer exact
  dependency timestamps or S3 causality from mount time alone.
- No implementation/tuning variable, new claim, command, RootFS read or import.
  Original2CPU8GiB startup only to read persistent journals, original runtime/data
  verified before and after, owned SSH then stopped compute. If logs rotated,
  record the gap without repeating a workload. User-requested ledger is retained.
  Evidence: `/tmp/sandbox0-claim-substages.0cnLzb`.

Result (2026-09-13; diagnostic only):

- Recovered32 manager +32 driver +4 slow-Ensure logs from the exact prior boots,
  slots/allocations/artifacts and PIDs. All nine node substage fields match;
  readiness clocks match microsecond rounding. Zero new claims or commands.
- Cold Coding03-off: XFS452.498–460.359ms, Overlay116.714–118.291ms, their exact
  per-sample sum570.682–578.591ms. Explicit session journal stages total only
  5.692–6.621ms; writer RPC1.050–6.655ms, attach13.478–21.344ms. Mount time is
  not pure S3 time. No basis for a journal/RPC-only fix or hardware exoneration.
- Exact slowest combined2.514395710s retains claim1.633414920s, command0.880966133s,
  nested Ensure0.662697s, runsc create0.258984s/start0.180162s, procd probe0.419253s.
  Even optimistic zero-XFS alone leaves all4 cold Coding combined2.054–2.062s.
  Cross-boot differences are not direct command-prefetch claim benefits.
- Other28 Ensure details are threshold-censored (successful enclosing calls
  below500ms), not zero. Preserve parser's scientific-notation failure; exact
  rational fix passes8tests/20assertions and8negative real-data mutations;
  independent raw-manager joins reproduce32 records. No query/workload replay.
- Prior135 artifacts and1359 unchanged source files retained; original runtime,
  job73058, data and physical absence checked. Original2CPU8GiB stopped through
  skill lifecycle and independent CLI. All full goal gates remain open.
  Details: [CLAIM-SUBSTAGES.md](experiments/CLAIM-SUBSTAGES.md).

### D-XFS-LOG-SCOPE — Exact log geometry limits the log-only opportunity (2026-09-13)

- Reuse D-MAPPING-TRACE's exact format2 artifacts and14373 original NBD reads;
  remotely read only4KiB per image through independent authenticated Readers.
  Both images have4AGs, logs64MiB Node/512MiB Coding. No new claim/command/mount.
- Cold log-read unions Node100.602–101.236ms, Coding130.313–130.824ms;
  Coding's374.867–378.286ms duration SUM double-counts concurrent work. Its
  unique log bytes2,104,832 differ from Node by only512B, not8x with log size.
  Zero log-range reads in all16 immediate first-command windows. No S3-only
  attribution or exact current-kernel caller proof follows from these ranges.
- Optimistic same-sample removal of ALL log-read time still leaves four cold
  Coding combined2.535145–2.535565s. Log-only work cannot close that observed
  gap. Historical instrumented samples are not D-PREFETCH-TRIAL's new artifacts;
  never subtract this budget from its452–460ms XFS mount figures.
- Successful inspection:6 underlying ciphertext calls/393,144B returned,
  two independent Go/Ruby geometry matches,26 corrupt-sector/identity rejects,
  5interval tests/113assertions and32real-data union-oracle matches. Go race x3,
  vet/build pass; all1359 runtime source files unchanged.
- Retain first Reader-init failure: runner omitted the existing test credential
  profile. Correct ONLY that diagnostic environment, same binary/targets,
  distinct unit/output; no invisible replay or failed-run zero-I/O claim.
- Original runtime/job73058/data/service PIDs and64detachedNBDs preserved;
  SSH closed, original2CPU8GiB stopped via skill lifecycle and CLI. No runtime
  promotion or fresh startup pass. All full goal gates remain open.
  Details: [XFS-LOG-SCOPE.md](experiments/XFS-LOG-SCOPE.md).
  Evidence: `/tmp/sandbox0-xfs-log-scope.Fli0cZ`.

### D-CODING-LOOP-NET — Coding cold benefit; cached/lifecycle gates remain (2026-09-13)

- The previous turn clarified demand/prefetch triggers but produced no new
  performance evidence. D-LOOP-DIO and D-LOOP-NET covered Node only; do not
  replay that rejected comparison. Coding's larger metadata footprint leaves
  its post-DIO net effect unmeasured.
- Reuse the retained Coding wrapper and exact format2 ordinary artifact;
  one ordinary/wrapper/wrapper/ordinary campaign, cold process-local Reader
  and encrypted-header caches then a cached-node NEW guest per pair. Real
  encrypted OSS/COW/NBD/runsc/procd and immediate literal node-v; unchanged
  10s request budgets, no guest before demand, no per-command training.
- Offline test-prefix publication is bounded at128objects/6GiB/600s; detach
  its readonly source before guests. This is image preparation, not hidden
  node prewarming or a claim timeout extension. No original object replacement.
- Compare both cold wrapper samples against the reverse ordinary control;
  require lower combined latency and storage cost, without cached-new
  regression. Failure or weak evidence prevents promotion. This isolated
  2CPU8GiB component excludes regional/PG/provider preparation and occupied
  width, so cannot pass any full production SLO or populated-root gate.
- Preserve original runtime/data/job, scoped resources and every failure;
  stop compute using the remote skill lifecycle. No production/code default,
  merge or tag. Local evidence: `/tmp/sandbox0-coding-loop-net.XgviED`.

Result:

- One complete8-guest campaign. Cold local combined O1=3.536s, W1=1.805s,
  W2=1.811s, O2=2.207s; both wrappers save396–402ms against reverse O2.
  Readiness saves438–508ms, but node-v is36–112ms slower. Not a regional pass.
- Cold HTTP attempts338/217/217/337 and ciphertext53.198/50.823/50.840/53.132MB.
  Distinct objects50/19/19/50; cache and request budgets unchanged. Cost benefit
  survives O2, unlike the prior Node result; do not attribute O1's full gap.
- Cached-new combined293/299/378/297ms, all zero source requests. W2's175ms
  node-v anomaly cannot be caused by a fresh S3 read. Conservative cached
  no-regression gate fails; two samples do not establish systematic regression
  or tails. No default adoption; full layered lifecycle remains incompatible.
- Offline86objects/1,688,988,275B encoded plaintext, immutable originals and
  source staging detached before guests.223 reports verified;8tests/70assertions,
  13negative evidence mutations, Go12tests x3/race plus vet/build pass. All1359
  runtime source files unchanged, no failed/replayed remote workload.
- Original runtime/job73058/data/network/tracing preserved;229 mount tables
  checked, all owned physical resources absent. Skill-managed stop and fresh
  cloud check confirm original2CPU8GiB Stopped/StopCharging. All full gates open.
  Details: [CODING-LOOP-NET.md](experiments/CODING-LOOP-NET.md).

### D-LAYERED-APPLY — Source coverage reaches actual Apply I/O (2026-09-13)

- Prior Coding comparison is progress; do not repeat it or completed layered
  Diff/resolver scans. Reuse main Apply in private Go overlays, routing separate
  old/source logical coverage through validation, conflict comparisons and copy.
  Layer identity also reaches replacement/hardlink/removal checks.
- Actual temp-file tests retain4KiB data/allocated bytes in a1GiB logical sparse
  addition without lower physical extents, detect conflicts before mutation,
  preserve tested copy-up hardlinks and disjoint sparse changes, and reject
  cross-layer inode-number shortcuts. Layer/device provenance is asserted,
  not authenticated kernel/ctld or actual EROFS evidence.
- First race run72passes/1skip. Added counterexample exposes prototype-only
  legacy-proof guard dependence on a caller flag. Keep proof-red and exact
  sources; reject any active non-default policy, even when that flag is absent.
  Private report cannot enter old ApplyResult/WorkerResult validation.
- Final two-package race count3:588pass events/42explicit skips;10new top-level
  Apply tests,45new pass events with subtests/repetitions. Vet/build pass.
  Same live test process completes; no restart or startup timing claim.
- Target remains current Scan/extent semantics, so full EROFS target resolution,
  non-regular entries, sparse/hardlink fidelity and authenticated durable
  lifecycle are NOT qualified. No format/default admission or goal gate closes.
- All1359 runtime source files unchanged; no remote/cloud operation, new claim,
  command, production change, merge or tag. Local evidence:
  `/tmp/sandbox0-layered-apply.XlDmWp`.
  Details: [LAYERED-APPLY.md](experiments/LAYERED-APPLY.md).

### D-LAYERED-TARGET — Complete native target scan and merge (2026-09-13)

- Previous explanation-only turn was no progress; continue the missing target
  path rather than replaying layout latency. Diagnostic overlays reuse the
  main scanner/Diff/Apply and qualified held-root resolver. All paths are
  included; regular underlying identity precedes xattr caching, and separate
  logical target coverage is used before and after Apply. No old proof bridge.
- Retain first ARM64-on-x86 loader failure (tests never execute). Explicit
  static amd64 cross-build and new owned clones then expose a real directory
  metadata bug on both full retained images: directory st_size is interpreted
  as mutable file length, causing an attempted writable open of a directory.
  The bug also reproduces against unmodified main Diff. Keep the red case;
  isolate the correction in overlays without skipping directory metadata.
- A long AF_UNIX fixture path is corrected using a held directory FD without
  relocating or skipping the XFS socket. A third, repaired campaign passes
  Node7,303 and Coding150,772 final paths. Disjoint sparse edits, actual lower
  copy-up, new hardlinks and conflict-before-mutation checks pass. Independent
  complete target rescans match Apply's final manifests. This is native,
  same-base controlled merging, not authenticated different-artifact rebase.
- Final two-package race count3:630pass events/99explicit skips; native XFS
  mechanisms37pass/1skip; standalone default metadata correction60pass/3skip.
  Vet/static cross-build pass. All failed binaries, operational targets and
  receipts are preserved. Do not reinterpret offline scan duration as startup.
- All1359 product files, original runtime/job73058, DB counts and immutable
  images are unchanged. Final227 mount tables show no owned mounts; loops and
  all64NBDs are detached. Remote skill lifecycle preserves18 upper clones and
  data, closes the owned SSH session and stops the original2CPU8GiB instance;
  independent cloud observation confirmsStopped/StopCharging.
- Zero new claims, guest commands, startup samples, prewarm or S3 publication.
  Full authenticated lifecycle, sparse/hardlink semantics, cached-command
  behavior and populated/history-bearing production-width1s/2s gates remain
  open. No production change, PR, merge or tag. Evidence:
  `/tmp/sandbox0-layered-target.4OzjM0`.
  Details: [LAYERED-TARGET.md](experiments/LAYERED-TARGET.md).

### D-SPARSE-CONTRACT — Native sparse counterexample, no startup samples (2026-09-13)

- One owned XFS fixture, pinned Linux6.8/erofs-utils1.7.1, plain and4KiB chunked
  EROFS plus same-XFS OverlayFS control. No kernel upgrade/full root rebuild.
  Plain copy-up changes sparse8KiB allocation to16MiB while preserving bytes;
  the same-XFS control preserves holes. Hash equality alone is insufficient.
- Chunk build/fsck/mount succeed, but sparse and tail payload reads return EIO.
  Reuse the exact retained28672-byte image in a read-only followup: all-hole and
  allocated-zero hash correctly; sparse/tail have no complete hash. Both FIEMAP
  and dump show failed ranges beyond image bounds. Exact producer defect remains
  unisolated; this path has no S3. Correct earlier mistaken all-hole attribution.
- The initial runner remains FAILED: its final source check rejects only SHARED
  flags introduced by same-XFS control copy-up. Independent exact-field comparison
  confirms unchanged source bytes/identity/allocation/SEEK/physical addresses.
  Do not erase this harness failure or label unexecuted chunk copy-up a pass.
- Read-only followup completes;88 exported reports verify,1359 product files
  and original upper hash/runtime/job73058/DB counts unchanged. Final228 mount
  tables clean, all64NBDs detached, artifacts retained. Skill-managed shutdown
  independently confirms original2CPU8GiB Stopped/StopCharging.
- Zero claims, guest commands, startup samples or production changes. Candidate
  remains unqualified. Current work is storage-layout correctness, not pre-claim
  rootfs prefetch. Rejected broad-ELF reads start only after a real command and
  their time cannot be moved outside cold-start accounting. All full gates open.
  Evidence: `/tmp/sandbox0-sparse-contract.vgvAn8/evidence.json`.
  Details: [SPARSE-CONTRACT.md](experiments/SPARSE-CONTRACT.md).

### D-SPARSE-PRODUCER — Exact upstream hole-merge defect isolated (2026-09-13)

- Previous native sparse counterexample is progress. Execute actual upstream
  blobchunk/hashmap/SHA code from pristine1.7.1, then only upstream545988a in a
  separate diagnostic worktree. No remote replay, kernel change or full rebuild.
- Old producer omits minextblks updates across holes and merges sparse/tail into
  overlong contiguous ranges. Both fail bounds exactly as the retained remote
  maps predict. The one-fix variant preserves all four payloads and source data
  coverage: sparse8KiB and tail4KiB. Normal and UBSan trap-mode results match.
  These are aarch64/local-source mechanism tests, not full amd64 mkfs admission.
- Preserve baseline exit1, normal build warnings,404 source-tar attempt and
  two sanitizer link failures. ASan libraries unavailable; ASan never executes.
  Existing git tag supplies verified source, no local packages installed.
- Separate blocker: tested Linux6.8 EROFS has generic seek; OverlayFS copy-up
  depends on real SEEK_DATA. Inspected upstream6.12 also lacks it;6.13 has the
  added implementation. Producer repair alone cannot fix current copy-up.
  Reject current tool/kernel combination for adoption, not all compact layouts.
  Next qualification must cover both actual producer and stock-kernel holes
  before another full-layout/startup trial; no automatic production upgrade.
- Prior46 artifacts and1359 product files unchanged. Existing header/mapping
  cache and bounded-read integration remain on XFS/Overlay, with no EROFS mount
  default. Verifier2tests/10assertions, eight invalid-result mutations rejected.
  Zero cloud starts, claims, commands, startup samples or production changes.
  All original cold/cached-new, first-command,1s/2s and full-size/density gates
  remain open. Evidence: `/tmp/sandbox0-sparse-producer.wDt9yJ/evidence.json`.
  Details: [SPARSE-PRODUCER.md](experiments/SPARSE-PRODUCER.md).

### D-STOCK-PROFILE — Vendor backport changes the next qualification target (2026-09-13)

- Previous question-response turn is no progress. Fresh infra main1bdd57f pins
  Alibaba Cloud Linux4.0.5 image20260801, officially kernel6.6.102-7.alnx4, not
  the retained Ubuntu6.8 fixture. Inspect exact signed vendor source/binaries;
  never infer backport capability from upstream version numbers.
- Vendor EROFS source and x86_64 module both contain the seek implementation.
  The file-operations llseek relocation points to the actual function, whose
  range contains both iomap calls. Packaged EROFS/Overlay config is enabled.
  This does not observe a running production kernel or qualify native copy-up.
- Repository candidate erofs-utils1.9.2 has transition accounting and zero-hole
  conversion; no complete producer execution yet. Downloaded1.9.4 postdates
  the image and is excluded, not silently treated as the installed version.
  Overlay copy-up still has1MiB splice requests that can span holes: actual
  allocation needs execution, not a source-only pass or guessed byte count.
- Four selected RPM signatures pass with the official key in a private DB;
  host trust/packages untouched. Metadata compressed/open and binary hashes
  match. Retain initial NOKEY exit4, unavailable Ruby XML-parser exit1 and source
  versus packaged config diff. Go standard-library metadata verifier succeeds.
- All240 prior artifacts and1359 product files unchanged. Evidence verifier
  passes3tests/17assertions (14 invalid-report mutations rejected). Zero cloud
  starts/kernel boots/claims/commands/startup samples. No default integration,
  timeout/cache/machine enlargement, rollout, merge or tag. Qualify the exact
  stock-kernel/full-producer copy-up next; all real startup/density gates open.
  Evidence: `/tmp/sandbox0-stock-profile.vhypbh/evidence.json`.
  Details: [STOCK-PROFILE.md](experiments/STOCK-PROFILE.md).

### D-STOCK-VM — Full producer reads correctly; copy-up cost blocks adoption (2026-09-13)

- Previous stock-profile turn is progress. Run exact vendor1.9.2 producer with
  host Ubuntu libraries, then one no-network TCG VM with signed vendor
  kernel6.6.102-7.alnx4/modules. Host kernel and2CPU8GiB shape unchanged; owned
  unpacked QEMU only, no package installation. Retain apt access warning and
  pre-launch explicit BIOS-path correction, not a hidden failed VM replay.
- Full mkfs/fsck/read/copy-up completes for plain/chunked EROFS and same-XFS
  control. All68 full payload observations match. Previous1.7.1 sparse/tail EIO
  is gone; chunk lower exposes the correct two4KiB data ranges.
- Sparse8KiB becomes3.875MiB allocated after chunk copy-up:2MiB initialized
  extents and1.875MiB unwritten (256x/496x respectively). Same-XFS stays8KiB;
  plain becomes16MiB. Tail4KiB becomes16KiB; chunk hole-only stays0. Separate
  EROFS logical st_blocks, page-cache-sensitive XFS SEEK, FIEMAP initialized/
  unwritten extents and object size; none is a measured S3 payload counter.
- Current main lower/upper/work share one reflink-enabled XFS, so the control
  represents an actual baseline capability. Reject this cross-FS candidate's
  copy-up cost; correct reads alone do not earn adoption. This unadopted path
  is not the proven cause of current default-runtime cold-start latency.
- One campaign/VM completes,58 files including serial console verify. Probe2tests
  and verifier2tests/18assertions pass (13 invalid-report mutations rejected).
  All149 prior artifacts/1359 product files unchanged. Original processes/job
  73058/DB rows preserved;222 mount tables clean,64NBDs detached, no owned loop/
  VM. Artifacts retained; skill shutdown plus independent cloud receipt proves
  original2CPU8GiB Stopped/StopCharging. Prior Stopping receipts retained.
- Zero claims/commands/regional samples; no timeout/cache/machine enlargement,
  prewarm, format default, rollout, merge or tag. Next layout work must preserve
  cheap same-FS copy-up or prove bounded replacement cost. All startup/full-root/
  occupied-width gates remain open. Evidence:
  `/tmp/sandbox0-stock-vm.WrELv4/evidence.json`.
  Details: [STOCK-VM.md](experiments/STOCK-VM.md).

### D-SESSION-ACTIVATION-SCOPE — Stopped-session work scales before readiness (2026-09-13)

- Previous question-response turn is no progress. Inspect actual claim-stage
  authority dependencies before considering parallelism; no unsafe reordering.
  New hypothesis is retained-session work before procd listens, not another
  tenant prefetch, platform-procd seed or filesystem-layout replay.
- Execute unchanged FileStore/Journal/Supervisor through a test-only Go overlay.
  Linux inotify captures only Activate; reject overflow and independently check
  logs, descriptor targets, resulting state and complete journal bytes.
- For0/1/32/128 stopped sessions, same-owner activation opens and atomically
  replaces exactly0/1/32/128 state files, opens the same number of journals and
  retains that many active-journal FDs. Every journal's bytes remain unchanged.
  Copied-store reset deletes all0/3/96/384 valid fixture files before returning,
  exposes zero source sessions and does not recover their old state/journals.
- Eight normal plus24 race-count3 subcases pass, zero failures/skips. Complete
  result matrices and1359 unchanged product files verify. Relevant session and
  controller source matches observed origin/main0f092204; no fresh fetch claimed.
- Counts establish history-dependent synchronous work, not S3 bytes or latency.
  Local warm tiny fixtures are not populated-root or production-width acceptance.
  No cloud start, claim, command, SLO sample, runtime/default change or production
  mutation. Prior empty-session cold misses remain unexplained by this finding.
- Next qualify bounded ownership-safe session activation/reset without losing
  stopped records, idempotency, cursor integrity, generation fencing or fork
  isolation. Do not simply background the old loop or weaken readiness. All
  original gates remain open. Evidence:
  `/tmp/sandbox0-session-activation.aUmkfG/evidence.json`.
  Details: [SESSION-ACTIVATION-SCOPE.md](experiments/SESSION-ACTIVATION-SCOPE.md).

### D-SESSION-OWNER-RESET — Repair missing-owner copied-state admission (2026-09-13)

- Previous session-scope turn is progress. Evaluating bounded activation reveals
  that BindSandbox adopts a missing owner before the manager's explicit copied-
  state reset is considered. This is an ownership prerequisite, not a speedup.
- Actual unchanged-source tests retain three failures: copied stopped state/key
  exposed, copied active session restarted, corrupt copied state decoded. A
  separate original-source controller test confirms copied sessions exposed at
  readiness or failed activation. The harmless host unit attempt is/bin/true;
  there are no guest commands or regional samples.
- Shared binding logic now forbids legacy adoption under explicit reset, leaves
  owner unpublished until cleanup succeeds, and supports a missing state dir.
  Ordinary legacy adoption, exact-owner retry/idempotency and foreign-owner
  fail-closed behavior remain. No format, authority, asynchronous cleanup,
  readiness proof, timeout, cache or prefetch change.
- Fixed related unit suites57pass events; final race count3 with the independent
  controller regression180pass events/zero failures or skips. Vet passes. Keep
  the first VCS-stamping build failure and recorder refusal; separate build with
  observed infra-main flags succeeds as a static stripped linux/amd64 procd.
- Five product files change: two implementation, two new tests and one doc.
  Previous inventory has1356unchanged/3changed; new1361-file source inventory:
  `/tmp/sandbox0-session-owner.M6Ksu5/source-after.json`. Prior artifacts remain
  tied to their old source identity; new procd is not imported or deployed.
- No remote/cloud action, new claim/node-v/SLO sample, production change, PR,
  merge or tag. Linear history work and empty-history cold misses remain open.
  Asked the user to confirm claim-only versus claim-plus-command hard2s scope;
  no reply yet, no acceptance relaxation. Evidence:
  `/tmp/sandbox0-session-owner.M6Ksu5/evidence.json`.
  Details: [SESSION-OWNER-RESET.md](experiments/SESSION-OWNER-RESET.md).

### D-FILE-ACCESS-CONTRACT — Exclusive metadata does not match host freeze (2026-09-13)

- Test stock runsc shared versus exclusive on two fresh synthetic same-XFS
  OverlayFS roots. DirectFS/systrap/overlay2 stay fixed. This checks a persistence
  prerequisite, not prefetch or cold-start latency; no procd, NBD or S3 workload.
- Both guest arms and four freeze/thaw observations complete. Eight host file
  payload observations match. Shared metadata matches; exclusive atimes and
  modified-file mtime/ctime differ even after guest file fsync (mtime1,589,005ns).
  Do not adopt as a semantics-equivalent configuration-only switch. No payload
  loss, universal timestamp contract or measured performance effect is proven.
- Preserve runner exit1 at final owned-mount guard; the matching mountinfo line
  was not captured. All five unmounts and loop detach pass. Later independent
  checks of226 mount tables find no owned mounts/loops/processes. This separate
  cleanup proof does not turn the failed unit into a passing test run.
- Retain ignored RuntimeMaxSec/oneshot warning and corrected upstream tag-object
  404 downloads. All59 exported reports verify, current1361 product files remain
  unchanged, original remote runtime/job/database preserved. Keep detached test
  data, close SSH master and stop original2CPU8GiB compute with independent
  Stopped/StopCharging confirmation. No replay or configuration/default change.
- Ubuntu6.8 fixture is not production-kernel/width qualification. Zero new
  claims/node-v/SLO samples, rollout, merge or tag. Empty-history cold latency,
  bounded historical-state activation and full-size/density gates stay open.
  Evidence: `/tmp/sandbox0-file-access.Nm9nMz/evidence.json`.
  Details: [FILE-ACCESS-CONTRACT.md](experiments/FILE-ACCESS-CONTRACT.md).

### D-INTRA-READ-SCOPE — Actual window-boundary splits, not a worker-count fix (2026-09-13)

- Previous file-access turn is progress. Reuse hash-bound current-format real
  claim/node-v traces, excluding mapping flights from payload-call counts.
  All14373 reads/32phase groups bind;12selected inputs and4raw traces verify.
- Cold Node command has70multi-payload NBD reads, enclosing union703.325–703.837ms;
  Coding has5–6, union99.294–99.511ms. Claim unions203.260–204.899ms versus
  46.064–46.356ms. These are exposure, not removable time or new SLO samples.
- All78 adjacent Node command call pairs use contiguous spans in the same object.
  Exact ordinary mapping profiles bind62singleton Node payload calls per sample
  to full units crossing absolute1MiB windows; Coding has1–3. Each examined cold
  multi-payload NBD read has one ReadAt scope. No dirty-span or mapping dependency
  is relabeled as independently parallel data work.
- Actual unchanged Reader: same16x64KiB content/final128KiB demand uses1source GET
  when aligned,2when shifted4KiB; identical total stored bytes and verified output.
  Raw/mixed-zstd4cells x3race pass12records/15testevents, zero failures/skips.
  A fresh Reader/cache consumer adds no data GET, not a new sandbox claim.
- Qualify canonical group boundaries within the existing1MiB/128MiB/8source
  limits before adding workers. Extra crossing-unit reads, overlapping roots,
  allocations and cached-new effects still need whole-workload cost checks;
  no candidate/default is implemented or admitted. Coding's main gap remains.
- Keep initial analyzer syntax exit1 and geometry lookup KeyError, its explicit
  local-process stop, and superseded locator flags. Correct trace bare hex versus
  profile sha256-prefixed digests; geometry-normalized.json is authoritative.
  Do not infer actual cross-artifact serving from the observer error.
- Five analysis tests/107assertions pass.1361product files/32prior artifacts
  unchanged. No remote/cloud/claim/command/SLO sample, production/merge/tag or
  gate relaxation. Evidence: `/tmp/sandbox0-intra-read.cgjqKe/evidence.json`.
  Details: [INTRA-READ-SCOPE.md](experiments/INTRA-READ-SCOPE.md).

### D-WINDOW-MEMBERSHIP — Indexed complete-unit groups, remote cost still open (2026-09-13)

- Keep actual-demand-triggered reads, claim-time RootFS binding and unchanged
  128MiB cache / 8 source permits / 1MiB windows / 10s public timeout.
- Reject start-bucket grouping: complete mixed-root source calls fall 7->4,
  but raw bytes rise 3145728->4063232 (+29.17%). Full-file accounting matters.
- Homogeneous run-relative grouping cuts that synthetic workload to 2 calls /
  2097152 bytes, but loses 572 Node / 9491 Coding original grouping opportunities
  and scans backward per demand. Do not adopt it from the synthetic gain alone.
- Third temporary candidate indexes greedy mixed-size complete-unit groups once
  per authenticated mapping decode; binary lookup and charged private metadata,
  no new wire format, source prewarm or independent cache. It retains 2 calls /
  2MiB / zero duplicates for the eight-Reader synthetic complete file.
- Full ordinary-image geometry: Node 4341 views, gains/losses 192/0; Coding
  80781 views, gains/losses 3209/9. These are static entry eligibility, not GETs
  or real startup. Retain the nine Coding losses and initial-envelope extra64KiB.
- Max-leaf near-end indexed lookup 73.44–120.3ns locally versus homogeneous
  scan 1.047–1.259ms; original planner 331.2–701.6ns. Preparation adds once-per-
  decode work: up to3.79ms / 256KiB for max-size singleton groups, fully charged
  to the existing mapping cache. Linux/arm64 local CPU, not remote x86 or SLO.
- Corrected mechanism x3 race:48 records/54 pass events. Geometry/invariants14;
  targeted existing regression x3:1314; full package race x1:932 pass/2 opt-in
  skips; vet passes. Preserve all failed fixture/patch attempts and corrections,
  including rebuilding derived metadata after a test mutates its copied page.
- All1361 product files and19 prior artifacts unchanged. No remote/cloud,
  new claim/command, default adoption, production/merge/tag or gate relaxation.
  Next is paired real encrypted full-demand cost, then original regional
  claim-plus-literal-node-v cold/cached-new and occupied-width acceptance.
- Evidence: `/tmp/sandbox0-window-membership.OvxY7g/evidence.json`.
  Details: [WINDOW-MEMBERSHIP.md](experiments/WINDOW-MEMBERSHIP.md).

### D-INDEXED-NET — Real encrypted reads improve aggregate cost, Coding still regresses (2026-09-13)

- Six separate processes on original2CPU/8192MiB: baseline/indexed/indexed/
  baseline/baseline/indexed. Each starts fresh decoded/mapping/header caches,
  then reuses them for new Reader identities. No prewarm, runtime rollout,
  live sandbox claim or guest command; this is serialized retained-demand cost.
- Per arm/run14373 exact logical reads plus16 constructors verify;86238 output
  hashes and96 constructors overall. Three baseline runs reproduce exact frozen
  source/cache shapes. Candidate output,8-source,128MiB,1MiB and10s bounds hold.
- Every baseline has547 source calls/581 real HTTP attempts; every indexed run
  436/470. Source bytes73819484->72336511 (-2.01%); observed HTTP body bytes
  82513100->79248704 (-3.96%). Cold HTTP389->301; cached Readers192->169.
- Gains are mostly Node. Coding body bytes rise1.47% cold and0.86% cached;
  cached Coding time overlaps/regresses. Do not use the aggregate to claim a
  universal reduction or adopt the product default. Evaluate both images live.
- Serialized cohort wall medians cold4.508s->3.900s, cached2.164s->2.077s;
  these are NOT single-sandbox startup latency. Preserve slower first baseline
  and all six samples; backend cache/environmental variance is uncontrolled.
  Process CPU median2.873s->2.799s; no host steal observed, no resident pressure.
- Both probe builds pass9 race tests. Independent checks verify complete demand
  ownership,12 HTTP unions and unchanged before/after state. Initial post-boot
  Nomad readiness, discovery and analyzer failures are retained.26 EOF-short
  response bodies explain requested versus actual byte differences identically
  in every arm; corrected analyzer does not rerun or relax the workload.
- All33 exported artifacts verify; original process/file/config identities,
  job73058/rows2121 preserved;64 NBDs detached, no probe left. Confirmed original
  2CPU/8192MiB Stopped/StopCharging2026-09-13T05:52:51.596924688Z; /data retained.
- All1361 product files/51 prior candidate artifacts unchanged. Next is isolated
  regional claim/readiness plus literal node-v cold/cached-new comparison,
  retaining Coding regression and original populated/occupied-width/1s/2s gates.
  Evidence: `/tmp/sandbox0-indexed-net.gtvXYg/evidence.json`.
  Details: [INDEXED-NET.md](experiments/INDEXED-NET.md).

### D-INDEXED-LIVE — Node improves, Coding remains variable and above2s (2026-09-13)

- Follow the real encrypted-source qualification with baseline/indexed/indexed/
  baseline live regional TLS claims and literal node-v. Four fresh boots,
  two8-wide mixed cohorts per boot,64 unique sandbox identities/commands.
  All commands exit0 with exact stdout, no POST retries. Cold counters prove
  zero tenant wire/NBD reads before claim; cached cohorts use NEW identities.
- Same16CPU64GiB diagnostic host,1750m/1792MiB leases, stock runsc20260817,
  two unchanged ordinary artifacts/frozen UTC procd,128MiB cache,8 source
  permits,1MiB windows and10s timeout. Only indexed ctld grouping differs.
  No import/prewarm, production NLB or occupied/populated-size acceptance.
- All64 claim/readiness samples stay below2s; maximum regional claim1.629619s.
  Combined claim-to-command completion exceeds2s in24/64 samples. Candidate
  Node cold maxima1.846701/1.974899s versus baselines2.007869/2.067729s.
  Candidate Coding cold2.503405/2.740504s versus2.614394/2.479726s: NOT a
  stable Coding improvement. Retain the slower second candidate and do not
  promote this as a general2s fix. The narrow Node margin is not a hard bound.
- Cached-new Node combined maxima: baseline1.123789/1.031990s, candidate
  1.030133/1.104205s. Coding: baseline1.688262/1.637024s, candidate
  1.544122/0.855212s. Identical candidate Coding command maxima vary from
  0.762792 to0.088502s; the live cause is not identified by this experiment.
- Full-arm completed NBD bytes remain1.533916–1.533990GB, including cleanup.
  Matched incoming TCP443 bytes86.94/87.06MB baseline versus83.46/77.54MB
  candidate include protocol overhead and possible undercount; not S3 GETs or
  completed HTTP payload bytes. No quota throttling/OOM or host steal observed.
  Minimum host available63.09GB proves this is not occupied-memory acceptance.
- Runtime/claim/harness local race tests493 pass events; driver113. Preserve
  privileged/soak skips. Independent verification checks132 arm exports,
  all64 command results, boot/binary/lease/RootFS/noatime/cleanup bindings.
  Correlated claim phases remain separate from unmeasured command-internal
  attribution. Generator/analyzer/early observation failures are retained;
  no benchmark replay. Arm03 extra settings/hash read occurred during cleanup,
  outside both timed windows; full-arm CPU observations include that audit.
- Original service/config hashes and two-carrier job definition restored at
  index74750, both original carriers ready,64NBDs detached; original/source
  DB checks unchanged, older trial2121rows, new owned clone2136rows. Fourteen
  restoration exports verified, private config/dump stays remote. Original
  2CPU8192MiB confirmedStopped/StopCharging06:50:42.360525157Z; /data retained.
- Keep the goal open. Do not rerun unchanged grouping, rejected recency-only
  or larger-cache tuning. Quantify a mixed-image whole-path benefit from the
  existing full-demand/cache evidence before another candidate; distinguish
  storage/host variance from cache effects and preserve populated/history,
  occupied real-width, first-command and durable lifecycle requirements.
  Evidence: `/tmp/sandbox0-indexed-live.teiDkw/evidence.json`.
  Details: [INDEXED-LIVE.md](experiments/INDEXED-LIVE.md).

### D-HISTORY-READ — current-root opening is bounded; history fragments demand (2026-09-13)

- The previous explanatory turn added no performance evidence. Revalidate all
  1361 inventoried product files and the latest live evidence before testing
  the unqualified writable-history requirement; no remote/cloud call.
- Add a fully backed 64MiB compressed block-map regression with three separate
  256-generation histories: same-block edits, alternating demanded-block edits
  and outside-demand edits. Alternate incremental/composite-batch publication;
  every history exercises root splitting. This is not XFS or encrypted S3.
- At generations0/1/16/64/256, all60 prefix reads verify; all15 whole64MiB
  images verify separately. Fresh/disabled-cache opening reads exactly one
  current root and zero data. Every second Reader with the128MiB cache reuses
  all demanded mapping/data. Reader identities are not real sandbox claims.
- With initially empty128MiB cache, fragmented128KiB demand rises from1 data
  source request to18 by generation16, remaining18 at64/256. Stored data bytes
  fall262592->98360: fewer bytes do not imply fewer serial waits. Same-block
  history needs3 data requests; outside-demand history stays1. With cache
  disabled, fragmented demand needs32 data and32 mapping reads after opening.
  Counts are RangeSource calls, not HTTP/OSS GETs; no SLO time is measured.
- Preliminary16MiB fixture output retained, superseded by64MiB split-root and
  whole-content coverage. Final targeted race34 pass events; full package934
  pass events, two opt-in skips preserved. Only the new test changes the
  product inventory1361->1362; runtime behavior, cache/concurrency/10s timeout
  and all prior product files remain unchanged. No local e2e or privileged test.
- Next evaluate demand-only independent fragment scheduling within existing
  node-wide admission, fairness and memory bounds, including integrity/error/
  tail/cancellation behavior. This history-specific evidence does not overturn
  the earlier limited fresh-image intra-read coverage or solve cold Coding.
  Actual history-bearing RootFS, cold/cached-new sandbox claims and literal
  node-v, occupied-width and all original1s/2s gates remain unverified.
  Evidence: `/tmp/sandbox0-history-read.dhNC4T/evidence.json`.
  Details: [HISTORY-READ.md](experiments/HISTORY-READ.md).

### D-DEMAND-FRAGMENTS — bounded parallel candidate; saturation limits the gain (2026-09-13)

- Previous goal turn made progress via the history regression. Reverify all
  1362 product files and its9 sealed artifacts, then implement an isolated
  demand-only candidate. No product-default, remote/cloud or timeout change.
- Schedule only actual caller bytes in an already verified mapping leaf,
  beginning at an uncached compressed view. Deduplicate required units; keep
  ordinary complete ranges unchanged. No next-page fetch, command hint or
  tenant prewarm. Complete compression/authentication units still amplify bytes.
- Reuse node-wide8-source admission, singleflight and Reader lifetime. Cap at
  8 private batches, each with at most8 workers and128KiB output; busy readers
  fall back without another queue. Additional private output<=1MiB and live
  verified-unit references<=4MiB per cache, excluding allocator/codec overhead.
  Join workers and expose only the verified logical prefix before first error.
- Controlled20ms-per-source model,128MiB initially empty cache: one Reader's
  18 calls/360ms become18 calls/60ms, unchanged98360storedbytes. Disabled cache
  32 calls/640ms/328128bytes become18/60ms/98360bytes. These are fake-clock
  mechanism results, not S3 or startup latency. Full64MiB history hashes pass;
  disabled-cache fragmented demand also reduces mapping reads32->1.
- Crucial source-saturation counterexample: eight distinct Readers need144
  requests and360ms makespan in BOTH paths. All five race repeats verify.
  Single-Reader6x does not establish node throughput or occupied-width gain.
- Corrected safety checks cover exact demand bounds, partial checksum errors,
  caller suffix, tail/holes, EOF, cancellation/join, encrypted memory-store
  reads and12-Reader fairness/fallback. Initial invalid coarse-view fixture
  failure and source retained; corrected to a valid view plus coarse neighbor.
  Two full-package race runs each pass951 events/two opt-in skips; V1 safety
  repeats85 passes, separate V2 saturation repeats25; final vet passes.
- Two local ABBA CPU/allocation screens retain144 raw samples. V1 repeated
  cache lookup regressed cached reads; V2 removes it and cached-new ranges
  overlap baseline at240B/one allocation. V2 cold128MiB-cache no-network median
  time still rises10.8%/40.8% atGOMAXPROCS1/8, allocation rises27.2%/27.3%.
  Disabled-cache CPU/allocation falls. No current128MiB or high-density net
  latency win is established; preserve every regression and model limitation.
- Keep candidate diagnostic, not a universal2s fix. Actual history-bearing
  XFS/remote encrypted transport and regional claim+literal node-v, cold/
  cached-node NEW identities, populated roots and occupied physical width
  remain required. The fresh Coding gap is not solved by this history-only
  mechanism. No local e2e, privileged mount, production change, merge or tag.
  Evidence: `/tmp/sandbox0-demand-fragments.Cz3DHh/evidence.json`.
  Details: [DEMAND-FRAGMENTS.md](experiments/DEMAND-FRAGMENTS.md).

### D-HISTORY-LIVE — real history fixture and new-sandbox qualification (2026-09-13)

- The previous question-answer turn clarified demand-only scheduling but added
  no acceptance evidence. Revalidate the live installation/SSH handles, finish
  the isolated Nomad baseline installation, and submit eight generic carriers.
  No demand-fragment candidate is deployed and all1362 product files remain
  unchanged. Preserve the10s timeout and claim-time tenant RootFS selection.
- One seed contains64MiB actual random data. Sixteen separate same-byte4KiB
  writes to alternating blocks of Node's first128KiB each preserve full Node
  and data-file SHA256 before/after. Seventeen pauses and16 resumes commit;
  writer epochs0..17 are all s3_materialized. Three public template captures
  at populated baseline/first/final rewrite become ready and remain retained.
- Authenticated, mapping-only full-tree inspection finds pages6/10/12/42 and
  partial views0/25/31/51 for original/populated/rewrite1/rewrite16. The final
  root contains a128KiB interval referring to18 immutable data units. This
  confirms real fragmentation, not its node-v critical-path cost. Full-tree
  page counts are not startup GET counts; no data unit is fetched by inventory.
- Three NEW sandbox identities from those captures execute literal node-v,
  verify full file contents, and delete cleanly. On the preparation-warmed,
  otherwise unoccupied node, combined durations are1.426028/1.101047/1.452942s.
  Keep claim, authenticated readiness and command-only values separate.
  Retain initial preparation seed's2.111587s combined miss. None is a formal
  cold-node or occupied-width sample, and no baseline/candidate claim A/B ran.
- Preserve two diagnostic failures: initial arbitrary8GiB disk guard, replaced
  by quantified3.747GB preparation budget including2GiB free-space floor; and
  mapping-tool launch without the live services' AWS profile. Identical mapping
  binary/input passes with verified service profile, without timeout changes.
  No cache/data deletion. Local helper race passes15 events/zero skips.
- Original binaries, four configs, exact two-carrier job definition and two
  ready allocations restored;64NBD detached. Three disposable probe identities
  are deleted; paused seed, three templates, isolated authority and encrypted
  objects retained. Export215 nonsecret JSON receipts with verified checksums.
  Fresh final cloud observation confirms2CPU/8GiB,Stopped/StopCharging.
- Still required: real encrypted-transport candidate qualification and fresh
  boot/cached-NEW identity regional claim plus literal node-v comparisons,
  occupied physical width, broad populated-root coverage and original1s/2s
  gates. The fresh Coding gap is not solved and no default adoption is made.
  Evidence: /tmp/sandbox0-history-live.GJreyW/evidence.json.
  Details: [HISTORY-LIVE.md](experiments/HISTORY-LIVE.md).

### D-HISTORY-DEMAND — real encrypted component gain, not startup acceptance (2026-09-13)

- Read-only ABBA of the isolated demand-fragment candidate against retained
  real history descriptors on 2CPU/8GiB. No runtime candidate installation,
  claim, guest command, object/DB mutation, cache deletion, or timeout change.
  This schedules an existing ReadAt demand; fixed test offsets are not a
  runtime startup profile, future-command prediction, or tenant prewarm.
- 288 reads: 96 fresh Reader/header-cache, 96 cached NEW Reader, 96 mixed
  simultaneous Readers. All content hashes match; all cached NEW Readers
  make zero additional source/HTTP requests. Local harness race15 pass/0skip.
- Empty-cache 18-fragment read median321.370->216.362ms, open+read
  341.888->235.884ms. Both paths keep20sourcecalls (2mapping+18data),36HTTP
  attempts and228436encodedsourcebytes; peak bodies1->8. Parallel wait is
  improved, not request amplification. Prior42-page whole-tree inventory
  is not current-demand mapping I/O. Metadata8/6 medians155.831/116.159ms
  become74.333/64.441ms; controls do not show a uniform improvement.
- Mixed-eight median286.835->231.949ms is shared-content component evidence,
  not occupied-node or unrelated-tenant throughput. Whole-test userCPU rises
  about2.04->2.45–2.50s and maxRSS32.7–34.9MiB->39.7MiB.
  Preserve CPU/memory costs and prior saturated-source no-gain counterexample.
- Initial preflight records original pool1/2 ready after reboot; job75270
  unchanged and no queued placement. No speculative fix or job resubmission.
  Read-only component guard preserves this anomaly (before/after1), not a
  healthy-pool or startup-gate success. All original recorded processes and
  configs, fixture2076rows, caches/data preserved; test unit/executables gone.
- Export21 checksummed receipts; original1362productfiles unchanged. Fresh
  final cloud check confirms2CPU/8GiB,Stopped/StopCharging. No production,
  merge/tag, local e2e, runtime adoption, or1s/2s assertion.
- Next: separately resolve pool readiness and run isolated real regional
  claim+literal node-v A/B with cold/cached-NEW identities and occupied width.
  Evidence: `/tmp/sandbox0-history-demand.h9Ota4/evidence.json`.
  Details: [HISTORY-DEMAND.md](experiments/HISTORY-DEMAND.md).

### D-DEMAND-LIVE — repeated partial signal, cold combined gate still fails (2026-09-13)

- Four fresh-boot ABBA arms on otherwise unoccupied 16CPU/64GiB: eight
  synchronized NEW identities, then eight cached-node NEW identities per arm.
  Total64 measured claim-plus-literal-node-v samples; two independent boots
  per implementation. No tenant RootFS prewarm, import replay or timeout change.
  Same-root lanes share loads; this is not occupied production acceptance.
- The isolated candidate parallelizes fragments of an already-arrived ReadAt.
  It does not predict commands; unchanged4MiB kernel readahead, coalescing and
  complete compression/authentication units still permit read amplification.
- Cold History16 claim median0.911->0.845s with nonoverlapping sample ranges.
  Coding combined median2.778->2.464s (about314ms), repeated across both boots.
  OriginalNode/History combined has no consistent per-boot improvement; do not
  turn the pooled13ms difference into adoption evidence.
- All64 functional checks pass. Readiness max1.681618s, claim max1.696987s;
  eight coldCoding samples miss1s. Cold combined2.008237–2.884520s:32/32 miss2s.
  Cached-node NEW combined1.439570–1.819655s:0/32 miss2s, not cold acceptance.
- NBD cold-window reads stay about765MB in each arm; not OSS bytes or GET
  counts. Host available memory>58.7GiB, no resident-pressure qualification.
  Resource windows and clocks checked; command-internal attribution remains
  absent and first-arm cold nested node timings are unavailable.
- Post-delete writer-grant lookup returned zero rows, retained as unavailable.
  Two separate post-ABBA baseline-runtime probes verify exact History00/16
  consumed heads and full Node/user-file hashes; excluded from latency stats.
  Original pool1/2ready also reproduced on16CPU; cause remains unresolved.
- Overlay race1237pass events/18explicit skips. All1362product files unchanged.
  Restore original binaries/configs/exact2carrier job and2ready allocations;
  delete64measured+2probe identities, detach64NBD; preserve seed/templates/data.
  Export132arm+48final receipts, seal173local artifacts; fresh cloud check
  confirms2CPU/8GiB,Stopped/StopCharging. No default adoption or production work.
- Next quantify current candidate coverage and remaining actual command-path
  waits, not another closed parameter sweep. Occupied width, populated large
  roots and original1s/2s requirements remain open.
  Evidence: `/tmp/sandbox0-demand-live.Q2NOx7/evidence.json`.
  Details: [DEMAND-LIVE.md](experiments/DEMAND-LIVE.md).

### D-DEMAND-COVERAGE — ordinary roots bypass fragment scheduling (2026-09-13)

- Exact Node/Coding ordinary immutable profiles contain4341/80781full views,
  zero partial views. The candidate's sole Reader dispatch requires a partial
  view; its helper rejects full views before cache/source access.
- Reverify173D-DEMAND-LIVE artifacts and bind profiles by exact descriptor and
  artifact identity. Reuse14373 historical reads from16identities/32phase
  groups with exact leases and literal node-v; zero partial-view intersections.
  This is not new live invocation counters or current runtime timing.
- Correct D-DEMAND-LIVE attribution: the observed Coding314ms combined gain
  remains, but is not proof of direct fragment parallelism on Coding's root.
  Shared history-reader/cache/source effects and cross-boot/provider variation
  are unisolated alternatives. Preserve the history-claim/component signal.
- Actual-candidate full-view guard/AST tests and positive history/demand-scope
  tests pass30events over3race repeats,0skips. Product1362files unchanged;
  no cloud/remote action, new claim/command, local e2e or default adoption.
- Require per-Reader batch/source attribution and exact generation identity
  before another mixed follow-up. Separately target ordinary full-view waits;
  do not repeat closed parameter sweeps or infer a universal1s/2s bound.
  Evidence: `/tmp/sandbox0-demand-coverage.shQ9Tb/evidence.json`.
  Details: [DEMAND-COVERAGE.md](experiments/DEMAND-COVERAGE.md).

### D-READER-ATTRIBUTION — diagnostic counters qualified, remote calibration pending (2026-09-13)

- Temporary overlay preserves demand scheduling and source/cache limits. It
  records Reader-lifetime work with exact branch identity, not request-stage
  critical-path durations. No extra source reads or tenant prewarm.
- Eight-Reader shared-flight test charges one actual source call to its leader
  and zero to seven followers. Ordinary/history coverage, cancellation, errors,
  cleanup, exclusive private reports and binding checks pass45race test events.
  Full overlay race1252pass events/18explicit skips; vet and static amd64 builds
  pass. No product-default change or new startup sample.
- Clean sequential native ARM64 cached4KiB screen: enabled counters add about
  0.31–0.37microseconds/read (66–84percent on this short path), with0allocations.
  Initial screen possibly overlapping vet is retained but excluded. Disabled
  ranges overlap the control; neither equivalence nor remote cost is proven.
- Diagnostic only. Require complete generation-bound reports and matching
  observer-free remote control; do not use instrumented latency as adoption
  evidence. All populated-root, occupied-width and regional1s/2s gates remain.
- Evidence: `/tmp/sandbox0-reader-attribution.W5e5Zc/evidence.json`, SHA256
  `e23bafebf2b9adbd74ba1cf31bfb82ebd431f5528f3e7ac194c34c6e9c420aa0`.
  Details: [READER-ATTRIBUTION.md](experiments/READER-ATTRIBUTION.md).

### D-READER-LIVE — admitted diagnostic control (2026-09-13)

- Compare uninstrumented V2 with observed V2, not another optimization ABBA.
  Two fresh boots,32claim-plus-literal-node-v samples over cold/cached-NEW
  cohorts and four ordinary/history roots. Exact bindings captured before
  delete; complete Reader reports required. No tenant prewarm or timeout change.
- Related new evidence: D-DEMAND-COVERAGE and D-READER-ATTRIBUTION. Determine
  actual fragment/source ownership, not infer it from mixed aggregate latency.
- A single boot per mode cannot isolate provider variance or prove diagnostic
  equivalence. Preserve all SLO misses; occupied width, large populated roots
  and production acceptance remain required, not replaced by this experiment.
- Frozen inputs, hypothesis, decision rules and rollback:
  [READER-LIVE.md](experiments/READER-LIVE.md).

Result (2026-09-13; Reader attribution complete, external queue sampler failed):

- Two fresh boots,32 NEW identities and32 literal node-v functional passes;
  cold/cached-NEW cohorts kept separate. All32 consumed writer/generation/head
  bindings captured before deletion. Exactly16 observed Reader reports bind
  correctly, with no warning/conflict/read/source/fragment error or live work.
- Ordinary Node/Coding: zero fragment helper entries/batches. History00 has
  12entries but zero batches. Only History16 dispatches: two batches/262144
  requested bytes per Reader. Each eight-Reader cohort covers524288bytes out
  of about766million Reader bytes. Byte coverage is not latency contribution.
  Do not promote history-fragment scheduling as a general ordinary-root fix.
- Cold/cached-NEW observed actual source calls530/508, requested bytes
  55906303/59631316; mapping-flight calls564/2. Coalesced leaders/followers
  127/3609 and173/3565. Lifetime sums include construction/cleanup, overlap,
  and are not HTTP GETs or critical-path timings. Source work remaining after
  metadata caching is consistent with older D-CRITICAL-BUDGET, not grounds
  for another unchanged shared-wait/cache/width/readahead trial.
- Public readiness max1.631578s, claim max1.650312s; four cold Coding samples
  miss1s. Cold combined2.127722–2.691780s:16/16miss2s. Cached-NEW combined
  1.532298–1.607659s:0/16miss2s. Single boot per mode, nonuniform timing changes;
  no observer-equivalence, fixed correction, algorithm-gain or hard-bound claim.
- Observed-arm queue sampler encountered ENODEV reading/sys/block/nbd0/pid;
  outer unit exited1 despite workload exit0. Complete Reader reports and
  independent final physical absence do not repair incomplete queue coverage.
  Retain the failed arm; no runtime/claim replay. Local analysis v1 zero-counter
  and full-arm-label mistakes retained, superseded by explicit unavailable
  control counters and separate functional/observer status in version2.
- Restore original binaries/configs/exact2carrier job; verify2ready and64NBD
  detached; delete only32measured identities. Preserve paused seed at17,
  three templates, original263/source2072/indexed2136 rows, caches and/data.
  Owned fixture now2174rows. Fresh cloud receipt11:17:32UTC confirms2CPU/8GiB,
  Stopped/StopCharging. All1362productfiles unchanged; no default adoption.
- Seal126local artifacts and checksummed remote exports. Evidence:
  `/tmp/sandbox0-reader-live.Q4hLga/evidence.json`, SHA256
  `ede4e1934174b071d1f0d88c692bdea5cab7b494135551c93452b8fca3b596dd`.
  Original populated-root, occupied-width and regional1s/2s gates remain open.

### D-DIRECT-XFS-CONTRACT — Direct block branch without OverlayFS (2026-09-13)

- Previous explanation turn made no performance progress. New structural
  hypothesis: expose the already isolated XFS branch directly, avoiding the
  second mount and upper/lower lookup/copy-up work. Not another EROFS, cache,
  worker or prefetch sweep. Existing populated uppers cannot be bypassed.
- Actual native encrypted-format2 Reader/Branch/NBD/XFS fixture passes writes,
  xattrs, hardlinks, deletion, sparse hole/truncation, unchanged generic rebase,
  incremental publication and reconstruction under a new identity/empty WAL.
  Six changes need8203source bytes, independently matched to actual Apply I/O;
  48MiB sparse logical size is not copied wholesale. Independent target changes
  and old-base data survive. No product changes or second state authority.
- This uses synthetic encrypted MemoryStore, not real OSS or stock runsc/procd.
  Zero claims/commands/startup samples;0.56s test duration is NOT readiness.
  Local guard race x3, three package unit suites, vet and native tests pass.
  Preserve full cold/cached-NEW regional claim+node-v, population/history and
  occupied-width gates; no latency saving, layout/default or lifecycle adoption.
- Next gate: same real artifacts, projection-only net read/latency comparison.
  Production needs authenticated layout/version/migration and full terminal,
  rebase and security contracts; do not reinterpret old Overlay generations.
- All1362productfiles unchanged. Original files/services/configs/job77379/DBs
  preserved; native test unit success, all64NBD and owned mounts/loops absent.
  Final original ready count1 is recorded without a two-carrier health claim.
  All fixtures/caches retained;2CPU8GiB Stopped/StopCharging at11:48:23UTC.
  Details: [DIRECT-XFS-CONTRACT.md](experiments/DIRECT-XFS-CONTRACT.md).
  Evidence: `/tmp/sandbox0-direct-xfs.DKqRyi/evidence.json`, SHA256
  `1b3cb1bc4461a2986567cd26621b15d49226f75913bc3ece85fc4668378a3a90`.

### D-DIRECT-XFS-NET — direct projection fails the performance screen (2026-09-13)

- Same immutable Node/Coding encrypted OSS roots, O1/D1/D2/O2, fresh empty
  Reader/header caches then cached NEW identity. Sixteen real node-v commands
  and authenticated procd proofs pass; no tenant prewarm, object publication,
  timeout increase or POST replay. Single2CPU8GiB boot/one guest, not regional
  claim, current guest parity, populated1TiB or occupied-width acceptance.
- Both direct cold samples fail combined-time/read-cost improvement versus
  reverse O2 for both images. Node D1/D2 combined +759.744/+30.997ms; Coding
  +1483.704/+69.680ms. NBD bytes barely change; ciphertext does not decrease.
  Cached direct Coding adds10/11GET attempts where ordinary has zero. All8
  direct rows are slower than matched O2. No layout/migration/default adoption.
- Ordinary controls themselves improve: Node cold combined2259.471→1301.380ms,
  Coding3734.239→2157.949ms. First-command read volumes remain almost identical
  while client object-body median duration falls18–20ms→8–9ms. Machine/network/
  object-storage variation remains unresolved; do not credit chronological gains
  to code or declare S3 exonerated. Overlapping I/O sums are not latency.
- Preserve all misses: cold ready4/8>1s, command6/8>1s, combined8/8>1s and6/8>2s;
  cached NEW0/8>1s at this narrower local boundary. No full-goal gate passes.
- Initial runner and explicit v2 resume fail cleanup after the two O1 pairs.
  Exact evidence identifies persistent stock-runsc null-netns nsfs mounts, not
  RootFS/NBD. Runner-only guarded normal unmount fixes v3; resume executes only
  the remaining six pairs with unchanged binary. Both failures remain failed.
  Retain prelaunch bytea parse, local unused-import and offline verifier-format
  errors as well. No runtime samples replayed or failed attempts erased.
- Independent verification checks101 exports,16 raw timing/command rows and
  identities,12 exact namespace unmounts,75 failure observations,21race PASS
  events/no skips, nine analysis checks, and unchanged1362product files/path set.
  Original files/services/config/job77379/DBs and/data preserved; all64NBD and
  owned mounts/network/cgroups absent. Final original ready count1 is explicit.
  Fresh2CPU8GiB Stopped/StopCharging receipt12:20:38UTC; no production changes.
- Candidate closed; do not repeat unchanged projection or implement migration
  without new evidence. Original regional/first-command/population/history and
  occupied-width1s/2s requirements remain. Details:
  [DIRECT-XFS-NET.md](experiments/DIRECT-XFS-NET.md).
  Seal187 artifacts, `/tmp/sandbox0-direct-xfs-net.wMe4BC/evidence.json`, SHA256
  `84a2dc9a04ba045655e7b44d8c16f8e12156279a5929d0f5c9449aae962705a7`.

### User convergence decision — stop new performance exploration (2026-09-13)

- User asks to converge and accepts the current approximately2s-level results,
  including somewhat above2s. This supersedes the open-ended search to force
  every observed sample below2s. Preserve all old misses and scope limitations;
  no universal size/load/startup guarantee is inferred.
- Freeze implementation and turn to necessary regression, diff review and one
  scoped PR handoff. No new remote tuning, prewarm, timeout increase, default
  switch, merge or production rollout. Rejected temporary candidates stay out.
- [CONVERGENCE.md](experiments/CONVERGENCE.md) is the current closeout authority.
  Automatic goal continuations must follow that finite closeout, not reopen a
  performance candidate just because the earlier hard threshold remains unmet.
- Closeout race regression:20/21core packages pass; original suite retains four
  XFS fixture FAIL events,2208PASS/166skip. Local/tmp reports directorydevice30
  and regular-filedevice31, so the unchanged cross-device guard rejects before
  intended assertions. Same source on an isolated same-device tmpfs fixture
  passes the affected package three times:186PASS/0FAIL/6privileged skips. No
  source/guard changes or new performance trial. Driver113PASS/0FAIL/2skips.
  Evidence `/tmp/sandbox0-cold-convergence.AVqslf`; original failure is not erased.
