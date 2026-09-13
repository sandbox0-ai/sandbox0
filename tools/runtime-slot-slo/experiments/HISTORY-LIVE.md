# D-HISTORY-LIVE: real checkpoint history, not a prefetch rollout

2026-09-13. Evidence root: /tmp/sandbox0-history-live.GJreyW.

## Scope and decision

The preceding user-question turn clarified the demand trigger but added no
runtime acceptance evidence. This experiment creates and qualifies real
history-bearing artifacts before admitting D-DEMAND-FRAGMENTS to a live test.
The remote runtime remains the earlier diagnostic baseline; the parallel
fragment candidate is not built into or deployed as ctld.

Keep generic single-use carriers, claim-time RootFS binding, encrypted OSS,
PostgreSQL authority, unchanged 10s API requests, and literal first node-v.
No production change, merge, tag, local e2e, cache deletion, or user-rootfs
prewarm. The remote-test skill's obsolete Kind deployment steps are not used.
Its ECS lifecycle and cache-preservation guidance apply; the user's explicit
experiment-ledger request takes precedence over its no-summary-doc guideline.

## Real fixture and preservation

- Isolated authority s0_history_live_gjreyw is cloned from the existing
  s0_mapping_oci_uqo1av authority without replaying OCI imports. The source
  has2072 sandbox rows; original sandbox0 has263 and prior indexed-live2136.
  All remain separate from the new authority.
- Temporarily use the previous16CPU/64GiB shape,14CPU/56GiB admitted capacity,
  eight generic carriers and1750m/1792MiB for each sandbox. This experiment
  executes sequential operations on an otherwise unoccupied worker; it is
  NOT occupied-width or production-ingress acceptance.
- Claim one seed from mixed-xe3dad-node22. First command is exactly
  ["node","-v"], returning v22.23.2. It uses the previously frozen UTC procd,
  not the later missing-owner reset repair.
- Write64MiB of actual random data, not a sparse truncate or repeated zero
  block. The file SHA256 is
  e3c8240776965b8388ed8c1276567ab4e2ddc902657a333cfd1f1771032069da.
  Node remains124836408bytes with SHA256
  3517c2df0b2f8cd7f422b4b8450ef81c6889f08eb03e281d6de9079b15e6a327.
- After the initial populated checkpoint, perform16 separate same-byte4KiB
  writes at alternating blocks1,3,...31 of the Node file's first128KiB.
  Node is not running during each shell/dd write. Verify both full-file
  hashes before and after every write, then pause and resume between writes.
- Observe33 committed lifecycle transactions:17 pauses and16 resumes.
  Eighteen generations, writer epochs0..17, are all s3_materialized.
  Every completed pause additionally proves no active lease or attached NBD.
- Capture history-gjreyw-00, history-gjreyw-01 and history-gjreyw-16 through
  the public template-from-sandbox API. All become ready. They retain the
  populated baseline, first rewrite and final rewrite heads respectively.
  Retain the paused seed, templates, isolated database and encrypted objects.
  The three disposable probe identities are deleted after verification.

The roots are16GiB logical with this controlled64MiB user-data addition.
They are NOT fully populated16GiB/1TiB roots or a broad arbitrary-history test.

## Mapping-only observation

Use the production authenticated Reader and encryption constructor in a
temporary test overlay. Read only exact mapping ranges authorized by the
verified parent tree, validate child bindings, and reject data-range requests.
Bound requests to512, total encoded mapping bytes to64MiB, and each range to
the existing8MiB maximum. These are diagnostic limits, not runtime changes.

| Root | Current-tree pages | Leaf entries | Partial views | Stored mapping bytes |
| --- | ---: | ---: | ---: | ---: |
| Original OCI artifact | 6 | 4341 | 0 | 185581 |
| Populated checkpoint00 | 10 | 5425 | 25 | 227515 |
| Rewrite01 | 12 | 5436 | 31 | 228488 |
| Rewrite16 | 42 | 5486 | 51 | 240740 |

At rewrite16, an aligned128KiB logical interval at offset4416077824 references
18 distinct immutable data units. There are21 aligned128KiB windows containing
partial views in the current tree. This establishes a real history-fragment
case beyond the synthetic fixture. It does not establish that the selected
interval is on the node-v critical path; that still needs actual demand tracing.

Page counts cover a full-tree metadata inventory, NOT pages fetched at startup
or ancestor replay. RangeSource calls are not necessarily HTTP GET counts.
No data payload is fetched by this inventory, and no latency saving is inferred.
The observed tree growth is a hypothesis input for path/occupancy analysis,
not proof that mapping pages cause the end-to-end miss.

## New-sandbox functional probes

After preparation, claim a NEW identity from each captured template, immediately
execute literal node-v, then verify the complete Node and64MiB file hashes.
All three pass and converge through DELETE to physical absence. These use the
private regional TLS endpoint on the preparation-warmed worker, sequentially.
They are neither fresh-node measurements nor a baseline/candidate comparison.

| Template | Trusted readiness | Claim wall | node-v only | Claim + node-v |
| --- | ---: | ---: | ---: | ---: |
| history-gjreyw-00 | 650.084ms | 665.765ms | 760.175ms | 1426.028ms |
| history-gjreyw-01 | 539.852ms | 555.405ms | 545.545ms | 1101.047ms |
| history-gjreyw-16 | 704.033ms | 719.569ms | 733.288ms | 1452.942ms |

Also retain the initial preparation seed: readiness883.324ms, claim902.862ms,
node-v1208.626ms, combined2111.587ms. Do not omit its2s combined miss, turn it
into a formal cold cohort, or compare its cache state to these probes.

## Failures and tool verification

- Initial preflight stopped at an arbitrary8GiB free-space guard. Read-only
  accounting found4.22GB free and a3.747GB bounded preparation budget including
  a2GiB free-space floor. No cache or old artifact was deleted. The experiment
  stayed above the floor; this budget did not authorize unbounded large writes.
- The first mapping inspector failed its first encrypted read after5s. Its
  launch omitted the AWS profile used by the actual manager/ctld processes.
  Verify those running environments and the profile-file hash, then run the
  identical binary/input with that profile. The corrected inventory passes.
  Preserve both receipts; no timeout, storage data or reader behavior changes.
  Do not count this tool-launch failure as observed OSS service latency.
- The API helper reuses the existing claim/context parser and first-command
  checks. Every action has an exclusive pre-POST intent and result receipt.
  Accepted/failed writes and request timeouts are never automatically replayed.
  Three-repeat race tests pass15 test events, no skips. Vet and local builds
  pass. No helper benchmark substitutes for live runtime acceptance.
- All1362 previously inventoried product files remain unchanged. Temporary
  Go/Ruby helpers and experiment records are the only new local work.

## Remaining gate

Final cleanup verification restores the eight original binary/catalog hashes,
four original config hashes and running config paths, exact original job
definition and two ready carrier allocations. All64NBD devices are detached.
Export215 checked nonsecret receipts; do not export configs, credentials,
encryption keys or the private database dump. The final Aliyun observation at
2026-09-13T08:38:02.015549219Z confirms Stopped/StopCharging, ecs.g9i.large,
2CPU/8192MiB. Preserved artifacts remain available for the next cold-boot test.

The parallel fragment candidate still requires real encrypted-transport and
regional claim/first-command comparison on these immutable captured roots.
Use a verified cold boot before the timed cold cohort, then NEW sandbox IDs
for the cached cohort; preparation reads above must not establish that cache.
Preserve unoccupied versus occupied-width labels, broad populated-root/history
coverage, machine/storage attribution, CPU/allocation costs, and every1s/2s
miss. The earlier fresh Coding gap and the universal high-density2s target
remain unresolved. No product-default adoption or goal completion is claimed.
