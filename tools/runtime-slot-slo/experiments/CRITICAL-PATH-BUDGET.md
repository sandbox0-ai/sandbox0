# D-CRITICAL-BUDGET: end-to-end opportunity budget

2026-09-12. Diagnostic reanalysis, not a new startup benchmark or acceptance.

## Decision

The next structural candidate must reduce block-payload dependency cost as well
as preserve ordinary-layout bulk efficiency. A root-page, mapping-index-only,
RootFS-ensure-only, or decoder-only change is not a sufficient standalone plan
for the observed Coding cold combined gap. Do not restart the rejected inline
remote experiment or an admission/cache/readahead parameter sweep.

This is a prioritization result, not proof that a particular replacement format
works. All requirements in [STRUCTURED-OPTIMIZATION.md](STRUCTURED-OPTIMIZATION.md)
remain intact. The full goal remains open.

## Evidence boundary

- Reused all16 exact-identity samples from sealed `D-MAPPING-TRACE`: eight cold
  and eight cached-node NEW sandboxes, four of each image in each cohort. Both
  workloads immediately execute `node -v`; these are real runtime observations.
- Historical fixture: private regional TLS,16CPU/64GiB, eight synchronized
  claims,128MiB cache, stock runsc20260810. It did not exercise occupied-memory
  acceptance or a populated1TiB root. Its narrow fixture and instrumentation
  prevent production acceptance. No new remote operation occurred this turn.
- Freshly fetched authorities: sandbox0 origin/main0f092204 and infra
  origin/mainceb895c. The actual measured binaries remain explicitly hashed
  diagnostic candidates, not an unqualified build of current main. Older local
  main branches were not substituted for origin/main.
- Reverified1353 product files and inventory,164 trace artifacts and the exact
  artifact-bound995Node/23344Coding metadata-unit inventory. No product edits.
- Independently reproduced all32 previous per-sandbox/phase wait unions exactly.
  The new classification adds no omitted GC-prefixed wait time in these windows.
  Five analysis-helper tests/323 assertions pass; not a product test suite.

## Correlated stage budget

The following values all belong to the SAME slowest-combined cold Coding sample,
not maxima assembled from different samples. Stage logs record durations, not
individual start/end timestamps. No absolute stage timeline is invented.

| Scope | Observed ms |
| --- | ---: |
| Regional-observed claim total | 1661.703 |
| Node claim, contained in claim | 1064.635 |
| RootFS ensure, contained in node claim | 605.004 |
| runsc create, contained in node claim | 227.705 |
| runsc start, contained in node claim | 220.152 |
| Other node/channel work, contained in node claim | 11.774 |
| Authenticated procd probe, separate from node claim | 518.976 |
| Other claim work, outside node claim/probe | 78.092 |
| Client-observed complete claim round trip | 1675.950 |
| First real command only | 989.994 |
| Client claim-to-command completion | 2665.969 |

The 2s combined gap is665.827-665.969ms over all four Coding cold samples.
If claim stayed unchanged, the first command would need to fit approximately
324-342ms, compared with the observed990-1008ms. Optimizing only the command
would still leave the preferred1s claim target unmet.

## Elimination screen: is the observed cost large enough?

For EACH matched sample, subtract only the indicated observed duration/exposure
and hold everything else fixed. This intentionally optimistic arithmetic is NOT
a speedup prediction, a true architectural lower bound, or permission to remove
authentication, mounts, checksums or readiness checks. Future changes can alter
downstream dependencies; those effects require their own model and measurement.

| Hypothetical zero-cost scope | Coding combined remaining, ms |
| --- | ---: |
| Root mapping constructor call | 2601.707-2601.849 |
| Entire RootFS ensure | 2021.815-2060.965 |
| runsc create and start | 2215.699-2219.254 |
| procd probe | 2146.010-2167.707 |
| All observed mapping-index calls, including root | 2010.991-2039.636 |

The last row includes same-sandbox unions of complete NBD mapping lookup regions
(CPU plus waits), and the image's largest verified root-constructor call. It is
not merely the64ms root page. Each row independently still misses2s in all four
Coding samples. Rows overlap and MUST NOT be added as independent savings.

## Where read dependency exposure remains

Per-sandbox interval unions avoid summing simultaneous NBD requests. They still
measure exposure, not the true guest/kernel critical path or guaranteed removable
time. Mapping-index and block-payload intervals can overlap across requests;
`analysis.json` retains their disjoint mapping-only/payload-only/overlap partition.

| Cold phase | Mapping-index wait, ms | Block-payload wait, ms |
| --- | ---: | ---: |
| Coding claim | 418.523-450.988 | 952.161-966.218 |
| Coding first command | 124.476-141.207 | 802.032-804.293 |
| Node claim | 25.011-28.915 | 560.411-567.290 |
| Node first command | 42.746-43.314 | 1034.931-1053.387 |

Block payload is not synonymous with regular-file bytes: it contains XFS
metadata too. Classifying requests against the complete retained metadata-unit
inventory gives these command payload-wait exposures:

- Coding:472.085-473.341ms in wholly inventoried-metadata requests;
  329.437-331.191ms in requests touching no inventoried metadata.
- Node:60.860-76.986ms in wholly inventoried-metadata requests;
  974.071-976.494ms in requests touching no inventoried metadata.
- Mixed requests and no-inventory requests retain their conservative labels.
  Unknown blocks are not declared regular files; requested bytes include kernel
  read-ahead and cannot all be declared required startup bytes.

One exact cold Coding command read demonstrates sequential mapping and payload
wait:4KiB at logical offset824893607936,29.702ms mapping then39.362ms payload,
69.249ms whole NBD request. The ordering is proven inside this read only; temporal
order between different reads is not treated as a causal dependency.

Shared-flight evidence remains material:3920 cold NBD waiter edges,3235 crossing
sandbox identity;1418 cached-new edges,1220 crossing identity. They are dependency
edges, not GET counts. A leader's physical cost must not be multiplied by its
followers. Increasing independent source admission does not remove waits for the
same shared operation. Prior single-outstanding analysis `D-DEMAND-CHAIN` further
supports small serial demand as the Coding issue; it is reused, not rediscovered.

Cached-new mapping/header wait is zero in all16 phase windows, while Coding
block-payload waits remain510-528ms in claim and498-520ms in the command. Header
and mapping caching are already effective in this fixture; their success does
not eliminate payload cache misses, eviction or first-node cold dependency cost.

## Ranked work and required next evidence

1. Highest priority: general block delivery/representation that shortens the
   filesystem-metadata small-read path WITHOUT fragmenting the ordinary mixed
   read path. This can affect claim and first command; the observed budget is
   materially larger than root-page or caller-assembly work. A candidate must
   predict reduced dependent delivery, not just fewer metadata-only GETs.
2. Preserve and account for ordinary data efficiency. Node's large non-inventory
   payload exposure rules out accepting a Coding-metadata-only win. Model source
   frames, encrypted bytes, repeated decode and node-wide memory together.
3. Treat root-page/index and decoder improvements as possible components, not
   standalone gate solutions. Existing inline/projection variants remain rejected
   for runtime admission until the representation's total cost changes.
4. Do not prioritize a wider source semaphore here: observed Coding source/NBD
   admission wait is at most12.642ms in claim and1.185ms in command, far below the
   666ms gap. This does not exclude contention under the still-missing occupied
   production-width workload, which must be checked separately.

Next deliverable: compare a candidate general layout/delivery model against the
ordinary baseline for BOTH filesystem-metadata and mixed requests, with an
explicit achievable-savings argument, immutable/COW ownership and memory bounds.
Reuse `D-DIRECTORY-GRAPH`, `D-DEMAND-CHAIN`, prior repacking/assist failures and
the frozen full mapping inventory; do not rediscover the graph or repeat a
rejected layout unchanged. If the model cannot reduce block-payload dependency
without moving equivalent cost into mapping/CPU/transfer, reject it before code
or remote deployment.

Still unknown: true kernel/guest fault-versus-read-ahead dependency edges,
sub-stage RootFS ensure timing for these exact retained samples, and runsc/procd
CPU-versus-blocking attribution. The source has timing hooks, but the retained
export does not contain all of these spans. Missing evidence must remain unknown
rather than inventing a precise removable-time or size-independent guarantee.

Raw evidence: `/tmp/sandbox0-critical-budget.4tfaKf/analysis.json` and `budget.json`.
