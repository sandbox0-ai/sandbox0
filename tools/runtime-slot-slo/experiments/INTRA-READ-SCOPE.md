# D-INTRA-READ-SCOPE: bound intra-read opportunity before adding concurrency

2026-09-13. Evidence: `/tmp/sandbox0-intra-read.cgjqKe/evidence.json`.
No product source/default change, remote action or new startup sample.

## Result

The current Reader processes payload ranges sequentially within one ReadAt.
In the retained real claim-plus-literal-node-v trace, this has materially
different exposure across the two images. Mapping flights are excluded from
the payload counts, and source flights remain distinct from provider GETs.

| Cold image / phase | NBD reads with multiple payload calls per sample | Union of their enclosing read intervals |
| --- | ---: | ---: |
| Node / claim | 14–15 | 203.260–204.899ms |
| Node / first command | 70 | 703.325–703.837ms |
| Coding / claim | 2 | 46.064–46.356ms |
| Coding / first command | 5–6 | 99.294–99.511ms |

These are interval exposures, not predicted savings, causal lower bounds or
new latency measurements. Even the enclosing intervals cover only a small
part of Coding's previously measured cold gap. Do not introduce workers and
claim the general cold-start problem solved from Node's larger exposure.

All78 adjacent payload-call pairs in each cold Node first-command sample
refer to physically adjacent source spans in the same object. The source
flight graph resolves every examined call to one leader. Shared callers can
refer to the same source attempt; these counts are not unique HTTP requests.

Exact original artifact profiles further bind62 singleton payload calls per
cold Node command sample to full mapping units crossing an absolute1MiB window
boundary. Coding has only1–3 such command calls. All tested multi-payload cold
NBD reads contain one Reader ReadAt scope; this is not branch dirty-span
subdivision being mislabeled as intra-read serialization.

## Unchanged-source mechanism reproduction

A test-only Go overlay constructs16 adjacent64KiB ranges in raw and mixed
raw/zstd representations. Read the same final128KiB with a128MiB cache; change
only the logical placement from aligned to a4KiB shift. The actual unchanged
Reader then makes1 versus2 source reads, respectively. Both read exactly the
same total stored bytes and return identical verified output.

The shifted plan groups15 units and excludes the crossing16th unit. A fresh
Reader over the same immutable generation and shared cache adds zero source
reads. This is a local cache-consumer check, NOT a new sandbox identity, remote
cached-node claim or end-to-end acceptance. No local e2e runs.

Four mechanism cells pass three race repetitions:12 complete result records,
15 passing test events including parent tests, no failures or skips. The
interval/mapping-scope helper passes5tests/107assertions, including100 interval
union comparisons against an independent discrete oracle.

## Candidate boundary, not adoption

Qualify a canonical grouping-boundary adjustment before adding intra-read
workers. A candidate must not split an authenticated unit merely because its
logical extent straddles an absolute window, while keeping stored/decoded
group size bounded at1MiB. It must preserve exact-small-read behavior,
zero/tiny-cache behavior, independent checksum verification, partial views,
holes, physical/leaf boundaries, cancellation and existing8-source admission.

Including the crossing unit can also read additional bytes for an earlier
demand that never reaches that unit. Therefore the mechanism test is not
sufficient for adoption: quantify complete-workload bytes, overlapping mixed-
root sharing, CPU/allocation and cache churn before real encrypted I/O and
regional claim-plus-command qualification. Do not repeat the rejected short-
demand merge or independent-worker/suffix candidates unchanged. The previous
SHARED-WINDOWS experiment concerned cross-image unequal memberships; this turn
adds actual same-request boundary attribution, not a second claim of that
already-known synthetic sharing defect.

Coding's dominant remaining dependency coverage is still unresolved. A Node
optimization does not replace populated/history-bearing roots, occupied real
width, current production-pin parity or the original1s/2s end-to-end gates.

## Provenance and retained corrections

- Bind all14373 reads/32 phase groups to their exact16 historical sandbox
  identities, leases, artifacts, clocks and hash-verified literal node-v case
  definition. Twelve selected trace/case/content inputs plus four raw traces
  verify. The profiles are the exact ordinary format2 artifacts, not an inline
  or retired-format reconstruction. Historical instrumented runsc20260810 and
  unoccupied width are not current production acceptance.
- The first delivery analyzer has a missing `end` and exits1 before execution;
  retain the failed source hash and correction. No output or remote replay.
- The initial geometry lookup wrongly compares bare SHA256 hex from the trace
  to prefixed digest strings from the profile. Its local process is explicitly
  stopped during error handling after checking exact argv; terminal output
  reports KeyError/exit1. High-CPU error-suggestion processing was suspected,
  not independently proved. This is not an S3 failure or an observed cross-
  artifact cache error.
- Preserve the intermediate `geometry.json` but mark its locator-difference
  flags invalid. `geometry-normalized.json` is authoritative. Content identities
  match requested logical views, normalized physical locators also match, and
  independent arithmetic checks confirm the window crossings. No observed
  cross-artifact serving is claimed from this intermediate reporting mistake.
- All1361 current product source files and32 prior FILE-ACCESS artifacts are
  unchanged. Only temporary diagnostics and this ledger change. No cloud call,
  runtime mutation, merge, tag or rollout; the preceding Stopped/StopCharging
  receipt remains historical, not freshly re-observed. All original gates stay
  open and the full goal remains active.
