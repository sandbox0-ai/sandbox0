# D-HISTORY-READ: separate generation history from demand fragmentation

2026-09-13. Evidence root: `/tmp/sandbox0-history-read.dhNC4T`.

## Decision

Keep a regression test for current-root opening and history-bearing demand
reads. Do not implement ancestry replay avoidance: the tested path already
opens one current mapping root, and does not fetch every prior generation.
History-induced fragmentation is a different, observed cost. It increases
independent data-source reads even with the existing 128 MiB cache, initially
empty. Fewer downloaded bytes must not be reported as fewer serialized waits.

This is mechanism evidence, not a new cold-start result or a general solution
for Coding. Do not adopt indexed grouping, increase cache/source concurrency
limits, extend the 10s timeout or warm tenant RootFS from this result.

## Fixture and boundaries

- `pkg/rootfsblock/history_read_test.go` creates an actually backed 64 MiB
  block image. Each 64 KiB range has distinct deterministic random content,
  with a random 16 KiB quarter repeated four times to exercise compression.
  It is not zero-filled/sparse, but it is also NOT a populated XFS RootFS,
  a large remote upload, an encrypted S3 transport or a real sandbox.
- Three separate histories each publish 256 generations: overwrite the same
  block, overwrite alternating blocks of the demanded 128 KiB prefix, or
  overwrite blocks outside that prefix. Alternate direct incremental and
  composite-plus-batch materialized publication using the production builders.
- Check generations 0, 1, 16, 64 and 256. The first write splits the original
  leaf root into a multilevel mapping tree. Each checkpoint uses fresh zero-
  and 128 MiB-cache readers, followed by another Reader identity on the same
  cache. These are Reader identities, NOT cold-node/cached-node sandbox claims.
- All 60 demand results match expected bytes. Separately verify the SHA-256
  of the complete 64 MiB current image at all 15 checkpoints, including edits
  outside the measured prefix. Whole-image validation cannot warm any later
  checkpoint's new cache.
- Opening with an empty/disabled cache fetches exactly one current root and
  zero data objects. With the 128 MiB cache, every second Reader opens and reads
  its prefix without another source request. No per-generation ancestor chain
  is passed to the Reader.
- Counts/bytes below are RangeSource requests and pre-envelope stored bytes,
  not HTTP/OSS GET counts or wire bytes. Mapping demand counts exclude the
  separately counted root-opening request. No wall time is an SLO measurement.

## Measured first-Reader demand costs

All rows request the same 131,072-byte logical prefix. The original-generation
row applies identically to all three histories.

| Generation / write pattern | Cache capacity | Mapping reads after open | Data reads | Data source bytes |
| --- | ---: | ---: | ---: | ---: |
| 0 / original | 128 MiB | 0 | 1 | 262,592 |
| 256 / same block | 128 MiB | 1 | 3 | 266,688 |
| 256 / alternating demanded blocks | 128 MiB | 1 | 18 | 98,360 |
| 256 / outside demand | 128 MiB | 1 | 1 | 32,824 |
| 0 / original | Disabled | 0 | 1 | 32,824 |
| 256 / same block | Disabled | 4 | 4 | 53,332 |
| 256 / alternating demanded blocks | Disabled | 32 | 32 | 328,128 |
| 256 / outside demand | Disabled | 1 | 1 | 32,824 |

The fragmented demand reaches 18 data requests at generation 16 and stays at
18 at 64 and 256 with the current cache. Sixteen final edited blocks and two
unchanged compressed ranges remain needed; older overwritten values do not.
The unrelated-write history retains one data request. A write immediately
outside the prefix also stops the old speculative neighboring window, which
explains its lower byte count; it is not proof that unrelated history speeds
up startup. Disabled-cache repeated mapping/view reads are a separate pressure
case, not the configuration used by the previous live benchmark.

## Qualification and changes

- The preliminary 16 MiB fixture passed but did not split the root or verify
  the whole current image. Its receipt is retained as `baseline.json`; it is
  superseded by the 64 MiB multilevel/full-content qualification, not presented
  as equivalent coverage. The unmeasured outside-prefix index was bounded to
  fixture capacity during construction.
- Final targeted race run passes all 34 parent/subtest pass events in 101.683s.
  The complete `rootfsblock` package passes 934 test events. The fully mapped
  1 TiB opt-in model and external RustFS integration test skip; they are not
  acceptance passes. No local e2e or privileged mount is executed.
- All 1361 previously inventoried product files remain byte-identical. The
  only product-tree addition is the new test. No runtime behavior or API/doc
  contract changes. No remote/cloud call, production change, merge or tag.
- Prior live evidence SHA-256 and the source inventory were reverified before
  work. This run does not freshly verify remote state; the last retained cloud
  receipt remains the previous run's 2 CPU/8 GiB Stopped/StopCharging result.

## Next experiment boundary

Evaluate demand-only independent range scheduling for fragmented current
generations, after resolving the authenticated mapping of an actual request.
It must reuse the existing node-wide eight-source admission and preserve
fairness, bounded retained buffers, checksum/partial-error behavior, composite
tail precedence and cancellation. Do not simply spawn more readers or increase
the source limit. This is a new history-specific input to scheduling evaluation,
not evidence overturning the earlier limited fresh-image intra-read coverage.

Before claiming a latency improvement, qualify an actual history-bearing XFS
artifact and measure regional claim/readiness, literal first `node -v` and
combined latency separately, on cold nodes and cached-node NEW sandboxes.
Populated large roots, occupied physical width and the original 1s/2s goals
remain open. Do not rerun an unchanged fresh-image grouping/cache experiment.
