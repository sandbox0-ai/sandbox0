# Resume admission latency

Memory resume retains its normal staging, image custody, writer attachment and
stock runsc restore checks. Two disposable local caches reduce repeated work:

- Journal scan exclusions retain one payload fingerprint per slot, and hash the
  current bytes inside every original Bolt admission transaction. Only fully
  validated records without custody/admission/staging work are excluded. Pruning
  forgets removed slots. Each independent scanner accepts at most 65,536 pairs;
  overflow keeps existing entries rather than thrashing on a sequential scan.
  Changed payloads and startup are decoded and validated again.
- A Store download can retain a single-use verification proof for 30 seconds,
  at most 16 images. Files are sealed on the materializer's exact descriptor
  after verified writes/range clones and fsync, under exclusive private staging
  custody. Linux inotify detects writes, attribute changes and inode removal;
  the exact directory, file inventory, inode identities and metadata are also
  checked before reuse. Any event (including overflow), mismatch, missing guard,
  eviction or process restart falls back to full content hashing. Peer downloads
  and durable journal receipts cannot populate this proof. Timers, eviction and
  Store shutdown close unused watches.

## Isolated reproduction

Run the opt-in checkpoint benchmark only on an isolated Linux test host. Use a
private XFS filesystem with reflink enabled; when backed by a loop file, enable
loop direct I/O so the host page cache does not hide cold destination reads.
The test creates its own synthetic 528 MiB image and requires 66 real verified
range clones. It never reads customer checkpoints or runtime journals.

```sh
TMPDIR=/short/private/xfs SANDBOX0_RESUME_LATENCY_TEST=1 \
  go test ./pkg/runtimecheckpoint -run '^TestResumeLatencyRemote$' -v -count=1

go test ./pkg/runtimecheckpoint ./pkg/nomadruntime -race -count=1
```

An isolated 16-vCPU/64-GiB Linux/XFS direct-I/O test on 2026-10-01 measured
restore admission's repeated content verification at 3.095437, 2.793897 and
2.768614 seconds before the fix, versus 0.000244, 0.000214 and 0.000200 seconds
with the guarded proof. Warm preparation remained approximately 0.323 seconds.
These are stage timings, not complete sandbox or application wake latency.

`TestMigrationPoolScanExclusionsWarmBeyondFormerRingSize` reproduces the journal
cache defect with 1,024 slots: the old 256-entry ring excludes zero records on
its second sequential scan. The keyed cache excludes all 1,024. Other tests
cover changed/corrupted records, newly active staging, cache capacity, file
corruption/replacement, restored timestamps, missing watches, restart, expiry
and single-use consumption.
