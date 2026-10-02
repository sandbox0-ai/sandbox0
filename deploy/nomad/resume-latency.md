# Resume admission latency

Memory resume retains its normal staging, image custody, writer attachment and
stock runsc restore checks. Disposable local caches reduce repeated work:

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

## Complete image retention

After immutable regional publication, the capture custodian may retain the
complete private image using hard links under `migration-images/retained-images`.
The stopped-source custodian narrows stock-runsc capture files to mode 0600
on their original inodes before linking them; all cache/image directories are private.
Keeping the original inode preserves its normal, reclaimable page-cache mapping;
assembling a new image with range clones does not. This cache does not pin RAM,
skip content hashing, grant restore authority or change the runsc release.

The node enables retention only after opening its enforced XFS staging project.
Limits are the smaller of 4 GiB and one eighth of staging bytes, the smaller of
1,024 and one eighth of staging inodes, and eight images. Retention is disabled
for shares below 1 MiB or four inodes. Entries expire after 24 hours without a
hit; expiration is reclaimed on access/startup. LRU admission enforces the limits.
The kernel project quota also charges the retained source extents after original
capture cleanup. Quota pressure evicts disposable cache links and retries normal
staging admission; it never deletes source, destination or journal custody.

A hit requires the exact regional reference and binding, normal staging
admission, exact inventory and full hashing of every image chunk. It links the
same inodes into a fresh private destination, syncs data and directories, then
may retain the existing process-local verification proof. All links precede
watch sealing, since adding/removing links changes inode ctime. Broken cache
metadata, changed content, unsupported hard links, cross-filesystem placement,
expiry and eviction use the existing verified chunk/regional download path.
An existing destination is never deleted by cache fallback. Only successful
publication populates retention; downloading into the chunk-cache path does not
add links after sealing and invalidate its existing verification optimization.

Cache manifests are disposable metadata, never execution authority. Private
rooted filesystem operations, bounded manifests/inventories, synced atomic entry
publication and per-operation directory flock coordinate overlapping ctld
generations. Valid cache entries survive service restart; watches do not. The
shared lock covers inventory and linking only; complete content hashing runs
independently in each destination so concurrent restores do not queue behind it.

Memory claim admission derives a soft preference for the exact capture node ID,
UID and boot from validated regional evidence. Compatible live slots on that
node precede FIFO selection. Existing capacity, fencing, retirement, heartbeat,
staging-budget and SKIP LOCKED checks still apply; other nodes remain eligible.
Retries keep their original slot. No public placement or API fields are added.
Image-preparation timing logs report `transport=retained-image` and
`retained_image_bytes` on successful hits.

Local unit coverage checks capture cleanup/restart inode identity, all publication
paths, corruption and symlink fallback, regional authority, admission, existing
destination custody, expiry, eviction and overlapping Store instances. Database
integration coverage exercises preference plus draining, insufficient capacity
and locked-slot fallback. The isolated Linux quota/stock-runsc acceptance
below measures restore performance and verifies process continuity; full
regional/Sandpi wake latency remains a separate rollout-canary measurement.

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

`TestPrivilegedRetainedCheckpointRestore` uses the implemented Store cache and
staged publisher, rather than the earlier manual-hardlink prototype. It creates
its own 4-GiB loop image, requires loop direct I/O, mounts XFS with enforced
project quotas, and checks the qualified stock-runsc release and binary digest.
Install `xfsprogs` and `util-linux` on an isolated root-capable Linux test host.
The complete qualified runsc bundle must be installed, including its adjacent
`gvisor-bin` directory. Build the guest test payload statically:

```sh
CGO_ENABLED=0 go test -c -o /tmp/retained-checkpoint.test ./pkg/gvisorcli
sudo env SANDBOX0_RUN_RETAINED_RESTORE=1 /tmp/retained-checkpoint.test \
  -test.run '^TestPrivilegedRetainedCheckpointRestore$' -test.v -test.timeout=15m
```

It runs three alternating pairs of verified chunk-clone and retained-inode
restores against the same 512-MiB random-memory checkpoint. Source files and
Store handles are removed before restore, proving that retained entries survive
capture cleanup and service restart. `mincore` checks that baseline page-file
inodes start cold and retained inodes are resident. Each restored guest answers
a fresh full-memory challenge and preserves its token, PID, counter, open unlinked
file offset, tmpfs contents and CPU features. Finally, it proves that retained
images consume actual project quota and eviction restores admission headroom.
Logs separate preparation, admission verification, runsc restore, first response
and challenged memory validation. The PR stock-runsc job invokes this test after
installing the qualified runtime and XFS tools. These timings exclude the full
regional/Sandpi HTTP path; they do not establish production end-to-end latency.

Run `TestCheckpointRestorePrefersCaptureNodeWithoutBlockingFallbackIntegration`
only against a dedicated disposable PostgreSQL database. The repository's test
fixture drops the `manager` schema; never point it at a production or shared
application database. `INTEGRATION_DATABASE_URL` must name that isolated database.
The test checks original-node preference, draining/capacity/locked-slot fallback
and exact retry placement.

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

## Retained-image remote acceptance (2026-10-02)

The implemented cache passed the isolated stock-runsc test on an Ubuntu 24.04
GitHub-hosted Linux runner. A 512-MiB random-memory guest produced a
540,150,349-byte complete image. All six restored guests passed full-memory
challenges, PID/token/counter continuity, tmpfs, open-unlinked FD offset and CPU
feature checks. Retained cache entries survived source cleanup and Store restart,
and actual XFS project-quota rejection and eviction recovery passed.

| Alternating pair | Chunk-clone prepare to first response | Retained-inode prepare to first response |
| --- | --- | --- |
| 0 | 5.123806 s | 0.593634 s |
| 1 | 1.436836 s | 0.632250 s |
| 2 | 1.445864 s | 0.525444 s |

The median fell from 1.445864 to 0.593634 seconds (58.9%). The first baseline
sample includes initial chunk-cache reads and is retained in the evidence.
Baseline page-image inodes had zero resident pages before restore; retained
inodes had all 131,829 pages resident. runsc restore alone was 1.036–1.068
seconds for baseline and 0.369–0.410 seconds for retention. Complete hashing
remained enabled. These three pairs demonstrate the mechanism on this test
host, not a production percentile or complete Sandpi wake SLO.

[Machine-readable samples](testdata/retained-restore-20261002.json) record
the tested commit, isolated environment, stage timings and
[remote workflow evidence](https://github.com/sandbox0-ai/sandbox0/actions/runs/36956283454).
