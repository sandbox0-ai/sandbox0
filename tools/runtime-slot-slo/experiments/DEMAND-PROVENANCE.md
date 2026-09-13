# D-DEMAND-PROVENANCE: separate read amplification from service latency

2026-09-12. Evidence: `/tmp/sandbox0-demand-provenance.CLqEfh`.
This attributes the sealed four native workloads; it is not a new startup run,
an approved RootFS format, or a production-width performance result.

## Findings

The wrapper's extra bytes already appear at the kernel NBD request boundary.
Its generic 4KiB base adapter is not responsible for most of the extra volume:

| Image | Ordinary unique NBD bytes | Wrapper unique NBD bytes | NBD delta | Base delta |
| --- | ---: | ---: | ---: | ---: |
| Node | 93,221,376 | 150,721,024 | 57,499,648 | 57,511,936 |
| Coding | 94,157,824 | 154,076,672 | 59,918,848 | 59,994,112 |

Every prefix NBD byte is covered by the base-read union. Base bytes outside that
union are only 105,984/118,272 for Node ordinary/wrapper and 50,176/125,440 for
Coding. This locates the measured amplification above Branch/Reader; it does
not prove mandatory application demand or invariant volume under another backend.
Kernel speculation and delivery-timing effects remain possible.

Old readonly source-file FIEMAP projects both ordinary workloads onto exactly
the same file offsets: Node reads 65,314,816 bytes and procd 18,874,368 bytes.
All 15,946 common complete Node blocks and 4,608 procd blocks have matching hashes
at matching file offsets across the two images. The different total RootFS sizes
therefore do not require different reads of these two identical executables.

A metadata-only remote session obtains the missing outer-XFS map for the exact
retained `lower/lower.erofs` files using `xfs_db -r`, without mounting XFS or
creating a loop/NBD/guest. Compose that map with the sealed plain-EROFS FIEMAP.
Reject unsupported extent flags and non-AG0 encoded block addresses; trim EOF,
exclude storage holes and do not interpret inline-tail physical zero as data.
The selected images have 14/696 outer-file extents, all in AG0.

| Selected file, same result in both images | Ordinary NBD MiB | Wrapper NBD MiB | Additional MiB |
| --- | ---: | ---: | ---: |
| Node executable body | 62.289 | 97.746 | 35.457 |
| procd body | 18.000 | 23.324 | 5.324 |

The wrapper covers every ordinary-demanded body range and additional ranges.
Node's reads are in the first command; procd's are before it. Together the two
files account for 42,762,240 extra bytes: **74.37% of Node's total NBD increase and
71.37% of Coding's**. This is file-data attribution, not a claimed latency saving.
Selected-file inline tails (2,616 Node / 2,233 procd bytes) are excluded; other
libraries/files/metadata and overreads remain in the residual, not all metadata.

Independently reread only the selected observed body blocks from the readonly
wrapper images: all **61,988 blocks / 253,902,848 bytes** match their original
trace hashes. No tree scan, image rebuild, guest or object request is needed.
This validates the projected byte positions against the retained observations;
it is not a new complete-image hash or authenticated writer/image binding.

Before path attribution, the two wrappers shared 10,441 wrapper-only nonzero
4KiB content fingerprints (42,766,336 distinct bytes). Fingerprints alone did
not establish paths, inode identity, ownership or required demand. The file
projection above supplies the missing path/offset evidence for selected bodies.

## Measurement adapter and main/candidate distinction

Freshly checked core main `0f092204` still reads Branch data block-by-block. The
frozen candidate alone adds the concrete `*rootfsblock.Reader` clean-span fast
path. Current candidate session constructors pass that concrete Reader directly.
The prior native diagnostic instead passes `observedFile` over `os.File`, so it
uses the generic full-block fallback. Its base-call counts/local timings must
not be relabeled as candidate encrypted-Reader/S3 performance.

A Go overlay adds no product file and tests 12 combinations of concrete Reader,
Reader adapter, source observer, zero/fitting cache and clean/dirty branch data.
All bytes and journal overrides remain checked. For the synthetic 256KiB fixture,
zero cache gives concrete/source-observed reads 2 data GETs (3 with the dirty
split), versus adapter 64 (63 with dirty override). With fitting cache all modes
use 2 GETs, but the adapter still makes 64/63 base calls. These are mechanism
tests, not a cache-size recommendation or startup measurement. Observe below
the concrete Reader; do not weaken its trusted-type/checksum guard for a probe.
The existing clean-span/holes/corruption tests also pass under the race detector.

Candidate coalescing can prefetch outside demand when cache permits. That is not
a new diagnosis: D-DATA-REUSE and B-RECENCY-ABBA already tested demand-only recency
and reverted it after no net win. Do not restart that cache-policy sweep based
on these synthetic GET counts.

## Next causal question, not another parameter sweep

The sealed wrapper harness requests a readonly loop, without requesting direct
I/O, over the outer XFS file. Upstream Linux 6.8 has distinct buffered
`vfs_iter_read` and direct `IOCB_DIRECT` loop paths; XFS's buffered read uses
`generic_file_read_iter`. See [loop implementation](https://github.com/torvalds/linux/blob/v6.8/drivers/block/loop.c)
and [XFS read dispatch](https://github.com/torvalds/linux/blob/v6.8/fs/xfs/xfs_file.c).
Those sources support a nested-buffering/readahead hypothesis, **not proof** of
the historical loop's actual DIO state or which layer generated each extra read.
The web fetch of EROFS's upstream data source failed; no conclusion uses it.

Before reopening this wrapper, observe the exact loop/inner demand and outer
XFS/NBD demand with live device identity/DIO state, and explain the added selected
file offsets. Do not infer that changing a readahead value or requesting DIO
will remove them. Any proposed structural change needs exact-byte safety tests,
its own complete-read result and encrypted/region net-cost validation. The
current wrapper remains unchosen and failed its complete-read-volume screen.

## Machine and S3 remain separate axes

[D-CORRELATED-READER](CORRELATED-READER.md) already pairs 581 request IDs with
actual OSS logs. In its cold reader-only cohort, client post-write P95 is
34.585ms/max97.942ms and OSS response P95 36ms/max98ms; 116 of 121 client waits
over20ms also have OSS response over20ms. TCP smoothed RTT P95 is0.801ms. That
supports a service-response contribution, not merely a long network RTT.

That separate host sample is about80% idle without steal, throttling or OOM,
but does show some CPU pressure. It does not exonerate a two-core host at actual
production occupancy, prove an OSS defect, or permit adding serialized helper
intervals to claim latency. No new S3 baseline, provider-cache guarantee, host
benchmark or hardware change is made here. Both excess demand and service
response tails can matter; they are not mutually exclusive explanations.

## Preservation and closure

The initial post-boot readonly guard gets Nomad connection refused; retain that
failed receipt. The later map operation passes its own unchanged baseline guard
before touching the staging mount; readiness recovers without a service restart.
Both short collection operations mount only the identified staging ext4 volume
readonly/noload in private mount namespaces and unmount successfully. No XFS
mount, loop, NBD attachment, carrier, policy, database or object mutation occurs.
This does not fix or relabel the prior workload runner's outer-unmount failure.

Final checks preserve original runtime files/PIDs/rows/job within this boot,
verify no staging mounts across232 process mount tables, no loops and64 detached
NBD devices. SSH closes; the same Makefile stop call completes, and a fresh
11:06:59 UTC cloud read confirms Stopped/StopCharging/no public IP. `/data` and
all prior failures remain. Six offline tests/221 assertions pass, in addition to
the Go race tests. All1,353 frozen product files and193 worktree entries remain;
only diagnostic helpers and experiment notes are added/updated.

No new regional or guest-command sample, timeout/cache/hardware/width change,
production rollout, merge, tag, format adoption or material deletion. Original
empty-node/cached-new identities, populated root/upper histories, actual occupied
production width, encrypted durable authority and regional claim plus immediate
real `node -v` within1s (preferred) or2s (accepted) all remain open gates.

## Follow-up: actual inner and outer requests

[D-LAYER-DEMAND](LAYER-DEMAND.md) now observes one exact wrapper workload with
loopDIO=0, equal4096KiB inner/outer readahead and no lost kernel events. After
conservatively accounting for pre-registration udev probes,57MiB of outer-file
NBD reads lie outside known inner requests and are all covered by readahead BIOs.
Inner backed Node/procd offsets match the ordinary control exactly. This locates
the measured candidate's second expansion in the buffered outer-image path;
it does not prove a DIO treatment, encrypted net benefit or format qualification.
