# D-XFS-LOG-SCOPE: log search is a bounded part of the remaining gap

2026-09-13. Evidence: `/tmp/sandbox0-xfs-log-scope.Fli0cZ`.
Remote retained diagnostic: `/data/sandbox0-xfs-log-scope-Fli0cZ`.
This changes the optimization priority, not runtime source or measured performance.

## Result

Do not treat XFS mount time as log-search time or pure S3 waiting. Bind freshly
read authenticated superblocks to the exact format2 artifacts already observed
in D-MAPPING-TRACE, then partition its existing 14,373 NBD reads. The prior
fixed speculative log-search-tree experiment remains rejected; it is not rerun.

| Exact image / cold claim, four samples each | Node | Coding |
| --- | ---: | ---: |
| Logical volume | 16GiB | 1TiB, sparse |
| Allocation groups | 4 | 4 |
| Internal log size | 64MiB | 512MiB |
| Requests intersecting the log | 54 | 57 |
| Of those, 512-byte requests | 22 | 25 |
| Unique requested log bytes | 2,104,320 | 2,104,832 |
| Log-request wall-time union | 100.602–101.236ms | 130.313–130.824ms |
| 512-byte request wall-time union | 79.269–79.344ms | 111.229–111.293ms |

The eightfold log-size difference does not imply fetching an entire log or an
eightfold read-volume difference. Coding's per-request duration SUM is
374.867–378.286ms, but concurrent requests overlap: its union is only about
130ms. Another 111.750–112.027ms of that union overlaps other requests from the
SAME writer. Overlap does not prove which request blocks the application, nor
that the log is removable without changing other scheduling.

No log-range NBD read intersects any of the 16 immediate first-command windows.
Cached-new log-read unions are Node 37.624–50.856ms and Coding
117.741–131.523ms; these are NEW sandbox identities, not repeated commands.
All 32 per-sandbox/phase byte and time unions match an independent event-sweep
oracle. No request crosses a log boundary or the retained claim/command boundary.

Even an optimistic diagnostic that removes each sample's ENTIRE log-request
union, holding every other duration fixed, leaves all four cold Coding combined
times at 2.535145–2.535565s. This rejects log-only work as sufficient for the
observed gap. It is not a predicted speedup, lower bound for all architectures,
permission to bypass recovery, or proof that every smaller log is ineffective.

These timings remain the historical, instrumented D-MAPPING-TRACE observations
with stock runsc20260810. They must NOT be subtracted from D-PREFETCH-TRIAL's
different artifacts or substituted for current-pin/occupied-width acceptance.
The latter trial's 452–460ms XFS mount figures have a different sample binding.

## Exact read and validation scope

Node artifact `2b5208c9…`, descriptor `8a28ddc3…`; Coding artifact `21a2f3ba…`,
descriptor `aff13a25…`. The full hashes, object allowlists, reader source inventory
and raw 512-byte superblocks are retained. The probe reads 4096 logical bytes
per image through the current authenticated Reader, each with an independent
128MiB cache and 16MiB ciphertext-request budget, under one 30s diagnostic
deadline. No NBD, mount, guest, claim, command, object mutation or ctld cache use.

The successful probe makes six underlying ciphertext range calls and receives
393,144 bytes, including mapping/encryption metadata. That is inspection I/O,
not startup amplification or an authenticated count of wire-level retries.
Go and independent Ruby decoders agree on both CRC32C-checked superblocks.
FSB addresses are converted through allocation-group geometry, not incorrectly
multiplied as linear block numbers. Two Go tests pass three race repetitions;
vet/build pass. Five interval tests pass 113 assertions, and all 26 corrupt-sector
or wrong-artifact mutations are rejected. All 1359 runtime source files remain
unchanged.

The format/algorithm reference is the pinned Linux v6.8
[XFS superblock definition](https://raw.githubusercontent.com/torvalds/linux/v6.8/fs/xfs/libxfs/xfs_format.h)
and [cycle-search implementation](https://raw.githubusercontent.com/torvalds/linux/v6.8/fs/xfs/xfs_log_recover.c).
The latter issues dependent single-basic-block reads during binary search.
This source reference plus log-range membership does not establish exact live
kernel function attribution for every retained request.

## Failures, disposition and remaining work

The first probe failed Reader initialization without exporting a superblock.
Its runner omitted the existing test AWS credential profile. Preserve the failed
unit and output; do not infer zero attempted object requests from absent counters.
The corrected runner adds that existing profile and uses the SAME binary and
targets, with distinct unit/output names. Both units are terminal; the successful
runner waits for its child. Two exploratory local JSON queries also failed on
Array/Integer-versus-Hash assumptions; no analysis result derives from those
queries. The final parser validates actual types/bindings and all original reads.

Original binaries/configs/service PIDs, job73058, original263/source2072/trial2121
sandbox rows and 64 detached NBDs are checked before/after. Preserve all `/data`;
owned SSH is closed and original 2CPU/8GiB compute is stopped through the remote
skill lifecycle, with a final independent cloud receipt.

No log prefetch, log-size tuning, recovery change, production rollout, merge or
tag is admitted. Focus the next causal budget on non-log filesystem and executable
dependencies across mount, runsc/procd and the actual command; do not reopen the
closed fixed-tree, bootstrap-profile, broad ELF or concurrency-parameter loops.
The 1s target/accepted2s, populated/history-bearing large root and genuinely
occupied production-width requirements remain unproved.
