# D-SPARSE-CONTRACT: sparse semantics block candidate admission

2026-09-13. Evidence: `/tmp/sandbox0-sparse-contract.vgvAn8/evidence.json`.
Remote retained fixture: `/data/sandbox0-xfs-staging-G3Q8sb/trial/sparse-contract-vgvAn8`.

## Result

This is one native filesystem counterexample campaign, followed by read-only
inspection of the exact failed image. It is not a startup benchmark, a smaller
replacement for the full Node/Coding roots, a guest test or an S3 test. The
retained Linux 6.8.0-124-generic kernel and erofs-utils 1.7.1-1build2 are unchanged.
The candidate remains unqualified; no runtime source or default is changed.

Four explicit XFS files distinguish holes from allocated zero bytes. The
16MiB `allocated-zero` and `sparse` files have identical complete payload hashes,
but only the latter has two allocated 4KiB ranges. `all-hole` has no written
data; `tail` is 12345 bytes with one written 4KiB range. Copy-up is triggered by
chmod to 0600 with metacopy disabled; this is not a payload write. The recorded
source modes were already 0600, so do not describe it as a 0644-to-0600 change.

| File | Source XFS allocation | Plain EROFS to XFS copy-up allocation | Same-XFS OverlayFS control allocation |
| --- | ---: | ---: | ---: |
| all-hole | 0 | 16MiB | 0 |
| allocated-zero | 16MiB | 16MiB | 16MiB |
| sparse | 8KiB | 16MiB | 8KiB |
| tail | 4KiB | 16KiB | 4KiB |

All four complete hashes match in the plain and control cases. Plain EROFS
nevertheless loses hole representation, and copy-up materializes the zeros.
The control preserves the observed SEEK_DATA/HOLE ranges and allocated sectors.

Adding only `--chunksize=4096` builds a 28672-byte image; mkfs, fsck and mount
all exit successfully. Reading `sparse` then fails with EIO, before its initial
observer can emit the whole report. A second, read-only observer retains errors
per file: `all-hole` and `allocated-zero` read completely with correct hashes;
`sparse` and `tail` fail with EIO and deliberately have no complete hash.
The earlier commentary naming `all-hole` as the failed read was incorrect.

Both kernel FIEMAP and dump.erofs describe the failed `sparse` file as one
16MiB range beginning at physical byte20480, and `tail` as16KiB from the same
offset. These mappings extend beyond the28672-byte image. This is evidence of
a local mapping/format compatibility defect, not S3 latency; the exact producing
code defect is not isolated here. The image is not rebuilt or repaired in place.
Its SHA256 remains `9bdf76c84497ff471cfeadd9214e500ed388d4b9ee918a9e2c45cfd744833233`.

Even the successfully read chunked `all-hole` file exposes an important split:
FIEMAP has zero extents, whereas SEEK_DATA covers16MiB and st_blocks reports16MiB.
These are separate observations, not interchangeable allocation measures.
Chunked copy-up was not reached and must not be recorded as passing or failing.

## Harness failure and preservation

The initial campaign remains terminal FAILED, not completed. After the chunk
failure it runs the same-XFS control, cleans up, then its source-unchanged check
rejects the newly added FIEMAP SHARED flags from that control. Independent
comparison finds unchanged source identity, bytes, size, mode, allocated sectors,
SEEK ranges and physical addresses; only those exact SHARED flags differ. The
offline verifier recognizes this specific observed transition without changing
production FIEMAP policy or overwriting the failed runner/receipt.

The separate read-only followup completes and hashes the original upper image,
which exactly matches its before record. It does not turn the initial campaign
into a pass. All88 exported JSON records have verified sizes and hashes; all1359
frozen product files remain unchanged. The final check examines228 process mount
tables, finds no owned mounts or loops, and verifies64 detached NBDs, original
process/file identities, DB counts and Nomad job index73058. All fixture images
and `/data` are preserved. The owned SSH connection closes before the single
compute stop; an independent cloud read at2026-09-13T02:22:48.383007813Z confirms
Stopped/StopCharging with the original2CPU/8192MiB shape.

There are zero claims, guest commands or startup samples. No kernel/mkfs update,
full image rebuild, tenant prewarm, command training, S3 publication, production
rollout, merge or tag occurs. The1s/2s regional ingress and immediate-node-v
requirements, populated/history-bearing roots and occupied-width tests remain
open. A faster component result cannot admit this unqualified layout.

## Prefetch clarification

The current candidate concerns persistent storage layout, not predicting when
a user will claim. Artifact preparation is offline storage work, not warming
disposable compute. A target node only knows the selected RootFS after claim;
its required fetches remain inside the measured claim path. Optional command
prefetch can only begin after the real command/executable is known and must
remain in command latency/resource accounting. The broad-ELF prototype failed
its activation screen in [PREFETCH-TRIAL.md](PREFETCH-TRIAL.md) and is not adopted.
