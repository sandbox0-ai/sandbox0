# D-SPARSE-PRODUCER: isolate producer corruption from kernel hole support

2026-09-13. Evidence: `/tmp/sandbox0-sparse-producer.wDt9yJ/evidence.json`.
This advances [SPARSE-CONTRACT.md](SPARSE-CONTRACT.md), not a new latency trial.

## Causal result

The retained erofs-utils1.7.1 producer has a specific chunk-merging defect.
`erofs_blob_write_chunked_file` clears `lastch` on a hole but fails to update
`minextblks` at that boundary. `erofs_blob_mergechunks` consequently collapses
the sparse index to a larger contiguous chunk. For the16MiB source with just
two4KiB data ranges, this creates one16MiB mapped range pointing into a blob
with only8KiB of data. This reproduces the prior remote out-of-bounds mapping.

Upstream already fixed this in
[545988a65131](https://android.googlesource.com/platform/external/erofs-utils/+/545988a6513160808fb971d777d00f33316588a4):
update the minimum extent size at data/hole transitions, discontinuities and
the final interval. The diagnostic starts from the pristine1.7.1 tag
`83d94dc619075e71ca4d0f42941cfc18d269a2af`, applies only that upstream change to a
separate worktree, and verifies identical added/deleted patch lines. This is
not a newly invented publisher or a maintained product fork.

The entry point compiles the actual upstream `blobchunk.c`, hashmap and SHA256
code. It reconstructs every byte through the emitted chunk pointers and real
blob, checks bounds, and compares mapped coverage with source SEEK ranges.
The four source files have the same size, full SHA256, allocated-sector count
and SEEK data-byte count as the prior remote XFS fixture.

| File |1.7.1 result |1.7.1 plus exact upstream fix |
| --- | --- | --- |
| all-hole | One hole chunk; correct complete content | Unchanged |
| allocated-zero |4096 data indexes; correct complete content | Unchanged |
| sparse | One16MiB data chunk; out of bounds; no complete decoded hash |4096 indexes;8KiB mapped data; complete hash matches |
| tail | One16KiB data chunk; out of bounds; no complete decoded hash | Four indexes;4KiB mapped data; complete hash matches |

The baseline exits1 with two counterexamples; the patched process exits0 with
all four cases passing. Matched UBSan trap-mode builds produce exactly the same
reports, with no trap. Baseline failures are retained, not converted to passes.
Normal builds retain upstream/test signedness and unused-parameter warnings.
The initial combined ASan/UBSan link attempt fails for both builds because the
local runtime libraries are absent. ASan never executes; trap-mode UBSan is
not a substitute claim of ASan coverage. No packages are installed. An initial
release-tar URL returns404; the verified upstream git tag is used instead.

This is local aarch64 execution with OverlayFS-hosted source files, not remote
amd64 execution of a full mkfs binary, EROFS mount, full image or guest. The
same source geometry and identical patch make the producer mechanism causal;
they do not qualify a new remote image/toolchain or the complete RootFS format.

## Independent kernel boundary

The producer fix does not fix hole visibility on the current tested kernel.
Upstream [Linux6.8 EROFS](https://raw.githubusercontent.com/torvalds/linux/v6.8/fs/erofs/data.c)
and [Linux6.12 EROFS](https://raw.githubusercontent.com/torvalds/linux/v6.12/fs/erofs/data.c)
use `generic_file_llseek`. The native preceding trial independently observed a
successfully read hole-only inode with zero FIEMAP extents but whole-file data
coverage from SEEK_DATA. Therefore this is not merely a source-version guess.

[Linux6.8 OverlayFS copy-up](https://raw.githubusercontent.com/torvalds/linux/v6.8/fs/overlayfs/copy_up.c)
tries reflink first, then relies on the lower file's SEEK_DATA to skip holes.
That explains why the same-XFS control preserves sparse allocation while the
tested EROFS-to-XFS path materializes zeros. Even correcting the broken chunk
index cannot make that kernel seek path reveal holes.

The [upstream seek-support patch](https://lists.openwall.net/linux-kernel/2024/10/11/397)
is present in the inspected
[Linux6.13 source](https://raw.githubusercontent.com/torvalds/linux/v6.13/fs/erofs/data.c).
This identifies a stock-kernel capability to qualify, not proof that any running
Ubuntu kernel, provider image, gVisor path or production deployment supports it.
No kernel or remote environment is changed this turn.

Decision: reject the current1.7.1/Linux6.8 candidate combination for adoption.
Do not repeat its startup scores. A further compact-layout attempt needs a
qualified producer and actual stock-kernel sparse copy-up before full-image
cost/lifecycle and regional performance trials. Newer mkfs versions also add
allocated-zero-to-hole conversion (upstream`d86e27a`), so a wholesale upgrade is
not equivalent to this one-fix comparison. Preserve the equal-bytes/different-
allocation control and evaluate that difference explicitly.

## Integration and preservation

Freshly fetched sandbox0 origin/main and diagnostic HEAD remain`0f092204`.
The dirty worktree's existing bounded header cache, decoded mapping cache,
source admission and NBD read concurrency remain untouched. Session manager
still constructs the shared128MiB Reader cache and uses the XFS/Overlay runtime;
EROFS has not been wired into that product mount path. Credential preparation
before capacity exposure selects or reads no tenant RootFS. These are source
integration observations, not a new deployment or regression-test result.

All46 artifacts in the prior sealed experiment and all1359 frozen product files
are verified unchanged. The offline verifier passes2 tests/10 assertions,
including eight deliberate invalid-result mutations. Source clones, failed
build receipts, original/fixed binaries and fixture files are retained. There
are zero remote starts, claims, guest commands or startup samples, and no local
e2e, production rollout, merge or tag. The previous stopped receipt remains
historical; this turn does not claim a fresh cloud-state observation.

The original1s preferred/2s accepted regional startup gate, immediate literal
node-v measurements, no tenant prewarm, populated/history-bearing large roots
and actually occupied production-width acceptance remain open.
