# Online SandboxVolume Small-File Benchmark

Date: 2026-07-27

## Conclusion

For this online `ali-ue1` workload, mounted S0FS was substantially slower than
the sandbox's pod-local `/tmp` filesystem for many-small-file operations:

- Write throughput was **903 ops/s**, or **27.3%** of pod-local throughput.
- Immediate verified read throughput was **1,566 ops/s**, or **41.8%** of
  pod-local throughput.
- Recursive list plus stat throughput was **7,681 ops/s**, or **15.7%** of
  pod-local throughput.

Equivalently, pod-local storage completed the same operations 3.7x faster for
writes, 2.4x faster for reads, and 6.4x faster for list plus stat.

The S0FS results were consistent across all three rounds and all 90,000 S0FS
read operations returned the expected 4 KiB payload. The primary concern is
performance rather than correctness in this run. Write latency and metadata
throughput are the largest steady-state gaps, while immediate reads also show a
large P95 tail.

## Environment

| Item | Value |
| --- | --- |
| Team | `volume-benchmark` |
| Team ID | `f64c6e02-35a2-4b09-a37b-7e2745ec6ce8` |
| Region | `ali-ue1` |
| Cluster ID | `terway` |
| Volume | RWO S0FS mounted at `/workspace` |
| Baseline | Pod-local `/tmp` in the same sandbox |
| Sandbox kernel | `Linux 4.19.0-gvisor x86_64` |
| Python | `3.12.3` |
| Template | Public `default` |
| Memory | `2Gi` |
| Ephemeral storage | `8Gi` |
| Template image | `sandbox0ai/otemplates@sha256:275c63e8b4f508afc8f99f3ab377b45ae6037b27fef33c4ba05a0f61c54c6e38` |
| s0 CLI | `v0.7.5`, revision `d11226130416da6bff3ef16f4aa7e12b95125bdd` |
| Formal run | `2026-07-27T08:35:54Z` to `2026-07-27T08:40:28Z` |

The dedicated team is retained for repeatable benchmark runs. The formal
benchmark sandbox and volume were deleted after the run, and the CLI's
previously selected `sandpi` team was restored.

## Workload

The benchmark used
[`online_volume_small_file_bench.py`](./online_volume_small_file_bench.py),
which reuses the in-sandbox workload from
[`volume_mount_bench.py`](./volume_mount_bench.py).

Each target and round performed:

1. Create 30,000 files.
2. Write a deterministic 4 KiB payload to every file.
3. Read every file immediately and verify its complete content.
4. Recursively list the tree and stat every file.
5. Remove the benchmark tree before the next round.

Configuration:

| Parameter | Value |
| --- | ---: |
| Files per target per round | 30,000 |
| File size | 4,096 bytes |
| Data per target per round | 117.2 MiB |
| Files per directory | 128 |
| Worker threads | 8 |
| Rounds | 3 |
| Pre-created directories | No |

Target order alternated by round to reduce ordering bias. Both targets ran in
the same sandbox and Python process. Aggregate values below are the median of
the three per-round summaries.

Exact command:

```sh
python3 scripts/online_volume_small_file_bench.py \
  --team-name volume-benchmark \
  --team-slug volume-benchmark \
  --file-count 30000 \
  --file-size 4096 \
  --parallelism 8 \
  --files-per-dir 128 \
  --rounds 3 \
  --json-output /tmp/sandbox0-volume-small-file-30000x4k-p8-r3.json
```

The orchestrator starts the remote workload asynchronously and polls short
commands for completion. This avoids making benchmark duration depend on an
HTTP request timeout or a long-lived WebSocket connection.

## Aggregate results

### Throughput

| Phase | Pod-local `/tmp` | Mounted S0FS | S0FS / local | Local speedup |
| --- | ---: | ---: | ---: | ---: |
| Write | 3,302 ops/s | 903 ops/s | 27.3% | 3.66x |
| Read and verify | 3,744 ops/s | 1,566 ops/s | 41.8% | 2.39x |
| List plus stat | 49,001 ops/s | 7,681 ops/s | 15.7% | 6.38x |

For 4 KiB files, the effective payload rates were:

| Phase | Pod-local `/tmp` | Mounted S0FS |
| --- | ---: | ---: |
| Write | 12.90 MiB/s | 3.53 MiB/s |
| Read and verify | 14.62 MiB/s | 6.12 MiB/s |

These payload rates characterize this metadata-heavy small-file workload. They
are not sequential large-file bandwidth measurements.

### Latency

| Phase | Local P50 | S0FS P50 | P50 multiplier | Local P95 | S0FS P95 | P95 multiplier |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Write | 0.503 ms | 8.634 ms | 17.2x | 0.662 ms | 14.307 ms | 21.6x |
| Read and verify | 0.418 ms | 1.795 ms | 4.3x | 0.545 ms | 32.514 ms | 59.7x |
| Stat during tree walk | 0.004 ms | 0.055 ms | 13.8x | 0.006 ms | 0.105 ms | 17.6x |

The list-plus-stat throughput includes directory traversal and all stat calls.
The latency percentiles in that row time individual stat calls only.

## Per-round stability

| Round | Target | Write ops/s | Read ops/s | List plus stat ops/s |
| ---: | --- | ---: | ---: | ---: |
| 1 | Pod-local `/tmp` | 3,293 | 3,705 | 49,001 |
| 1 | Mounted S0FS | 903 | 1,578 | 7,667 |
| 2 | Mounted S0FS | 924 | 1,566 | 7,697 |
| 2 | Pod-local `/tmp` | 3,355 | 3,744 | 43,725 |
| 3 | Pod-local `/tmp` | 3,302 | 3,745 | 49,565 |
| 3 | Mounted S0FS | 901 | 1,561 | 7,681 |

S0FS varied only from 901 to 924 write ops/s, 1,561 to 1,578 read ops/s, and
7,667 to 7,697 list-plus-stat ops/s. The result is therefore not driven by one
outlier round.

## Control-plane observations

The formal run recorded:

| Operation | Elapsed |
| --- | ---: |
| Create RWO S0FS volume | 0.476 s |
| Claim sandbox and return mounted bootstrap status | 0.562 s |
| Complete the full remote benchmark command | 270.076 s |
| Delete sandbox | 0.526 s |
| Delete detached volume | 0.488 s |

The volume was reported as `mounted` before the remote workload started.
Control-plane timings are included for run context, but this report is focused
on mounted data-plane performance.

## Interpretation

1. **Small-file writes are latency-bound.** At 4 KiB per file, S0FS delivered
   only 3.53 MiB/s even though it sustained about 903 operations per second.
   Improving per-file create/open/write/close latency and reducing FUSE or
   metadata round trips matters more than increasing object-store bulk
   bandwidth for this workload.
2. **Metadata traversal is the largest throughput ratio gap.** S0FS reached
   15.7% of local list-plus-stat throughput. Large repository trees and package
   directories will expose this cost even when their total byte size is small.
3. **Immediate read throughput is better than write throughput, but the tail is
   high.** Median S0FS read latency was 1.80 ms, while P95 reached 32.51 ms.
   Workloads that open many files on a critical path can accumulate these tail
   delays.
4. **The result is repeatable within this sandbox.** The three S0FS rounds were
   tightly grouped and all content verification passed.

For agent workspaces, the practical mitigation is to avoid exploding caches or
dependencies into unnecessary file counts, keep packed or archived forms where
possible, and optimize S0FS metadata batching and hot-read tail latency. Mounted
S0FS provides persistence, but this run does not show local-filesystem-like
performance for a 30,000-file tree.

## Limitations

- Reads immediately followed writes in the same sandbox, so they represent a
  warm read-after-write path, not a cold read after reattachment.
- Files were closed but not explicitly `fsync`ed. This is not a durable
  per-file-fsync benchmark.
- The write phase included initial directory creation because directories were
  not pre-created.
- The Python thread pool and gVisor scheduler contribute to absolute latency.
  The same-process local baseline makes the relative comparison more useful
  than the absolute numbers.
- `/tmp` is the sandbox's ephemeral pod-local filesystem. It is a baseline, not
  a competing persistent-volume product.
- This is one region, one sandbox allocation, one template shape, and one time
  window. It is not an availability or performance SLA.
- Delete throughput, cross-sandbox reattachment, cold-cache reads, snapshot
  restore, and concurrent multi-sandbox access were not measured.

The separate
[`online_volume_reattach_bench.py`](./online_volume_reattach_bench.py)
profile now covers destroy-and-reattach content verification. See
[`SANDBOX_VOLUME_BENCHMARK_STANDARD.md`](./SANDBOX_VOLUME_BENCHMARK_STANDARD.md)
for its result and for the boundary between warm throughput, phase-level sync,
and lifecycle durability.
