# Sandbox Volume Benchmark Standard

Date: 2026-07-27

## Executive conclusion

There is no public, reusable volume benchmark shared by E2B, Modal, Daytona,
Runloop, ComputeSDK, and Archil.

The public benchmarks found for these projects measure different layers:

- ComputeSDK measures sandbox time to interactive, burst scale, and a cold
  development workload. Its storage suite measures object-storage APIs, not a
  filesystem mounted into a sandbox.
- Daytona measures end-to-end coding-agent rollout infrastructure.
- Runloop's public benchmarks evaluate agents against scored datasets.
- Modal publishes sandbox-scale and SWE-bench demonstrations.
- E2B publishes startup claims and a historical memory/huge-pages experiment.
- Archil publishes filesystem performance specifications, but not a public
  workload script with raw results.

Consequently, a defensible comparison needs separate standards for sandbox
startup, realistic development work, and persistent-volume I/O. It must not
collapse them into one score.

For Sandbox0 persistent volumes, this repository now uses the following
three-part data-plane standard:

1. write 30,000 files, destroy the producer sandbox, reattach the same volume
   to a fresh consumer, and verify every byte;
2. pinned `mdtest` 4.0.0, one task, 30,000 files, 4 KiB writes and reads, and
   phase-level `sync`;
3. a 30,000-file, 4 KiB, eight-worker workload that verifies every byte read.

The online Sandbox0 suite passed all timed invocations and validation checks.
After producer destruction and fresh-sandbox reattachment, all 30,000 files
and 122,880,000 bytes matched the expected dataset digest.
The S0FS `mdtest` medians were 343 file creations/s, 8,096 stats/s, 3,213
reads/s, and 2,205 removals/s. The separate eight-worker test passed full
content verification and produced 903 writes/s, 1,566 verified reads/s, and
7,681 list-plus-stat operations/s.

These are Sandbox0 baselines, not yet a current provider leaderboard. No
equivalent public volume results were found for the named providers, and the
same runner has not yet been executed with their credentials.

## Public benchmark audit

| Project | Public benchmark or claim | What it actually measures | Reusable for persistent-volume comparison? |
| --- | --- | --- | --- |
| [ComputeSDK](https://github.com/computesdk/benchmarks) | Time to Interactive (TTI) | API request through sandbox readiness and the first `node -v`; 100 sequential, 100 staggered at 200 ms, and 100 burst creations | No. Its methodology explicitly excludes filesystem operations. Reuse it unchanged for the control-plane layer. |
| [ComputeSDK DAX](https://github.com/computesdk/benchmarks/blob/51e918c64877913981e632b58f55eec8ca1fb1e2/benchmarks/scripts/dax-benchmark.sh) | Cold OpenCode development workload | Fresh sandbox, prerequisite installation, pinned Node and Bun, pinned repository clone, `bun install`, and `bun typecheck`; target shape is 8 vCPU and 16 GiB | No direct volume isolation. Reuse it as a macro sandbox/rootfs/network/CPU test. |
| [ComputeSDK Storage](https://github.com/computesdk/benchmarks/blob/51e918c64877913981e632b58f55eec8ca1fb1e2/benchmarks/storage/benchmark.ts) | Upload/download at 1, 4, 10, and 16 MB | StorageSDK object API latency and throughput across S3-compatible and blob providers | No. It does not mount a POSIX-like sandbox volume. |
| [E2B](https://e2b.dev/) | Sandbox startup claims; 2024 "up to 5x" experiment | Startup/interactive latency claims; the older experiment timed the first read of 0.5 GB after a huge-pages memory change | No public harness or raw volume results found. [E2B Volumes](https://e2b.dev/docs/volumes) are currently private beta. |
| [Modal](https://modal.com/blog/sandbox-launch) | SWE-bench Verified in seven minutes; sandbox creation throughput | Parallel evaluation infrastructure and sandbox scale, not storage isolation | No public volume benchmark script or raw result found. Volume v1/v2 semantics also require explicit commit/reload treatment. |
| [Daytona](https://www.daytona.io/dotfiles/the-hidden-infrastructure-tax-in-coding-agent-rl) | RL infrastructure benchmark | Fresh episode latency and 10/50/150-action coding-agent trajectories on 1 vCPU and 1 GiB, with fixed commands across Docker, EC2, Kubernetes, Fargate, and Daytona | No. It is useful as an end-to-end rollout benchmark. The article's linked benchmark repository was not publicly retrievable during this audit. |
| [Runloop](https://docs.runloop.ai/docs/benchmarks/public-benchmarks) | Interactive and orchestrated public benchmarks | Agent scenarios, devboxes, traces, and correctness scorers | No. These are agent evaluations, not storage benchmarks. |
| [Archil](https://docs.archil.com/details/performance) | Sub-millisecond cached TTFB, 10-30 ms example uncached S3 reads, up to 10 GB/s and 10,000 IOPS across clients | Product performance specifications for cached shared disks | Not as published. No public workload parameters, runner, environment, or raw samples were found. |

ComputeSDK is the closest existing cross-provider harness. Its current source
contains adapters for E2B, Modal, Daytona, Runloop, and Archil, but its sandbox
tests exercise startup or a macro development workload. Its object-storage
benchmark and snapshot/fork benchmark operate on buckets and objects rather
than mounted sandbox volumes.

## Recommended benchmark layers

The unified suite should report a vector of results. It should not produce one
composite "fastest sandbox" score.

| Layer | Standard | Primary outputs |
| --- | --- | --- |
| Control plane | ComputeSDK TTI, unchanged | median, P95, P99, success rate, first-ready latency, and burst wall clock |
| Macro development workload | ComputeSDK DAX, unchanged | prepare, download, clone, install, typecheck, total time, disk footprint, and success rate |
| Mounted volume data plane | Sandbox Volume Benchmark v1 below | operation throughput, latency percentiles, correctness, durability, and reattach latency |

This separation matters because a fast sandbox start says nothing about mounted
filesystem metadata performance, while a fast warm read says nothing about
durable persistence after sandbox destruction.

## Sandbox Volume Benchmark v1

### Eligibility

A result belongs in the primary volume table only when the product provides a
mutable filesystem mount whose data:

- is independent of one sandbox's lifecycle;
- can be attached to a newly created sandbox;
- exposes ordinary file operations at a mount path;
- has documented persistence or commit semantics.

Runloop's persistent devbox disk or a sandbox filesystem snapshot can be tested
in a separate persistent-rootfs class, but it must not be labeled as the same
resource type as an independent shared volume.

### V0: persistence and correctness gate

This is a mandatory pass/fail gate:

1. Create a new volume.
2. Create a producer sandbox with that volume mounted.
3. Write the deterministic 30,000 x 4 KiB dataset and record a manifest of
   file paths, lengths, and SHA-256 hashes.
4. Invoke the provider's documented persistence boundary. This may be
   filesystem `sync`, a volume commit API, sandbox shutdown, or a documented
   combination.
5. Destroy the producer sandbox.
6. Create a new consumer sandbox and attach the same volume.
7. read and hash every file, then compare the complete manifest.
8. Delete the consumer sandbox and volume.

Record volume-create, first-attach, persistence, reattach, first-read, complete
verification, and cleanup timings. A provider with any missing or corrupt file
fails the gate and receives no throughput rank.

This provider-specific lifecycle wrapper is the only part of the suite that
should vary by provider.

The Sandbox0 implementation is
[`online_volume_reattach_bench.py`](./online_volume_reattach_bench.py).

### V1: pinned mdtest 4 KiB profile

The common command, after substituting only the mount path, is:

```text
mdtest -d TARGET/sandbox-volume-small-files-4k-v1 \
  -F -n 30000 -w 4096 -e 4096 -u -Y
```

The runner pins IOR commit
[`967a9f65109760db8a3ac14a7fdd007f337d2960`](https://github.com/hpc/ior/tree/967a9f65109760db8a3ac14a7fdd007f337d2960),
which is the IOR 4.0.0 release.

Semantics:

- one mdtest task;
- 30,000 files;
- write 4,096 bytes to every file;
- read 4,096 bytes from every file;
- file creation, stat, read, and removal rates;
- `-Y` calls the backend `sync` operation after each phase, includes it in the
  phase timing, and flushes node I/O;
- three timed invocations, alternating target order when a local diagnostic
  baseline is present;
- no guest cache drop, because that cannot be performed consistently across
  managed sandbox products.

Dependency installation and compilation are outside the timed invocations.
Each result must record the exact IOR commit, reported mdtest version, command,
raw per-run rates, environment, mount type, and cache policy.

mdtest confirms operation completion and byte counts, but this profile does not
compare the payload read with the payload written. The V0 and V2 correctness
checks are therefore mandatory companions.

### V2: verified parallel small-file profile

Each target and round performs:

1. create 30,000 files, distributed at 128 files per directory;
2. write a deterministic 4 KiB payload to every file with eight worker threads;
3. immediately read and compare the complete payload of every file;
4. recursively list the tree and stat every file;
5. remove the tree.

Run three rounds and report:

- write, verified-read, and list-plus-stat operations/s;
- write, read, and stat P50/P95 latency;
- files attempted, files verified, and all errors;
- effective payload MiB/s, clearly labeled as a small-file result.

The implementation is
[`online_volume_small_file_bench.py`](./online_volume_small_file_bench.py).

### V3: optional large-file profile

Large-file streaming is useful, but it is a different workload. If included,
pin fio 3.42 and use a portable buffered POSIX job with end-of-job sync. Direct
I/O, asynchronous engines, and queue depths should only be added as separate
rows after verifying that every filesystem supports them with the same
semantics.

Do not use the large-file result as the headline for an agent workspace that
is dominated by repository files, package trees, and caches.

### H0: historical bridge only

The exact historical JuiceFS/EFS profile remains available:

```text
mdtest -d TARGET/mdtest -b 6 -I 8 -z 4
```

It creates 12,440 zero-byte items, performs no explicit sync or fsync, and does
not verify content. Its value is that JuiceFS published results for the same
visible arguments. It must remain a historical reference, not a current
provider ranking.

See
[`VOLUME_MDTEST_BENCHMARK.md`](./VOLUME_MDTEST_BENCHMARK.md)
for the exact workload, environment differences, S0FS result, and historical
JuiceFS/EFS table.

## Fair-run controls

Every provider run must disclose and, where possible, hold constant:

- benchmark source commit and script checksum;
- sandbox image digest and operating system;
- requested and runtime-visible CPU and memory;
- provider, region, runner location, and timestamp;
- volume product/version, access mode, mount options, and storage class;
- file count, size, directory layout, task/thread count, and rounds;
- whether the volume is fresh, warm, reattached, committed, or restored;
- page-cache policy and whether cache dropping was available;
- success rate, timeouts, retries, and rate-limit responses;
- all persistence calls performed before producer teardown.

Use the closest common CPU/memory shape. If a provider cannot supply that shape,
publish the actual shape and keep the result in a visibly separate row.

For a public comparison, use at least three repetitions per data-plane profile
and the ComputeSDK-prescribed 100 samples for each TTI mode. Publish median and
range for data-plane throughput, and median/P95/P99 plus success rate for
control-plane latency. Do not trim failures.

## Online Sandbox0 result

### Environment

| Field | Value |
| --- | --- |
| Team | `volume-benchmark` |
| Team ID | `f64c6e02-35a2-4b09-a37b-7e2745ec6ce8` |
| Region | `ali-ue1` |
| Cluster | `terway` |
| Template | public `default` |
| Image | `sandbox0ai/otemplates@sha256:275c63e8b4f508afc8f99f3ab377b45ae6037b27fef33c4ba05a0f61c54c6e38` |
| Declared resources | 2 GiB memory, 8 GiB ephemeral storage |
| Runtime | Ubuntu 24.04.4, Linux `4.19.0-gvisor`, x86-64 |
| Visible CPUs | 2 |
| Runtime-visible memory | 128 MiB |
| Local diagnostic target | `/tmp`, tmpfs |
| Volume target | `/workspace`, 9p inside the gVisor sandbox |
| Cache control | None |
| Timed runs | 3 per target |

The declared and runtime-visible memory values are both reported because they
describe different control-plane and guest views.

OpenMPI singleton startup under gVisor required `PMIX_MCA_gds=hash`. This
changes the PMIx datastore and does not change the filesystem workload.

### V0 persistence and correctness result

The producer wrote a deterministic 30,000 x 4 KiB dataset, closed every file,
called `os.sync()`, and was destroyed. A fresh consumer sandbox then mounted
the same volume and compared every complete payload.

| Check or timing | Result |
| --- | ---: |
| Files written | 30,000 |
| Bytes written | 122,880,000 |
| Producer write throughput | 924.420 files/s |
| Producer write P50 / P95 | 8.409 / 14.493 ms |
| Producer create with volume | 0.701 s |
| Producer deletion | 0.526 s |
| Consumer create with reattached volume | 3.361 s, two attempts |
| First verified 4 KiB read after reattach | 28.717 ms |
| Files observed | 30,000 |
| Files fully verified | 30,000 |
| Consumer verified-read throughput | 1,342.188 files/s |
| Consumer read P50 / P95 | 1.754 / 53.251 ms |
| Missing, corrupt, or unexpected files | 0 |
| Aggregate dataset SHA-256 | `db90d53e32e98768e7804cbe1402256df527219ba2201bf06cf45eff90daa278` |
| Overall gate | PASS |

The producer and consumer ran in separate sandboxes,
`rs-orsxe53bpe-default-z5f9x` and `rs-orsxe53bpe-default-pjmzg`, on cluster
`terway`. The temporary volume was
`59241fac-765c-4368-8968-2c239f88c514`.

The raw aggregate JSON was written to
`/tmp/sandbox0-volume-reattach-30000x4k-p8.json` with SHA-256
`6757b72719595ebc7bac6427568c486a06887913bac955d62088a77cd2876b4b`.
Both sandboxes and the volume were deleted successfully.

This is one lifecycle correctness run, not a statistically meaningful
availability result. A public reliability claim should repeat the full V0
sequence and report its success rate.

### V1 mdtest result

Values are operations per second. Each cell shows the median and three-run
range.

| Operation | Pod-local `/tmp` | S0FS `/workspace` | S0FS / local |
| --- | ---: | ---: | ---: |
| File creation | 25,222.716 (25,195.580-25,507.818) | 343.044 (342.661-345.031) | 1.36% |
| File stat | 60,613.899 (60,502.041-61,064.608) | 8,095.711 (7,962.965-8,272.031) | 13.36% |
| File read | 26,895.069 (25,164.510-26,996.992) | 3,213.392 (3,188.447-3,218.922) | 11.95% |
| File removal | 29,805.426 (27,333.009-29,918.383) | 2,204.971 (2,195.374-2,223.602) | 7.40% |

All six mdtest invocations:

- exited successfully;
- reported `mdtest-4.0.0`;
- reported one task and 30,000 files;
- emitted all four required operation rates.

The low S0FS file-creation rate includes the cost of the phase-level `sync`.
It must not be directly compared with the V2 eight-worker write rate, which
does not request an explicit sync.

The raw aggregate JSON was written to
`/tmp/sandbox0-volume-mdtest-4k-v1-r3.json` with SHA-256
`674ba770fbd2e938149dd1780919d76e6ce1456dd3b5ef2196aff5ef832b9b96`.

The run used sandbox `rs-orsxe53bpe-default-m6lvp` and volume
`13df4f09-113b-48f1-a247-858248334166`. Both were deleted successfully. The
retained benchmark team contains no sandbox or volume after cleanup.

### V2 verified result

The earlier online V2 run produced:

| Phase | Pod-local `/tmp` | S0FS `/workspace` | S0FS / local |
| --- | ---: | ---: | ---: |
| Write | 3,302 ops/s | 903 ops/s | 27.3% |
| Read and verify | 3,744 ops/s | 1,566 ops/s | 41.8% |
| List plus stat | 49,001 ops/s | 7,681 ops/s | 15.7% |

All 90,000 S0FS reads returned the expected 4 KiB content. See
[`VOLUME_SMALL_FILE_BENCHMARK.md`](./VOLUME_SMALL_FILE_BENCHMARK.md)
for latency percentiles, per-round values, and limitations.

## Reproduction

Run the V0 persistence and correctness gate:

```sh
python3 scripts/online_volume_reattach_bench.py \
  --team-id f64c6e02-35a2-4b09-a37b-7e2745ec6ce8 \
  --file-count 30000 \
  --file-size 4096 \
  --parallelism 8 \
  --files-per-dir 128 \
  --json-output /tmp/sandbox0-volume-reattach-30000x4k-p8.json
```

Run the current V1 profile against the dedicated online team:

```sh
python3 scripts/online_volume_mdtest_bench.py \
  --team-id f64c6e02-35a2-4b09-a37b-7e2745ec6ce8 \
  --workload-profile sandbox-volume-small-files-4k-v1 \
  --rounds 3 \
  --json-output /tmp/sandbox0-volume-mdtest-4k-v1-r3.json
```

Run the content-verified V2 profile:

```sh
python3 scripts/online_volume_small_file_bench.py \
  --team-id f64c6e02-35a2-4b09-a37b-7e2745ec6ce8 \
  --file-count 30000 \
  --file-size 4096 \
  --parallelism 8 \
  --files-per-dir 128 \
  --rounds 3 \
  --json-output /tmp/sandbox0-volume-small-file-30000x4k-p8-r3.json
```

The runners write structured JSON and remove temporary resources unless
`--keep-resources` is explicitly supplied. V1 and V2 run the local and S0FS
targets inside the same sandbox. V0 intentionally uses two different
sandboxes connected to the same volume.

## What is needed for a direct provider comparison

The benchmark workload itself is now provider-neutral once a mount path exists.
The remaining work is lifecycle adapters and credentials:

- E2B private-beta volume access and an API key;
- Modal credentials and an explicit choice of Volume v1 or v2;
- Daytona API credentials;
- an Archil disk, backing bucket, region, and credentials;
- a classification decision for Runloop, because its documented persistent
  devbox disk is not an independent shared volume;
- one common resource class and region policy.

For each eligible provider, the adapter should create the volume and sandbox,
return the mount path, execute the same V0/V1/V2 payload, perform the
provider-documented persistence action, attach the volume to a fresh sandbox,
verify it, and clean up.

Until those direct runs exist, vendor specifications and historical JuiceFS or
EFS values should be shown as contextual references, never as current rows in
the same leaderboard.

## Primary sources

- [ComputeSDK benchmark methodology](https://github.com/computesdk/benchmarks/blob/51e918c64877913981e632b58f55eec8ca1fb1e2/METHODOLOGY.md)
- [ComputeSDK DAX runner](https://github.com/computesdk/benchmarks/blob/51e918c64877913981e632b58f55eec8ca1fb1e2/benchmarks/scripts/dax-benchmark.sh)
- [ComputeSDK object-storage runner](https://github.com/computesdk/benchmarks/blob/51e918c64877913981e632b58f55eec8ca1fb1e2/benchmarks/storage/benchmark.ts)
- [E2B Volumes](https://e2b.dev/docs/volumes)
- [E2B huge-pages experiment](https://e2b.dev/blog/up-to-5x-faster-sandboxes)
- [Modal Sandbox launch and SWE-bench demonstration](https://modal.com/blog/sandbox-launch)
- [Modal million-sandbox scale test](https://modal.com/blog/scaling-to-1-million-concurrent-sandboxes-in-seconds)
- [Modal Volumes](https://modal.com/docs/guide/volumes)
- [Daytona RL infrastructure benchmark](https://www.daytona.io/dotfiles/the-hidden-infrastructure-tax-in-coding-agent-rl)
- [Daytona Volumes](https://www.daytona.io/docs/volumes/)
- [Runloop public benchmarks](https://docs.runloop.ai/docs/benchmarks/public-benchmarks)
- [Runloop devbox lifecycle](https://docs.runloop.ai/docs/devboxes/lifecycle)
- [Archil performance specifications](https://docs.archil.com/details/performance)
- [IOR 4.0.0 release](https://github.com/hpc/ior/releases/tag/4.0.0)
- [Current JuiceFS benchmark methodology](https://github.com/juicedata/juicefs/blob/main/docs/en/benchmark/metadata_engines_benchmark.md)
- [Historical JuiceFS/EFS mdtest comparison](https://juicefs.com/docs/community/mdtest/)
