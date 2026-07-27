# Online S0FS mdtest 3.4 Benchmark

Date: 2026-07-27

## Executive conclusion

The historical JuiceFS metadata workload can run directly against an online
Sandbox0 volume. The reproducible runner is
[`online_volume_mdtest_bench.py`](./online_volume_mdtest_bench.py).

On the production `ali-ue1` deployment, the three-run S0FS medians were:

- 3,264.706 zero-byte file creates/s;
- 7,623.840 file stats/s;
- 4,382.087 zero-byte file open-close operations/s, reported by mdtest as
  `File read`;
- 2,993.224 file removals/s.

Against the historical numbers published by JuiceFS, the S0FS medians were
1.26-7.47 times the published JuiceFS rates and 6.08-39.27 times the published
Amazon EFS rates across the nine reported operations.

This is a valid same-workload historical comparison, but it is not a current
same-environment provider benchmark. The JuiceFS/EFS results were already
present in January 2021, used a different machine and operating system, did
not identify the exact IOR commit, and did not disclose important EFS
provisioning and cache details. The defensible wording is:

> S0FS exceeded the historical JuiceFS-published mdtest 3.4 results under the
> same workload geometry.

It is not defensible to claim from this run alone that S0FS is faster than a
current, correctly provisioned EFS or JuiceFS deployment.

## What mdtest 3.4 is

`mdtest` is the metadata benchmark distributed by the
[IOR project](https://github.com/hpc/ior). It measures filesystem namespace
operations such as directory and file creation, stat, open, and removal. It
can use MPI to coordinate multiple tasks, although this profile uses only one
task.

There was no tagged IOR `3.4.0` release. After IOR 3.3.0, the main development
branch identified itself as `3.4.0+dev`; the next tagged major release was
4.0.0. The JuiceFS output likewise reports `mdtest-3.4.0+dev`.

The runner pins IOR commit
[`d339caa501a146449a45ab876079dc37f513fc43`](https://github.com/hpc/ior/tree/d339caa501a146449a45ab876079dc37f513fc43).
This was the last upstream IOR commit before the
[first public JuiceFS documentation commit](https://github.com/juicedata/juicefs/blob/d23762a7e926f896a3dd615677882b8e98ed29df/docs/mdtest.md)
on 2021-01-08, and it reports version `3.4.0+dev`. JuiceFS did not publish its
exact IOR commit, so this is a period-compatible pin rather than proof of an
identical binary.

## Exact workload

Every timed invocation uses the published JuiceFS/EFS arguments without
adding mdtest workload flags:

```text
mdtest -d TARGET/mdtest -b 6 -I 8 -z 4
```

The arguments mean:

- `-b 6`: each directory-tree node has a branching factor of six;
- `-z 4`: the hierarchy has depth four;
- `-I 8`: mdtest operates on eight items at every tree node;
- `-d`: select the target filesystem path.

The tree contains:

```text
1 + 6 + 6^2 + 6^3 + 6^4 = 1,555 tree nodes
1,555 nodes * 8 items = 12,440 items per file/directory phase
```

Each invocation uses one task and one iteration. It reports:

- directory creation, stat, and removal;
- file creation, stat, read, and removal;
- tree creation and removal.

The default write and read sizes are both zero. File creation therefore
creates and closes empty files. `File read` only opens and closes empty files;
it does not read or verify file content. The command does not request `fsync`
and the benchmark does not drop caches.

This workload must not replace the separate
[30,000 x 4 KiB verified small-file benchmark](./VOLUME_SMALL_FILE_BENCHMARK.md),
which measures actual payload writes and content-verified reads.

## Online run environment

| Field | Value |
| --- | --- |
| Team | `volume-benchmark` |
| Team ID | `f64c6e02-35a2-4b09-a37b-7e2745ec6ce8` |
| Region | `ali-ue1` |
| Cluster | `terway` |
| Template | public `default` |
| Image | `sandbox0ai/otemplates@sha256:275c63e8b4f508afc8f99f3ab377b45ae6037b27fef33c4ba05a0f61c54c6e38` |
| Template resources | 2 GiB memory, 8 GiB ephemeral storage |
| Runtime view | Ubuntu 24.04.4, `4.19.0-gvisor`, x86-64 |
| Visible CPUs | 2 |
| `/proc/meminfo` visible memory | 128 MiB |
| Pod-local target | `/tmp`, tmpfs |
| Volume target | `/workspace`, 9p inside the gVisor sandbox |
| S0FS access mode | RWO |
| Runs | 3 per target, alternating target order |
| Cache control | None |

The template declares 2 GiB, while `/proc/meminfo` exposed 128 MiB to this
process. Both values are recorded because they describe different control
plane and runtime views.

Direct OpenMPI singleton startup under gVisor failed with a PMIx shared-memory
mapping error. The successful run set:

```text
PMIX_MCA_gds=hash
```

This selects the PMIx hash datastore instead of its shared-memory datastore.
It does not change the mdtest arguments or filesystem operation sequence. The
override is recorded in every raw command result.

The successful run created:

- sandbox `rs-orsxe53bpe-default-b7pnc`;
- S0FS volume `7f4efef8-c19a-4814-a452-06e4b76ede99`.

Both resources were deleted successfully after the benchmark. The retained
team contains no benchmark sandbox or volume.

## Measured results

Values are operations per second. Each cell shows the median followed by the
three-run range.

| Operation | pod-local median (range) | S0FS median (range) | S0FS / local |
| --- | ---: | ---: | ---: |
| Directory creation | 62,872.917 (62,189.394-68,444.052) | 6,468.323 (6,457.969-6,675.388) | 0.103 |
| Directory stat | 61,909.174 (60,784.239-61,936.627) | 7,973.029 (7,959.727-8,082.917) | 0.129 |
| Directory removal | 61,622.154 (61,576.426-62,123.288) | 8,330.968 (8,181.079-8,381.197) | 0.135 |
| File creation | 40,532.058 (32,489.901-40,815.632) | 3,264.706 (3,241.212-3,289.803) | 0.081 |
| File stat | 61,212.695 (44,736.172-61,770.960) | 7,623.840 (7,612.402-7,685.679) | 0.125 |
| File read | 41,832.288 (41,272.503-55,845.841) | 4,382.087 (4,318.080-4,395.942) | 0.105 |
| File removal | 60,941.072 (44,568.236-62,004.187) | 2,993.224 (2,992.892-3,004.816) | 0.049 |
| Tree creation | 259,572.725 (252,936.874-261,045.673) | 7,379.352 (6,093.138-7,468.718) | 0.028 |
| Tree removal | 246,641.590 (18,848.030-253,587.364) | 7,138.821 (7,105.167-7,177.229) | 0.029 |

All six invocations:

- exited successfully;
- reported `mdtest-3.4.0+dev`;
- reported one task and 12,440 files/directories;
- emitted all nine expected operation rates.

The core S0FS file-operation results were stable across the three runs. The
pod-local tree-removal phase had one large outlier, which is why the report
uses medians rather than means.

## Historical JuiceFS and EFS comparison

The following external values come from the
[JuiceFS mdtest page](https://juicefs.com/docs/community/mdtest/). S0FS is the
three-run median from 2026-07-27; the published JuiceFS and EFS columns are
single historical runs.

| Operation | S0FS median | EFS published | S0FS / EFS | JuiceFS published | S0FS / JuiceFS |
| --- | ---: | ---: | ---: | ---: | ---: |
| Directory creation | 6,468.323 | 192.301 | 33.64x | 1,416.582 | 4.57x |
| Directory stat | 7,973.029 | 1,311.166 | 6.08x | 3,810.083 | 2.09x |
| Directory removal | 8,330.968 | 213.132 | 39.09x | 1,115.108 | 7.47x |
| File creation | 3,264.706 | 179.293 | 18.21x | 1,410.288 | 2.31x |
| File stat | 7,623.840 | 915.230 | 8.33x | 5,023.227 | 1.52x |
| File read | 4,382.087 | 371.012 | 11.81x | 3,487.947 | 1.26x |
| File removal | 2,993.224 | 217.498 | 13.76x | 1,163.371 | 2.57x |
| Tree creation | 7,379.352 | 187.906 | 39.27x | 1,503.004 | 4.91x |
| Tree removal | 7,138.821 | 218.357 | 32.69x | 1,119.806 | 6.38x |

The most relevant file-operation range is narrower than the full nine-row
range:

- versus historical EFS: 8.33-18.21x;
- versus historical JuiceFS: 1.26-2.57x.

The directory/tree ratios are larger, but tree creation/removal use a
different operation count from the 12,440 item phases and should not be
collapsed into one composite score.

## Why this is not a current direct provider leaderboard

The workload shape is closely matched:

- same published arguments;
- same reported mdtest version family;
- one task;
- 12,440 files/directories;
- zero-byte files.

The environments are not matched:

- JuiceFS used an EC2 `c5.large`, Ubuntu 18.04, and kernel 5.4;
- Sandbox0 used its current `ali-ue1` production runtime with gVisor;
- the JuiceFS page does not identify its exact IOR or JuiceFS commits;
- EFS size, performance mode, throughput mode, burst-credit state, and cache
  state are not disclosed;
- the historical page reports one run, while S0FS reports a three-run median;
- the test dates and cloud regions differ.

These differences can materially affect metadata throughput. Matching only
the command makes the workload comparable, not the storage services
interchangeable.

## Other published provider results

### Exact historical workload: usable as references

- The JuiceFS page publishes JuiceFS and Amazon EFS results with the same
  command and geometry. These are the only external values included in the
  main comparison table.
- Its S3FS run uses `-z 2`, producing only 344 files/directories, so it is not
  comparable to the `-z 4` S0FS run.

### Similar command, excluded from direct comparison

- [TigrisFS publishes the same visible arguments](https://github.com/tigrisdata/tigrisfs/blob/main/bench/README.md),
  but explicitly uses a modified `vitalif/mdtest` that performs directory
  `fsync` after phases. Its output also contains directory rename, which is
  absent from the pinned period-compatible IOR build. It is a related
  benchmark, not the same executable or semantics.
- The GeeseFS repository contains the same older table and the same modified
  mdtest caveat.
- [OCI File Storage with Lustre publishes mdtest results](https://docs.oracle.com/en-us/iaas/Content/Resources/Assets/whitepapers/running-benchmarks-on-oci-file-storage-with-lustre.pdf),
  but uses IOR 4.0.0, 16-32 clients, more than one million files, and
  different flags. Those rates cannot be mixed with this one-task profile.

No exact public mdtest result was found for current E2B Volumes, Modal Volumes,
Daytona Volumes, Runloop storage, ComputeSDK sandbox storage, or Archil. Their
documentation therefore cannot support a numeric row in this table.

## Interpretation

This result is strong evidence that the current S0FS hot metadata path handles
this zero-byte, one-task namespace workload well. It also aligns with the
separate small-file run: S0FS produced 7,623.840 file stats/s here and about
7,681 recursive list-plus-stat operations/s in the 30,000-file test.

It is not evidence about:

- 4 KiB or larger payload throughput;
- content integrity;
- `fsync` durability;
- cold reads or cache misses;
- reattach, snapshot, restore, or fork latency;
- multiple clients or concurrent writers;
- current EFS, JuiceFS, NFS, Archil, or sandbox-provider performance.

For a publishable current provider ranking, mount the other filesystem in the
same compute environment and run this exact pinned runner against both targets.
Until then, keep the historical comparison label in every chart and claim.

## Reproduction

From the `sandbox0` repository:

```bash
python3 scripts/online_volume_mdtest_bench.py \
  --team-id f64c6e02-35a2-4b09-a37b-7e2745ec6ce8 \
  --rounds 3 \
  --json-output /tmp/sandbox0-volume-mdtest-juicefs-public-r3.json
```

The successful raw result was written locally to:

```text
/tmp/sandbox0-volume-mdtest-juicefs-public-r3.json
```

Dependency installation and IOR compilation took most of the end-to-end
command time and are recorded separately from every mdtest invocation. The
actual median process time was about 2.1 seconds per pod-local invocation and
18.3 seconds per S0FS invocation.
