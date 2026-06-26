# S0FS POSIX/FUSE suite runner

Issue 611 tracks test-suite selection and integration for S0FS. The goal is to expose compatibility, integrity, recovery, and performance issues with trusted suites and Sandbox0-specific workloads. This directory intentionally does not fix S0FS behavior.

## Suite selection

The selection is recorded in `s0fs_suite_catalog.json`.

- `smoke`: deterministic in-repo POSIX-style operations for quick regressions.
- `git-integrity`: git clone, pack verification, fsck, and working-tree digest parity against pod-local overlay storage.
- `bulk-smallfile-integrity`: deterministic large small-file tree workload, defaulting to 30000 files totaling 500 MiB.
- `archive-copy-rsync`: generated tree plus `tar`, `cp -a`, and `rsync -a` digest parity checks.
- `pjdfstest`: external POSIX syscall suite and the first standard conformance baseline.
- `fsx` / `fsstress`: external filesystem stress workloads for data-integrity and namespace pressure.
- `xfstests-generic`: broader fstests coverage, starting with safe FUSE-compatible subsets.
- `fio` / `mdtest` / `filebench`: benchmark candidates used as trend signals after correctness is stable.

`s0fs_known_failures.json` is the initial classification file. It is a reviewed failure matrix, not a mechanism to hide regressions.

## Runner

Use `scripts/s0fs_posix_suite.py` against a deployed Sandbox0 cluster. The runner creates a temporary template with a mounted RWO `SandboxVolume`, claims a sandbox, runs selected suites inside the sandbox pod, writes structured JSON results, and removes temporary resources unless `--keep-resources` is set.

Quick smoke run:

```sh
python3 scripts/s0fs_posix_suite.py \
  --suite smoke \
  --suite git-integrity \
  --json-output /tmp/s0fs-posix-results.json
```

Manual pjdfstest run:

```sh
python3 scripts/s0fs_posix_suite.py \
  --suite pjdfstest \
  --install-deps \
  --json-output /tmp/s0fs-pjdfstest-results.json
```

Manual fsx/fsstress run:

```sh
python3 scripts/s0fs_posix_suite.py \
  --suite fsx \
  --suite fsstress \
  --install-deps \
  --fsx-operations 1000 \
  --fsstress-operations 1000 \
  --json-output /tmp/s0fs-fsx-fsstress-results.json
```

When `fsx` or `fsstress` is selected with `--install-deps`, the runner builds the pinned xfstests `ltp/fsx` and `ltp/fsstress` tools inside the sandbox if the binaries are not already available. When `mdtest` is selected with `--install-deps`, the runner first tries the distro `ior` package, then builds pinned IOR 4.0.0 from source if the package is unavailable.

By default, the `fsx` runner disables fallocate, clone range, dedupe range, and exchange range operations. That keeps the first `fsx` profile focused on basic file data integrity for S0FS. Use `--fsx-include-extended-ops` only when intentionally probing those non-core operations.

Large small-file run matching the issue-style workload:

```sh
python3 scripts/s0fs_posix_suite.py \
  --suite bulk-smallfile-integrity \
  --bulk-file-count 30000 \
  --bulk-total-bytes 524288000 \
  --bulk-concurrency 8 \
  --json-output /tmp/s0fs-bulk-smallfile-results.json
```

Archive/copy/rsync tree run:

```sh
python3 scripts/s0fs_posix_suite.py \
  --suite archive-copy-rsync \
  --archive-file-count 3000 \
  --archive-total-bytes 52428800 \
  --json-output /tmp/s0fs-archive-copy-rsync-results.json
```

Fio and mdtest benchmark run:

```sh
python3 scripts/s0fs_posix_suite.py \
  --suite fio \
  --suite mdtest \
  --install-deps \
  --fio-size 128m \
  --mdtest-files 10000 \
  --json-output /tmp/s0fs-fio-mdtest-results.json
```

Filebench smoke run:

```sh
python3 scripts/s0fs_posix_suite.py \
  --suite filebench \
  --install-deps \
  --filebench-files 10000 \
  --filebench-file-size 4k \
  --json-output /tmp/s0fs-filebench-results.json
```

The default quick suites compare pod-local overlay storage with mounted S0FS. Heavy external suites are intended for manual or nightly workflows until runtime and failure categories are understood.

## First remote run

The initial issue 611 remote-kind run produced:

- `smoke` and `git-integrity`: passed on mounted S0FS and matched the pod-local overlay baseline.
- `pjdfstest` at `ededbeb2b44929972898afb87474b0937f78a877`: pod-local overlay passed 238 files / 8798 tests; mounted S0FS completed the same suite and failed. The captured tail includes failures in `rename`, `rmdir`, `symlink`, `truncate`, `unlink`, and `utimensat`.
- `fsx` / `fsstress` from xfstests at `cac9fe2b8dc3e6dbc0c2383b9e3b4d3c1b9e7dd0`: the runner built `ltp/fsx` and `ltp/fsstress` in the sandbox. The default S0FS basic profile passed `fsx` 1000 operations and `fsstress` 1000 operations / 4 processes. An explicit extended-ops `fsx` profile hit unsupported `XFS_IOC_EXCHANGE_RANGE`.
- `archive-copy-rsync`: a 100-file / 1 MiB smoke profile passed on pod-local overlay and mounted S0FS.
- `fio`: a 16 MiB write/read smoke profile passed on pod-local overlay and mounted S0FS.
- `mdtest`: the distro `ior` package was unavailable in the sandbox image, so the runner built pinned IOR 4.0.0 from source. A 200-file smoke profile passed on pod-local overlay and mounted S0FS.
- `bulk-smallfile-integrity`: a 200-file / 2 MiB smoke profile passed on both targets. The 30000-file / 500 MiB profile passed on pod-local overlay, but failed on mounted S0FS: 29963 files were written, 37 write operations returned `FileNotFoundError`, and verification saw 4430 missing files. The first sampled missing files were under `d008/...`.
- `filebench`: the distro `filebench` package was unavailable, so the runner built and installed pinned Filebench from source into a private prefix. A 200-file create/stat smoke profile aborted with Filebench's own buffer-overflow failure on both pod-local overlay and mounted S0FS, so it is recorded as a harness/runtime limitation rather than S0FS evidence.

Those failures are tracked in `s0fs_known_failures.json` as untriaged evidence. They are not skipped by the runner and are not part of issue 611's fix scope.
