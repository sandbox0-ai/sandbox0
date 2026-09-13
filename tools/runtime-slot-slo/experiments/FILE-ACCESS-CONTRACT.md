# D-FILE-ACCESS-CONTRACT: exclusive is not a qualified configuration-only switch

2026-09-13. Evidence: `/tmp/sandbox0-file-access.Nm9nMz/evidence.json`.
Retained remote fixture: `/data/sandbox0-file-access-Nm9nMz`.

## Question and boundary

Could stock runsc's exclusive file access remove shared-mode metadata
revalidation without changing the existing host-XFS checkpoint semantics?
Current defaults use shared access with DirectFS. Before any latency comparison,
test whether guest-visible state is represented at the host freeze boundary.
This is not prefetch, tenant prewarming or a cold-start acceptance run.

The exact runsc release is `release-20260817.0`, binary SHA256
`048b89aada69dc3333422e139d6e9d02f8ab06bda52398060e0fbdacca00074c`.
The original installed runsc remains unchanged. The test uses Ubuntu kernel
6.8.0-124-generic and the original 2CPU/8GiB machine, not the vendor production
kernel or production-width capacity.

The fixture is a 512MiB synthetic XFS image with separate same-filesystem
OverlayFS roots for shared and exclusive. Keep DirectFS, systrap, overlay2=none,
noatime host mounts and a shared control bind outside the frozen filesystem.
A static guest probe reads one file and modifies another, then reports state.
Host freezes XFS, reads both files and thaws. Repeat after successful guest
file fsync. The second guest observation is stat-only, so it does not dirty
atime again by reading payload. There is no procd, NBD, S3 or user RootFS.

## Observation

| Root file access | Payload | Host versus guest metadata at freeze |
| --- | --- | --- |
| shared, before/after fsync | Matches | Size, mode, atime, mtime and ctime match |
| exclusive, before/after fsync | Matches | Both atimes differ; modified-file mtime/ctime differ by 1,589,005ns |

Both guest arms exit0 and all four freeze/thaw observations succeed. Eight
host file observations match the corresponding initial guest payload hashes.
After-fsync guest reports intentionally have no fresh payload hash. Fsync did
not eliminate the observed exclusive-mode metadata differences.

Reject an unqualified configuration-only adoption, not exclusive mode forever.
No payload loss, failed real S3 snapshot/resume, universal nanosecond timestamp
SLA or performance effect is established. Public snapshot documentation calls
the operation a point-in-time filesystem copy, and rebase explicitly preserves
mtime (`pkg/rootfsrebase/apply_linux.go`); a semantic change needs a deliberate
contract and integration qualification, not only a faster claim sample.

The inspected exact upstream source maps shared mode to remote revalidation,
while exclusive mode can retain authoritative metadata. Native-FD DirectFS
read/write paths also exist: generic caching comments do not prove payload loss
in this fixture. Sources: [filesystem guide](https://gvisor.dev/docs/user_guide/filesystem/),
[mount setup](https://github.com/google/gvisor/blob/50e1502a95d36ad2faf2c7ef33b8bf21fe975293/runsc/boot/vfs.go),
[timestamp implementation](https://github.com/google/gvisor/blob/50e1502a95d36ad2faf2c7ef33b8bf21fe975293/pkg/sentry/fsimpl/gofer/time.go).

## Retained failure and cleanup

The systemd unit fails with exit1 at the runner's final `owned mounts remain`
substring guard. Its pre-exit mountinfo was not captured, so the exact matching
line is unknown. Do not call this definitely a false positive or rewrite it as
a passing runner. Both runsc deletions, all five unmounts and loop detach exit0.
A later independent check scans 226 mount tables and finds no owned mounts,
loops or processes. Original binaries/process identities, Nomad job and database
row count remain unchanged; all64 NBD devices are detached.

Retain the warning that RuntimeMaxSec has no effect for Type=oneshot; no unit-
level 120s bound is established. Guest coordination and host report waits have
their own deadlines. Also retain initial upstream404 downloads caused by using
an annotated tag object instead of its peeled commit; selected files were then
downloaded at the exact corrected commit. Neither arm is rerun.

All59 exported reports/configs/logs pass byte/hash verification. The current
1361-file product inventory is unchanged. Preserve the detached fixture image
remotely, close the owned SSH master, and stop compute through the remote-test
lifecycle; independently confirm Stopped/StopCharging on the original shape.

Zero regional claims, node-v commands or SLO samples. No default, cache, timeout,
machine, production rollout, merge or tag change. Existing cold/cached-new,
claim/readiness/command/combined and populated-root/density gates remain open.
