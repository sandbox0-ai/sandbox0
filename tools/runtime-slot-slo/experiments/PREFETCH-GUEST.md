# D-PREFETCH-GUEST: actual stock guest reads and the noatime boundary

2026-09-13. Evidence: `/tmp/sandbox0-prefetch-guest.uSVgcF`; remote diagnostics:
`/data/sandbox0-prefetch-guest-uSVgcF`. Continue the unchanged bounded prototype
from [EXEC-PREFETCH.md](EXEC-PREFETCH.md); no runtime default/product source edits.
Original regional1s/2s, literal first-command, cold/cached-new, occupied-width,
populated-root and stateless-node requirements remain unachieved.

## Result that changes the next action

Stock runsc can execute the prototype's pinned-file opening and real parallel
reads. However, the guest O_NOATIME flag alone does NOT preserve atime on the
tested ordinary host mount: both guest-visible and host file timestamps change.
Do not silently call that safe, and do not interpret successful open/read returns
as proof of no metadata writes.

The actual product already mounts XFS and merged OverlayFS with MS_NOATIME:
`pkg/rootfssession/runtime_linux.go`, `xfsMountFlags` and `MountOverlay`. Repeat
with the SAME guest binary, same flags/test and data, adding only a private
host-bind noatime mount. It passes; both guest and host atime remain unchanged.
No implementation flag is removed, no production mount/configuration is changed.
This closes the specific runtime-opening gate UNDER that mount precondition,
not general safety for arbitrary user-created mounts or executable paths.

| Guest-generated8MiB ELF fixture | Ordinary host mount | Private noatime mount |
| --- | ---: | ---: |
| Same prototype/probe executable | yes | yes |
| Initialized payload plan, bytes | 8,388,608 | 8,388,608 |
| Completed ReadAt bytes incl. headers | 8,388,728 | 8,388,728 |
| ReadAt calls | 10 | 10 |
| Verified actual sample bytes | yes | yes |
| Guest and host atime preserved | no | yes |
| Compatibility test | fail | pass |

The fixture was written INSIDE each guest immediately before reading. It is
intentionally warm, has no S3 backend, and is not an imported tenant RootFS.
The small recorded stage durations are NOT cold-start, command or claim timings.
No regional latency sample, new sandbox API claim, real Node command or speedup
is claimed. Network isolation is none and cgroups are bounded by the owning
systemd unit; this is not full carrier/security/network-policy parity or width.

## Exact execution and failure handling

Use official stock release-20260817.0, SHA256
`048b89aada69dc3333422e139d6e9d02f8ab06bda52398060e0fbdacca00074c`,
with systrap/shared/DirectFS=true/overlay2=none, matching the file-access class.
The unchanged six-file prototype is linked into a linux/amd64 Go1.25.5 guest
test binary, SHA256
`ecc2068de353718980bf3e95bda7b2412a75d33fa8dbe2bd1f5620cdde21ca6b`.
The only additional test creates a bounded patterned ELF, verifies a real sample,
runs the existing public opt-in hook, checks exact counters/closed ownership,
checks atime, and checks the off result. It does not replace the read engine.

Both runs use fresh isolated directories, stock-runsc state roots, private mount
namespaces, and separate systemd units bounded to180s/768MiB/no swap/512 tasks/
100% CPU. The machine remains original2CPU8GiB, not production-width. Ordinary
run fails its atime assertion; noatime run passes the identical assertion.

Two harness failures are preserved separately from runtime behavior:

- `--help` is not the full flag listing. An initial option check incorrectly
  rejected availability of ignore-cgroups; `runsc flags` establishes it exists.
  No guest was launched by that failed check.
- The ordinary run's teardown reporter expected an empty array from runsc list,
  but this pin returned JSON null. Its unit therefore fails AFTER saving the
  genuine guest-test failure. Subsequent read-only inspection confirms no owned
  processes/mounts remain. The new runner accepts only null or an empty array
  as an empty list, checks exact IDs otherwise, and preserves the old failure.
  No runtime is restarted because of an observation timeout or missing summary.

The host ordinary fixture has the same changed atime observed by the guest.
The noatime fixture retains Unix1600000000 on both sides. This supports a host
mount dependency, not an assertion that every gVisor O_NOATIME implementation
or filesystem behaves identically. The pinned Gofer PRead source corroborates
that shared mode reads through the host handle and avoids its non-shared cached
atime-update branch; it is not by itself a complete flag-propagation proof.
Primary [pinned Gofer source](https://github.com/google/gvisor/blob/50e1502a95d36ad2faf2c7ef33b8bf21fe975293/pkg/sentry/fsimpl/gofer/regular_file.go)
is retained with SHA256
`461044dc7a308cfaefee5b97c9361508dbecb3ceb178d7b8be1c73a9a0f7720c`.
Web fetch cache misses are not support evidence; the direct immutable download
succeeds and is recorded separately.

## Next action and limits; do not repeat this gate

Proceed to a controlled actual S3-backed RootFS on/off trial, verifying its
exact noatime mount precondition, procd content and runtime generation. Use
the SAME experimental procd and per-image RootFS for on/off. Do not substitute
a wrapper command or an unaccounted bind-injected procd: literal node-v and
the regional-to-procd boundary stay intact. Keep total pre-exec waiting and
physically unfinished reads in time/resource/cleanup accounting.

Prepare candidate templates through the current durable importer, preserving
format2, the qualified file-range/grouping policy, OCI source and procd protocol,
changing only the injected procd build. Existing importer scripts explain this
path but have stale database/source-count/operation guards; do not replay them.
Use an owned operation/prefix and fresh authority/capacity checks. Inspect the
qualified staging filesystem's actual free capacity before importing; do not
delete historical data or expand hardware as an alleged startup improvement.
No import, database clone, object publication or candidate installation happens
in this turn. Do not repeat old carrier recovery or file-coordinate scans.

Before any default promotion, optional reads outside verified noatime-backed
RootFS still require safe handling. That unresolved generality is not permission
to change process/RootFS semantics; nor does it prevent a clearly scoped causal
trial of the actual Node/Coding RootFS under its established mount contract.
Current-pin regional cold combined maxima2.221/2.668s remain misses. No present
result excludes S3 response waiting or machine contention under real occupancy.

## Cleanup and preservation

Final read-only verification covers both exact units: MainPID0, no control group,
empty exact runsc roots, no owned processes, no owned host mounts, all64NBDs
detached. The first failed unit is preserved as failed; the second is inactive/
success/exit0. No material diagnostic data is deleted; both fixture files,
source, executable, configuration, logs and failure records remain on/data.

Original processes/configuration/binaries, job72102,263 original and2072 formal
sandbox rows are unchanged. Old baseline ready1 is reported, not repaired or
timed. All1359 current product files and38 sealed prototype files are verified.
SSH is explicitly closed and its handle consumed. Per the remote testing skill,
compute is stopped; the fresh CLI receipt records the resulting instance state.
No Kind bootstrap, local e2e, production mutation, merge, tag or timeout increase.
