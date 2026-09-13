# D-POOL-BOOT: recovery failure before RootFS reads

2026-09-12. Read-only original-fixture diagnosis plus a local unit-level
persistence reproduction; not a startup optimization or acceptance result.

## Findings

The previous zero-ready pool has a concrete failure chain. The original job
`sandbox0-warm-slots` remains system/job index71527 with two generic carriers,
restart attempts0. No job, service, configuration or RootFS operation was made.

| UTC event | Observation |
| --- | --- |
| 15:10:39, previous boot | Both existing allocations fail recovery with `decode recovered task config: EOF`. |
| 15:10:40 | Attempts to start new task identities inside those same allocations fail registration with409 on the exact allocation unique constraint. Restart policy remains fail closed. |
| 15:10:44.080–44.350 | Nomad automatically garbage-collects both failed client allocations because disk usage96% exceeds its80% GC threshold. |
| 15:10:44.787–44.852 | PostgreSQL marks both slots terminal with `allocation_missing`; manager reports two completed reconciliations. |
| Previous boot's later storage trial | No ready carrier. Capacity is still reported; no sandbox claim was attempted. |
| 15:30:55, current ordinary boot | Node-update evaluation71563 places two new allocations at71564. |
| 15:30:57.949–58.011 | New allocation identities register against the new boot; two slots become ready without manual pool repair. |

Old allocations are `fb687415-d1db-862a-9614-215f9e293682` and
`f1dd1ea4-7d54-2a28-1508-8366805335bd`. Their replacements are
`27ec29a3-38c1-c4c3-df77-8d909ad76778` and
`54d5c658-1edb-3042-ce30-85d84eb8ad21`. The retained Nomad catalog has no
intervening alloc-stop evaluation after old node-update71540. Old allocations
still have desired=`run`, client=`failed`, now linked to these replacements.

## Current code reproduces the EOF independently

`nomad-driver-sandbox0/internal/driver/plugin.go:494` decodes driver configuration
from `handle.Config` during recovery. Nomad's `TaskConfig.rawDriverConfig` is
private. Its LocalState MsgPack persistence omits that field, although public
task identity and opaque `DriverState` survive. Current `PersistedState` does not
save the normalized driver configuration separately.

A temporary test overlay uses Nomad's actual LocalState type and the same
`structs.MsgpackHandle` codec used by boltdd Put/Get, then calls current product
`RecoverTask`. Three repetitions under the race detector all reproduce the exact
EOF. The existing direct-memory recovery/heartbeat test passes all three times:
it does not cross the persistence boundary. These diagnostic passes mean the
defect was reproduced, not that recovery is fixed. No daemon, guest, privileged
runtime or local e2e is launched.

Driver module dependency is Nomad1.11.1; the remote daemon is1.11.3. Both local
upstream source versions' persistence/restore paths were inspected and hashed.
Current candidate `plugin.go` equals origin/main exactly. Thus the EOF mechanism
is not merely an old-binary observation; the entire remote reboot/replacement
behavior has not yet been qualified with the candidate binaries.

The missing refill notification is consistent with the already implemented
carrier-refill fix, whose sealed evidence is retained at
`/tmp/sandbox0-carrier-refill.QE6r3a/evidence.json`. Candidate reconciliation
unconditionally obtains post-terminal scheduling acknowledgement before the slot
leaves the durable queue, even after physical client GC. Do not implement that
fix again or infer this read-only boot validates it. Config persistence is a
distinct newly reproduced defect.

## Machine/S3 boundary and next action

Read-only df confirms `/`, `/data` and `/var/lib/sandbox0` share the same ext4
filesystem: reported96% use,5,752,266,752 available bytes,14% inode use. Disk
pressure did trigger automatic GC. No ENOSPC, disk-latency measurement or causal
disk speedup is established. The EOF reproduction requires neither a full disk
nor S3. No new S3 request or machine performance benchmark was run.

Correct the initial pool-rebuild suggestion: D-REGIONAL-RUN's `pool-pin.rb`
only validates current-boot readiness and compatibility; it does not reconstruct
the pool after reboot. Its measured ready-carrier cold combined maxima remain
Node2.221s/Coding2.668s. The zero-pool observation does not explain those samples.

Next: a narrow versioned driver-state persistence regression/fix, preserving
exact task/allocation/boot/namespace identity and rejecting unsafe legacy state.
Then qualify actual candidate recovery/refill. Do not weaken uniqueness, allow
same-allocation reuse, change restart policy, default away missing security
configuration, prewarm user RootFS or increase timeouts. Do not repeat the
rejected size-only S3 sweep. All original cold/cached-new, populated-root/upper
history, occupied-width and1s/2s full-path gates remain open.

## Evidence and cleanup

- Frozen1353 product files and193 dirty entries preserved. Only experiment notes
  and temporary diagnostic sources changed; no product fix, commit, merge or tag.
- Initial collection failures are retained: multiline SQL JSON parsing, then
  journalctl rejecting dashed boot UUIDs. Later journal commands all succeed;
  both startup windows are untruncated. The intentionally600-row tail reaches its
  cap, so its collector receipt stays `complete=false`; do not erase that limit.
- Original263 sandbox rows, formal/source2072 rows, artifact identities, service
  binaries/configurations, job71527 and64 detached NBDs remain verified. Automatic
  new-boot slots/capacity/heartbeats are recorded separately from task mutations.
- Explicit SSH close succeeds; the owned master then terminates with255. Stop
  workflow completes; fresh15:50:54UTC CLI read confirms `Stopped/StopCharging`,
  original2CPU/8GiB and no public IP. No data was deleted.
- Evidence root: `/tmp/sandbox0-pool-boot.cr1zqD`; remote sanitized receipts:
  `/data/sandbox0-pool-boot-cr1zqD`. Evidence SHA256:
  `02234027c1d6b9c79e564781f177a250d8a230f25cee50dda2e3106322d78ec6`.
