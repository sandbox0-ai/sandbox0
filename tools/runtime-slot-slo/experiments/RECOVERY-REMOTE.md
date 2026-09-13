# D-RECOVERY-REMOTE: qualify persisted warm-carrier recovery

2026-09-13 Asia/Beijing. This is recovery qualification, not a new startup latency
sample. Machine/S3 causality and the original regional first-command gates remain
separate. Artifact root: `/tmp/sandbox0-recovery-remote.ev9NvC`.

## Controlled change

The remote Nomad daemon is1.11.3. Install the D-PERSISTED-CONFIG linux/amd64 driver
`ed647ff9627156b43a212e4882c0cd920dda418b14435e22d8debdfab8630899`
with the already-qualified candidate manager/ctld and stock runsc20260817. The
1355-file source inventory remains unchanged; no product change is made here.
Use the original2CPU/8GiB test instance and two tenant-neutral warm carriers.
There are no guests, claims, commands, RootFS mounts, tenant prewarming or image
imports. This is neither production-width nor occupied-density acceptance.

Clone the idle source fixture into isolated database `s0_recovery_remote_ev9nvc`,
preserving the source2072 sandbox rows and10 artifact bindings. Use separate
node state/journals/branch paths and actual2CPU physical-capacity settings. Keep
object maintenance disabled and artifact/importer format inputs matched. Record
exact binary/catalog/config hashes and recoverable backups before installation.

One setup error is retained: the first owned manager config also disabled the
composite materializer, which the current Nomad manager requires. Manager failed
closed before any test job submission. After checking zero composite generations,
materialization batches, object-deletion rows and active imports, enable that
required component in the owned config only. Preserve the old config/hash and
an immutable correction receipt. This is not a product recovery failure or a
startup sample. The guard now also requires ActiveState=active, not just a PID.

Two earlier submission checks failed before their mutation guard: the first ran
before installation completed; the second found the failed manager. No job was
submitted by either. Installation was observed to completion, never retried.
Systemd gateway shutdown takes its existing90s timeout; it was not increased.

## Actual results

| Event | Required outcome | Observed outcome |
| --- | --- | --- |
| One same-boot Nomad service restart | Recover existing allocation/slot IDs and fresh authenticated heartbeats | Pass; both IDs preserved, Nomad PID4651→5954, both `warm-slot-recovered` events |
| One ordinary ECS reboot | Do not reuse old incarnation; automatically replenish new allocations | Pass; both old slots terminal and two different allocations ready |
| Job stability during both tests | No manual submit, stop or purge to manufacture refill | JobModifyIndex71629 and version371 unchanged |

Same boot: `9a24c1b3-9f2a-42ea-b6d7-f1ee36f52032`. The preserved allocations
are `4bac56aa-e32f-749d-ab9a-77ff59db8c1d` and
`578e2872-90c1-3442-09c1-26851ca83fbf`. Fresh PG heartbeats follow the completed
restart. Capture the complete Nomad journal within the bounded restart window;
an earlier2500-line tail is retained but is not the sole recovery evidence.

New boot: `11f458dc-dc9b-4144-aa76-5602037547eb`. Old network namespace paths no
longer exist, so recovery rejects them; Nomad's attempted new task inside each
old allocation receives409 and its no-restart policy terminates the attempt.
PG subsequently records both old slots terminal with `allocation_missing`. New
allocations `c5737540-0e67-e1fe-37b8-c466a77011bc` and
`62a66494-9926-d382-c0bb-beabd8919943` become ready without manual repair. Keep
those expected failure/warning events rather than describing the boot as free
of errors. This real case exercises missing old netns; other exact-incarnation
adversarial cases remain covered by local regressions, not this remote run.

## Machine/S3 interpretation and remaining gates

The host remains95–96% disk-used and Nomad collects historical allocations under
its80% GC threshold. This confirms environmental pressure, not an ENOSPC or
disk-latency explanation for ready-carrier startup. The recovery fix is separately
demonstrated despite that pressure. No S3 latency experiment occurs here; zero
workload reads/imports must not be promoted to a measured absence of every
background object-store request.

The latest regional cold claim-plus-command maxima remain Node2.221s and
Coding2.668s. No speedup is credited to this recovery fix. Both cold/cached-new
cohorts, populated large roots/upper histories, actual occupied machine width,
and the full regional ingress-to-procd1s/2s boundaries remain open. Do not repeat
the rejected object-size-only S3 sweep or duplicate the existing refill fix.
No production rollout, merge, tag, timeout increase or local e2e is performed.

## Restoration

Original binaries, catalog and configuration hashes are restored; all four owned
systemd dropins are moved into the experiment's recoverable archive. Original
job semantics are restored at JobModifyIndex71677 and both original-fixture
carriers are ready. The original263-row database, formal/source2072-row histories
and source artifact bindings are preserved; isolated2072 rows are retained.
All64 NBD devices are detached and there are no active resource leases or branch
mounts. Final object-work counts remain zero.

The33-document sanitized collection is downloaded and checksum verified;
SHA256 `fa9170e501c5130fbde548c4e491e00cf2599746589f1005f999d3aa8129a174`.
Private database dump, full config contents and original job body stay remote.
The complete restart and new-boot windows each contain2694 records; restart
contains both recovery events and no EOF. Preserve the earlier bounded tail,
setup failures and correction separately. Product inventory remains1355 files.
Owned SSH is closed and ECS is confirmedStopped/StopCharging at its original
2CPU/8GiB size. No historical data is deleted.
