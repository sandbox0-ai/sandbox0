# D-PREFETCH-IMPORT: real candidate artifacts, not a startup result

2026-09-13. Local evidence: `/tmp/sandbox0-prefetch-import.RAsJyS`; retained
remote diagnostics: `/data/sandbox0-prefetch-import-RAsJyS`. Follow
[PREFETCH-GUEST.md](PREFETCH-GUEST.md) without repeating its stock-guest gate.

## Result and scope

Both Node and Coding pass the current durable OCI importer and complete
authenticated object-content audits. This produces actual encrypted S3-backed
RootFS artifacts containing the experimental procd for a SAME-binary, SAME
per-image-artifact on/off test. It does not measure claim, command or startup
latency, establish a speedup, or enable a product default.

| Import result | Node | Coding |
| --- | ---: | ---: |
| Logical XFS bytes | 17,179,869,184 | 1,099,511,627,776 |
| Allocated XFS image bytes at publication | 333,692,928 | 5,471,105,024 |
| Published and fully audited objects | 8 | 96 |
| Authenticated plaintext object bytes | 99,657,145 | 1,706,485,817 |
| Import worker seconds, NOT startup | 13.632489316 | 268.991289010 |
| Complete test seconds, including audit | 14.58 | 282.24 |
| Ready / failed / abandoned operations | 1 / 0 / 0 | 1 / 0 / 0 |

Coding remains a sparse logical1TiB image with about5.47GB allocated, not a
fully populated or history-bearing1TiB root. Import inputs and object audits
can warm disposable-node caches; none of that is admissible as a cold startup
sample. A later cold trial must explicitly establish its cache boundary.

## Exact durable identities

Use the independent PostgreSQL clone `s0_prefetch_import_rasjys`, OID346105,
cloned from quiescent `s0_mapping_oci_uqo1av` on the same local writer authority.
It retains2072 sandbox rows and receives only these owned import operations.
The original263-row database, formal2072-row database and source artifact/import
inventories are unchanged. No schema migration, claim or guest command is run.

Node:

- OCI: `docker.io/library/node@sha256:4d676821dff059fd00d277ee4261ef34ea712317fed0737c03941481b5760c96`
- Operation: `template-import:d672245649146c52e2e54802ec79d6b0e4cd6cc779e3b2c136288bc95d4f380f`
- Artifact: `sha256:b31477ddf25372952ce0f1546c298654c359368397f71a02fe534b9b886c49a1`
- Object prefix: `rootfs/prefetch-import-rasjys/node/v2`

Coding:

- OCI: `docker.io/sandbox0ai/otemplates@sha256:4861af110b573e4bf23ffb537ac7acd74696383b25af51815838e6d0a767cfa6`
- Operation: `template-import:3fb483fd2002e9bc3c04fff36b2886e78ed779680179ba13b9d2bf2d79ad5bf9`
- Artifact: `sha256:058e029538b7c20c3bd14dba439930181b5f6be3012dc691244561f089d6e089`
- Object prefix: `rootfs/prefetch-import-rasjys/coding/v2`

Both use procd SHA256
`8eec5c8a9cbdc53fc77ae2dc82596a76f0df3aaccc40ced3ead934a55023cda5`,
protocol `sandbox0.procd.v3`, format/descriptor2,64KiB data ranges,
`xfs-file-ranges-v1` without fallback and `contiguous-mapping-v1`.
Ready attestation version3 binds those exact inputs. The current
`configureRootFSImportWorker`, journal, immutable conditional publication and
encrypted store are used, not a bind-injected procd or manually synthesized
descriptor. The private importer overlay observes committed PostgreSQL intent
BEFORE every PUT and authenticates/decrypts/checksums EVERY resulting object.
A descriptor-only Reader also verifies the XFS superblock; no repeated full
mapping-tree/layout audit is needed.

The diagnostic import test is compiled against the unchanged1359-file current
source inventory, product HEAD/origin-main
`0f09220460581bfc1fdc331f34ebc85bf38381e7`. Import binary SHA256:
`6300f782083609b7beac4ab0d01adb47e6998542f82c23c208bbfd1d16861827`.
Compilation is not reported as a local import test. The two actual tests run
remotely on original2CPU8GiB, not production-width hardware.

## Trigger clarification requested by the user

This candidate is command-triggered bounded read-ahead, not prediction of a
future tenant RootFS or command. In the private CMD overlay, Start first creates
the actual exec.Cmd with the existing path resolution, environment and CWD,
then creates its cancelable runner, invokes the optional hook, and starts that
same command. The exact `S0_EXEC_PREFETCH=elf-v1` command environment enables it;
off performs no optional RootFS I/O. No sandbox/claim-wide flag is required.

For literal `node -v`, the executable is known only after the command request
arrives. The hook reads bounded ELF metadata and parallel-reads initialized
PT_LOAD file ranges of that main executable. It does not know the exact future
working set, resolve speculative shared libraries, predict scripts/children, or
fetch the whole RootFS. Extra reads may outweigh reduced serialized page-fault
waiting; that is the hypothesis the on/off trial must test, not an established
optimization. It targets first-command execution, not claim readiness itself.

The unchanged prototype admits one job/procd, four1MiB readers and at most128MiB
reserved ReadAt bytes including metadata. Optional waiting has a500ms cap, NOT
an increased HTTP deadline. Pending kernel work may outlive that wait and keeps
its admission/resources until physical completion. Report that honestly; do
not infer zero pending I/O from missing counters or exclude pre-exec time from
first-command latency. Default safety outside the verified actual RootFS
noatime mount precondition remains unresolved.

The existing diagnostic workload schema does not yet forward command env vars.
The external API already supports top-level CreateContextRequest.env_vars;
adapt and test only the diagnostic harness as needed for the on/off treatment.
Do not replace literal node-v with a shell/env wrapper or set the flag at
sandbox/template level, where it would also affect claim readiness commands.

## Execution receipts, errors and preservation

Initial preflight fails because the retained staging filesystem is not mounted.
A local guard also mistakenly expects XFS for the staging disk; correct it to
the verified existing ext4 UUID before upload. The disk HOLDS XFS image files;
it is not itself an XFS filesystem. Both issues are retained, not hidden or
worked around by reformatting, deleting history or enlarging hardware.

Readonly cloud/device inspection binds existing64GiB disk
`d-t4n1qmzpkzn19uxs6o0p`, serial `t4n1qmzpkzn19uxs6o0p`, ext4 UUID
`59c7f761-4479-46df-b2ef-8ea7d7bce30b` to the exact test ECS. Its device name on
this boot is `/dev/nvme1n1`; resolve again on any later boot. Mount the retained
path `/data/sandbox0-xfs-staging-G3Q8sb` rw/nosuid/nodev/noatime. Starting free
capacity is35,155,968,000B; independent system-disk/DB-clone headroom also passes.
Use only the new `prefetch-import-RAsJyS` work subdirectory. Each importer
normally removes its transient image/work tree; historical data is preserved.

Each case is submitted once to an owned systemd unit with1800s lifetime,
6GiB memory maximum/no swap/1024 tasks. Original importer build15m, lease2m and
renewal30s are unchanged. Observation of ready publication while the auditing
child is still alive is NOT terminal: wait for full test/unit completion.
Both final units are inactive/dead/success/exit0 with absent children/cgroups.
Lease owners/expiry clear at ready; no pending/building operations or active
resource leases remain in the clone.

Final cleanup checks exact units, processes, staging-backed loops, mounts and
all64 detached NBDs before normally unmounting the retained ext4 disk. Preserve
the new DB, S3 objects, private configs, all logs and reports on/data; no broad
deletion. Original service PIDs/binaries/configs and job72102 remain unchanged;
old ready1 is observed, not repaired or timed. Close the owned SSH master and
consume its terminal handle, then use the remote skill lifecycle to stop
compute and verify original2CPU8GiB Stopped/StopCharging via sanitized CLI.

## Next action; do not repeat this import

Use these exact retained artifacts for same-binary on/off regional ingress to
authenticated procd and immediate literal node-v, with unchanged10s HTTP
budgets. Record cold versus cached-node NEW identities, full claim/command/
combined time, misses, machine contention and S3/I/O amplification. Install
the already-qualified current runtime/recovery driver; do not revisit the old
default carrier EOF/409 loop as an unrelated prerequisite.

Previous current-pin cold combined maxima2.221/2.668s still fail2s. Nothing in
these import results rules out S3 response waiting or occupied-node contention.
The original1s goal/accepted2s fallback, stateless workers, no tenant prewarm,
true populated/history roots and occupied-width requirements remain open.
No production rollout, merge, tag, local e2e or runtime default promotion.
