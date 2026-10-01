# Same-node runtime updates

A live node release replaces ctld, its RootFS backend and network admission,
and the Nomad task driver without stopping a Sandbox or adding a worker. The
node needs measured spare memory for the temporary overlap of service processes.
A short block I/O or control RPC delay during ownership transfer is possible.

## Preserved execution

The allocation and task start time, Sentry and guest PIDs, heap, open file
handles, tmpfs, mmap, resource cgroup, network namespace, NBD device and XFS /
Overlay mounts remain the same. This path does not checkpoint, restore,
re-create a guest, remount its RootFS, restart Nomad or retire its writer.
Running runsc, procd and the host kernel remain pinned. Replacing those running
executables requires a separately qualified execution transition; kernel
reboots use the existing explicit memory maintenance protocol.

## Ownership protocol

1. Stage and verify an immutable release. Prove the exact node, boot, source
   executables, complete allocation inventory and sufficient host headroom.
2. Own a regional admission fence. Serialize its installation with outstanding
   autoscaler provider decisions, and hold subsequent scaling/consolidation.
   Existing resource and writer leases continue normally. Pending pre-fence
   claims must settle before any physical transfer.
3. The source quiesces runtime RPCs, node commands and reconciliation. Each NBD
   server stops at a complete request frame, drains accepted work and replies,
   flushes its writable branch and exports a sealed metadata index. It passes
   the connected NBD socket, proxy listeners and the primary flock open-file
   description over a root-only Unix socket using SCM_RIGHTS.
4. The candidate validates the full journal set, raw record digests, immutable
   mappings, sealed index/WAL inode and bounds, kernel NBD geometry, existing
   mounts, listener addresses, configuration digest and writer authority. It
   starts an independent renewal guard before accepting ownership. Tokens are
   never written to the transfer receipt.
5. The source re-proves consumed regional writer grants, fsyncs a committing
   receipt, detaches userspace storage owners without disconnect/unmount, stops
   listener admission and fsyncs the committed receipt. The candidate adopts
   the same primary flock with an advanced epoch and opens the same durable
   journals. Existing NBD attachments serve through the new userspace backend.
6. Atomically publish the driver executable and signal only the proved old
   plugin processes through pidfds. Nomad reloads the driver while keeping its
   allocation. Active recovery verifies the existing running guest, adopts its
   consumer lease after the old process has exited, and resumes the original
   regional claim heartbeat. Warm tasks retain their original registration.
7. Prove continuity, start a new standby and persist the new service pair for
   host boot. Release only the operation's exact regional fence.

The sealed index transfers metadata, not dirty block payloads. It avoids
replaying a potentially large WAL during the overlap. One socket and one
sealed index FD are sent per Ready RootFS session; descriptor transport is
batched and bounded. Regional PostgreSQL remains the sole writer/resource
lease authority. Local handoff cannot consume another grant or extend an
expired regional lease.

## Network transition

New TCP/TLS connections use the successor's inherited listeners. Established
TCP, TLS and HTTP/2 flows finish on their source process. That process retains
policy checks, credential revocation, audit and metering. It follows the
current owner's root-only policy snapshot and fails closed if that owner is
unavailable beyond the bounded policy freshness window. The source exits when
its retained flows finish; there is no forced flow-drain deadline.

UDP socket ownership transfers, but userspace UDP session state is recreated;
a brief UDP loss window is possible. Process-local per-Sandbox bandwidth token
buckets cannot be duplicated across owners: a live update rejects nonzero
local ingress/egress limits. Shared regional/team quota enforcement continues.
Changes to listener topology, network configuration or private trust settings
are rejected before transfer rather than treated as compatible code updates.

## Enablement and bounds

Set `nomad_runtime.transferable_nbd: true` before a node's first guest. Fresh
node enrollment renders this setting. Explicit legacy configuration remains
supported, with `live_update_protocol` omitted/zero in runtime health.

An occupied ioctl-NBD node cannot safely convert its attachment in place. Its
first live release is deferred, with zero stopped Sandboxes and no admission
fence installed. Conversion is allowed only when naturally idle, with no
running guests, RootFS mounts or kernel NBD owners; warm allocations and Nomad
stay running. This is an enablement constraint, not successful publication of
a new backend on an occupied legacy node.

The deployment guard currently permits at most 64 active guests and 512
total warm/active tasks, at least 4 GiB MemAvailable, and fewer than two retained
proxy generations. Wire/index tests cover 512-session descriptor bounds, but
that is not active-guest density qualification. The complete-node fixture tests
two simultaneous active guests; production density and memory headroom need
separate qualification before relying on the guard's ceiling. All stock runsc
companions must be byte-identical across the live release.

## Failure custody

| Boundary | Required behavior |
| --- | --- |
| Candidate rejects / disconnects before acceptance | Abort preparation; resume the original serving owner. |
| Invalid FD, journal/index, configuration or writer proof | Reject before irreversible ownership transfer. |
| Commit acknowledgement is lost | Match the exact fsynced receipt, binary/manifest digest and inherited flock; never replay the transfer. |
| Failure after transfer | Keep admission fence and journal. Inspect the actual serving owner and roll forward; never restart the predecessor against owned storage. |
| Driver publication or transport fails after a healthy transfer | Resume only the exact journal after proving unchanged guest/allocation and candidate identities. |
| Writer/consumer lease is revoked or expires | Fence according to existing authority rules; live publication does not weaken lease enforcement. |
| Candidate/host crashes after ownership commits | Crash recovery remains separate from planned zero-stop continuity. |

Root-only CLI capabilities are `ctld --live-update-capabilities` and the paired
`--live-update-from=<state-root>/ha/live-update.sock --live-update-id=<operation>`.
The caller must own regional admission/scaling custody and validate its exact
inventory. `manager --live-node-update-capabilities` advertises the matching
autoscaler hold. Neither CLI capability is a blanket compatibility claim.

## Validation

The isolated Linux fixture uses real kernel generic-netlink NBD, XFS/Overlay,
stock runsc, Nomad 1.11.3, ctld, task driver and systemd service replacement.
Regional authority/S3 peers in the complete-node fixture are synthetic; SQL
fence, capacity and autoscaler concurrency have separate real PostgreSQL tests.
Three complete releases with two simultaneous guests retain allocation/task identity, Sentry and guest PIDs,
heap/tmpfs tokens, shared mmap and an unlinked open file while RootFS I/O
continues and each predecessor storage process exits. A post-transfer
publication failure is injected separately before completing the same journal.

Additional tests exercise complete/partial NBD frames and in-flight operations,
flush/FUA, repeated abort/commit, cross-process FD ownership, malformed indexes,
consumer revocation, configuration rejection and lost commit acknowledgements.
Real transparent TCP and TLS/HTTP2 proxy tests retain old flows, admit new ones,
and enforce revocation without forced drain. Race suites cover ownership,
renewal, networking, driver recovery and autoscaler holds.
