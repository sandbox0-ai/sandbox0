# Procd Volume Boundary

## Goal

Procd should not sit on the hot path for SandboxVolume I/O. Sandbox processes use the normal Linux VFS path, while ctld owns node-local volume portals through CSI and host-side FUSE.

## Runtime Path

1. `SandboxTemplate.spec.volumeMounts` declares fixed mount points.
2. The manager builds sandbox pods with CSI inline volumes for those mount points.
3. Kubelet calls ctld's CSI node service to publish each portal on the node.
4. At claim time, manager binds requested `SandboxVolume` IDs to the predeclared portals through ctld.
5. The sandbox process reads and writes through `kernel VFS -> FUSE -> ctld -> S0FS`.
6. Procd still owns process, file API, webhook, and REPL/Cmd lifecycle, but it no longer mounts volumes or talks to storage-proxy for volume I/O.

## Consequences

- Dynamic post-claim volume mount/unmount is intentionally removed.
- Sandbox pods do not receive S3 or database credentials.
- Storage-proxy continues to own volume metadata, HTTP volume management, snapshots, restore, direct file API, and sync API.
- Ctld is the node-local storage daemon and owns the local S0FS engine/WAL for mounted volume portals.
