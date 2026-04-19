# Procd Volume Protocol

## Goal

Procd mounts `SandboxVolume` as a POSIX filesystem inside sandbox pods. The volume hot path must favor small-file latency and throughput over backward compatibility.

The final design removes gRPC from the procd-to-storage-proxy volume path. Procd talks to storage-proxy through `s0vp`, a session-scoped binary protocol over TCP. Storage-proxy remains the only component holding PostgreSQL and S3 credentials; sandbox pods never connect to object storage directly.

## Runtime Path

1. `procd` receives a mount request.
2. `procd` sends `OpMountVolume` to storage-proxy over `s0vp` with its internal token.
3. storage-proxy validates the token, mounts the volume, and returns a mount session ID and secret.
4. If the service-selected storage-proxy is not the primary for that volume, it returns a protocol redirect and `procd` reconnects to the primary address.
5. `procd` opens one long-lived `s0vp` TCP connection and sends `OpHello` with the mount session credential.
6. FUSE operations call `volumeFS`, which requires the binary session and sends one framed request per POSIX operation.
7. storage-proxy dispatches frames to the filesystem operation handler and replies with compact binary responses.
8. storage-proxy pushes watch/invalidate events on the same session connection as event frames.
9. `procd` remounts on root invalidation and forwards normal file events to the existing file watch pipeline.

There is no gRPC server, gRPC client, protobuf transport, or gRPC MountSession fallback on this path.

## Wire Format

Every `s0vp` frame has a fixed 24-byte little-endian header:

| Field | Size | Description |
| --- | ---: | --- |
| magic | 4 | `S0VP` |
| version | 2 | protocol version |
| op | 2 | operation code |
| flags | 2 | error/event bits |
| reserved | 2 | reserved |
| request_id | 8 | multiplexing key |
| payload_len | 4 | payload byte length |

Payloads are custom little-endian field encodings. Strings and byte slices use `uint32 length + raw bytes`. The mount session is volume-scoped, so hot operations do not repeat `volume_id` in every payload.

The client supports concurrent in-flight operations over a single connection. Responses are matched by `request_id`; watch events use `FlagEvent` and do not consume a pending request.

## POSIX Coverage

`s0vp` covers the mounted volume operations used by FUSE:

- namespace: lookup, mkdir, create, mknod, unlink, rmdir, rename, link, symlink, readlink
- metadata: getattr, setattr, statfs, access
- file IO: open, read, write, release, flush, fsync, fallocate, copy-file-range
- directory IO: opendir, readdir, releasedir
- extended attributes: getxattr, setxattr, listxattr, removexattr
- locking: getlk, setlk, setlkw, flock
- lifecycle/events: mount, hello, heartbeat, unmount, watch event

Unsupported FUSE operations still return `ENOSYS`.

## Deployment Contract

storage-proxy exposes:

- `s0vp`: volume protocol, default `8082`
- `http`: management API, default `8081`
- `metrics`: Prometheus metrics

The old storage-proxy gRPC port and config fields are removed. The infra plan wires procd to the storage-proxy service DNS name and `volumeProtocolPort`, not the HTTP service exposure port.

## Failure Semantics

`volumeFS` fails closed when a binary session is missing. It does not fall back to any alternate transport.

storage-proxy returns compact protocol status codes on the `s0vp` frame. These are wire-level codes, not gRPC transport errors. Procd maps them to FUSE status values:

- not found -> `ENOENT`
- permission/auth failure -> `EPERM`
- invalid argument -> `EINVAL`
- unsupported operation -> `ENOSYS`
- unavailable or internal failure -> `EIO`

## Performance Direction

This design removes the highest-overhead per-operation layers from the current mounted volume path:

- no HTTP/2 stream machinery on every filesystem op
- no protobuf marshal/unmarshal in the hot path
- no repeated volume ID in session-scoped requests
- one persistent TCP connection with request multiplexing
- event delivery shares the same session instead of opening a second stream

The next order-of-magnitude step should add compound operations that match small-file workloads, especially `CreateWriteClose`, `ReadSmallFile`, and batched lookup/stat. Those can be encoded as first-class `s0vp` ops without changing the storage credential boundary.
