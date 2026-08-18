# Sandbox0 Nomad gVisor Driver (experimental)

This task driver is part of an isolated Nomad + gVisor architecture PoC. It is
not a replacement for the production Kubernetes runtime path.

The driver creates a generic warm gVisor container with `runsc create`. The OCI
root initially points to a private placeholder mount. After manager/ctld have
attached a RootFS generation and applied network policy, an authorized caller
bind-mounts that generation over the private root mount and invokes stock
`runsc start`. The claim is one-shot and is rejected after the first attempt.

## Status

Implemented:

- Nomad task-driver plugin lifecycle and HCL schemas
- stock `runsc` CLI adapter with `overlay2=none`
- generic writable OCI bundle generation
- pre-created warm container
- local Unix control socket with `/status` and `/claim`
- RootFS bind, start, stop, delete, signal, and cleanup paths
- one-shot claim and basic recovery semantics
- on-disk task state for driver crash recovery
- unit tests with a fake runsc runtime

Not implemented:

- manager/ctld integration and slot registry
- RootFS attach, PostgreSQL writer fencing, and S3 persistence
- network-policy token verification
- procd first-command-ready accounting
- guest stdout/stderr console forwarding
- full cgroup and Nomad stats integration
- production Nomad deployment and upgrade automation

## Build and test

```sh
go test ./...
go vet ./...
go build -o nomad-driver-sandbox0 .
```

The module is intentionally separate from the repository root so the Nomad
dependency is not imposed on every Sandbox0 service.

## Example client configuration

```hcl
plugin "sandbox0-gvisor" {
  config {
    runsc_path         = "/usr/local/bin/runsc"
    runsc_root         = "/var/run/sandbox0/runsc"
    control_dir        = "/var/run/sandbox0/nomad-slots"
    allowed_rootfs_dir = "/var/lib/sandbox0/rootfs"
    platform           = "systrap"
    overlay2           = "none"
    file_access        = "shared"
    directfs           = true
  }
}
```

See `example/warm-slot.nomad` for a warm allocation. A development smoke task
may set `wait_for_claim = false` and provide an allowed RootFS directory only
when the client explicitly sets `dev_smoke_enabled = true`; the production path
always waits for manager authorization.
