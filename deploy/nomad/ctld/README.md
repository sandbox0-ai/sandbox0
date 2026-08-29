# Nomad ctld host deployment

See `../README.md` for the complete manager, node, warm-pool, quota, and
acceptance deployment contract.

Nomad nodes run two `ctld` systemd instances directly in the host mount and
network namespaces. They share the HA lock and durable state under
`/var/lib/sandbox0/ctld`; only the elected primary opens NBD, Bolt, network,
control sockets, and per-lease cgroups. Do not add systemd filesystem isolation options such as
`ProtectSystem`, `RootDirectory`, `BindPaths`, or `PrivateDevices`: those
create a private mount/device namespace and break the exact mount namespace
shared with the Nomad task driver.

The A/B pair protects node-local runtime availability during a process crash or
certificate rollout. It does not make the ECS worker stateful and it is not a
stopped standby server. Both processes run on the same disposable node; all
durable sandbox truth remains in PostgreSQL and S3.

Build the three pinned binaries, provision the files referenced by
`ctld.yaml` under `/etc/sandbox0/pki` and `/etc/sandbox0/tokens`, copy the
examples, and replace every placeholder. The supplied host check requires the
fixed PKI/token paths from the example, root ownership, and no group/other
write permission. Set the four `SANDBOX0_RESOURCE_*` values to the allocatable
capacity of the dedicated cpuset after reserving host/ctld/Nomad overhead; do
not copy carrier-job resources into these values. Start from
`nomad-plugin.hcl.example` for the Nomad client plugin configuration. Register
`SANDBOX0_NOMAD_ADDRESS` with the node's private IP, and issue the Nomad client
certificate with that exact IP SAN; a loopback URL is not valid for an
IP-scoped certificate. Register
the Nomad client with `client.node_pool = "sandbox0"` and set node metadata
`sandbox0_dedicated=true`; do not target that pool from general jobs. Then
install:

```sh
sudo ./deploy/nomad/ctld/install-node.sh \
  --ctld ./bin/ctld \
  --driver ./bin/nomad-driver-sandbox0 \
  --runsc ./bin/runsc \
  --config ./node/ctld.yaml \
  --network-config ./node/ctld-networking.yaml \
  --nomad-config ./node/nomad-plugin.hcl \
  --env ./node/ctld.env \
  --start
```

The Nomad client's `alloc_dir` and `alloc_mounts_dir` must resolve beneath
`nomad_runtime.consumer_mount_root` (`/opt/nomad` in the supplied config).
Keeping Nomad's default allocation directory beneath another `data_dir`
causes ctld to reject the task driver's stable RootFS mount. The optional
`nomad_runtime.node_control_timeout` defaults to `10s` and may be increased
only for a demonstrably slow node-local runtime. Increasing it does not change
the regional command-ready SLO or turn a late claim into a pass.
The driver-level `runsc_operation_timeout_seconds` separately bounds each
`runsc create` and `runsc start`; it defaults to 30 seconds and is capped at
120 seconds. Keep the node-control and claim-lease deadlines above the
observed runsc duration. A larger value is a diagnostic accommodation for
software-emulated nodes, not an SLO relaxation.

The installer adds `sandbox0-ctld.target` as a hard Nomad dependency, loads a
64-device NBD pool, applies required networking sysctls, installs tmpfiles
rules for the reboot-volatile runtime directories, and places the task driver
in `/opt/nomad/plugins`. Before each ctld start it provisions the root-owned
`/sys/fs/cgroup/sandbox0` cgroup-v2 subtree, initializes its cpuset from the
parent's effective confinement, and enables `cpu`, `cpuset`, `memory`, and
`pids` for per-lease children. Startup fails if those controllers are not
available/delegated or the root itself contains processes; existing active
lease children are preserved across A/B restarts. Installation fails instead of reloading an in-use NBD
module when it was already loaded with fewer than 64 devices; drain and reboot
that node to apply the installed module option. The driver still performs a
synchronous ctld socket fingerprint before advertising a warm slot.
For a full node reboot, the authenticated new boot may execute cleanup for an
old boot only through the plugin-independent path. The durable slot journal
must match the old incarnation, and cleanup must independently observe the old
runsc and resource cgroup absent before returning a terminal proof.

Before admitting a new node image, exercise the stock runsc binary against a
disposable delegated sibling of the production subtree. The test root must be
a direct `/sys/fs/cgroup/sandbox0-test-*` child with the same controllers,
empty process set, unconstrained root limits, and a `0-1` cpuset backed by at
least 1 GiB of host memory. Point the test at an unpacked minimal OCI rootfs:

```sh
sudo env \
  SANDBOX0_TEST_CGROUP_ROOT=/sys/fs/cgroup/sandbox0-test-runsc \
  SANDBOX0_TEST_RUNSC=/usr/local/bin/runsc \
  SANDBOX0_TEST_RUNSC_ROOTFS=/var/lib/sandbox0/test-rootfs \
  SANDBOX0_TEST_RUNSC_PLATFORM=systrap \
  go test ./pkg/nomadruntime \
    -run 'TestRuntimeResourceCgroup(V2|Runsc)Integration' -v
```

The opt-in test writes the exact lease limits, starts stock runsc with the
production `overlay2=none`, shared file access, and DirectFS flags, proves the
live runtime occupies and is CPU-throttled by that cgroup, rejects release
while occupied, and only accepts release after runsc deletion and physical
cgroup disappearance. `SANDBOX0_TEST_RUNSC_START_TIMEOUT` may be raised to at
most ten minutes only for software-emulated hosts; it is not an SLO override.

The configured `nomad_runtime.nbd_devices` list, not only the kernel
`nbds_max`, is the usable RootFS concurrency bound. Keep that list at least as
wide as the largest synchronized claim batch plus operational replacement
headroom. The supplied environment example configures 16 devices, while the
production acceptance warm job reserves eight slots.

For an existing node, replace the binaries and run `rollout-node.sh`. It
restarts slot B and then A, waiting for each instance to become primary-ready
or synchronized-standby-ready before touching its peer. Drain and roll nodes
one at a time for changes that alter the runsc compatibility digest, RootFS
format, NBD pool, or network policy format. Roll back with the previous pinned
binary/config set using the same B-then-A sequence; never downgrade across an
on-disk journal version that the previous binary cannot read.
