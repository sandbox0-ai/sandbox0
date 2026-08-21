# Nomad ctld host deployment

Nomad nodes run two `ctld` systemd instances directly in the host mount and
network namespaces. They share the HA lock and durable state under
`/var/lib/sandbox0/ctld`; only the elected primary opens NBD, Bolt, network,
and control sockets. Do not add systemd filesystem isolation options such as
`ProtectSystem`, `RootDirectory`, `BindPaths`, or `PrivateDevices`: those
create a private mount/device namespace and break the exact mount namespace
shared with the Nomad task driver.

Build the three pinned binaries, provision the files referenced by
`ctld.yaml` under `/etc/sandbox0/pki` and `/etc/sandbox0/tokens`, copy the
examples, and replace every placeholder. The supplied host check requires the
fixed PKI/token paths from the example, root ownership, and no group/other
write permission. Start from `nomad-plugin.hcl.example` for the Nomad client
plugin configuration. Then install:

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

The installer adds `sandbox0-ctld.target` as a hard Nomad dependency, loads a
64-device NBD pool, applies required networking sysctls, installs tmpfiles
rules for the reboot-volatile runtime directories, and places the task driver
in `/opt/nomad/plugins`. Installation fails instead of reloading an in-use NBD
module when it was already loaded with fewer than 64 devices; drain and reboot
that node to apply the installed module option. The driver still performs a
synchronous ctld socket fingerprint before advertising a warm slot.

For an existing node, replace the binaries and run `rollout-node.sh`. It
restarts slot B and then A, waiting for each instance to become primary-ready
or synchronized-standby-ready before touching its peer. Drain and roll nodes
one at a time for changes that alter the runsc compatibility digest, RootFS
format, NBD pool, or network policy format. Roll back with the previous pinned
binary/config set using the same B-then-A sequence; never downgrade across an
on-disk journal version that the previous binary cannot read.
