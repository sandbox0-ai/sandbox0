#!/bin/sh

set -eu

config_version="${1:-}"
runtime_handler="${2:-sandbox0-rootfs}"
runtime_type="${3:-io.containerd.runc.v2}"

case "${config_version}" in
  2)
    cat <<EOF
[proxy_plugins.sandbox0]
  type = "snapshot"
  address = "/run/sandbox0-rootfs-snapshotter/snapshotter.sock"

[plugins."io.containerd.grpc.v1.cri".containerd.runtimes."${runtime_handler}"]
  runtime_type = "${runtime_type}"
  snapshotter = "sandbox0"
EOF
    ;;
  3)
    cat <<EOF
[proxy_plugins.sandbox0]
  type = "snapshot"
  address = "/run/sandbox0-rootfs-snapshotter/snapshotter.sock"

[plugins."io.containerd.cri.v1.runtime".containerd.runtimes."${runtime_handler}"]
  runtime_type = "${runtime_type}"
  snapshotter = "sandbox0"
EOF
    ;;
  *)
    echo "usage: $0 <containerd-config-version: 2|3> [runtime-handler] [runtime-type]" >&2
    exit 2
    ;;
esac
