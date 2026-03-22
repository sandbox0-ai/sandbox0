#!/bin/sh
set -eu

service="${SERVICE:-}"
if [ -z "$service" ]; then
  echo "SERVICE is required (e.g. regional-gateway, global-gateway, cluster-gateway, manager, scheduler, storage-proxy, k8s-plugin, infra-operator, netd)" >&2
  exit 1
fi

case "$service" in
  regional-gateway|global-gateway|cluster-gateway|manager|scheduler|storage-proxy|k8s-plugin|infra-operator|netd)
    exec "/usr/local/bin/$service" "$@"
    ;;
  *)
    echo "Unknown SERVICE: $service" >&2
    exit 1
    ;;
esac
