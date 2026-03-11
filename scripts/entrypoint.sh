#!/bin/sh
set -eu

service="${SERVICE:-}"
if [ -z "$service" ]; then
  echo "SERVICE is required (e.g. edge-gateway, global-directory, internal-gateway, manager, scheduler, storage-proxy, k8s-plugin, infra-operator, netd)" >&2
  exit 1
fi

case "$service" in
  edge-gateway|global-directory|internal-gateway|manager|scheduler|storage-proxy|k8s-plugin|infra-operator|netd)
    exec "/usr/local/bin/$service" "$@"
    ;;
  *)
    echo "Unknown SERVICE: $service" >&2
    exit 1
    ;;
esac
