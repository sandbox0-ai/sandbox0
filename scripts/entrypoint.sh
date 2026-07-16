#!/bin/sh
set -eu

service="${SERVICE:-}"
if [ -z "$service" ]; then
  echo "SERVICE is required (e.g. regional-gateway, ssh-gateway, global-gateway, cluster-gateway, manager, scheduler, ctld, infra-operator)" >&2
  exit 1
fi

case "$service" in
  regional-gateway|ssh-gateway|global-gateway|cluster-gateway|manager|scheduler|ctld|infra-operator)
    exec "/usr/local/bin/$service" "$@"
    ;;
  *)
    echo "Unknown SERVICE: $service" >&2
    exit 1
    ;;
esac
