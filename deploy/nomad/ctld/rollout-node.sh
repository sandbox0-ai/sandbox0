#!/bin/sh
set -eu

wait_ready() {
  slot=$1
  # A promoted primary must finish fail-closed durable RootFS recovery before
  # it can report ready. The Go runtime bounds that recovery at 15 minutes;
  # this host rollout adds one minute for election and probe propagation.
  deadline=$(( $(date +%s) + 960 ))
  while [ "$(date +%s)" -lt "$deadline" ]; do
    if "${CTLD_BIN:-/usr/local/bin/ctld}" \
      -ha-probe=ready \
      -ha-probe-socket="/run/sandbox0/ctld-${slot}-ha.sock" \
      -http-addr="${SANDBOX0_CTLD_HTTP_ADDR:-:8095}" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  echo "ctld slot $slot did not become role-ready within 960 seconds" >&2
  return 1
}

[ "$(id -u)" -eq 0 ] || { echo "rollout must run as root" >&2; exit 1; }
for restarted_slot in b a; do
  systemctl restart "sandbox0-ctld@${restarted_slot}.service"
  # Restarting the old primary promotes its peer. The restarted standby may
  # synchronize before that promoted primary has reopened the privileged
  # runtime, so readiness is complete only after both slots are role-ready.
  for observed_slot in a b; do
    wait_ready "$observed_slot"
  done
done
