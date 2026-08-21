#!/bin/sh
set -eu

usage() {
  cat >&2 <<'EOF'
Usage: install-node.sh --ctld PATH --driver PATH --runsc PATH --config PATH --network-config PATH --nomad-config PATH --env PATH [--root PATH] [--start]
EOF
  exit 64
}

ctld=
driver=
runsc=
config=
network_config=
nomad_config=
environment=
root=/
start=0
while [ "$#" -gt 0 ]; do
  case "$1" in
    --ctld) [ "$#" -ge 2 ] || usage; ctld=$2; shift 2 ;;
    --driver) [ "$#" -ge 2 ] || usage; driver=$2; shift 2 ;;
    --runsc) [ "$#" -ge 2 ] || usage; runsc=$2; shift 2 ;;
    --config) [ "$#" -ge 2 ] || usage; config=$2; shift 2 ;;
    --network-config) [ "$#" -ge 2 ] || usage; network_config=$2; shift 2 ;;
    --nomad-config) [ "$#" -ge 2 ] || usage; nomad_config=$2; shift 2 ;;
    --env) [ "$#" -ge 2 ] || usage; environment=$2; shift 2 ;;
    --root) [ "$#" -ge 2 ] || usage; root=$2; shift 2 ;;
    --start) start=1; shift ;;
    *) usage ;;
  esac
done

[ "$(id -u)" -eq 0 ] || { echo "installer must run as root" >&2; exit 1; }
for file in "$ctld" "$driver" "$runsc" "$config" "$network_config" "$nomad_config" "$environment"; do
  [ -f "$file" ] || usage
done
for binary in "$ctld" "$driver" "$runsc"; do
  [ -x "$binary" ] || { echo "binary is not executable: $binary" >&2; exit 1; }
done
case "$root" in /*) ;; *) echo "--root must be absolute" >&2; exit 1 ;; esac
[ "$root" != "/" ] || root=

asset_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
dest() { printf '%s%s' "$root" "$1"; }

install -d -m 0755 "$(dest /usr/local/bin)" "$(dest /usr/local/libexec/sandbox0)"
install -d -m 0755 "$(dest /opt/nomad/plugins)" "$(dest /etc/nomad.d)" "$(dest /etc/systemd/system/nomad.service.d)"
install -d -m 0755 "$(dest /etc/modules-load.d)" "$(dest /etc/modprobe.d)" "$(dest /etc/sysctl.d)" "$(dest /etc/tmpfiles.d)"
install -d -m 0700 "$(dest /etc/sandbox0)" "$(dest /etc/sandbox0/pki)" "$(dest /etc/sandbox0/tokens)"
install -d -m 0750 "$(dest /var/lib/sandbox0/ctld)" "$(dest /var/lib/sandbox0/ctld/nomad)"
install -d -m 0755 "$(dest /run/sandbox0)" "$(dest /run/sandbox0/nomad-slots)" "$(dest /run/netns)" "$(dest /opt/nomad)"

install -m 0755 "$ctld" "$(dest /usr/local/bin/ctld)"
install -m 0755 "$runsc" "$(dest /usr/local/bin/runsc)"
install -m 0755 "$driver" "$(dest /opt/nomad/plugins/nomad-driver-sandbox0)"
install -m 0755 "$asset_dir/ctld-host-check" "$(dest /usr/local/libexec/sandbox0/ctld-host-check)"
install -m 0755 "$asset_dir/rollout-node.sh" "$(dest /usr/local/libexec/sandbox0/ctld-rollout-node)"
install -m 0600 "$config" "$(dest /etc/sandbox0/ctld.yaml)"
install -m 0600 "$network_config" "$(dest /etc/sandbox0/ctld-networking.yaml)"
install -m 0600 "$nomad_config" "$(dest /etc/nomad.d/30-sandbox0-gvisor.hcl)"
install -m 0600 "$environment" "$(dest /etc/sandbox0/ctld.env)"
[ -f "$(dest /etc/sandbox0/ctld-a.env)" ] || install -m 0600 "$asset_dir/ctld-a.env.example" "$(dest /etc/sandbox0/ctld-a.env)"
[ -f "$(dest /etc/sandbox0/ctld-b.env)" ] || install -m 0600 "$asset_dir/ctld-b.env.example" "$(dest /etc/sandbox0/ctld-b.env)"
install -m 0644 "$asset_dir/sandbox0-ctld@.service" "$(dest /etc/systemd/system/sandbox0-ctld@.service)"
install -m 0644 "$asset_dir/sandbox0-ctld.target" "$(dest /etc/systemd/system/sandbox0-ctld.target)"
install -m 0644 "$asset_dir/nomad.service.d/20-sandbox0-ctld.conf" "$(dest /etc/systemd/system/nomad.service.d/20-sandbox0-ctld.conf)"
install -m 0644 "$asset_dir/modules-load.d/sandbox0-ctld.conf" "$(dest /etc/modules-load.d/sandbox0-ctld.conf)"
install -m 0644 "$asset_dir/modprobe.d/sandbox0-nbd.conf" "$(dest /etc/modprobe.d/sandbox0-nbd.conf)"
install -m 0644 "$asset_dir/sysctl.d/90-sandbox0-ctld.conf" "$(dest /etc/sysctl.d/90-sandbox0-ctld.conf)"
install -m 0644 "$asset_dir/tmpfiles.d/sandbox0-ctld.conf" "$(dest /etc/tmpfiles.d/sandbox0-ctld.conf)"

if [ "$start" -eq 1 ]; then
  [ -z "$root" ] || { echo "--start cannot be combined with a staging --root" >&2; exit 1; }
  for module in nbd overlay xfs bridge br_netfilter nf_tproxy_ipv4 xt_TPROXY; do
    modprobe "$module"
  done
  configured_nbd_devices=$(cat /sys/module/nbd/parameters/nbds_max)
  case "$configured_nbd_devices" in ''|*[!0-9]*) echo "invalid nbd nbds_max: $configured_nbd_devices" >&2; exit 1 ;; esac
  [ "$configured_nbd_devices" -ge 64 ] || {
    echo "nbd is already loaded with nbds_max=$configured_nbd_devices; drain and reboot the node to apply nbds_max=64" >&2
    exit 1
  }
  systemd-tmpfiles --create /etc/tmpfiles.d/sandbox0-ctld.conf
  sysctl --load=/etc/sysctl.d/90-sandbox0-ctld.conf
  if command -v udevadm >/dev/null 2>&1; then
    udevadm settle
  fi
  systemctl daemon-reload
  if systemctl is-active --quiet sandbox0-ctld.target; then
    /usr/local/libexec/sandbox0/ctld-rollout-node
  else
    systemctl enable --now sandbox0-ctld.target
  fi
  systemctl try-restart nomad.service
fi
