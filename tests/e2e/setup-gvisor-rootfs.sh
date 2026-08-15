#!/usr/bin/env bash
set -euo pipefail

RUNSC_RELEASE="${RUNSC_RELEASE:-release/latest}"
RUNSC_BIN="${RUNSC_BIN:-/usr/local/bin/runsc}"
RUNSC_SHIM_BIN="${RUNSC_SHIM_BIN:-/usr/local/bin/containerd-shim-runsc-v1}"
RUNSC_ROOTFS_CONFIG="${RUNSC_ROOTFS_CONFIG:-/etc/containerd/runsc-rootfs.toml}"

log() {
  printf '[%s] %s\n' "$(date -Is)" "$*"
}

run_as_root() {
  if [[ "$(id -u)" == "0" ]]; then
    "$@"
    return
  fi
  if command -v sudo >/dev/null 2>&1; then
    sudo "$@"
    return
  fi
  echo "root privileges are required to install gVisor runtime files" >&2
  exit 1
}

runsc_arch() {
  case "$(uname -m)" in
    x86_64)
      printf 'x86_64\n'
      ;;
    aarch64|arm64)
      printf 'aarch64\n'
      ;;
    *)
      echo "unsupported architecture for gVisor runsc: $(uname -m)" >&2
      exit 1
      ;;
  esac
}

retry() {
  local attempts="$1"
  shift
  local delay=3
  local i=1
  while true; do
    if "$@"; then
      return 0
    fi
    if (( i >= attempts )); then
      return 1
    fi
    sleep "${delay}"
    i=$((i + 1))
    delay=$((delay * 2))
    (( delay > 30 )) && delay=30
  done
}

install_runsc_binaries() {
  if [[ -x "${RUNSC_BIN}" && -x "${RUNSC_SHIM_BIN}" ]]; then
    log "Using existing gVisor binaries at ${RUNSC_BIN} and ${RUNSC_SHIM_BIN}"
    return
  fi

  local arch tmp
  arch="$(runsc_arch)"
  tmp="$(mktemp -d)"

  log "Installing gVisor runsc ${RUNSC_RELEASE} for ${arch}"
  retry 5 curl -fsSL -o "${tmp}/runsc" "https://storage.googleapis.com/gvisor/releases/${RUNSC_RELEASE}/${arch}/runsc"
  retry 5 curl -fsSL -o "${tmp}/containerd-shim-runsc-v1" "https://storage.googleapis.com/gvisor/releases/${RUNSC_RELEASE}/${arch}/containerd-shim-runsc-v1"
  run_as_root install -m 0755 "${tmp}/runsc" "${RUNSC_BIN}"
  run_as_root install -m 0755 "${tmp}/containerd-shim-runsc-v1" "${RUNSC_SHIM_BIN}"
  rm -rf "${tmp}"
}

write_runsc_rootfs_config() {
  local tmp
  tmp="$(mktemp)"
  cat >"${tmp}" <<'EOF'
[runsc_config]
  systemd-cgroup = "false"
  overlay2 = "none"
  file-access = "shared"
EOF

  run_as_root mkdir -p "$(dirname "${RUNSC_ROOTFS_CONFIG}")"
  run_as_root install -m 0644 "${tmp}" "${RUNSC_ROOTFS_CONFIG}"
  rm -f "${tmp}"
}

install_runsc_binaries
write_runsc_rootfs_config

log "Prepared $(${RUNSC_BIN} --version | head -n1)"
log "Prepared gvisor-rootfs config at ${RUNSC_ROOTFS_CONFIG}"
