#!/usr/bin/env bash
set -euo pipefail

cluster="${1:-sandbox0-e2e}"
runtime_class="${2:-gvisor}"
runtime_handler="${3:-$runtime_class}"
kubectl_context="${KUBECTL_CONTEXT:-kind-${cluster}}"

if ! command -v kind >/dev/null 2>&1; then
  echo "kind is required" >&2
  exit 1
fi
if ! command -v docker >/dev/null 2>&1; then
  echo "docker is required" >&2
  exit 1
fi

nodes="$(kind get nodes --name "$cluster")"
if [ -z "$nodes" ]; then
  echo "kind cluster $cluster has no nodes" >&2
  exit 1
fi

for node in $nodes; do
  echo "Installing gVisor runtime in kind node $node"
  docker exec -i "$node" bash -se -- "$runtime_handler" <<'NODE_SCRIPT'
set -euo pipefail
handler="$1"

if ! command -v curl >/dev/null 2>&1 && ! command -v wget >/dev/null 2>&1; then
  apt-get update
  apt-get install -y ca-certificates curl wget
fi

if command -v runsc >/dev/null 2>&1 && command -v containerd-shim-runsc-v1 >/dev/null 2>&1; then
  echo "gVisor binaries already installed"
else
  tmp="$(mktemp -d)"
  trap 'rm -rf "$tmp"' EXIT
  cd "$tmp"
  arch="$(uname -m)"
  url="https://storage.googleapis.com/gvisor/releases/release/latest/${arch}"
  if command -v curl >/dev/null 2>&1; then
    curl -fsSLO "${url}/runsc"
    curl -fsSLO "${url}/runsc.sha512"
    curl -fsSLO "${url}/containerd-shim-runsc-v1"
    curl -fsSLO "${url}/containerd-shim-runsc-v1.sha512"
  else
    wget "${url}/runsc" "${url}/runsc.sha512" "${url}/containerd-shim-runsc-v1" "${url}/containerd-shim-runsc-v1.sha512"
  fi
  sha512sum -c runsc.sha512
  sha512sum -c containerd-shim-runsc-v1.sha512
  chmod 0755 runsc containerd-shim-runsc-v1
  install -m 0755 runsc containerd-shim-runsc-v1 /usr/local/bin/
fi

mkdir -p /etc/containerd/conf.d
cat >/etc/containerd/runsc.toml <<'RUNSC_CONFIG'
[runsc_config]
  overlay2 = "none"
  file-access = "shared"
  directfs = "true"
RUNSC_CONFIG

config=/etc/containerd/config.toml
if ! grep -q 'conf.d/\*.toml' "$config"; then
  if grep -q '^imports[[:space:]]*=' "$config"; then
    tmp_config="$(mktemp)"
    awk '
      /^imports[[:space:]]*=/ {
        if ($0 ~ /\[[[:space:]]*\]/) {
          sub(/\[[[:space:]]*\]/, "[\"/etc/containerd/conf.d/*.toml\"]")
        } else {
          sub(/\][[:space:]]*$/, ", \"/etc/containerd/conf.d/*.toml\"]")
        }
      }
      { print }
    ' "$config" >"$tmp_config"
    cat "$tmp_config" >"$config"
    rm -f "$tmp_config"
  else
    tmp_config="$(mktemp)"
    awk '
      !inserted && /^version[[:space:]]*=/ {
        print
        print "imports = [\"/etc/containerd/conf.d/*.toml\"]"
        inserted = 1
        next
      }
      { print }
      END {
        if (!inserted) {
          print "imports = [\"/etc/containerd/conf.d/*.toml\"]"
        }
      }
    ' "$config" >"$tmp_config"
    cat "$tmp_config" >"$config"
    rm -f "$tmp_config"
  fi
fi

version="$(awk -F= '/^version[[:space:]]*=/{gsub(/[[:space:]]/, "", $2); print $2; exit}' "$config")"
if [ "$version" = "3" ]; then
  runtime_plugin='io.containerd.cri.v1.runtime'
else
  runtime_plugin='io.containerd.grpc.v1.cri'
fi

cat >/etc/containerd/conf.d/sandbox0-gvisor.toml <<EOF
[plugins."${runtime_plugin}".containerd.runtimes."${handler}"]
  runtime_type = "io.containerd.runsc.v1"
[plugins."${runtime_plugin}".containerd.runtimes."${handler}".options]
  TypeUrl = "io.containerd.runsc.v1.options"
  ConfigPath = "/etc/containerd/runsc.toml"
EOF

systemctl restart containerd
runsc --version
NODE_SCRIPT
done

if command -v kubectl >/dev/null 2>&1; then
  cat <<EOF | kubectl --context "$kubectl_context" apply -f -
apiVersion: node.k8s.io/v1
kind: RuntimeClass
metadata:
  name: ${runtime_class}
handler: ${runtime_handler}
EOF
fi
