#!/usr/bin/env bash
set -euo pipefail

cni="${1:-}"
if [[ -z "${cni}" ]]; then
  echo "usage: $0 <flannel|canal|cilium-native-host-legacy>" >&2
  exit 2
fi

FLANNEL_VERSION="${FLANNEL_VERSION:-v0.28.1}"
CALICO_VERSION="${CALICO_VERSION:-v3.32.0}"
CILIUM_VERSION="${CILIUM_VERSION:-1.19.4}"
CNI_PLUGINS_VERSION="${CNI_PLUGINS_VERSION:-v1.8.0}"
POD_CIDR="${POD_CIDR:-10.244.0.0/16}"

run_privileged() {
  if [[ "$(id -u)" == "0" ]]; then
    "$@"
  elif command -v sudo >/dev/null 2>&1; then
    sudo "$@"
  else
    "$@"
  fi
}

ensure_bridge_netfilter() {
  run_privileged modprobe br_netfilter || true
  run_privileged sysctl -w net.bridge.bridge-nf-call-iptables=1 >/dev/null || true
  run_privileged sysctl -w net.bridge.bridge-nf-call-ip6tables=1 >/dev/null || true
}

linux_arch() {
  case "$(uname -m)" in
    x86_64)
      echo amd64
      ;;
    aarch64 | arm64)
      echo arm64
      ;;
    *)
      uname -m
      ;;
  esac
}

ensure_kind_bridge_plugin() {
  if ! command -v docker >/dev/null 2>&1; then
    return
  fi

  local nodes
  nodes="$(kubectl get nodes -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}')"
  if [[ -z "${nodes}" ]]; then
    return
  fi

  local missing=false
  local node
  for node in ${nodes}; do
    if ! docker exec "${node}" test -x /opt/cni/bin/bridge >/dev/null 2>&1; then
      missing=true
      break
    fi
  done
  if [[ "${missing}" != "true" ]]; then
    return
  fi

  local tmp
  tmp="$(mktemp -d)"

  local archive="${tmp}/cni-plugins.tgz"
  local arch
  arch="$(linux_arch)"
  curl -fsSL -o "${archive}" "https://github.com/containernetworking/plugins/releases/download/${CNI_PLUGINS_VERSION}/cni-plugins-linux-${arch}-${CNI_PLUGINS_VERSION}.tgz"
  tar -xzf "${archive}" -C "${tmp}" ./bridge

  for node in ${nodes}; do
    if docker exec "${node}" test -x /opt/cni/bin/bridge >/dev/null 2>&1; then
      continue
    fi
    docker cp "${tmp}/bridge" "${node}:/opt/cni/bin/bridge"
    docker exec "${node}" chmod +x /opt/cni/bin/bridge
  done
  rm -rf "${tmp}"
}

wait_for_cluster_network() {
  kubectl wait node --all --for=condition=Ready --timeout=5m
  kubectl -n kube-system rollout status deployment/coredns --timeout=5m
}

install_flannel() {
  kubectl apply -f "https://raw.githubusercontent.com/flannel-io/flannel/${FLANNEL_VERSION}/Documentation/kube-flannel.yml"
  kubectl -n kube-flannel rollout status daemonset/kube-flannel-ds --timeout=5m
  wait_for_cluster_network
}

install_canal() {
  kubectl apply -f "https://raw.githubusercontent.com/projectcalico/calico/${CALICO_VERSION}/manifests/canal.yaml"
  kubectl -n kube-system rollout status daemonset/canal --timeout=5m
  kubectl -n kube-system rollout status deployment/calico-kube-controllers --timeout=5m
  wait_for_cluster_network
}

install_cilium_native_host_legacy() {
  helm repo add cilium https://helm.cilium.io/ --force-update >/dev/null
  helm repo update cilium >/dev/null
  helm upgrade --install cilium cilium/cilium \
    --version "${CILIUM_VERSION}" \
    --namespace kube-system \
    --set routingMode=native \
    --set autoDirectNodeRoutes=true \
    --set ipv4NativeRoutingCIDR="${POD_CIDR}" \
    --set ipam.mode=kubernetes \
    --set k8s.requireIPv4PodCIDR=true \
    --set bpf.hostLegacyRouting=true \
    --set enableIPv4Masquerade=false \
    --set kubeProxyReplacement=false \
    --set operator.replicas=1
  kubectl -n kube-system rollout status daemonset/cilium --timeout=8m
  kubectl -n kube-system rollout status deployment/cilium-operator --timeout=8m
  wait_for_cluster_network
}

case "${cni}" in
  flannel)
    ensure_bridge_netfilter
    ensure_kind_bridge_plugin
    install_flannel
    ;;
  canal)
    ensure_bridge_netfilter
    ensure_kind_bridge_plugin
    install_canal
    ;;
  cilium-native-host-legacy)
    install_cilium_native_host_legacy
    ;;
  *)
    echo "unsupported cni: ${cni}" >&2
    exit 2
    ;;
esac
