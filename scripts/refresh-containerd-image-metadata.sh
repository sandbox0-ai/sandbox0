#!/bin/sh

set -eu

cluster_name="${1:-}"
image_ref="${2:-}"
platform="${3:-}"

if [ -z "${cluster_name}" ] || [ -z "${image_ref}" ]; then
  echo "usage: $0 <kind-cluster-name> <registry-image-ref> [platform]" >&2
  exit 2
fi

if [ -z "${platform}" ]; then
  first_node="$(kind get nodes --name "${cluster_name}" | head -n 1)"
  architecture="$(docker exec "${first_node}" uname -m)"
  case "${architecture}" in
    x86_64) architecture="amd64" ;;
    aarch64) architecture="arm64" ;;
  esac
  platform="linux/${architecture}"
fi

# docker save rewrites a pulled image into a local OCI manifest and may also
# change compressed layer identities. Fetch the canonical registry metadata and
# content so ctld records a digest the head publisher can resolve and kubelet
# can unpack every descriptor referenced by that manifest.
for node in $(kind get nodes --name "${cluster_name}"); do
  echo "Refreshing canonical registry content for ${image_ref} on ${node}..."
  docker exec "${node}" ctr --namespace=k8s.io content fetch \
    --platform "${platform}" \
    "${image_ref}" >/dev/null
done
