#!/usr/bin/env bash
# Install the production stock runsc release only on a disposable Actions runner.
set -euo pipefail
[[ "${GITHUB_ACTIONS:-}" == true && "$(uname -s)" == Linux ]]
image="${1:?immutable build image or unique CI tag required}"
case "$(uname -m)" in
  x86_64) arch=x86_64; digest=94a1b9716797efcb0b348fc3283ddaa1ee5c7898b61eeccb08ee9ac7dc903c15 ;;
  aarch64) arch=aarch64; digest=218cef646c1c04a62d9acb988127e6da7c4f9e5aaefeef6929893df7a349ecb9 ;;
  *) exit 1 ;;
esac
work=$(mktemp -d "${RUNNER_TEMP}/dind-runsc.XXXXXXXX")
name="sandbox0-dind-ci-${GITHUB_RUN_ID}-${GITHUB_RUN_ATTEMPT}"
trap 'docker rm -f "${name}" >/dev/null 2>&1 || true' EXIT
curl --fail --location --retry 3 \
  "https://github.com/google/gvisor/releases/download/release-20260914.0/gvisor-${arch}.tar.bz2" \
  -o "${work}/runsc.tar.bz2"
printf '%s  %s\n' "${digest}" "${work}/runsc.tar.bz2" | sha256sum --check --status
tar -xjf "${work}/runsc.tar.bz2" -C "${work}"
sudo "${work}/runsc" install
sudo systemctl restart docker
# The pushed image is the test subject. Reclaim the ephemeral builder's cache
# before pulling large agent images into the runner's Docker image store.
docker buildx prune --all --force
docker pull "${image}"
timeout 600 docker run --name "${name}" --runtime=runsc --cap-add=ALL \
  --memory=4g --tmpfs /var/lib/docker:rw,nosuid,size=1g \
  --mount "type=bind,src=$(pwd)/tools/docker-in-sandbox/smoke.sh,dst=/tmp/dind-smoke.sh,readonly" \
  "${image}" bash /tmp/dind-smoke.sh
