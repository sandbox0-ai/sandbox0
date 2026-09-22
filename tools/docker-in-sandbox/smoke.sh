#!/usr/bin/env bash
# Run inside a disposable sandbox configured for Docker in Sandbox.
# Tests real image extraction, nested process creation, builds, and networking.
set -euo pipefail

name="sandbox0-dind-smoke-$$"
work=$(mktemp -d /tmp/sandbox0-dind-smoke.XXXXXXXX)
daemon_pid=""
cleanup() {
  result=$?
  if [ "${result}" -ne 0 ]; then docker logs "${name}" >&2 2>/dev/null || true; fi
  docker rm -f "${name}" >/dev/null 2>&1 || true
  docker image rm "${name}" >/dev/null 2>&1 || true
  if [ -n "${daemon_pid}" ]; then
    kill "${daemon_pid}" 2>/dev/null || true
    wait "${daemon_pid}" 2>/dev/null || true
  fi
  rm -rf "${work}"
}
trap cleanup EXIT

if ! docker info >/dev/null 2>&1; then
  /usr/local/bin/sandbox0-dockerd-entrypoint >"${work}/daemon.log" 2>&1 &
  daemon_pid=$!
  ready=false
  for ((attempt = 0; attempt < 60; attempt++)); do
    if docker info >/dev/null 2>&1; then
      ready=true
      break
    fi
    if ! kill -0 "${daemon_pid}" 2>/dev/null; then break; fi
    sleep 1
  done
  if [ "${ready}" != true ]; then
    cat "${work}/daemon.log" >&2
    exit 1
  fi
fi

docker info --format 'Docker {{.ServerVersion}}, storage {{.Driver}}'
# Pin the multi-platform Alpine 3.21 manifest used by the acceptance test.
base="alpine@sha256:ce64758a109eb420d874a118f87920e625e12d3634e03b4a5573fd9f6e5d3507"
timeout 180 docker pull "${base}"
test "$(timeout 60 docker run --rm --network=host "${base}" echo nested-run-ok)" = nested-run-ok
cat >"${work}/Dockerfile" <<EOF
FROM ${base}
RUN mkdir /www && echo nested-build-ok > /www/index.html
COPY serve.sh /serve.sh
CMD ["sh", "/serve.sh"]
EOF
cat >"${work}/serve.sh" <<'EOF'
while true; do
    { printf 'HTTP/1.0 200 OK\r\nContent-Length: 16\r\n\r\n'; cat /www/index.html; } | nc -l -p 18880
done
EOF
timeout 180 docker build --network=host -t "${name}" "${work}"
docker run -d --name "${name}" --network=host "${name}"
for ((attempt = 0; attempt < 30; attempt++)); do
  if curl --noproxy '*' --fail --silent --max-time 2 http://127.0.0.1:18880/ >"${work}/response"; then break; fi
  sleep 1
done
test "$(cat "${work}/response")" = nested-build-ok
echo 'PASS: pull, run, build with RUN, and sandbox-local service networking'
