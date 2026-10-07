#!/usr/bin/env bash
set -euo pipefail

arch="$(dpkg --print-architecture)"
case "$arch" in
  amd64|arm64) ;;
  *) echo "Unsupported Chrome Stable architecture: $arch" >&2; exit 1 ;;
esac

work="$(mktemp -d /tmp/chrome-stable.XXXXXXXX)"
trap 'rm -rf -- "$work"' EXIT
curl -fsSL --retry 3 --max-time 180 \
  "https://dl.google.com/linux/direct/google-chrome-stable_current_${arch}.deb" \
  -o "$work/google-chrome-stable.deb"
test "$(dpkg-deb --field "$work/google-chrome-stable.deb" Package)" = google-chrome-stable
test "$(dpkg-deb --field "$work/google-chrome-stable.deb" Architecture)" = "$arch"
apt-get -o DPkg::Lock::Timeout=120 update -qq
DEBIAN_FRONTEND=noninteractive apt-get -o DPkg::Lock::Timeout=120 install -y --no-install-recommends "$work/google-chrome-stable.deb"
google-chrome-stable --version
# Retain Chrome's process sandbox for the non-root shared desktop.
test "$(stat -c '%a %U:%G' /opt/google/chrome/chrome-sandbox)" = '4755 root:root'
