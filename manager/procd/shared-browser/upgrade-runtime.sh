#!/usr/bin/env bash
# Run inside an existing sandbox after its Browser service has been stopped.
# SOURCE is /opt/sandbox0-browser extracted from the exact verified template image.
set -euo pipefail
[[ $(id -u) == 0 ]] || { echo 'Browser upgrade requires sandbox root' >&2; exit 1; }
source_root="$(realpath "${1:?Verified runtime source directory required}")"
[[ "$source_root" != /opt/sandbox0-browser ]] || { echo 'Stage the source separately' >&2; exit 1; }
[[ "$(node "$source_root/browser.cjs" --version)" == 'sandbox0-browser 2' ]]
test -f "$source_root/novnc/core/rfb.js"
test -f "$source_root/novnc/LICENSE.txt"
if [[ -d /opt/sandbox0-browser ]] && diff -qr "$source_root" /opt/sandbox0-browser >/dev/null; then
  install -m 0755 /opt/sandbox0-browser/sandbox0-browser /usr/local/bin/sandbox0-browser
  echo 'Exact browser runtime is already installed'
  exit 0
fi
exec 9>/tmp/sandbox0-browser.lock
flock -n 9 || { echo 'Stop the Browser service before upgrading' >&2; exit 1; }
node - <<'JS'
const net = require('node:net');
Promise.all([5900, 9222, 9224].map(port => new Promise(resolve => {
  const socket = net.connect({ host: '127.0.0.1', port });
  socket.once('connect', () => { socket.destroy(); resolve(port); });
  socket.once('error', () => { socket.destroy(); resolve(null); });
}))).then(ports => {
  if (ports.some(Boolean)) { console.error('Browser ports are still occupied; upgrade refused'); process.exitCode = 1; }
});
JS
# Package changes affect only this guest filesystem. Never replace its RootFS.
if ! command -v tint2 >/dev/null || ! command -v feh >/dev/null || ! command -v wmctrl >/dev/null || ! command -v xprop >/dev/null || ! command -v Xtigervnc >/dev/null || ! command -v openbox >/dev/null; then
  timeout 300 bash -c 'set -e; exec 8>/tmp/sandbox0-browser-packages.lock; flock -w 180 8; apt-get -o DPkg::Lock::Timeout=120 update -qq; DEBIAN_FRONTEND=noninteractive apt-get -o DPkg::Lock::Timeout=120 install -y --no-install-recommends tint2 feh wmctrl x11-utils tigervnc-standalone-server openbox'
fi
command -v Xtigervnc >/dev/null
command -v openbox >/dev/null
getent passwd sandbox-browser >/dev/null
if ! command -v google-chrome-stable >/dev/null; then
  timeout 600 bash "$source_root/install-chrome-stable.sh"
fi
staging="$(mktemp -d /opt/sandbox0-browser-upgrade.XXXXXX)"
cleanup() { rm -rf "$staging"; }
trap cleanup EXIT
cp -a "$source_root/." "$staging/"
node --check "$staging/browser.cjs"
node --check "$staging/desktop.cjs"
node --check "$staging/supervisor.cjs"
backup="/opt/sandbox0-browser-before-v2"
if [[ -e /opt/sandbox0-browser ]]; then
  [[ ! -e "$backup" ]] || { echo 'Previous runtime backup exists; reconcile before retrying' >&2; exit 1; }
  mv /opt/sandbox0-browser "$backup"
fi
if ! mv "$staging" /opt/sandbox0-browser; then
  [[ ! -e "$backup" ]] || mv "$backup" /opt/sandbox0-browser
  exit 1
fi
install -m 0755 /opt/sandbox0-browser/sandbox0-browser /usr/local/bin/sandbox0-browser
sandbox0-browser --version
echo 'Browser runtime upgraded; profile and Agent processes were preserved'
