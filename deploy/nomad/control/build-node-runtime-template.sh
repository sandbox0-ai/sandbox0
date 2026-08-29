#!/usr/bin/env bash
set -euo pipefail

source_root=""
output=""
usage() {
  cat >&2 <<'EOF'
Usage: build-node-runtime-template.sh --source DIR --output FILE

Builds the immutable control-plane-owned configuration template returned only
to signed ESS workers. Files may end in .tmpl; manager renders those files with
the exact RuntimeConfigIdentity before enrollment completes.
EOF
  exit 64
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --source) source_root="$2"; shift 2 ;;
    --output) output="$2"; shift 2 ;;
    -h|--help) usage ;;
    *) usage ;;
  esac
done
[[ -n "${source_root}" && -n "${output}" ]] || usage
source_root="$(cd -- "${source_root}" && pwd)"
output="$(python3 - "${output}" <<'PY'
import pathlib
import sys
print(pathlib.Path(sys.argv[1]).expanduser().resolve())
PY
)"
install -d -m 0755 "$(dirname -- "${output}")"

python3 - "${source_root}" "${output}" <<'PY'
import gzip
import io
import os
import pathlib
import stat
import sys
import tarfile

source = pathlib.Path(sys.argv[1])
output = pathlib.Path(sys.argv[2])
required = {
    "etc/sandbox0/ctld.yaml",
    "etc/sandbox0/ctld-networking.yaml",
    "etc/sandbox0/ctld.env",
    "etc/sandbox0/ctld-a.env",
    "etc/sandbox0/ctld-b.env",
    "etc/sandbox0/internal-auth/data-public.pem",
    "etc/sandbox0/pki/manager-ca.pem",
    "etc/sandbox0/tokens/nomad.token",
    "etc/nomad.d/30-sandbox0-gvisor.hcl",
    "opt/cni/config/10-sandbox0.conflist",
}

def allowed(name):
    return (name.startswith("etc/sandbox0/") and len(name) > len("etc/sandbox0/")) or name in {
        "etc/nomad.d/30-sandbox0-gvisor.hcl",
        "opt/cni/config/10-sandbox0.conflist",
    }

files = []
rendered = set()
for path in sorted(source.rglob("*")):
    relative = path.relative_to(source).as_posix()
    info = path.lstat()
    if stat.S_ISDIR(info.st_mode):
        continue
    if not stat.S_ISREG(info.st_mode) or path.is_symlink():
        raise SystemExit("template source contains a non-regular path: %s" % relative)
    output_name = relative[:-5] if relative.endswith(".tmpl") else relative
    if not allowed(output_name):
        raise SystemExit("template source path is outside the node contract: %s" % output_name)
    if output_name in rendered:
        raise SystemExit("template source renders a duplicate path: %s" % output_name)
    rendered.add(output_name)
    mode = stat.S_IMODE(info.st_mode)
    if mode not in (0o600, 0o644):
        raise SystemExit("template source mode must be 0600 or 0644: %s" % relative)
    payload = path.read_bytes()
    if not payload or len(payload) > 2 << 20:
        raise SystemExit("template source file size is invalid: %s" % relative)
    files.append((relative, mode, payload))

missing = sorted(required - rendered)
if missing:
    raise SystemExit("template source is missing: %s" % ", ".join(missing))

by_output = {name[:-5] if name.endswith(".tmpl") else name: (name, payload) for name, _, payload in files}
for output_name, markers in {
    "etc/sandbox0/ctld.env": (
        b"{{.NodeName}}", b"{{.NodeID}}", b"{{.NodeUID}}",
        b"{{.RegionID}}", b"{{.ClusterID}}",
    ),
    "opt/cni/config/10-sandbox0.conflist": (b"{{.AllocationCIDR}}",),
}.items():
    source_name, payload = by_output[output_name]
    if not source_name.endswith(".tmpl") or any(marker not in payload for marker in markers):
        raise SystemExit("identity-bearing template is missing exact markers: %s" % output_name)

buffer = io.BytesIO()
with gzip.GzipFile(fileobj=buffer, mode="wb", mtime=0) as compressed:
    with tarfile.open(fileobj=compressed, mode="w") as archive:
        for relative, mode, payload in files:
            info = tarfile.TarInfo("node-runtime-template/" + relative)
            info.size = len(payload)
            info.mode = mode
            info.uid = info.gid = 0
            info.uname = info.gname = ""
            info.mtime = 0
            archive.addfile(info, io.BytesIO(payload))

temporary = output.with_name(output.name + ".tmp")
temporary.write_bytes(buffer.getvalue())
temporary.chmod(0o600)
os.replace(temporary, output)
print("archive=%s" % output)
PY
