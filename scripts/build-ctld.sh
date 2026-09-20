#!/bin/sh
# Build procd and ctld for the same target without writing generated files into
# the source tree. Go's overlay also resolves go:embed input files.
set -eu
root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
output=${1:?usage: build-ctld.sh OUTPUT [PREBUILT_PROCD]}
output=$(python3 -c 'import os,sys; print(os.path.abspath(sys.argv[1]))' "$output")
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT HUP INT TERM
cd "$root"
if [ "$#" -gt 1 ]; then
    procd=$(python3 -c 'import os,sys; print(os.path.abspath(sys.argv[1]))' "$2")
else
    procd="$work/procd"
    CGO_ENABLED=0 go build -trimpath -buildvcs=false -ldflags='-s -w' -o "$procd" ./manager/cmd/procd
fi
python3 - "$root" "$procd" "$work/overlay.json" <<'PYTHON'
import json, pathlib, sys
root, procd, output = sys.argv[1:]
pathlib.Path(output).write_text(json.dumps({"Replace": {str(pathlib.Path(root) / "ctld/internal/procdassets/procd.bin"): procd}}))
PYTHON
CGO_ENABLED=0 go build -trimpath -buildvcs=false -ldflags='-s -w' -tags=procd_bundle -overlay="$work/overlay.json" -o "$output" ./ctld/cmd/ctld
