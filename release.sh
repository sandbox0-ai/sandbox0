#!/usr/bin/env bash
set -euo pipefail

VERSION="${1:-}"
if [[ -z "${VERSION}" ]]; then
  echo "usage: $0 v0.1.0" >&2
  exit 2
fi

# Helm chart `version` must be SemVer. We accept `v0.1.0` and write `0.1.0`.
CHART_VERSION="${VERSION#v}"

if ! [[ "${CHART_VERSION}" =~ ^[0-9]+\.[0-9]+\.[0-9]+([\-+].*)?$ ]]; then
  echo "error: VERSION must look like v0.1.0 (got: ${VERSION})" >&2
  exit 2
fi

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CHART_YAML="${ROOT_DIR}/infra/helm/Chart.yaml"
VALUES_YAML="${ROOT_DIR}/infra/helm/values.yaml"

if ! command -v python3 >/dev/null 2>&1; then
  echo "error: python3 is required" >&2
  exit 1
fi

# 1) Update parent chart version/appVersion
python3 - <<'PY' "${CHART_YAML}" "${CHART_VERSION}" "${VERSION}"
import sys, re, pathlib

path = pathlib.Path(sys.argv[1])
chart_version = sys.argv[2]
app_version = sys.argv[3]

s = path.read_text()
s2 = re.sub(r'(?m)^version:\s*.*$', f"version: {chart_version}", s)
s2 = re.sub(r'(?m)^appVersion:\s*.*$', f'appVersion: "{app_version}"', s2)
if s2 == s:
    raise SystemExit(f"error: did not update {path} (unexpected format?)")
path.write_text(s2)
PY

# 2) Update global.tag in values.yaml (YAML anchor kept intact)
python3 - <<'PY' "${VALUES_YAML}" "${VERSION}"
import sys, re, pathlib

path = pathlib.Path(sys.argv[1])
v = sys.argv[2]

s = path.read_text()
s2, n = re.subn(
    r'(?m)^(\s*tag:\s*&globalTag\s*)"[^"]*"\s*$',
    rf'\1"{v}"',
    s,
)
if n != 1:
    raise SystemExit(f"error: expected to update exactly 1 global tag in {path}, updated {n}")
path.write_text(s2)
PY

if ! command -v git >/dev/null 2>&1; then
  echo "note: git not found; updated chart files only" >&2
  exit 0
fi

if ! git -C "${ROOT_DIR}" rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  echo "note: not a git repository; updated chart files only" >&2
  exit 0
fi

# 3) Commit and tag
git -C "${ROOT_DIR}" add "${CHART_YAML}" "${VALUES_YAML}"
git -C "${ROOT_DIR}" commit -m "release: ${VERSION}"

if git -C "${ROOT_DIR}" rev-parse "${VERSION}" >/dev/null 2>&1; then
  echo "error: git tag ${VERSION} already exists" >&2
  exit 1
fi
git -C "${ROOT_DIR}" tag "${VERSION}"

echo "Done: committed and tagged ${VERSION}"
