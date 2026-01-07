#!/usr/bin/env bash

set -o errexit
set -o nounset
set -o pipefail

SCRIPT_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
MODULE_NAME="github.com/sandbox0-ai/infra"
API_PKG="${MODULE_NAME}/manager/pkg/apis"
OUTPUT_PKG="${MODULE_NAME}/manager/pkg/generated"

OUTPUT_BASE=$(mktemp -d)
trap "rm -rf ${OUTPUT_BASE}" EXIT

echo "Generating deepcopy..."
go run k8s.io/code-generator/cmd/deepcopy-gen@v0.29.0 \
    --go-header-file="${SCRIPT_ROOT}/hack/boilerplate.go.txt" \
    --input-dirs="${API_PKG}/sandbox0/v1alpha1" \
    --output-file-base=zz_generated.deepcopy \
    --output-base="${OUTPUT_BASE}"

echo "Generating clientset..."
go run k8s.io/code-generator/cmd/client-gen@v0.29.0 \
    --go-header-file="${SCRIPT_ROOT}/hack/boilerplate.go.txt" \
    --clientset-name="versioned" \
    --input-base="" \
    --input="${API_PKG}/sandbox0/v1alpha1" \
    --output-package="${OUTPUT_PKG}/clientset" \
    --output-base="${OUTPUT_BASE}"

echo "Generating listers..."
go run k8s.io/code-generator/cmd/lister-gen@v0.29.0 \
    --go-header-file="${SCRIPT_ROOT}/hack/boilerplate.go.txt" \
    --input-dirs="${API_PKG}/sandbox0/v1alpha1" \
    --output-package="${OUTPUT_PKG}/listers" \
    --output-base="${OUTPUT_BASE}"

echo "Generating informers..."
go run k8s.io/code-generator/cmd/informer-gen@v0.29.0 \
    --go-header-file="${SCRIPT_ROOT}/hack/boilerplate.go.txt" \
    --input-dirs="${API_PKG}/sandbox0/v1alpha1" \
    --versioned-clientset-package="${OUTPUT_PKG}/clientset/versioned" \
    --listers-package="${OUTPUT_PKG}/listers" \
    --output-package="${OUTPUT_PKG}/informers" \
    --output-base="${OUTPUT_BASE}"

echo "Copying generated files..."
cp -R "${OUTPUT_BASE}/${MODULE_NAME}/manager/pkg/"* "${SCRIPT_ROOT}/pkg/"

echo "Done!"

