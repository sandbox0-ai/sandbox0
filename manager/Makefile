.PHONY: generate
generate: ## Generate clientset, informers, and listers
	@echo "Generating code..."
	@bash hack/update-codegen.sh

.PHONY: verify-codegen
verify-codegen: ## Verify generated code is up to date
	@echo "Verifying generated code..."
	@bash hack/verify-codegen.sh

.PHONY: install-tools
install-tools: ## Install code generation tools
	@echo "Installing code generation tools..."
	@go install k8s.io/code-generator/cmd/client-gen@latest
	@go install k8s.io/code-generator/cmd/lister-gen@latest
	@go install k8s.io/code-generator/cmd/informer-gen@latest
	@go install k8s.io/code-generator/cmd/deepcopy-gen@latest
	@go install sigs.k8s.io/controller-tools/cmd/controller-gen@latest

.PHONY: manifests
manifests: ## Generate CRD manifests
	@echo "Generating CRD manifests..."
	@controller-gen crd:crdVersions=v1 paths="./pkg/apis/sandbox0/v1alpha1/..." output:crd:dir=./deploy/k8s

.PHONY: build
build: ## Build the manager binary
	@echo "Building manager..."
	@go build -o bin/manager ./cmd/manager

.PHONY: test
test: ## Run tests
	@echo "Running tests..."
	@go test -v ./...

.PHONY: lint
lint: ## Run linter
	@echo "Running linter..."
	@golangci-lint run

.PHONY: help
help: ## Display this help
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n"} /^[a-zA-Z_-]+:.*?##/ { printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)
