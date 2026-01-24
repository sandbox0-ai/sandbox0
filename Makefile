.PHONY: all build test test-all test-integration test-integration-verbose test-e2e test-e2e-kind test-e2e-destroy test-e2e-specific lint tidy vendor clean helm-update helm-configs release docker-build docker-build-local build-local-all docker-push proto manifests

# Tool Binaries
LOCALBIN ?= $(shell pwd)/bin
$(LOCALBIN):
	mkdir -p $(LOCALBIN)

CONTROLLER_GEN ?= $(LOCALBIN)/controller-gen
CONTROLLER_TOOLS_VERSION ?= v0.20.0

SERVICES := edge-gateway internal-gateway manager scheduler storage-proxy k8s-plugin procd infra-operator

# Default version
VERSION ?= latest
TAG ?= $(VERSION)

# Colors for output
YELLOW := \033[1;33m
GREEN  := \033[1;32m
CYAN   := \033[1;36m
RESET  := \033[0m

all: manifests proto
	@for service in $(SERVICES); do \
		$(MAKE) build SERVICE=$$service; \
	done

# Build specific service: make build <service>
build: manifests proto
	@service="$(filter-out build test test-all lint tidy vendor clean helm-update docker-build docker-build-local build-local-all docker-push,$(MAKECMDGOALS))"; \
	[ -z "$$service" ] && service="$(SERVICE)"; \
	for s in $$service; do \
		if ! echo "$(SERVICES)" | grep -qw "$$s"; then \
			echo "Error: Unknown service '$$s'"; exit 1; \
		fi; \
		printf "$(GREEN)Building $$s...$(RESET)\n"; \
		if [ "$$s" = "procd" ]; then \
			dir="manager"; bin="procd"; src="./manager/cmd/procd"; \
		elif [ "$$s" = "infra-operator" ]; then \
			$(MAKE) operator-manifests; \
			dir="infra-operator"; bin="infra-operator"; src="./infra-operator/cmd/infra-operator"; \
		elif [ "$$s" = "k8s-plugin" ]; then \
			dir="k8s-plugin"; bin="k8s-plugin"; src="./k8s-plugin"; \
		else \
			dir="$$s"; bin="$$s"; src="./$$s/cmd/$$s"; \
		fi; \
		if [ -n "$(BIN_DIR)" ]; then \
			mkdir -p $(BIN_DIR); \
			out="$(BIN_DIR)/$$s"; \
		else \
			mkdir -p $$dir/bin; \
			out="$$dir/bin/$$bin"; \
		fi; \
		if [ "$$s" = "storage-proxy" ] || [ "$$s" = "infra-operator" ]; then \
			CGO_ENABLED=1 go build -v -o $$out $$src; \
		else \
			CGO_ENABLED=0 go build -v -o $$out $$src; \
		fi || exit 1; \
	done

docker-build:
	@printf "$(GREEN)Docker building unified infra image...$(RESET)\n"
	docker build -t sandbox0ai/infra:$(TAG) -f Dockerfile .
	#docker buildx build --platform=linux/amd64 -t sandbox0ai/infra:$(TAG) -f Dockerfile .

docker-push:
	@printf "$(GREEN)Docker pushing unified infra image...$(RESET)\n"
	docker push sandbox0ai/infra:$(TAG)

build-local-all: manifests proto
	@for service in $(SERVICES); do \
		$(MAKE) build SERVICE=$$service BIN_DIR=$(shell pwd)/bin; \
	done

# NOTE: This only works on Linux because storage-proxy and infra-operator cannot be cross-compiled.
docker-build-local: build-local-all
	@printf "$(GREEN)Docker building with local binaries...$(RESET)\n"
	docker build -t sandbox0ai/infra:$(TAG) -f Dockerfile.local .

test:
	@service="$(filter-out build test test-all lint tidy vendor clean helm-update,$(MAKECMDGOALS))"; \
	if [ -z "$$service" ]; then \
		echo "Available services: $(SERVICES)"; \
		echo "Usage: make test <service> or make test-all"; \
		exit 1; \
	elif echo "$(SERVICES)" | grep -qw "$$service"; then \
		printf "$(CYAN)Testing $$service...$(RESET)\n"; \
		if [ "$$service" = "edge-gateway" ]; then \
			GOTOOLCHAIN=go1.25.0+auto go test -v -race -cover ./edge-gateway/...; \
		elif [ "$$service" = "internal-gateway" ]; then \
			GOTOOLCHAIN=go1.25.0+auto go test -v -race -cover ./internal-gateway/...; \
		elif [ "$$service" = "manager" ]; then \
			GOTOOLCHAIN=go1.25.0+auto go test -v -race -cover ./manager/...; \
		elif [ "$$service" = "procd" ]; then \
			GOTOOLCHAIN=go1.25.0+auto go test -v -race -cover ./manager/procd/...; \
		elif [ "$$service" = "scheduler" ]; then \
			GOTOOLCHAIN=go1.25.0+auto go test -v -race -cover ./scheduler/...; \
		elif [ "$$service" = "storage-proxy" ]; then \
			GOTOOLCHAIN=go1.25.0+auto go test -v -race -cover ./storage-proxy/...; \
		elif [ "$$service" = "k8s-plugin" ]; then \
			GOTOOLCHAIN=go1.25.0+auto go test -v -race -cover ./k8s-plugin/...; \
		elif [ "$$service" = "infra-operator" ]; then \
			GOTOOLCHAIN=go1.25.0+auto go test -v -race -cover ./infra-operator/...; \
		fi; \
	else \
		echo "Error: Unknown service '$$service'"; \
		echo "Available services: $(SERVICES)"; \
		exit 1; \
	fi

test-all:
	@for service in $(SERVICES); do \
		printf "$(CYAN)Testing $$service...$(RESET)\n"; \
		$(MAKE) test $$service || exit 1; \
	done

# Integration tests
test-integration:
	@printf "$(CYAN)Running integration tests...$(RESET)\n"
	GOTOOLCHAIN=go1.25.0+auto go test -v -race -cover ./tests/integration/... -timeout=10m

test-integration-verbose:
	@printf "$(CYAN)Running integration tests (verbose)...$(RESET)\n"
	GOTOOLCHAIN=go1.25.0+auto go test -v -race -cover ./tests/integration/... -timeout=10m -v

# E2E test mode: all, control-plane, data-plane, combined
E2E_TEST_MODE ?= combined

# E2E tests
test-e2e:
	@printf "$(CYAN)Running E2E tests (mode: $(E2E_TEST_MODE))...$(RESET)\n"
	unset http_proxy && unset https_proxy && unset all_proxy && E2E_TEST_MODE=$(E2E_TEST_MODE) go test -v -count=1 ./tests/e2e/... -timeout=30m

test-e2e-kind:
	@printf "$(CYAN)Creating Kind cluster...$(RESET)\n"
	unset http_proxy && unset https_proxy && unset all_proxy && kind create cluster --config tests/e2e/kind-config.yaml --name sandbox0-e2e

test-e2e-destroy:
	@printf "$(YELLOW)Destroying Kind cluster...$(RESET)\n"
	kind delete cluster --name sandbox0-e2e

test-e2e-specific:
	@if [ -z "$(SPEC)" ]; then \
		echo "Error: SPEC is required"; \
		echo "Usage: make test-e2e-specific SPEC=Describe/It"; \
		exit 1; \
	fi
	@printf "$(CYAN)Running E2E test: $(SPEC)...$(RESET)\n"
	unset http_proxy && unset https_proxy && unset all_proxy && go test -v ./tests/e2e/... -focus="$(SPEC)" -timeout=30m

# Prevent make from treating service names as targets
edge-gateway internal-gateway manager scheduler storage-proxy k8s-plugin procd infra-operator:
	@:

lint:
	golangci-lint run ./...

tidy:
	go mod tidy

vendor:
	go mod vendor

clean:
	@for service in $(SERVICES); do \
		printf "$(YELLOW)Cleaning $$service...$(RESET)\n"; \
		if [ "$$service" = "procd" ]; then \
			rm -rf manager/bin/procd; \
		else \
			rm -rf $$service/bin; \
		fi; \
	done
	rm -rf storage-proxy/proto/fs/*.pb.go
	rm -rf vendor
	rm -rf bin

app-configs:
	@printf "$(CYAN)Generating default Helm configs...$(RESET)\n"
	@CONFIG_PATH=/dev/null go run ./tools/configdump

proto:
	@printf "$(CYAN)Generating storage-proxy protobufs...$(RESET)\n"
	@mkdir -p storage-proxy/proto/fs
	@protoc --go_out=. --go_opt=paths=source_relative \
		--go-grpc_out=. --go-grpc_opt=paths=source_relative \
		storage-proxy/proto/filesystem.proto
	@mv storage-proxy/proto/*.pb.go storage-proxy/proto/fs/

.PHONY: controller-gen
controller-gen: $(CONTROLLER_GEN)
$(CONTROLLER_GEN): $(LOCALBIN)
	@test -s $(LOCALBIN)/controller-gen && $(LOCALBIN)/controller-gen --version | grep -q $(CONTROLLER_TOOLS_VERSION) || \
	GOBIN=$(LOCALBIN) go install sigs.k8s.io/controller-tools/cmd/controller-gen@$(CONTROLLER_TOOLS_VERSION)

manifests: controller-gen
	@printf "$(CYAN)Generating manager CRDs...$(RESET)\n"
	@$(CONTROLLER_GEN) crd paths="./manager/pkg/apis/..." output:crd:artifacts:config=infra-operator/chart/crds/

.PHONY: operator-manifests
operator-manifests: controller-gen
	@printf "$(CYAN)Generating infra-operator manifests...$(RESET)\n"
	@$(CONTROLLER_GEN) rbac:roleName=manager-role crd webhook object paths="./infra-operator/..." output:crd:artifacts:config=infra-operator/chart/crds output:rbac:artifacts:config=infra-operator/chart/crds output:webhook:artifacts:config=infra-operator/chart/crds
	@mv infra-operator/chart/crds/role.yaml infra-operator/chart/files/clusterrole.yaml

.PHONY: operator-install
operator-install: operator-manifests
	kubectl apply -f infra-operator/chart/crds/

.PHONY: operator-run
operator-run: operator-install
	go run ./infra-operator/cmd/infra-operator
