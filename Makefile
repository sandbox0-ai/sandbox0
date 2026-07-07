.PHONY: all build test test-all test-integration test-integration-verbose test-e2e test-e2e-kind test-e2e-destroy test-e2e-load-images test-e2e-prepare-kind test-e2e-setup-gvisor-rootfs test-e2e-specific test-e2e-mountpoint-s3-compat test-e2e-s0fs-posix test-e2e-s0fs-posix-prepare test-e2e-netd-cni lint tidy vendor clean helm-update helm-configs release docker-build docker-build-local build-local-all docker-push proto manifests apispec oapi-codegen

# Tool Binaries
LOCALBIN ?= $(shell pwd)/bin
$(LOCALBIN):
	mkdir -p $(LOCALBIN)

CONTROLLER_GEN ?= $(LOCALBIN)/controller-gen
CONTROLLER_TOOLS_VERSION ?= v0.20.0

OAPI_CODEGEN ?= $(LOCALBIN)/oapi-codegen
OAPI_CODEGEN_VERSION ?= v2.4.1

PROTOC ?= protoc
GO ?= env GOWORK=off go

SERVICES := regional-gateway ssh-gateway global-gateway cluster-gateway manager scheduler storage-proxy ctld procd netd infra-operator
E2E_SSH_FIXTURE_SOURCE_IMAGE := lscr.io/linuxserver/openssh-server@sha256:68b605929e83b2efe000da09269688f6d82a44579e8a18e2d9e8c8d272917cf7
E2E_SSH_FIXTURE_IMAGE := sandbox0ai/e2e-openssh-server:68b605929e83
E2E_DEPENDENCY_IMAGES := postgres:16-alpine rustfs/rustfs:1.0.0-alpha.79 registry:2.8.3 sandbox0ai/otemplates:default-v0.2.0 $(E2E_SSH_FIXTURE_IMAGE)
E2E_IMAGE_PLATFORM ?= linux/$(shell uname -m | sed -e 's/x86_64/amd64/' -e 's/aarch64/arm64/')
E2E_CLUSTER_NAME ?= sandbox0-e2e
S0FS_POSIX_PREPARE_INFRA ?= true
S0FS_POSIX_CI_INSTALL_DEPS ?= true
S0FS_POSIX_CI_GIT_FILE_COUNT ?= 120
S0FS_POSIX_CI_GIT_FILE_SIZE ?= 2048
S0FS_POSIX_CI_BULK_FILE_COUNT ?= 1000
S0FS_POSIX_CI_BULK_TOTAL_BYTES ?= 16777216
S0FS_POSIX_CI_BULK_CONCURRENCY ?= 4
S0FS_POSIX_CI_BULK_FSYNC_EVERY ?= 25
S0FS_POSIX_CI_ARCHIVE_FILE_COUNT ?= 200
S0FS_POSIX_CI_ARCHIVE_TOTAL_BYTES ?= 2097152
S0FS_POSIX_CI_ARCHIVE_CONCURRENCY ?= 4
S0FS_POSIX_CI_FSX_OPERATIONS ?= 1000
S0FS_POSIX_CI_FSSTRESS_OPERATIONS ?= 1000
S0FS_POSIX_CI_FSSTRESS_PROCESSES ?= 4

# Default version
VERSION ?= latest
TAG ?= $(VERSION)
PROCD_BIN_TAG ?= $(TAG)-procd-bin

# Colors for output
YELLOW := \033[1;33m
GREEN  := \033[1;32m
CYAN   := \033[1;36m
RESET  := \033[0m

all: manifests proto apispec
	@for service in $(SERVICES); do \
		$(MAKE) build SERVICE=$$service GOOS=$(GOOS); \
	done

# Build specific service: make build <service>
build: manifests proto apispec
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
			dir="infra-operator"; bin="infra-operator"; src="./infra-operator/cmd/infra-operator"; \
		elif [ "$$s" = "ctld" ]; then \
			dir="ctld"; bin="ctld"; src="./ctld/cmd/ctld"; \
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
			host_os="$$(uname -s)"; \
			if [ "$$host_os" != "Linux" ] && [ "$(GOOS)" = "linux" ]; then \
				printf "$(YELLOW)Skipping $$s: requires Linux host and GOOS=linux$(RESET)\n"; \
				continue; \
			fi; \
			CGO_ENABLED=1 GOOS=$(GOOS) $(GO) build -v -o $$out $$src; \
		else \
			CGO_ENABLED=0 GOOS=$(GOOS) $(GO) build -v -o $$out $$src; \
		fi || exit 1; \
		if [ "$$s" = "procd" ]; then \
			runner_out="$$(dirname $$out)/python-runner"; \
			CGO_ENABLED=0 GOOS=$(GOOS) $(GO) build -v -o $$runner_out ./manager/cmd/python-runner || exit 1; \
		fi; \
	done

docker-build:
	@printf "$(GREEN)Docker building unified infra image...$(RESET)\n"
	docker build -t sandbox0ai/infra:$(TAG) -f Dockerfile .
	docker build --target procd-bin -t sandbox0ai/infra:$(PROCD_BIN_TAG) -f Dockerfile .
	#docker buildx build --platform=linux/amd64 -t sandbox0ai/infra:$(TAG) -f Dockerfile .

docker-push:
	@printf "$(GREEN)Docker pushing unified infra image...$(RESET)\n"
	docker push sandbox0ai/infra:$(TAG)
	docker push sandbox0ai/infra:$(PROCD_BIN_TAG)

build-local-all: manifests proto apispec
	@for service in $(SERVICES); do \
		$(MAKE) build SERVICE=$$service BIN_DIR=$(shell pwd)/bin GOOS=linux; \
	done

docker-build-local: build-local-all
	@printf "$(GREEN)Docker building with local binaries...$(RESET)\n"
	docker build -t sandbox0ai/infra:$(TAG) -f Dockerfile.local .
	docker build --target procd-bin -t sandbox0ai/infra:$(PROCD_BIN_TAG) -f Dockerfile.local .

test:
	@service="$(filter-out build test test-all lint tidy vendor clean helm-update,$(MAKECMDGOALS))"; \
	if [ -z "$$service" ]; then \
		echo "Available services: $(SERVICES)"; \
		echo "Usage: make test <service> or make test-all"; \
		exit 1; \
	elif echo "$(SERVICES)" | grep -qw "$$service"; then \
		printf "$(CYAN)Testing $$service...$(RESET)\n"; \
		if [ "$$service" = "regional-gateway" ]; then \
			GOTOOLCHAIN=go1.25.0+auto $(GO) test -v -race -cover ./regional-gateway/...; \
		elif [ "$$service" = "ssh-gateway" ]; then \
			GOTOOLCHAIN=go1.25.0+auto $(GO) test -v -race -cover ./ssh-gateway/...; \
		elif [ "$$service" = "global-gateway" ]; then \
			GOTOOLCHAIN=go1.25.0+auto $(GO) test -v -race -cover ./global-gateway/...; \
		elif [ "$$service" = "cluster-gateway" ]; then \
			GOTOOLCHAIN=go1.25.0+auto $(GO) test -v -race -cover ./cluster-gateway/...; \
		elif [ "$$service" = "manager" ]; then \
			GOTOOLCHAIN=go1.25.0+auto $(GO) test -v -race -cover ./manager/...; \
		elif [ "$$service" = "procd" ]; then \
			GOTOOLCHAIN=go1.25.0+auto $(GO) test -v -race -cover ./manager/procd/...; \
		elif [ "$$service" = "netd" ]; then \
			GOTOOLCHAIN=go1.25.0+auto $(GO) test -v -race -cover ./netd/...; \
		elif [ "$$service" = "scheduler" ]; then \
			GOTOOLCHAIN=go1.25.0+auto $(GO) test -v -race -cover ./scheduler/...; \
		elif [ "$$service" = "storage-proxy" ]; then \
			GOTOOLCHAIN=go1.25.0+auto $(GO) test -v -race -cover ./storage-proxy/...; \
		elif [ "$$service" = "ctld" ]; then \
			GOTOOLCHAIN=go1.25.0+auto $(GO) test -v -race -cover ./ctld/...; \
		elif [ "$$service" = "infra-operator" ]; then \
			GOTOOLCHAIN=go1.25.0+auto $(GO) test -v -race -cover ./infra-operator/...; \
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
		GOTOOLCHAIN=go1.25.0+auto $(GO) test -v -race -cover ./pkg/...; \
	done

# Integration tests
test-integration:
	@printf "$(CYAN)Running integration tests...$(RESET)\n"
	GOTOOLCHAIN=go1.25.0+auto $(GO) test -v -race -cover ./tests/integration/... -timeout=10m

test-integration-verbose:
	@printf "$(CYAN)Running integration tests (verbose)...$(RESET)\n"
	GOTOOLCHAIN=go1.25.0+auto $(GO) test -v -race -cover ./tests/integration/... -timeout=10m -v

# E2E tests
test-e2e:
	@printf "$(CYAN)Running E2E tests...$(RESET)\n"
	$(GO) test -v -count=1 ./tests/e2e/... -timeout=30m

# Clean kind image
# docker exec -it $(docker ps | grep sandbox0-e2e | cut -f1 -d" ") bash -c 'ctr -n=k8s.io images rm docker.io/sandbox0ai/infra:latest'

test-e2e-local:
	@printf "$(CYAN)Running E2E tests locally...$(RESET)\n"
	unset http_proxy && unset https_proxy && unset all_proxy && E2E_USE_EXISTING_CLUSTER=true $(GO) test -v -count=1 ./tests/e2e/... -timeout=30m

test-e2e-setup-gvisor-rootfs:
	@printf "$(CYAN)Installing gVisor rootfs runtime files for Kind...$(RESET)\n"
	unset http_proxy && unset https_proxy && unset all_proxy && bash tests/e2e/setup-gvisor-rootfs.sh

test-e2e-kind: test-e2e-setup-gvisor-rootfs
	@printf "$(CYAN)Creating Kind cluster...$(RESET)\n"
	unset http_proxy && unset https_proxy && unset all_proxy && kind create cluster --config tests/e2e/kind-config.yaml --name "$(E2E_CLUSTER_NAME)"
	kubectl --context "kind-$(E2E_CLUSTER_NAME)" apply -f tests/e2e/runtimeclasses/gvisor-rootfs.yaml

test-e2e-load-images:
	@printf "$(CYAN)Loading E2E images into Kind cluster...$(RESET)\n"
	@if ! docker image inspect sandbox0ai/infra:$(TAG) >/dev/null 2>&1; then \
		echo "sandbox0ai/infra:$(TAG) is missing; run make docker-build-local first"; \
		exit 1; \
	fi
	@if ! docker image inspect sandbox0ai/infra:$(PROCD_BIN_TAG) >/dev/null 2>&1; then \
		echo "sandbox0ai/infra:$(PROCD_BIN_TAG) is missing; run make docker-build-local first"; \
		exit 1; \
	fi
	@if ! docker image inspect "$(E2E_SSH_FIXTURE_IMAGE)" >/dev/null 2>&1; then \
		printf "$(YELLOW)Pulling SSH fixture source $(E2E_SSH_FIXTURE_SOURCE_IMAGE)...$(RESET)\n"; \
		docker pull --platform "$(E2E_IMAGE_PLATFORM)" "$(E2E_SSH_FIXTURE_SOURCE_IMAGE)" || exit 1; \
		docker tag "$(E2E_SSH_FIXTURE_SOURCE_IMAGE)" "$(E2E_SSH_FIXTURE_IMAGE)" || exit 1; \
	fi
	@load_image() { \
		image="$$1"; \
		for node in $$(kind get nodes --name sandbox0-e2e); do \
			printf "$(YELLOW)Loading $$image into $$node...$(RESET)\n"; \
			docker save "$$image" | docker exec --privileged -i "$$node" ctr --namespace=k8s.io images import --digests --snapshotter=overlayfs - || return 1; \
		done; \
	}; \
	load_image sandbox0ai/infra:$(TAG); \
	load_image sandbox0ai/infra:$(PROCD_BIN_TAG); \
	for image in $(E2E_DEPENDENCY_IMAGES); do \
		if ! docker image inspect "$$image" >/dev/null 2>&1; then \
			printf "$(YELLOW)Pulling $$image for $(E2E_IMAGE_PLATFORM)...$(RESET)\n"; \
			docker pull --platform "$(E2E_IMAGE_PLATFORM)" "$$image" || exit 1; \
		fi; \
		load_image "$$image" || exit 1; \
	done

test-e2e-prepare-kind: docker-build-local test-e2e-kind test-e2e-load-images

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
	unset http_proxy && unset https_proxy && unset all_proxy && $(GO) test -v ./tests/e2e/... -focus="$(SPEC)" -timeout=30m

test-e2e-mountpoint-s3-compat:
	@printf "$(CYAN)Running mountpoint-s3 compatibility E2E suite...$(RESET)\n"
	unset http_proxy && unset https_proxy && unset all_proxy && \
		E2E_SINGLE_CLUSTER_SCENARIOS=volumes \
		E2E_MOUNTPOINT_S3_COMPAT=true \
		E2E_USE_EXISTING_CLUSTER=true \
		$(GO) test -v -count=1 ./tests/e2e/scenarios/single-cluster \
		-run TestSingleCluster \
		-ginkgo.focus="API volumes mode.*mountpoint-s3 general bucket compatibility semantics" \
		-timeout=30m

test-e2e-s0fs-posix-prepare:
	@printf "$(CYAN)Preparing fullmode infra for S0FS POSIX E2E suite...$(RESET)\n"
	unset http_proxy && unset https_proxy && unset all_proxy && \
		E2E_USE_EXISTING_CLUSTER=true \
		E2E_CLUSTER_NAME="$(E2E_CLUSTER_NAME)" \
		E2E_OPERATOR_IMAGE_TAG="$(TAG)" \
		$(GO) run ./tests/e2e/cmd/prepare-s0fs-posix

test-e2e-s0fs-posix:
	@if [ "$(S0FS_POSIX_PREPARE_INFRA)" = "true" ]; then \
		$(MAKE) test-e2e-s0fs-posix-prepare; \
	fi
	@printf "$(CYAN)Running S0FS POSIX E2E suite...$(RESET)\n"
	unset http_proxy && unset https_proxy && unset all_proxy && python3 scripts/s0fs_posix_suite.py \
		--kind-cluster "$(E2E_CLUSTER_NAME)" \
		--kube-context "kind-$(E2E_CLUSTER_NAME)" \
		$(if $(filter true,$(S0FS_POSIX_CI_INSTALL_DEPS)),--install-deps,) \
		--suite smoke \
		--suite git-integrity \
		--suite bulk-smallfile-integrity \
		--suite archive-copy-rsync \
		--suite fsx \
		--suite fsstress \
		--git-file-count "$(S0FS_POSIX_CI_GIT_FILE_COUNT)" \
		--git-file-size "$(S0FS_POSIX_CI_GIT_FILE_SIZE)" \
		--bulk-file-count "$(S0FS_POSIX_CI_BULK_FILE_COUNT)" \
		--bulk-total-bytes "$(S0FS_POSIX_CI_BULK_TOTAL_BYTES)" \
		--bulk-concurrency "$(S0FS_POSIX_CI_BULK_CONCURRENCY)" \
		--bulk-fsync-every "$(S0FS_POSIX_CI_BULK_FSYNC_EVERY)" \
		--bulk-post-write-stat \
		--archive-file-count "$(S0FS_POSIX_CI_ARCHIVE_FILE_COUNT)" \
		--archive-total-bytes "$(S0FS_POSIX_CI_ARCHIVE_TOTAL_BYTES)" \
		--archive-concurrency "$(S0FS_POSIX_CI_ARCHIVE_CONCURRENCY)" \
		--fsx-operations "$(S0FS_POSIX_CI_FSX_OPERATIONS)" \
		--fsstress-operations "$(S0FS_POSIX_CI_FSSTRESS_OPERATIONS)" \
		--fsstress-processes "$(S0FS_POSIX_CI_FSSTRESS_PROCESSES)"

test-e2e-netd-cni:
	@printf "$(CYAN)Running netd CNI E2E tests...$(RESET)\n"
	unset http_proxy && unset https_proxy && unset all_proxy && E2E_SINGLE_CLUSTER_SCENARIOS=network-policy $(GO) test -v -count=1 ./tests/e2e/scenarios/single-cluster -run TestSingleCluster -ginkgo.focus="API network policy mode.*(enforces transparent TCP egress through netd|resolves cluster DNS over UDP with netd active|blocks private sandbox traffic while preserving public exposure and cluster service access)" -timeout=30m

# Prevent make from treating service names as targets
regional-gateway ssh-gateway global-gateway cluster-gateway manager scheduler storage-proxy ctld procd netd infra-operator:
	@:

lint:
	golangci-lint run ./...

tidy:
	$(GO) mod tidy

vendor:
	$(GO) mod vendor

clean:
	@for service in $(SERVICES); do \
		printf "$(YELLOW)Cleaning $$service...$(RESET)\n"; \
		if [ "$$service" = "procd" ]; then \
			rm -rf manager/bin/procd manager/bin/python-runner; \
		else \
			rm -rf $$service/bin; \
		fi; \
	done
	rm -rf storage-proxy/proto/fs/*.pb.go
	rm -rf vendor
	rm -rf bin

app-configs:
	@printf "$(CYAN)Generating default Helm configs...$(RESET)\n"
	@CONFIG_PATH=/dev/null $(GO) run ./tools/configdump

proto: protoc
	@printf "$(CYAN)Generating storage-proxy protobufs...$(RESET)\n"
	@rm -f storage-proxy/proto/*.pb.go storage-proxy/proto/fs/*.pb.go
	@mkdir -p storage-proxy/proto/fs
	@PATH="$(LOCALBIN):$(PATH)" $(PROTOC) --go_out=. --go_opt=paths=source_relative \
		storage-proxy/proto/filesystem.proto
	@mv storage-proxy/proto/*.pb.go storage-proxy/proto/fs/

.PHONY: apispec oapi-codegen
apispec: oapi-codegen
	@printf "$(CYAN)Generating API spec code...$(RESET)\n"
	@PATH="$(LOCALBIN):$(PATH)" $(GO) generate ./pkg/apispec/...

oapi-codegen: $(OAPI_CODEGEN)
$(OAPI_CODEGEN): $(LOCALBIN)
	@test -s $(LOCALBIN)/oapi-codegen && $(LOCALBIN)/oapi-codegen --version | grep -q $(OAPI_CODEGEN_VERSION) || \
	GOBIN=$(LOCALBIN) $(GO) install github.com/oapi-codegen/oapi-codegen/v2/cmd/oapi-codegen@$(OAPI_CODEGEN_VERSION)

.PHONY: controller-gen
controller-gen: $(CONTROLLER_GEN)
$(CONTROLLER_GEN): $(LOCALBIN)
	@test -s $(LOCALBIN)/controller-gen && $(LOCALBIN)/controller-gen --version | grep -q $(CONTROLLER_TOOLS_VERSION) || \
	GOBIN=$(LOCALBIN) $(GO) install sigs.k8s.io/controller-tools/cmd/controller-gen@$(CONTROLLER_TOOLS_VERSION)

.PHONY: protoc install-protoc
protoc:
	@command -v $(PROTOC) >/dev/null 2>&1 || $(MAKE) install-protoc
	@if ! PATH="$(LOCALBIN):$(PATH)" command -v protoc-gen-go >/dev/null 2>&1; then \
		GOBIN=$(LOCALBIN) $(GO) install google.golang.org/protobuf/cmd/protoc-gen-go@latest; \
	fi

install-protoc: $(LOCALBIN)
	@printf "$(CYAN)Installing protoc...$(RESET)\n"
	@set -e; \
	if command -v apt-get >/dev/null 2>&1; then \
		apt-get update -y >/dev/null; \
		apt-get install -y protobuf-compiler; \
	elif command -v yum >/dev/null 2>&1; then \
		yum install -y protobuf-compiler; \
	elif command -v dnf >/dev/null 2>&1; then \
		dnf install -y protobuf-compiler; \
	elif command -v apk >/dev/null 2>&1; then \
		apk add --no-cache protobuf; \
	elif command -v brew >/dev/null 2>&1; then \
		brew install protobuf; \
	else \
		echo "Error: protoc not found and no supported package manager detected."; \
		exit 1; \
	fi

manifests: controller-gen
	@printf "$(CYAN)Generating manager deepcopy code...$(RESET)\n"
	@GOWORK=off $(CONTROLLER_GEN) object paths="./manager/pkg/apis/..."
	@printf "$(CYAN)Generating manager CRDs...$(RESET)\n"
	@GOWORK=off $(CONTROLLER_GEN) crd paths="./manager/pkg/apis/..." output:crd:artifacts:config=infra-operator/chart/crds/
	@printf "$(CYAN)Generating infra-operator manifests...$(RESET)\n"
	@GOWORK=off $(CONTROLLER_GEN) rbac:roleName=manager-role crd webhook object paths="./infra-operator/..." output:crd:artifacts:config=infra-operator/chart/crds output:rbac:artifacts:config=infra-operator/chart/crds output:webhook:artifacts:config=infra-operator/chart/crds
	@test ! -f infra-operator/chart/crds/role.yaml || mv infra-operator/chart/crds/role.yaml infra-operator/chart/files/clusterrole.yaml

.PHONY: operator-install
operator-install: manifests
	kubectl apply -f infra-operator/chart/crds/

.PHONY: operator-run
operator-run: operator-install
	S0_DEV=true $(GO) run ./infra-operator/cmd/infra-operator
