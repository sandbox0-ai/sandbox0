.PHONY: all build test test-all test-integration test-integration-verbose lint lint-nomad-driver tidy tidy-nomad-driver test-nomad-driver vendor clean release docker-build docker-build-local build-local-all docker-push apispec oapi-codegen

LOCALBIN ?= $(shell pwd)/bin
$(LOCALBIN):
	mkdir -p $(LOCALBIN)

OAPI_CODEGEN ?= $(LOCALBIN)/oapi-codegen
OAPI_CODEGEN_VERSION ?= v2.4.1

GO ?= env GOWORK=off go
BINARIES := regional-gateway ssh-gateway global-gateway cluster-gateway manager scheduler ctld procd
TEST_SUITES := $(BINARIES)
VERSION ?= latest
TAG ?= $(VERSION)
PROCD_BIN_TAG ?= $(TAG)-procd-bin

YELLOW := \033[1;33m
GREEN  := \033[1;32m
CYAN   := \033[1;36m
RESET  := \033[0m

all: apispec
	@for service in $(BINARIES); do \
		$(MAKE) build SERVICE=$$service GOOS=$(GOOS); \
	done

build: apispec
	@service="$(filter-out build test test-all lint tidy vendor clean docker-build docker-build-local build-local-all docker-push,$(MAKECMDGOALS))"; \
	[ -z "$$service" ] && service="$(SERVICE)"; \
	for s in $$service; do \
		if ! echo "$(BINARIES)" | grep -qw "$$s"; then \
			echo "Error: Unknown service '$$s'"; exit 1; \
		fi; \
		printf "$(GREEN)Building $$s...$(RESET)\n"; \
		if [ "$$s" = "procd" ]; then \
			dir="manager"; bin="procd"; src="./manager/cmd/procd"; \
		elif [ "$$s" = "ctld" ]; then \
			dir="ctld"; bin="ctld"; src="./ctld/cmd/ctld"; \
		else \
			dir="$$s"; bin="$$s"; src="./$$s/cmd/$$s"; \
		fi; \
		if [ -n "$(BIN_DIR)" ]; then \
			mkdir -p "$(BIN_DIR)"; out="$(BIN_DIR)/$$s"; \
		else \
			mkdir -p "$$dir/bin"; out="$$dir/bin/$$bin"; \
		fi; \
		CGO_ENABLED=0 GOOS=$(GOOS) $(GO) build -buildvcs=false -v -o "$$out" "$$src" || exit 1; \
		if [ "$$s" = "procd" ]; then \
			runner_out="$$(dirname "$$out")/python-runner"; \
			CGO_ENABLED=0 GOOS=$(GOOS) $(GO) build -buildvcs=false -v -o "$$runner_out" ./manager/cmd/python-runner || exit 1; \
		fi; \
	done

regional-gateway ssh-gateway global-gateway cluster-gateway manager scheduler ctld procd:
	@:

test:
	@service="$(filter-out build test test-all lint tidy vendor clean,$(MAKECMDGOALS))"; \
	if [ -z "$$service" ]; then \
		echo "Available test suites: $(TEST_SUITES)"; \
		echo "Usage: make test <service> or make test-all"; \
		exit 1; \
	elif ! echo "$(TEST_SUITES)" | grep -qw "$$service"; then \
		echo "Error: Unknown service '$$service'"; exit 1; \
	fi; \
	printf "$(CYAN)Testing $$service...$(RESET)\n"; \
	case "$$service" in \
		procd) package="./manager/procd/..." ;; \
		*) package="./$$service/..." ;; \
	esac; \
	GOTOOLCHAIN=go1.25.0+auto $(GO) test -buildvcs=false -v -race -cover "$$package"

test-all:
	@for service in $(TEST_SUITES); do \
		$(MAKE) test $$service || exit 1; \
	done
	GOTOOLCHAIN=go1.25.0+auto $(GO) test -buildvcs=false -v -race -cover ./pkg/... ./tests/architecture/...

test-integration:
	@printf "$(CYAN)Running integration tests...$(RESET)\n"
	GOTOOLCHAIN=go1.25.0+auto $(GO) test -buildvcs=false -v -race -cover ./tests/integration/... -timeout=10m

test-integration-verbose: test-integration

lint:
	golangci-lint run ./...

lint-nomad-driver:
	cd nomad-driver-sandbox0 && $(GO) vet ./...

tidy:
	$(GO) mod tidy

tidy-nomad-driver:
	cd nomad-driver-sandbox0 && $(GO) mod tidy

test-nomad-driver:
	cd nomad-driver-sandbox0 && $(GO) test -race ./...

vendor:
	$(GO) mod vendor

clean:
	@for service in $(BINARIES); do \
		printf "$(YELLOW)Cleaning $$service...$(RESET)\n"; \
		if [ "$$service" = "procd" ]; then \
			rm -rf manager/bin/procd manager/bin/python-runner; \
		else \
			rm -rf "$$service/bin"; \
		fi; \
	done
	rm -rf vendor bin

docker-build:
	@printf "$(GREEN)Docker building unified infra image...$(RESET)\n"
	docker build -t sandbox0ai/infra:$(TAG) -f Dockerfile .
	docker build --target procd-bin -t sandbox0ai/infra:$(PROCD_BIN_TAG) -f Dockerfile .

docker-push:
	docker push sandbox0ai/infra:$(TAG)
	docker push sandbox0ai/infra:$(PROCD_BIN_TAG)

build-local-all: apispec
	@for service in $(BINARIES); do \
		$(MAKE) build SERVICE=$$service BIN_DIR=$(shell pwd)/bin GOOS=linux || exit 1; \
	done

docker-build-local: build-local-all
	docker build -t sandbox0ai/infra:$(TAG) -f Dockerfile.local .
	docker build --target procd-bin -t sandbox0ai/infra:$(PROCD_BIN_TAG) -f Dockerfile.local .

apispec: oapi-codegen
	@printf "$(CYAN)Generating API spec code...$(RESET)\n"
	@PATH="$(LOCALBIN):$(PATH)" $(GO) generate ./pkg/apispec/...

oapi-codegen: $(OAPI_CODEGEN)
$(OAPI_CODEGEN): $(LOCALBIN)
	@test -s $(LOCALBIN)/oapi-codegen && $(LOCALBIN)/oapi-codegen --version | grep -q $(OAPI_CODEGEN_VERSION) || \
	GOBIN=$(LOCALBIN) $(GO) install github.com/oapi-codegen/oapi-codegen/v2/cmd/oapi-codegen@$(OAPI_CODEGEN_VERSION)
