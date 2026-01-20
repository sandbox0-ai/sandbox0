.PHONY: all build build-all test test-all lint tidy vendor clean helm-update helm-configs release docker-build docker-push proto

SERVICES := edge-gateway internal-gateway manager scheduler storage-proxy netd k8s-plugin procd

# Default version
VERSION ?= latest
TAG ?= $(VERSION)

# Colors for output
YELLOW := \033[1;33m
GREEN  := \033[1;32m
CYAN   := \033[1;36m
RESET  := \033[0m

all: build-all

# Build all services
build-all:
	@$(MAKE) proto
	@printf "$(GREEN)Building edge-gateway...$(RESET)\n"
	@mkdir -p edge-gateway/bin
	@go build -v -o edge-gateway/bin/edge-gateway ./edge-gateway/cmd/edge-gateway
	@printf "$(GREEN)Building internal-gateway...$(RESET)\n"
	@mkdir -p internal-gateway/bin
	@go build -v -o internal-gateway/bin/internal-gateway ./internal-gateway/cmd/internal-gateway
	@printf "$(GREEN)Building manager...$(RESET)\n"
	@mkdir -p manager/bin
	@go build -v -o manager/bin/manager ./manager/cmd/manager
	@printf "$(GREEN)Building procd...$(RESET)\n"
	@mkdir -p manager/bin
	@go build -v -o manager/bin/procd ./manager/cmd/procd
	@printf "$(GREEN)Building scheduler...$(RESET)\n"
	@mkdir -p scheduler/bin
	@go build -v -o scheduler/bin/scheduler ./scheduler/cmd/scheduler
	@printf "$(GREEN)Building storage-proxy...$(RESET)\n"
	@mkdir -p storage-proxy/bin
	@go build -v -o storage-proxy/bin/storage-proxy ./storage-proxy/cmd/storage-proxy
	@printf "$(GREEN)Building netd...$(RESET)\n"
	@mkdir -p netd/bin
	@go build -v -o netd/bin/netd ./netd/cmd/netd
	@printf "$(GREEN)Building k8s-plugin...$(RESET)\n"
	@mkdir -p k8s-plugin/bin
	@go build -v -o k8s-plugin/bin/k8s-plugin ./k8s-plugin

# Build specific service: make build <service>
build:
	@service="$(filter-out build build-all test test-all lint tidy vendor clean helm-update docker-build docker-push,$(MAKECMDGOALS))"; \
	if [ -z "$$service" ]; then \
		echo "Error: Please specify a service or use 'make build-all'"; \
		echo "Available services: $(SERVICES)"; \
		echo "Usage: make build <service> or make build-<service>"; \
		exit 1; \
	elif echo "$(SERVICES)" | grep -qw "$$service"; then \
		printf "$(GREEN)Building $$service...$(RESET)\n"; \
		if [ "$$service" = "edge-gateway" ]; then \
			mkdir -p edge-gateway/bin; \
			go build -v -o edge-gateway/bin/edge-gateway ./edge-gateway/cmd/edge-gateway; \
		elif [ "$$service" = "internal-gateway" ]; then \
			mkdir -p internal-gateway/bin; \
			go build -v -o internal-gateway/bin/internal-gateway ./internal-gateway/cmd/internal-gateway; \
		elif [ "$$service" = "manager" ]; then \
			mkdir -p manager/bin; \
			go build -v -o manager/bin/manager ./manager/cmd/manager; \
		elif [ "$$service" = "procd" ]; then \
			mkdir -p manager/bin; \
			go build -v -o manager/bin/procd ./manager/cmd/procd; \
		elif [ "$$service" = "scheduler" ]; then \
			mkdir -p scheduler/bin; \
			go build -v -o scheduler/bin/scheduler ./scheduler/cmd/scheduler; \
		elif [ "$$service" = "storage-proxy" ]; then \
			$(MAKE) proto; \
			mkdir -p storage-proxy/bin; \
			go build -v -o storage-proxy/bin/storage-proxy ./storage-proxy/cmd/storage-proxy; \
		elif [ "$$service" = "netd" ]; then \
			mkdir -p netd/bin; \
			go build -v -o netd/bin/netd ./netd/cmd/netd; \
		elif [ "$$service" = "k8s-plugin" ]; then \
			mkdir -p k8s-plugin/bin; \
			go build -v -o k8s-plugin/bin/k8s-plugin ./k8s-plugin; \
		fi; \
	else \
		echo "Error: Unknown service '$$service'"; \
		echo "Available services: $(SERVICES)"; \
		exit 1; \
	fi

docker-build:
	@printf "$(GREEN)Docker building unified infra image...$(RESET)\n"
	docker build -t sandbox0ai/infra:$(TAG) -f Dockerfile .
	#docker buildx build --platform=linux/amd64 -t sandbox0ai/infra:v0.0.0 -f Dockerfile .

docker-push:
	@printf "$(GREEN)Docker pushing unified infra image...$(RESET)\n"
	docker push sandbox0ai/infra:$(TAG)

test:
	@service="$(filter-out build build-all test test-all lint tidy vendor clean helm-update,$(MAKECMDGOALS))"; \
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
		elif [ "$$service" = "netd" ]; then \
			GOTOOLCHAIN=go1.25.0+auto go test -v -race -cover ./netd/...; \
		elif [ "$$service" = "k8s-plugin" ]; then \
			GOTOOLCHAIN=go1.25.0+auto go test -v -race -cover ./k8s-plugin/...; \
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

# Prevent make from treating service names as targets
edge-gateway internal-gateway manager scheduler storage-proxy netd k8s-plugin procd:
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
			rm -rf helm/configs/$$service.yaml; \
			rm -rf $$service/bin; \
			rm -rf helm/charts/$$service; \
		fi; \
	done
	rm -rf storage-proxy/proto/fs/*.pb.go
	rm -rf vendor

helm-update:
	@mkdir -p helm/charts
	@for service in $(SERVICES); do \
		if [ -d "$$service/chart" ]; then \
			echo "Copying chart for $$service..."; \
			rm -rf helm/charts/$$service; \
			cp -r $$service/chart helm/charts/$$service; \
		fi; \
	done

helm-configs:
	@printf "$(CYAN)Generating default Helm configs...$(RESET)\n"
	@CONFIG_PATH=/dev/null go run ./tools/configdump

helm-clean:
	@mkdir -p helm/charts
	@for service in $(SERVICES); do \
		if [ -d "$$service/chart" ]; then \
			echo "Deleting chart for $$service..."; \
			rm -rf helm/charts/$$service; \
		fi; \
	done

# Release helm chart and git tag in one shot:
#   make release VERSION=v0.1.0
release:
	@if [ -z "$(VERSION)" ]; then \
		echo "Error: VERSION is required. Usage: make release VERSION=v0.1.0"; \
		exit 2; \
	fi
	@bash release.sh "$(VERSION)"

proto:
	@printf "$(CYAN)Generating storage-proxy protobufs...$(RESET)\n"
	@mkdir -p storage-proxy/proto/fs
	@protoc --go_out=. --go_opt=paths=source_relative \
		--go-grpc_out=. --go-grpc_opt=paths=source_relative \
		storage-proxy/proto/filesystem.proto
	@mv storage-proxy/proto/*.pb.go storage-proxy/proto/fs/
