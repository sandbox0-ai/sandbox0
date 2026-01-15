.PHONY: all build build-all obuild obuild-all test lint tidy vendor clean helm-update release

SERVICES := edge-gateway internal-gateway manager storage-proxy netd k8s-plugin

all: build-all

# Build all services
build-all:
	@for service in $(SERVICES); do \
		echo "Building $$service..."; \
		if [ -f "$$service/Makefile" ]; then \
			$(MAKE) -C $$service build; \
		elif [ -d "$$service/cmd" ]; then \
			go build -v -o $$service/bin/$$service ./$$service/cmd/...; \
		elif [ -f "$$service/main.go" ]; then \
			go build -v -o $$service/bin/$$service ./$$service/; \
		fi; \
	done

# Build specific service: make build <service>
build:
	@service="$(filter-out build build-all obuild obuild-all test lint tidy vendor clean helm-update,$(MAKECMDGOALS))"; \
	if [ -z "$$service" ]; then \
		echo "Error: Please specify a service or use 'make build-all'"; \
		echo "Available services: $(SERVICES)"; \
		echo "Usage: make build <service> or make build-<service>"; \
		exit 1; \
	elif echo "$(SERVICES)" | grep -qw "$$service"; then \
		echo "Building $$service..."; \
		if [ -f "$$service/Makefile" ]; then \
			$(MAKE) -C $$service build; \
		elif [ -d "$$service/cmd" ]; then \
			go build -v -o $$service/bin/$$service ./$$service/cmd/...; \
		elif [ -f "$$service/main.go" ]; then \
			go build -v -o $$service/bin/$$service ./$$service/; \
		fi; \
	else \
		echo "Error: Unknown service '$$service'"; \
		echo "Available services: $(SERVICES)"; \
		exit 1; \
	fi

test:
	@service="$(filter-out build build-all obuild obuild-all test lint tidy vendor clean helm-update,$(MAKECMDGOALS))"; \
	if [ -z "$$service" ]; then \
		echo "Available services: $(SERVICES)"; \
		echo "Usage: make test <service>"; \
		exit 1; \
	elif echo "$(SERVICES)" | grep -qw "$$service"; then \
		echo "Testing $$service..."; \
		if [ -f "$$service/Makefile" ]; then \
			$(MAKE) -C $$service test; \
		elif [ -d "$$service/cmd" ]; then \
			@GOTOOLCHAIN=go1.25.0+auto go test -v -race -cover ./$$service/cmd/...; \
		elif [ -f "$$service/main.go" ]; then \
			@GOTOOLCHAIN=go1.25.0+auto go test -v -race -cover ./$$service/; \
		fi; \
	else \
		echo "Error: Unknown service '$$service'"; \
		echo "Available services: $(SERVICES)"; \
		exit 1; \
	fi

# Direct go build specific service: make obuild <service> (no Makefile delegation)
obuild:
	@service="$(filter-out build build-all obuild test lint tidy vendor clean helm-update,$(MAKECMDGOALS))"; \
	if [ -z "$$service" ]; then \
		echo "Error: Please specify a service or use 'make obuild-all'"; \
		echo "Available services: $(SERVICES)"; \
		echo "Usage: make obuild <service>"; \
		exit 1; \
	elif echo "$(SERVICES)" | grep -qw "$$service"; then \
		echo "Direct go build $$service..."; \
		if [ -d "$$service/cmd" ]; then \
			go vet ./$$service/cmd/...; \
			go build -v -o $$service/bin ./$$service/cmd/...; \
		elif [ -f "$$service/main.go" ]; then \
			go vet ./$$service/; \
			go build -v -o $$service/bin ./$$service/; \
		else \
			echo "Warning: No cmd directory or main.go found for $$service"; \
		fi; \
	else \
		echo "Error: Unknown service '$$service'"; \
		echo "Available services: $(SERVICES)"; \
		exit 1; \
	fi

# Prevent make from treating service names as targets
edge-gateway internal-gateway manager storage-proxy k8s-plugin:
	@:

lint:
	golangci-lint run ./...

tidy:
	go mod tidy

vendor:
	go mod vendor

clean:
	@for service in $(SERVICES); do \
		echo "Cleaning $$service..."; \
		rm -rf $$service/bin; \
	done
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

# Release helm chart and git tag in one shot:
#   make release VERSION=v0.1.0
release:
	@if [ -z "$(VERSION)" ]; then \
		echo "Error: VERSION is required. Usage: make release VERSION=v0.1.0"; \
		exit 2; \
	fi
	@bash release.sh "$(VERSION)"
