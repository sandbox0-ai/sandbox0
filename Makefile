.PHONY: all build build-all obuild obuild-all test lint tidy vendor clean

SERVICES := internal-gateway manager storage-proxy netd k8s-plugin

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
	@service="$(filter-out build build-all obuild obuild-all test lint tidy vendor clean,$(MAKECMDGOALS))"; \
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

# Direct go build specific service: make obuild <service> (no Makefile delegation)
obuild:
	@service="$(filter-out build build-all obuild test lint tidy vendor clean,$(MAKECMDGOALS))"; \
	if [ -z "$$service" ]; then \
		echo "Error: Please specify a service or use 'make obuild-all'"; \
		echo "Available services: $(SERVICES)"; \
		echo "Usage: make obuild <service>"; \
		exit 1; \
	elif echo "$(SERVICES)" | grep -qw "$$service"; then \
		echo "Direct go build $$service..."; \
		if [ -d "$$service/cmd" ]; then \
			go build -v -o $$service/bin ./$$service/cmd/...; \
		elif [ -f "$$service/main.go" ]; then \
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
internal-gateway manager storage-proxy k8s-plugin:
	@:

test:
	go test -v ./...

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

