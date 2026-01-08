.PHONY: all build test lint tidy vendor clean

SERVICES := internal-gateway manager storage-proxy k8s-plugin

all: build

build:
	@for service in $(SERVICES); do \
		echo "Building $$service..."; \
		if [ -d "$$service/cmd" ]; then \
			go build -v -o $$service/bin/$$service ./$$service/cmd/...; \
		elif [ -f "$$service/main.go" ]; then \
			go build -v -o $$service/bin/$$service ./$$service/; \
		fi; \
	done

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

