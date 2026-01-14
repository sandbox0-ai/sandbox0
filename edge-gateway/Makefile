.PHONY: build run test lint clean docker-build docker-push

# Variables
BINARY_NAME=edge-gateway
DOCKER_IMAGE=sandbox0/edge-gateway
VERSION?=latest

# Go parameters
GOCMD=GOTOOLCHAIN=go1.25.0+auto go
GOBUILD=$(GOCMD) build
GOTEST=$(GOCMD) test
GOGET=$(GOCMD) get
GOMOD=$(GOCMD) mod
GOFMT=gofmt

# Build the binary
build:
	$(GOBUILD) -o bin/$(BINARY_NAME) -v ./cmd/edge-gateway

# Run locally
run: build
	./bin/$(BINARY_NAME)

# Run tests
test:
	$(GOTEST) -v -race -cover ./...

# Run linter
lint:
	golangci-lint run ./...

# Format code
fmt:
	$(GOFMT) -s -w .

# Clean build artifacts
clean:
	rm -rf bin/

# Download dependencies
deps:
	cd .. && go mod download
	cd .. && go mod tidy

# Vendor dependencies
vendor:
	cd .. && go mod vendor

# Build Docker image
docker-build:
	docker build -t $(DOCKER_IMAGE):$(VERSION) .

# Push Docker image
docker-push:
	docker push $(DOCKER_IMAGE):$(VERSION)

# Build and push Docker image
docker: docker-build docker-push

# Run locally with environment
run-local:
	DATABASE_URL="postgres://localhost:5432/sandbox0?sslmode=disable" \
	INTERNAL_GATEWAY_URL="http://localhost:8443" \
	EDGE_GATEWAY_HTTP_PORT=8080 \
	./bin/$(BINARY_NAME)

# Help
help:
	@echo "Available targets:"
	@echo "  build        - Build the binary"
	@echo "  run          - Build and run locally"
	@echo "  test         - Run tests"
	@echo "  lint         - Run linter"
	@echo "  fmt          - Format code"
	@echo "  clean        - Clean build artifacts"
	@echo "  deps         - Download dependencies"
	@echo "  vendor       - Vendor dependencies"
	@echo "  docker-build - Build Docker image"
	@echo "  docker-push  - Push Docker image"
	@echo "  docker       - Build and push Docker image"
	@echo "  run-local    - Run locally with environment"
