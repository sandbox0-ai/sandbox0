.PHONY: build run test clean docker-build docker-push

# Variables
BINARY_NAME=scheduler
IMAGE_NAME=sandbox0ai/scheduler
VERSION?=v0.1.0

# Build the binary
build:
	go build -o bin/$(BINARY_NAME) ./cmd/scheduler

# Run locally
run:
	go run ./cmd/scheduler

# Run tests
test:
	go test -v ./...

# Clean build artifacts
clean:
	rm -rf bin/

# Build Docker image
docker-build:
	docker build -t $(IMAGE_NAME):$(VERSION) -f Dockerfile ../..

# Push Docker image
docker-push:
	docker push $(IMAGE_NAME):$(VERSION)

# Build and push
docker: docker-build docker-push

# Run migrations (requires DATABASE_URL env var)
migrate-up:
	@echo "Run migrations using your preferred tool"
	@echo "Example: psql \$$DATABASE_URL -f migrations/00001_init_schema.sql"

# Format code
fmt:
	go fmt ./...

# Lint code
lint:
	golangci-lint run

# Generate mocks
generate:
	go generate ./...
