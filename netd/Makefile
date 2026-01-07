.PHONY: build clean docker-build docker-push test lint

# Variables
BINARY_NAME := netd
IMAGE_NAME := sandbox0/netd
IMAGE_TAG := latest

# Build the binary
build:
	@echo "Building $(BINARY_NAME)..."
	@mkdir -p bin
	CGO_ENABLED=0 go build -o bin/$(BINARY_NAME) ./cmd/netd

# Clean build artifacts
clean:
	@echo "Cleaning..."
	@rm -rf bin/

# Build Docker image
docker-build:
	@echo "Building Docker image..."
	docker build -t $(IMAGE_NAME):$(IMAGE_TAG) -f Dockerfile ..

# Push Docker image
docker-push: docker-build
	@echo "Pushing Docker image..."
	docker push $(IMAGE_NAME):$(IMAGE_TAG)

# Run tests
test:
	@echo "Running tests..."
	go test -v ./...

# Run linter
lint:
	@echo "Running linter..."
	golangci-lint run ./...

# Run locally (requires root for iptables)
run: build
	@echo "Running $(BINARY_NAME)..."
	sudo NODE_NAME=local ./bin/$(BINARY_NAME)

# Deploy to Kubernetes
deploy:
	@echo "Deploying to Kubernetes..."
	kubectl apply -k deploy/k8s/

# Undeploy from Kubernetes
undeploy:
	@echo "Undeploying from Kubernetes..."
	kubectl delete -k deploy/k8s/

