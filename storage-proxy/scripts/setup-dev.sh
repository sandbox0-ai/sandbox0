#!/bin/bash
set -e

# Development environment setup script

echo "Setting up development environment..."

# Check Go installation
if ! command -v go &> /dev/null; then
    echo "Error: Go is not installed"
    exit 1
fi

echo "✓ Go version: $(go version)"

# Check protoc installation
if ! command -v protoc &> /dev/null; then
    echo "Error: protoc is not installed"
    echo "Please install Protocol Buffers compiler:"
    echo "  macOS: brew install protobuf"
    echo "  Ubuntu: apt-get install protobuf-compiler"
    exit 1
fi

echo "✓ protoc version: $(protoc --version)"

# Install Go protobuf plugins
echo "Installing protobuf Go plugins..."
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest

echo "✓ Protobuf plugins installed"

# Download Go dependencies
echo "Downloading Go dependencies..."
go mod download
go mod tidy

echo "✓ Dependencies downloaded"

# Generate protobuf code
echo "Generating protobuf code..."
bash scripts/generate-proto.sh

echo "✓ Development environment setup complete"
echo ""
echo "Next steps:"
echo "  1. Set environment variables (copy .env.example to .env)"
echo "  2. Run tests: make test"
echo "  3. Build: make build"
echo "  4. Run locally: make run"
