# Build stage
FROM golang:1.22-alpine AS builder

# Install build dependencies
RUN apk add --no-cache git make

# Set working directory
WORKDIR /app

# Copy go mod files
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy source code
COPY . .

# Build the binary
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o procd ./cmd/procd

# Runtime stage
FROM alpine:3.19

# Install runtime dependencies
# Note: nftables and iptables removed - network isolation is handled by netd service
RUN apk add --no-cache \
    ca-certificates \
    tzdata \
    bash \
    python3 \
    py3-pip \
    nodejs \
    npm \
    fuse

# Install IPython for Python REPL
RUN pip3 install --break-system-packages ipython

# Create non-root user
RUN addgroup -g 1000 sandbox && \
    adduser -u 1000 -G sandbox -s /bin/bash -D sandbox

# Create workspace directory
RUN mkdir -p /workspace && chown sandbox:sandbox /workspace

# Copy binary from builder
COPY --from=builder /app/procd /usr/local/bin/procd

# Set environment variables
ENV PROCD_HTTP_PORT=8080 \
    PROCD_LOG_LEVEL=info \
    PROCD_ROOT_PATH=/workspace \
    PROCD_MAX_CONTEXTS=100

# Expose HTTP port
EXPOSE 8080

# Volume for workspace
VOLUME /workspace

# Run as root (needed for FUSE mounts and process management)
USER root

# Health check
HEALTHCHECK --interval=10s --timeout=5s --start-period=5s --retries=3 \
    CMD wget --no-verbose --tries=1 --spider http://localhost:8080/healthz || exit 1

# Entry point
ENTRYPOINT ["/usr/local/bin/procd"]

