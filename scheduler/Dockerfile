# Build stage
FROM golang:1.22-alpine AS builder

WORKDIR /app

# Install build dependencies
RUN apk add --no-cache git

# Copy go.mod and go.sum first for better caching
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build the binary
RUN CGO_ENABLED=0 GOOS=linux go build -o /scheduler ./cmd/scheduler

# Runtime stage
FROM alpine:3.19

# Install ca-certificates for HTTPS
RUN apk add --no-cache ca-certificates

# Create non-root user
RUN adduser -D -u 1000 scheduler

# Copy binary from builder
COPY --from=builder /scheduler /usr/local/bin/scheduler

# Set ownership
RUN chown scheduler:scheduler /usr/local/bin/scheduler

# Switch to non-root user
USER scheduler

# Expose port
EXPOSE 8080

# Run the binary
ENTRYPOINT ["/usr/local/bin/scheduler"]
