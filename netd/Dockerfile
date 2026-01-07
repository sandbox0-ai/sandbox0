FROM golang:1.24 AS builder

WORKDIR /app

# Copy go mod files
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build netd
RUN CGO_ENABLED=0 GOOS=linux go build -o /netd ./netd/cmd/netd

# Runtime image
FROM alpine:3.19

# Install required tools for iptables and tc
RUN apk add --no-cache iptables ip6tables iproute2 ca-certificates

# Copy binary
COPY --from=builder /netd /usr/local/bin/netd

# Set entrypoint
ENTRYPOINT ["/usr/local/bin/netd"]

