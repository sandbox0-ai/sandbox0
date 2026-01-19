# Build stage
FROM golang:1.21-alpine AS builder

WORKDIR /workspace

# Copy go mod files
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -a -o manager cmd/manager/main.go \
  && CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -a -o procd cmd/procd/main.go

# Runtime stage
FROM alpine:3.19

RUN apk --no-cache add ca-certificates

WORKDIR /app

COPY --from=builder /workspace/manager .
COPY --from=builder /workspace/procd .

USER 65532:65532

ENTRYPOINT ["/app/manager"]

