FROM golang:1.25-alpine AS builder

WORKDIR /workspace

RUN apk add --no-cache git make protobuf-dev protoc gcc musl-dev sqlite-dev zstd-dev lz4-dev

ENV GOPROXY=https://goproxy.cn,direct
ENV GOSUMDB=sum.golang.google.cn

COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    go mod download

COPY . .

# Generate protobuf code for storage-proxy
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    go install google.golang.org/protobuf/cmd/protoc-gen-go@latest && \
    go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest && \
    mkdir -p storage-proxy/proto/fs && \
    protoc --go_out=. --go_opt=paths=source_relative \
      --go-grpc_out=. --go-grpc_opt=paths=source_relative \
      storage-proxy/proto/filesystem.proto && \
    mv storage-proxy/proto/*.pb.go storage-proxy/proto/fs/

RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    BUILD_GOOS="${TARGETOS:-$(go env GOOS)}" && \
    BUILD_GOARCH="${TARGETARCH:-$(go env GOARCH)}" && \
    CGO_ENABLED=0 GOOS="${BUILD_GOOS}" GOARCH="${BUILD_GOARCH}" go build -o /out/edge-gateway ./edge-gateway/cmd/edge-gateway && \
    CGO_ENABLED=0 GOOS="${BUILD_GOOS}" GOARCH="${BUILD_GOARCH}" go build -o /out/global-directory ./global-directory/cmd/global-directory && \
    CGO_ENABLED=0 GOOS="${BUILD_GOOS}" GOARCH="${BUILD_GOARCH}" go build -o /out/internal-gateway ./internal-gateway/cmd/internal-gateway && \
    CGO_ENABLED=0 GOOS="${BUILD_GOOS}" GOARCH="${BUILD_GOARCH}" go build -o /out/manager ./manager/cmd/manager && \
    CGO_ENABLED=0 GOOS="${BUILD_GOOS}" GOARCH="${BUILD_GOARCH}" go build -o /out/procd ./manager/cmd/procd && \
    CGO_ENABLED=0 GOOS="${BUILD_GOOS}" GOARCH="${BUILD_GOARCH}" go build -o /out/scheduler ./scheduler/cmd/scheduler && \
    CGO_ENABLED=1 GOOS="${BUILD_GOOS}" GOARCH="${BUILD_GOARCH}" go build -o /out/storage-proxy ./storage-proxy/cmd/storage-proxy && \
    CGO_ENABLED=1 GOOS="${BUILD_GOOS}" GOARCH="${BUILD_GOARCH}" go build -o /out/infra-operator ./infra-operator/cmd/infra-operator && \
    CGO_ENABLED=0 GOOS="${BUILD_GOOS}" GOARCH="${BUILD_GOARCH}" go build -o /out/k8s-plugin ./k8s-plugin && \
    CGO_ENABLED=0 GOOS="${BUILD_GOOS}" GOARCH="${BUILD_GOARCH}" go build -o /out/netd ./netd/cmd/netd

FROM alpine:3.19

RUN apk add --no-cache ca-certificates tzdata zstd-libs lz4-libs iptables ipset nftables iproute2

COPY --from=builder /out/ /usr/local/bin/
COPY scripts/entrypoint.sh /usr/local/bin/entrypoint
RUN chmod +x /usr/local/bin/entrypoint

ENTRYPOINT ["/usr/local/bin/entrypoint"]
