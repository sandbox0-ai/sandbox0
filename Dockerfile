FROM node:20-bookworm-slim AS procd-node-runtime

RUN set -eux; \
    mkdir -p /procd-runtime/lib; \
    cp /usr/local/bin/node /procd-runtime/node; \
    ldd /usr/local/bin/node | awk '{ if ($3 ~ /^\//) print $3; else if ($1 ~ /^\//) print $1 }' | sort -u | while read lib; do cp "$lib" /procd-runtime/lib/; done; \
    for lib in /lib/*/libnss_dns.so.2 /lib/*/libnss_files.so.2 /lib/*/libresolv.so.2; do [ -e "$lib" ] && cp "$lib" /procd-runtime/lib/ || true; done; \
    loader="$(ldd /usr/local/bin/node | awk '/ld-linux|ld-musl/ { if ($1 ~ /^\//) print $1; else if ($3 ~ /^\//) print $3; exit }')"; \
    test -n "$loader"; \
    cp "$loader" /procd-runtime/ld-linux; \
    chmod 0755 /procd-runtime/node /procd-runtime/ld-linux

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
    CGO_ENABLED=0 GOOS="${BUILD_GOOS}" GOARCH="${BUILD_GOARCH}" go build -o /out/regional-gateway ./regional-gateway/cmd/regional-gateway && \
    CGO_ENABLED=0 GOOS="${BUILD_GOOS}" GOARCH="${BUILD_GOARCH}" go build -o /out/ssh-gateway ./ssh-gateway/cmd/ssh-gateway && \
    CGO_ENABLED=0 GOOS="${BUILD_GOOS}" GOARCH="${BUILD_GOARCH}" go build -o /out/global-gateway ./global-gateway/cmd/global-gateway && \
    CGO_ENABLED=0 GOOS="${BUILD_GOOS}" GOARCH="${BUILD_GOARCH}" go build -o /out/cluster-gateway ./cluster-gateway/cmd/cluster-gateway && \
    CGO_ENABLED=0 GOOS="${BUILD_GOOS}" GOARCH="${BUILD_GOARCH}" go build -o /out/manager ./manager/cmd/manager && \
    CGO_ENABLED=0 GOOS="${BUILD_GOOS}" GOARCH="${BUILD_GOARCH}" go build -o /out/python-runner ./manager/cmd/python-runner && \
    CGO_ENABLED=0 GOOS="${BUILD_GOOS}" GOARCH="${BUILD_GOARCH}" go build -buildvcs=false -o /out/pty-helper ./manager/procd/pty-helper && \
    CGO_ENABLED=0 GOOS="${BUILD_GOOS}" GOARCH="${BUILD_GOARCH}" go build -o /out/scheduler ./scheduler/cmd/scheduler && \
    CGO_ENABLED=1 GOOS="${BUILD_GOOS}" GOARCH="${BUILD_GOARCH}" go build -o /out/storage-proxy ./storage-proxy/cmd/storage-proxy && \
    CGO_ENABLED=1 GOOS="${BUILD_GOOS}" GOARCH="${BUILD_GOARCH}" go build -o /out/infra-operator ./infra-operator/cmd/infra-operator && \
    CGO_ENABLED=0 GOOS="${BUILD_GOOS}" GOARCH="${BUILD_GOARCH}" go build -o /out/ctld ./ctld/cmd/ctld && \
    CGO_ENABLED=0 GOOS="${BUILD_GOOS}" GOARCH="${BUILD_GOARCH}" go build -o /out/netd ./netd/cmd/netd

FROM alpine:3.19

RUN apk add --no-cache ca-certificates tzdata zstd-libs lz4-libs iptables ipset nftables iproute2

COPY --from=builder /out/ /usr/local/bin/
COPY --from=procd-node-runtime /procd-runtime/ /usr/local/sandbox0/procd-runtime/
COPY manager/procd/package.json /usr/local/sandbox0/procd/package.json
COPY manager/procd/src/ /usr/local/sandbox0/procd/src/
COPY scripts/entrypoint.sh /usr/local/bin/entrypoint
RUN chmod +x /usr/local/bin/entrypoint

ENTRYPOINT ["/usr/local/bin/entrypoint"]
