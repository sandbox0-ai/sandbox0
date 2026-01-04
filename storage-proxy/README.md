# Storage Proxy

Storage Proxy is a secure, high-performance service that manages all persistent storage access for the Sandbox0 platform. It provides a gRPC interface for file system operations backed by JuiceFS, with complete credential isolation and network security.

## Features

- **Credential Isolation**: All S3 and PostgreSQL credentials are isolated in the proxy service
- **Zero JuiceFS Modifications**: Uses official JuiceFS Go SDK without any source code changes
- **Network Security**: Compatible with network isolation through packet marking
- **High Performance**: gRPC over HTTP/2 with streaming support
- **JWT Authentication**: Token-based authentication with volume access control
- **Audit Logging**: Complete audit trail of all file operations
- **Prometheus Metrics**: Comprehensive metrics for monitoring
- **Kubernetes Native**: Designed to run in Kubernetes with IRSA support

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│  Procd Pod                    Storage Proxy Service          │
│  ┌─────────────┐            ┌──────────────────────────┐   │
│  │ RemoteFS    │  gRPC      │ FileSystemServer         │   │
│  │ (FUSE)      │◄───────────┤ (JWT Auth)               │   │
│  │             │            │                          │   │
│  └─────────────┘            │ ┌──────────────────────┐ │   │
│                              │ │ JuiceFS SDK (no FUSE)│ │   │
│                              │ │ - meta.Client        │ │   │
│                              │ │ - chunk.CachedStore  │ │   │
│                              │ │ - vfs.VFS            │ │   │
│                              │ └──────────────────────┘ │   │
│                              └──────────────────────────┘   │
│                                        │                     │
│                                        ▼                     │
│                              ┌──────────────────────────┐   │
│                              │ S3 (chunks)              │   │
│                              │ PostgreSQL (metadata)    │   │
│                              └──────────────────────────┘   │
└─────────────────────────────────────────────────────────────┘
```

## Quick Start

### Prerequisites

- Go 1.23+
- Protocol Buffers compiler (protoc)
- Docker (for containerized deployment)
- Kubernetes cluster (for production deployment)

### Build from Source

```bash
# Clone the repository
cd infra/storage-proxy

# Install protobuf dependencies
make install-deps

# Generate protobuf code
make proto

# Build binary
make build

# Run tests
make test
```

### Running Locally

```bash
# Set required environment variables
export JWT_SECRET="your-secret-key-here"
export DEFAULT_META_URL="postgres://user:pass@localhost:5432/juicefs"
export AWS_ACCESS_KEY_ID="your-aws-access-key"
export AWS_SECRET_ACCESS_KEY="your-aws-secret-key"
export AWS_REGION="us-east-1"

# Run the server
make run
```

### Docker Build

```bash
# Build Docker image
docker build -t sandbox0/storage-proxy:latest .

# Run container
docker run -p 8080:8080 -p 8081:8081 \
  -e JWT_SECRET="your-secret" \
  -e DEFAULT_META_URL="postgres://..." \
  sandbox0/storage-proxy:latest
```

## Kubernetes Deployment

### Prerequisites

1. **Secrets Configuration**

Edit `deploy/k8s/secret.yaml` with your credentials:

```yaml
stringData:
  jwt-secret: "YOUR_JWT_SECRET_HERE"
  postgres-url: "postgres://user:pass@postgres:5432/juicefs"
  # Optional: AWS credentials (if not using IRSA)
  aws-access-key-id: "YOUR_AWS_ACCESS_KEY"
  aws-secret-access-key: "YOUR_AWS_SECRET_KEY"
```

2. **IRSA Configuration (Recommended for EKS)**

If using IAM Roles for Service Accounts (IRSA):

```yaml
# In serviceaccount.yaml, uncomment and set:
annotations:
  eks.amazonaws.com/role-arn: arn:aws:iam::ACCOUNT_ID:role/storage-proxy-role
```

### Deploy

```bash
# Apply all manifests
kubectl apply -k deploy/k8s/

# Or apply individually
kubectl apply -f deploy/k8s/namespace.yaml
kubectl apply -f deploy/k8s/secret.yaml
kubectl apply -f deploy/k8s/serviceaccount.yaml
kubectl apply -f deploy/k8s/deployment.yaml
kubectl apply -f deploy/k8s/service.yaml
kubectl apply -f deploy/k8s/poddisruptionbudget.yaml
kubectl apply -f deploy/k8s/networkpolicy.yaml
```

### Verify Deployment

```bash
# Check pods
kubectl get pods -n sandbox0-system -l app=storage-proxy

# Check service
kubectl get svc -n sandbox0-system storage-proxy

# View logs
kubectl logs -n sandbox0-system -l app=storage-proxy -f

# Check health
kubectl port-forward -n sandbox0-system svc/storage-proxy 8081:8081
curl http://localhost:8081/health
```

## Configuration

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `JWT_SECRET` | JWT signing secret (required) | - |
| `DEFAULT_META_URL` | PostgreSQL URL for JuiceFS metadata | - |
| `AWS_ACCESS_KEY_ID` | AWS access key (if not using IRSA) | - |
| `AWS_SECRET_ACCESS_KEY` | AWS secret key (if not using IRSA) | - |
| `AWS_REGION` | AWS region | `us-east-1` |
| `GRPC_PORT` | gRPC server port | `8080` |
| `HTTP_PORT` | HTTP management API port | `8081` |
| `METRICS_PORT` | Prometheus metrics port | `9090` |
| `LOG_LEVEL` | Log level (debug, info, warn, error) | `info` |
| `AUDIT_LOG` | Enable audit logging | `true` |
| `DEFAULT_CACHE_SIZE` | Default cache size per volume | `1G` |
| `DEFAULT_CACHE_DIR` | Cache directory | `/var/lib/storage-proxy/cache` |

## API Reference

### gRPC API

The gRPC API provides POSIX-like file system operations:

- `GetAttr` - Get file/directory attributes
- `Lookup` - Look up entry in directory
- `Open` - Open file
- `Read` - Read file data
- `Write` - Write file data
- `Create` - Create file
- `Mkdir` - Create directory
- `Unlink` - Delete file
- `ReadDir` - Read directory entries
- `Rename` - Rename file/directory
- `SetAttr` - Set file attributes
- `Flush` - Flush file data
- `Fsync` - Synchronize file data
- `Release` - Close file

All operations require JWT authentication via the `Authorization: Bearer <token>` header.

### HTTP Management API

#### Health Check

```bash
GET /health
```

#### List Volumes

```bash
GET /api/v1/volumes
```

#### Get Volume Info

```bash
GET /api/v1/volumes/{volume_id}
```

#### Mount Volume

```bash
POST /api/v1/volumes/{volume_id}
Content-Type: application/json

{
  "action": "mount",
  "config": {
    "meta_url": "postgres://...",
    "s3_bucket": "sandbox0-volumes",
    "s3_prefix": "teams/team-123/volumes/vol-456",
    "s3_region": "us-east-1",
    "cache_size": "1G",
    "cache_dir": "/var/lib/storage-proxy/cache/vol-456"
  }
}
```

#### Unmount Volume

```bash
POST /api/v1/volumes/{volume_id}
Content-Type: application/json

{
  "action": "unmount"
}
```

### Metrics

Prometheus metrics are exposed at `/metrics` on the metrics port (default 9090):

- `storage_proxy_volumes_mounted` - Number of mounted volumes
- `storage_proxy_operations_total` - Total operations by type
- `storage_proxy_operations_duration_seconds` - Operation latency
- `storage_proxy_cache_hit_rate` - Cache hit rate
- `storage_proxy_s3_operations_total` - S3 operations
- `storage_proxy_authentication_total` - Authentication attempts

## Security

### JWT Token Authentication

All gRPC requests must include a valid JWT token:

```
Authorization: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...
```

Token claims:
```json
{
  "volume_id": "vol-abc123",
  "sandbox_id": "sb-def456",
  "team_id": "team-789",
  "exp": 1706745600,
  "iat": 1706659200
}
```

Tokens are generated by the Manager service when claiming a sandbox.

### Network Security

- **Network Policies**: Restrict ingress/egress to authorized services
- **Packet Marking**: Compatible with nftables-based network isolation in Procd
- **mTLS**: Optional service mesh integration (Istio/Linkerd)

### Credential Isolation

- S3 and PostgreSQL credentials are **never** exposed to Procd or user code
- Credentials stored in Kubernetes Secrets
- IRSA recommended for AWS credentials (no static keys)

## Monitoring

### Logs

Structured JSON logs are written to stdout:

```json
{
  "level": "info",
  "msg": "Audit event",
  "volume_id": "vol-123",
  "sandbox_id": "sb-456",
  "operation": "read",
  "size": 4096,
  "latency_ms": 5,
  "status": "success"
}
```

### Audit Logs

All file operations are logged with:
- Timestamp
- Volume ID
- Sandbox ID
- Operation type
- Path/Inode
- Size (for read/write)
- Latency
- Status (success/error)

## Troubleshooting

### Volume Mount Fails

```bash
# Check PostgreSQL connectivity
kubectl exec -n sandbox0-system storage-proxy-0 -- \
  psql "$DEFAULT_META_URL" -c "SELECT 1"

# Check S3 access
kubectl exec -n sandbox0-system storage-proxy-0 -- \
  aws s3 ls s3://sandbox0-volumes/
```

### Authentication Errors

```bash
# Verify JWT secret matches between Manager and Storage Proxy
kubectl get secret -n sandbox0-system storage-proxy-secrets -o yaml

# Check token validity (use jwt.io or jwt-cli)
echo "$TOKEN" | jwt decode -
```

### Performance Issues

```bash
# Check cache metrics
curl http://storage-proxy:9090/metrics | grep cache

# Check S3 latency
curl http://storage-proxy:9090/metrics | grep s3_duration

# Increase cache size if hit rate is low
kubectl edit statefulset -n sandbox0-system storage-proxy
```

## Development

### Project Structure

```
storage-proxy/
├── cmd/
│   └── storage-proxy/      # Main entry point
├── pkg/
│   ├── auth/               # JWT authentication
│   ├── audit/              # Audit logging
│   ├── config/             # Configuration
│   ├── grpc/               # gRPC server implementation
│   ├── http/               # HTTP management API
│   ├── metrics/            # Prometheus metrics
│   └── volume/             # JuiceFS volume manager
├── proto/                  # Protocol buffer definitions
├── deploy/
│   └── k8s/                # Kubernetes manifests
├── Dockerfile
├── Makefile
└── README.md
```

### Testing

```bash
# Run unit tests
go test ./...

# Run with coverage
go test -cover ./...

# Run integration tests (requires PostgreSQL and S3)
go test -tags=integration ./...
```

