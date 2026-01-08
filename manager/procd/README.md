# Procd

Procd is the core container component (PID=1) of Sandbox0, responsible for process management, file operations, and SandboxVolume mounting within the sandbox.

## Overview

Procd runs as the init process inside each sandbox pod and provides:

1. **Process Management**: Unified process abstraction supporting REPL and Shell process types
2. **SandboxVolume Management**: Persistent storage mounting via FUSE and gRPC to Storage Proxy
3. **File Operations**: File read/write, directory operations, and file system watching

## Architecture

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                        Procd Architecture                                    │
├──────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│   Procd (PID=1)                                                              │
│   ┌───────────────────────────────────────────────────────────────────────┐  │
│   │                        HTTP Server (Port: 8080)                       │  │
│   │  ┌─────────────┐ ┌─────────────┐                 ┌─────────────┐      │  │
│   │  │  Context    │ │SandboxVolume│                 │   File      │      │  │
│   │  │   APIs      │ │    APIs     │                 │   APIs      │      │  │
│   │  └─────────────┘ └─────────────┘                 └─────────────┘      │  │
│   └───────────────────────────────────────────────────────────────────────┘  │
│                                    │                                         │
│                                    ▼                                         │
│   ┌───────────────────────────────────────────────────────────────────────┐  │
│   │                        Core Managers                                  │  │
│   │  ┌─────────────┐ ┌─────────────┐                 ┌─────────────┐      │  │
│   │  │  Context    │ │SandboxVolume│                 │   File      │      │  │
│   │  │  Manager    │ │  Manager    │                 │  Manager    │      │  │
│   │  └─────────────┘ └─────────────┘                 └─────────────┘      │  │
│   └───────────────────────────────────────────────────────────────────────┘  │
│                                                                              │
└──────────────────────────────────────────────────────────────────────────────┘
```

## Building

```bash
# Build binary
make build

# Build for Linux
make build-linux

# Build Docker image
make docker-build

# Run tests
make test

# Run linter
make lint
```

## Configuration

Environment variables:

| Variable | Description | Default |
|----------|-------------|---------|
| `SANDBOX_ID` | Sandbox identifier | - |
| `TEMPLATE_ID` | Template identifier | - |
| `NODE_NAME` | Kubernetes node name | - |
| `PROCD_HTTP_PORT` | HTTP server port | 8080 |
| `PROCD_LOG_LEVEL` | Log level (debug/info/warn/error) | info |
| `PROCD_ROOT_PATH` | Root path for file operations | /workspace |
| `PROCD_MAX_CONTEXTS` | Maximum number of contexts | 100 |
| `STORAGE_PROXY_BASE_URL` | Storage Proxy base URL | storage-proxy.sandbox0-system.svc.cluster.local |
| `STORAGE_PROXY_REPLICAS` | Number of Storage Proxy replicas | 3 |

## API Endpoints

### Health
- `GET /healthz` - Health check
- `GET /readyz` - Readiness check

### Context Management
- `GET /api/v1/contexts` - List contexts
- `POST /api/v1/contexts` - Create context
- `GET /api/v1/contexts/{id}` - Get context
- `DELETE /api/v1/contexts/{id}` - Delete context
- `POST /api/v1/contexts/{id}/restart` - Restart context
- `POST /api/v1/contexts/{id}/input` - Write input
- `GET /api/v1/contexts/{id}/ws` - WebSocket for I/O

### SandboxVolume
- `POST /api/v1/sandboxvolumes/mount` - Mount volume
- `POST /api/v1/sandboxvolumes/unmount` - Unmount volume
- `GET /api/v1/sandboxvolumes/status` - Get mount status

### Files
- `GET /api/v1/files/{path}` - Read file
- `GET /api/v1/files/{path}?stat=true` - Get file info
- `GET /api/v1/files/{path}?list=true` - List directory
- `POST /api/v1/files/{path}` - Write file
- `POST /api/v1/files/{path}?mkdir=true` - Create directory
- `POST /api/v1/files/move` - Move file
- `DELETE /api/v1/files/{path}` - Delete file
- `GET /api/v1/files/watch` - WebSocket for file watching

## Security

### Network Isolation

Network isolation is NOT handled by procd. It is managed by the `netd` service, which runs as a DaemonSet on each node and enforces network policies via CRDs (`SandboxNetworkPolicy` and `SandboxBandwidthPolicy`).

The `netd` service provides:
- **IP/CIDR filtering**: Precise control over outbound traffic destinations
- **Domain filtering**: Support for domain and wildcard domain filtering (via L7 proxy)
- **DNS spoofing protection**: Independent DNS resolution with rebinding protection
- **Private IP blacklist**: Default blocking of private network ranges
- **Bandwidth control**: Per-sandbox rate limiting and metering
- **Audit logging**: Connection auditing for security and billing

See `infra/netd/README.md` for details.

### Required Capabilities

procd no longer requires `NET_ADMIN` capability. The container runs with default unprivileged capabilities, but it is recommended to drop unnecessary ones like `NET_RAW`:

```yaml
securityContext:
  capabilities:
    drop:
    - NET_RAW
```

### Recommended: Kata Containers

For enhanced security isolation, use Kata Containers runtime:

```yaml
spec:
  runtimeClassName: kata
```

## Monitoring

Procd exposes Prometheus metrics:

```
# Process metrics
procd_contexts_total              # Current context count
procd_contexts_by_type            # Count by type

# SandboxVolume metrics
procd_sandboxvolumes_mounted      # Mounted volume count
procd_sandboxvolume_mount_duration_ms

# File metrics
procd_file_operations_total       # File operation count
procd_file_watchers_active        # Active watcher count
```

## License

Proprietary - Sandbox0 Inc.

