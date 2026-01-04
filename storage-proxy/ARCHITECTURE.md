# Storage Proxy Architecture

This document provides an in-depth look at the Storage Proxy architecture, design decisions, and implementation details.

## Overview

Storage Proxy is a critical security component in the Sandbox0 architecture that provides:
1. Complete credential isolation for S3 and PostgreSQL access
2. Centralized file system operations through gRPC
3. JWT-based authentication and authorization
4. Comprehensive audit logging
5. High-performance caching with JuiceFS

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    Storage Proxy Architecture                                │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  Procd (Pod)                     Storage Proxy (StatefulSet)                 │
│  ┌─────────────────────┐       ┌─────────────────────────────────────────┐ │
│  │ /workspace (FUSE)   │       │ gRPC Server (port 8080)                 │ │
│  │   └─ test.txt       │◄──────├─► FileSystemService                      │ │
│  │                     │ gRPC  │  ├─ Read/Write/Create/Mkdir            │ │
│  │ RemoteFS (FUSE)     │ + JWT │  ├─ GetAttr/Lookup/ReadDir             │ │
│  │   └─ gRPC Client    │       │  └─ Rename/Flush/Fsync                │ │
│  └─────────────────────┘       └─────────────────────────────────────────┘ │
│           ▲                                   │                             │
│           │                                   ▼                             │
│      nftables                    ┌─────────────────────────────────────────┐│
│      mark==0x2 → ACCEPT          │ JWT Authentication Middleware          ││
│                                   │  ├─ Token validation                  ││
│                                   │  ├─ Volume access control             ││
│                                   │  └─ Audit logging                    ││
│                                   └─────────────────────────────────────────┘│
│                                                 │                            │
│                                                 ▼                            │
│                                   ┌─────────────────────────────────────────┐│
│                                   │ Volume Manager                         ││
│                                   │  ├─ Mount/Unmount volumes             ││
│                                   │  ├─ Volume lifecycle                  ││
│                                   │  └─ JuiceFS SDK integration           ││
│                                   └─────────────────────────────────────────┘│
│                                                 │                            │
│                                                 ▼                            │
│                                   ┌─────────────────────────────────────────┐│
│                                   │ JuiceFS Embedded Library (SDK Mode)    ││
│                                   │  ┌───────────────────────────────────┐││
│                                   │  │ vfs.Ik (In-memory, no FUSE)       │││
│                                   │  ├─► meta.Meta → PostgreSQL          │││
│                                   │  └─► chunk.CachedStore → S3          │││
│                                   │  └───────────────────────────────────┘││
│                                   └─────────────────────────────────────────┘│
│                                                              │               │
│                                                              ▼               │
│                                   ┌─────────────────────────────────────────┐│
│                                   │ Storage Backend                        ││
│                                   │  ├── PostgreSQL (metadata)             ││
│                                   │  │   - Inodes, directories, attributes ││
│                                   │  │   - File structure                  ││
│                                   │  └── S3 (chunks)                       ││
│                                   │      - File data in 64MB chunks        ││
│                                   └─────────────────────────────────────────┘│
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Component Details

### 1. gRPC Server

**Location**: `pkg/grpc/server.go`

The gRPC server implements the `FileSystem` service defined in `proto/filesystem.proto`. It provides:

- **File Operations**: Read, Write, Create, Mkdir, Unlink, Rename
- **Metadata Operations**: GetAttr, SetAttr, Lookup, ReadDir
- **Handle Management**: Open, Release, Flush, Fsync
- **Volume Management**: MountVolume, UnmountVolume

**Key Features**:
- Concurrent request handling
- Streaming support for large files
- Error handling with proper gRPC status codes
- Audit logging for all operations

### 2. JWT Authentication

**Location**: `pkg/auth/auth.go`

JWT-based authentication provides:

**Token Structure**:
```json
{
  "volume_id": "vol-abc123",
  "sandbox_id": "sb-def456",
  "team_id": "team-789",
  "exp": 1706745600,
  "iat": 1706659200,
  "iss": "storage-proxy"
}
```

**Authentication Flow**:
1. Client sends JWT token in `Authorization: Bearer <token>` header
2. Unary interceptor validates token signature
3. Token claims extracted and added to context
4. Volume access validated against claims
5. Request processed if authorized

**Security Features**:
- HMAC-SHA256 signing
- Expiration validation (24-hour default)
- Volume access control
- Token revocation support

### 3. Volume Manager

**Location**: `pkg/volume/manager.go`

The Volume Manager handles JuiceFS volume lifecycle:

**Mount Process**:
1. Initialize JuiceFS metadata client (PostgreSQL)
2. Load or create file system format
3. Create S3 object storage client
4. Initialize chunk store with local cache
5. Create VFS instance (SDK mode, no FUSE)
6. Store volume context for future operations

**Unmount Process**:
1. Flush all pending writes
2. Close metadata session
3. Shutdown chunk store
4. Remove volume context

**Key Features**:
- Multiple volume support
- Thread-safe volume map
- Automatic cache management
- Connection pooling

### 4. JuiceFS Integration

**Architecture**: SDK Mode (No FUSE)

Storage Proxy uses JuiceFS as a library, not as a FUSE filesystem:

```
┌─────────────────────────────────────────────────────┐
│ Storage Proxy                                        │
│                                                      │
│ ┌────────────────────────────────────────────────┐ │
│ │ vfs.Ik (Virtual File System Interface)         │ │
│ │  - In-memory operations                        │ │
│ │  - No FUSE overhead                            │ │
│ │  - Direct Go function calls                    │ │
│ └────────────────────────────────────────────────┘ │
│          │                          │               │
│          ▼                          ▼               │
│ ┌──────────────────┐    ┌──────────────────┐      │
│ │ meta.Meta        │    │ chunk.CachedStore│      │
│ │ (PostgreSQL)     │    │ (S3 + local cache)│      │
│ └──────────────────┘    └──────────────────┘      │
└─────────────────────────────────────────────────────┘
```

**Benefits**:
- No FUSE kernel module required
- Lower latency (no context switching)
- Better error handling
- Easier debugging

### 5. Caching Strategy

**Location**: `pkg/volume/manager.go`

JuiceFS chunk store provides multi-layer caching:

```
┌─────────────────────────────────────────────┐
│ Cache Hierarchy                              │
├─────────────────────────────────────────────┤
│                                              │
│ Layer 1: Memory Cache                       │
│  - Recently accessed blocks                 │
│  - Fast access (nanoseconds)                │
│  - Size: ~32MB per volume                   │
│                                              │
│ Layer 2: Local Disk Cache                   │
│  - Persistent cache on SSD                  │
│  - Size: 1GB default (configurable)         │
│  - Survives pod restarts (StatefulSet PVC)  │
│                                              │
│ Layer 3: S3 Object Storage                  │
│  - Permanent storage                        │
│  - Unlimited size                           │
│  - Higher latency (milliseconds)            │
│                                              │
└─────────────────────────────────────────────┘
```

**Cache Eviction**: LRU (Least Recently Used)

### 6. Security Model

**Credential Isolation Matrix**:

```
┌─────────────────┬──────────┬─────┬────────────┐
│ Component       │ Postgres │ S3  │ JWT Secret │
├─────────────────┼──────────┼─────┼────────────┤
│ Procd           │    ❌    │ ❌  │     ❌     │
│ RemoteFS        │    ❌    │ ❌  │     ❌     │
│ Storage Proxy   │    ✅    │ ✅  │     ✅     │
│ User Code       │    ❌    │ ❌  │     ❌     │
└─────────────────┴──────────┴─────┴────────────┘
```

**Network Security**:
- Packet marking (SO_MARK=0x2) bypasses nftables rules
- Network policies restrict ingress/egress
- Optional mTLS with service mesh

### 7. Audit Logging

**Location**: `pkg/audit/logger.go`

Every operation is logged with:
- Timestamp
- Volume ID
- Sandbox ID
- Operation type
- Inode/Path
- Size (for read/write)
- Latency
- Status (success/error)

**Log Format** (JSON):
```json
{
  "timestamp": "2024-01-01T12:00:00Z",
  "volume_id": "vol-abc123",
  "sandbox_id": "sb-def456",
  "operation": "write",
  "path": "/workspace/data.csv",
  "size_bytes": 12345,
  "latency_ms": 15,
  "status": "success"
}
```

### 8. Metrics and Monitoring

**Location**: `pkg/metrics/metrics.go`

Prometheus metrics exposed at `/metrics`:

**Volume Metrics**:
- `storage_proxy_volumes_mounted` - Active volumes
- `storage_proxy_volumes_mount_errors_total` - Mount failures

**Operation Metrics**:
- `storage_proxy_operations_total{operation, volume_id}` - Total operations
- `storage_proxy_operations_duration_seconds{operation, volume_id}` - Latency histogram
- `storage_proxy_operations_errors_total{operation, error_type}` - Error count

**Cache Metrics**:
- `storage_proxy_cache_hit_rate{volume_id}` - Cache efficiency
- `storage_proxy_cache_used_bytes{volume_id}` - Cache usage

**S3 Metrics**:
- `storage_proxy_s3_operations_total{operation}` - S3 API calls
- `storage_proxy_s3_bytes_total{operation}` - Data transferred

## Performance Characteristics

### Latency

**Read Operation** (cache hit):
- gRPC overhead: ~0.1ms
- Memory cache: ~0.01ms
- **Total: ~0.11ms**

**Read Operation** (disk cache hit):
- gRPC overhead: ~0.1ms
- Disk cache: ~0.5ms
- **Total: ~0.6ms**

**Read Operation** (cache miss):
- gRPC overhead: ~0.1ms
- S3 GET: ~50ms (varies by region)
- **Total: ~50.1ms**

**Write Operation**:
- gRPC overhead: ~0.1ms
- Memory buffer: ~0.01ms
- **Total: ~0.11ms** (async write to S3)

### Throughput

**Sequential Read** (warm cache):
- ~500 MB/s per volume
- Limited by network bandwidth

**Sequential Write**:
- ~300 MB/s per volume
- Async writeback to S3

**Random Read/Write** (4KB blocks):
- ~10,000 IOPS per volume
- Cache-dependent

### Scalability

**Horizontal Scaling**:
- Stateless operation handling
- Volume affinity not required
- Scale to 10+ replicas

**Vertical Scaling**:
- More CPU: Higher concurrent request handling
- More Memory: Larger memory cache
- More Disk: Larger disk cache (better hit rate)

## Design Decisions

### 1. Why SDK Mode vs FUSE?

**SDK Mode** (Chosen):
- ✅ No kernel module required
- ✅ Lower latency (no context switching)
- ✅ Better error handling
- ✅ Easier to debug
- ✅ Portable across environments

**FUSE Mode** (Not chosen):
- ❌ Requires FUSE kernel module
- ❌ Higher latency (user↔kernel transitions)
- ❌ Complex error handling
- ❌ Harder to debug

### 2. Why gRPC vs REST?

**gRPC** (Chosen):
- ✅ HTTP/2 multiplexing
- ✅ Binary protocol (efficient)
- ✅ Streaming support
- ✅ Strong typing with protobuf
- ✅ Built-in authentication

**REST** (Not chosen):
- ❌ HTTP/1.1 limitations
- ❌ JSON overhead
- ❌ No native streaming

### 3. Why StatefulSet vs Deployment?

**StatefulSet** (Chosen):
- ✅ Stable persistent storage (PVC)
- ✅ Stable network identity
- ✅ Ordered deployment/scaling
- ✅ Per-pod cache volumes

**Deployment** (Not chosen):
- ❌ No persistent storage
- ❌ Cache lost on pod restart

### 4. Why JWT vs mTLS?

**JWT** (Chosen):
- ✅ Simpler integration
- ✅ Token-based access control
- ✅ Easy to revoke
- ✅ Works without service mesh

**mTLS** (Optional, can be added):
- Both can be used together
- mTLS at transport layer
- JWT at application layer

## Future Enhancements

### Short Term

1. **Streaming Optimizations**
   - Implement chunked read/write
   - Add read-ahead prefetching
   - Optimize buffer sizes

2. **Cache Improvements**
   - Distributed cache with Redis
   - Cache warming on mount
   - Smarter eviction policies

3. **Monitoring Enhancements**
   - Distributed tracing (Jaeger)
   - Performance profiling
   - Anomaly detection

### Long Term

1. **Multi-Region Support**
   - Cross-region replication
   - Read-from-nearest-region
   - Global cache

2. **Advanced Security**
   - Hardware security module (HSM) integration
   - Key rotation automation
   - Zero-knowledge encryption

3. **Performance**
   - GPU acceleration for compression
   - RDMA support
   - Tiered storage (hot/warm/cold)

## Troubleshooting Guide

### High Latency

**Symptom**: p99 latency > 1s

**Diagnosis**:
```bash
# Check cache hit rate
curl http://storage-proxy:9090/metrics | grep cache_hit_rate

# Check S3 latency
curl http://storage-proxy:9090/metrics | grep s3_duration
```

**Solution**:
- Increase cache size if hit rate < 50%
- Check S3 region (use same region as cluster)
- Scale horizontally

### Out of Memory

**Symptom**: Pods OOMKilled

**Diagnosis**:
```bash
# Check memory usage
kubectl top pod -n sandbox0-system -l app=storage-proxy

# Check memory limits
kubectl get pod -n sandbox0-system storage-proxy-0 -o yaml | grep -A 5 resources
```

**Solution**:
- Increase memory limits
- Reduce memory cache size
- Check for memory leaks

### Authentication Failures

**Symptom**: Unauthenticated errors

**Diagnosis**:
```bash
# Check JWT secret
kubectl get secret -n sandbox0-system storage-proxy-secrets -o yaml

# Validate token
echo "$TOKEN" | jwt decode -
```

**Solution**:
- Ensure JWT_SECRET matches between Manager and Storage Proxy
- Check token expiration
- Verify volume access in database

