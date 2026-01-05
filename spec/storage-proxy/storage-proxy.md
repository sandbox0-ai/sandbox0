# Storage Proxy - Design Specification

TODO:
1. sandboxvolume http api

## 一、设计目标

Storage Proxy 是一个独立的服务，负责管理所有持久化存储访问，将 JuiceFS 完全从 Procd 中移除。

**VERY IMPORTANT**: storage-proxy作为sandbox0的底层存储依赖,**稳定性大于一切**,非必要不添加新功能,尽量使用juicefs sdk而不自己实现.

### 核心原则

1. **凭证隔离**：所有 S3、PostgreSQL 凭证仅在 Proxy 中存储
2. **零 JuiceFS 修改**：使用 JuiceFS 官方 Go SDK，无需修改源码
3. **网络隔离兼容**：通过 packet marking 绕过 Procd 网络规则
4. **高性能**：gRPC over HTTP/2，支持流式传输

---

## 二、架构设计

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    Storage Proxy Architecture                                │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  Internal Gateway (Coordinator)                                              │
│       │                                                                      │
│       ├──► Storage Proxy (HTTP Port 8081)                                    │
│       │    - SandboxVolume CRUD                                              │
│       │    - Attach (generates token)                                        │
│       │    - Detach                                                          │
│       │                                                                      │
│       └──► Procd (HTTP Port 8080)                                            │
│            - Mount (with token from Storage Proxy)                           │
│            - Unmount                                                          │
│                                                                              │
│  Storage Proxy (Independent Service)                                         │
│  ┌─────────────────────────────────────────────────────────────────────────┐ │
│  │ HTTP Server (Port 8081)                                                 │ │
│  │  ┌───────────────────────────────────────────────────────────────────┐ │ │
│  │  │ SandboxVolume Management                                          │ │ │
│  │  │  - Create/Delete SandboxVolume                                    │ │ │
│  │  │  - Attach (generates token for Procd)                             │ │ │
│  │  │  - Detach                                                          │ │ │
│  │  │  - Snapshot/Restore                                               │ │ │
│  │  └───────────────────────────────────────────────────────────────────┘ │ │
│  │                              │                                          │ │
│  │                              ▼                                          │ │
│  │  ┌───────────────────────────────────────────────────────────────────┐ │ │
│  │  │ SandboxVolume Metadata (PostgreSQL)                               │ │ │
│  │  │  - volumes table                                                   │ │ │
│  │  │  - snapshots table                                                 │ │ │
│  │  └───────────────────────────────────────────────────────────────────┘ │ │
│  └─────────────────────────────────────────────────────────────────────────┘ │
│                                                                              │
│  Procd (Pod)                     Storage Proxy (gRPC Server)                 │
│  ┌─────────────────────┐       ┌─────────────────────────────────────────┐ │
│  │ /workspace (FUSE)   │       │ gRPC Server (Port 8080)                 │ │
│  │   └─ test.txt       │◄──────├─► FileSystemService                    │ │
│  │                     │ gRPC  │  ├─ Read/Write/Create/Mkdir           │ │ │
│  │ RemoteFS (FUSE)     │       │  ├─ GetAttr/Lookup/ReadDir            │ │ │
│  │   └─ gRPC Client    │       │  └─ Rename/Flush/Fsync               │ │ │
│  └─────────────────────┘       └─────────────────────────────────────────┘ │
│           ▲                                   │                             │
│           │                                   ▼                             │
│      nftables                    ┌─────────────────────────────────────────┐│
│      mark==0x2 → ACCEPT          │ JuiceFS Embedded Library (SDK Mode)    ││
│                                   │  ┌───────────────────────────────────┐││
│                                   │  │ vfs.VFS (In-memory, no FUSE)      │││
│                                   │  ├─► meta.Client → PostgreSQL        │││
│                                   │  └─► chunk.CachedStore → S3          │││
│                                   │  └───────────────────────────────────┘││
│                                   └─────────────────────────────────────────┘│
│                                                              │               │
│                                                              ▼               │
│                                   ┌─────────────────────────────────────────┐│
│                                   │ Storage Backend (Real Credentials)      │││
│                                   │  ├── PostgreSQL (juicefs metadata +     │││
│                                   │  │             sandboxvolume metadata)  │││
│                                   │  └── S3 (chunk data)                   │││
│                                   └─────────────────────────────────────────┘│
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 三、组件设计

### 3.1 服务组成

```
storage-proxy/
├── HTTP Server (Port 8081)
│   └── SandboxVolumeService
│       ├── Create/Delete SandboxVolume
│       ├── Attach (generates token for Procd)
│       ├── Detach
│       ├── Snapshot/Restore
│       └── List/Get SandboxVolume
│
├── gRPC Server (Port 8080)
│   └── FileSystemService
│
├── JuiceFS SDK Layer
│   ├── vfs.VFS (in-memory filesystem)
│   ├── meta.Client (PostgreSQL)
│   └── chunk.CachedStore (S3 + local cache)
│
├── SandboxVolume Manager
│   ├── Metadata management (PostgreSQL)
│   ├── JuiceFS volume management
│   └─→ Cache management
│
└── Security Layer
    ├── JWT token generation/validation
    ├── SandboxVolume access control
    ├── Audit logging
    └─→ Rate limiting
```

---

## 四、SandboxVolume HTTP API

### 4.1 Create SandboxVolume

```http
POST /api/v1/sandboxvolumes
Content-Type: application/json
Authorization: Bearer <api_key>

{
    "name": "my-workspace",
    "team_id": "team-123",
    "capacity_gb": 10
}

Response: 201 Created
{
    "id": "sbv-abc123",
    "name": "my-workspace",
    "team_id": "team-123",
    "capacity_gb": 10,
    "juicefs_config": {
        "s3_prefix": "teams/team-123/sandboxvolumes/sbv-abc123"
    },
    "created_at": "2024-01-01T00:00:00Z"
}
```

### 4.2 List SandboxVolumes

```http
GET /api/v1/sandboxvolumes?team_id=team-123
Authorization: Bearer <api_key>

Response: 200 OK
{
    "sandboxvolumes": [
        {
            "id": "sbv-abc123",
            "name": "my-workspace",
            "team_id": "team-123",
            "capacity_gb": 10,
            "size_bytes": 1024000,
            "created_at": "2024-01-01T00:00:00Z"
        }
    ]
}
```

### 4.3 Get SandboxVolume

```http
GET /api/v1/sandboxvolumes/{sandboxvolume_id}
Authorization: Bearer <api_key>

Response: 200 OK
{
    "id": "sbv-abc123",
    "name": "my-workspace",
    "team_id": "team-123",
    "capacity_gb": 10,
    "size_bytes": 1024000,
    "file_count": 42,
    "mount_count": 1,
    "created_at": "2024-01-01T00:00:00Z",
    "last_accessed_at": "2024-01-01T01:00:00Z"
}
```

### 4.4 Delete SandboxVolume

```http
DELETE /api/v1/sandboxvolumes/{sandboxvolume_id}
Authorization: Bearer <api_key>

Response: 200 OK
{
    "deleted": true,
    "juicefs_deleted": true,
    "s3_objects_deleted": 1523
}
```

### 4.5 Attach to Sandbox (Prepare Token)

> **Note**: This API only prepares the mount token. The actual mount is performed by Procd after receiving the token via internal-gateway.

```http
POST /api/v1/sandboxvolumes/{sandboxvolume_id}/attach
Content-Type: application/json
Authorization: Bearer <api_key>

{
    "sandbox_id": "sb-456",
    "mount_point": "/workspace",
    "read_only": false,
    "snapshot_id": "snap-001"
}

Response: 200 OK
{
    "sandboxvolume_id": "sbv-abc123",
    "sandbox_id": "sb-456",
    "mount_point": "/workspace",
    "token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...",
    "storage_proxy_address": "storage-proxy.sandbox0-system.svc.cluster.local:8080"
}
```

**Response fields:**
- `token`: JWT token for Procd to authenticate gRPC calls to Storage Proxy
- `storage_proxy_address`: gRPC server address for Procd to connect

### 4.6 Detach from Sandbox

```http
POST /api/v1/sandboxvolumes/{sandboxvolume_id}/detach
Content-Type: application/json
Authorization: Bearer <api_key>

{
    "sandbox_id": "sb-456"
}

Response: 200 OK
{
    "detached": true
}
```

> **Note**: After detach, internal-gateway should call Procd to unmount the volume.

### 4.7 Create Snapshot

```http
POST /api/v1/sandboxvolumes/{sandboxvolume_id}/snapshots
Content-Type: application/json
Authorization: Bearer <api_key>

{
    "name": "snapshot-before-changes"
}

Response: 201 Created
{
    "id": "snap-001",
    "sandboxvolume_id": "sbv-abc123",
    "name": "snapshot-before-changes",
    "created_at": "2024-01-01T00:00:00Z"
}
```

### 4.8 Restore from Snapshot

```http
POST /api/v1/sandboxvolumes/{sandboxvolume_id}/restore
Content-Type: application/json
Authorization: Bearer <api_key>

{
    "snapshot_id": "snap-001"
}

Response: 200 OK
{
    "restored": true,
    "snapshot_id": "snap-001"
}
```

---

## 五、gRPC Protocol

FileSystem 服务提供完整的 POSIX 文件系统操作：

- **文件操作**：Read, Write, Create, Open, Release
- **目录操作**：Mkdir, ReadDir, Lookup
- **元数据操作**：GetAttr, SetAttr
- **高级操作**：Unlink, Rename, Flush, Fsync

---

## 六、安全认证

### 6.1 JWT Token Authentication

基于 JWT token 的认证机制：

**Token 结构**：
```json
{
  "sandboxvolume_id": "sbv-abc123",
  "sandbox_id": "sb-def456",
  "team_id": "team-789",
  "exp": 1706745600,
  "iat": 1706659200,
  "iss": "storage-proxy"
}
```

**认证流程**：
1. 客户端在请求头中发送 `Authorization: Bearer <token>`
2. 服务端验证 token 签名和过期时间
3. 验证 sandbox 对 sandboxvolume 的访问权限
4. 允许访问或返回认证错误

### 6.2 Token 生命周期

- **生成**：Storage Proxy 在 sandboxvolume attach 时生成 token
- **使用**：Procd RemoteFS 在每个 gRPC 调用中发送 token
- **验证**：Storage Proxy 验证 token 并提取权限信息
- **过期**：Token 过期后自动刷新
- **撤销**：Sandbox 删除或 volume detach 时立即撤销相关 token

---

## 七、部署配置

### 7.1 Kubernetes Deployment

Storage Proxy 作为 StatefulSet 部署：

- **副本数**：3 个副本提供高可用性
- **存储**：本地 SSD 缓存 (100GB)
- **网络**：gRPC (8080), HTTP (8081)
- **资源**：2CPU, 4GB RAM (请求), 4CPU, 8GB RAM (限制)

### 7.2 Procd Configuration

Procd 通过环境变量配置 Storage Proxy 地址：

```yaml
env:
  - name: STORAGE_PROXY_URL
    value: "storage-proxy.sandbox0-system.svc.cluster.local:8080"
```

---

## 八、监控与观测

### 8.1 Metrics

暴露 Prometheus 指标：

- **SandboxVolume指标**：已挂载数量、挂载错误数
- **操作指标**：操作计数、延迟分布、错误统计
- **缓存指标**：命中率、使用字节数
- **S3 指标**：S3 操作计数、数据传输量

### 8.2 Audit Logging

所有操作记录审计日志：
- 时间戳、SandboxVolume ID、Sandbox ID
- 操作类型、路径、大小
- 延迟、状态、错误信息

---

## 九、优势总结

| 特性 | 说明 |
|------|------|
| **统一管理** | SandboxVolume元数据与JuiceFS元数据统一在PostgreSQL |
| **零 JuiceFS 修改** | 使用官方 Go SDK |
| **完整 POSIX** | gRPC 提供完整文件系统语义 |
| **网络隔离兼容** | Packet marking 绕过 Procd 防火墙 |
| **高性能** | gRPC over HTTP/2，支持流式传输 |
| **集中式缓存** | Proxy 端缓存，多 Pod 共享 |
| **审计日志** | 所有操作集中记录 |
| **独立扩展** | Proxy 可独立于 Sandbox 扩展 |
