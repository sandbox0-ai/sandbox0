# Storage Proxy - Design Specification

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
│  Procd (Pod)                     Storage Proxy (Independent Service)         │
│  ┌─────────────────────┐       ┌─────────────────────────────────────────┐ │
│  │ /workspace (FUSE)   │       │ gRPC Server                            │ │
│  │   └─ test.txt       │◄──────├─► FileSystemService                    │ │
│  │                     │ gRPC  │  ├─ Read/Write/Create/Mkdir           │ │
│  │ RemoteFS (FUSE)     │       │  ├─ GetAttr/Lookup/ReadDir            │ │
│  │   └─ gRPC Client    │       │  └─ Rename/Flush/Fsync               │ │
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
│                                   │  ├── PostgreSQL (juicefs metadata)      │││
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
├── gRPC Server (port 8080)
│   └── FileSystemService
│
├── JuiceFS SDK Layer
│   ├── vfs.VFS (in-memory filesystem)
│   ├── meta.Client (PostgreSQL)
│   └── chunk.CachedStore (S3 + local cache)
│
├── Volume Management
│   ├── Mount/Unmount volumes
│   ├── Volume lifecycle
│   └─→ Cache management
│
└── Security Layer
    ├── JWT token validation
    ├── Volume access control
    ├── Audit logging
    └─→ Rate limiting
```

---

## 四、gRPC Protocol

FileSystem 服务提供完整的 POSIX 文件系统操作：

- **文件操作**：Read, Write, Create, Open, Release
- **目录操作**：Mkdir, ReadDir, Lookup
- **元数据操作**：GetAttr, SetAttr
- **高级操作**：Unlink, Rename, Flush, Fsync
- **卷管理**：MountVolume, UnmountVolume

---

## 五、安全认证

### 5.1 JWT Token Authentication

基于 JWT token 的认证机制：

**Token 结构**：
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

**认证流程**：
1. 客户端在请求头中发送 `Authorization: Bearer <token>`
2. 服务端验证 token 签名和过期时间
3. 验证 sandbox 对 volume 的访问权限
4. 允许访问或返回认证错误

### 5.2 Token 生命周期

- **生成**：Manager 在 sandbox 分配时生成 24 小时有效期 token
- **使用**：Procd RemoteFS 在每个 gRPC 调用中发送 token
- **验证**：Storage Proxy 验证 token 并提取权限信息
- **过期**：Token 过期后自动刷新
- **撤销**：Sandbox 删除时立即撤销相关 token

---

## 六、部署配置

### 6.1 Kubernetes Deployment

Storage Proxy 作为 StatefulSet 部署：

- **副本数**：3 个副本提供高可用性
- **存储**：本地 SSD 缓存 (100GB)
- **网络**：gRPC (8080), HTTP 管理 (8081)
- **资源**：2CPU, 4GB RAM (请求), 4CPU, 8GB RAM (限制)

### 6.2 Procd Configuration

Procd 通过环境变量配置 Storage Proxy 地址：

```yaml
env:
  - name: STORAGE_PROXY_URL
    value: "storage-proxy.sandbox0-system.svc.cluster.local:8080"
```

---

## 七、监控与观测

### 7.1 Metrics

暴露 Prometheus 指标：

- **卷指标**：已挂载卷数、挂载错误数
- **操作指标**：操作计数、延迟分布、错误统计
- **缓存指标**：命中率、使用字节数
- **S3 指标**：S3 操作计数、数据传输量

### 7.2 Audit Logging

所有操作记录审计日志：
- 时间戳、卷 ID、Sandbox ID
- 操作类型、路径、大小
- 延迟、状态、错误信息

---

## 八、优势总结

| 特性 | 说明 |
|------|------|
| **凭证隔离** | 所有 S3/PG 凭证仅在 Proxy 中 |
| **零 JuiceFS 修改** | 使用官方 Go SDK |
| **完整 POSIX** | gRPC 提供完整文件系统语义 |
| **网络隔离兼容** | Packet marking 绕过 Procd 防火墙 |
| **高性能** | gRPC over HTTP/2，支持流式传输 |
| **集中式缓存** | Proxy 端缓存，多 Pod 共享 |
| **审计日志** | 所有操作集中记录 |
| **独立扩展** | Proxy 可独立于 Sandbox 扩展 |
