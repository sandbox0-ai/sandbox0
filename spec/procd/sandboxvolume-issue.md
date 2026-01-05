# SandboxVolume 设计问题清单

本文档记录 SandboxVolume 架构中已识别的设计问题和需要澄清的点。

---

## 问题 1: Token 过期与刷新机制

### 严重程度: 高

### 问题描述
spec 中提到 "Token expires and auto-refreshes"，但存在以下问题：
- Token 存储在 `MountContext.Token` 的内存中
- FUSE mount 是长期运行的（数小时到数天）
- JWT token 不可避免会过期

### 当前设计
```go
// From client-sandboxvolume.md:88-101
type MountContext struct {
    Token             string  // JWT auth token (in-memory only)
    grpcClient         fs.FileSystemClient
    fuseConn           *fuse.Conn
    ...
}
```

### 建议方案
- **方案 A**: 短期 Token + 定期刷新（需要 token refresh API）
- **方案 B**: 长期 Token（无 `exp` 或设置很长的过期时间）
- **方案 C**: gRPC auth error 时触发 re-attach 流程获取新 token

### 需要澄清
应该采用哪种方案？Procd 在 FUSE 操作过程中如何处理 token 过期？

---

## 问题 2: Attach/Detach 状态一致性

### 严重程度: 高

### 问题描述
Storage Proxy 记录的 attach 状态与 Procd 实际挂载状态可能不一致。

### 故障场景
```
1. Internal Gateway → Storage Proxy attach ✓ (token generated)
2. Internal Gateway → Procd mount ✗ (network failure / Procd crash)
   结果: Storage Proxy 认为已 attached，Procd 实际未挂载
```

### 当前设计
attach/detach 流程（storage-proxy.md:207-253, internal-gateway.md:234-290）缺少：
- 原子性保证
- 部分失败时的回滚机制

### 需要的解决方案
- Internal Gateway 必须在 Procd mount 失败时调用 Storage Proxy detach
- 或者通过 health check 实现最终一致性

---

## 问题 3: Procd 重启后挂载状态丢失

### 严重程度: 中

### 问题描述
- 挂载状态只存在于 Procd 内存中（`map[string]*MountContext`）
- 如果 Procd 进程重启（crash 或正常重启），挂载信息会丢失
- FUSE mount point 可能变成 "ghost mount"（FUSE 进程已死，挂载点仍存在）

### 需要的解决方案
- Procd 需要在启动时恢复挂载状态，或者
- Sandbox 删除时必须强制清理挂载点

---

## 问题 4: Snapshot Restore 期间的挂载状态

### 严重程度: 中

### 问题描述
Restore API（storage-proxy.md:275-291）只恢复 JuiceFS 数据。
如果 SandboxVolume 在 restore 期间已挂载：
- FUSE cache 是否会失效？
- 用户是否需要重新挂载才能看到恢复后的数据？

### 需要澄清
需要定义 volume 已挂载时 restore 的行为约束和通知机制。

---

## 问题 5: Packet Marking 可移植性

### 严重程度: 低 (设计限制)

### 问题描述
```go
// From client-sandboxvolume.md:495-523
syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, 0x24, 0x2)
```
- `SO_MARK` (0x24) 是 Linux 特有的 socket option
- 限制了 Sandbox0 只能在 Linux 上运行
- 不可移植到 macOS/Windows 容器平台

### 需要的行动
明确记录平台限制，或考虑跨平台支持的替代方案。

---

## 问题 6: Storage Proxy 与 JuiceFS 元数据耦合

### 严重程度: 低 (架构关注点)

### 问题描述
当前设计（storage-proxy.md:77-78）：
```
PostgreSQL (juicefs metadata + sandboxvolume metadata)
```

### 潜在问题
- 业务元数据与 JuiceFS 系统元数据耦合在同一数据库
- 如果需要将 JuiceFS 迁移到独立的 Redis/MySQL，需要进行数据库拆分
- JuiceFS 版本升级可能与业务表结构冲突

### 建议缓解方案
使用 PostgreSQL schema 隔离：`juicefs.*` vs `sandbox0.*`

---

## 问题 7: Snapshot/Restore 性能与限制

### 严重程度: 低 (文档缺失)

### 缺失信息
- snapshot 是 JuiceFS snapshot 还是 S3 snapshot？
- 大型卷的 snapshot 需要多长时间？
- restore 是否会影响正在进行的读写操作？
- 是否有 quota 限制（snapshot 数量、存储空间）？

### 需要补充文档
在 spec 中添加性能特性和限制说明。

---

## 总结

| 问题类型 | 严重程度 | 数量 |
|---------|---------|-----|
| 需要澄清的设计决策 | 高 | 2 |
| 边界情况处理 | 中 | 2 |
| 性能优化 | 中 | 1 |
| 架构关注点 | 低 | 2 |
| 文档缺失 | 低 | 1 |

### 整体评估
架构设计是**合理且清晰的**，主要关注点在于：
1. **错误处理与状态一致性**（token 过期、attach 失败回滚）
2. **边界情况处理**（Procd 重启、restore cache 失效）
3. **性能优化**（Procd本地读缓存、Node亲和性路由）

这些问题应在实现前予以澄清。
