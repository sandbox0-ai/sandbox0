# Sandbox0 Manager 设计规范

## 一、设计目标

Manager 是 sandbox0 的核心管理服务，负责：
1. **沙箱模板管理**：通过 K8s Operator 维护 `SandboxTemplate` CRD
2. **沙箱实例认领**：提供 HTTP API 接收 internal-gateway 请求来实例化沙箱
3. **资源调度**：根据模板配置和资源水位进行沙箱调度

---

## 二、核心设计：ReplicaSet + Pod 状态存储

### 2.1 状态存储架构

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        State Storage in k8s                                 │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  1. SandboxTemplate CRD - 模板配置（唯一事实来源）                            │
│     ├── spec.pool.minIdle - ReplicaSet 副本数                                │
│     └── status - 活跃/空闲数统计                                             │
│                                                                              │
│  2. ReplicaSet - 维护空闲池 (replicas = minIdle)                              │
│     └── Selector: pool-type=idle                                             │
│                                                                              │
│  3. Pod - Sandbox 实例                                                       │
│     ├── Labels:                                                             │
│     │   ├── sandbox0.ai/template-id: python-dev                              │
│     │   ├── sandbox0.ai/pool-type: idle/active                               │
│     │   └── sandbox0.ai/sandbox-id: sb-xxx                                   │
│     └── Annotations:                                                         │
│         ├── sandbox0.ai/team-id: team-123                                    │
│         ├── sandbox0.ai/user-id: user-456                                    │
│         ├── sandbox0.ai/claimed-at: 2024-01-01T00:00:00Z                     │
│         ├── sandbox0.ai/expires-at: 2024-01-01T01:00:00Z                     │
│         └── sandbox0.ai/config: {"env_vars": {...}, "ttl": 3600}            │
│                                                                              │
│  4. Cleanup Controller                                                       │
│     ├── 监控 maxIdle（List Pods 删除多余）                                   │
│     ├── 过期清理（List Pods 过滤 expires_at < now）                           │
│     └── 回收释放（active → idle 改 labels）                                  │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 三、Operator 逻辑

### 3.1 Reconcile 流程

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    Operator Reconcile Flow (ReplicaSet)                     │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  1. Watch SandboxTemplate CRD                                               │
│     │                                                                       │
│     ▼                                                                       │
│  2. 检查变化类型 (Create/Update/Delete)                                      │
│     │                                                                       │
│     ├─► Create: 创建 ReplicaSet (replicas=minIdle)                         │
│     ├─► Update: 更新 ReplicaSet replicas                                   │
│     └─► Delete: 清理 ReplicaSet                                            │
│     │                                                                       │
│     ▼                                                                       │
│  3. ReplicaSet 管理                                                         │
│     │                                                                       │
│     ├─► 创建/更新 ReplicaSet                                                │
│     │   - Selector: sandbox0.ai/template-id=<name>, pool-type=idle          │
│     │   - Replicas: minIdle                                                 │
│     │   - Template: Pod Template（含 procd 容器）                            │
│     │                                                                       │
│     ├─► k8s ReplicaSet 控制器自动维持 Pod 数量                              │
│     │   - Pod 不足时立即创建                                                │
│     │   - Pod 被认领移出后立即补充                                          │
│     │                                                                       │
│     ▼                                                                       │
│  4. 状态同步                                                                │
│     │                                                                       │
│     ├─► 从 informer cache 查询实际 idle/active 数量                           │
│     ├─► 更新 CRD Status                                                    │
│     └─► 更新 Conditions                                                    │
│     │                                                                       │
│     ▼                                                                       │
│  5. 事件记录                                                                │
│     │                                                                       │
│     └─► 发送 K8s Events                                                    │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 3.2 PoolManager

负责管理 ReplicaSet：
- 确保 ReplicaSet 存在且配置正确
- 检查 replicas 是否匹配 minIdle
- 从 informer cache 查询实际状态并更新 CRD Status

### 3.3 Cleanup Controller

定期执行的清理任务：
- **enforceMaxIdle**: 强制执行 maxIdle 限制，删除多余空闲 Pod
- **expiredCleanup**: 清理过期的 active Pod（expires_at < now）
- **reclaimReleased**: 回收释放的 sandbox（active → idle 改 labels）

---

## 四、与 Procd 的交互

### 4.1 网络策略应用

优先级: `Config.Network` > `Template.Spec.Network` > `默认 allow-all`

通过 Procd API 动态更新网络策略，无需重启 sandbox。

---

## 五、与 E2B 功能对比

| 功能 | E2B | Sandbox0 | 说明 |
|------|-----|----------|------|
| 模板定义 | JSON + Dockerfile | CRD | K8s 原生 |
| 多容器 | 不支持 | ✅ Sidecar | 更灵活 |
| 资源配额 | CPU/Memory/Disk | CPU/Memory/GPU | 支持 GPU |
| 水池管理 | 内置 | **ReplicaSet + Cleanup Controller** | k8s 原生，高可靠 |
| 状态存储 | 内置 | **Pod annotations (informer cache)** | 无额外依赖 |
| 事件系统 | 内置 | **k8s Events** | 原生集成 |
| 冷启动 | 取决于池大小 | **ReplicaSet 实时补充** | 池更可靠 |
| 元数据引擎 | 内置 | **PostgreSQL (统一)** | 无需 Redis，简化架构 |

---

## 六、总结

### 设计优势

1. **纯 k8s 原生**：CRD + Operator + ReplicaSet + Informer，无额外依赖
2. **高性能**：informer 本地缓存读取 <1ms，比 PG 网络查询更快
3. **高可靠**：ReplicaSet 控制器实时维持 minIdle，毫秒级检测 Pod 缺失
4. **灵活的多容器**：主容器 + Sidecar，支持复杂架构
5. **职责分离**：Operator 管理 ReplicaSet，Cleanup Controller 处理缩容和回收

### 认领流程

```
空闲池认领（热路径）：
1. 从 informer cache 查询空闲 Pod（本地内存，<1ms）
2. 修改 labels（idle → active）和清除 ownerRef
3. ReplicaSet 自动补充新的空闲 Pod
耗时：几十毫秒

池为空认领（冷启动）：
1. 创建独立 Pod（不属于 ReplicaSet）
耗时：取决于镜像拉取时间
```
