# Sandbox0 多区域部署架构

## 架构概览

Sandbox0 采用三层架构设计，支持水平扩展以实现近乎无限的沙箱容量：

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                              Global Service                                      │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐  │
│  │   账单系统   │  │  用户管理   │  │  配置中心   │  │  Global Router       │  │
│  │  (Billing)  │  │   (IAM)     │  │  (Config)   │  │  (按 Region 分流)    │  │
│  └─────────────┘  └─────────────┘  └─────────────┘  └─────────────────────┘  │
└─────────────────────────────────────────────────────────────────────────────────┘
                                       │
                                       ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                                  Region 层                                       │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                 │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │                    Region: aws/us-east-1                                │   │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────────────────┐ │   │
│  │  │ Edge Gateway│  │ PostgreSQL  │  │           S3                    │ │   │
│  │  │  (统一入口)  │  │ (元数据存储) │  │      (JuiceFS 数据块)          │ │   │
│  │  └─────────────┘  └─────────────┘  └─────────────────────────────────┘ │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                                                                                 │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │                    Region: aws/eu-west-1                                │   │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────────────────┐ │   │
│  │  │ Edge Gateway│  │ PostgreSQL  │  │           S3                    │ │   │
│  │  └─────────────┘  └─────────────┘  └─────────────────────────────────┘ │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                                                                                 │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │                    Region: gcp/us-east-1                                │   │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────────────────┐ │   │
│  │  │ Edge Gateway│  │ PostgreSQL  │  │           GCS                   │ │   │
│  │  └─────────────┘  └─────────────┘  └─────────────────────────────────┘ │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                                                                                 │
└─────────────────────────────────────────────────────────────────────────────────┘
                                       │
                                       ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                                 Cluster 层（数据平面）                           │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                 │
│  Region: aws/us-east-1                                                          │
│  ┌─────────────────────┐    ┌─────────────────────┐    ┌─────────────────────┐  │
│  │   Cluster 1         │    │   Cluster 2         │    │   Cluster N         │  │
│  │   (EKS)             │    │   (EKS)             │    │   (EKS)             │  │
│  │                     │    │                     │    │                     │  │
│  │ ┌─────────────────┐ │    │ ┌─────────────────┐ │    │ ┌─────────────────┐ │  │
│  │ │ Internal Gateway│ │    │ │ Internal Gateway│ │    │ │ Internal Gateway│ │  │
│  │ │   (Pod)         │ │    │ │   (Pod)         │ │    │ │   (Pod)         │ │  │
│  │ └─────────────────┘ │    │ └─────────────────┘ │    │ └─────────────────┘ │  │
│  │ ┌─────────────────┐ │    │ ┌─────────────────┐ │    │ ┌─────────────────┐ │  │
│  │ │ Manager         │ │    │ │ Manager         │ │    │ │ Manager         │ │  │
│  │ │   (Operator)    │ │    │ │   (Operator)    │ │    │ │   (Operator)    │  │
│  │ └─────────────────┘ │    │ └─────────────────┘ │    │ └─────────────────┘ │  │
│  │ ┌─────────────────┐ │    │ ┌─────────────────┐ │    │ ┌─────────────────┐ │  │
│  │ │ Netd (DS)       │ │    │ │ Netd (DS)       │ │    │ │ Netd (DS)       │ │  │
│  │ │   (每个节点)     │ │    │ │   (每个节点)     │ │    │ │   (每个节点)     │  │
│  │ └─────────────────┘ │    │ └─────────────────┘ │    │ └─────────────────┘ │  │
│  │ ┌─────────────────┐ │    │ ┌─────────────────┐ │    │ ┌─────────────────┐ │  │
│  │ │ Storage Proxy   │ │    │ │ Storage Proxy   │ │    │ │ Storage Proxy   │ │  │
│  │ │   (StatefulSet) │ │    │ │   (StatefulSet) │ │    │ │   (StatefulSet) │ │  │
│  │ └─────────────────┘ │    │ └─────────────────┘ │    │ └─────────────────┘ │  │
│  │ ┌─────────────────┐ │    │ ┌─────────────────┐ │    │ ┌─────────────────┐ │  │
│  │ │ Sandbox Pods    │ │    │ │ Sandbox Pods    │ │    │ │ Sandbox Pods    │ │  │
│  │ │   (N 个)        │ │    │ │   (N 个)        │ │    │ │   (N 个)        │ │  │
│  │ └─────────────────┘ │    │ └─────────────────┘ │    │ └─────────────────┘ │  │
│  └─────────────────────┘    └─────────────────────┘    └─────────────────────┘  │
│                                                                                 │
└─────────────────────────────────────────────────────────────────────────────────┘
```

## 架构分层说明

### 1. Global Service（全局服务层）

**职责**：
- 全局唯一的控制平面
- 管理跨 Region 的元数据
- 处理账单、计量、配额
- 用户身份管理（SSO、API Key）
- Global Router：根据用户/沙箱 ID 路由到正确的 Region

**部署方式**：
- 单 Region 高可用部署（建议 us-east-1）
- 独立数据库存储全局配置
- 与 Region 控制平面通过 HTTPS 通信

**关键服务**：
| 服务 | 功能 |
|-----|------|
| Billing Service | 跨 Region 计量聚合、账单生成 |
| IAM Service | 用户/团队管理、权限控制 |
| Config Service | 全局配置、Feature Flag |
| Global Router | Region 路由决策 |

### 2. Region（区域层）

**Region ID 规范**：
```
格式: <cloud-provider>/<region-code>

示例:
- aws/us-east-1       (AWS 美国东部-1)
- aws/eu-west-1       (AWS 欧洲西部-1)
- gcp/us-east-1       (GCP 美国东部-1)
- gcp/europe-west1    (GCP 欧洲西部-1)
- azure/eastus        (Azure 美国东部)
- azure/westeurope    (Azure 欧洲西部)
```

**每个 Region 包含**：

| 组件 | 职责 | 部署方式 |
|-----|------|---------|
| Edge Gateway | Region 统一入口，代理内部服务 | Deployment + LB |
| PostgreSQL | JuiceFS 元数据、SandboxVolume 数据 | Managed DB (RDS) |
| S3/GCS/Azure Blob | JuiceFS 数据块存储 | Object Storage |

**Region 隔离特性**：
- 每个 Region 有独立的 S3/GCS 和 PostgreSQL
- 同一 Region 内的所有 Cluster 共享存储层
- 数据驻留合规（数据不出 Region）
- Region 间无直接依赖，可独立运行

### 3. Cluster（数据平面集群层）

**每个 Cluster 包含**：

| 组件 | 职责 | 部署方式 |
|-----|------|---------|
| Internal Gateway | 集群内部统一入口、认证 | Deployment |
| Manager | K8s Operator、沙箱生命周期管理 | Deployment |
| Netd | 网络数据平面、流量控制 | DaemonSet |
| Storage Proxy | JuiceFS gRPC 服务、文件操作 | StatefulSet |
| Procd | 沙箱 PID=1 进程 | Pod |
| Sandbox Pods | 用户沙箱实例 | Pod |

**水平扩展方式**：
- 向 Region 添加新 Cluster → 自动连接到控制平面
- Cluster 内添加 Node → 增加沙箱容量
- Manager 自动管理 Cluster 内的沙箱调度

## 部署模式

### 模式 1：单 Region 多集群（推荐）

```
Global Service
    │
    ▼
Region: aws/us-east-1
    │
    ├─── Cluster 1 (EKS) ────────┐
    ├─── Cluster 2 (EKS) ────────┤
    ├─── Cluster 3 (EKS) ────────┤── 共享控制平面
    ├─── Cluster 4 (EKS) ────────┤   (S3 + RDS)
    ├─── Cluster 5 (EKS) ────────┤
    └─── Cluster N (EKS) ────────┘
```

**适用场景**：
- 标准生产环境
- 用户集中在单一地区
- 支持灵活的 VPC 规划

**容量**：
- Cluster 数量：无硬性限制
- 单 Cluster IP 容量：~65,500 (/16 子网）
- 扩展方式：添加新 Cluster 即可

### 模式 2：多 Region（地理分布）

```
Global Service
    │
    ├─── Region: aws/us-east-1 ─── Clusters 1-N
    ├─── Region: aws/eu-west-1 ─── Clusters 1-N
    ├─── Region: gcp/europe-west1 ─ Clusters 1-N
    └─── Region: azure/eastus ──── Clusters 1-N
```

**适用场景**：
- 全球用户分布
- 数据驻留合规要求
- 低延迟访问需求

**数据策略**：
- 每租户固定到一个 Region
- 跨 Region 数据复制（可选，用于灾备）
- 无跨 Region 实时流量

### 模式 3：多云多实例（独立部署）

**注意**：这不是跨云通信架构，而是多个独立实例

```
实例 A（AWS）              实例 B（GCP）
┌─────────────────┐       ┌─────────────────┐
│ Global Service  │       │ Global Service  │
│      │          │       │      │          │
│   aws/us-east-1 │       │ gcp/us-east1   │
│      │          │       │      │          │
│  Clusters 1-N   │       │  Clusters 1-N   │
└─────────────────┘       └─────────────────┘
```

**适用场景**：
- 有云平台偏好
- 政府合同要求特定云
- 企业谈判筹码

**特点**：
- 完全独立的实例
- 不共享数据
- 租户自行选择实例

## 网络架构

### VPC 网络架构

Cluster 通过 **Private Link** 直接访问存储服务（S3、PostgreSQL），无需经过控制平面 VPC。

```
                        Region: aws/us-east-1
│
├─── 控制平面 VPC: 10.0.0.0/16
│    │
│    ├─── Edge Gateway (LB) ◄────┐
│    │                          │
│    └─── RDS PostgreSQL ─────────┼── Private Link
│                                  │
├─── 客户 VPC A: 任意 CIDR ◄───────┤
│    │                           │
│    ├─── Cluster 1 ──────────────┤── Private Link ──┐
│    │                                             │
│    └─── Cluster 2 ───────────────────────────────┤
│                                                   │
├─── 客户 VPC B: 任意 CIDR ◄────────────────────────┤
│    │                                             │
│    └─── Cluster N ───────────────────────────────┘
│
└─── S3 (全局服务，通过 Gateway Interface Endpoint 访问)
```

### 网络连接

| 连接目标 | 连接方式 | 说明 |
|---------|---------|------|
| Edge Gateway | 公网/Private Link | API 请求路由 |
| PostgreSQL | **Private Link** | RDS Proxy |
| S3 | **Gateway Interface Endpoint** | Private Link for S3 |

**优势**：
- 客户 VPC CIDR 完全独立，无限制
- 存储流量不经过控制平面 VPC，降低带宽压力
- 满足企业合规要求（独立网络边界）

## 部署流程

### 初始化新 Region

1. **创建基础设施**
   ```bash
   # 创建控制平面 VPC、子网
   # 部署 RDS PostgreSQL（启用 Private Link）
   # 创建 S3 Bucket
   # 配置 S3 Gateway Interface Endpoint
   # 创建 Edge Gateway（公网 LB 或 Private Link）
   ```

2. **部署控制平面**
   ```bash
   # 部署 Edge Gateway
   # 初始化 JuiceFS (连接 RDS + S3)
   # 运行数据库迁移
   ```

3. **在 Global Service 注册 Region**
   ```bash
   POST /api/v1/regions
   {
     "region_id": "aws/us-east-1",
     "edge_gateway_url": "https://edge.us-east-1.sandbox0.ai",
     "storage_config": { ... }
   }
   ```

### 添加 Cluster 到 Region

1. **在客户 VPC 创建 Private Link**
   ```bash
   # 创建 S3 Gateway Interface Endpoint
   aws ec2 create-vpc-endpoint \
     --vpc-id $VPC_ID \
     --service-name com.amazonaws.us-east-1.s3 \
     --vpc-endpoint-type GatewayLoadBalancer

   # 创建 RDS Proxy / Private Link
   aws rds create-db-proxy \
     --db-proxy-name sandbox0-proxy \
     --engine-family POSTGRESQL \
     --vpc-subnet-ids $SUBNET_IDS

   # 或创建 RDS Instance Private Link endpoint
   aws ec2 create-vpc-endpoint \
     --vpc-id $VPC_ID \
     --service-name com.amazonaws.us-east-1.rds \
     --vpc-endpoint-type Interface
   ```

2. **部署 Cluster 组件**
   ```bash
   helm install sandbox0-cluster ./chart/ \
     --set region=aws/us-east-1 \
     --set cluster-id=cluster-3 \
     --set edge-gateway.url=https://edge.us-east-1.sandbox0.ai \
     --set storage.s3.bucket=juicefs-us-east-1 \
     --set storage.s3.endpoint=$S3_PRIVATELINK_ENDPOINT \
     --set storage.pg.host=$RDS_PROXY_ENDPOINT
   ```

3. **在 Region 控制平面注册 Cluster**
   ```bash
   POST /api/v1/clusters
   {
     "cluster_id": "cluster-3",
     "region_id": "aws/us-east-1",
     "vpc_id": "vpc-xxxxx",
     "api_endpoint": "https://cluster-3.internal.sandbox0.ai"
   }
   ```

## 配置示例

### Cluster 配置 (values.yaml)

```yaml
# Region 标识
region: aws/us-east-1
clusterId: cluster-1

# VPC 配置
vpc:
  id: vpc-xxxxx  # 客户 VPC ID（任意 CIDR）
  subnetCidr: 10.1.0.0/16

# Edge Gateway 配置
edgeGateway:
  url: https://edge.us-east-1.sandbox0.ai
  authToken: ${EDGE_TOKEN}

# Storage 配置（通过 Private Link）
storage:
  juicefs:
    meta:
      type: postgres
      dsn: host=${RDS_PROXY_ENDPOINT} port=5432 dbname=juicefs
      # RDS Proxy 或 RDS Instance Private Link endpoint
    storage:
      type: s3
      endpoint: ${S3_ENDPOINT}  # Gateway Interface Endpoint URL
      bucket: juicefs-us-east-1
      region: us-east-1
      accessKey: ${S3_ACCESS_KEY}
      secretKey: ${S3_SECRET_KEY}

# Manager 配置
manager:
  defaultClusterId: cluster-1
  replicaCount: 2

# Storage Proxy 配置
storageProxy:
  replicaCount: 3
  resources:
    requests:
      cpu: 500m
      memory: 1Gi
    limits:
      cpu: 2000m
      memory: 4Gi
```

### Private Link Endpoint 配置

```yaml
# S3 Gateway Interface Endpoint
s3:
  endpointType: gateway
  serviceName: com.amazonaws.us-east-1.s3
  routeTableIds:
    - rtb-xxxxx

# RDS Private Link
rds:
  endpointType: interface
  # Option 1: 使用 RDS Proxy（推荐）
  proxyEndpoint: sandbox0-proxy.proxy-xxxxx.us-east-1.rds.amazonaws.com
  # Option 2: 直接使用 RDS Instance Private Link
  instanceEndpoint: juicerds.xxxx.us-east-1.rds.amazonaws.com
```

## 成本优化建议

| 优化项 | 方案 | 成本 |
|-------|------|------|
| S3 访问 | Gateway Interface Endpoint | $0.01/GB（比公网省 $0.08/GB）|
| PostgreSQL | RDS Proxy + Private Link | ~$0.015/小时 |
| 跨 VPC 连接 | 无需 Peering/Transit Gateway | $0（存储流量直接走 Private Link）|
| 数据库 | 使用 RDS Provisioned IOPS | 按需 |
| 存储缓存 | 增加 Storage Proxy 本地缓存 | 减少 S3 请求 |

**Private Link 成本估算**（单 Cluster）：

| 项目 | 成本 |
|-----|------|
| S3 Gateway Endpoint | $0.01/GB 流量 |
| RDS Proxy | ~$0.015/小时 ≈ $11/月 |
| VPC Endpoint (Interface) | ~$0.01/小时/端 |

**优势**：
- 无需 VPC Peering，节省跨 VPC 流量费用
- 客户 VPC 完全独立，无 CIDR 限制
- 存储流量不经过控制平面 VPC

## 容量规划

### 单 Region 容量

| 类型 | Cluster 数限制 | 单 Cluster IP 容量 | 总 IP 容量 |
|------|---------------|-------------------|-----------|
| 跨 VPC 部署 | 无限制 | ~65,500 (/16) | 无限制 |

### 扩展路径

```
阶段 1: 初始部署
├── 控制平面 VPC (Edge Gateway, RDS, S3)
└── 客户 VPC A + Cluster 1-5

阶段 2: 水平扩展
├── 控制平面 VPC
├── 客户 VPC A + Cluster 1-10 (扩容)
├── 客户 VPC B + Cluster 1-5 (新增客户)
└── 客户 VPC C + Cluster 1-3 (新增客户)

优势:
- 无 CIDR 冲突问题（Private Link）
- 无需 VPC Peering（降低复杂度）
- 客户完全自治
```

## 监控与可观测性

### Region 级指标

- 活跃 Cluster 数量
- 总沙箱容量
- S3 存储使用量
- 数据库连接数
- Private Link 端点数量

### Cluster 级指标

- 沙箱创建成功率
- 平均冷启动时间
- Storage Proxy 缓存命中率
- Private Link 流量统计
- Private Link 端点健康状态

### 全局指标

- 跨 Region 请求分布
- 各 Region 错误率
- 全局资源利用率
- 租户账单统计

## 故障隔离

- **Cluster 故障**：不影响其他 Cluster，沙箱在其他 Cluster 创建
- **VPC 故障**：不影响其他 VPC 中的 Cluster
- **Region 故障**：Global Router 将流量切换到备用 Region
- **控制平面故障**：多副本 Edge Gateway，自动故障转移
- **存储故障**：S3 和 RDS 多可用区部署，自动故障转移
- **Private Link 故障**：
  - S3 Gateway Endpoint 高可用设计（多 AZ）
  - RDS Proxy 自动故障转移
  - 可回退到公网访问（需安全组配置）
