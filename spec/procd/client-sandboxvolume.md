# Procd - SandboxVolume Client (RemoteFS Client)

## 一、设计目标

Procd 的 SandboxVolumeManager 负责将远程文件系统（SandboxVolume）挂载到 Pod 内的指定路径。

**注意**：SandboxVolume 是 sandbox0 的持久化存储概念，用于区别 k8s 原生的 Volume/PVC。

### 核心原则

1. **零存储凭证**：Procd 不持有任何 S3、PostgreSQL 凭证
2. **轻量级**：只负责 FUSE 挂载和 gRPC 客户端
3. **网络隔离兼容**：通过 packet marking 绕过用户网络规则
4. **快速挂载**：<50ms 挂载延迟

---

## 二、架构设计

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    Procd SandboxVolume Architecture                           │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  Storage Proxy                                                               │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │ SandboxVolume Management                                             │   │
│  │  - Create/Delete SandboxVolume                                       │   │
│  │  - Attach/Detach to Sandbox                                          │   │
│  │  - Snapshot/Restore                                                  │   │
│  │  - Calls Procd to mount/unmount                                      │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                    │                                         │
│                                    ▼                                         │
│  Procd (PID=1, in Pod)                                                       │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │ SandboxVolumeManager                                                  │   │
│  │ ┌───────────────────────────────────────────────────────────────┐   │   │
│  │ │ Mount/Unmount API (HTTP)                                       │   │   │
│  │ │  - POST /api/v1/sandboxvolumes/mount                           │   │   │
│  │ │  - POST /api/v1/sandboxvolumes/unmount                         │   │   │
│  │ └───────────────────────────────────────────────────────────────┘   │   │
│  │                           │                                            │   │
│  │                           ▼                                            │   │
│  │ ┌───────────────────────────────────────────────────────────────┐   │   │
│  │ │ RemoteFS (FUSE filesystem)                                     │   │   │
│  │ │  ├── Implements fuse.Filesystem interface                     │   │   │
│  │ │  ├── Forwards all operations to gRPC client                   │   │   │
│  │ │  └─→ gRPC call to Storage Proxy                               │   │   │
│  │ └───────────────────────────────────────────────────────────────┘   │   │
│  │                           │                                            │   │
│  │                           ▼                                            │   │
│  │ ┌───────────────────────────────────────────────────────────────┐   │   │
│  │ │ gRPC Client                                                    │   │   │
│  │ │  ├── Connection to Storage Proxy                              │   │   │
│  │ │  ├── Packet marking (SO_MARK=0x2)                             │   │   │
│  │ │  └─→ Bypass nftables rules                                     │   │   │
│  │ └───────────────────────────────────────────────────────────────┘   │   │
│  │                                                                        │   │
│  │ /workspace (FUSE mount point)                                         │   │
│  │ └─→ User files accessed here                                         │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                    │ gRPC (mark=0x2)                        │
│                                    ▼                                        │
│                          Storage Proxy (JuiceFS Backend)                   │
│                          (Has all credentials)                            │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 三、数据结构定义

### 3.1 SandboxVolumeManager

```go
// SandboxVolumeManager manages remote filesystem mounts in Procd
type SandboxVolumeManager struct {
    mu     sync.RWMutex
    mounts map[string]*MountContext  // sandboxvolumeID -> MountContext

    // Configuration
    proxyBaseURL    string  // Storage Proxy base URL
    proxyReplicas   int     // Number of Storage Proxy replicas
    nodeName        string  // Current Pod's node name (for affinity routing)

    // Cache configuration
    cacheMaxBytes   int64   // Local cache size limit
    cacheTTL        time.Duration // Cache TTL
}

// MountContext represents an active mount
type MountContext struct {
    SandboxVolumeID   string
    MountPoint        string
    Token             string  // JWT auth token from Storage Proxy (in-memory only)

    // FUSE
    fuseConn           *fuse.Conn
    fuseServerCancel   context.CancelFunc

    // gRPC client
    grpcClient         fs.FileSystemClient

    MountedAt          time.Time
}
```

### 3.2 Mount Request/Response

```go
// MountRequest request to mount a sandboxvolume
type MountRequest struct {
    SandboxVolumeID string `json:"sandboxvolume_id"`  // SandboxVolume ID
    SandboxID       string `json:"sandbox_id"`
    MountPoint      string `json:"mount_point"`       // e.g., "/workspace"
    Token           string `json:"token"`             // JWT auth token from Storage Proxy
}

// MountResponse response for mount request
type MountResponse struct {
    SandboxVolumeID string `json:"sandboxvolume_id"`
    MountPoint      string `json:"mount_point"`
    MountedAt       string `json:"mounted_at"`  // ISO timestamp
}

// UnmountRequest request to unmount a sandboxvolume
type UnmountRequest struct {
    SandboxVolumeID string `json:"sandboxvolume_id"`
}
```

---

## 四、RemoteFS 实现

### 4.1 RemoteFS 结构

```go
// RemoteFS implements fuse.Filesystem via gRPC client
type RemoteFS struct {
    client           fs.FileSystemClient  // gRPC client
    sandboxvolumeID  string
    token            string
    rootInode        string

    // Local read cache (see Section 10 for details)
    cache            *CacheStore          // LRU cache for read operations
    cacheMaxBytes    int64                // Cache size limit (default: 100MB)
    cacheTTL         time.Duration        // Cache TTL (default: 30s)
}

// CacheStore local read cache for performance optimization
type CacheStore struct {
    mu    sync.RWMutex
    entries map[string]*CacheEntry        // inode -> CacheEntry
    maxBytes int64
    currentBytes int64
}

// CacheEntry represents a cached file data or metadata
type CacheEntry struct {
    inode    string
    data     []byte                      // Cached file data (nil for metadata-only entries)
    attr     *fs.GetAttrResponse         // Cached attributes
    expireAt time.Time                   // TTL-based expiration
}

// RemoteFSNode represents a file/directory node
type RemoteFSNode struct {
    inode string
    fs    *RemoteFS
    attr  *fs.GetAttrResponse
}

// RemoteFileHandle represents an open file
type RemoteFileHandle struct {
    inode string
    fs    *RemoteFS
    id    uint64  // Handle ID
}
```

### 4.2 FUSE Operations (gRPC Client)

```go
// Attr implements fs.Node
func (n *RemoteFSNode) Attr(ctx context.Context, a *fuse.Attr) error {
    req := &fs.GetAttrRequest{Inode: n.inode}
    resp, err := n.fs.client.GetAttr(withAuth(ctx, n.fs.token), req)
    if err != nil {
        return err
    }

    a.Inode = resp.Ino
    a.Mode = syscallMode(resp.Mode)
    a.Size = resp.Size
    a.Mtime = time.Unix(resp.MtimeSec, resp.MtimeNsec)
    a.Atime = time.Unix(resp.AtimeSec, resp.AtimeNsec)
    a.Ctime = time.Unix(resp.CtimeSec, resp.CtimeNsec)
    a.Nlink = resp.Nlink
    a.Uid = resp.Uid
    a.Gid = resp.Gid
    a.Rdev = resp.Rdev
    a.Blocks = resp.Blocks

    return nil
}

// Create implements fs.NodeCreater
func (n *RemoteFSNode) Create(ctx context.Context, req *fuse.CreateRequest, resp *fuse.CreateResponse) (fs.Node, fs.Handle, error) {
    creq := &fs.CreateRequest{
        Parent: n.inode,
        Name:   req.Name,
        Mode:   uint32(req.Mode),
        Flags:  uint32(req.Flags),
    }

    cresp, err := n.fs.client.Create(withAuth(ctx, n.fs.token), creq)
    if err != nil {
        return nil, nil, err
    }

    node := &RemoteFSNode{
        inode: cresp.Inode,
        fs:    n.fs,
        attr:  cresp.Attr,
    }

    handle := &RemoteFileHandle{
        inode: cresp.Inode,
        fs:    n.fs,
        id:    cresp.HandleId,
    }

    resp.Attr = fuseAttrFrom(cresp.Attr)

    return node, handle, nil
}

// Lookup implements fs.NodeRequestLookuper
func (n *RemoteFSNode) Lookup(ctx context.Context, req *fuse.LookupRequest, resp *fuse.LookupResponse) (fs.Node, error) {
    lreq := &fs.LookupRequest{
        Parent: n.inode,
        Name:   req.Name,
    }

    lresp, err := n.fs.client.Lookup(withAuth(ctx, n.fs.token), lreq)
    if err != nil {
        return nil, err
    }

    node := &RemoteFSNode{
        inode: lresp.Inode,
        fs:    n.fs,
        attr:  lresp.Attr,
    }

    resp.Attr = fuseAttrFrom(lresp.Attr)
    resp.EntryValid = time.Hour

    return node, nil
}

// Read implements fs.HandleReader
func (h *RemoteFileHandle) Read(ctx context.Context, req *fuse.ReadRequest, resp *fuse.ReadResponse) error {
    rreq := &fs.ReadRequest{
        Inode:    h.inode,
        HandleId: h.id,
        Offset:   req.Offset,
        Size:     int64(len(resp.Data)),
    }

    rresp, err := h.fs.client.Read(withAuth(ctx, h.fs.token), rreq)
    if err != nil {
        return err
    }

    n := copy(resp.Data, rresp.Data)
    resp.Data = resp.Data[:n]

    return nil
}

// Write implements fs.HandleWriter
func (h *RemoteFileHandle) Write(ctx context.Context, req *fuse.WriteRequest, resp *fuse.WriteResponse) error {
    wreq := &fs.WriteRequest{
        Inode:    h.inode,
        HandleId: h.id,
        Offset:   req.Offset,
        Data:     req.Data,
    }

    wresp, err := h.fs.client.Write(withAuth(ctx, h.fs.token), wreq)
    if err != nil {
        return err
    }

    resp.Size = int(wresp.BytesWritten)
    return nil
}

// Mkdir implements fs.NodeMkdirer
func (n *RemoteFSNode) Mkdir(ctx context.Context, req *fuse.MkdirRequest) (fs.Node, error) {
    mreq := &fs.MkdirRequest{
        Parent: n.inode,
        Name:   req.Name,
        Mode:   uint32(req.Mode),
    }

    mresp, err := n.fs.client.Mkdir(withAuth(ctx, n.fs.token), mreq)
    if err != nil {
        return nil, err
    }

    return &RemoteFSNode{
        inode: mresp.Inode,
        fs:    n.fs,
        attr:  mresp.Attr,
    }, nil
}

// Readdir implements fs.NodeReaddirer
func (n *RemoteFSNode) Readdir(ctx context.Context) (fuse.Dirent, error) {
    req := &fs.ReadDirRequest{
        Inode: n.inode,
    }

    resp, err := n.fs.client.ReadDir(withAuth(ctx, n.fs.token), req)
    if err != nil {
        return nil, err
    }

    var entries []fuse.Dirent
    for _, e := range resp.Entries {
        entries = append(entries, fuse.Dirent{
            Inode: e.Inode,
            Type:  fuse.DT_File,
            Name:  e.Name,
        })
    }

    return entries, nil
}

// Unlink implements fs.NodeRemover
func (n *RemoteFSNode) Unlink(ctx context.Context, req *fuse.RemoveRequest) error {
    ureq := &fs.UnlinkRequest{
        Parent: n.inode,
        Name:   req.Name,
    }

    _, err := n.fs.client.Unlink(withAuth(ctx, n.fs.token), ureq)
    return err
}

// Rename implements fs.NodeRenamer
func (n *RemoteFSNode) Rename(ctx context.Context, req *fuse.RenameRequest, newDir fs.Node) error {
    newParent := newDir.(*RemoteFSNode).inode

    rreq := &fs.RenameRequest{
        OldParent: n.inode,
        OldName:   req.OldName,
        NewParent: newParent,
        NewName:   req.NewName,
    }

    _, err := n.fs.client.Rename(withAuth(ctx, n.fs.token), rreq)
    return err
}

// Flush implements fs.NodeFlusher
func (h *RemoteFileHandle) Flush(ctx context.Context, req *fuse.FlushRequest) error {
    freq := &fs.FlushRequest{
        HandleId: h.id,
    }

    _, err := h.fs.client.Flush(withAuth(ctx, h.fs.token), freq)
    return err
}

// Fsync implements fs.NodeFsyncer
func (h *RemoteFileHandle) Fsync(ctx context.Context, req *fuse.FsyncRequest) error {
    freq := &fs.FsyncRequest{
        HandleId: h.id,
        Datasync: req.Fdatasync,
    }

    _, err := h.fs.client.Fsync(withAuth(ctx, h.fs.token), freq)
    return err
}
```

### 4.3 Helper Functions

```go
// withAuth adds Bearer token to context
func withAuth(ctx context.Context, token string) context.Context {
    md := metadata.Pairs("authorization", "Bearer "+token)
    return metadata.NewOutgoingContext(ctx, md)
}

// syscallMode converts protobuf mode to syscall mode
func syscallMode(mode uint32) uint32 {
    return mode & 0777
}

// fuseAttrFrom converts protobuf attr to fuse.Attr
func fuseAttrFrom(attr *fs.GetAttrResponse) fuse.Attr {
    return fuse.Attr{
        Inode:       attr.Ino,
        Mode:       syscallMode(attr.Mode),
        Nlink:      attr.Nlink,
        Uid:        attr.Uid,
        Gid:        attr.Gid,
        Rdev:       attr.Rdev,
        Size:       attr.Size,
        Blocks:     attr.Blocks,
        Mtime:      time.Unix(attr.MtimeSec, attr.MtimeNsec),
        Atime:      time.Unix(attr.AtimeSec, attr.AtimeNsec),
        Ctime:      time.Unix(attr.CtimeSec, attr.CtimeNsec),
    }
}
```

---

## 五、SandboxVolumeManager 实现

### 5.1 Mount SandboxVolume

```go
// Mount mounts a remote filesystem
func (vm *SandboxVolumeManager) Mount(ctx context.Context, req *MountRequest) (*MountResponse, error) {
    vm.mu.Lock()
    defer vm.mu.Unlock()

    // Check if already mounted
    if _, exists := vm.mounts[req.SandboxVolumeID]; exists {
        return nil, fmt.Errorf("sandboxvolume %s already mounted", req.SandboxVolumeID)
    }

    // Create gRPC connection with packet marking
    conn, err := vm.createGRPCConnection()
    if err != nil {
        return nil, fmt.Errorf("create grpc connection: %w", err)
    }

    client := fs.NewFileSystemClient(conn)

    // Create RemoteFS
    remoteFS := &RemoteFS{
        client:           client,
        sandboxvolumeID:  req.SandboxVolumeID,
        token:            req.Token,
        rootInode:        "1",  // Root inode is always "1"
    }

    // Ensure mount point directory exists
    if err := os.MkdirAll(req.MountPoint, 0755); err != nil {
        return nil, fmt.Errorf("create mount point: %w", err)
    }

    // Mount FUSE
    fuseConn, err := fuse.Mount(req.MountPoint,
        fuse.FSName("sandbox0"),
        fuse.Subtype("remote"),
        fuse.LocalVolume(),
        fuse.AllowOther(),
    )
    if err != nil {
        return nil, fmt.Errorf("fuse mount: %w", err)
    }

    // Create context for FUSE server
    serverCtx, cancel := context.WithCancel(context.Background())

    // Serve FUSE in goroutine
    go func() {
        defer cancel()
        if err := fs.Serve(fuseConn, remoteFS); err != nil {
            log.Printf("FUSE serve error: %v", err)
        }
    }()

    // Wait for mount to be ready
    select {
    case <-fuseConn.Ready:
        if err := fuseConn.MountError; err != nil {
            fuseConn.Close()
            return nil, fmt.Errorf("mount ready: %w", err)
        }
    case <-time.After(10 * time.Second):
        fuseConn.Close()
        return nil, fmt.Errorf("mount timeout")
    }

    // Store mount context
    vm.mounts[req.SandboxVolumeID] = &MountContext{
        SandboxVolumeID:  req.SandboxVolumeID,
        MountPoint:        req.MountPoint,
        Token:             req.Token,
        FuseConn:          fuseConn,
        FuseServerCancel:  cancel,
        GrpcClient:        client,
        MountedAt:         time.Now(),
    }

    log.Printf("Mounted sandboxvolume %s at %s", req.SandboxVolumeID, req.MountPoint)

    return &MountResponse{
        SandboxVolumeID: req.SandboxVolumeID,
        MountPoint:      req.MountPoint,
        MountedAt:       time.Now().Format(time.RFC3339),
    }, nil
}
```

### 5.2 Create gRPC Connection with Packet Marking

```go
// getStorageProxyAddress returns the preferred Storage Proxy address using node affinity
func (vm *SandboxVolumeManager) getStorageProxyAddress() string {
    // Hash-based routing: same node -> same replica
    hash := fnv.New32()
    hash.Write([]byte(vm.nodeName))
    replicaIndex := hash.Sum32() % uint32(vm.proxyReplicas)

    return fmt.Sprintf("storage-proxy-%d.%s:8080", replicaIndex, vm.proxyBaseURL)
}

// createGRPCConnection creates gRPC connection with packet marking and node affinity
func (vm *SandboxVolumeManager) createGRPCConnection() (*grpc.ClientConn, error) {
    // Get preferred Storage Proxy address (node affinity routing)
    proxyAddr := vm.getStorageProxyAddress()

    // Custom dialer that sets SO_MARK socket option
    dialer := &net.Dialer{
        Control: func(network, address string, c syscall.RawConn) error {
            var opErr error
            err := c.Control(func(fd uintptr) {
                // Set SO_MARK = 0x2 to bypass nftables rules
                opErr = syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, 0x24, 0x2)
            })
            if err != nil {
                return err
            }
            return opErr
        },
    }

    // Create gRPC connection
    return grpc.Dial(proxyAddr,
        grpc.WithTransportCredentials(insecure.NewCredentials()),
        grpc.WithContextDialer(func(ctx context.Context, addr string) (net.Conn, error) {
            return dialer.DialContext(ctx, "tcp", addr)
        }),
        grpc.WithDefaultCallOptions(
            grpc.MaxCallRecvMsgSize(100*1024*1024),  // 100MB max message size
        ),
    )
}
```

### 5.3 Unmount SandboxVolume

```go
// Unmount unmounts a sandboxvolume
func (vm *SandboxVolumeManager) Unmount(ctx context.Context, sandboxvolumeID string) error {
    vm.mu.Lock()
    defer vm.mu.Unlock()

    mountCtx, exists := vm.mounts[sandboxvolumeID]
    if !exists {
        return fmt.Errorf("sandboxvolume %s not mounted", sandboxvolumeID)
    }

    // Cancel FUSE server
    mountCtx.FuseServerCancel()

    // Close FUSE connection
    if err := mountCtx.FuseConn.Close(); err != nil {
        log.Printf("Warning: close fuse conn: %v", err)
    }

    // Unmount filesystem
    if err := syscall.Unmount(mountCtx.MountPoint, 0); err != nil {
        return fmt.Errorf("unmount: %w", err)
    }

    // Close gRPC connection
    if closer, ok := mountCtx.GrpcClient.(interface{ Close() error }); ok {
        closer.Close()
    }

    delete(vm.mounts, sandboxvolumeID)

    log.Printf("Unmounted sandboxvolume %s", sandboxvolumeID)

    return nil
}
```

---

## 六、HTTP API

### 6.1 Mount SandboxVolume

```http
POST /api/v1/sandboxvolumes/mount
Content-Type: application/json

{
    "sandboxvolume_id": "sbv-abc123",
    "sandbox_id": "sb-def456",
    "mount_point": "/workspace",
    "token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9..."
}

Response: 200 OK
{
    "sandboxvolume_id": "sbv-abc123",
    "mount_point": "/workspace",
    "mounted_at": "2024-01-01T00:00:00Z"
}

Error Response: 409 Conflict
{
    "error": "sandboxvolume_sbv-abc123_already_mounted"
}
```

### 6.2 Unmount SandboxVolume

```http
POST /api/v1/sandboxvolumes/unmount
Content-Type: application/json

{
    "sandboxvolume_id": "sbv-abc123"
}

Response: 200 OK
{}

Error Response: 404 Not Found
{
    "error": "sandboxvolume_not_mounted"
}
```

### 6.3 Get Mount Status

```http
GET /api/v1/sandboxvolumes/status

Response: 200 OK
{
    "mounts": [
        {
            "sandboxvolume_id": "sbv-abc123",
            "mount_point": "/workspace",
            "mounted_at": "2024-01-01T00:00:00Z",
            "mounted_duration_sec": 3600
        }
    ]
}
```

---

## 七、与 Storage Proxy 的交互

```
┌─────────────────────────────────────────────────────────────────────────────┐
│              Internal Gateway Coordinated Mount Flow                          │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  1. User requests to attach SandboxVolume to Sandbox                        │
│     POST /api/v1/sandboxvolumes/{id}/attach                                 │
│     {                                                                       │
│       "sandbox_id": "sb-123",                                               │
│       "mount_point": "/workspace"                                           │
│     }                                                                       │
│                                                                              │
│  2. Internal Gateway calls Storage Proxy to prepare mount                   │
│     POST http://storage-proxy:8081/api/v1/sandboxvolumes/{id}/attach        │
│     {                                                                       │
│       "sandbox_id": "sb-123",                                               │
│       "mount_point": "/workspace"                                           │
│     }                                                                       │
│     → Returns token for gRPC authentication                                │
│                                                                              │
│  3. Internal Gateway calls Procd API to mount sandboxvolume                 │
│     POST http://procd-{pod-id}:8080/api/v1/sandboxvolumes/mount              │
│     {                                                                       │
│       "sandboxvolume_id": "sbv-456",                                        │
│       "sandbox_id": "sb-123",                                               │
│       "mount_point": "/workspace",                                          │
│       "token": "eyJhbGc...",                                                │
│       "storage_proxy_address": "storage-proxy:8080"                         │
│     }                                                                       │
│                                                                              │
│  4. Procd SandboxVolumeManager mounts RemoteFS                              │
│     ├─ Create gRPC connection (with SO_MARK=0x2)                            │
│     ├─ Mount FUSE at /workspace                                             │
│     ├─ Start FUSE server (forwards to gRPC)                                 │
│     └─ Return success                                                       │
│                                                                              │
│  5. User can now access files in /workspace                                 │
│     All file operations: User → FUSE → gRPC → Storage Proxy → S3/PG          │
│                                                                              │
│  6. On sandbox deletion or volume detach                                    │
│     Internal Gateway calls:                                                 │
│     a) Storage Proxy to detach: POST /api/v1/sandboxvolumes/{id}/detach     │
│     b) Procd to unmount: POST /api/v1/sandboxvolumes/unmount                │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

**Architecture Benefits:**
- **No circular dependency**: Storage Proxy and Procd are independent
- **Internal Gateway as coordinator**: Orchestrates the mount/attach flow
- **Clear separation**: Storage Proxy manages metadata, Procd manages FUSE mount

---

## 八、网络配置

### 8.1 nftables Configuration

```bash
# nftables rules in Procd (applied on startup)
table inet sb0-firewall {
    chain SANDBOX0_OUTPUT {
        # Proxy bypass (highest priority)
        meta mark & 0x2 == 0x2 accept

        # Private IP blacklist (for user traffic)
        ip daddr @predef_deny drop

        # User deny list
        ip daddr @user_deny drop

        # Whitelist mode: redirect TCP to proxy
        meta l4proto tcp tcp dport != 8080 redirect to 127.0.0.1:1080
    }
}
```

### 8.2 Environment Variables

```yaml
# Procd container environment
env:
  # Node name for affinity routing (from downward API)
  - name: NODE_NAME
    valueFrom:
      fieldRef:
        fieldPath: spec.nodeName

  # Storage Proxy base configuration
  - name: STORAGE_PROXY_BASE_URL
    value: "storage-proxy.storage-proxy.svc.cluster.local"

  - name: STORAGE_PROXY_REPLICAS
    value: "3"  # Number of StatefulSet replicas

  # Local cache configuration
  - name: CACHE_MAX_BYTES
    value: "104857600"  # 100MB

  - name: CACHE_TTL_SECONDS
    value: "30"  # 30 seconds
```

### 8.3 Node Affinity Routing

Procd使用Node亲和性路由优先连接同节点的Storage Proxy实例：

**原理：**
- Storage Proxy作为StatefulSet部署，有稳定的网络标识：`storage-proxy-{0..n}`
- Procd通过`NODE_NAME`环境变量获取所在节点
- 使用hash函数将节点名映射到Storage Proxy副本索引

**优势：**
- 减少跨节点网络延迟
- 简单但有效的负载均衡
- 利用StatefulSet的稳定网络特性

---

## 九、错误处理

```go
var (
    ErrSandboxVolumeAlreadyMounted = errors.New("sandboxvolume_already_mounted")
    ErrSandboxVolumeNotMounted    = errors.New("sandboxvolume_not_mounted")
    ErrInvalidMountPoint          = errors.New("invalid_mount_point")
    ErrMountTimeout        = errors.New("mount_timeout")
    ErrUnmountFailed       = errors.New("unmount_failed")
    ErrConnectionFailed    = errors.New("grpc_connection_failed")
)
```

---

## 十、性能优化

### 10.1 性能目标

| Operation | 目标延迟 | 说明 |
|-----------|----------|------|
| Mount | ~30-50ms | gRPC connect + FUSE mount |
| Read (cached) | ~2-3ms | gRPC roundtrip |
| Write | ~5-10ms | gRPC + async write |
| Create | ~3-5ms | gRPC roundtrip |
| Lookup | ~1-2ms | gRPC roundtrip |

### 10.2 性能问题分析

当前架构中每个文件操作都需要经过完整的调用链：

```
User → FUSE → gRPC → Storage Proxy → JuiceFS → S3/PostgreSQL
```

**性能影响：**
- 每个读操作至少1次网络往返（即使命中Storage Proxy缓存）
- 大量小文件读取场景延迟累积明显
- Storage Proxy在高并发下可能成为瓶颈

### 10.3 性能优化方案

#### 10.3.1 Procd本地读缓存

在RemoteFS中添加本地缓存层（见4.1节数据结构定义）：

**缓存策略：**
- **仅缓存读操作**，写操作直接透传（避免一致性问题）
- 基于inode的LRU淘汰策略
- TTL过期自动失效（默认30秒）
- 可配置缓存大小上限（默认100MB）

**缓存实现示例：**

```go
// GetAttr with local cache
func (n *RemoteFSNode) Attr(ctx context.Context, a *fuse.Attr) error {
    // Try cache first
    if entry := n.fs.cache.Get(n.inode); entry != nil && !entry.Expired() {
        *a = fuseAttrFrom(entry.attr)
        return nil
    }

    // Cache miss - fetch from Storage Proxy
    req := &fs.GetAttrRequest{Inode: n.inode}
    resp, err := n.fs.client.GetAttr(withAuth(ctx, n.fs.token), req)
    if err != nil {
        return err
    }

    // Update cache
    n.fs.cache.Put(&CacheEntry{
        inode:    n.inode,
        attr:     resp,
        expireAt: time.Now().Add(n.fs.cacheTTL),
    })

    *a = fuseAttrFrom(resp)
    return nil
}

// Read with local cache
func (h *RemoteFileHandle) Read(ctx context.Context, req *fuse.ReadRequest, resp *fuse.ReadResponse) error {
    cacheKey := fmt.Sprintf("%s:%d", h.inode, req.Offset)

    // Try cache first
    if entry := h.fs.cache.Get(cacheKey); entry != nil && !entry.Expired() {
        n := copy(resp.Data, entry.data)
        resp.Data = resp.Data[:n]
        return nil
    }

    // Cache miss - fetch from Storage Proxy
    rreq := &fs.ReadRequest{
        Inode:    h.inode,
        HandleId: h.id,
        Offset:   req.Offset,
        Size:     int64(len(resp.Data)),
    }

    rresp, err := h.fs.client.Read(withAuth(ctx, h.fs.token), rreq)
    if err != nil {
        return err
    }

    n := copy(resp.Data, rresp.Data)
    resp.Data = resp.Data[:n]

    // Update cache (for small files only, e.g., < 1MB)
    if len(rresp.Data) < 1024*1024 {
        h.fs.cache.Put(&CacheEntry{
            inode:    cacheKey,
            data:     resp.Data,
            expireAt: time.Now().Add(h.fs.cacheTTL),
        })
    }

    return nil
}
```

**缓存失效：**
- 写操作后立即失效相关inode的缓存
- TTL自动过期
- LRU淘汰达到内存上限时

#### 10.3.2 Node亲和性路由

由于Storage Proxy是StatefulSet部署，优先调用同Node的实例：

```go
// SandboxVolumeManager with node-aware routing
type SandboxVolumeManager struct {
    mu     sync.RWMutex
    mounts map[string]*MountContext

    // Configuration
    proxyBaseURL    string  // Base URL: storage-proxy.storage-proxy.svc.cluster.local
    proxyReplicas   int     // Number of Storage Proxy replicas
    nodeName        string  // Current Pod's node name (from env)
}

// getStorageProxyAddress returns the preferred Storage Proxy address
func (vm *SandboxVolumeManager) getStorageProxyAddress() string {
    // Hash-based routing: same node -> same replica
    hash := fnv.New32()
    hash.Write([]byte(vm.nodeName))
    replicaIndex := hash.Sum32() % uint32(vm.proxyReplicas)

    return fmt.Sprintf("storage-proxy-%d.%s:8080", replicaIndex, vm.proxyBaseURL)
}
```

**优势：**
- 减少跨节点网络延迟
- 利用StatefulSet的稳定网络标识
- 简单的负载均衡策略

#### 10.3.3 写操作策略

**不实施写合并/写缓冲**，原因：
- 引入一致性问题（Procd crash时数据丢失）
- 与JuiceFS的写优化机制重复
- 简化错误处理流程

写操作直接透传到Storage Proxy，由JuiceFS SDK处理写入优化。

### 10.4 实现优先级

| 优先级 | 优化项 | 复杂度 | 收益 |
|--------|--------|--------|------|
| P0 | Node亲和性路由 | 低 | 高（减少网络延迟） |
| P1 | Procd本地读缓存 | 中 | 高（减少gRPC调用） |
| P2 | 监控指标 | 低 | 中（可观测性） |

### 10.5 监控指标

需要添加的Prometheus指标：
- `procd_cache_hit_rate`: 缓存命中率
- `procd_cache_size_bytes`: 缓存使用大小
- `procd_grpc_latency_ms`: gRPC调用延迟（P50/P99）
- `procd_fuse_ops_total`: FUSE操作计数（按类型）

---

## 十一、优势总结

| 特性 | 说明 |
|------|------|
| **零凭证** | Procd 不持有任何 S3/PG 凭证 |
| **轻量级** | 只负责 FUSE + gRPC 客户端 |
| **网络隔离** | Packet marking 绕过用户规则 |
| **快速挂载** | <50ms 延迟 |
| **简化架构** | 无需 JuiceFS 嵌入 |
| **集中式存储** | 所有存储逻辑在 Proxy |
