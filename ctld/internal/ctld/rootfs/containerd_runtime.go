package rootfs

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	containerd "github.com/containerd/containerd/v2/client"
	"github.com/containerd/containerd/v2/core/content"
	"github.com/containerd/containerd/v2/core/images"
	"github.com/containerd/containerd/v2/core/snapshots"
	"github.com/opencontainers/go-digest"
	"github.com/sandbox0-ai/sandbox0/pkg/ctldapi"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
)

const (
	defaultCRIEndpoint        = "/host-run/containerd/containerd.sock"
	defaultContainerdEndpoint = "/host-run/containerd/containerd.sock"
	defaultNamespace          = "k8s.io"
	defaultDialTimeout        = 10 * time.Second
)

type criRuntimeService interface {
	ListContainers(ctx context.Context, in *runtimeapi.ListContainersRequest, opts ...grpc.CallOption) (*runtimeapi.ListContainersResponse, error)
	ListPodSandbox(ctx context.Context, in *runtimeapi.ListPodSandboxRequest, opts ...grpc.CallOption) (*runtimeapi.ListPodSandboxResponse, error)
	PodSandboxStats(ctx context.Context, in *runtimeapi.PodSandboxStatsRequest, opts ...grpc.CallOption) (*runtimeapi.PodSandboxStatsResponse, error)
}

type criImageService interface {
	ImageStatus(ctx context.Context, in *runtimeapi.ImageStatusRequest, opts ...grpc.CallOption) (*runtimeapi.ImageStatusResponse, error)
}

type ContainerdRuntimeConfig struct {
	CRIEndpoint            string
	ContainerdEndpoint     string
	Namespace              string
	DialTimeout            time.Duration
	CRIClient              criRuntimeService
	CRIImageClient         criImageService
	CRIDialContext         func(ctx context.Context, endpoint string) (*grpc.ClientConn, error)
	ContainerdClient       containerdClient
	ContainerdDataRoot     string
	ContainerdHostDataRoot string
}

type ContainerdRuntime struct {
	criEndpoint             string
	containerdEndpoint      string
	containerdDataRoot      string
	containerdHostDataRoot  string
	namespace               string
	dialTimeout             time.Duration
	criClient               criRuntimeService
	criImageClient          criImageService
	criDialContext          func(ctx context.Context, endpoint string) (*grpc.ClientConn, error)
	criMu                   sync.Mutex
	criConn                 *grpc.ClientConn
	connectedCRIClient      criRuntimeService
	connectedCRIImageClient criImageService
	containerdClient        containerdClient
}

type containerdClient interface {
	LoadContainer(ctx context.Context, id string) (containerd.Container, error)
	SnapshotService(snapshotterName string) snapshots.Snapshotter
	ContentStore() content.Store
	ImageService() images.Store
	Close() error
}

func NewContainerdRuntime(cfg ContainerdRuntimeConfig) *ContainerdRuntime {
	criEndpoint := strings.TrimSpace(cfg.CRIEndpoint)
	if criEndpoint == "" {
		criEndpoint = defaultCRIEndpoint
	}
	containerdEndpoint := strings.TrimSpace(cfg.ContainerdEndpoint)
	if containerdEndpoint == "" {
		containerdEndpoint = defaultContainerdEndpoint
	}
	namespace := strings.TrimSpace(cfg.Namespace)
	if namespace == "" {
		namespace = defaultNamespace
	}
	timeout := cfg.DialTimeout
	if timeout <= 0 {
		timeout = defaultDialTimeout
	}
	imageClient := cfg.CRIImageClient
	if imageClient == nil {
		imageClient, _ = cfg.CRIClient.(criImageService)
	}
	return &ContainerdRuntime{
		criEndpoint:            criEndpoint,
		containerdEndpoint:     containerdEndpoint,
		containerdDataRoot:     strings.TrimSpace(cfg.ContainerdDataRoot),
		containerdHostDataRoot: strings.TrimSpace(cfg.ContainerdHostDataRoot),
		namespace:              namespace,
		dialTimeout:            timeout,
		criClient:              cfg.CRIClient,
		criImageClient:         imageClient,
		criDialContext:         cfg.CRIDialContext,
		containerdClient:       cfg.ContainerdClient,
	}
}

func (r *ContainerdRuntime) Inspect(ctx context.Context, target ctldapi.RootFSContainerRef) (ctldapi.RootFSInfo, error) {
	containerID, podUID, err := r.resolveContainerID(ctx, target)
	if err != nil {
		return ctldapi.RootFSInfo{}, err
	}
	client, closeClient, err := r.client(ctx)
	if err != nil {
		return ctldapi.RootFSInfo{}, err
	}
	defer closeClient()
	return inspectContainer(ctx, client, r.namespace, target, containerID, podUID)
}

func (r *ContainerdRuntime) resolveContainerID(ctx context.Context, target ctldapi.RootFSContainerRef) (string, string, error) {
	client, err := r.runtimeClient(ctx)
	if err != nil {
		return "", "", err
	}
	requestedID := normalizeContainerID(target.ContainerID)
	filter := &runtimeapi.ContainerFilter{}
	if requestedID != "" {
		filter.Id = requestedID
	} else {
		filter.State = &runtimeapi.ContainerStateValue{State: runtimeapi.ContainerState_CONTAINER_RUNNING}
	}
	resp, err := client.ListContainers(ctx, &runtimeapi.ListContainersRequest{Filter: filter})
	if err != nil {
		return "", "", fmt.Errorf("list cri containers: %w", err)
	}
	for _, item := range resp.GetContainers() {
		if requestedID != "" && strings.TrimSpace(item.GetId()) != requestedID {
			continue
		}
		metadata := item.GetMetadata()
		if metadata == nil || metadata.GetName() != target.ContainerName {
			continue
		}
		labels := item.GetLabels()
		if labels["io.kubernetes.pod.namespace"] != target.Namespace || labels["io.kubernetes.pod.name"] != target.PodName {
			continue
		}
		podUID := labels["io.kubernetes.pod.uid"]
		if target.PodUID != "" && podUID != target.PodUID {
			continue
		}
		if requestedID == "" && item.GetState() != runtimeapi.ContainerState_CONTAINER_RUNNING {
			continue
		}
		return item.GetId(), podUID, nil
	}
	if requestedID != "" {
		return "", "", fmt.Errorf("%w: container %s for %s in pod %s/%s", ErrNotFound, requestedID, target.ContainerName, target.Namespace, target.PodName)
	}
	return "", "", fmt.Errorf("%w: running container %s in pod %s/%s", ErrNotFound, target.ContainerName, target.Namespace, target.PodName)
}

func normalizeContainerID(containerID string) string {
	containerID = strings.TrimSpace(containerID)
	if _, raw, ok := strings.Cut(containerID, "://"); ok {
		return strings.TrimSpace(raw)
	}
	return containerID
}

// ListPodSandboxes returns ready node-local CRI sandboxes for isolated stats collection.
func (r *ContainerdRuntime) ListPodSandboxes(ctx context.Context) ([]*runtimeapi.PodSandbox, error) {
	client, err := r.runtimeClient(ctx)
	if err != nil {
		return nil, err
	}
	resp, err := client.ListPodSandbox(ctx, &runtimeapi.ListPodSandboxRequest{
		Filter: &runtimeapi.PodSandboxFilter{State: &runtimeapi.PodSandboxStateValue{State: runtimeapi.PodSandboxState_SANDBOX_READY}},
	})
	if err != nil {
		return nil, fmt.Errorf("list CRI pod sandboxes: %w", err)
	}
	return resp.GetItems(), nil
}

// PodSandboxStats returns one isolated CRI sandbox stats sample.
func (r *ContainerdRuntime) PodSandboxStats(ctx context.Context, sandboxID string) (*runtimeapi.PodSandboxStats, error) {
	client, err := r.runtimeClient(ctx)
	if err != nil {
		return nil, err
	}
	resp, err := client.PodSandboxStats(ctx, &runtimeapi.PodSandboxStatsRequest{PodSandboxId: sandboxID})
	if err != nil {
		return nil, fmt.Errorf("get CRI pod sandbox %s stats: %w", sandboxID, err)
	}
	if resp.GetStats() == nil {
		return nil, fmt.Errorf("get CRI pod sandbox %s stats: empty response", sandboxID)
	}
	return resp.GetStats(), nil
}

// Close releases the cached CRI connection. Injected clients are not owned.
func (r *ContainerdRuntime) Close() error {
	if r == nil {
		return nil
	}
	r.criMu.Lock()
	conn := r.criConn
	r.criConn = nil
	r.connectedCRIClient = nil
	r.connectedCRIImageClient = nil
	r.criMu.Unlock()
	if conn == nil {
		return nil
	}
	return conn.Close()
}

func (r *ContainerdRuntime) runtimeClient(ctx context.Context) (criRuntimeService, error) {
	if r != nil && r.criClient != nil {
		return r.criClient, nil
	}
	if r == nil {
		return nil, fmt.Errorf("containerd runtime is nil")
	}
	r.criMu.Lock()
	defer r.criMu.Unlock()
	if r.connectedCRIClient != nil {
		return r.connectedCRIClient, nil
	}
	endpoint := r.criEndpoint
	timeout := r.dialTimeout
	dialCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	dialer := dialCRIEndpoint
	if r.criDialContext != nil {
		dialer = r.criDialContext
	}
	conn, err := dialer(dialCtx, normalizeCRIEndpoint(endpoint))
	if err != nil {
		return nil, fmt.Errorf("dial cri endpoint %s: %w", endpoint, err)
	}
	r.criConn = conn
	r.connectedCRIClient = runtimeapi.NewRuntimeServiceClient(conn)
	r.connectedCRIImageClient = runtimeapi.NewImageServiceClient(conn)
	return r.connectedCRIClient, nil
}

func (r *ContainerdRuntime) imageClient(ctx context.Context) (criImageService, error) {
	if r != nil && r.criImageClient != nil {
		return r.criImageClient, nil
	}
	if r == nil {
		return nil, fmt.Errorf("containerd runtime is nil")
	}
	if r.criClient != nil {
		return nil, fmt.Errorf("cri image service client is not configured")
	}
	if _, err := r.runtimeClient(ctx); err != nil {
		return nil, err
	}
	r.criMu.Lock()
	defer r.criMu.Unlock()
	if r.connectedCRIImageClient == nil {
		return nil, fmt.Errorf("cri image service client is not connected")
	}
	return r.connectedCRIImageClient, nil
}

func (r *ContainerdRuntime) client(ctx context.Context) (containerdClient, func(), error) {
	if r != nil && r.containerdClient != nil {
		return r.containerdClient, func() {}, nil
	}
	endpoint := defaultContainerdEndpoint
	namespace := defaultNamespace
	timeout := defaultDialTimeout
	if r != nil {
		endpoint = r.containerdEndpoint
		namespace = r.namespace
		timeout = r.dialTimeout
	}
	client, err := containerd.New(endpoint, containerd.WithDefaultNamespace(namespace), containerd.WithTimeout(timeout))
	if err != nil {
		return nil, nil, fmt.Errorf("connect containerd endpoint %s: %w", endpoint, err)
	}
	if err := ctx.Err(); err != nil {
		_ = client.Close()
		return nil, nil, err
	}
	return client, func() { _ = client.Close() }, nil
}

func inspectContainer(ctx context.Context, client containerdClient, namespace string, target ctldapi.RootFSContainerRef, containerID, podUID string) (ctldapi.RootFSInfo, error) {
	container, err := client.LoadContainer(ctx, containerID)
	if err != nil {
		return ctldapi.RootFSInfo{}, fmt.Errorf("load container %s: %w", containerID, err)
	}
	containerInfo, err := container.Info(ctx)
	if err != nil {
		return ctldapi.RootFSInfo{}, fmt.Errorf("inspect container %s: %w", containerID, err)
	}
	containerdID := strings.TrimSpace(container.ID())
	if containerdID == "" {
		containerdID = containerID
	}
	info := ctldapi.RootFSInfo{
		ContainerID: containerdID, ContainerName: target.ContainerName,
		PodNamespace: target.Namespace, PodName: target.PodName, PodUID: firstNonEmpty(target.PodUID, podUID),
		RuntimeHandler: containerInfo.Runtime.Name, Snapshotter: containerInfo.Snapshotter,
		SnapshotKey: containerInfo.SnapshotKey, BaseImageRef: containerInfo.Image,
	}
	info.Runtime = runtimeFamily(containerInfo.Runtime.Name)
	if imageDigest, err := imageDigest(ctx, client, containerInfo.Image); err == nil {
		info.BaseImageDigest = imageDigest
	}
	if info.BaseImageDigest == "" {
		info.BaseImageDigest = digestFromReference(containerInfo.Image)
	}
	parent, chain, err := snapshotParentChain(ctx, client.SnapshotService(containerInfo.Snapshotter), containerInfo.SnapshotKey)
	if err != nil {
		return ctldapi.RootFSInfo{}, fmt.Errorf("inspect snapshot parent chain: %w", err)
	}
	info.SnapshotParent = parent
	info.SnapshotParentChain = chain
	_ = namespace
	return info, nil
}

func snapshotParentChain(ctx context.Context, snapshotter snapshots.Snapshotter, snapshotKey string) (string, []string, error) {
	if snapshotter == nil || strings.TrimSpace(snapshotKey) == "" {
		return "", nil, nil
	}
	info, err := snapshotter.Stat(ctx, snapshotKey)
	if err != nil {
		return "", nil, err
	}
	parent := strings.TrimSpace(info.Parent)
	chain := make([]string, 0, 8)
	for key := parent; key != ""; {
		chain = append(chain, key)
		nextInfo, err := snapshotter.Stat(ctx, key)
		if err != nil {
			return parent, chain, err
		}
		key = strings.TrimSpace(nextInfo.Parent)
	}
	return parent, chain, nil
}

func imageDigest(ctx context.Context, client containerdClient, imageRef string) (string, error) {
	imageRef = strings.TrimSpace(imageRef)
	if imageRef == "" {
		return "", nil
	}
	image, err := client.ImageService().Get(ctx, imageRef)
	if err != nil {
		return "", err
	}
	if image.Target.Digest == "" {
		return "", nil
	}
	return image.Target.Digest.String(), nil
}

func runtimeFamily(handler string) string {
	raw := strings.ToLower(strings.TrimSpace(handler))
	switch {
	case strings.Contains(raw, "runsc") || strings.Contains(raw, "gvisor"):
		return "gvisor"
	case strings.Contains(raw, "runc"):
		return "runc"
	case strings.Contains(raw, "kata"):
		return "kata"
	default:
		return raw
	}
}

func digestFromReference(ref string) string {
	if position := strings.LastIndex(ref, "@"); position >= 0 && position+1 < len(ref) {
		if value, err := digest.Parse(ref[position+1:]); err == nil {
			return value.String()
		}
	}
	return ""
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			return trimmed
		}
	}
	return ""
}

func dialCRIEndpoint(ctx context.Context, endpoint string) (*grpc.ClientConn, error) {
	return grpc.DialContext(ctx, endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
}

func normalizeCRIEndpoint(endpoint string) string {
	endpoint = strings.TrimSpace(endpoint)
	if endpoint == "" {
		return "unix://" + defaultCRIEndpoint
	}
	if strings.Contains(endpoint, "://") {
		return endpoint
	}
	if strings.HasPrefix(endpoint, "/") {
		return "unix://" + endpoint
	}
	return endpoint
}
