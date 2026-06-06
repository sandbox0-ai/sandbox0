package rootfs

import (
	"context"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"time"

	containerd "github.com/containerd/containerd/v2/client"
	"github.com/containerd/containerd/v2/core/content"
	"github.com/containerd/containerd/v2/core/images"
	"github.com/containerd/containerd/v2/core/mount"
	"github.com/containerd/containerd/v2/core/snapshots"
	crootfs "github.com/containerd/containerd/v2/pkg/rootfs"
	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/sandbox0-ai/sandbox0/pkg/ctldapi"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
)

const (
	defaultCRIEndpoint        = "/host-run/containerd/containerd.sock"
	defaultContainerdEndpoint = "/host-run/containerd/containerd.sock"
	defaultContainerdRoot     = "/host-run/containerd"
	defaultNamespace          = "k8s.io"
	defaultDialTimeout        = 10 * time.Second
)

type criRuntimeService interface {
	ListContainers(ctx context.Context, in *runtimeapi.ListContainersRequest, opts ...grpc.CallOption) (*runtimeapi.ListContainersResponse, error)
}

type ContainerdRuntimeConfig struct {
	CRIEndpoint        string
	ContainerdEndpoint string
	ContainerdRoot     string
	Namespace          string
	DialTimeout        time.Duration
	CRIClient          criRuntimeService
	CRIDialContext     func(ctx context.Context, endpoint string) (*grpc.ClientConn, error)
	ContainerdClient   containerdClient
}

type ContainerdRuntime struct {
	criEndpoint        string
	containerdEndpoint string
	containerdRoot     string
	namespace          string
	dialTimeout        time.Duration
	criClient          criRuntimeService
	criDialContext     func(ctx context.Context, endpoint string) (*grpc.ClientConn, error)
	containerdClient   containerdClient
}

type containerdClient interface {
	LoadContainer(ctx context.Context, id string) (containerd.Container, error)
	SnapshotService(snapshotterName string) snapshots.Snapshotter
	DiffService() containerd.DiffService
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
	containerdRoot := strings.TrimSpace(cfg.ContainerdRoot)
	if containerdRoot == "" {
		containerdRoot = defaultContainerdRoot
	}
	namespace := strings.TrimSpace(cfg.Namespace)
	if namespace == "" {
		namespace = defaultNamespace
	}
	timeout := cfg.DialTimeout
	if timeout <= 0 {
		timeout = defaultDialTimeout
	}
	return &ContainerdRuntime{
		criEndpoint:        criEndpoint,
		containerdEndpoint: containerdEndpoint,
		containerdRoot:     containerdRoot,
		namespace:          namespace,
		dialTimeout:        timeout,
		criClient:          cfg.CRIClient,
		criDialContext:     cfg.CRIDialContext,
		containerdClient:   cfg.ContainerdClient,
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

	info, err := inspectContainer(ctx, client, r.containerdRoot, r.namespace, target, containerID, podUID)
	if err != nil {
		return ctldapi.RootFSInfo{}, err
	}
	return info, nil
}

func (r *ContainerdRuntime) CreateDiff(ctx context.Context, info ctldapi.RootFSInfo) (ctldapi.RootFSDiffDescriptor, io.ReadSeekCloser, error) {
	if strings.TrimSpace(info.SnapshotKey) == "" || strings.TrimSpace(info.Snapshotter) == "" {
		return ctldapi.RootFSDiffDescriptor{}, nil, fmt.Errorf("%w: snapshot key and snapshotter are required", ErrBadRequest)
	}
	client, closeClient, err := r.client(ctx)
	if err != nil {
		return ctldapi.RootFSDiffDescriptor{}, nil, err
	}

	desc, err := crootfs.CreateDiff(ctx, info.SnapshotKey, client.SnapshotService(info.Snapshotter), client.DiffService())
	if err != nil {
		closeClient()
		return ctldapi.RootFSDiffDescriptor{}, nil, err
	}
	reader, err := content.BlobReadSeeker(ctx, client.ContentStore(), desc)
	if err != nil {
		closeClient()
		return ctldapi.RootFSDiffDescriptor{}, nil, err
	}
	return descriptorFromOCI(desc), closeReadSeekWithFunc{ReadSeekCloser: reader, closeFunc: closeClient}, nil
}

func (r *ContainerdRuntime) ApplyDiff(ctx context.Context, info ctldapi.RootFSInfo, desc ctldapi.RootFSDiffDescriptor, reader io.Reader) (ctldapi.RootFSDiffDescriptor, error) {
	if strings.TrimSpace(info.ContainerID) == "" {
		return ctldapi.RootFSDiffDescriptor{}, fmt.Errorf("%w: container id is required", ErrBadRequest)
	}
	client, closeClient, err := r.client(ctx)
	if err != nil {
		return ctldapi.RootFSDiffDescriptor{}, err
	}
	defer closeClient()

	ociDesc, err := descriptorToOCI(desc)
	if err != nil {
		return ctldapi.RootFSDiffDescriptor{}, err
	}
	ref := "sandbox0-rootfs-apply-" + strings.ReplaceAll(ociDesc.Digest.String(), ":", "-")
	if err := content.WriteBlob(ctx, client.ContentStore(), ref, reader, ociDesc); err != nil {
		return ctldapi.RootFSDiffDescriptor{}, fmt.Errorf("write rootfs diff into containerd content store: %w", err)
	}
	liveRootFS := filepath.Join(r.containerdRoot, "io.containerd.runtime.v2.task", r.namespace, info.ContainerID, "rootfs")
	applied, err := client.DiffService().Apply(ctx, ociDesc, []mount.Mount{{
		Type:    "bind",
		Source:  liveRootFS,
		Options: []string{"rbind", "rw"},
	}})
	if err != nil {
		return ctldapi.RootFSDiffDescriptor{}, err
	}
	return descriptorFromOCI(applied), nil
}

func (r *ContainerdRuntime) resolveContainerID(ctx context.Context, target ctldapi.RootFSContainerRef) (string, string, error) {
	client, conn, err := r.runtimeClient(ctx)
	if err != nil {
		return "", "", err
	}
	if conn != nil {
		defer conn.Close()
	}
	resp, err := client.ListContainers(ctx, &runtimeapi.ListContainersRequest{
		Filter: &runtimeapi.ContainerFilter{
			State: &runtimeapi.ContainerStateValue{State: runtimeapi.ContainerState_CONTAINER_RUNNING},
		},
	})
	if err != nil {
		return "", "", fmt.Errorf("list cri containers: %w", err)
	}
	for _, item := range resp.GetContainers() {
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
		if item.GetState() != runtimeapi.ContainerState_CONTAINER_RUNNING {
			continue
		}
		return item.GetId(), podUID, nil
	}
	return "", "", fmt.Errorf("%w: running container %s in pod %s/%s", ErrNotFound, target.ContainerName, target.Namespace, target.PodName)
}

func (r *ContainerdRuntime) runtimeClient(ctx context.Context) (criRuntimeService, *grpc.ClientConn, error) {
	if r != nil && r.criClient != nil {
		return r.criClient, nil, nil
	}
	endpoint := defaultCRIEndpoint
	if r != nil && strings.TrimSpace(r.criEndpoint) != "" {
		endpoint = r.criEndpoint
	}
	timeout := defaultDialTimeout
	if r != nil && r.dialTimeout > 0 {
		timeout = r.dialTimeout
	}
	dialCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	dialer := dialCRIEndpoint
	if r != nil && r.criDialContext != nil {
		dialer = r.criDialContext
	}
	conn, err := dialer(dialCtx, normalizeCRIEndpoint(endpoint))
	if err != nil {
		return nil, nil, fmt.Errorf("dial cri endpoint %s: %w", endpoint, err)
	}
	return runtimeapi.NewRuntimeServiceClient(conn), conn, nil
}

func (r *ContainerdRuntime) client(ctx context.Context) (containerdClient, func(), error) {
	if r != nil && r.containerdClient != nil {
		return r.containerdClient, func() {}, nil
	}
	endpoint := defaultContainerdEndpoint
	namespace := defaultNamespace
	timeout := defaultDialTimeout
	if r != nil {
		if strings.TrimSpace(r.containerdEndpoint) != "" {
			endpoint = r.containerdEndpoint
		}
		if strings.TrimSpace(r.namespace) != "" {
			namespace = r.namespace
		}
		if r.dialTimeout > 0 {
			timeout = r.dialTimeout
		}
	}
	dialCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	client, err := containerd.New(endpoint, containerd.WithDefaultNamespace(namespace), containerd.WithTimeout(timeout))
	if err != nil {
		return nil, nil, fmt.Errorf("connect containerd endpoint %s: %w", endpoint, err)
	}
	select {
	case <-dialCtx.Done():
		_ = client.Close()
		return nil, nil, dialCtx.Err()
	default:
	}
	return client, func() { _ = client.Close() }, nil
}

func inspectContainer(ctx context.Context, client containerdClient, containerdRoot, namespace string, target ctldapi.RootFSContainerRef, containerID, podUID string) (ctldapi.RootFSInfo, error) {
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
		ContainerID:    containerdID,
		ContainerName:  target.ContainerName,
		PodNamespace:   target.Namespace,
		PodName:        target.PodName,
		PodUID:         firstNonEmpty(target.PodUID, podUID),
		RuntimeHandler: containerInfo.Runtime.Name,
		Snapshotter:    containerInfo.Snapshotter,
		SnapshotKey:    containerInfo.SnapshotKey,
		BaseImageRef:   containerInfo.Image,
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
	d := image.Target.Digest
	if d == "" {
		return "", nil
	}
	return d.String(), nil
}

func descriptorFromOCI(desc ocispec.Descriptor) ctldapi.RootFSDiffDescriptor {
	return ctldapi.RootFSDiffDescriptor{
		MediaType: desc.MediaType,
		Digest:    desc.Digest.String(),
		Size:      desc.Size,
	}
}

func descriptorToOCI(desc ctldapi.RootFSDiffDescriptor) (ocispec.Descriptor, error) {
	d, err := digest.Parse(strings.TrimSpace(desc.Digest))
	if err != nil {
		return ocispec.Descriptor{}, fmt.Errorf("%w: invalid descriptor digest: %v", ErrBadRequest, err)
	}
	return ocispec.Descriptor{
		MediaType: strings.TrimSpace(desc.MediaType),
		Digest:    d,
		Size:      desc.Size,
	}, nil
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
	if idx := strings.LastIndex(ref, "@"); idx >= 0 && idx+1 < len(ref) {
		if d, err := digest.Parse(ref[idx+1:]); err == nil {
			return d.String()
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

type closeReadSeekWithFunc struct {
	io.ReadSeekCloser
	closeFunc func()
}

func (r closeReadSeekWithFunc) Close() error {
	err := r.ReadSeekCloser.Close()
	if r.closeFunc != nil {
		r.closeFunc()
	}
	return err
}
