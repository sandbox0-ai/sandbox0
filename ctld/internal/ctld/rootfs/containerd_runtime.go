package rootfs

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	containerd "github.com/containerd/containerd/v2/client"
	"github.com/containerd/containerd/v2/core/content"
	"github.com/containerd/containerd/v2/core/images"
	"github.com/containerd/containerd/v2/core/mount"
	"github.com/containerd/containerd/v2/core/snapshots"
	crootfs "github.com/containerd/containerd/v2/pkg/rootfs"
	"github.com/containerd/continuity/fs"
	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/sandbox0-ai/sandbox0/pkg/ctldapi"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
)

const (
	defaultCRIEndpoint            = "/host-run/containerd/containerd.sock"
	defaultContainerdEndpoint     = "/host-run/containerd/containerd.sock"
	defaultContainerdRoot         = "/host-run/containerd"
	defaultContainerdHostRoot     = "/run/containerd"
	defaultContainerdDataRoot     = "/host-var-lib/containerd"
	defaultContainerdHostDataRoot = "/var/lib/containerd"
	defaultRootFSCacheDir         = "/var/lib/sandbox0/ctld/rootfs"
	defaultNamespace              = "k8s.io"
	defaultDialTimeout            = 10 * time.Second
)

type criRuntimeService interface {
	ListContainers(ctx context.Context, in *runtimeapi.ListContainersRequest, opts ...grpc.CallOption) (*runtimeapi.ListContainersResponse, error)
	ListPodSandboxStats(ctx context.Context, in *runtimeapi.ListPodSandboxStatsRequest, opts ...grpc.CallOption) (*runtimeapi.ListPodSandboxStatsResponse, error)
}

type ContainerdRuntimeConfig struct {
	CRIEndpoint            string
	ContainerdEndpoint     string
	ContainerdRoot         string
	ContainerdHostRoot     string
	ContainerdDataRoot     string
	ContainerdHostDataRoot string
	RootFSCacheDir         string
	Namespace              string
	DialTimeout            time.Duration
	CRIClient              criRuntimeService
	CRIDialContext         func(ctx context.Context, endpoint string) (*grpc.ClientConn, error)
	ContainerdClient       containerdClient
}

type ContainerdRuntime struct {
	criEndpoint            string
	containerdEndpoint     string
	containerdRoot         string
	containerdHostRoot     string
	containerdDataRoot     string
	containerdHostDataRoot string
	rootFSCacheDir         string
	namespace              string
	dialTimeout            time.Duration
	criClient              criRuntimeService
	criDialContext         func(ctx context.Context, endpoint string) (*grpc.ClientConn, error)
	criMu                  sync.Mutex
	criConn                *grpc.ClientConn
	connectedCRIClient     criRuntimeService
	containerdClient       containerdClient
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
	containerdHostRoot := strings.TrimSpace(cfg.ContainerdHostRoot)
	if containerdHostRoot == "" {
		containerdHostRoot = defaultContainerdHostRoot
	}
	containerdDataRoot := strings.TrimSpace(cfg.ContainerdDataRoot)
	if containerdDataRoot == "" {
		containerdDataRoot = defaultContainerdDataRoot
	}
	containerdHostDataRoot := strings.TrimSpace(cfg.ContainerdHostDataRoot)
	if containerdHostDataRoot == "" {
		containerdHostDataRoot = defaultContainerdHostDataRoot
	}
	rootFSCacheDir := strings.TrimSpace(cfg.RootFSCacheDir)
	if rootFSCacheDir == "" {
		rootFSCacheDir = defaultRootFSCacheDir
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
		criEndpoint:            criEndpoint,
		containerdEndpoint:     containerdEndpoint,
		containerdRoot:         containerdRoot,
		containerdHostRoot:     containerdHostRoot,
		containerdDataRoot:     containerdDataRoot,
		containerdHostDataRoot: containerdHostDataRoot,
		rootFSCacheDir:         rootFSCacheDir,
		namespace:              namespace,
		dialTimeout:            timeout,
		criClient:              cfg.CRIClient,
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

	info, err := inspectContainer(ctx, client, r.containerdRoot, r.namespace, target, containerID, podUID)
	if err != nil {
		return ctldapi.RootFSInfo{}, err
	}
	return info, nil
}

func (r *ContainerdRuntime) CreateDiff(ctx context.Context, info ctldapi.RootFSInfo, excludedPaths []string, portalPaths []ctldapi.RootFSPortalPath) (ctldapi.RootFSDiffDescriptor, *ctldapi.RootFSDiffStats, io.ReadSeekCloser, error) {
	if strings.TrimSpace(info.SnapshotKey) == "" || strings.TrimSpace(info.Snapshotter) == "" {
		return ctldapi.RootFSDiffDescriptor{}, nil, nil, fmt.Errorf("%w: snapshot key and snapshotter are required", ErrBadRequest)
	}
	client, closeClient, err := r.client(ctx)
	if err != nil {
		return ctldapi.RootFSDiffDescriptor{}, nil, nil, err
	}

	if desc, reader, ok, fastErr := r.createOverlayUpperDiff(ctx, client, info, excludedPaths, portalPaths); ok && fastErr == nil {
		closeClient()
		return desc, nil, reader, nil
	} else if ok && fastErr != nil {
		desc, err := crootfs.CreateDiff(ctx, info.SnapshotKey, client.SnapshotService(info.Snapshotter), client.DiffService())
		if err != nil {
			closeClient()
			return ctldapi.RootFSDiffDescriptor{}, nil, nil, fmt.Errorf("overlayfs fast diff: %v; containerd diff: %w", fastErr, err)
		}
		rootDesc, reader, needsClient, err := rootFSDiffReaderFromContent(ctx, client, desc, excludedPaths, portalPaths)
		if err != nil {
			closeClient()
			return ctldapi.RootFSDiffDescriptor{}, nil, nil, err
		}
		if !needsClient {
			closeClient()
			return rootDesc, nil, reader, nil
		}
		return rootDesc, nil, closeReadSeekWithFunc{ReadSeekCloser: reader, closeFunc: closeClient}, nil
	}

	desc, err := crootfs.CreateDiff(ctx, info.SnapshotKey, client.SnapshotService(info.Snapshotter), client.DiffService())
	if err != nil {
		closeClient()
		return ctldapi.RootFSDiffDescriptor{}, nil, nil, err
	}
	rootDesc, reader, needsClient, err := rootFSDiffReaderFromContent(ctx, client, desc, excludedPaths, portalPaths)
	if err != nil {
		closeClient()
		return ctldapi.RootFSDiffDescriptor{}, nil, nil, err
	}
	if !needsClient {
		closeClient()
		return rootDesc, nil, reader, nil
	}
	return rootDesc, nil, closeReadSeekWithFunc{ReadSeekCloser: reader, closeFunc: closeClient}, nil
}

func (r *ContainerdRuntime) CreateDiffFromBaseline(ctx context.Context, info ctldapi.RootFSInfo, baselineLayerID string, excludedPaths []string, portalPaths []ctldapi.RootFSPortalPath) (ctldapi.RootFSDiffDescriptor, *ctldapi.RootFSDiffStats, io.ReadSeekCloser, error) {
	if strings.TrimSpace(baselineLayerID) == "" {
		return ctldapi.RootFSDiffDescriptor{}, nil, nil, fmt.Errorf("%w: baseline layer id is required", ErrBadRequest)
	}
	client, closeClient, err := r.client(ctx)
	if err != nil {
		return ctldapi.RootFSDiffDescriptor{}, nil, nil, err
	}
	defer closeClient()

	upperdir, err := r.activeOverlayUpperdir(ctx, client, info)
	if err != nil {
		return ctldapi.RootFSDiffDescriptor{}, nil, nil, err
	}
	baselineStateDir := r.rootFSBaselineStatePath(info, baselineLayerID)
	baselineDir := filepath.Join(baselineStateDir, "upper")
	indexPath := rootFSFileSizeIndexPath(baselineStateDir)
	if st, err := os.Stat(baselineDir); err != nil {
		if os.IsNotExist(err) {
			baselineDir = r.rootFSLegacyBaselinePath(info, baselineLayerID)
			indexPath = ""
			if legacy, legacyErr := os.Stat(baselineDir); legacyErr != nil {
				if os.IsNotExist(legacyErr) {
					return ctldapi.RootFSDiffDescriptor{}, nil, nil, fmt.Errorf("%w: rootfs baseline %s is not captured", ErrNotFound, baselineLayerID)
				}
				return ctldapi.RootFSDiffDescriptor{}, nil, nil, fmt.Errorf("inspect rootfs baseline: %w", legacyErr)
			} else if !legacy.IsDir() {
				return ctldapi.RootFSDiffDescriptor{}, nil, nil, fmt.Errorf("%w: rootfs baseline path is not a directory", ErrConflict)
			}
		} else {
			return ctldapi.RootFSDiffDescriptor{}, nil, nil, fmt.Errorf("inspect rootfs baseline: %w", err)
		}
	} else if !st.IsDir() {
		return ctldapi.RootFSDiffDescriptor{}, nil, nil, fmt.Errorf("%w: rootfs baseline path is not a directory", ErrConflict)
	}
	var fileSizes rootFSFileSizeIndex
	if indexPath != "" {
		// The index is advisory. Missing, corrupt, or newer metadata falls back to
		// the legacy baseline filesystem walk without blocking pause.
		fileSizes, _ = loadRootFSFileSizeIndex(indexPath)
	}
	return writeOverlayUpperDiffFromBaseline(ctx, baselineDir, upperdir, excludedPaths, portalPaths, fileSizes)
}

func (r *ContainerdRuntime) ApplyDiff(ctx context.Context, info ctldapi.RootFSInfo, desc ctldapi.RootFSDiffDescriptor, reader io.Reader, excludedPaths []string, portalPaths []ctldapi.RootFSPortalPath) (ctldapi.RootFSDiffDescriptor, []rootFSFileChange, error) {
	if strings.TrimSpace(info.ContainerID) == "" {
		return ctldapi.RootFSDiffDescriptor{}, nil, fmt.Errorf("%w: container id is required", ErrBadRequest)
	}
	client, closeClient, err := r.client(ctx)
	if err != nil {
		return ctldapi.RootFSDiffDescriptor{}, nil, err
	}
	defer closeClient()

	liveRootFS, err := liveRootFSPath(r.containerdRoot, r.containerdHostRoot, r.namespace, info)
	if err != nil {
		return ctldapi.RootFSDiffDescriptor{}, nil, err
	}
	filteredDesc, filteredReader, changes, err := filterRootFSDiffTarForApply(desc, reader, excludedPaths, portalPaths)
	if err != nil {
		return ctldapi.RootFSDiffDescriptor{}, nil, fmt.Errorf("filter rootfs diff: %w", err)
	}
	defer filteredReader.Close()
	desc = filteredDesc
	reader = filteredReader

	ociDesc, err := descriptorToOCI(desc)
	if err != nil {
		return ctldapi.RootFSDiffDescriptor{}, nil, err
	}
	ref := "sandbox0-rootfs-apply-" + strings.ReplaceAll(ociDesc.Digest.String(), ":", "-")
	if err := content.WriteBlob(ctx, client.ContentStore(), ref, reader, ociDesc); err != nil {
		return ctldapi.RootFSDiffDescriptor{}, nil, fmt.Errorf("write rootfs diff into containerd content store: %w", err)
	}
	applied, err := client.DiffService().Apply(ctx, ociDesc, []mount.Mount{{
		Type:    "bind",
		Source:  liveRootFS,
		Options: []string{"rbind", "rw"},
	}})
	if err != nil {
		return ctldapi.RootFSDiffDescriptor{}, nil, err
	}
	return descriptorFromOCI(applied), changes, nil
}

func (r *ContainerdRuntime) CaptureBaseline(ctx context.Context, info ctldapi.RootFSInfo, baselineLayerID string, excludedPaths []string, portalPaths []ctldapi.RootFSPortalPath, fileSizes rootFSFileSizeIndex) error {
	if strings.TrimSpace(baselineLayerID) == "" {
		return fmt.Errorf("%w: baseline layer id is required", ErrBadRequest)
	}
	client, closeClient, err := r.client(ctx)
	if err != nil {
		return err
	}
	defer closeClient()

	upperdir, err := r.activeOverlayUpperdir(ctx, client, info)
	if err != nil {
		return err
	}
	target := r.rootFSBaselineStatePath(info, baselineLayerID)
	parent := filepath.Dir(target)
	if err := os.MkdirAll(parent, 0o700); err != nil {
		return fmt.Errorf("create rootfs baseline parent: %w", err)
	}
	tmp, err := os.MkdirTemp(parent, ".baseline-*")
	if err != nil {
		return fmt.Errorf("create rootfs baseline temp dir: %w", err)
	}
	removeTmp := true
	defer func() {
		if removeTmp {
			_ = os.RemoveAll(tmp)
		}
	}()
	upperTmp := filepath.Join(tmp, "upper")
	if err := os.MkdirAll(upperTmp, 0o700); err != nil {
		return fmt.Errorf("create rootfs baseline upper directory: %w", err)
	}
	if err := fs.CopyDir(upperTmp, upperdir); err != nil {
		return fmt.Errorf("copy rootfs baseline: %w", err)
	}
	if err := newRootFSPathFilter(rootFSExcludedPathsWithPortals(excludedPaths, portalPaths)).RemoveAll(upperTmp); err != nil {
		return fmt.Errorf("filter rootfs baseline: %w", err)
	}
	if fileSizes != nil {
		indexTmp, err := writeRootFSFileSizeIndexTemp(tmp, fileSizes)
		if err == nil {
			if err := os.Rename(indexTmp, rootFSFileSizeIndexPath(tmp)); err != nil {
				_ = os.Remove(indexTmp)
			}
		}
	}
	if err := os.RemoveAll(target); err != nil {
		return fmt.Errorf("replace rootfs baseline: %w", err)
	}
	if err := os.Rename(tmp, target); err != nil {
		return fmt.Errorf("publish rootfs baseline: %w", err)
	}
	removeTmp = false
	return nil
}

func rootFSDiffReaderFromContent(ctx context.Context, client containerdClient, desc ocispec.Descriptor, excludedPaths []string, portalPaths []ctldapi.RootFSPortalPath) (ctldapi.RootFSDiffDescriptor, io.ReadSeekCloser, bool, error) {
	rootDesc := descriptorFromOCI(desc)
	reader, err := content.BlobReadSeeker(ctx, client.ContentStore(), desc)
	if err != nil {
		return ctldapi.RootFSDiffDescriptor{}, nil, false, err
	}
	if !shouldFilterRootFSDiffTar(rootDesc) {
		return rootDesc, reader, true, nil
	}

	filteredDesc, filteredReader, err := filterRootFSDiffTarForSave(rootDesc, reader, excludedPaths, portalPaths)
	closeErr := reader.Close()
	if err != nil {
		return ctldapi.RootFSDiffDescriptor{}, nil, false, err
	}
	if closeErr != nil {
		_ = filteredReader.Close()
		return ctldapi.RootFSDiffDescriptor{}, nil, false, closeErr
	}
	return filteredDesc, filteredReader, false, nil
}

func (r *ContainerdRuntime) rootFSBaselineStatePath(info ctldapi.RootFSInfo, baselineLayerID string) string {
	return filepath.Join(r.rootFSBaselineRoot(), "baselines", "v2", rootFSBaselineKey(info, baselineLayerID))
}

func (r *ContainerdRuntime) rootFSLegacyBaselinePath(info ctldapi.RootFSInfo, baselineLayerID string) string {
	return filepath.Join(r.rootFSBaselineRoot(), "baselines", rootFSBaselineKey(info, baselineLayerID))
}

func (r *ContainerdRuntime) rootFSBaselineRoot() string {
	root := defaultRootFSCacheDir
	if r != nil && strings.TrimSpace(r.rootFSCacheDir) != "" {
		root = r.rootFSCacheDir
	}
	return root
}

func rootFSBaselineKey(info ctldapi.RootFSInfo, baselineLayerID string) string {
	sum := sha256.Sum256([]byte(strings.Join([]string{
		info.PodNamespace,
		info.PodName,
		info.PodUID,
		info.ContainerName,
		info.ContainerID,
		strings.TrimSpace(baselineLayerID),
	}, "\x00")))
	return hex.EncodeToString(sum[:])
}

func (r *ContainerdRuntime) resolveContainerID(ctx context.Context, target ctldapi.RootFSContainerRef) (string, string, error) {
	client, err := r.runtimeClient(ctx)
	if err != nil {
		return "", "", err
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

// ListPodSandboxStats returns one bulk node-local CRI stats snapshot.
func (r *ContainerdRuntime) ListPodSandboxStats(ctx context.Context) ([]*runtimeapi.PodSandboxStats, error) {
	client, err := r.runtimeClient(ctx)
	if err != nil {
		return nil, err
	}
	resp, err := client.ListPodSandboxStats(ctx, &runtimeapi.ListPodSandboxStatsRequest{})
	if err != nil {
		return nil, fmt.Errorf("list CRI pod sandbox stats: %w", err)
	}
	return resp.GetStats(), nil
}

// Close releases the cached CRI connection. Injected CRI clients are not owned
// by ContainerdRuntime and are left open.
func (r *ContainerdRuntime) Close() error {
	if r == nil {
		return nil
	}
	r.criMu.Lock()
	conn := r.criConn
	r.criConn = nil
	r.connectedCRIClient = nil
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
	endpoint := defaultCRIEndpoint
	if strings.TrimSpace(r.criEndpoint) != "" {
		endpoint = r.criEndpoint
	}
	timeout := defaultDialTimeout
	if r.dialTimeout > 0 {
		timeout = r.dialTimeout
	}
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
	return r.connectedCRIClient, nil
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

func liveRootFSPath(containerdRoot, containerdHostRoot, namespace string, info ctldapi.RootFSInfo) (string, error) {
	taskRoot := filepath.Join(containerdRoot, "io.containerd.runtime.v2.task", namespace)
	hostTaskRoot := filepath.Join(containerdHostRoot, "io.containerd.runtime.v2.task", namespace)
	if id := strings.TrimSpace(info.ContainerID); id != "" {
		liveRootFS := filepath.Join(taskRoot, id, "rootfs")
		if st, err := os.Stat(liveRootFS); err == nil && st.IsDir() {
			return filepath.Join(hostTaskRoot, id, "rootfs"), nil
		} else if err != nil && !os.IsNotExist(err) {
			return "", fmt.Errorf("inspect live rootfs %s: %w", liveRootFS, err)
		}
	}

	liveRootFS, err := findLiveRootFSByTaskAnnotations(taskRoot, hostTaskRoot, info)
	if err == nil {
		return liveRootFS, nil
	}
	return "", err
}

func findLiveRootFSByTaskAnnotations(taskRoot, hostTaskRoot string, info ctldapi.RootFSInfo) (string, error) {
	entries, err := os.ReadDir(taskRoot)
	if err != nil {
		return "", fmt.Errorf("scan containerd task root %s: %w", taskRoot, err)
	}
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		taskDir := filepath.Join(taskRoot, entry.Name())
		raw, err := os.ReadFile(filepath.Join(taskDir, "config.json"))
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return "", fmt.Errorf("read task config %s: %w", taskDir, err)
		}
		var spec struct {
			Annotations map[string]string `json:"annotations"`
		}
		if err := json.Unmarshal(raw, &spec); err != nil {
			return "", fmt.Errorf("parse task config %s: %w", taskDir, err)
		}
		if !rootFSTaskMatches(spec.Annotations, info) {
			continue
		}
		liveRootFS := filepath.Join(taskDir, "rootfs")
		if st, err := os.Stat(liveRootFS); err == nil && st.IsDir() {
			return filepath.Join(hostTaskRoot, entry.Name(), "rootfs"), nil
		} else if err != nil && !os.IsNotExist(err) {
			return "", fmt.Errorf("inspect live rootfs %s: %w", liveRootFS, err)
		}
	}
	return "", fmt.Errorf("%w: live rootfs for container %s in pod %s/%s", ErrNotFound, info.ContainerName, info.PodNamespace, info.PodName)
}

func rootFSTaskMatches(annotations map[string]string, info ctldapi.RootFSInfo) bool {
	if annotations == nil {
		return false
	}
	if annotations["io.kubernetes.cri.container-type"] != "container" {
		return false
	}
	if annotations["io.kubernetes.cri.container-name"] != info.ContainerName {
		return false
	}
	if annotations["io.kubernetes.cri.sandbox-namespace"] != info.PodNamespace {
		return false
	}
	if annotations["io.kubernetes.cri.sandbox-name"] != info.PodName {
		return false
	}
	if info.PodUID != "" && annotations["io.kubernetes.cri.sandbox-uid"] != info.PodUID {
		return false
	}
	return true
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
