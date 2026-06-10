package rootfs

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	containerd "github.com/containerd/containerd/v2/client"
	"github.com/containerd/containerd/v2/core/content"
	"github.com/containerd/containerd/v2/core/images"
	"github.com/containerd/containerd/v2/core/mount"
	"github.com/containerd/containerd/v2/core/snapshots"
	"github.com/containerd/containerd/v2/pkg/archive"
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
	defaultCRIEndpoint             = "/host-run/containerd/containerd.sock"
	defaultContainerdEndpoint      = "/host-run/containerd/containerd.sock"
	defaultContainerdRoot          = "/host-run/containerd"
	defaultContainerdHostRoot      = "/run/containerd"
	defaultContainerdStateRoot     = "/host-var-lib/containerd"
	defaultContainerdStateHostRoot = "/var/lib/containerd"
	defaultNamespace               = "k8s.io"
	defaultDialTimeout             = 10 * time.Second
)

var errOverlayUpperDiffUnavailable = errors.New("overlay upperdir diff unavailable")

type criRuntimeService interface {
	ListContainers(ctx context.Context, in *runtimeapi.ListContainersRequest, opts ...grpc.CallOption) (*runtimeapi.ListContainersResponse, error)
}

type ContainerdRuntimeConfig struct {
	CRIEndpoint             string
	ContainerdEndpoint      string
	ContainerdRoot          string
	ContainerdHostRoot      string
	ContainerdStateRoot     string
	ContainerdStateHostRoot string
	Namespace               string
	DialTimeout             time.Duration
	CRIClient               criRuntimeService
	CRIDialContext          func(ctx context.Context, endpoint string) (*grpc.ClientConn, error)
	ContainerdClient        containerdClient
}

type ContainerdRuntime struct {
	criEndpoint             string
	containerdEndpoint      string
	containerdRoot          string
	containerdHostRoot      string
	containerdStateRoot     string
	containerdStateHostRoot string
	namespace               string
	dialTimeout             time.Duration
	criClient               criRuntimeService
	criDialContext          func(ctx context.Context, endpoint string) (*grpc.ClientConn, error)
	containerdClient        containerdClient
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
	containerdStateRoot := strings.TrimSpace(cfg.ContainerdStateRoot)
	if containerdStateRoot == "" {
		containerdStateRoot = defaultContainerdStateRoot
	}
	containerdStateHostRoot := strings.TrimSpace(cfg.ContainerdStateHostRoot)
	if containerdStateHostRoot == "" {
		containerdStateHostRoot = defaultContainerdStateHostRoot
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
		criEndpoint:             criEndpoint,
		containerdEndpoint:      containerdEndpoint,
		containerdRoot:          containerdRoot,
		containerdHostRoot:      containerdHostRoot,
		containerdStateRoot:     containerdStateRoot,
		containerdStateHostRoot: containerdStateHostRoot,
		namespace:               namespace,
		dialTimeout:             timeout,
		criClient:               cfg.CRIClient,
		criDialContext:          cfg.CRIDialContext,
		containerdClient:        cfg.ContainerdClient,
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

	if desc, reader, err := r.createOverlayUpperDiff(ctx, client, info); err == nil {
		closeClient()
		return desc, reader, nil
	} else if ctxErr := ctx.Err(); ctxErr != nil {
		closeClient()
		return ctldapi.RootFSDiffDescriptor{}, nil, ctxErr
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

	liveRootFS, err := liveRootFSPath(r.containerdRoot, r.containerdHostRoot, r.namespace, info)
	if err != nil {
		return ctldapi.RootFSDiffDescriptor{}, err
	}

	ociDesc, err := descriptorToOCI(desc)
	if err != nil {
		return ctldapi.RootFSDiffDescriptor{}, err
	}
	ref := "sandbox0-rootfs-apply-" + strings.ReplaceAll(ociDesc.Digest.String(), ":", "-")
	if err := content.WriteBlob(ctx, client.ContentStore(), ref, reader, ociDesc); err != nil {
		return ctldapi.RootFSDiffDescriptor{}, fmt.Errorf("write rootfs diff into containerd content store: %w", err)
	}
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

func (r *ContainerdRuntime) createOverlayUpperDiff(ctx context.Context, client containerdClient, info ctldapi.RootFSInfo) (ctldapi.RootFSDiffDescriptor, io.ReadSeekCloser, error) {
	snapshotter := client.SnapshotService(info.Snapshotter)
	if snapshotter == nil {
		return ctldapi.RootFSDiffDescriptor{}, nil, errOverlayUpperDiffUnavailable
	}
	activeMounts, err := snapshotter.Mounts(ctx, info.SnapshotKey)
	if err != nil {
		return ctldapi.RootFSDiffDescriptor{}, nil, fmt.Errorf("inspect active snapshot mounts: %w", err)
	}
	activeMounts = mapMountsToLocalHostPaths(activeMounts, r.hostPathMappings())
	upperdir, ok := overlayUpperDir(activeMounts)
	if !ok {
		return ctldapi.RootFSDiffDescriptor{}, nil, errOverlayUpperDiffUnavailable
	}
	if st, err := os.Stat(upperdir); err != nil {
		return ctldapi.RootFSDiffDescriptor{}, nil, fmt.Errorf("inspect overlay upperdir %s: %w", upperdir, err)
	} else if !st.IsDir() {
		return ctldapi.RootFSDiffDescriptor{}, nil, fmt.Errorf("overlay upperdir %s is not a directory", upperdir)
	}

	snapInfo, err := snapshotter.Stat(ctx, info.SnapshotKey)
	if err != nil {
		return ctldapi.RootFSDiffDescriptor{}, nil, fmt.Errorf("inspect active snapshot: %w", err)
	}
	parent := strings.TrimSpace(snapInfo.Parent)
	if parent == "" {
		return ctldapi.RootFSDiffDescriptor{}, nil, errOverlayUpperDiffUnavailable
	}
	viewKey := newRootFSParentViewKey()
	parentMounts, err := snapshotter.View(ctx, viewKey, parent)
	if err != nil {
		return ctldapi.RootFSDiffDescriptor{}, nil, fmt.Errorf("create parent snapshot view: %w", err)
	}
	defer func() { _ = snapshotter.Remove(ctx, viewKey) }()

	parentMounts = mapMountsToLocalHostPaths(parentMounts, r.hostPathMappings())
	var desc ctldapi.RootFSDiffDescriptor
	var reader io.ReadSeekCloser
	err = mount.WithReadonlyTempMount(ctx, parentMounts, func(lowerRoot string) error {
		var diffErr error
		desc, reader, diffErr = createOverlayUpperDiffTar(ctx, lowerRoot, upperdir)
		return diffErr
	})
	if err != nil {
		return ctldapi.RootFSDiffDescriptor{}, nil, err
	}
	return desc, reader, nil
}

func createOverlayUpperDiffTar(ctx context.Context, lowerRoot, upperdir string) (_ ctldapi.RootFSDiffDescriptor, _ io.ReadSeekCloser, retErr error) {
	tmp, err := os.CreateTemp("", "sandbox0-rootfs-diff-*.tar")
	if err != nil {
		return ctldapi.RootFSDiffDescriptor{}, nil, err
	}
	defer func() {
		if retErr != nil {
			_ = tmp.Close()
			_ = os.Remove(tmp.Name())
		}
	}()

	digester := digest.Canonical.Digester()
	counter := &countingWriter{w: io.MultiWriter(tmp, digester.Hash())}
	cw := archive.NewChangeWriter(counter, upperdir)
	diffErr := fs.DiffDirChanges(ctx, lowerRoot, upperdir, fs.DiffSourceOverlayFS, cw.HandleChange)
	closeErr := cw.Close()
	if diffErr != nil {
		return ctldapi.RootFSDiffDescriptor{}, nil, fmt.Errorf("create overlay upperdir diff: %w", diffErr)
	}
	if closeErr != nil {
		return ctldapi.RootFSDiffDescriptor{}, nil, fmt.Errorf("close overlay upperdir diff: %w", closeErr)
	}
	if _, err := tmp.Seek(0, io.SeekStart); err != nil {
		return ctldapi.RootFSDiffDescriptor{}, nil, err
	}
	return ctldapi.RootFSDiffDescriptor{
		MediaType: ocispec.MediaTypeImageLayer,
		Digest:    digester.Digest().String(),
		Size:      counter.n,
	}, tempFileReadSeekCloser{File: tmp, path: tmp.Name()}, nil
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

func overlayUpperDir(mounts []mount.Mount) (string, bool) {
	for _, m := range mounts {
		if m.Type != "overlay" {
			continue
		}
		for _, opt := range m.Options {
			key, value, ok := strings.Cut(opt, "=")
			if ok && key == "upperdir" && strings.TrimSpace(value) != "" {
				return value, true
			}
		}
	}
	return "", false
}

type hostPathMapping struct {
	hostRoot  string
	localRoot string
}

func (r *ContainerdRuntime) hostPathMappings() []hostPathMapping {
	return []hostPathMapping{
		{hostRoot: r.containerdHostRoot, localRoot: r.containerdRoot},
		{hostRoot: r.containerdStateHostRoot, localRoot: r.containerdStateRoot},
	}
}

func mapMountsToLocalHostPaths(mounts []mount.Mount, mappings []hostPathMapping) []mount.Mount {
	if len(mounts) == 0 {
		return nil
	}
	mapped := make([]mount.Mount, 0, len(mounts))
	for _, m := range mounts {
		clone := m
		clone.Source = mapHostPathToLocalRoot(clone.Source, mappings)
		if len(m.Options) > 0 {
			clone.Options = make([]string, 0, len(m.Options))
			for _, opt := range m.Options {
				clone.Options = append(clone.Options, mapMountOptionToLocalRoot(opt, mappings))
			}
		}
		mapped = append(mapped, clone)
	}
	return mapped
}

func mapMountOptionToLocalRoot(opt string, mappings []hostPathMapping) string {
	key, value, ok := strings.Cut(opt, "=")
	if !ok {
		return opt
	}
	switch key {
	case "lowerdir":
		parts := strings.Split(value, ":")
		for i := range parts {
			parts[i] = mapHostPathToLocalRoot(parts[i], mappings)
		}
		return key + "=" + strings.Join(parts, ":")
	case "upperdir", "workdir":
		return key + "=" + mapHostPathToLocalRoot(value, mappings)
	default:
		return opt
	}
}

func mapHostPathToLocalRoot(pathValue string, mappings []hostPathMapping) string {
	pathValue = strings.TrimSpace(pathValue)
	if pathValue == "" {
		return pathValue
	}
	cleanPath := filepath.Clean(pathValue)
	for _, mapping := range mappings {
		if strings.TrimSpace(mapping.hostRoot) == "" || strings.TrimSpace(mapping.localRoot) == "" {
			continue
		}
		hostRoot := filepath.Clean(mapping.hostRoot)
		localRoot := filepath.Clean(mapping.localRoot)
		if cleanPath == hostRoot {
			return localRoot
		}
		prefix := hostRoot + string(filepath.Separator)
		if strings.HasPrefix(cleanPath, prefix) {
			return filepath.Join(localRoot, strings.TrimPrefix(cleanPath, prefix))
		}
	}
	return pathValue
}

func newRootFSParentViewKey() string {
	var b [8]byte
	if _, err := rand.Read(b[:]); err == nil {
		return "sandbox0-rootfs-parent-view-" + hex.EncodeToString(b[:])
	}
	return fmt.Sprintf("sandbox0-rootfs-parent-view-%d", time.Now().UnixNano())
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

type countingWriter struct {
	w io.Writer
	n int64
}

func (w *countingWriter) Write(p []byte) (int, error) {
	n, err := w.w.Write(p)
	w.n += int64(n)
	return n, err
}

type tempFileReadSeekCloser struct {
	*os.File
	path string
}

func (r tempFileReadSeekCloser) Close() error {
	err := r.File.Close()
	if removeErr := os.Remove(r.path); err == nil {
		err = removeErr
	}
	return err
}
