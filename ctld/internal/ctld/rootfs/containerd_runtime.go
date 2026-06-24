package rootfs

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	containerd "github.com/containerd/containerd/v2/client"
	"github.com/containerd/containerd/v2/core/images"
	"github.com/containerd/containerd/v2/core/snapshots"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/opencontainers/go-digest"
	"github.com/sandbox0-ai/sandbox0/ctld/internal/ctld/portal"
	"github.com/sandbox0-ai/sandbox0/pkg/ctldapi"
	"github.com/sandbox0-ai/sandbox0/pkg/s0fs"
	"github.com/sandbox0-ai/sandbox0/pkg/volumefuse"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/objectstore"
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
	defaultRootFSUserMountPath    = "/sandbox0/rootfs"
	defaultNamespace              = "k8s.io"
	defaultDialTimeout            = 10 * time.Second
)

type criRuntimeService interface {
	ListContainers(ctx context.Context, in *runtimeapi.ListContainersRequest, opts ...grpc.CallOption) (*runtimeapi.ListContainersResponse, error)
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
	containerdClient       containerdClient
	s0fsMu                 sync.Mutex
	s0fsMounts             map[string]*s0fsRootFSMount
	s0fsMountsByPodUID     map[string]*s0fsRootFSMount
}

type s0fsRootFSMount struct {
	mu                 sync.Mutex
	key                string
	podUID             string
	volumeID           string
	teamID             string
	hostMountPath      string
	containerMountPath string
	engine             *s0fs.Engine
	server             *fuse.Server
	session            volumefuse.Session
	mountNamespacePath string
	mountRootPath      string
	mountPath          string
	portalMounts       map[string]string
	runtimeMounts      map[string]string
}

type liveRootFSPaths struct {
	mountedPath        string
	hostPath           string
	mountNamespacePath string
	mountInfoPath      string
}

type containerdClient interface {
	LoadContainer(ctx context.Context, id string) (containerd.Container, error)
	SnapshotService(snapshotterName string) snapshots.Snapshotter
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
		s0fsMounts:             make(map[string]*s0fsRootFSMount),
		s0fsMountsByPodUID:     make(map[string]*s0fsRootFSMount),
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

func (r *ContainerdRuntime) CommitS0FSRootFS(ctx context.Context, req S0FSCommitRequest) (ctldapi.RootFSHeadDescriptor, error) {
	if req.Store == nil {
		return ctldapi.RootFSHeadDescriptor{}, fmt.Errorf("%w: rootfs object store is required", ErrBadRequest)
	}
	volumeID := rootFSS0FSVolumeID(req.FilesystemID, req.ParentHead, req.SandboxID)
	if volumeID == "" {
		return ctldapi.RootFSHeadDescriptor{}, fmt.Errorf("%w: rootfs filesystem id is required", ErrBadRequest)
	}
	teamID := strings.TrimSpace(req.TeamID)
	if active := r.takeS0FSMount(req.Info); active != nil {
		return commitActiveS0FSMount(ctx, active)
	}

	engine, err := r.openRootFSS0FSEngine(ctx, req.Store, teamID, volumeID)
	if err != nil {
		return ctldapi.RootFSHeadDescriptor{}, err
	}
	defer engine.Close()

	var base *s0fs.SnapshotState
	if !rootFSHeadDescriptorEmpty(req.ParentHead) {
		base, _, err = loadRootFSS0FSHead(ctx, req.Store, req.ParentHead)
		if err != nil {
			return ctldapi.RootFSHeadDescriptor{}, err
		}
	}
	client, closeClient, err := r.client(ctx)
	if err != nil {
		return ctldapi.RootFSHeadDescriptor{}, err
	}
	defer closeClient()
	upperdir, err := r.activeOverlayUpperdir(ctx, client, req.Info)
	if err != nil {
		return ctldapi.RootFSHeadDescriptor{}, err
	}
	liveRootFS, err := liveRootFSPathsForContainer(r.containerdRoot, r.containerdHostRoot, r.namespace, req.Info)
	if err != nil {
		return ctldapi.RootFSHeadDescriptor{}, err
	}
	state, err := s0fs.ImportHostTree(ctx, upperdir, s0fs.HostImportOptions{
		Base:          base,
		ExcludedPaths: rootFSImportExcludedPaths(req.ExcludedPaths, liveRootFS.mountInfoPath),
	})
	if err != nil {
		return ctldapi.RootFSHeadDescriptor{}, err
	}
	if err := engine.ReplaceState(state); err != nil {
		return ctldapi.RootFSHeadDescriptor{}, err
	}
	manifest, err := engine.EnsureMaterialized(ctx)
	if err != nil {
		return ctldapi.RootFSHeadDescriptor{}, err
	}
	return rootFSS0FSHeadFromManifest(teamID, volumeID, manifest)
}

func (r *ContainerdRuntime) AttachS0FSRootFS(ctx context.Context, req S0FSAttachRequest) (ctldapi.RootFSHeadDescriptor, string, error) {
	if req.Store == nil {
		return ctldapi.RootFSHeadDescriptor{}, "", fmt.Errorf("%w: rootfs object store is required", ErrBadRequest)
	}
	if err := validateRootFSHeadDescriptor(req.Head); err != nil {
		return ctldapi.RootFSHeadDescriptor{}, "", err
	}
	teamID := strings.TrimSpace(req.Head.TeamID)
	targetFilesystemID := rootFSS0FSVolumeID(req.FilesystemID, req.Head, "")
	if targetFilesystemID == "" {
		return ctldapi.RootFSHeadDescriptor{}, "", fmt.Errorf("%w: rootfs filesystem id is required", ErrBadRequest)
	}
	engine, err := r.openRootFSS0FSEngine(ctx, req.Store, teamID, targetFilesystemID)
	if err != nil {
		return ctldapi.RootFSHeadDescriptor{}, "", err
	}
	sourceState, sourceManifest, err := loadRootFSS0FSHead(ctx, req.Store, req.Head)
	if err != nil {
		_ = engine.Close()
		return ctldapi.RootFSHeadDescriptor{}, "", err
	}
	if err := engine.ReplaceState(sourceState); err != nil {
		_ = engine.Close()
		return ctldapi.RootFSHeadDescriptor{}, "", err
	}
	head, err := rootFSS0FSHeadFromLoadedManifest(teamID, targetFilesystemID, sourceManifest)
	if err != nil {
		_ = engine.Close()
		return ctldapi.RootFSHeadDescriptor{}, "", err
	}

	liveRootFS, err := liveRootFSPathsForContainer(r.containerdRoot, r.containerdHostRoot, r.namespace, req.Info)
	if err != nil {
		_ = engine.Close()
		return ctldapi.RootFSHeadDescriptor{}, "", err
	}
	if liveRootFS.mountNamespacePath == "" {
		_ = engine.Close()
		return ctldapi.RootFSHeadDescriptor{}, "", fmt.Errorf("%w: live rootfs mount namespace is not available", ErrNotFound)
	}

	mountKey := rootFSS0FSMountKey(req.Info)
	if mountKey == "" {
		_ = engine.Close()
		return ctldapi.RootFSHeadDescriptor{}, "", fmt.Errorf("%w: rootfs mount key is required", ErrBadRequest)
	}
	if old := r.takeS0FSMount(req.Info); old != nil {
		_ = old.close()
	}

	containerMountPath := defaultRootFSUserMountPath
	hostMountPath := filepath.Join(liveRootFS.mountedPath, strings.TrimPrefix(containerMountPath, "/"))
	if err := os.MkdirAll(hostMountPath, 0o755); err != nil {
		_ = engine.Close()
		return ctldapi.RootFSHeadDescriptor{}, "", fmt.Errorf("create s0fs rootfs mountpoint: %w", err)
	}

	runtimeMountPaths := rootFSRuntimeBindMountPaths(rootFSMountInfoExcludedPaths(liveRootFS.mountInfoPath), containerMountPath)
	session := portal.NewS0FSRootFSSession(targetFilesystemID, teamID, engine, liveRootFS.mountedPath, nil)
	server, err := mountS0FSRootFS(session, containerMountPath, liveRootFS.mountNamespacePath, liveRootFS.mountedPath)
	if err != nil {
		session.Close()
		_ = engine.Close()
		return ctldapi.RootFSHeadDescriptor{}, "", err
	}
	runtimeMounts, err := bindRuntimeRootFSMounts(liveRootFS.mountNamespacePath, liveRootFS.mountedPath, containerMountPath, runtimeMountPaths)
	if err != nil {
		_ = unmountS0FSRootFS(server, liveRootFS.mountNamespacePath, liveRootFS.mountedPath, containerMountPath)
		session.Close()
		_ = engine.Close()
		return ctldapi.RootFSHeadDescriptor{}, "", err
	}

	active := &s0fsRootFSMount{
		key:                mountKey,
		podUID:             req.Info.PodUID,
		volumeID:           targetFilesystemID,
		teamID:             teamID,
		hostMountPath:      hostMountPath,
		containerMountPath: containerMountPath,
		engine:             engine,
		server:             server,
		session:            session,
		mountNamespacePath: liveRootFS.mountNamespacePath,
		mountRootPath:      liveRootFS.mountedPath,
		mountPath:          containerMountPath,
		portalMounts:       make(map[string]string),
		runtimeMounts:      runtimeMounts,
	}
	r.s0fsMu.Lock()
	r.s0fsMounts[mountKey] = active
	if strings.TrimSpace(active.podUID) != "" {
		r.s0fsMountsByPodUID[active.podUID] = active
	}
	r.s0fsMu.Unlock()

	return head, containerMountPath, nil
}

func (r *ContainerdRuntime) BindRootFSVolumePortal(ctx context.Context, req portal.RootFSVolumePortalBindRequest) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	active := r.s0fsMountByPodUID(req.PodUID)
	if active == nil {
		return nil
	}
	mountPath := cleanRootFSPath(req.MountPath)
	if mountPath == "/" {
		return fmt.Errorf("%w: rootfs volume portal mount path must be non-root", ErrBadRequest)
	}
	sourcePath, targetPath := active.rootFSVolumePortalPaths(mountPath)
	if sourcePath == "" || targetPath == "" || sourcePath == targetPath {
		return nil
	}

	active.mu.Lock()
	defer active.mu.Unlock()
	if active.portalMounts == nil {
		active.portalMounts = make(map[string]string)
	}
	if active.portalMounts[mountPath] == targetPath {
		return nil
	}
	if err := os.MkdirAll(targetPath, 0o755); err != nil {
		return fmt.Errorf("create rootfs portal mountpoint: %w", err)
	}
	if err := bindMountPathInMountNamespace(active.mountNamespacePath, sourcePath, targetPath); err != nil {
		return err
	}
	active.portalMounts[mountPath] = targetPath
	return nil
}

func (r *ContainerdRuntime) UnbindRootFSVolumePortal(ctx context.Context, req portal.RootFSVolumePortalBindRequest) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	active := r.s0fsMountByPodUID(req.PodUID)
	if active == nil {
		return nil
	}
	mountPath := cleanRootFSPath(req.MountPath)
	if mountPath == "/" {
		return nil
	}

	active.mu.Lock()
	defer active.mu.Unlock()
	targetPath := active.portalMounts[mountPath]
	if targetPath == "" {
		return nil
	}
	err := unmountAbsolutePathInMountNamespace(active.mountNamespacePath, targetPath)
	delete(active.portalMounts, mountPath)
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

func commitActiveS0FSMount(ctx context.Context, active *s0fsRootFSMount) (ctldapi.RootFSHeadDescriptor, error) {
	defer active.close()
	manifest, err := active.engine.EnsureMaterialized(ctx)
	if err != nil {
		return ctldapi.RootFSHeadDescriptor{}, err
	}
	return rootFSS0FSHeadFromManifest(active.teamID, active.volumeID, manifest)
}

func (m *s0fsRootFSMount) close() error {
	if m == nil {
		return nil
	}
	m.mu.Lock()
	for mountPath, targetPath := range m.portalMounts {
		_ = unmountAbsolutePathInMountNamespace(m.mountNamespacePath, targetPath)
		delete(m.portalMounts, mountPath)
	}
	unmountRuntimeRootFSMounts(m.mountNamespacePath, m.mountRootPath, m.runtimeMounts)
	if m.server != nil {
		_ = unmountS0FSRootFS(m.server, m.mountNamespacePath, m.mountRootPath, m.mountPath)
	}
	m.mu.Unlock()
	if m.session != nil {
		m.session.Close()
	}
	if m.engine != nil {
		return m.engine.Close()
	}
	return nil
}

func (r *ContainerdRuntime) takeS0FSMount(info ctldapi.RootFSInfo) *s0fsRootFSMount {
	key := rootFSS0FSMountKey(info)
	if key == "" {
		return nil
	}
	r.s0fsMu.Lock()
	defer r.s0fsMu.Unlock()
	active := r.s0fsMounts[key]
	if active == nil && strings.TrimSpace(info.PodUID) != "" {
		active = r.s0fsMountsByPodUID[strings.TrimSpace(info.PodUID)]
	}
	delete(r.s0fsMounts, key)
	if active != nil && strings.TrimSpace(active.podUID) != "" {
		delete(r.s0fsMountsByPodUID, active.podUID)
	}
	if active != nil && strings.TrimSpace(active.key) != "" {
		delete(r.s0fsMounts, active.key)
	}
	return active
}

func (r *ContainerdRuntime) s0fsMountByPodUID(podUID string) *s0fsRootFSMount {
	podUID = strings.TrimSpace(podUID)
	if podUID == "" {
		return nil
	}
	r.s0fsMu.Lock()
	defer r.s0fsMu.Unlock()
	return r.s0fsMountsByPodUID[podUID]
}

func (r *ContainerdRuntime) openRootFSS0FSEngine(ctx context.Context, store objectstore.Store, teamID, volumeID string) (*s0fs.Engine, error) {
	if strings.TrimSpace(volumeID) == "" {
		return nil, fmt.Errorf("%w: rootfs s0fs volume id is required", ErrBadRequest)
	}
	cacheDir := filepath.Join(r.rootFSCacheDir, "s0fs", safeRootFSPath(teamID), safeRootFSPath(volumeID))
	if err := os.MkdirAll(cacheDir, 0o700); err != nil {
		return nil, fmt.Errorf("create rootfs s0fs cache dir: %w", err)
	}
	rootStore := rootFSS0FSObjectStore(store, teamID, volumeID)
	return s0fs.Open(ctx, s0fs.Config{
		VolumeID:             volumeID,
		WALPath:              filepath.Join(cacheDir, "engine.wal"),
		ObjectStore:          rootStore,
		ObjectStoreForVolume: rootFSS0FSObjectStoreResolver(store, teamID),
	})
}

func loadRootFSS0FSHead(ctx context.Context, store objectstore.Store, head ctldapi.RootFSHeadDescriptor) (*s0fs.SnapshotState, *s0fs.Manifest, error) {
	if err := validateRootFSHeadDescriptor(head); err != nil {
		return nil, nil, err
	}
	materializer := rootFSS0FSMaterializer(store, head.TeamID, head.VolumeID)
	manifest, err := materializer.LoadManifestByKey(ctx, head.ManifestKey)
	if err != nil {
		return nil, nil, err
	}
	return manifest.State, manifest, nil
}

func rootFSS0FSMaterializer(store objectstore.Store, teamID, volumeID string) *s0fs.Materializer {
	return s0fs.NewMaterializer(
		volumeID,
		rootFSS0FSObjectStore(store, teamID, volumeID),
		nil,
		rootFSS0FSObjectStoreResolver(store, teamID),
	)
}

func rootFSS0FSObjectStoreResolver(store objectstore.Store, teamID string) s0fs.ObjectStoreResolver {
	return func(volumeID string) (objectstore.Store, error) {
		return rootFSS0FSObjectStore(store, teamID, volumeID), nil
	}
}

func rootFSS0FSObjectStore(store objectstore.Store, teamID, volumeID string) objectstore.Store {
	return objectstore.Prefix(store, filepath.ToSlash(filepath.Join("rootfs", "s0fs", safeRootFSPath(teamID), safeRootFSPath(volumeID))))
}

func rootFSS0FSVolumeID(filesystemID string, parent ctldapi.RootFSHeadDescriptor, sandboxID string) string {
	if strings.TrimSpace(filesystemID) != "" {
		return strings.TrimSpace(filesystemID)
	}
	if strings.TrimSpace(parent.FilesystemID) != "" {
		return strings.TrimSpace(parent.FilesystemID)
	}
	if strings.TrimSpace(parent.VolumeID) != "" {
		return strings.TrimSpace(parent.VolumeID)
	}
	return strings.TrimSpace(sandboxID)
}

func rootFSS0FSMountKey(info ctldapi.RootFSInfo) string {
	if strings.TrimSpace(info.ContainerID) != "" {
		return strings.TrimSpace(info.ContainerID)
	}
	return strings.TrimSpace(info.PodNamespace) + "/" + strings.TrimSpace(info.PodName) + "/" + strings.TrimSpace(info.ContainerName)
}

func (m *s0fsRootFSMount) rootFSVolumePortalPaths(mountPath string) (string, string) {
	if m == nil {
		return "", ""
	}
	mountPath = cleanRootFSPath(mountPath)
	if mountPath == "/" || m.mountRootPath == "" || m.containerMountPath == "" {
		return "", ""
	}
	relativeMountPath := strings.TrimPrefix(mountPath, "/")
	sourcePath := filepath.Join(m.mountRootPath, relativeMountPath)
	targetPath := filepath.Join(m.mountRootPath, strings.TrimPrefix(rootFSNestedMountPath(m.containerMountPath, mountPath), "/"))
	return sourcePath, targetPath
}

func rootFSNestedMountPath(rootFSMountPath, mountPath string) string {
	rootFSMountPath = cleanRootFSPath(rootFSMountPath)
	mountPath = cleanRootFSPath(mountPath)
	if rootFSMountPath == "/" {
		return mountPath
	}
	if mountPath == "/" {
		return rootFSMountPath
	}
	return filepath.Join(rootFSMountPath, strings.TrimPrefix(mountPath, "/"))
}

func bindRuntimeRootFSMounts(namespacePath, mountRootPath, rootFSMountPath string, mountPaths []string) (map[string]string, error) {
	bound := make(map[string]string)
	for _, mountPath := range mountPaths {
		sourcePath, targetPath := rootFSRuntimeBindPaths(rootFSMountPath, mountPath)
		if sourcePath == "" || targetPath == "" || sourcePath == targetPath {
			continue
		}
		if err := withMountNamespaceRoot(namespacePath, mountRootPath, func() error {
			if err := prepareRuntimeRootFSBindTarget(sourcePath, targetPath); err != nil {
				return err
			}
			return bindMountPathInMountNamespaceRoot("", "", sourcePath, targetPath)
		}); err != nil {
			unmountRuntimeRootFSMounts(namespacePath, mountRootPath, bound)
			return nil, err
		}
		bound[mountPath] = targetPath
	}
	return bound, nil
}

func unmountRuntimeRootFSMounts(namespacePath, mountRootPath string, mounts map[string]string) {
	for _, pair := range sortedBoundMountTargets(mounts) {
		mountPath, targetPath := pair[0], pair[1]
		_ = unmountBindPathInMountNamespaceRoot(namespacePath, mountRootPath, targetPath)
		delete(mounts, mountPath)
	}
}

func rootFSRuntimeBindPaths(rootFSMountPath, mountPath string) (string, string) {
	mountPath = cleanRootFSPath(mountPath)
	rootFSMountPath = cleanRootFSPath(rootFSMountPath)
	if mountPath == "/" || mountPath == rootFSMountPath || strings.HasPrefix(mountPath, rootFSMountPath+"/") {
		return "", ""
	}
	return mountPath, rootFSNestedMountPath(rootFSMountPath, mountPath)
}

func rootFSRuntimeBindMountPaths(paths []string, rootFSMountPath string) []string {
	rootFSMountPath = cleanRootFSPath(rootFSMountPath)
	candidates := make([]string, 0, len(paths))
	seen := make(map[string]struct{}, len(paths))
	for _, value := range paths {
		clean := cleanRootFSPath(value)
		if clean == "/" || clean == rootFSMountPath || strings.HasPrefix(clean, rootFSMountPath+"/") {
			continue
		}
		if _, ok := seen[clean]; ok {
			continue
		}
		seen[clean] = struct{}{}
		candidates = append(candidates, clean)
	}
	sort.Slice(candidates, func(i, j int) bool {
		leftDepth := rootFSPathDepth(candidates[i])
		rightDepth := rootFSPathDepth(candidates[j])
		if leftDepth != rightDepth {
			return leftDepth < rightDepth
		}
		return candidates[i] < candidates[j]
	})
	selected := make([]string, 0, len(candidates))
	for _, candidate := range candidates {
		if rootFSRuntimeBindCovered(candidate, selected) {
			continue
		}
		selected = append(selected, candidate)
	}
	return selected
}

func rootFSRuntimeBindCovered(candidate string, selected []string) bool {
	for _, parent := range selected {
		if candidate == parent || strings.HasPrefix(candidate, parent+"/") {
			return true
		}
	}
	return false
}

func rootFSPathDepth(value string) int {
	value = strings.Trim(strings.TrimSpace(value), "/")
	if value == "" {
		return 0
	}
	return strings.Count(value, "/") + 1
}

func prepareRuntimeRootFSBindTarget(sourcePath, targetPath string) error {
	info, err := os.Stat(sourcePath)
	if err != nil {
		return fmt.Errorf("stat runtime rootfs bind source %s: %w", sourcePath, err)
	}
	if info.IsDir() {
		if err := os.MkdirAll(targetPath, 0o755); err != nil {
			return fmt.Errorf("create runtime rootfs bind target %s: %w", targetPath, err)
		}
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(targetPath), 0o755); err != nil {
		return fmt.Errorf("create runtime rootfs bind target parent %s: %w", filepath.Dir(targetPath), err)
	}
	f, err := os.OpenFile(targetPath, os.O_CREATE|os.O_RDONLY, 0o644)
	if err != nil {
		return fmt.Errorf("create runtime rootfs bind target %s: %w", targetPath, err)
	}
	return f.Close()
}

func sortedBoundMountTargets(mounts map[string]string) [][2]string {
	if len(mounts) == 0 {
		return nil
	}
	pairs := make([][2]string, 0, len(mounts))
	for mountPath, targetPath := range mounts {
		pairs = append(pairs, [2]string{mountPath, targetPath})
	}
	sort.Slice(pairs, func(i, j int) bool {
		leftDepth := rootFSPathDepth(pairs[i][0])
		rightDepth := rootFSPathDepth(pairs[j][0])
		if leftDepth != rightDepth {
			return leftDepth > rightDepth
		}
		return pairs[i][0] > pairs[j][0]
	})
	return pairs
}

func rootFSS0FSHeadFromManifest(teamID, volumeID string, manifest *s0fs.Manifest) (ctldapi.RootFSHeadDescriptor, error) {
	return rootFSS0FSHeadFromLoadedManifest(teamID, volumeID, manifest)
}

func mountS0FSRootFS(session volumefuse.Session, targetPath, mountNamespacePath, mountRootPath string) (*fuse.Server, error) {
	fs := volumefuse.New("sandbox0-rootfs", time.Second, session)
	opts := &fuse.MountOptions{
		FsName:            "sandbox0-rootfs",
		Name:              "sandbox0-rootfs",
		MaxBackground:     128,
		EnableLocks:       true,
		AllowOther:        os.Getuid() == 0,
		DirectMountStrict: true,
		MaxWrite:          256 * 1024,
	}
	var server *fuse.Server
	if mountNamespacePath != "" && mountRootPath != "" {
		var err error
		server, err = mountFuseServerInMountNamespace(fs, targetPath, mountNamespacePath, mountRootPath, opts)
		if err != nil {
			return nil, fmt.Errorf("mount s0fs rootfs: %w", err)
		}
	} else {
		var err error
		server, err = fuse.NewServer(fs, targetPath, opts)
		if err != nil {
			return nil, fmt.Errorf("mount s0fs rootfs: %w", err)
		}
	}
	go server.Serve()
	if err := server.WaitMount(); err != nil {
		_ = unmountS0FSRootFS(server, mountNamespacePath, mountRootPath, targetPath)
		return nil, fmt.Errorf("wait for s0fs rootfs mount: %w", err)
	}
	return server, nil
}

func unmountS0FSRootFS(server *fuse.Server, mountNamespacePath, mountRootPath, targetPath string) error {
	if server == nil {
		return nil
	}
	if mountNamespacePath != "" && mountRootPath != "" && targetPath != "" {
		return unmountPathInMountNamespace(mountNamespacePath, mountRootPath, targetPath)
	}
	return server.Unmount()
}

func rootFSS0FSHeadFromLoadedManifest(teamID, volumeID string, manifest *s0fs.Manifest) (ctldapi.RootFSHeadDescriptor, error) {
	if manifest == nil {
		return ctldapi.RootFSHeadDescriptor{}, fmt.Errorf("s0fs materializer did not return a manifest")
	}
	headVolumeID := volumeID
	if strings.TrimSpace(manifest.VolumeID) != "" {
		headVolumeID = manifest.VolumeID
	}
	return ctldapi.RootFSHeadDescriptor{
		Engine:        ctldapi.RootFSStorageEngineS0FS,
		TeamID:        teamID,
		FilesystemID:  volumeID,
		VolumeID:      headVolumeID,
		ManifestKey:   fmt.Sprintf("manifests/%020d.json", manifest.ManifestSeq),
		ManifestSeq:   manifest.ManifestSeq,
		CheckpointSeq: manifest.CheckpointSeq,
	}, nil
}

func safeRootFSPath(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return "_"
	}
	replacer := strings.NewReplacer("/", "_", "\\", "_", "\x00", "_", "..", "_")
	return replacer.Replace(value)
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
	paths, err := liveRootFSPathsForContainer(containerdRoot, containerdHostRoot, namespace, info)
	if err != nil {
		return "", err
	}
	return paths.hostPath, nil
}

func mountedLiveRootFSPath(containerdRoot, containerdHostRoot, namespace string, info ctldapi.RootFSInfo) (string, error) {
	paths, err := liveRootFSPathsForContainer(containerdRoot, containerdHostRoot, namespace, info)
	if err != nil {
		return "", err
	}
	return paths.mountedPath, nil
}

func liveRootFSPathsForContainer(containerdRoot, containerdHostRoot, namespace string, info ctldapi.RootFSInfo) (liveRootFSPaths, error) {
	taskRoot := filepath.Join(containerdRoot, "io.containerd.runtime.v2.task", namespace)
	hostTaskRoot := filepath.Join(containerdHostRoot, "io.containerd.runtime.v2.task", namespace)
	if id := strings.TrimSpace(info.ContainerID); id != "" {
		taskDir := filepath.Join(taskRoot, id)
		hostTaskDir := filepath.Join(hostTaskRoot, id)
		liveRootFS := filepath.Join(taskDir, "rootfs")
		if paths, ok, err := liveRootFSPathsFromTaskDir(taskDir, hostTaskDir, liveRootFS); err != nil {
			return liveRootFSPaths{}, err
		} else if ok {
			return paths, nil
		}
	}

	liveRootFS, err := findLiveRootFSByTaskAnnotations(taskRoot, hostTaskRoot, info)
	if err == nil {
		return liveRootFS, nil
	}
	return liveRootFSPaths{}, err
}

func liveRootFSPathsFromTaskDir(taskDir, hostTaskDir, mountedRootFS string) (liveRootFSPaths, bool, error) {
	taskRootExists := false
	if st, err := os.Stat(mountedRootFS); err == nil && st.IsDir() {
		taskRootExists = true
	} else if err != nil && !os.IsNotExist(err) {
		return liveRootFSPaths{}, false, fmt.Errorf("inspect live rootfs %s: %w", mountedRootFS, err)
	}
	if pidRootFS, nsPath, mountInfoPath := taskProcessRootFSPath(taskDir); pidRootFS != "" {
		hostPath := pidRootFS
		if taskRootExists {
			hostPath = filepath.Join(hostTaskDir, "rootfs")
		}
		return liveRootFSPaths{
			mountedPath:        pidRootFS,
			hostPath:           hostPath,
			mountNamespacePath: nsPath,
			mountInfoPath:      mountInfoPath,
		}, true, nil
	}
	if taskRootExists {
		return liveRootFSPaths{
			mountedPath:        mountedRootFS,
			hostPath:           filepath.Join(hostTaskDir, "rootfs"),
			mountNamespacePath: "",
		}, true, nil
	}
	return liveRootFSPaths{}, false, nil
}

func taskProcessRootFSPath(taskDir string) (string, string, string) {
	raw, err := os.ReadFile(filepath.Join(taskDir, "init.pid"))
	if err != nil {
		return "", "", ""
	}
	pid := strings.TrimSpace(string(raw))
	if pid == "" {
		return "", "", ""
	}
	for _, r := range pid {
		if r < '0' || r > '9' {
			return "", "", ""
		}
	}
	procRoot := filepath.Join("/proc", pid)
	return filepath.Join(procRoot, "root"), filepath.Join(procRoot, "ns", "mnt"), filepath.Join(procRoot, "mountinfo")
}

func rootFSImportExcludedPaths(extraPaths []string, mountInfoPath string) []string {
	seen := make(map[string]struct{}, len(defaultRootFSSnapshotExcludedPaths)+len(extraPaths))
	out := make([]string, 0, len(defaultRootFSSnapshotExcludedPaths)+len(extraPaths))
	add := func(raw string) {
		clean := cleanRootFSPath(raw)
		if clean == "/" {
			return
		}
		if _, ok := seen[clean]; ok {
			return
		}
		seen[clean] = struct{}{}
		out = append(out, clean)
	}
	for _, value := range defaultRootFSSnapshotExcludedPaths {
		add(value)
	}
	for _, value := range extraPaths {
		add(value)
	}
	for _, value := range rootFSMountInfoExcludedPaths(mountInfoPath) {
		add(value)
	}
	return out
}

func rootFSMountInfoExcludedPaths(mountInfoPath string) []string {
	mountInfoPath = strings.TrimSpace(mountInfoPath)
	if mountInfoPath == "" {
		return nil
	}
	raw, err := os.ReadFile(mountInfoPath)
	if err != nil {
		return nil
	}
	var out []string
	for _, line := range strings.Split(string(raw), "\n") {
		fields := strings.Split(line, " ")
		if len(fields) < 5 {
			continue
		}
		mountPoint := decodeMountInfoPath(fields[4])
		if clean := cleanRootFSPath(mountPoint); clean != "/" {
			out = append(out, clean)
		}
	}
	return out
}

func decodeMountInfoPath(value string) string {
	if !strings.Contains(value, "\\") {
		return value
	}
	var b strings.Builder
	for i := 0; i < len(value); i++ {
		if value[i] == '\\' && i+3 < len(value) {
			if n, err := strconv.ParseUint(value[i+1:i+4], 8, 8); err == nil {
				b.WriteByte(byte(n))
				i += 3
				continue
			}
		}
		b.WriteByte(value[i])
	}
	return b.String()
}

func findLiveRootFSByTaskAnnotations(taskRoot, hostTaskRoot string, info ctldapi.RootFSInfo) (liveRootFSPaths, error) {
	entries, err := os.ReadDir(taskRoot)
	if err != nil {
		return liveRootFSPaths{}, fmt.Errorf("scan containerd task root %s: %w", taskRoot, err)
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
			return liveRootFSPaths{}, fmt.Errorf("read task config %s: %w", taskDir, err)
		}
		var spec struct {
			Annotations map[string]string `json:"annotations"`
		}
		if err := json.Unmarshal(raw, &spec); err != nil {
			return liveRootFSPaths{}, fmt.Errorf("parse task config %s: %w", taskDir, err)
		}
		if !rootFSTaskMatches(spec.Annotations, info) {
			continue
		}
		paths, ok, err := liveRootFSPathsFromTaskDir(taskDir, filepath.Join(hostTaskRoot, entry.Name()), filepath.Join(taskDir, "rootfs"))
		if err != nil {
			return liveRootFSPaths{}, err
		}
		if ok {
			return paths, nil
		}
	}
	return liveRootFSPaths{}, fmt.Errorf("%w: live rootfs for container %s in pod %s/%s", ErrNotFound, info.ContainerName, info.PodNamespace, info.PodName)
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
