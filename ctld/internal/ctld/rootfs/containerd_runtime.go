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
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
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
}

type s0fsRootFSMount struct {
	key                string
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
}

type liveRootFSPaths struct {
	mountedPath        string
	hostPath           string
	mountNamespacePath string
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
		s0fsMounts:             make(map[string]*s0fsRootFSMount),
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

func (r *ContainerdRuntime) CreateDiff(ctx context.Context, info ctldapi.RootFSInfo, excludedPaths []string, portalPaths []ctldapi.RootFSPortalPath) (ctldapi.RootFSDiffDescriptor, io.ReadSeekCloser, error) {
	if strings.TrimSpace(info.SnapshotKey) == "" || strings.TrimSpace(info.Snapshotter) == "" {
		return ctldapi.RootFSDiffDescriptor{}, nil, fmt.Errorf("%w: snapshot key and snapshotter are required", ErrBadRequest)
	}
	client, closeClient, err := r.client(ctx)
	if err != nil {
		return ctldapi.RootFSDiffDescriptor{}, nil, err
	}

	if desc, reader, ok, fastErr := r.createOverlayUpperDiff(ctx, client, info, excludedPaths, portalPaths); ok && fastErr == nil {
		closeClient()
		return desc, reader, nil
	} else if ok && fastErr != nil {
		desc, err := crootfs.CreateDiff(ctx, info.SnapshotKey, client.SnapshotService(info.Snapshotter), client.DiffService())
		if err != nil {
			closeClient()
			return ctldapi.RootFSDiffDescriptor{}, nil, fmt.Errorf("overlayfs fast diff: %v; containerd diff: %w", fastErr, err)
		}
		rootDesc, reader, needsClient, err := rootFSDiffReaderFromContent(ctx, client, desc, excludedPaths, portalPaths)
		if err != nil {
			closeClient()
			return ctldapi.RootFSDiffDescriptor{}, nil, err
		}
		if !needsClient {
			closeClient()
			return rootDesc, reader, nil
		}
		return rootDesc, closeReadSeekWithFunc{ReadSeekCloser: reader, closeFunc: closeClient}, nil
	}

	desc, err := crootfs.CreateDiff(ctx, info.SnapshotKey, client.SnapshotService(info.Snapshotter), client.DiffService())
	if err != nil {
		closeClient()
		return ctldapi.RootFSDiffDescriptor{}, nil, err
	}
	rootDesc, reader, needsClient, err := rootFSDiffReaderFromContent(ctx, client, desc, excludedPaths, portalPaths)
	if err != nil {
		closeClient()
		return ctldapi.RootFSDiffDescriptor{}, nil, err
	}
	if !needsClient {
		closeClient()
		return rootDesc, reader, nil
	}
	return rootDesc, closeReadSeekWithFunc{ReadSeekCloser: reader, closeFunc: closeClient}, nil
}

func (r *ContainerdRuntime) CreateDiffFromBaseline(ctx context.Context, info ctldapi.RootFSInfo, baselineLayerID string, excludedPaths []string, portalPaths []ctldapi.RootFSPortalPath) (ctldapi.RootFSDiffDescriptor, io.ReadSeekCloser, error) {
	if strings.TrimSpace(baselineLayerID) == "" {
		return ctldapi.RootFSDiffDescriptor{}, nil, fmt.Errorf("%w: baseline layer id is required", ErrBadRequest)
	}
	client, closeClient, err := r.client(ctx)
	if err != nil {
		return ctldapi.RootFSDiffDescriptor{}, nil, err
	}
	defer closeClient()

	upperdir, err := r.activeOverlayUpperdir(ctx, client, info)
	if err != nil {
		return ctldapi.RootFSDiffDescriptor{}, nil, err
	}
	baselineDir := r.rootFSBaselinePath(info, baselineLayerID)
	if st, err := os.Stat(baselineDir); err != nil {
		if os.IsNotExist(err) {
			return ctldapi.RootFSDiffDescriptor{}, nil, fmt.Errorf("%w: rootfs baseline %s is not captured", ErrNotFound, baselineLayerID)
		}
		return ctldapi.RootFSDiffDescriptor{}, nil, fmt.Errorf("inspect rootfs baseline: %w", err)
	} else if !st.IsDir() {
		return ctldapi.RootFSDiffDescriptor{}, nil, fmt.Errorf("%w: rootfs baseline path is not a directory", ErrConflict)
	}
	return writeOverlayUpperDiffFromBaseline(ctx, baselineDir, upperdir, excludedPaths, portalPaths)
}

func (r *ContainerdRuntime) ApplyDiff(ctx context.Context, info ctldapi.RootFSInfo, desc ctldapi.RootFSDiffDescriptor, reader io.Reader, excludedPaths []string, portalPaths []ctldapi.RootFSPortalPath) (ctldapi.RootFSDiffDescriptor, error) {
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
	filteredDesc, filteredReader, err := filterRootFSDiffTarForApply(desc, reader, excludedPaths, portalPaths)
	if err != nil {
		return ctldapi.RootFSDiffDescriptor{}, fmt.Errorf("filter rootfs diff: %w", err)
	}
	defer filteredReader.Close()
	desc = filteredDesc
	reader = filteredReader

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

func (r *ContainerdRuntime) CaptureBaseline(ctx context.Context, info ctldapi.RootFSInfo, baselineLayerID string, excludedPaths []string, portalPaths []ctldapi.RootFSPortalPath) error {
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
	target := r.rootFSBaselinePath(info, baselineLayerID)
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
	if err := fs.CopyDir(tmp, upperdir); err != nil {
		return fmt.Errorf("copy rootfs baseline: %w", err)
	}
	if err := newRootFSPathFilter(rootFSExcludedPathsWithPortals(excludedPaths, portalPaths)).RemoveAll(tmp); err != nil {
		return fmt.Errorf("filter rootfs baseline: %w", err)
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
	liveRootFS, err := mountedLiveRootFSPath(r.containerdRoot, r.containerdHostRoot, r.namespace, req.Info)
	if err != nil {
		return ctldapi.RootFSHeadDescriptor{}, err
	}
	state, err := s0fs.ImportHostTree(ctx, liveRootFS, s0fs.HostImportOptions{
		Base:          base,
		ExcludedPaths: req.ExcludedPaths,
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
	volumeID := strings.TrimSpace(req.FilesystemID)
	if volumeID == "" {
		volumeID = strings.TrimSpace(req.Head.FilesystemID)
	}
	if volumeID == "" {
		volumeID = strings.TrimSpace(req.Head.VolumeID)
	}
	engine, err := r.openRootFSS0FSEngine(ctx, req.Store, teamID, volumeID)
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
	head, err := rootFSS0FSHeadFromLoadedManifest(teamID, volumeID, sourceManifest)
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
	containerMountPath := defaultRootFSUserMountPath
	hostMountPath := filepath.Join(liveRootFS.mountedPath, strings.TrimPrefix(containerMountPath, "/"))
	if err := os.MkdirAll(hostMountPath, 0o755); err != nil {
		_ = engine.Close()
		return ctldapi.RootFSHeadDescriptor{}, "", fmt.Errorf("create s0fs rootfs mountpoint: %w", err)
	}
	mountKey := rootFSS0FSMountKey(req.Info)
	if old := r.takeS0FSMount(req.Info); old != nil {
		_ = old.close()
	}
	session := portal.NewS0FSSession(volumeID, teamID, engine, nil)
	server, err := mountS0FSRootFS(session, containerMountPath, liveRootFS.mountNamespacePath, liveRootFS.mountedPath)
	if err != nil {
		_ = engine.Close()
		return ctldapi.RootFSHeadDescriptor{}, "", err
	}
	active := &s0fsRootFSMount{
		key:                mountKey,
		volumeID:           volumeID,
		teamID:             teamID,
		hostMountPath:      hostMountPath,
		containerMountPath: containerMountPath,
		engine:             engine,
		server:             server,
		session:            session,
		mountNamespacePath: liveRootFS.mountNamespacePath,
		mountRootPath:      liveRootFS.mountedPath,
		mountPath:          containerMountPath,
	}
	r.s0fsMu.Lock()
	r.s0fsMounts[mountKey] = active
	r.s0fsMu.Unlock()

	return head, containerMountPath, nil
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
	if m.server != nil {
		_ = unmountS0FSRootFS(m.server, m.mountNamespacePath, m.mountRootPath, m.mountPath)
	}
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
	delete(r.s0fsMounts, key)
	return active
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
		VolumeID:    volumeID,
		WALPath:     filepath.Join(cacheDir, "engine.wal"),
		ObjectStore: rootStore,
		ObjectStoreForVolume: func(sourceVolumeID string) (objectstore.Store, error) {
			return rootFSS0FSObjectStore(store, teamID, sourceVolumeID), nil
		},
	})
}

func loadRootFSS0FSHead(ctx context.Context, store objectstore.Store, head ctldapi.RootFSHeadDescriptor) (*s0fs.SnapshotState, *s0fs.Manifest, error) {
	if err := validateRootFSHeadDescriptor(head); err != nil {
		return nil, nil, err
	}
	materializer := s0fs.NewMaterializer(head.VolumeID, rootFSS0FSObjectStore(store, head.TeamID, head.VolumeID), nil)
	manifest, err := materializer.LoadManifestByKey(ctx, head.ManifestKey)
	if err != nil {
		return nil, nil, err
	}
	return manifest.State, manifest, nil
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
	mountFn := func() error {
		var err error
		if mountNamespacePath != "" && mountRootPath != "" {
			server, err = mountFuseServerInMountNamespace(fs, targetPath, mountNamespacePath, mountRootPath, opts)
		} else {
			server, err = fuse.NewServer(fs, targetPath, opts)
		}
		if err != nil {
			return fmt.Errorf("mount s0fs rootfs: %w", err)
		}
		go server.Serve()
		if err := server.WaitMount(); err != nil {
			_ = unmountS0FSRootFS(server, mountNamespacePath, mountRootPath, targetPath)
			return fmt.Errorf("wait for s0fs rootfs mount: %w", err)
		}
		return nil
	}
	if err := mountFn(); err != nil {
		return nil, err
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

func rootFSS0FSHeadFromManifest(teamID, volumeID string, manifest *s0fs.Manifest) (ctldapi.RootFSHeadDescriptor, error) {
	return rootFSS0FSHeadFromLoadedManifest(teamID, volumeID, manifest)
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

func (r *ContainerdRuntime) rootFSBaselinePath(info ctldapi.RootFSInfo, baselineLayerID string) string {
	root := defaultRootFSCacheDir
	if r != nil && strings.TrimSpace(r.rootFSCacheDir) != "" {
		root = r.rootFSCacheDir
	}
	sum := sha256.Sum256([]byte(strings.Join([]string{
		info.PodNamespace,
		info.PodName,
		info.PodUID,
		info.ContainerName,
		info.ContainerID,
		strings.TrimSpace(baselineLayerID),
	}, "\x00")))
	return filepath.Join(root, "baselines", hex.EncodeToString(sum[:]))
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
	if st, err := os.Stat(mountedRootFS); err == nil && st.IsDir() {
		return liveRootFSPaths{
			mountedPath:        mountedRootFS,
			hostPath:           filepath.Join(hostTaskDir, "rootfs"),
			mountNamespacePath: taskMountNamespacePath(taskDir),
		}, true, nil
	} else if err != nil && !os.IsNotExist(err) {
		return liveRootFSPaths{}, false, fmt.Errorf("inspect live rootfs %s: %w", mountedRootFS, err)
	}
	if pidRootFS, nsPath := taskProcessRootFSPath(taskDir); pidRootFS != "" {
		return liveRootFSPaths{
			mountedPath:        pidRootFS,
			hostPath:           pidRootFS,
			mountNamespacePath: nsPath,
		}, true, nil
	}
	return liveRootFSPaths{}, false, nil
}

func taskMountNamespacePath(taskDir string) string {
	_, nsPath := taskProcessRootFSPath(taskDir)
	return nsPath
}

func taskProcessRootFSPath(taskDir string) (string, string) {
	raw, err := os.ReadFile(filepath.Join(taskDir, "init.pid"))
	if err != nil {
		return "", ""
	}
	pid := strings.TrimSpace(string(raw))
	if pid == "" {
		return "", ""
	}
	for _, r := range pid {
		if r < '0' || r > '9' {
			return "", ""
		}
	}
	procRoot := filepath.Join("/proc", pid)
	return filepath.Join(procRoot, "root"), filepath.Join(procRoot, "ns", "mnt")
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
