package portal

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/ctldapi"
	"github.com/sandbox0-ai/sandbox0/pkg/naming"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/objectstore"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/s0fs"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/volume"
	"go.uber.org/zap"
	"golang.org/x/sys/unix"
)

const (
	defaultSandboxRootFSSubdir     = "rootfs"
	defaultSandboxRootFSVolumeName = "sandbox-rootfs"
	kubeletEmptyDirVolumeRoot      = "/var/lib/kubelet/pods"
	containerdTaskRoot             = "/run/containerd/io.containerd.runtime.v2.task/k8s.io"
)

var (
	mountOverlayRootFSForBind = mountOverlayRootFS
	unmountRootFSForRelease   = unmountRootFS
)

// BindSandboxRootFS prepares the node-local mutable upperdir owner for one
// sandbox runtime generation. The immutable base image is identified here but
// not stored in s0fs.
func (m *Manager) BindSandboxRootFS(ctx context.Context, req ctldapi.BindSandboxRootFSRequest) (ctldapi.BindSandboxRootFSResponse, error) {
	if err := ctx.Err(); err != nil {
		return ctldapi.BindSandboxRootFSResponse{}, err
	}
	req.FilesystemID = strings.TrimSpace(req.FilesystemID)
	req.TeamID = strings.TrimSpace(req.TeamID)
	req.SandboxID = strings.TrimSpace(req.SandboxID)
	req.PodUID = strings.TrimSpace(req.PodUID)
	if req.FilesystemID == "" || req.TeamID == "" || req.SandboxID == "" || req.PodUID == "" {
		return ctldapi.BindSandboxRootFSResponse{}, fmt.Errorf("filesystem_id, team_id, sandbox_id and pod_uid are required")
	}
	if req.RuntimeGeneration <= 0 {
		return ctldapi.BindSandboxRootFSResponse{}, fmt.Errorf("runtime_generation is required")
	}

	key := rootFSKey(req.FilesystemID)
	m.mu.Lock()
	if existing := m.rootfs[key]; existing != nil {
		if sameRootFSOwner(existing, req) {
			resp := rootFSBindResponse(existing)
			m.mu.Unlock()
			return resp, nil
		}
		conflict := fmt.Errorf("sandbox rootfs %s already bound to sandbox %s generation %d", req.FilesystemID, existing.sandboxID, existing.runtimeGeneration)
		m.mu.Unlock()
		return ctldapi.BindSandboxRootFSResponse{}, conflict
	}
	m.mu.Unlock()

	cacheDir := filepath.Join(m.rootDir, defaultSandboxRootFSSubdir, safePath(req.TeamID), safePath(req.FilesystemID))
	if err := os.MkdirAll(cacheDir, 0o700); err != nil {
		return ctldapi.BindSandboxRootFSResponse{}, fmt.Errorf("create rootfs cache dir: %w", err)
	}
	paths, err := sandboxRootFSPaths(req, cacheDir)
	if err != nil {
		return ctldapi.BindSandboxRootFSResponse{}, err
	}
	if err := os.MkdirAll(paths.targetHostPath, 0o755); err != nil {
		return ctldapi.BindSandboxRootFSResponse{}, fmt.Errorf("create rootfs mount point: %w", err)
	}

	engine, err := m.openRootFSEngine(ctx, req.TeamID, req.FilesystemID, cacheDir)
	if err != nil {
		return ctldapi.BindSandboxRootFSResponse{}, err
	}
	if err := restoreRootFSUpperDir(ctx, engine, paths.upperDir); err != nil {
		_ = engine.Close()
		return ctldapi.BindSandboxRootFSResponse{}, err
	}
	if err := mountOverlayRootFSForBind(paths.baseRootPath, paths.upperDir, paths.workDir, paths.targetHostPath); err != nil {
		_ = engine.Close()
		return ctldapi.BindSandboxRootFSResponse{}, err
	}
	mount := &rootFSMount{
		filesystemID:      req.FilesystemID,
		teamID:            req.TeamID,
		sandboxID:         req.SandboxID,
		podUID:            req.PodUID,
		runtimeGeneration: req.RuntimeGeneration,
		baseImageRef:      strings.TrimSpace(req.BaseImageRef),
		baseImageDigest:   strings.TrimSpace(req.BaseImageDigest),
		mountPoint:        paths.mountPoint,
		targetHostPath:    paths.targetHostPath,
		baseRootPath:      paths.baseRootPath,
		cacheDir:          cacheDir,
		upperDir:          paths.upperDir,
		workDir:           paths.workDir,
		mountedAt:         time.Now().UTC(),
		s0fs:              engine,
		overlayMounted:    true,
	}

	m.mu.Lock()
	if existing := m.rootfs[key]; existing != nil {
		resp := rootFSBindResponse(existing)
		m.mu.Unlock()
		_ = unmountRootFSForRelease(paths.targetHostPath)
		_ = engine.Close()
		if !sameRootFSOwner(existing, req) {
			return ctldapi.BindSandboxRootFSResponse{}, fmt.Errorf("sandbox rootfs %s already bound to sandbox %s generation %d", req.FilesystemID, existing.sandboxID, existing.runtimeGeneration)
		}
		return resp, nil
	}
	m.rootfs[key] = mount
	m.startRootFSMaterializer(mount)
	resp := rootFSBindResponse(mount)
	m.mu.Unlock()
	return resp, nil
}

func (m *Manager) FlushSandboxRootFS(ctx context.Context, req ctldapi.FlushSandboxRootFSRequest) (ctldapi.FlushSandboxRootFSResponse, error) {
	if err := ctx.Err(); err != nil {
		return ctldapi.FlushSandboxRootFSResponse{}, err
	}
	filesystemID := strings.TrimSpace(req.FilesystemID)
	if filesystemID == "" {
		return ctldapi.FlushSandboxRootFSResponse{}, fmt.Errorf("filesystem_id is required")
	}

	m.mu.Lock()
	mount := m.rootfs[rootFSKey(filesystemID)]
	if mount != nil && !sameRootFSFlushOwner(mount, req) {
		err := fmt.Errorf("sandbox rootfs %s is bound to sandbox %s generation %d", filesystemID, mount.sandboxID, mount.runtimeGeneration)
		m.mu.Unlock()
		return ctldapi.FlushSandboxRootFSResponse{}, err
	}
	if mount != nil {
		s0 := mount.s0fs
		m.mu.Unlock()
		if s0 == nil {
			return ctldapi.FlushSandboxRootFSResponse{Flushed: true, FilesystemID: filesystemID}, nil
		}
		if err := syncRootFSUpperToS0FS(ctx, s0, mount.upperDir); err != nil {
			return ctldapi.FlushSandboxRootFSResponse{}, fmt.Errorf("sync sandbox rootfs upperdir: %w", err)
		}
		if _, err := s0.SyncMaterialize(ctx); err != nil {
			return ctldapi.FlushSandboxRootFSResponse{}, fmt.Errorf("materialize sandbox rootfs: %w", err)
		}
		return ctldapi.FlushSandboxRootFSResponse{Flushed: true, FilesystemID: filesystemID}, nil
	}
	m.mu.Unlock()

	teamID := strings.TrimSpace(req.TeamID)
	if teamID == "" {
		return ctldapi.FlushSandboxRootFSResponse{}, fmt.Errorf("team_id is required when sandbox rootfs is not already bound")
	}
	cacheDir := filepath.Join(m.rootDir, defaultSandboxRootFSSubdir, safePath(teamID), safePath(filesystemID))
	if err := os.MkdirAll(cacheDir, 0o700); err != nil {
		return ctldapi.FlushSandboxRootFSResponse{}, fmt.Errorf("create rootfs cache dir: %w", err)
	}
	engine, err := m.openRootFSEngine(ctx, teamID, filesystemID, cacheDir)
	if err != nil {
		return ctldapi.FlushSandboxRootFSResponse{}, err
	}
	defer engine.Close()
	if _, err := engine.SyncMaterialize(ctx); err != nil {
		return ctldapi.FlushSandboxRootFSResponse{}, fmt.Errorf("materialize sandbox rootfs: %w", err)
	}
	return ctldapi.FlushSandboxRootFSResponse{Flushed: true, FilesystemID: filesystemID}, nil
}

func (m *Manager) ReleaseSandboxRootFS(ctx context.Context, req ctldapi.ReleaseSandboxRootFSRequest) (ctldapi.ReleaseSandboxRootFSResponse, error) {
	if err := ctx.Err(); err != nil {
		return ctldapi.ReleaseSandboxRootFSResponse{}, err
	}
	filesystemID := strings.TrimSpace(req.FilesystemID)
	if filesystemID == "" {
		return ctldapi.ReleaseSandboxRootFSResponse{}, fmt.Errorf("filesystem_id is required")
	}
	key := rootFSKey(filesystemID)

	m.mu.Lock()
	mount := m.rootfs[key]
	if mount == nil {
		m.mu.Unlock()
		return ctldapi.ReleaseSandboxRootFSResponse{Released: true, FilesystemID: filesystemID}, nil
	}
	if !sameRootFSReleaseOwner(mount, req) {
		err := fmt.Errorf("sandbox rootfs %s is bound to sandbox %s generation %d", filesystemID, mount.sandboxID, mount.runtimeGeneration)
		m.mu.Unlock()
		return ctldapi.ReleaseSandboxRootFSResponse{}, err
	}
	m.mu.Unlock()

	if err := m.closeRootFSMount(ctx, mount); err != nil {
		return ctldapi.ReleaseSandboxRootFSResponse{}, err
	}
	m.mu.Lock()
	if m.rootfs[key] == mount {
		delete(m.rootfs, key)
	}
	m.mu.Unlock()
	return ctldapi.ReleaseSandboxRootFSResponse{Released: true, FilesystemID: filesystemID}, nil
}

func (m *Manager) openRootFSEngine(ctx context.Context, teamID, filesystemID, cacheDir string) (*s0fs.Engine, error) {
	remoteStore, err := m.createRootFSObjectStore(teamID, filesystemID)
	if err != nil {
		return nil, fmt.Errorf("create rootfs object storage: %w", err)
	}
	encryption, err := volume.S0FSEncryptionConfig(m.storage)
	if err != nil {
		return nil, err
	}
	segmentTargetSize, err := volume.S0FSSegmentTargetSize(m.storage)
	if err != nil {
		return nil, err
	}
	engine, err := s0fs.Open(ctx, s0fs.Config{
		VolumeID:          filesystemID,
		WALPath:           filepath.Join(cacheDir, "upperdir.wal"),
		ObjectStore:       remoteStore,
		SegmentTargetSize: segmentTargetSize,
		ObjectStoreForVolume: func(sourceID string) (objectstore.Store, error) {
			return m.createRootFSObjectStore(teamID, sourceID)
		},
		HeadStore:      newSandboxFilesystemHeadStore(m.repo),
		Encryption:     encryption,
		LocalDiskGuard: m.localDiskGuard(cacheDir),
	})
	if err != nil {
		return nil, fmt.Errorf("open sandbox rootfs s0fs upperdir: %w", err)
	}
	return engine, nil
}

func (m *Manager) createRootFSObjectStore(teamID, filesystemID string) (objectstore.Store, error) {
	if m == nil || m.storage == nil || strings.TrimSpace(m.storage.S3Bucket) == "" {
		return nil, nil
	}
	store, err := objectstore.Create(objectstore.Config{
		Type:         m.storage.ObjectStorageType,
		Bucket:       m.storage.S3Bucket,
		Region:       m.storage.S3Region,
		Endpoint:     m.storage.S3Endpoint,
		AccessKey:    m.storage.S3AccessKey,
		SecretKey:    m.storage.S3SecretKey,
		SessionToken: m.storage.S3SessionToken,
	})
	if err != nil {
		return nil, err
	}
	prefix, err := naming.S3VolumePrefix(teamID, filesystemID)
	if err != nil {
		return nil, err
	}
	return objectstore.Prefix(store, prefix+"/s0fs/"), nil
}

func (m *Manager) startRootFSMaterializer(mount *rootFSMount) {
	if m == nil || mount == nil || mount.s0fs == nil {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	mount.materializeCancel = cancel
	mount.materializeDone = done
	go func(filesystemID string) {
		defer close(done)
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := syncRootFSUpperToS0FS(ctx, mount.s0fs, mount.upperDir); err != nil && m.logger != nil {
					m.logger.Warn("ctld sandbox rootfs upperdir sync failed", zap.String("filesystem_id", filesystemID), zap.Error(err))
					continue
				}
				if _, err := mount.s0fs.SyncMaterialize(ctx); err != nil && m.logger != nil {
					m.logger.Warn("ctld sandbox rootfs materialize failed", zap.String("filesystem_id", filesystemID), zap.Error(err))
				}
			}
		}
	}(mount.filesystemID)
}

func (m *Manager) closeRootFSMount(ctx context.Context, mount *rootFSMount) error {
	if mount == nil {
		return nil
	}
	if mount.materializeCancel != nil {
		mount.materializeCancel()
		mount.materializeCancel = nil
	}
	if mount.materializeDone != nil {
		<-mount.materializeDone
		mount.materializeDone = nil
	}
	if mount.s0fs != nil {
		if err := syncRootFSUpperToS0FS(ctx, mount.s0fs, mount.upperDir); err != nil {
			return fmt.Errorf("sync sandbox rootfs upperdir: %w", err)
		}
		if _, err := mount.s0fs.SyncMaterialize(ctx); err != nil {
			return fmt.Errorf("materialize sandbox rootfs: %w", err)
		}
		if mount.overlayMounted {
			if err := unmountRootFSForRelease(mount.targetHostPath); err != nil {
				return err
			}
			mount.overlayMounted = false
		}
		if err := mount.s0fs.Close(); err != nil {
			return err
		}
		mount.s0fs = nil
	}
	return nil
}

type rootFSPaths struct {
	mountPoint     string
	targetHostPath string
	baseRootPath   string
	upperDir       string
	workDir        string
}

func sandboxRootFSPaths(req ctldapi.BindSandboxRootFSRequest, cacheDir string) (rootFSPaths, error) {
	generation := fmt.Sprintf("%d", req.RuntimeGeneration)
	paths := rootFSPaths{
		mountPoint: strings.TrimSpace(req.TargetPath),
		upperDir:   filepath.Join(cacheDir, "runtime", generation, "upper"),
		workDir:    filepath.Join(cacheDir, "runtime", generation, "work"),
	}
	if paths.mountPoint == "" {
		paths.mountPoint = "/sandbox0/rootfs"
	}
	paths.targetHostPath = strings.TrimSpace(req.TargetHostPath)
	if paths.targetHostPath == "" {
		volumeName := strings.TrimSpace(req.RootFSVolumeName)
		if volumeName == "" {
			volumeName = defaultSandboxRootFSVolumeName
		}
		if strings.TrimSpace(req.PodUID) == "" {
			return rootFSPaths{}, fmt.Errorf("pod_uid is required to resolve sandbox rootfs host path")
		}
		paths.targetHostPath = filepath.Join(kubeletEmptyDirVolumeRoot, safePath(req.PodUID), "volumes", "kubernetes.io~empty-dir", safePath(volumeName))
	}
	paths.baseRootPath = strings.TrimSpace(req.BaseRootPath)
	if paths.baseRootPath == "" {
		paths.baseRootPath = containerRootFSPath(req.ContainerID)
	}
	if paths.baseRootPath == "" {
		return rootFSPaths{}, fmt.Errorf("container_id or base_root_path is required to bind sandbox rootfs")
	}
	return paths, nil
}

func containerRootFSPath(containerID string) string {
	id := strings.TrimSpace(containerID)
	if id == "" {
		return ""
	}
	if idx := strings.Index(id, "://"); idx >= 0 {
		id = id[idx+len("://"):]
	}
	id = safePath(id)
	if id == "" {
		return ""
	}
	return filepath.Join(containerdTaskRoot, id, "rootfs")
}

func mountOverlayRootFS(lowerDir, upperDir, workDir, targetPath string) error {
	if strings.TrimSpace(lowerDir) == "" || strings.TrimSpace(upperDir) == "" || strings.TrimSpace(workDir) == "" || strings.TrimSpace(targetPath) == "" {
		return fmt.Errorf("lowerdir, upperdir, workdir and target path are required")
	}
	if info, err := os.Stat(lowerDir); err != nil {
		return fmt.Errorf("stat sandbox rootfs lowerdir: %w", err)
	} else if !info.IsDir() {
		return fmt.Errorf("sandbox rootfs lowerdir %s is not a directory", lowerDir)
	}
	for _, dir := range []string{upperDir, targetPath} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return fmt.Errorf("create rootfs directory %s: %w", dir, err)
		}
	}
	if err := resetDirectory(workDir, 0o755); err != nil {
		return fmt.Errorf("reset sandbox rootfs workdir: %w", err)
	}
	options := fmt.Sprintf("lowerdir=%s,upperdir=%s,workdir=%s", lowerDir, upperDir, workDir)
	if err := unix.Mount("overlay", targetPath, "overlay", 0, options); err != nil {
		return fmt.Errorf("mount sandbox rootfs overlay: %w", err)
	}
	return nil
}

func unmountRootFS(targetPath string) error {
	targetPath = strings.TrimSpace(targetPath)
	if targetPath == "" {
		return nil
	}
	if err := unix.Unmount(targetPath, unix.MNT_DETACH); err != nil {
		if errors.Is(err, unix.EINVAL) || errors.Is(err, unix.ENOENT) {
			return nil
		}
		return fmt.Errorf("unmount sandbox rootfs overlay: %w", err)
	}
	return nil
}

func rootFSKey(filesystemID string) string {
	return strings.TrimSpace(filesystemID)
}

func sameRootFSOwner(existing *rootFSMount, req ctldapi.BindSandboxRootFSRequest) bool {
	if existing == nil {
		return false
	}
	return existing.filesystemID == strings.TrimSpace(req.FilesystemID) &&
		existing.sandboxID == strings.TrimSpace(req.SandboxID) &&
		existing.podUID == strings.TrimSpace(req.PodUID) &&
		existing.runtimeGeneration == req.RuntimeGeneration
}

func sameRootFSFlushOwner(existing *rootFSMount, req ctldapi.FlushSandboxRootFSRequest) bool {
	if existing == nil {
		return false
	}
	sandboxID := strings.TrimSpace(req.SandboxID)
	if sandboxID != "" && existing.sandboxID != sandboxID {
		return false
	}
	podUID := strings.TrimSpace(req.PodUID)
	if podUID != "" && existing.podUID != podUID {
		return false
	}
	return req.RuntimeGeneration <= 0 || existing.runtimeGeneration == req.RuntimeGeneration
}

func sameRootFSReleaseOwner(existing *rootFSMount, req ctldapi.ReleaseSandboxRootFSRequest) bool {
	if existing == nil {
		return false
	}
	sandboxID := strings.TrimSpace(req.SandboxID)
	if sandboxID != "" && existing.sandboxID != sandboxID {
		return false
	}
	podUID := strings.TrimSpace(req.PodUID)
	if podUID != "" && existing.podUID != podUID {
		return false
	}
	return req.RuntimeGeneration <= 0 || existing.runtimeGeneration == req.RuntimeGeneration
}

func rootFSBindResponse(mount *rootFSMount) ctldapi.BindSandboxRootFSResponse {
	if mount == nil {
		return ctldapi.BindSandboxRootFSResponse{}
	}
	return ctldapi.BindSandboxRootFSResponse{
		FilesystemID: mount.filesystemID,
		MountPoint:   mount.mountPoint,
		MountedAt:    mount.mountedAt.Format(time.RFC3339),
	}
}
