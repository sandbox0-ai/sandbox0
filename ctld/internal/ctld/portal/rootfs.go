package portal

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/ctldapi"
	"github.com/sandbox0-ai/sandbox0/pkg/volumefuse"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/db"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/objectstore"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/s0fs"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/volume"
)

func (m *Manager) PrepareRootFS(ctx context.Context, req ctldapi.PrepareRootFSRequest) (ctldapi.PrepareRootFSResponse, error) {
	if m == nil {
		return ctldapi.PrepareRootFSResponse{}, fmt.Errorf("ctld portal manager is not configured")
	}
	if err := ctx.Err(); err != nil {
		return ctldapi.PrepareRootFSResponse{}, err
	}
	sandboxID := strings.TrimSpace(req.SandboxID)
	teamID := strings.TrimSpace(req.TeamID)
	volumeID := strings.TrimSpace(req.RootFSVolumeID)
	if sandboxID == "" || teamID == "" || volumeID == "" {
		return ctldapi.PrepareRootFSResponse{}, fmt.Errorf("sandbox_id, team_id and rootfs_volume_id are required")
	}
	rootDir := strings.TrimSpace(m.rootDir)
	if rootDir == "" {
		rootDir = defaultRootDir
	}

	m.mu.Lock()
	if m.rootfs == nil {
		m.rootfs = make(map[string]*rootfsMount)
	}
	if m.boundVolumes == nil {
		m.boundVolumes = make(map[string]*boundVolume)
	}
	if m.volumes == nil {
		m.volumes = newLocalVolumeManager()
	}
	if existing := m.rootfs[sandboxID]; existing != nil {
		resp := rootfsResponse(existing)
		m.mu.Unlock()
		if existing.volumeID != volumeID {
			return ctldapi.PrepareRootFSResponse{}, fmt.Errorf("sandbox rootfs already prepared with volume %s", existing.volumeID)
		}
		return resp, nil
	}
	if bound := m.boundVolumes[volumeID]; bound != nil {
		m.mu.Unlock()
		return ctldapi.PrepareRootFSResponse{}, fmt.Errorf("rootfs volume %s is already owned by ctld", volumeID)
	}
	m.mu.Unlock()

	volumeRecord, err := m.validateBindableVolume(ctx, ctldBindContext{
		volumeID: volumeID,
		teamID:   teamID,
	})
	if err != nil {
		return ctldapi.PrepareRootFSResponse{}, err
	}
	accessMode, err := validateBindableAccessMode(volumeRecord.AccessMode)
	if err != nil {
		return ctldapi.PrepareRootFSResponse{}, err
	}
	if accessMode != volume.AccessModeRWO {
		return ctldapi.PrepareRootFSResponse{}, fmt.Errorf("rootfs volume %s must use RWO access mode", volumeID)
	}

	baseDir := filepath.Join(rootDir, "rootfs", safePath(teamID), safePath(sandboxID), safePath(volumeID))
	cacheDir := filepath.Join(baseDir, "cache")
	mountPath := filepath.Join(baseDir, "s0fs")
	upperDir := filepath.Join(mountPath, "upper")
	workDir := filepath.Join(mountPath, "work")
	if err := os.MkdirAll(cacheDir, 0o700); err != nil {
		return ctldapi.PrepareRootFSResponse{}, fmt.Errorf("create rootfs cache dir: %w", err)
	}
	if err := os.MkdirAll(mountPath, 0o700); err != nil {
		return ctldapi.PrepareRootFSResponse{}, fmt.Errorf("create rootfs mount dir: %w", err)
	}

	remoteStore, err := m.createObjectStore(teamID, volumeID)
	if err != nil {
		return ctldapi.PrepareRootFSResponse{}, fmt.Errorf("create object storage: %w", err)
	}
	encryption, err := volume.S0FSEncryptionConfig(m.storage)
	if err != nil {
		return ctldapi.PrepareRootFSResponse{}, err
	}
	segmentTargetSize, err := volume.S0FSSegmentTargetSize(m.storage)
	if err != nil {
		return ctldapi.PrepareRootFSResponse{}, err
	}
	engine, err := s0fs.Open(ctx, s0fs.Config{
		VolumeID:          volumeID,
		WALPath:           filepath.Join(cacheDir, "engine.wal"),
		ObjectStore:       remoteStore,
		SegmentTargetSize: segmentTargetSize,
		ObjectStoreForVolume: func(sourceVolumeID string) (objectstore.Store, error) {
			return m.createObjectStore(teamID, sourceVolumeID)
		},
		HeadStore:      db.NewS0FSHeadStore(m.repo),
		Encryption:     encryption,
		LocalDiskGuard: m.localDiskGuard(cacheDir),
	})
	if err != nil {
		return ctldapi.PrepareRootFSResponse{}, fmt.Errorf("open rootfs s0fs engine: %w", err)
	}

	mountedAt := time.Now().UTC()
	volCtx := &volume.VolumeContext{
		VolumeID:  volumeID,
		TeamID:    volumeRecord.TeamID,
		Backend:   volume.BackendS0FS,
		S0FS:      engine,
		Access:    accessMode,
		MountedAt: mountedAt,
		RootInode: 1,
		RootPath:  "/",
		CacheDir:  cacheDir,
	}
	bound := &boundVolume{
		volumeID:  volumeID,
		teamID:    volumeRecord.TeamID,
		access:    accessMode,
		mountedAt: mountedAt,
		refCount:  1,
		volCtx:    volCtx,
	}

	registered := false
	serverMounted := false
	cleanup := func(server fuseMount) {
		if serverMounted && server != nil {
			_ = server.Unmount()
		}
		m.volumes.remove(volumeID)
		if registered {
			m.unregisterOwner(bound)
		}
		_ = engine.Close()
		_ = os.RemoveAll(baseDir)
	}

	if err := m.registerOwner(ctx, bound, "", ""); err != nil {
		cleanup(nil)
		return ctldapi.PrepareRootFSResponse{}, fmt.Errorf("register ctld rootfs owner: %w", err)
	}
	registered = true
	m.volumes.add(volCtx)

	fs := volumefuse.New("rootfs-"+sandboxID, time.Second, newLocalSession(volumeID, m.volumes, m.logrus))
	mounter := m.mountFS
	if mounter == nil {
		mounter = mountPortalFS
	}
	server, err := mounter(fs, mountPath)
	if err != nil {
		cleanup(nil)
		return ctldapi.PrepareRootFSResponse{}, err
	}
	serverMounted = true
	if err := os.MkdirAll(upperDir, 0o755); err != nil {
		cleanup(server)
		return ctldapi.PrepareRootFSResponse{}, fmt.Errorf("create rootfs upperdir: %w", err)
	}
	if err := os.MkdirAll(workDir, 0o755); err != nil {
		cleanup(server)
		return ctldapi.PrepareRootFSResponse{}, fmt.Errorf("create rootfs workdir: %w", err)
	}

	mount := &rootfsMount{
		sandboxID: sandboxID,
		volumeID:  volumeID,
		teamID:    volumeRecord.TeamID,
		cacheDir:  cacheDir,
		mountPath: mountPath,
		upperDir:  upperDir,
		workDir:   workDir,
		fs:        fs,
		server:    server,
		mountedAt: mountedAt,
	}

	m.mu.Lock()
	if m.rootfs == nil {
		m.rootfs = make(map[string]*rootfsMount)
	}
	if m.boundVolumes == nil {
		m.boundVolumes = make(map[string]*boundVolume)
	}
	if existing := m.rootfs[sandboxID]; existing != nil {
		resp := rootfsResponse(existing)
		m.mu.Unlock()
		cleanup(server)
		if existing.volumeID != volumeID {
			return ctldapi.PrepareRootFSResponse{}, fmt.Errorf("sandbox rootfs already prepared with volume %s", existing.volumeID)
		}
		return resp, nil
	}
	if existing := m.boundVolumes[volumeID]; existing != nil {
		m.mu.Unlock()
		cleanup(server)
		return ctldapi.PrepareRootFSResponse{}, fmt.Errorf("rootfs volume %s is already owned by ctld", existing.volumeID)
	}
	m.rootfs[sandboxID] = mount
	m.boundVolumes[volumeID] = bound
	m.startMaterializer(bound)
	resp := rootfsResponse(mount)
	m.mu.Unlock()
	return resp, nil
}

func (m *Manager) CheckpointRootFS(ctx context.Context, req ctldapi.CheckpointRootFSRequest) (ctldapi.CheckpointRootFSResponse, error) {
	if m == nil {
		return ctldapi.CheckpointRootFSResponse{}, fmt.Errorf("ctld portal manager is not configured")
	}
	if err := ctx.Err(); err != nil {
		return ctldapi.CheckpointRootFSResponse{}, err
	}
	sandboxID := strings.TrimSpace(req.SandboxID)
	if sandboxID == "" {
		return ctldapi.CheckpointRootFSResponse{}, fmt.Errorf("sandbox_id is required")
	}
	m.mu.Lock()
	rootfs := m.rootfs[sandboxID]
	var bound *boundVolume
	if rootfs != nil {
		bound = m.boundVolumes[rootfs.volumeID]
	}
	m.mu.Unlock()
	if rootfs == nil || bound == nil || bound.volCtx == nil || bound.volCtx.S0FS == nil {
		return ctldapi.CheckpointRootFSResponse{}, fmt.Errorf("sandbox rootfs %s is not prepared", sandboxID)
	}
	if err := m.volumes.prepareSnapshotCheckpoint(ctx, rootfs.volumeID); err != nil {
		return ctldapi.CheckpointRootFSResponse{}, err
	}
	defer m.volumes.completeSnapshotCheckpoint(rootfs.volumeID)
	if _, err := bound.volCtx.S0FS.SyncMaterialize(ctx); err != nil {
		return ctldapi.CheckpointRootFSResponse{}, err
	}
	return ctldapi.CheckpointRootFSResponse{Checkpointed: true}, nil
}

func (m *Manager) ReleaseRootFS(ctx context.Context, req ctldapi.ReleaseRootFSRequest) (ctldapi.ReleaseRootFSResponse, error) {
	if m == nil {
		return ctldapi.ReleaseRootFSResponse{}, fmt.Errorf("ctld portal manager is not configured")
	}
	if err := ctx.Err(); err != nil {
		return ctldapi.ReleaseRootFSResponse{}, err
	}
	sandboxID := strings.TrimSpace(req.SandboxID)
	if sandboxID == "" {
		return ctldapi.ReleaseRootFSResponse{}, fmt.Errorf("sandbox_id is required")
	}

	m.mu.Lock()
	rootfs := m.rootfs[sandboxID]
	if rootfs == nil {
		m.mu.Unlock()
		return ctldapi.ReleaseRootFSResponse{Released: true}, nil
	}
	bound := m.boundVolumes[rootfs.volumeID]
	if bound != nil {
		if err := m.volumes.prepareSnapshotCheckpoint(ctx, rootfs.volumeID); err != nil {
			m.mu.Unlock()
			return ctldapi.ReleaseRootFSResponse{}, err
		}
	}
	if bound != nil && bound.materializeCancel != nil {
		bound.materializeCancel()
		bound.materializeCancel = nil
	}
	var done chan struct{}
	if bound != nil && bound.materializeDone != nil {
		done = bound.materializeDone
		bound.materializeDone = nil
	}
	if done != nil {
		<-done
	}
	if rootfs.server != nil {
		if err := rootfs.server.Unmount(); err != nil {
			m.volumes.completeSnapshotCheckpoint(rootfs.volumeID)
			m.mu.Unlock()
			return ctldapi.ReleaseRootFSResponse{}, err
		}
	}
	if bound != nil {
		if err := m.volumes.UnmountVolume(ctx, rootfs.volumeID, ""); err != nil {
			m.volumes.completeSnapshotCheckpoint(rootfs.volumeID)
			m.mu.Unlock()
			return ctldapi.ReleaseRootFSResponse{}, err
		}
		m.volumes.completeSnapshotCheckpoint(rootfs.volumeID)
		delete(m.boundVolumes, rootfs.volumeID)
		m.unregisterOwner(bound)
	}
	delete(m.rootfs, sandboxID)
	m.mu.Unlock()
	_ = os.RemoveAll(filepath.Dir(rootfs.cacheDir))
	return ctldapi.ReleaseRootFSResponse{Released: true}, nil
}

func rootfsResponse(rootfs *rootfsMount) ctldapi.PrepareRootFSResponse {
	if rootfs == nil {
		return ctldapi.PrepareRootFSResponse{}
	}
	return ctldapi.PrepareRootFSResponse{
		Prepared:       true,
		SandboxID:      rootfs.sandboxID,
		RootFSVolumeID: rootfs.volumeID,
		MountPoint:     rootfs.mountPath,
		UpperDir:       rootfs.upperDir,
		WorkDir:        rootfs.workDir,
		MountedAt:      rootfs.mountedAt.Format(time.RFC3339),
	}
}
