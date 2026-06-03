package portal

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/ctldapi"
	"github.com/sandbox0-ai/sandbox0/pkg/volumeportal"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/db"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/objectstore"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/s0fs"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/volume"
	"go.uber.org/zap"
)

func (m *Manager) BindRootfs(ctx context.Context, req ctldapi.BindRootfsRequest) (ctldapi.BindRootfsResponse, error) {
	if err := ctx.Err(); err != nil {
		return ctldapi.BindRootfsResponse{}, err
	}
	portalName := volumeportal.NormalizePortalName(req.PortalName, req.MountPath)
	if portalName == "" {
		return ctldapi.BindRootfsResponse{}, fmt.Errorf("rootfs portal name or mount path is required")
	}
	filesystemID := strings.TrimSpace(req.SandboxFilesystemID)
	if req.PodUID == "" || filesystemID == "" || req.TeamID == "" {
		return ctldapi.BindRootfsResponse{}, fmt.Errorf("pod_uid, sandboxfilesystem_id and team_id are required")
	}
	filesystemRecord, err := m.validateBindableRootfs(ctx, filesystemID, req.TeamID)
	if err != nil {
		return ctldapi.BindRootfsResponse{}, err
	}

	key := portalKey(req.PodUID, portalName)
	m.mu.Lock()
	pm := m.portals[key]
	if pm == nil {
		m.mu.Unlock()
		return ctldapi.BindRootfsResponse{}, fmt.Errorf("rootfs portal %s for pod %s is not published", portalName, req.PodUID)
	}
	if pm.rootfsID != "" && pm.rootfsID != filesystemID {
		response := rootfsResponse(pm)
		m.mu.Unlock()
		return ctldapi.BindRootfsResponse{}, fmt.Errorf("rootfs portal already bound to %s", response.SandboxFilesystemID)
	}
	if pm.rootfsID == filesystemID {
		response := rootfsResponse(pm)
		m.mu.Unlock()
		return response, nil
	}
	if existing := m.boundRootfs[filesystemID]; existing != nil {
		m.mu.Unlock()
		return ctldapi.BindRootfsResponse{}, fmt.Errorf("filesystem %s already has an active owner", filesystemID)
	}
	m.mu.Unlock()

	mountedAt := time.Now().UTC()
	cacheDir := filepath.Join(m.rootDir, "rootfs", safePath(req.TeamID), safePath(filesystemID))
	if err := os.MkdirAll(cacheDir, 0o700); err != nil {
		return ctldapi.BindRootfsResponse{}, fmt.Errorf("create local rootfs dir: %w", err)
	}
	remoteStore, err := m.createFilesystemObjectStore(req.TeamID, filesystemID)
	if err != nil {
		return ctldapi.BindRootfsResponse{}, fmt.Errorf("create filesystem object storage: %w", err)
	}
	encryption, err := volume.S0FSEncryptionConfig(m.storage)
	if err != nil {
		return ctldapi.BindRootfsResponse{}, err
	}
	segmentTargetSize, err := volume.S0FSSegmentTargetSize(m.storage)
	if err != nil {
		return ctldapi.BindRootfsResponse{}, err
	}
	engine, err := s0fs.Open(ctx, s0fs.Config{
		VolumeID:          filesystemID,
		WALPath:           filepath.Join(cacheDir, "engine.wal"),
		ObjectStore:       remoteStore,
		SegmentTargetSize: segmentTargetSize,
		ObjectStoreForVolume: func(sourceFilesystemID string) (objectstore.Store, error) {
			return m.createFilesystemObjectStore(req.TeamID, sourceFilesystemID)
		},
		HeadStore:      db.NewSandboxFilesystemS0FSHeadStore(m.repo),
		Encryption:     encryption,
		LocalDiskGuard: m.localDiskGuard(cacheDir),
	})
	if err != nil {
		return ctldapi.BindRootfsResponse{}, fmt.Errorf("open local rootfs s0fs engine: %w", err)
	}
	volCtx := &volume.VolumeContext{
		VolumeID:  filesystemID,
		TeamID:    filesystemRecord.TeamID,
		Backend:   volume.BackendS0FS,
		S0FS:      engine,
		Access:    volume.AccessModeRWO,
		MountedAt: mountedAt,
		RootInode: 1,
		RootPath:  "/",
		CacheDir:  cacheDir,
	}

	m.mu.Lock()
	pm = m.portals[key]
	if pm == nil {
		m.mu.Unlock()
		_ = engine.Close()
		return ctldapi.BindRootfsResponse{}, fmt.Errorf("rootfs portal %s for pod %s is not published", portalName, req.PodUID)
	}
	if pm.rootfsID != "" {
		response := rootfsResponse(pm)
		m.mu.Unlock()
		_ = engine.Close()
		if response.SandboxFilesystemID != filesystemID {
			return ctldapi.BindRootfsResponse{}, fmt.Errorf("rootfs portal already bound to %s", response.SandboxFilesystemID)
		}
		return response, nil
	}
	if existing := m.boundRootfs[filesystemID]; existing != nil {
		m.mu.Unlock()
		_ = engine.Close()
		return ctldapi.BindRootfsResponse{}, fmt.Errorf("filesystem %s already has an active owner", filesystemID)
	}
	bound := &boundRootfs{
		filesystemID: filesystemID,
		teamID:       filesystemRecord.TeamID,
		mountedAt:    mountedAt,
		volCtx:       volCtx,
	}
	m.boundRootfs[filesystemID] = bound
	m.filesystems.add(volCtx)
	m.attachRootfsPortalLocked(pm, filesystemID, filesystemRecord.TeamID, mountedAt)
	if err := m.registerRootfsOwner(ctx, bound); err != nil {
		clearRootfsPortalLocked(pm)
		delete(m.boundRootfs, filesystemID)
		m.filesystems.remove(filesystemID)
		m.mu.Unlock()
		_ = engine.Close()
		return ctldapi.BindRootfsResponse{}, fmt.Errorf("register ctld filesystem owner: %w", err)
	}
	m.startRootfsMaterializer(bound)
	response := rootfsResponse(pm)
	m.mu.Unlock()

	return response, nil
}

func (m *Manager) CommitRootfs(ctx context.Context, req ctldapi.CommitRootfsRequest) (ctldapi.CommitRootfsResponse, error) {
	if err := ctx.Err(); err != nil {
		return ctldapi.CommitRootfsResponse{}, err
	}
	bound, err := m.boundRootfsForRequest(req.PodUID, req.PortalName, req.MountPath, req.SandboxFilesystemID)
	if err != nil {
		return ctldapi.CommitRootfsResponse{}, err
	}
	head, err := commitRootfs(ctx, bound)
	if err != nil {
		return ctldapi.CommitRootfsResponse{}, err
	}
	return ctldapi.CommitRootfsResponse{
		SandboxFilesystemID: bound.filesystemID,
		S0FSHead:            head,
		Committed:           true,
	}, nil
}

func (m *Manager) UnbindRootfs(ctx context.Context, req ctldapi.UnbindRootfsRequest) (ctldapi.UnbindRootfsResponse, error) {
	if err := ctx.Err(); err != nil {
		return ctldapi.UnbindRootfsResponse{}, err
	}
	portalName := volumeportal.NormalizePortalName(req.PortalName, req.MountPath)
	if req.PodUID == "" || portalName == "" {
		return ctldapi.UnbindRootfsResponse{}, fmt.Errorf("pod_uid and rootfs portal identity are required")
	}
	key := portalKey(req.PodUID, portalName)

	m.mu.Lock()
	pm := m.portals[key]
	if pm == nil || pm.rootfsID == "" {
		m.mu.Unlock()
		return ctldapi.UnbindRootfsResponse{Unbound: true}, nil
	}
	if req.SandboxFilesystemID != "" && pm.rootfsID != req.SandboxFilesystemID {
		m.mu.Unlock()
		return ctldapi.UnbindRootfsResponse{}, fmt.Errorf("rootfs portal is not bound to filesystem %s", req.SandboxFilesystemID)
	}
	filesystemID := pm.rootfsID
	head, err := m.unbindRootfsLocked(ctx, pm)
	m.mu.Unlock()
	if err != nil {
		return ctldapi.UnbindRootfsResponse{}, err
	}
	return ctldapi.UnbindRootfsResponse{
		SandboxFilesystemID: filesystemID,
		S0FSHead:            head,
		Unbound:             true,
	}, nil
}

func (m *Manager) boundRootfsForRequest(podUID, portalName, mountPath, filesystemID string) (*boundRootfs, error) {
	portalName = volumeportal.NormalizePortalName(portalName, mountPath)
	if podUID == "" || portalName == "" {
		return nil, fmt.Errorf("pod_uid and rootfs portal identity are required")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	pm := m.portals[portalKey(podUID, portalName)]
	if pm == nil || pm.rootfsID == "" {
		return nil, fmt.Errorf("rootfs portal %s for pod %s is not bound", portalName, podUID)
	}
	if filesystemID != "" && pm.rootfsID != filesystemID {
		return nil, fmt.Errorf("rootfs portal is not bound to filesystem %s", filesystemID)
	}
	bound := m.boundRootfs[pm.rootfsID]
	if bound == nil {
		return nil, fmt.Errorf("filesystem %s is not bound", pm.rootfsID)
	}
	return bound, nil
}

func (m *Manager) attachRootfsPortalLocked(pm *portalMount, filesystemID, teamID string, mountedAt time.Time) {
	if pm == nil {
		return
	}
	if pm.fs != nil {
		pm.fs.SetSession(newLocalSession(filesystemID, m.filesystems, m.logrus))
	}
	pm.rootfsID = filesystemID
	pm.teamID = teamID
	pm.mountedAt = mountedAt
}

func (m *Manager) unbindRootfsLocked(ctx context.Context, pm *portalMount) (string, error) {
	if pm == nil || pm.rootfsID == "" {
		return "", nil
	}
	filesystemID := pm.rootfsID
	bound := m.boundRootfs[filesystemID]
	if bound == nil {
		clearRootfsPortalLocked(pm)
		return "", nil
	}
	m.stopRootfsMaterializer(bound)
	head, err := commitRootfs(ctx, bound)
	if err != nil {
		m.startRootfsMaterializer(bound)
		return "", err
	}
	if err := m.filesystems.UnmountVolume(ctx, filesystemID, ""); err != nil {
		m.startRootfsMaterializer(bound)
		return "", err
	}
	clearRootfsPortalLocked(pm)
	delete(m.boundRootfs, filesystemID)
	m.unregisterRootfsOwner(bound)
	return head, nil
}

func clearRootfsPortalLocked(pm *portalMount) {
	if pm == nil {
		return
	}
	if pm.fs != nil {
		pm.fs.SetSession(unboundSession{})
	}
	pm.rootfsID = ""
	pm.teamID = ""
	pm.mountedAt = time.Time{}
}

func commitRootfs(ctx context.Context, bound *boundRootfs) (string, error) {
	if bound == nil || bound.volCtx == nil || bound.volCtx.S0FS == nil {
		return "", fmt.Errorf("rootfs is not bound")
	}
	manifest, err := bound.volCtx.S0FS.EnsureMaterialized(ctx)
	if err != nil {
		return "", err
	}
	if manifest == nil || manifest.ManifestSeq == 0 {
		return "", nil
	}
	return rootfsManifestKey(manifest.ManifestSeq), nil
}

func (m *Manager) startRootfsMaterializer(bound *boundRootfs) {
	if bound == nil || bound.volCtx == nil || bound.volCtx.S0FS == nil || bound.materializeCancel != nil {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	bound.materializeCancel = cancel
	bound.materializeDone = done
	go func(filesystemID string) {
		defer close(done)
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if _, err := bound.volCtx.S0FS.SyncMaterialize(ctx); err != nil && m.logger != nil {
					m.logger.Warn("ctld filesystem materialize failed", zap.String("filesystem_id", filesystemID), zap.Error(err))
				}
			}
		}
	}(bound.filesystemID)
}

func (m *Manager) stopRootfsMaterializer(bound *boundRootfs) {
	if bound == nil {
		return
	}
	if bound.materializeCancel != nil {
		bound.materializeCancel()
		bound.materializeCancel = nil
	}
	if bound.materializeDone != nil {
		<-bound.materializeDone
		bound.materializeDone = nil
	}
}

func rootfsResponse(pm *portalMount) ctldapi.BindRootfsResponse {
	if pm == nil {
		return ctldapi.BindRootfsResponse{}
	}
	return ctldapi.BindRootfsResponse{
		SandboxFilesystemID: pm.rootfsID,
		RootPath:            pm.mountPath,
		MountedAt:           pm.mountedAt.Format(time.RFC3339),
	}
}

func rootfsManifestKey(seq uint64) string {
	return fmt.Sprintf("manifests/%020d.json", seq)
}
