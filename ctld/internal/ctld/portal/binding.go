package portal

import (
	"context"
	"fmt"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/ctldapi"
	"github.com/sandbox0-ai/sandbox0/pkg/volumeportal"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/db"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/s0fs"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/volume"
)

func (m *Manager) validatePortalBindRequest(
	ctx context.Context,
	req ctldapi.BindVolumePortalRequest,
) (string, *db.SandboxVolume, volume.AccessMode, error) {
	portalName := volumeportal.NormalizePortalName(req.PortalName, req.MountPath)
	if portalName == "" {
		return "", nil, "", fmt.Errorf("portal name or mount path is required")
	}
	if req.PodUID == "" || req.SandboxVolumeID == "" || req.TeamID == "" {
		return "", nil, "", fmt.Errorf("pod_uid, sandboxvolume_id and team_id are required")
	}
	volumeRecord, err := m.validateBindableVolume(ctx, ctldBindContext{
		volumeID: req.SandboxVolumeID,
		teamID:   req.TeamID,
	})
	if err != nil {
		return "", nil, "", err
	}
	accessMode, err := validateBindableAccessMode(volumeRecord.AccessMode)
	if err != nil {
		return "", nil, "", err
	}
	return portalName, volumeRecord, accessMode, nil
}

// reserveBoundVolume pins a shared backend before a portal switches to it.
// The caller must either attach the portal or roll the reservation back.
func (m *Manager) reserveBoundVolume(
	ctx context.Context,
	req ctldapi.BindVolumePortalRequest,
	volumeRecord *db.SandboxVolume,
	accessMode volume.AccessMode,
	exceptPortalKey string,
) (*boundVolume, bool, error) {
	m.mu.Lock()
	if bound := m.boundVolumes[req.SandboxVolumeID]; bound != nil {
		ownerOnly := bound.refCount == 0
		conflictPath := boundMountPath(m.portals, req.SandboxVolumeID, exceptPortalKey)
		if err := validateBoundVolumeReservation(bound, req, accessMode, conflictPath); err != nil {
			m.mu.Unlock()
			return nil, false, err
		}
		if err := m.ensureBoundVolumeSessionLocked(bound); err != nil {
			m.mu.Unlock()
			return nil, false, err
		}
		bound.refCount++
		if ownerOnly && bound.materializeCancel == nil && bound.materializeDone == nil {
			m.startMaterializer(bound)
		}
		m.mu.Unlock()
		return bound, false, nil
	}
	if existing := findBoundPortalForVolume(m.portals, req.SandboxVolumeID, exceptPortalKey); existing != nil {
		conflictPath := existing.mountPath
		m.mu.Unlock()
		return nil, false, fmt.Errorf("volume %s is already bound to %s", req.SandboxVolumeID, conflictPath)
	}
	m.mu.Unlock()

	mountedAt := time.Now().UTC()
	newBound, cleanupNewBound, err := m.openBoundVolume(ctx, req, volumeRecord, accessMode, mountedAt)
	if err != nil {
		return nil, false, err
	}
	m.mu.Lock()
	if bound := m.boundVolumes[req.SandboxVolumeID]; bound != nil {
		ownerOnly := bound.refCount == 0
		conflictPath := boundMountPath(m.portals, req.SandboxVolumeID, exceptPortalKey)
		if err := validateBoundVolumeReservation(bound, req, accessMode, conflictPath); err != nil {
			m.mu.Unlock()
			cleanupNewBound()
			return nil, false, err
		}
		if err := m.ensureBoundVolumeSessionLocked(bound); err != nil {
			m.mu.Unlock()
			cleanupNewBound()
			return nil, false, err
		}
		bound.refCount++
		if ownerOnly && bound.materializeCancel == nil && bound.materializeDone == nil {
			m.startMaterializer(bound)
		}
		m.mu.Unlock()
		cleanupNewBound()
		return bound, false, nil
	}
	m.boundVolumes[req.SandboxVolumeID] = newBound
	m.volumes.add(newBound.volCtx)
	if err := m.registerOwner(ctx, newBound); err != nil {
		delete(m.boundVolumes, req.SandboxVolumeID)
		m.volumes.remove(req.SandboxVolumeID)
		m.mu.Unlock()
		cleanupNewBound()
		return nil, false, fmt.Errorf("register ctld volume owner: %w", err)
	}
	m.startMaterializer(newBound)
	m.mu.Unlock()
	return newBound, true, nil
}

func validateBoundVolumeReservation(bound *boundVolume, req ctldapi.BindVolumePortalRequest, accessMode volume.AccessMode, conflictPath string) error {
	if bound == nil {
		return fmt.Errorf("volume %s binding is unavailable", req.SandboxVolumeID)
	}
	if bound.closing {
		return fmt.Errorf("volume %s is closing", req.SandboxVolumeID)
	}
	if bound.teamID != req.TeamID {
		return fmt.Errorf("volume %s belongs to team %s", req.SandboxVolumeID, bound.teamID)
	}
	if bound.access != accessMode {
		return fmt.Errorf("volume %s access mode changed from %s to %s", req.SandboxVolumeID, bound.access, accessMode)
	}
	if bound.refCount > 0 && accessMode != volume.AccessModeROX {
		return fmt.Errorf("volume %s is already bound to %s", req.SandboxVolumeID, conflictPath)
	}
	return nil
}

// ensureBoundVolumeSessionLocked restores the cheap local session wrapper
// after a failed S0FS cleanup closed it but left the underlying engine mounted.
// S3 sessions carry process-local buffers and cannot be reconstructed here.
func (m *Manager) ensureBoundVolumeSessionLocked(bound *boundVolume) error {
	if bound == nil {
		return fmt.Errorf("bound volume is required")
	}
	if bound.session != nil {
		return nil
	}
	if bound.volCtx == nil || bound.volCtx.S0FS == nil || volume.NormalizeBackend(bound.volCtx.Backend) != volume.BackendS0FS {
		return fmt.Errorf("volume %s session is unavailable", bound.volumeID)
	}
	if m.volumes == nil {
		return fmt.Errorf("volume %s local context is unavailable", bound.volumeID)
	}
	mounted, err := m.volumes.GetVolume(bound.volumeID)
	if err != nil || mounted != bound.volCtx {
		return fmt.Errorf("volume %s local context is unavailable", bound.volumeID)
	}
	rootInode := uint64(bound.volCtx.RootInode)
	if rootInode == 0 {
		rootInode = s0fs.RootInode
	}
	if _, err := bound.volCtx.S0FS.GetAttr(rootInode); err != nil {
		return fmt.Errorf("volume %s S0FS context is unavailable: %w", bound.volumeID, err)
	}
	bound.session = newLocalSession(bound.volumeID, m.volumes, m.logrus)
	return nil
}

func (m *Manager) rollbackBoundVolumeReservation(ctx context.Context, bound *boundVolume, removeIfUnused bool) error {
	if bound == nil {
		return nil
	}
	m.mu.Lock()
	if bound.refCount > 0 {
		bound.refCount--
	}
	var cleanup *boundVolumeCleanup
	if removeIfUnused && bound.refCount == 0 && m.boundVolumes[bound.volumeID] == bound && !bound.closing {
		cleanup = m.releaseOwnerOnlyVolumeLocked(bound.volumeID, bound)
	}
	m.mu.Unlock()
	return m.finishBoundVolumeCleanup(ctx, cleanup)
}
