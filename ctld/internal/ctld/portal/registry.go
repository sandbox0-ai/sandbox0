package portal

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/db"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/volume"
	"go.uber.org/zap"
)

func (m *Manager) ownerPodID() string {
	switch {
	case m == nil:
		return ""
	case m.podNamespace != "" && m.podName != "":
		return m.podNamespace + "/" + m.podName
	case m.podName != "":
		return m.podName
	default:
		return ""
	}
}

func (m *Manager) validateBindableVolume(ctx context.Context, req ctldBindContext) (*db.SandboxVolume, error) {
	if m == nil || m.repo == nil {
		return nil, fmt.Errorf("ctld volume registry unavailable")
	}
	vol, err := m.repo.GetSandboxVolume(ctx, req.volumeID)
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(vol.TeamID) != strings.TrimSpace(req.teamID) {
		return nil, fmt.Errorf("volume %s does not belong to team %s", req.volumeID, req.teamID)
	}
	if volume.NormalizeAccessMode(vol.AccessMode) != volume.AccessModeRWO {
		return nil, fmt.Errorf("ctld volume portal only supports RWO volumes, got %s", vol.AccessMode)
	}

	heartbeatTimeout := 15
	if m.storage != nil && m.storage.HeartbeatTimeout > 0 {
		heartbeatTimeout = m.storage.HeartbeatTimeout
	}
	mounts, err := m.repo.GetActiveMounts(ctx, req.volumeID, heartbeatTimeout)
	if err != nil {
		return nil, err
	}
	selfPodID := m.ownerPodID()
	for _, mount := range mounts {
		if !isConflictingMountForCtldBind(mount, m.clusterID, selfPodID) {
			continue
		}
		return nil, fmt.Errorf("volume %s already has an active owner on %s/%s", req.volumeID, mount.ClusterID, mount.PodID)
	}
	return vol, nil
}

func isConflictingMountForCtldBind(mount *db.VolumeMount, selfClusterID, selfPodID string) bool {
	if mount == nil {
		return false
	}
	if mount.ClusterID == selfClusterID && mount.PodID == selfPodID {
		return false
	}
	opts := volume.DecodeMountOptions(mount.MountOptions)
	return opts.OwnerKind != volume.OwnerKindStorageProxy
}

type ctldBindContext struct {
	volumeID string
	teamID   string
}

func (m *Manager) registerOwner(ctx context.Context, pm *portalMount, accessMode volume.AccessMode) error {
	if m == nil || m.repo == nil || pm == nil || pm.volumeID == "" {
		return fmt.Errorf("ctld volume registry unavailable")
	}
	ownerPodID := m.ownerPodID()
	if ownerPodID == "" {
		return fmt.Errorf("ctld pod identity unavailable")
	}

	opts := volume.MountOptions{
		AccessMode:   accessMode,
		OwnerKind:    volume.OwnerKindCtld,
		OwnerPort:    8095,
		NodeName:     m.nodeName,
		PodNamespace: m.podNamespace,
	}
	rawOpts, err := json.Marshal(opts)
	if err != nil {
		return err
	}
	rawMsg := json.RawMessage(rawOpts)
	mount := &db.VolumeMount{
		ID:            uuid.NewString(),
		VolumeID:      pm.volumeID,
		ClusterID:     m.clusterID,
		PodID:         ownerPodID,
		LastHeartbeat: time.Now().UTC(),
		MountedAt:     pm.mountedAt,
		MountOptions:  &rawMsg,
	}
	if err := m.repo.CreateMount(ctx, mount); err != nil {
		return err
	}

	interval := m.heartbeatInterval
	if interval <= 0 {
		interval = 5 * time.Second
	}
	heartbeatCtx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	pm.heartbeatCancel = cancel
	pm.heartbeatDone = done
	go func(volumeID string) {
		defer close(done)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-heartbeatCtx.Done():
				return
			case <-ticker.C:
				if err := m.repo.UpdateMountHeartbeat(context.Background(), volumeID, m.clusterID, ownerPodID); err != nil && m.logger != nil {
					m.logger.Warn("ctld volume owner heartbeat failed", zap.String("volume_id", volumeID), zap.Error(err))
				}
			}
		}
	}(pm.volumeID)
	return nil
}

func (m *Manager) unregisterOwner(pm *portalMount, volumeID string) {
	if m == nil || pm == nil {
		return
	}
	if pm.heartbeatCancel != nil {
		pm.heartbeatCancel()
		pm.heartbeatCancel = nil
	}
	if pm.heartbeatDone != nil {
		<-pm.heartbeatDone
		pm.heartbeatDone = nil
	}
	if m.repo == nil || volumeID == "" {
		return
	}
	if err := m.repo.DeleteMount(context.Background(), volumeID, m.clusterID, m.ownerPodID()); err != nil && m.logger != nil {
		m.logger.Warn("ctld volume owner unregister failed", zap.String("volume_id", volumeID), zap.Error(err))
	}
}
