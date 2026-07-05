package http

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/sandbox0-ai/sandbox0/pkg/ctldapi"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/db"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/snapshot"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/volume"
)

type volumeSnapshotCheckpoint struct {
	server   *Server
	volumeID string
	ctlds    []string
}

func (c *volumeSnapshotCheckpoint) Prepared() bool {
	return c != nil && len(c.ctlds) > 0
}

func (c *volumeSnapshotCheckpoint) Complete(ctx context.Context) error {
	if c == nil {
		return nil
	}
	var firstErr error
	client := ctldapi.NewClient(c.server.ctldHTTPClientOrDefault())
	for i := len(c.ctlds) - 1; i >= 0; i-- {
		_, err := client.CompleteVolumeSnapshotCheckpoint(ctx, c.ctlds[i], ctldapi.CompleteVolumeSnapshotCheckpointRequest{
			SandboxVolumeID: c.volumeID,
		})
		if err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (c *volumeSnapshotCheckpoint) Abort(ctx context.Context) {
	if c == nil {
		return
	}
	client := ctldapi.NewClient(c.server.ctldHTTPClientOrDefault())
	for i := len(c.ctlds) - 1; i >= 0; i-- {
		_, err := client.AbortVolumeSnapshotCheckpoint(ctx, c.ctlds[i], ctldapi.AbortVolumeSnapshotCheckpointRequest{
			SandboxVolumeID: c.volumeID,
		})
		if err != nil && c.server != nil && c.server.logger != nil {
			c.server.logger.WithError(err).WithField("volume_id", c.volumeID).Warn("Failed to abort volume snapshot checkpoint")
		}
	}
}

func (s *Server) prepareVolumeSnapshotCheckpoint(ctx context.Context, volumeID, teamID string) (*volumeSnapshotCheckpoint, error) {
	checkpoint := &volumeSnapshotCheckpoint{server: s, volumeID: volumeID}
	if s == nil || s.repo == nil {
		return checkpoint, nil
	}
	volumeRecord, err := s.repo.GetSandboxVolume(ctx, volumeID)
	if err != nil {
		if errors.Is(err, db.ErrNotFound) {
			return nil, snapshot.ErrVolumeNotFound
		}
		return nil, err
	}
	if volumeRecord.TeamID != teamID {
		return nil, snapshot.ErrVolumeNotFound
	}
	if volume.NormalizeBackend(volumeRecord.Backend) != volume.BackendS0FS {
		return nil, snapshot.ErrUnsupportedBackend
	}

	mounts, err := s.getActiveVolumeMounts(ctx, volumeID)
	if err != nil {
		return nil, err
	}
	accessMode := volume.NormalizeAccessMode(volumeRecord.AccessMode)
	switch accessMode {
	case volume.AccessModeROX:
		return checkpoint, nil
	case volume.AccessModeRWX:
		if hasWritableActiveMount(mounts) {
			return nil, snapshot.ErrActiveRWXSnapshotUnsupported
		}
		return checkpoint, nil
	}

	client := ctldapi.NewClient(s.ctldHTTPClientOrDefault())
	for _, mount := range mounts {
		if mount == nil {
			continue
		}
		opts := volume.DecodeMountOptions(mount.MountOptions)
		if opts.OwnerKind != volume.OwnerKindCtld {
			continue
		}
		ownerURL, err := s.resolveVolumeMountURL(ctx, mount)
		if err != nil {
			checkpoint.Abort(context.Background())
			return nil, err
		}
		if ownerURL == nil {
			continue
		}
		resp, err := client.PrepareVolumeSnapshotCheckpoint(ctx, ownerURL.String(), ctldapi.PrepareVolumeSnapshotCheckpointRequest{
			SandboxVolumeID: volumeID,
		})
		if err != nil {
			checkpoint.Abort(context.Background())
			if ctldapi.IsConflictError(err) {
				return nil, fmt.Errorf("%w: %v", snapshot.ErrVolumeBusy, err)
			}
			return nil, err
		}
		if resp == nil || !resp.Prepared {
			checkpoint.Abort(context.Background())
			if resp != nil && strings.TrimSpace(resp.Error) != "" {
				return nil, fmt.Errorf("%w: %s", snapshot.ErrVolumeBusy, strings.TrimSpace(resp.Error))
			}
			return nil, snapshot.ErrVolumeBusy
		}
		checkpoint.ctlds = append(checkpoint.ctlds, ownerURL.String())
	}
	return checkpoint, nil
}

func (s *Server) getActiveVolumeMounts(ctx context.Context, volumeID string) ([]*db.VolumeMount, error) {
	if s == nil || s.repo == nil || strings.TrimSpace(volumeID) == "" {
		return nil, nil
	}
	heartbeatTimeout := 15
	if s.cfg != nil && s.cfg.HeartbeatTimeout > 0 {
		heartbeatTimeout = s.cfg.HeartbeatTimeout
	}
	return s.repo.GetActiveMounts(ctx, volumeID, heartbeatTimeout)
}

func hasWritableActiveMount(mounts []*db.VolumeMount) bool {
	for _, mount := range mounts {
		if mount == nil {
			continue
		}
		opts := volume.DecodeMountOptions(mount.MountOptions)
		if volume.NormalizeAccessMode(string(opts.AccessMode)) != volume.AccessModeROX {
			return true
		}
	}
	return false
}
