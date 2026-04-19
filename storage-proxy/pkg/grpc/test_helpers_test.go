package grpc

import (
	"context"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/db"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/notify"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/volume"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func authContext(teamID, sandboxID string) context.Context {
	return internalauth.WithClaims(context.Background(), &internalauth.Claims{
		TeamID:    teamID,
		SandboxID: sandboxID,
	})
}

func newTestFileSystemServer(volMgr volumeManager, repo VolumeRepository, hub *notify.Hub) *FileSystemServer {
	return NewFileSystemServer(volMgr, repo, hub, nil, logrus.New(), nil, nil)
}

type fakeVolumeManager struct {
	volumes        map[string]*volume.VolumeContext
	mountCalls     int
	unmountCalls   int
	ackCalls       int
	mountSessionID string
	mountSecret    string
	mountedAt      time.Time
	lastMount      struct {
		s3Prefix   string
		volumeID   string
		teamID     string
		config     *volume.VolumeConfig
		accessMode volume.AccessMode
	}
	trackedSandboxID string
	trackedVolumeID  string
	trackedSessionID string
}

func (m *fakeVolumeManager) MountVolume(_ context.Context, s3Prefix, volumeID, teamID string, config *volume.VolumeConfig, accessMode volume.AccessMode) (string, string, time.Time, error) {
	m.mountCalls++
	m.lastMount.s3Prefix = s3Prefix
	m.lastMount.volumeID = volumeID
	m.lastMount.teamID = teamID
	m.lastMount.config = config
	m.lastMount.accessMode = accessMode
	sessionID := m.mountSessionID
	if sessionID == "" {
		sessionID = "session-test"
	}
	sessionSecret := m.mountSecret
	if sessionSecret == "" {
		sessionSecret = "secret-test"
	}
	mountedAt := m.mountedAt
	if mountedAt.IsZero() {
		mountedAt = time.Unix(1700000000, 0)
	}
	return sessionID, sessionSecret, mountedAt, nil
}

func (m *fakeVolumeManager) UnmountVolume(_ context.Context, _, _ string) error {
	m.unmountCalls++
	return nil
}

func (m *fakeVolumeManager) AckInvalidate(_, _, _ string, _ bool, _ string) error {
	m.ackCalls++
	return nil
}

func (m *fakeVolumeManager) GetVolume(volumeID string) (*volume.VolumeContext, error) {
	if vol, ok := m.volumes[volumeID]; ok {
		return vol, nil
	}
	return nil, status.Error(codes.NotFound, "volume not mounted")
}

func (m *fakeVolumeManager) TrackVolumeSession(sandboxID, volumeID, sessionID string) {
	m.trackedSandboxID = sandboxID
	m.trackedVolumeID = volumeID
	m.trackedSessionID = sessionID
}

type fakeVolumeRepo struct {
	volumes map[string]*db.SandboxVolume
	owners  map[string]*db.SandboxVolumeOwner
}

func (f *fakeVolumeRepo) GetSandboxVolume(_ context.Context, id string) (*db.SandboxVolume, error) {
	if f == nil {
		return nil, db.ErrNotFound
	}
	if volume, ok := f.volumes[id]; ok {
		return volume, nil
	}
	return nil, db.ErrNotFound
}

func (f *fakeVolumeRepo) GetSandboxVolumeOwner(_ context.Context, volumeID string) (*db.SandboxVolumeOwner, error) {
	if f == nil {
		return nil, db.ErrNotFound
	}
	if owner, ok := f.owners[volumeID]; ok {
		return owner, nil
	}
	if volume, ok := f.volumes[volumeID]; ok {
		return &db.SandboxVolumeOwner{VolumeID: volume.ID}, nil
	}
	return nil, db.ErrNotFound
}
