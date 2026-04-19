package fsserver

import (
	"context"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/fserror"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/notify"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/volume"
	"github.com/sirupsen/logrus"
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
	mountedAt      time.Time
	lastMount      struct {
		s3Prefix   string
		volumeID   string
		teamID     string
		accessMode volume.AccessMode
	}
}

func (m *fakeVolumeManager) MountVolume(_ context.Context, s3Prefix, volumeID, teamID string, accessMode volume.AccessMode) (string, time.Time, error) {
	m.mountCalls++
	m.lastMount.s3Prefix = s3Prefix
	m.lastMount.volumeID = volumeID
	m.lastMount.teamID = teamID
	m.lastMount.accessMode = accessMode
	sessionID := m.mountSessionID
	if sessionID == "" {
		sessionID = "session-test"
	}
	mountedAt := m.mountedAt
	if mountedAt.IsZero() {
		mountedAt = time.Unix(1700000000, 0)
	}
	return sessionID, mountedAt, nil
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
	return nil, fserror.New(fserror.NotFound, "volume not mounted")
}
