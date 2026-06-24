package portal

import (
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/s0fs"
	"github.com/sandbox0-ai/sandbox0/pkg/volumefuse"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/fsmeta"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/volume"
	"github.com/sirupsen/logrus"
)

// NewS0FSSession returns a node-local FUSE session backed directly by an s0fs
// engine. It is used by ctld frontends that publish s0fs without involving
// storage-proxy as a runtime hop.
func NewS0FSSession(volumeID, teamID string, engine *s0fs.Engine, logger *logrus.Logger) volumefuse.Session {
	mgr := newLocalVolumeManager()
	mgr.add(&volume.VolumeContext{
		VolumeID:  volumeID,
		TeamID:    teamID,
		Backend:   volume.BackendS0FS,
		S0FS:      engine,
		Access:    volume.AccessModeRWO,
		MountedAt: time.Now().UTC(),
		RootInode: fsmeta.Ino(s0fs.RootInode),
		RootPath:  "/",
	})
	return newLocalSession(volumeID, mgr, logger)
}
