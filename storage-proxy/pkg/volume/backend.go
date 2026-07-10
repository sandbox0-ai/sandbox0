package volume

import (
	"context"
	"fmt"
	"time"

	obsmetrics "github.com/sandbox0-ai/sandbox0/pkg/observability/metrics"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/s0fs"
)

const (
	BackendS0FS = "s0fs"
	BackendS3   = "s3"
)

func DefaultBackendType() string {
	return BackendS0FS
}

// Backend is the storage engine boundary behind storage-proxy volumes.
// Implementations own filesystem metadata, file data, durability, and backend
// cleanup for mounted volumes.
type Backend interface {
	MountVolume(ctx context.Context, req BackendMountRequest) (*VolumeContext, error)
	UnmountVolume(ctx context.Context, volCtx *VolumeContext) error
}

type StorageObserver interface {
	ObserveVolumeState(ctx context.Context, volumeID, teamID string, state *s0fs.SnapshotState, observedAt time.Time) error
}

// BackendMountRequest is the storage-engine input for mounting a volume.
type BackendMountRequest struct {
	S3Prefix        string
	VolumeID        string
	TeamID          string
	AccessMode      AccessMode
	MountedAt       time.Time
	Metrics         *obsmetrics.StorageProxyMetrics
	StorageObserver StorageObserver
}

// FlushAll flushes dirty data for the mounted volume.
func (v *VolumeContext) FlushAll(path string) error {
	if v == nil {
		return fmt.Errorf("volume context is nil")
	}
	if v.S0FS != nil {
		return v.S0FS.Fsync(uint64(v.RootInode))
	}
	return fmt.Errorf("volume %s backend %q does not expose flush", v.VolumeID, v.Backend)
}

func (v *VolumeContext) IsS0FS() bool {
	return v != nil && v.Backend == BackendS0FS && v.S0FS != nil
}
