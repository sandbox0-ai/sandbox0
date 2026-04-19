package volume

import (
	"context"
	"fmt"
	"time"

	obsmetrics "github.com/sandbox0-ai/sandbox0/pkg/observability/metrics"
)

// Backend is the storage engine boundary behind storage-proxy volumes.
// Implementations own filesystem metadata, file data, durability, and backend
// cleanup for mounted volumes.
type Backend interface {
	MountVolume(ctx context.Context, req BackendMountRequest) (*VolumeContext, error)
	UnmountVolume(ctx context.Context, volCtx *VolumeContext) error
}

// BackendMountRequest is the storage-engine input for mounting a volume.
type BackendMountRequest struct {
	S3Prefix   string
	VolumeID   string
	TeamID     string
	Config     *VolumeConfig
	AccessMode AccessMode
	MountedAt  time.Time
	Metrics    *obsmetrics.StorageProxyMetrics
}

// FlushAll flushes dirty data for the mounted volume.
func (v *VolumeContext) FlushAll(path string) error {
	if v == nil {
		return fmt.Errorf("volume context is nil")
	}
	if v.VFS == nil {
		return fmt.Errorf("volume %s backend %q does not expose flush", v.VolumeID, v.Backend)
	}
	return v.VFS.FlushAll(path)
}
