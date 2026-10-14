package session

import (
	"context"
	"fmt"
	"math"
)

// XFSUsage separates the mounted filesystem size from its larger device.
type XFSUsage struct {
	SizeBytes  int64
	FreeBytes  int64
	BlockBytes int64
}

// XFSGrowthRuntime performs online growth on an exact mounted NBD filesystem.
// The session owner must serialize it against freeze, teardown and transfer.
type XFSGrowthRuntime interface {
	InspectXFS(devicePath, xfsRoot string) (XFSUsage, error)
	GrowXFS(context.Context, string, string, int64) (XFSUsage, error)
}

// XFSGrowthTarget doubles capacity before free space drops below the greater
// of 20 percent and 64 MiB. The device ceiling is fixed; no host disk resize
// is involved. Zero means that no growth is currently required.
func XFSGrowthTarget(usage XFSUsage, deviceBytes int64) (int64, error) {
	if usage.BlockBytes <= 0 || usage.SizeBytes <= 0 || usage.FreeBytes < 0 || usage.FreeBytes > usage.SizeBytes ||
		usage.SizeBytes%usage.BlockBytes != 0 || deviceBytes < usage.SizeBytes || deviceBytes%usage.BlockBytes != 0 {
		return 0, fmt.Errorf("invalid XFS or sparse device geometry")
	}
	if usage.SizeBytes == deviceBytes || usage.FreeBytes > max(usage.SizeBytes/5, 64<<20) {
		return 0, nil
	}
	target := deviceBytes
	if usage.SizeBytes <= math.MaxInt64/2 {
		target = min(deviceBytes, usage.SizeBytes*2)
	}
	return target - target%usage.BlockBytes, nil
}
