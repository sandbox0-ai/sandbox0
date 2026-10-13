package session

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestXFSGrowthTargetTriggersEarlyAndHonorsCeiling(t *testing.T) {
	usage := XFSUsage{SizeBytes: 8 << 30, FreeBytes: 2 << 30, BlockBytes: 4096}
	target, err := XFSGrowthTarget(usage, 64<<40)
	require.NoError(t, err)
	require.Zero(t, target)
	usage.FreeBytes = 1 << 30
	target, err = XFSGrowthTarget(usage, 64<<40)
	require.NoError(t, err)
	require.Equal(t, int64(16<<30), target)
	target, err = XFSGrowthTarget(usage, 12<<30)
	require.NoError(t, err)
	require.Equal(t, int64(12<<30), target)
	target, err = XFSGrowthTarget(usage, usage.SizeBytes)
	require.NoError(t, err)
	require.Zero(t, target)
}

func TestXFSGrowthTargetRejectsInvalidAndAvoidsOverflow(t *testing.T) {
	for _, usage := range []XFSUsage{{}, {SizeBytes: 8 << 30, FreeBytes: -1, BlockBytes: 4096}, {SizeBytes: 4097, BlockBytes: 4096}} {
		_, err := XFSGrowthTarget(usage, 64<<40)
		require.Error(t, err)
	}
	limit := int64(math.MaxInt64 - 4095)
	target, err := XFSGrowthTarget(XFSUsage{SizeBytes: 1 << 62, FreeBytes: 0, BlockBytes: 4096}, limit)
	require.NoError(t, err)
	require.Equal(t, limit, target)
}
