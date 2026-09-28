//go:build linux

package rootfsblock

import (
	"bytes"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/opencontainers/go-digest"
	"github.com/stretchr/testify/require"
)

func TestCheckpointRangeCloneVerifiesSourceAndFallsBack(t *testing.T) {
	config := testDiskConfig(t, 2*MaxMappingRootBytes)
	cache := openTestDiskCache(t, 0, config)
	payload := bytes.Repeat([]byte{0x54}, MaxMappingRootBytes)
	checksum := digest.FromBytes(payload).String()
	cache.PutCheckpointChunk(checksum, payload)
	file, err := os.OpenFile(filepath.Join(t.TempDir(), "image"), os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
	require.NoError(t, err)
	defer file.Close()
	require.NoError(t, file.Truncate(int64(len(payload))))
	cloned, err := cache.CloneCheckpointChunk(t.Context(), checksum, int64(len(payload)), file, 0)
	require.NoError(t, err)
	if !cloned {
		verified, ok := cache.GetCheckpointChunk(checksum, int64(len(payload)))
		require.True(t, ok, "unsupported cloning must retain the verified read fallback")
		_, err = file.WriteAt(verified, 0)
		require.NoError(t, err)
	}
	actual := make([]byte, len(payload))
	_, err = file.ReadAt(actual, 0)
	require.NoError(t, err)
	require.Equal(t, payload, actual)

	name := strings.TrimPrefix(checksum, "sha256:") + "-" + strconv.Itoa(len(payload))
	require.NoError(t, os.WriteFile(filepath.Join(config.Directory, name), bytes.Repeat([]byte{0x66}, len(payload)), 0o600))
	second, err := os.OpenFile(filepath.Join(t.TempDir(), "corrupt"), os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
	require.NoError(t, err)
	defer second.Close()
	require.NoError(t, second.Truncate(int64(len(payload))))
	cloned, err = cache.CloneCheckpointChunk(t.Context(), checksum, int64(len(payload)), second, 0)
	require.NoError(t, err)
	require.False(t, cloned, "corrupt cache content cannot become a successful clone")
	_, hit := cache.GetCheckpointChunk(checksum, int64(len(payload)))
	require.False(t, hit)
	require.Positive(t, cache.Stats().DiskErrors)
}
