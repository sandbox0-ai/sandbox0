//go:build linux

package nomadruntime

import (
	"io/fs"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"
)

// Count actual allocated blocks of compute-side artifacts only. The isolated
// RustFS server stores durable data on this same test host, so whole-host df
// would conflate the object service with the worker's disposable storage.
func autogrowNodeArtifactBytes(t *testing.T, root string) int64 {
	t.Helper()
	var allocated int64
	for _, name := range []string{"branches", "read-cache", "sessions.db"} {
		err := filepath.WalkDir(filepath.Join(root, name), func(_ string, entry fs.DirEntry, err error) error {
			if os.IsNotExist(err) {
				return nil // A rotation or cache eviction can remove a sampled path.
			}
			if err != nil {
				return err
			}
			info, err := entry.Info()
			if os.IsNotExist(err) {
				return nil
			}
			if err != nil {
				return err
			}
			if stat, ok := info.Sys().(*syscall.Stat_t); ok {
				allocated += stat.Blocks * 512
			}
			return nil
		})
		require.NoError(t, err)
	}
	return allocated
}
