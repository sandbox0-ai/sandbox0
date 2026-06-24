package rootfsstore

import (
	"path/filepath"
	"strings"

	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/objectstore"
)

// S0FSObjectStore returns the durable object namespace for one persistent
// rootfs filesystem.
func S0FSObjectStore(store objectstore.Store, teamID, filesystemID string) objectstore.Store {
	return objectstore.Prefix(store, filepath.ToSlash(filepath.Join("rootfs", "s0fs", SafePath(teamID), SafePath(filesystemID))))
}

func SafePath(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return "_"
	}
	replacer := strings.NewReplacer("/", "_", "\\", "_", "\x00", "_", "..", "_")
	return replacer.Replace(value)
}
