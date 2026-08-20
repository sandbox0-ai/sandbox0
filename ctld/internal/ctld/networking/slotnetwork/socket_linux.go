//go:build linux

package slotnetwork

import (
	"os"
	"syscall"
)

func pathOwnedByRoot(info os.FileInfo) bool {
	stat, ok := info.Sys().(*syscall.Stat_t)
	return ok && stat.Uid == 0
}
