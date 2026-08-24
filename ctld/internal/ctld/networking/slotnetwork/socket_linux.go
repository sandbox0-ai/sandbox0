//go:build linux

package slotnetwork

import (
	"os"
	"syscall"
)

func pathOwnedByRoot(info os.FileInfo) bool {
	return pathOwnedByUID(info, 0)
}

func pathOwnedByUID(info os.FileInfo, expectedUID uint32) bool {
	stat, ok := info.Sys().(*syscall.Stat_t)
	return ok && stat.Uid == expectedUID
}
