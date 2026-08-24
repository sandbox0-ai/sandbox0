//go:build !linux

package slotnetwork

import "os"

func pathOwnedByRoot(os.FileInfo) bool {
	return false
}

func pathOwnedByUID(os.FileInfo, uint32) bool {
	return false
}
