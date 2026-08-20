//go:build !linux

package slotnetwork

import "os"

func pathOwnedByRoot(os.FileInfo) bool {
	return false
}
