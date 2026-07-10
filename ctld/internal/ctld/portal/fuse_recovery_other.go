//go:build !linux

package portal

import "fmt"

const fuseCapabilityRecovery = uint64(1) << 57

func recoverFUSEConnection(tag string) (int, uint64, uint64, error) {
	return -1, 0, 0, fmt.Errorf("FUSE connection recovery is not supported on this platform")
}
