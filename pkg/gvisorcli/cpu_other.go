//go:build !linux || (!amd64 && !arm64)

package gvisorcli

import "fmt"

func nativeCPUStateLayout() (uint32, string, error) {
	return 0, "", fmt.Errorf("unsupported CPU observation platform")
}
