//go:build !linux || (!amd64 && !arm64)

package gvisorcli

import (
	"context"
	"fmt"
)

func (*Command) PrepareCPULaunch(context.Context) error {
	return fmt.Errorf("CPU launch recording requires native Linux amd64 or arm64")
}
func (*Command) BeginCPULaunch(context.Context, string) (CPULaunchVerifier, error) {
	return nil, fmt.Errorf("CPU launch recording requires native Linux amd64 or arm64")
}

func (*Command) ExecutableDigest(context.Context) (string, error) {
	return "", fmt.Errorf("CPU executable observation requires native Linux amd64 or arm64")
}
