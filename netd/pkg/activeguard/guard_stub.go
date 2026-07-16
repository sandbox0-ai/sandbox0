//go:build !linux

// Package activeguard fences node-local netd runtimes during workload and ctld
// HA transitions.
package activeguard

import (
	"context"
	"fmt"
)

const (
	EnvPath         = "NETD_ACTIVE_LOCK_PATH"
	EnvInitialDelay = "NETD_ACTIVE_LOCK_INITIAL_DELAY"
	EnvMaxHold      = "NETD_ACTIVE_LOCK_MAX_HOLD"
)

type Guard struct{}

func Acquire(context.Context, string) (*Guard, error) {
	return nil, fmt.Errorf("netd active guard is only supported on Linux")
}

func (*Guard) Close() error { return nil }
