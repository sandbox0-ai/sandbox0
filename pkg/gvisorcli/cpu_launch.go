package gvisorcli

import (
	"context"
	"os"
	"sync"
	"time"

	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
)

// LaunchCPURecorder keeps expensive stock-runsc observations in warm-up. Begin
// and Complete only revalidate native capability and immutable executable state.
// Its absence or failure means no migration launch evidence, not a failed claim.
type LaunchCPURecorder interface {
	PrepareCPULaunch(context.Context) error
	BeginCPULaunch(context.Context, string) (CPULaunchVerifier, error)
}

// CPULaunchVerifier is one bounded launch interval. The caller completes it only
// after successful create/start, before persisting the claim's active state.
type CPULaunchVerifier interface {
	Complete(context.Context) (*protocol.MigrationCPUObservation, string, error)
}

// CPULaunchCache holds at most one immutable node-local observation. Share it
// between a driver's runners; no cache state is used as regional authority.
type CPULaunchCache struct {
	gate     chan struct{}
	mu       sync.RWMutex
	snapshot *cpuLaunchSnapshot
	closed   bool
}

func NewCPULaunchCache() *CPULaunchCache { return &CPULaunchCache{gate: make(chan struct{}, 1)} }

// Close releases the shared executable monitor and invalidates outstanding
// witnesses. A closed driver cache cannot be prepared again.
func (c *CPULaunchCache) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closed = true
	if c.snapshot != nil && c.snapshot.guard != nil {
		return c.snapshot.guard.Close()
	}
	return nil
}

type cpuExecutableGuard interface {
	Unchanged() error
	Close() error
}

type cpuLaunchSnapshot struct {
	path             string
	file             os.FileInfo
	executableDigest string
	boot             string
	observation      protocol.MigrationCPUObservation
	hardware         map[string]string
	guard            cpuExecutableGuard
}

type cpuLaunchVerifier struct {
	mu       sync.Mutex
	used     bool
	expires  time.Time
	command  *Command
	snapshot *cpuLaunchSnapshot
}

const cpuLaunchSupportedVersion = "runsc version release-20260914.0"
const cpuLaunchWitnessMaxAge = 2 * time.Minute
const maxCPUObservedExecutableBytes = 512 << 20

var _ LaunchCPURecorder = (*Command)(nil)
