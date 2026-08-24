//go:build !linux

package nomadruntime

import (
	"fmt"

	"github.com/containerd/errdefs"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
)

func newRuntimeResourceCgroup(string, protocol.NodeChannelCapacity) (runtimeResourceCgroup, error) {
	return nil, fmt.Errorf("runtime resource cgroups require Linux: %w", errdefs.ErrNotImplemented)
}
