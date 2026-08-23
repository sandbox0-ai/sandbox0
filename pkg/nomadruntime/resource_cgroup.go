package nomadruntime

import (
	"context"

	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
)

type runtimeResourceCgroup interface {
	Prepare(context.Context, protocol.RuntimeResourceLease) error
	RemoveAndConfirm(context.Context, protocol.RuntimeResourceLease) (bool, error)
}
