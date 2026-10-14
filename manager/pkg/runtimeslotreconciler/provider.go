package runtimeslotreconciler

import (
	"context"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
)

// ProviderRecoveryController is an independent regional boundary. Unavailable
// client RPCs only trigger observation; they never prove physical absence.
type ProviderRecoveryController interface {
	Recover(context.Context, sandboxstore.RuntimeSlot) (bool, error)
}
