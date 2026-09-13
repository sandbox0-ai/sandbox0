package nomadruntime

import (
	"context"
	"fmt"
	"strings"

	"github.com/sandbox0-ai/sandbox0/pkg/objectstore"
)

// Initialize identity before the primary exposes its runtime socket/capacity.
// In particular OSS Create is deliberately a no-op: runtime credentials must
// not need bucket lifecycle permissions, nor should the first tenant claim
// have to initialize a process/role credential provider. No RootFS is selected
// or read here, and later credential expiry remains owned by the provider.
func initializeRuntimeObjectStore(ctx context.Context, store objectstore.Store) error {
	if err := objectstore.PrepareCredentials(ctx, store); err != nil {
		return fmt.Errorf("prepare RootFS object credentials: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := store.Create(); err != nil && !strings.Contains(strings.ToLower(err.Error()), "alreadyownedbyyou") {
		return fmt.Errorf("create RootFS bucket: %w", err)
	}
	return nil
}
