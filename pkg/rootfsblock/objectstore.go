package rootfsblock

import (
	"bytes"
	"context"
	"fmt"
	"io"

	"github.com/sandbox0-ai/sandbox0/pkg/objectstore"
)

// ObjectStorePublisher adapts Sandbox0's conditional object storage API to
// the block-map builder. Existing keys are read and compared without HEAD or
// LIST so an exact retry is idempotent and any content conflict fails closed.
type ObjectStorePublisher struct {
	Store objectstore.ContextConditionalStore
}

func (p ObjectStorePublisher) PutImmutable(ctx context.Context, key string, payload []byte) error {
	if p.Store == nil || !objectstore.SupportsContextConditionalCreate(p.Store) {
		return fmt.Errorf("contextual conditional object store is required")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	created, err := p.Store.PutIfAbsentContext(ctx, key, bytes.NewReader(payload))
	if err != nil || created {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	body, err := p.Store.GetContext(ctx, key, 0, int64(len(payload))+1)
	if err != nil {
		return fmt.Errorf("read existing immutable object: %w", err)
	}
	defer body.Close()
	existing, err := io.ReadAll(io.LimitReader(body, int64(len(payload))+1))
	if err != nil {
		return fmt.Errorf("read existing immutable object: %w", err)
	}
	if !bytes.Equal(existing, payload) {
		return fmt.Errorf("immutable object %q already exists with different content", key)
	}
	return nil
}
