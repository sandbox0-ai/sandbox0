package runtimecheckpoint

import (
	"context"
	"fmt"
	"strings"

	"github.com/sandbox0-ai/sandbox0/pkg/objectstore"
)

const cleanupBatchSize = 64

// Collector deletes only an exact image binding authorized by regional terminal
// custody. Its caller must exclude all future upload, download and restore work
// before invoking it. Object age or an absent manifest is never that authority.
type Collector struct {
	objects objectstore.ContextCleanupStore
}

func NewCollector(objects objectstore.Store) (*Collector, error) {
	if !objectstore.SupportsContextCleanup(objects) {
		return nil, fmt.Errorf("checkpoint cleanup needs context-aware object storage")
	}
	return &Collector{objects: objects.(objectstore.ContextCleanupStore)}, nil
}

// Collect removes at most one bounded page, including chunks from an interrupted
// upload. Repeated calls start at the beginning; deletion cannot invalidate a
// retained pagination token. Completion requires a separate empty listing.
func (c *Collector) Collect(ctx context.Context, binding Binding) (bool, error) {
	digest, err := binding.Digest()
	if err != nil {
		return false, err
	}
	prefix := imagePrefix(digest)
	objects, more, _, err := c.objects.ListContext(ctx, prefix, "", "", "", cleanupBatchSize)
	if err != nil {
		return false, err
	}
	if len(objects) > cleanupBatchSize || len(objects) == 0 && more {
		return false, fmt.Errorf("invalid checkpoint cleanup listing")
	}
	previous := ""
	for _, object := range objects {
		if object.IsPrefix || !strings.HasPrefix(object.Key, prefix) || object.Key <= previous {
			return false, fmt.Errorf("checkpoint cleanup listing escaped exact binding")
		}
		name := strings.TrimPrefix(object.Key, prefix)
		if name != "manifest.json" && (!strings.HasPrefix(name, "chunks/") ||
			validateDigest("sha256:"+strings.TrimPrefix(name, "chunks/")) != nil) {
			return false, fmt.Errorf("checkpoint cleanup encountered an unknown object")
		}
		previous = object.Key
	}
	for _, object := range objects {
		if err := c.objects.DeleteContext(ctx, object.Key); err != nil && !objectstore.IsNotFound(err) {
			return false, err
		}
	}
	return len(objects) == 0, nil
}
