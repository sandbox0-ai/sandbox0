package rootfsmaintenance

import (
	"context"
	"fmt"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/rootfshead"
)

func (c *Controller) scanUnknownRootFSObjects(ctx context.Context) (int, error) {
	if c == nil || c.store == nil || c.deleter == nil {
		return 0, nil
	}
	c.orphanScanMu.Lock()
	defer c.orphanScanMu.Unlock()
	if _, err := c.store.CleanupStaleRootFSWriteLeases(ctx); err != nil {
		return 0, err
	}
	if c.objectLister == nil {
		return 0, nil
	}
	root := rootfshead.ObjectRootPrefix() + "/"
	objects, more, err := c.objectLister.ListRootFSObjects(root, c.orphanScanCursor, int64(c.cfg.BatchSize))
	if err != nil {
		return 0, fmt.Errorf("list rootfs v3 objects for orphan scan: %w", err)
	}
	cutoff := time.Now().Add(-c.cfg.UnknownObjectGrace)
	deleted := 0
	for _, object := range objects {
		if err := ctx.Err(); err != nil {
			return deleted, err
		}
		if object.Key == "" || object.Modified.IsZero() || !object.Modified.Before(cutoff) {
			continue
		}
		prefix, err := rootfshead.TeamPrefixFromObjectKey(object.Key)
		if err != nil {
			continue
		}
		removed, err := c.store.DeleteUnknownRootFSObject(ctx, object.Key, prefix, c.deleter)
		if err != nil {
			return deleted, err
		}
		if removed {
			deleted++
		}
	}
	if more && len(objects) > 0 {
		c.orphanScanCursor = objects[len(objects)-1].Key
	} else {
		c.orphanScanCursor = ""
	}
	return deleted, nil
}
