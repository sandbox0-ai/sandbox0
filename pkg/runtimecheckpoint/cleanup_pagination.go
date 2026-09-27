package runtimecheckpoint

import (
	"context"
	"fmt"

	"github.com/sandbox0-ai/sandbox0/pkg/objectstore"
)

const maxCleanupCursors = 1024

type cleanupCursor struct {
	token    string
	sequence uint64
}

// Versioned OSS may return an empty, truncated ListObjectsV2 page while
// scanning delete markers. Only following its opaque continuation can prove
// emptiness. Each pass still performs one bounded listing. Remember cursors
// only across empty pages: once a page contains objects, reset before deleting
// anything so no deletion invalidates a retained cursor. Source terminal
// custody excludes uploads and new readers throughout this traversal.
//
// Losing/evicting this hint repeats the scan, never grants cleanup authority.
// No completion is reported until the provider returns an untruncated empty
// page; an unbound live capture still requires its independent authorization.
func (c *Collector) listCleanupPage(ctx context.Context, prefix string, limit int64) ([]objectstore.Info, bool, error) {
	c.cursorMu.Lock()
	cursor := c.cursors[prefix]
	c.cursorMu.Unlock()
	objects, more, next, err := c.objects.ListContext(ctx, prefix, "", cursor.token, "", limit)
	if err != nil {
		return nil, false, err
	}
	if len(objects) == 0 && more && (next == "" || next == cursor.token) {
		return nil, false, fmt.Errorf("checkpoint cleanup listing has no advancing continuation")
	}
	c.cursorMu.Lock()
	defer c.cursorMu.Unlock()
	if len(objects) != 0 || !more {
		delete(c.cursors, prefix)
	} else if c.cursors[prefix].token == cursor.token {
		if c.cursors == nil {
			c.cursors = make(map[string]cleanupCursor)
		}
		if _, exists := c.cursors[prefix]; !exists && len(c.cursors) >= maxCleanupCursors {
			var oldest string
			var oldestSequence uint64
			for key, value := range c.cursors {
				if oldest == "" || value.sequence < oldestSequence {
					oldest, oldestSequence = key, value.sequence
				}
			}
			delete(c.cursors, oldest)
		}
		c.cursorSequence++
		c.cursors[prefix] = cleanupCursor{token: next, sequence: c.cursorSequence}
	}
	return objects, more, nil
}
