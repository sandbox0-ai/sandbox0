package runtimecheckpoint

import (
	"context"
	"encoding/json"
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
	reader  checkpointObjectReader
}

func NewCollector(objects objectstore.Store) (*Collector, error) {
	if !objectstore.SupportsContextCleanup(objects) {
		return nil, fmt.Errorf("checkpoint cleanup needs context-aware object storage")
	}
	reader, ok := objects.(checkpointObjectReader)
	if !ok {
		return nil, fmt.Errorf("checkpoint cleanup needs context-aware reads")
	}
	return &Collector{objects: objects.(objectstore.ContextCleanupStore), reader: reader}, nil
}

// Collect removes at most one bounded page, including chunks from an interrupted
// upload. Repeated calls start at the beginning; deletion cannot invalidate a
// retained pagination token. Completion requires a separate empty listing.
func (c *Collector) Collect(ctx context.Context, binding Binding) (bool, error) {
	digest, err := binding.Digest()
	if err != nil {
		return false, err
	}
	scope, err := captureScopeForBinding(binding)
	if err != nil {
		return false, err
	}
	capture, _ := capturePrefix(scope)
	bound, readErr := readCheckpointObject(ctx, c.reader, capture+"publication.json", MaxManifestBytes)
	if readErr != nil && !objectstore.IsNotFound(readErr) {
		return false, readErr
	}
	if readErr == nil {
		want, _ := json.Marshal(binding)
		if string(bound) != string(want) {
			return false, fmt.Errorf("capture cleanup binding belongs to another filesystem cut")
		}
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
	if len(objects) != 0 {
		return false, nil
	}
	if readErr == nil {
		return c.CollectCapture(ctx, scope)
	}
	// No publication marker may mean early capture never completed, rather
	// than an empty scope. That requires terminal capture authority, not an
	// invented final RootFS binding supplied to this collector.
	remaining, more, _, err := c.objects.ListContext(ctx, capture, "", "", "", 1)
	if err != nil {
		return false, err
	}
	if len(remaining) != 0 || more {
		return false, fmt.Errorf("unbound capture cleanup requires terminal capture authority")
	}
	return true, nil
}

// CollectCapture removes a bounded page of tentative data only after regional
// terminal custody excludes every future upload, publication, read and restore
// for this exact source operation. It also handles failed capture with no final
// RootFS descriptor. Neither age nor reservation existence grants that authority.
func (c *Collector) CollectCapture(ctx context.Context, scope CaptureScope) (bool, error) {
	prefix, err := capturePrefix(scope)
	if err != nil {
		return false, err
	}
	objects, more, _, err := c.objects.ListContext(ctx, prefix, "", "", "", cleanupBatchSize)
	if err != nil {
		return false, err
	}
	if len(objects) > cleanupBatchSize || len(objects) == 0 && more {
		return false, fmt.Errorf("invalid capture cleanup listing")
	}
	previous := ""
	names := make([]string, len(objects))
	for i, object := range objects {
		names[i], err = captureObjectName(prefix, previous, object)
		if err != nil {
			return false, err
		}
		previous = object.Key
	}
	// A lost publication reply can leave a final manifest even when regional
	// failure handling knows only the capture scope. Retain the binding marker
	// until that exact manifest and every staged object have been removed.
	bound, err := readCheckpointObject(ctx, c.reader, prefix+"publication.json", MaxManifestBytes)
	if err != nil && !objectstore.IsNotFound(err) {
		return false, err
	}
	if err == nil {
		var binding Binding
		if json.Unmarshal(bound, &binding) != nil || !scope.matches(binding) {
			return false, fmt.Errorf("capture cleanup publication changed source")
		}
		canonical, _ := json.Marshal(binding)
		if string(canonical) != string(bound) {
			return false, fmt.Errorf("capture cleanup publication is not canonical")
		}
		bd, _ := binding.Digest()
		_, err := readCheckpointObject(ctx, c.reader, manifestKey(bd), MaxManifestBytes)
		if err != nil && !objectstore.IsNotFound(err) {
			return false, err
		}
		if err == nil {
			if err := c.objects.DeleteContext(ctx, manifestKey(bd)); err != nil && !objectstore.IsNotFound(err) {
				return false, err
			}
			return false, nil
		}
	}
	for i, object := range objects {
		// Keep the binding marker until it is the sole remaining object, so
		// partial cleanup and a lost reply cannot discard the ownership proof.
		if names[i] == "publication.json" && (len(objects) != 1 || more) {
			continue
		}
		if err := c.objects.DeleteContext(ctx, object.Key); err != nil && !objectstore.IsNotFound(err) {
			return false, err
		}
	}
	return len(objects) == 0, nil
}
