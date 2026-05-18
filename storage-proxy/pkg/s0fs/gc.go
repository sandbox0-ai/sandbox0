package s0fs

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/objectstore"
)

// GarbageCollectionPlan is a precomputed list of immutable S0FS objects that
// are not referenced by the retained states and can be deleted by a caller that
// has already established the required consistency guards.
type GarbageCollectionPlan struct {
	store        objectstore.Store
	Segments     []string
	Manifests    []string
	LiveSegments int
}

type GarbageCollectionResult struct {
	DeletedSegments  []string
	DeletedManifests []string
}

// PlanGarbageCollection lists S0FS segment and manifest objects that are not
// referenced by the supplied retained states.
func (m *Materializer) PlanGarbageCollection(ctx context.Context, retainedStates []*SnapshotState, retainedManifests map[string]struct{}) (*GarbageCollectionPlan, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !m.Enabled() {
		return nil, fmt.Errorf("%w: materializer is not configured", ErrInvalidInput)
	}
	liveSegments := liveSegmentKeysForVolume(m.volumeID, retainedStates...)
	segmentObjects, err := listObjectKeys(ctx, m.store, segmentDir+"/")
	if err != nil {
		return nil, err
	}
	manifestObjects, err := listObjectKeys(ctx, m.store, manifestDir+"/")
	if err != nil {
		return nil, err
	}

	plan := &GarbageCollectionPlan{
		store:        m.store,
		LiveSegments: len(liveSegments),
	}
	for _, key := range segmentObjects {
		if _, ok := liveSegments[key]; ok {
			continue
		}
		plan.Segments = append(plan.Segments, key)
	}
	for _, key := range manifestObjects {
		if _, ok := retainedManifests[key]; ok {
			continue
		}
		plan.Manifests = append(plan.Manifests, key)
	}
	return plan, nil
}

func (p *GarbageCollectionPlan) Apply(ctx context.Context) (*GarbageCollectionResult, error) {
	result := &GarbageCollectionResult{}
	if p == nil || p.store == nil {
		return result, nil
	}
	for _, key := range p.Segments {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		if err := p.store.Delete(key); err != nil {
			return result, fmt.Errorf("delete s0fs segment %s: %w", key, err)
		}
		result.DeletedSegments = append(result.DeletedSegments, key)
	}
	for _, key := range p.Manifests {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		if err := p.store.Delete(key); err != nil {
			return result, fmt.Errorf("delete s0fs manifest %s: %w", key, err)
		}
		result.DeletedManifests = append(result.DeletedManifests, key)
	}
	return result, nil
}

// DeleteAllObjects removes every object visible through the supplied store.
func DeleteAllObjects(ctx context.Context, store objectstore.Store) ([]string, error) {
	if store == nil {
		return nil, nil
	}
	keys, err := listObjectKeys(ctx, store, "")
	if err != nil {
		return nil, err
	}
	for _, key := range keys {
		if err := ctx.Err(); err != nil {
			return keys, err
		}
		if err := store.Delete(key); err != nil {
			return keys, fmt.Errorf("delete s0fs object %s: %w", key, err)
		}
	}
	return keys, nil
}

func liveSegmentKeysForVolume(volumeID string, states ...*SnapshotState) map[string]struct{} {
	volumeID = strings.TrimSpace(volumeID)
	live := make(map[string]struct{})
	for _, state := range states {
		if state == nil {
			continue
		}
		for _, extents := range state.ColdFiles {
			for _, extent := range extents {
				segment := state.Segments[extent.SegmentID]
				if segment == nil || strings.TrimSpace(segment.Key) == "" {
					continue
				}
				segmentVolumeID := strings.TrimSpace(segment.VolumeID)
				if segmentVolumeID == "" {
					segmentVolumeID = volumeID
				}
				if segmentVolumeID != volumeID {
					continue
				}
				live[segment.Key] = struct{}{}
			}
		}
	}
	return live
}

func listObjectKeys(ctx context.Context, store objectstore.Store, prefix string) ([]string, error) {
	var (
		keys       []string
		startAfter string
		token      string
	)
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		objects, hasMore, nextToken, err := store.List(prefix, startAfter, token, "", 1000)
		if err != nil {
			return nil, fmt.Errorf("list s0fs objects %q: %w", prefix, err)
		}
		for _, object := range objects {
			key := strings.TrimLeft(strings.TrimSpace(object.Key), "/")
			if key == "" {
				continue
			}
			keys = append(keys, key)
		}
		if !hasMore {
			break
		}
		if len(objects) > 0 {
			startAfter = objects[len(objects)-1].Key
		}
		token = nextToken
	}
	sort.Strings(keys)
	return keys, nil
}
