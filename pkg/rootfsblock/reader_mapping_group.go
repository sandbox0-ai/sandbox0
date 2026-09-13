package rootfsblock

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/opencontainers/go-digest"
)

const (
	mappingGroupStoredLimit  = 1 << 20
	mappingGroupDecodedLimit = 4 << 20
	mappingGroupPageLimit    = 32
)

type mappingGroupPlan struct {
	objects  []ObjectRange
	identity string
}

// Only an authenticated parent can supply group geometry. Missing siblings,
// gaps, and large objects use exact demand; no index, guessed object length,
// tenant-specific profile, or scan of unrelated mapping objects is required.
func (r *Reader) planMappingGroup(parent MappingPage, target ObjectRange) (mappingGroupPlan, bool) {
	if parent.formatVersion() != CompressedFormatVersion || parent.Level == 0 || r.cache.maxBytes == 0 {
		return mappingGroupPlan{}, false
	}
	objects := make([]ObjectRange, 0, 4)
	var decoded int64
	for _, child := range parent.Entries {
		object := child.Object
		if child.Kind != MappingEntryChild || object.Key != target.Key {
			continue
		}
		if err := object.Validate(MaxMappingRootBytes); err != nil {
			return mappingGroupPlan{}, false
		}
		duplicate := false
		for _, previous := range objects {
			duplicate = duplicate || previous == object
		}
		if duplicate {
			continue
		}
		decoded += object.Length
		if len(objects) == mappingGroupPageLimit || decoded > mappingGroupDecodedLimit {
			return mappingGroupPlan{}, false
		}
		objects = append(objects, object)
	}
	// Conservative reserve for encoded mapping payload plus decoded entries and
	// cache overhead. A cache too small to retain the group uses exact demand.
	if len(objects) < 2 || decoded*4+int64(len(objects))*512 > r.cache.maxBytes {
		return mappingGroupPlan{}, false
	}
	sort.Slice(objects, func(i, j int) bool { return objects[i].Offset < objects[j].Offset })
	end := objects[0].Offset
	found := false
	var identity strings.Builder
	for _, object := range objects {
		if object.Offset != end {
			return mappingGroupPlan{}, false
		}
		end += object.StoredLength() // ObjectRange.Validate already bounded this sum.
		if end-objects[0].Offset > mappingGroupStoredLimit {
			return mappingGroupPlan{}, false
		}
		found = found || object == target
		fmt.Fprintf(&identity, "%q/%d/%d/%d/%q/%q;", object.Key, object.Offset, object.Length, object.EncodedLength, object.Encoding, object.Checksum)
	}
	if !found {
		return mappingGroupPlan{}, false
	}
	return mappingGroupPlan{objects: objects, identity: "mapping-group/v1/" + digest.FromString(identity.String()).String()}, true
}

type mappingGroupPayload struct {
	payload []byte
	err     error
}

func (r *Reader) readMappingChildGroup(parent MappingPage, target ObjectRange) (MappingPage, error) {
	if page, ok := r.cache.getPage(rangeCacheKey(target)); ok {
		return page, nil
	}
	plan, ok := r.planMappingGroup(parent, target)
	if !ok {
		return r.readMappingPage(target)
	}
	return r.readMappingPageUsing(target, func() ([]byte, error) { return r.readMappingGroupRange(plan, target) })
}

// Group delivery verifies independently owned immutable mapping bytes. Parsing
// each page's entries belongs to its demanded content flight, not every sibling
// that happened to share this physical source read. No encoded cache is added.
func (r *Reader) readMappingGroupRange(plan mappingGroupPlan, target ObjectRange) ([]byte, error) {
	key := rangeCacheKey(target)
	if payload, ok := r.cache.get(key); ok {
		return payload, nil
	}
	value, err, _ := r.cache.requests.Do(plan.identity, func() (any, error) {
		result := make(map[readCacheKey]mappingGroupPayload, len(plan.objects))
		first, last := len(plan.objects), -1
		check := func() {
			first, last = len(plan.objects), -1
			for i, object := range plan.objects {
				k := rangeCacheKey(object)
				if _, ok := result[k]; ok {
					continue
				}
				if payload, ok := r.cache.get(k); ok {
					result[k] = mappingGroupPayload{payload: payload}
				} else {
					first, last = min(first, i), i
				}
			}
		}
		check()
		if last < first {
			return result, nil
		}
		release, err := r.acquireSourceSlot()
		if err != nil {
			return nil, err
		}
		defer release()
		check()
		if last < first {
			return result, nil
		}
		start := plan.objects[first].Offset
		end := plan.objects[last].Offset + plan.objects[last].StoredLength()
		stored, err := r.readSourceRange(target.Key, start, end-start)
		if err != nil {
			return nil, &mappingGroupTransportError{err}
		}
		var maxDecoded int64
		for _, object := range plan.objects {
			maxDecoded = max(maxDecoded, object.Length)
		}
		decoder, err := newRangeDecoder(maxDecoded)
		if err != nil {
			return nil, err
		}
		defer decoder.Close()
		for _, object := range plan.objects {
			k := rangeCacheKey(object)
			if _, ok := result[k]; ok {
				continue
			}
			if payload, ok := r.cache.get(k); ok {
				result[k] = mappingGroupPayload{payload: payload}
				continue
			}
			begin := object.Offset - start
			payload, err := decodeRangePayloadInto(r.ioLifetime(), object, stored[begin:begin+object.StoredLength()], make([]byte, 0, object.Length), decoder)
			if canceled := r.ioLifetime().Err(); canceled != nil {
				return nil, canceled
			}
			if err == nil {
				r.cache.addVerified(k, payload)
			}
			result[k] = mappingGroupPayload{payload: payload, err: err}
		}
		return result, nil
	})
	if err != nil {
		var transport *mappingGroupTransportError
		if errors.As(err, &transport) && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) && r.ioLifetime().Err() == nil {
			// Already inside a page flight. Re-enter only the RANGE flight:
			// recursing into readMappingPage here would self-deadlock.
			return r.readRange(target)
		}
		return nil, err
	}
	result, ok := value.(map[readCacheKey]mappingGroupPayload)[key]
	if !ok {
		return nil, fmt.Errorf("mapping group omitted demanded page")
	}
	return result.payload, result.err
}

type mappingGroupTransportError struct{ err error }

func (e *mappingGroupTransportError) Error() string { return e.err.Error() }
func (e *mappingGroupTransportError) Unwrap() error { return e.err }
