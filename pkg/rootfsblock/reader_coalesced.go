package rootfsblock

import (
	"bytes"
	"fmt"
	"sort"
	"strings"

	"github.com/opencontainers/go-digest"
)

const (
	bulkReadThreshold  = 128 << 10
	coalescedReadBytes = 1 << 20
)

type dataReadDemand struct {
	offset   int64
	bytes    int64
	coalesce bool
}

// readDataRange preserves small-demand granularity while amortizing bulk reads
// over adjacent, independently authenticated mapping entries. The returned span
// is fully checksum-verified; it need not itself be a persisted mapping entry.
func (r *Reader) readDataRange(entry MappingEntry, leaf MappingPage, demand *dataReadDemand) (uint64, []byte, error) {
	if payload, ok := r.cache.get(rangeCacheKey(entry.Object)); ok {
		return entry.LogicalStart, entryDataView(entry, payload), nil
	}
	var entries []MappingEntry
	if demand.coalesce {
		entries = r.coalescedEntries(entry, leaf, demand.offset, demand.bytes)
	}
	if len(entries) < 2 {
		payload, err := r.readRange(entry.Object)
		if err != nil {
			return entry.LogicalStart, nil, err
		}
		return entry.LogicalStart, entryDataView(entry, payload), nil
	}
	loaded, err := r.readCoalesced(entries)
	if err != nil {
		// A speculative neighbor must not make a healthy demanded range
		// unavailable. Retrying only the exact demanded range also preserves
		// partial-read behavior when a bulk source request fails partway.
		// Do not repeatedly retry a failing speculative window for every small
		// entry encountered by the remainder of this caller's demand.
		demand.coalesce = false
		payload, err := r.readRange(entry.Object)
		if err != nil {
			return entry.LogicalStart, nil, err
		}
		return entry.LogicalStart, entryDataView(entry, payload), nil
	}
	index := sort.Search(len(entries), func(i int) bool { return entries[i].LogicalStart >= entry.LogicalStart })
	if !loaded.valid[index] {
		return entry.LogicalStart, nil, fmt.Errorf("object range checksum mismatch")
	}
	first, last := index, index+1
	for first > 0 && loaded.valid[first-1] {
		first--
	}
	for last < len(entries) && loaded.valid[last] {
		last++
	}
	start := int64(entries[first].LogicalStart-entries[0].LogicalStart) * LogicalBlockSize
	end := int64(entries[last-1].LogicalStart-entries[0].LogicalStart)*LogicalBlockSize + entries[last-1].Object.Length
	return entries[first].LogicalStart, loaded.payload[start:end], nil
}

func entryDataView(entry MappingEntry, payload []byte) []byte {
	start := int64(entry.DataOffset)
	return payload[start : start+int64(entry.BlockCount)*LogicalBlockSize]
}

func fullDataRange(entry MappingEntry) bool {
	return entry.DataOffset == 0 && entry.Object.Length == int64(entry.BlockCount)*LogicalBlockSize
}

// coalescedEntries only uses the already verified leaf, never fetching mapping
// pages to speculate. Logical windows make overlapping bulk misses share one
// flight. Holes, object boundaries, noncontiguous pack offsets and coarse legacy
// entries all stop a window. A small/disabled cache disables speculative bytes.
func (r *Reader) coalescedEntries(entry MappingEntry, leaf MappingPage, demandOffset, demandBytes int64) []MappingEntry {
	if demandBytes < bulkReadThreshold || entry.Object.Length >= bulkReadThreshold || !fullDataRange(entry) {
		return nil
	}
	const windowBlocks = coalescedReadBytes / LogicalBlockSize
	start := entry.LogicalStart / windowBlocks * windowBlocks
	end := start + windowBlocks
	if r.cache.maxBytes < 2*coalescedReadBytes {
		start = max(start, uint64(demandOffset/LogicalBlockSize))
		demandEnd := demandOffset + demandBytes
		demandEndBlock := uint64(demandEnd / LogicalBlockSize)
		if demandEnd%LogicalBlockSize != 0 {
			demandEndBlock++
		}
		end = min(end, demandEndBlock)
	}
	index := sort.Search(len(leaf.Entries), func(i int) bool { return leaf.Entries[i].LogicalStart >= entry.LogicalStart })
	if index == len(leaf.Entries) || entry.LogicalStart < start || entry.LogicalStart+uint64(entry.BlockCount) > end {
		return nil
	}
	adjacent := func(left, right MappingEntry) bool {
		return left.Kind == MappingEntryData && right.Kind == MappingEntryData &&
			fullDataRange(left) && fullDataRange(right) &&
			left.Object.Length < bulkReadThreshold && right.Object.Length < bulkReadThreshold &&
			left.LogicalStart+uint64(left.BlockCount) == right.LogicalStart &&
			left.Object.Key == right.Object.Key && left.Object.Offset+left.Object.StoredLength() == right.Object.Offset
	}
	first, last := index, index+1
	for first > 0 && leaf.Entries[first-1].LogicalStart >= start && adjacent(leaf.Entries[first-1], leaf.Entries[first]) {
		first--
	}
	for last < len(leaf.Entries) && leaf.Entries[last].LogicalStart+uint64(leaf.Entries[last].BlockCount) <= end && adjacent(leaf.Entries[last-1], leaf.Entries[last]) {
		last++
	}
	return leaf.Entries[first:last]
}

type coalescedRead struct {
	payload []byte
	valid   []bool
}

func (r *Reader) readCoalesced(entries []MappingEntry) (coalescedRead, error) {
	for _, entry := range entries {
		if entry.Object.Encoding != "" {
			return r.readEncodedCoalesced(entries)
		}
	}
	var identity strings.Builder
	var length int64
	for _, entry := range entries {
		fmt.Fprintf(&identity, "%s/%d;", entry.Object.Checksum, entry.Object.Length)
		length += entry.Object.Length
	}
	// The ordered checksum/length list binds every byte independently of the
	// publisher's physical object location, just like individual cache keys.
	key := "coalesced/" + digest.FromString(identity.String()).String()
	value, err, _ := r.cache.requests.Do(key, func() (any, error) {
		cached := make([][]byte, len(entries))
		complete := true
		for index, entry := range entries {
			cached[index], _ = r.cache.get(rangeCacheKey(entry.Object))
			complete = complete && cached[index] != nil
		}
		result := coalescedRead{valid: make([]bool, len(entries))}
		if complete {
			result.payload = make([]byte, 0, length)
			for index, payload := range cached {
				result.payload = append(result.payload, payload...)
				result.valid[index] = true
			}
			return result, nil
		}
		release, err := r.acquireSourceSlot()
		if err != nil {
			return nil, err
		}
		defer release()
		// Keep the canonical whole-window flight identity, but trim verified
		// cache hits at either edge from its single transport request. Splitting
		// interior cache hits would add dependent GETs for small byte savings.
		// Recheck misses after admission: another exact read may have filled
		// them while this window waited for a source slot.
		first, last, offset := length, int64(0), int64(0)
		for index, entry := range entries {
			if cached[index] == nil {
				cached[index], _ = r.cache.get(rangeCacheKey(entry.Object))
			}
			if cached[index] == nil {
				first = min(first, offset)
				last = offset + entry.Object.Length
			}
			offset += entry.Object.Length
		}
		// One whole-window buffer retains cached edges even if they are evicted
		// before the caller consumes this result. The extra byte belongs to the
		// existing strict length check; no second bulk allocation is needed.
		payload := make([]byte, length+1)
		if first < last {
			if err := r.readSourceRangeInto(entries[0].Object.Key, entries[0].Object.Offset+first, payload[first:last+1]); err != nil {
				return coalescedRead{}, err
			}
		}
		result.payload = payload[:length]
		offset = 0
		for index, entry := range entries {
			fragment := payload[offset : offset+entry.Object.Length]
			offset += entry.Object.Length
			if cached[index] != nil {
				// A verified immutable hit is authoritative even when redundant
				// source bytes inside the fetched span are unavailable/corrupt.
				copy(fragment, cached[index])
				result.valid[index] = true
				continue
			}
			if digest.FromBytes(fragment).String() != entry.Object.Checksum {
				continue
			}
			result.valid[index] = true
			if r.cache.maxBytes > 0 {
				// Each entry must own its allocation: an evicted neighbor must
				// not leave a 1MiB array retained but charged as only 64KiB.
				r.cache.addVerified(rangeCacheKey(entry.Object), bytes.Clone(fragment))
			}
		}
		return result, nil
	})
	if err != nil {
		return coalescedRead{}, err
	}
	return value.(coalescedRead), nil
}
