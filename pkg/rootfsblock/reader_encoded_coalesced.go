package rootfsblock

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/opencontainers/go-digest"
)

// A bulk demand shares one physical GET across adjacent encoded ranges, while
// every decoded range keeps its own checksum. Cached edges are trimmed using
// physical offsets, never by adding decoded byte counts to a compressed pack.
func (r *Reader) readEncodedCoalesced(entries []MappingEntry) (coalescedRead, error) {
	var identity strings.Builder
	var length int64
	for _, entry := range entries {
		fmt.Fprintf(&identity, "%s/%d;", entry.Object.Checksum, entry.Object.Length)
		length += entry.Object.Length
	}
	if len(entries) < 2 || length > coalescedReadBytes {
		return coalescedRead{}, fmt.Errorf("encoded coalesced range exceeds bound")
	}
	key := "coalesced/" + digest.FromString(identity.String()).String()
	value, err, _ := r.cache.requests.Do(key, func() (any, error) {
		cached := make([][]byte, len(entries))
		complete := true
		for index, entry := range entries {
			cached[index], _ = r.cache.get(rangeCacheKey(entry.Object))
			complete = complete && cached[index] != nil
		}
		if !complete {
			release, err := r.acquireSourceSlot()
			if err != nil {
				return nil, err
			}
			defer release()
		}
		first, last := len(entries), -1
		for index, entry := range entries {
			if cached[index] == nil {
				cached[index], _ = r.cache.get(rangeCacheKey(entry.Object))
			}
			if cached[index] == nil {
				first, last = min(first, index), index
			}
		}
		var encoded []byte
		var physicalStart int64
		if first <= last {
			physicalStart = entries[first].Object.Offset
			physicalLength := entries[last].Object.Offset + entries[last].Object.StoredLength() - physicalStart
			if physicalLength <= 0 || physicalLength > coalescedReadBytes {
				return nil, fmt.Errorf("encoded coalesced physical range exceeds bound")
			}
			var err error
			encoded, err = r.readSourceRange(entries[first].Object.Key, physicalStart, physicalLength)
			if err != nil {
				return nil, err
			}
		}
		result := coalescedRead{payload: make([]byte, length), valid: make([]bool, len(entries))}
		decoder, err := newRangeDecoder(CompressedDataRangeBytes)
		if err != nil {
			return nil, err
		}
		defer decoder.Close()
		var offset int64
		for index, entry := range entries {
			fragment := result.payload[offset : offset+entry.Object.Length]
			offset += entry.Object.Length
			if cached[index] != nil {
				copy(fragment, cached[index])
				result.valid[index] = true
				continue
			}
			start := entry.Object.Offset - physicalStart
			end := start + entry.Object.StoredLength()
			if start < 0 || end > int64(len(encoded)) {
				return nil, fmt.Errorf("encoded entry exceeds physical coalesced range")
			}
			_, err := decodeRangePayloadInto(r.ioLifetime(), entry.Object, encoded[start:end], fragment[:0:len(fragment)], decoder)
			if err != nil {
				if canceled := r.ioLifetime().Err(); canceled != nil {
					return nil, canceled
				}
				continue
			}
			result.valid[index] = true
			if r.cache.maxBytes > 0 {
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
