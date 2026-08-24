package rootfsblock

import (
	"context"
	"fmt"
	"io"
	"math"
	"sort"

	"github.com/opencontainers/go-digest"
)

const (
	MaxChangedBlockMappingDepth = 16
	MaxChangedBlockMappingPages = 262_144
	MaxChangedBlockMappingBytes = int64(1 << 30)
)

// ChangedBlockLimitError reports a source generation whose dirty block set
// cannot fit within the caller's explicit admission bound.
type ChangedBlockLimitError struct {
	Limit           int
	RequiredAtLeast uint64
}

func (e *ChangedBlockLimitError) Error() string {
	if e == nil {
		return "changed block limit exceeded"
	}
	return fmt.Sprintf("changed block set requires at least %d entries, limit is %d",
		e.RequiredAtLeast, e.Limit)
}

// ChangedBlocks compares two immutable generation descriptors by traversing
// only mapping metadata. It never reads mapped data objects. The returned
// logical block numbers are sorted, unique, and bounded by maxChangedBlocks.
// Composite-tail blocks are conservatively dirty because their payloads are
// node-flush overrides rather than immutable mapping entries.
func ChangedBlocks(
	ctx context.Context,
	source RangeSource,
	oldDescriptor, currentDescriptor Descriptor,
	maxChangedBlocks int,
) ([]uint64, error) {
	if source == nil {
		return nil, fmt.Errorf("range source is required")
	}
	if maxChangedBlocks <= 0 {
		return nil, fmt.Errorf("max changed blocks must be positive")
	}
	if err := oldDescriptor.Validate(); err != nil {
		return nil, fmt.Errorf("old descriptor: %w", err)
	}
	if err := currentDescriptor.Validate(); err != nil {
		return nil, fmt.Errorf("current descriptor: %w", err)
	}
	if oldDescriptor.LogicalSizeBytes != currentDescriptor.LogicalSizeBytes ||
		oldDescriptor.BlockSizeBytes != currentDescriptor.BlockSizeBytes {
		return nil, fmt.Errorf("generation descriptors have different block geometry")
	}
	tailBlocks, err := changedCompositeTailBlocks(oldDescriptor, currentDescriptor)
	if err != nil {
		return nil, err
	}
	if sameMappingRoot(oldDescriptor.MappingRoot, currentDescriptor.MappingRoot) {
		if len(tailBlocks) > maxChangedBlocks {
			return nil, &ChangedBlockLimitError{Limit: maxChangedBlocks, RequiredAtLeast: uint64(len(tailBlocks))}
		}
		return tailBlocks, nil
	}

	oldIterator, err := newMappingExtentIterator(ctx, source, oldDescriptor)
	if err != nil {
		return nil, fmt.Errorf("open old mapping: %w", err)
	}
	currentIterator, err := newMappingExtentIterator(ctx, source, currentDescriptor)
	if err != nil {
		return nil, fmt.Errorf("open current mapping: %w", err)
	}
	oldExtent, oldFound, err := oldIterator.next()
	if err != nil {
		return nil, fmt.Errorf("read old mapping: %w", err)
	}
	currentExtent, currentFound, err := currentIterator.next()
	if err != nil {
		return nil, fmt.Errorf("read current mapping: %w", err)
	}
	totalBlocks := uint64(oldDescriptor.LogicalSizeBytes / oldDescriptor.BlockSizeBytes)
	changed := make([]uint64, 0, min(maxChangedBlocks, 1024))
	for cursor := uint64(0); cursor < totalBlocks; {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		for oldFound && mappingEntryEnd(oldExtent) <= cursor {
			oldExtent, oldFound, err = oldIterator.next()
			if err != nil {
				return nil, fmt.Errorf("read old mapping: %w", err)
			}
		}
		for currentFound && mappingEntryEnd(currentExtent) <= cursor {
			currentExtent, currentFound, err = currentIterator.next()
			if err != nil {
				return nil, fmt.Errorf("read current mapping: %w", err)
			}
		}
		oldData := oldFound && oldExtent.LogicalStart <= cursor
		currentData := currentFound && currentExtent.LogicalStart <= cursor
		end := totalBlocks
		if oldData {
			end = min(end, mappingEntryEnd(oldExtent))
		} else if oldFound {
			end = min(end, oldExtent.LogicalStart)
		}
		if currentData {
			end = min(end, mappingEntryEnd(currentExtent))
		} else if currentFound {
			end = min(end, currentExtent.LogicalStart)
		}
		if end <= cursor {
			return nil, fmt.Errorf("mapping extent iterator did not advance at block %d", cursor)
		}
		if oldData != currentData ||
			(oldData && !sameMappedRangeAt(oldExtent, currentExtent, cursor)) {
			if err := appendChangedBlockRange(&changed, cursor, end, maxChangedBlocks); err != nil {
				return nil, err
			}
		}
		cursor = end
	}
	return mergeChangedBlocks(changed, tailBlocks, maxChangedBlocks)
}

type mappingExtentIterator struct {
	ctx       context.Context
	source    RangeSource
	stack     []mappingPageFrame
	pageCount int
	pageBytes int64
}

type mappingPageFrame struct {
	page  MappingPage
	index int
}

func newMappingExtentIterator(
	ctx context.Context,
	source RangeSource,
	descriptor Descriptor,
) (*mappingExtentIterator, error) {
	iterator := &mappingExtentIterator{ctx: ctx, source: source}
	root, err := iterator.readPage(descriptor.MappingRoot.Object, descriptor.MappingRoot.RootDigest)
	if err != nil {
		return nil, err
	}
	totalBlocks := uint64(descriptor.LogicalSizeBytes / descriptor.BlockSizeBytes)
	if root.StartBlock != 0 || root.BlockCount != totalBlocks {
		return nil, fmt.Errorf("root mapping page does not cover the logical device")
	}
	iterator.stack = append(iterator.stack, mappingPageFrame{page: root})
	return iterator, nil
}

func (i *mappingExtentIterator) next() (MappingEntry, bool, error) {
	for len(i.stack) > 0 {
		if err := i.ctx.Err(); err != nil {
			return MappingEntry{}, false, err
		}
		frame := &i.stack[len(i.stack)-1]
		if frame.index == len(frame.page.Entries) {
			i.stack = i.stack[:len(i.stack)-1]
			continue
		}
		entry := frame.page.Entries[frame.index]
		frame.index++
		if entry.Kind == MappingEntryData {
			return entry, true, nil
		}
		if len(i.stack) >= MaxChangedBlockMappingDepth {
			return MappingEntry{}, false, fmt.Errorf("mapping tree depth exceeds %d", MaxChangedBlockMappingDepth)
		}
		child, err := i.readPage(entry.Object, entry.Object.Checksum)
		if err != nil {
			return MappingEntry{}, false, fmt.Errorf("read child mapping page: %w", err)
		}
		if child.Level+1 != frame.page.Level || child.StartBlock != entry.LogicalStart ||
			child.BlockCount != uint64(entry.BlockCount) {
			return MappingEntry{}, false, fmt.Errorf("mapping child does not match its parent entry")
		}
		i.stack = append(i.stack, mappingPageFrame{page: child})
	}
	return MappingEntry{}, false, nil
}

func (i *mappingExtentIterator) readPage(object ObjectRange, expectedDigest string) (MappingPage, error) {
	if i.pageCount >= MaxChangedBlockMappingPages || object.Length > MaxChangedBlockMappingBytes-i.pageBytes {
		return MappingPage{}, fmt.Errorf("mapping metadata exceeds %d pages or %d bytes",
			MaxChangedBlockMappingPages, MaxChangedBlockMappingBytes)
	}
	if err := i.ctx.Err(); err != nil {
		return MappingPage{}, err
	}
	body, err := i.source.Get(object.Key, object.Offset, object.Length)
	if err != nil {
		return MappingPage{}, err
	}
	defer body.Close()
	payload, err := io.ReadAll(io.LimitReader(body, object.Length+1))
	if err != nil {
		return MappingPage{}, err
	}
	if int64(len(payload)) != object.Length {
		return MappingPage{}, fmt.Errorf("mapping range returned %d bytes, expected %d", len(payload), object.Length)
	}
	actualDigest := digest.FromBytes(payload).String()
	if actualDigest != object.Checksum || actualDigest != expectedDigest {
		return MappingPage{}, fmt.Errorf("mapping page checksum mismatch")
	}
	page, err := DecodeMappingPage(payload)
	if err != nil {
		return MappingPage{}, err
	}
	i.pageCount++
	i.pageBytes += object.Length
	return page, nil
}

func changedCompositeTailBlocks(oldDescriptor, currentDescriptor Descriptor) ([]uint64, error) {
	totalBlocks := uint64(oldDescriptor.LogicalSizeBytes / oldDescriptor.BlockSizeBytes)
	unique := make(map[uint64]struct{})
	for name, tail := range map[string]*CompositeTail{
		"old": oldDescriptor.CompositeTail, "current": currentDescriptor.CompositeTail,
	} {
		if tail == nil {
			continue
		}
		records, _, err := DecodeCompositeTail(*tail, totalBlocks)
		if err != nil {
			return nil, fmt.Errorf("%s composite tail: %w", name, err)
		}
		for _, record := range records {
			unique[record.Block] = struct{}{}
		}
	}
	blocks := make([]uint64, 0, len(unique))
	for block := range unique {
		blocks = append(blocks, block)
	}
	sort.Slice(blocks, func(left, right int) bool { return blocks[left] < blocks[right] })
	return blocks, nil
}

func appendChangedBlockRange(target *[]uint64, start, end uint64, limit int) error {
	if end <= start {
		return nil
	}
	required := uint64(len(*target)) + end - start
	if required > uint64(limit) {
		return &ChangedBlockLimitError{Limit: limit, RequiredAtLeast: required}
	}
	for block := start; block < end; block++ {
		*target = append(*target, block)
	}
	return nil
}

func mergeChangedBlocks(mapping, tail []uint64, limit int) ([]uint64, error) {
	if len(tail) == 0 {
		return mapping, nil
	}
	merged := make([]uint64, 0, min(limit, len(mapping)+len(tail)))
	left, right := 0, 0
	for left < len(mapping) || right < len(tail) {
		var next uint64
		switch {
		case right >= len(tail) || (left < len(mapping) && mapping[left] < tail[right]):
			next = mapping[left]
			left++
		case left >= len(mapping) || tail[right] < mapping[left]:
			next = tail[right]
			right++
		default:
			next = mapping[left]
			left++
			right++
		}
		if len(merged) == 0 || merged[len(merged)-1] != next {
			if len(merged) == limit {
				return nil, &ChangedBlockLimitError{Limit: limit, RequiredAtLeast: uint64(limit + 1)}
			}
			merged = append(merged, next)
		}
	}
	return merged, nil
}

func mappingEntryEnd(entry MappingEntry) uint64 {
	return entry.LogicalStart + uint64(entry.BlockCount)
}

func sameMappedRangeAt(oldEntry, currentEntry MappingEntry, block uint64) bool {
	oldDelta := int64(block-oldEntry.LogicalStart) * LogicalBlockSize
	currentDelta := int64(block-currentEntry.LogicalStart) * LogicalBlockSize
	if oldEntry.Object.Offset > math.MaxInt64-oldDelta || currentEntry.Object.Offset > math.MaxInt64-currentDelta {
		return false
	}
	oldOffset := oldEntry.Object.Offset + oldDelta
	currentOffset := currentEntry.Object.Offset + currentDelta
	return oldEntry.Object.Key == currentEntry.Object.Key && oldOffset == currentOffset
}

func sameMappingRoot(oldRoot, currentRoot MappingRootLocator) bool {
	return oldRoot.Version == currentRoot.Version && oldRoot.RootDigest == currentRoot.RootDigest &&
		oldRoot.Object == currentRoot.Object
}
