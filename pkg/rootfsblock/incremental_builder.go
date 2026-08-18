package rootfsblock

import (
	"context"
	"fmt"
	"sort"

	"github.com/opencontainers/go-digest"
)

const MaxMappingEntriesPerGeneration = 4 << 20

// BuildIncrementalGeneration publishes a new complete mapping root by
// structurally reusing every unchanged immutable data range and replacing
// only the supplied final block values. It never republishes unchanged data.
func BuildIncrementalGeneration(
	ctx context.Context,
	source RangeSource,
	base Descriptor,
	updates []BlockUpdate,
	publisher ImmutableObjectPublisher,
	options BuildOptions,
) (BuildResult, error) {
	if source == nil || publisher == nil {
		return BuildResult{}, fmt.Errorf("range source and publisher are required")
	}
	options, err := normalizeBuildOptions(options)
	if err != nil {
		return BuildResult{}, err
	}
	materializedBase := base
	materializedBase.CompositeTail = nil
	reader, err := NewReader(source, materializedBase, DefaultReadCacheBytes)
	if err != nil {
		return BuildResult{}, err
	}
	totalBlocks := uint64(base.LogicalSizeBytes / LogicalBlockSize)
	normalized, err := normalizeBlockUpdates(updates, totalBlocks)
	if err != nil {
		return BuildResult{}, err
	}
	if base.CompositeTail != nil {
		tail, _, err := DecodeCompositeTail(*base.CompositeTail, totalBlocks)
		if err != nil {
			return BuildResult{}, err
		}
		normalized = mergeFinalBlockUpdates(tail, normalized)
	}
	baseEntries, err := reader.dataEntries(ctx)
	if err != nil {
		return BuildResult{}, err
	}
	state := generationBuilder{ctx: ctx, publisher: publisher, options: options}
	unchanged, err := splitUnchangedEntries(reader, baseEntries, normalized)
	if err != nil {
		return BuildResult{}, err
	}
	dirty, err := state.publishBlockUpdates(normalized)
	if err != nil {
		return BuildResult{}, err
	}
	entries := append(unchanged, dirty...)
	sort.Slice(entries, func(i, j int) bool { return entries[i].LogicalStart < entries[j].LogicalStart })
	if len(entries) > MaxMappingEntriesPerGeneration {
		return BuildResult{}, fmt.Errorf("incremental generation has too many mapping entries")
	}
	root, rootPayload, err := state.publishMappingTree(entries, uint64(base.LogicalSizeBytes/LogicalBlockSize))
	if err != nil {
		return BuildResult{}, err
	}
	descriptor := Descriptor{
		Version: DescriptorVersion, LogicalSizeBytes: base.LogicalSizeBytes, BlockSizeBytes: LogicalBlockSize,
		MappingRoot: MappingRootLocator{
			Version: MappingPageVersion, RootDigest: digest.FromBytes(rootPayload).String(), Object: root,
		},
	}
	payload, err := EncodeDescriptor(descriptor)
	if err != nil {
		return BuildResult{}, err
	}
	return BuildResult{Descriptor: descriptor, Payload: payload, Objects: state.objects, Bytes: state.bytes}, nil
}

func mergeFinalBlockUpdates(first, second []BlockUpdate) []BlockUpdate {
	final := make(map[uint64]BlockUpdate, len(first)+len(second))
	for _, update := range first {
		final[update.Block] = update
	}
	for _, update := range second {
		final[update.Block] = update
	}
	updates := make([]BlockUpdate, 0, len(final))
	for _, update := range final {
		updates = append(updates, update)
	}
	sort.Slice(updates, func(i, j int) bool { return updates[i].Block < updates[j].Block })
	return updates
}

func normalizeBlockUpdates(updates []BlockUpdate, totalBlocks uint64) ([]BlockUpdate, error) {
	normalized := make([]BlockUpdate, len(updates))
	for index, update := range updates {
		if update.Block >= totalBlocks || len(update.Data) != LogicalBlockSize {
			return nil, fmt.Errorf("block update %d is outside the device or is not one logical block", index)
		}
		normalized[index] = BlockUpdate{Block: update.Block, Data: append([]byte(nil), update.Data...)}
	}
	sort.Slice(normalized, func(i, j int) bool { return normalized[i].Block < normalized[j].Block })
	for index := 1; index < len(normalized); index++ {
		if normalized[index-1].Block == normalized[index].Block {
			return nil, fmt.Errorf("block update %d duplicates logical block %d", index, normalized[index].Block)
		}
	}
	return normalized, nil
}

func (r *Reader) dataEntries(ctx context.Context) ([]MappingEntry, error) {
	entries := make([]MappingEntry, 0)
	var collect func(MappingPage) error
	collect = func(page MappingPage) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		for _, entry := range page.Entries {
			if entry.Kind == MappingEntryData {
				entries = append(entries, entry)
				if len(entries) > MaxMappingEntriesPerGeneration {
					return fmt.Errorf("base generation has too many mapping entries")
				}
				continue
			}
			payload, err := r.readRange(entry.Object)
			if err != nil {
				return fmt.Errorf("read mapping child: %w", err)
			}
			child, err := DecodeMappingPage(payload)
			if err != nil {
				return fmt.Errorf("decode mapping child: %w", err)
			}
			if child.Level+1 != page.Level || child.StartBlock != entry.LogicalStart || child.BlockCount != uint64(entry.BlockCount) {
				return fmt.Errorf("mapping child does not match its parent entry")
			}
			if err := collect(child); err != nil {
				return err
			}
		}
		return nil
	}
	if err := collect(r.root); err != nil {
		return nil, err
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].LogicalStart < entries[j].LogicalStart })
	var previousEnd uint64
	for index, entry := range entries {
		if index > 0 && entry.LogicalStart < previousEnd {
			return nil, fmt.Errorf("base generation contains overlapping data entries")
		}
		previousEnd = entry.LogicalStart + uint64(entry.BlockCount)
	}
	return entries, nil
}

func splitUnchangedEntries(reader *Reader, entries []MappingEntry, updates []BlockUpdate) ([]MappingEntry, error) {
	result := make([]MappingEntry, 0, len(entries))
	dirtyIndex := 0
	for _, entry := range entries {
		end := entry.LogicalStart + uint64(entry.BlockCount)
		for dirtyIndex < len(updates) && updates[dirtyIndex].Block < entry.LogicalStart {
			dirtyIndex++
		}
		entryDirtyStart := dirtyIndex
		for dirtyIndex < len(updates) && updates[dirtyIndex].Block < end {
			dirtyIndex++
		}
		if entryDirtyStart == dirtyIndex {
			result = append(result, entry)
			continue
		}
		payload, err := reader.readRange(entry.Object)
		if err != nil {
			return nil, fmt.Errorf("read data range split by dirty blocks: %w", err)
		}
		cursor := entry.LogicalStart
		for _, update := range updates[entryDirtyStart:dirtyIndex] {
			block := update.Block
			if cursor < block {
				result = append(result, splitDataEntry(entry, cursor, block, payload))
			}
			cursor = block + 1
		}
		if cursor < end {
			result = append(result, splitDataEntry(entry, cursor, end, payload))
		}
	}
	return result, nil
}

func splitDataEntry(entry MappingEntry, start, end uint64, payload []byte) MappingEntry {
	startOffset := int64(start-entry.LogicalStart) * LogicalBlockSize
	length := int64(end-start) * LogicalBlockSize
	fragment := payload[startOffset : startOffset+length]
	return MappingEntry{
		LogicalStart: start, BlockCount: uint32(end - start), Kind: MappingEntryData,
		Object: ObjectRange{
			Key: entry.Object.Key, Offset: entry.Object.Offset + startOffset, Length: length,
			Checksum: digest.FromBytes(fragment).String(),
		},
	}
}

func (b *generationBuilder) publishBlockUpdates(updates []BlockUpdate) ([]MappingEntry, error) {
	pending := make([]pendingDataEntry, 0, max(1, b.options.PackBytes/b.options.DataRangeBytes))
	pendingBytes := 0
	result := make([]MappingEntry, 0, len(updates))
	flushPack := func() error {
		if len(pending) == 0 {
			return nil
		}
		published, err := b.publishPack(pending)
		if err != nil {
			return err
		}
		result = append(result, published...)
		pending = pending[:0]
		pendingBytes = 0
		return nil
	}
	for index := 0; index < len(updates); {
		if allZero(updates[index].Data) {
			index++
			continue
		}
		start := updates[index].Block
		payload := make([]byte, 0, b.options.DataRangeBytes)
		payload = append(payload, updates[index].Data...)
		index++
		for index < len(updates) && !allZero(updates[index].Data) && updates[index].Block == start+uint64(len(payload)/LogicalBlockSize) &&
			len(payload)+LogicalBlockSize <= b.options.DataRangeBytes {
			payload = append(payload, updates[index].Data...)
			index++
		}
		if pendingBytes+len(payload) > b.options.PackBytes {
			if err := flushPack(); err != nil {
				return nil, err
			}
		}
		pending = append(pending, pendingDataEntry{
			entry: MappingEntry{
				LogicalStart: start, BlockCount: uint32(len(payload) / LogicalBlockSize), Kind: MappingEntryData,
				Object: ObjectRange{Offset: int64(pendingBytes), Length: int64(len(payload)), Checksum: digest.FromBytes(payload).String()},
			},
			data: payload,
		})
		pendingBytes += len(payload)
	}
	if err := flushPack(); err != nil {
		return nil, err
	}
	return result, nil
}
