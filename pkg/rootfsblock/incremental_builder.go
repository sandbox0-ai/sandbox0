package rootfsblock

import (
	"context"
	"fmt"
	"io"
	"sort"

	"github.com/opencontainers/go-digest"
)

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
	if err := base.Validate(); err != nil {
		return BuildResult{}, err
	}
	totalBlocks := uint64(base.LogicalSizeBytes / LogicalBlockSize)
	normalized, err := normalizeBlockUpdates(updates, totalBlocks)
	if err != nil {
		return BuildResult{}, err
	}
	blocks := make([]uint64, len(normalized))
	for index, update := range normalized {
		blocks[index] = update.Block
	}
	return buildIncrementalGeneration(ctx, source, base, &sliceBlockUpdateReader{updates: normalized}, blocks, publisher, options)
}

// BuildIncrementalGenerationFromBlockReader is the streaming form used by a
// live branch checkpoint. It buffers bounded data packs instead of copying
// every dirty 4 KiB payload before immutable publication.
func BuildIncrementalGenerationFromBlockReader(
	ctx context.Context,
	source RangeSource,
	base Descriptor,
	updates BlockUpdateReader,
	publisher ImmutableObjectPublisher,
	options BuildOptions,
) (BuildResult, error) {
	if updates == nil {
		return BuildResult{}, fmt.Errorf("block update reader is required")
	}
	if err := base.Validate(); err != nil {
		return BuildResult{}, err
	}
	blocks, err := updates.Blocks()
	if err != nil {
		return BuildResult{}, fmt.Errorf("list dirty blocks: %w", err)
	}
	blocks, err = normalizeDirtyBlocks(blocks, uint64(base.LogicalSizeBytes/LogicalBlockSize))
	if err != nil {
		return BuildResult{}, err
	}
	return buildIncrementalGeneration(ctx, source, base, updates, blocks, publisher, options)
}

func buildIncrementalGeneration(
	ctx context.Context,
	source RangeSource,
	base Descriptor,
	updates BlockUpdateReader,
	blocks []uint64,
	publisher ImmutableObjectPublisher,
	options BuildOptions,
) (BuildResult, error) {
	if source == nil || publisher == nil {
		return BuildResult{}, fmt.Errorf("range source and publisher are required")
	}
	options, err := inheritBuildFormat(options, base)
	if err != nil {
		return BuildResult{}, err
	}
	materializedBase := base
	materializedBase.CompositeTail = nil
	cache, err := NewReadCache(0)
	if err != nil {
		return BuildResult{}, err
	}
	reader, err := NewReaderWithCacheContext(ctx, source, materializedBase, cache)
	if err != nil {
		return BuildResult{}, err
	}
	totalBlocks := uint64(base.LogicalSizeBytes / LogicalBlockSize)
	if base.CompositeTail != nil {
		tail, _, err := DecodeCompositeTail(*base.CompositeTail, totalBlocks)
		if err != nil {
			return BuildResult{}, err
		}
		baseUpdates := mergeFinalBlockUpdates(tail, nil)
		baseBlocks := make([]uint64, len(baseUpdates))
		for index, update := range baseUpdates {
			baseBlocks[index] = update.Block
		}
		updates = &overlayBlockUpdateReader{
			base: &sliceBlockUpdateReader{updates: baseUpdates}, next: updates, nextBlocks: blocks,
		}
		blocks = mergeDirtyBlocks(baseBlocks, blocks)
	}
	plan, err := prepareMappingEdits(ctx, reader, blocks)
	if err != nil {
		return BuildResult{}, err
	}
	state := generationBuilder{
		ctx: ctx, publisher: publisher, options: options,
		references: make(map[string]ObjectReference),
	}
	defer state.encoder.close()
	dirty, err := state.publishBlockUpdates(updates, blocks, plan.leafEnd)
	if err != nil {
		return BuildResult{}, err
	}
	if err := plan.addData(dirty); err != nil {
		return BuildResult{}, err
	}
	err = publishMappingEdits(ctx, []*mappingEditPlan{plan}, options.PageEntries, func(pages []mappingEditPage) ([]publishedPage, error) {
		result := make([]publishedPage, len(pages))
		for index, page := range pages {
			var err error
			result[index], err = state.publishPage(page.page)
			if err != nil {
				return nil, err
			}
		}
		return result, nil
	})
	if err != nil {
		return BuildResult{}, err
	}
	root := plan.root.replacements[0]
	descriptor := Descriptor{
		Version: options.formatVersion(), LogicalSizeBytes: base.LogicalSizeBytes, BlockSizeBytes: LogicalBlockSize,
		MappingRoot: MappingRootLocator{
			Version: options.formatVersion(), RootDigest: root.object.Checksum, Object: root.object,
		},
	}
	payload, err := EncodeDescriptor(descriptor)
	if err != nil {
		return BuildResult{}, err
	}
	return BuildResult{
		Descriptor: descriptor, Payload: payload,
		Objects: state.objects, Bytes: state.bytes,
		References: sortedBatchObjectReferences(state.references),
	}, nil
}

type sliceBlockUpdateReader struct {
	updates []BlockUpdate
}

func (r *sliceBlockUpdateReader) Blocks() ([]uint64, error) {
	blocks := make([]uint64, len(r.updates))
	for index, update := range r.updates {
		blocks[index] = update.Block
	}
	return blocks, nil
}

func (r *sliceBlockUpdateReader) ReadBlock(block uint64, target []byte) (int, error) {
	index := sort.Search(len(r.updates), func(index int) bool { return r.updates[index].Block >= block })
	if index == len(r.updates) || r.updates[index].Block != block {
		return 0, fmt.Errorf("logical block %d is not present", block)
	}
	if len(target) != LogicalBlockSize {
		return 0, fmt.Errorf("block target must contain exactly %d bytes", LogicalBlockSize)
	}
	return copy(target, r.updates[index].Data), nil
}

type overlayBlockUpdateReader struct {
	base       BlockUpdateReader
	next       BlockUpdateReader
	nextBlocks []uint64
}

func (r *overlayBlockUpdateReader) Blocks() ([]uint64, error) {
	base, err := r.base.Blocks()
	if err != nil {
		return nil, err
	}
	return mergeDirtyBlocks(base, r.nextBlocks), nil
}

func (r *overlayBlockUpdateReader) ReadBlock(block uint64, target []byte) (int, error) {
	index := sort.Search(len(r.nextBlocks), func(index int) bool { return r.nextBlocks[index] >= block })
	if index < len(r.nextBlocks) && r.nextBlocks[index] == block {
		return r.next.ReadBlock(block, target)
	}
	return r.base.ReadBlock(block, target)
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

func normalizeDirtyBlocks(blocks []uint64, totalBlocks uint64) ([]uint64, error) {
	normalized := append([]uint64(nil), blocks...)
	sort.Slice(normalized, func(i, j int) bool { return normalized[i] < normalized[j] })
	for index, block := range normalized {
		if block >= totalBlocks {
			return nil, fmt.Errorf("dirty block %d is outside the device", block)
		}
		if index > 0 && normalized[index-1] == block {
			return nil, fmt.Errorf("dirty block %d is duplicated", block)
		}
	}
	return normalized, nil
}

func mergeDirtyBlocks(first, second []uint64) []uint64 {
	result := make([]uint64, 0, len(first)+len(second))
	left, right := 0, 0
	for left < len(first) || right < len(second) {
		switch {
		case left == len(first):
			result = append(result, second[right:]...)
			return result
		case right == len(second):
			result = append(result, first[left:]...)
			return result
		case first[left] < second[right]:
			result = append(result, first[left])
			left++
		case second[right] < first[left]:
			result = append(result, second[right])
			right++
		default:
			result = append(result, second[right])
			left++
			right++
		}
	}
	return result
}

func splitUnchangedEntries(reader *Reader, entries []MappingEntry, dirtyBlocks []uint64) ([]MappingEntry, error) {
	result := make([]MappingEntry, 0, len(entries))
	dirtyIndex := 0
	for _, entry := range entries {
		end := entry.LogicalStart + uint64(entry.BlockCount)
		for dirtyIndex < len(dirtyBlocks) && dirtyBlocks[dirtyIndex] < entry.LogicalStart {
			dirtyIndex++
		}
		entryDirtyStart := dirtyIndex
		for dirtyIndex < len(dirtyBlocks) && dirtyBlocks[dirtyIndex] < end {
			dirtyIndex++
		}
		if entryDirtyStart == dirtyIndex {
			result = append(result, entry)
			continue
		}
		if dirtyIndex-entryDirtyStart == int(entry.BlockCount) {
			continue
		}
		var payload []byte
		if entry.Object.Encoding == "" {
			var err error
			payload, err = reader.readRange(entry.Object)
			if err != nil {
				return nil, fmt.Errorf("read data range split by dirty blocks: %w", err)
			}
		}
		cursor := entry.LogicalStart
		for _, block := range dirtyBlocks[entryDirtyStart:dirtyIndex] {
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
	if entry.Object.Encoding != "" {
		entry.LogicalStart, entry.BlockCount = start, uint32(end-start)
		entry.DataOffset += uint32(startOffset)
		return entry
	}
	fragment := payload[startOffset : startOffset+length]
	return MappingEntry{
		LogicalStart: start, BlockCount: uint32(end - start), Kind: MappingEntryData,
		Object: ObjectRange{
			Key: entry.Object.Key, Offset: entry.Object.Offset + startOffset, Length: length,
			Checksum: digest.FromBytes(fragment).String(),
		},
	}
}

func (b *generationBuilder) publishBlockUpdates(updates BlockUpdateReader, blocks []uint64, leafEnd func(uint64) uint64) ([]MappingEntry, error) {
	pending := make([]pendingDataEntry, 0, max(1, b.options.PackBytes/b.options.DataRangeBytes))
	pendingBytes := 0
	result := make([]MappingEntry, 0, len(blocks))
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
	readBlock := func(block uint64) ([]byte, error) {
		if err := b.ctx.Err(); err != nil {
			return nil, err
		}
		payload := make([]byte, LogicalBlockSize)
		n, err := updates.ReadBlock(block, payload)
		if err != nil {
			return nil, fmt.Errorf("read dirty block %d: %w", block, err)
		}
		if n != len(payload) {
			return nil, fmt.Errorf("read dirty block %d: %w", block, io.ErrUnexpectedEOF)
		}
		return payload, nil
	}
	for index := 0; index < len(blocks); {
		data, err := readBlock(blocks[index])
		if err != nil {
			return nil, err
		}
		if allZero(data) {
			index++
			continue
		}
		start := blocks[index]
		end := leafEnd(start)
		if end <= start {
			return nil, fmt.Errorf("dirty block has no edited mapping leaf")
		}
		payload := make([]byte, 0, b.options.DataRangeBytes)
		payload = append(payload, data...)
		index++
		for index < len(blocks) && blocks[index] < end && blocks[index] == start+uint64(len(payload)/LogicalBlockSize) &&
			len(payload)+LogicalBlockSize <= b.options.DataRangeBytes {
			data, err = readBlock(blocks[index])
			if err != nil {
				return nil, err
			}
			index++
			if allZero(data) {
				break
			}
			payload = append(payload, data...)
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
