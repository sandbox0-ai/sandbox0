package rootfsblock

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/opencontainers/go-digest"
)

// BatchIncrementalInput identifies one composite generation. ID is an
// orchestration identity and is never encoded into immutable object content.
type BatchIncrementalInput struct {
	ID         string
	Descriptor Descriptor
}

// BatchBuildResult separates shared physical publication cost from the
// independent descriptors produced for each logical generation.
type BatchBuildResult struct {
	Results    map[string]BuildResult
	Objects    int
	Bytes      int64
	References []ObjectReference
}

type batchPreparedGeneration struct {
	id         string
	descriptor Descriptor
	entries    []MappingEntry
	root       *publishedPage
	plan       *mappingEditPlan
}

type batchDataItem struct {
	owner int
	entry MappingEntry
	data  []byte
}

type batchPayload struct {
	owner   int
	payload []byte
}

type batchObjectBuilder struct {
	ctx        context.Context
	publisher  ImmutableObjectPublisher
	options    BuildOptions
	objects    int
	bytes      int64
	references map[string]ObjectReference
	owners     []map[string]ObjectReference
	encoder    rangeEncoder
}

// BuildIncrementalGenerationsBatch materializes multiple complete logical
// generations while sharing bounded data and mapping packs. Each descriptor
// points only at its own immutable ranges even when an object also contains
// ranges owned by other generations in the same isolation lane.
func BuildIncrementalGenerationsBatch(
	ctx context.Context,
	source RangeSource,
	inputs []BatchIncrementalInput,
	publisher ImmutableObjectPublisher,
	options BuildOptions,
) (BatchBuildResult, error) {
	if source == nil || publisher == nil {
		return BatchBuildResult{}, fmt.Errorf("range source and publisher are required")
	}
	if len(inputs) == 0 {
		return BatchBuildResult{}, fmt.Errorf("at least one incremental generation is required")
	}
	options, err := inheritBuildFormat(options, inputs[0].Descriptor)
	if err != nil {
		return BatchBuildResult{}, err
	}
	builder := batchObjectBuilder{
		ctx: ctx, publisher: publisher, options: options,
		references: make(map[string]ObjectReference),
		owners:     make([]map[string]ObjectReference, len(inputs)),
	}
	defer builder.encoder.close()
	prepared := make([]batchPreparedGeneration, len(inputs))
	cache, err := NewReadCache(DefaultReadCacheBytes)
	if err != nil {
		return BatchBuildResult{}, err
	}
	dirty := make([]batchDataItem, 0)
	seenIDs := make(map[string]struct{}, len(inputs))
	for index, input := range inputs {
		if input.Descriptor.Version != options.formatVersion() {
			return BatchBuildResult{}, fmt.Errorf("batch cannot mix RootFS formats")
		}
		if err := ctx.Err(); err != nil {
			return BatchBuildResult{}, err
		}
		input.ID = strings.TrimSpace(input.ID)
		if input.ID == "" {
			return BatchBuildResult{}, fmt.Errorf("batch generation %d has an empty ID", index)
		}
		if _, exists := seenIDs[input.ID]; exists {
			return BatchBuildResult{}, fmt.Errorf("batch generation ID %q is duplicated", input.ID)
		}
		seenIDs[input.ID] = struct{}{}
		generation, items, err := prepareBatchIncrementalGeneration(ctx, source, input, index, cache)
		if err != nil {
			return BatchBuildResult{}, fmt.Errorf("prepare generation %q: %w", input.ID, err)
		}
		prepared[index] = generation
		dirty = append(dirty, items...)
	}
	if err := builder.publishDataItems(prepared, dirty); err != nil {
		return BatchBuildResult{}, err
	}
	if err := builder.publishMappingTrees(prepared); err != nil {
		return BatchBuildResult{}, err
	}

	results := make(map[string]BuildResult, len(prepared))
	for index := range prepared {
		generation := &prepared[index]
		if generation.root == nil {
			return BatchBuildResult{}, fmt.Errorf("generation %q has no mapping root", generation.id)
		}
		descriptor := Descriptor{
			Version: options.formatVersion(), LogicalSizeBytes: generation.descriptor.LogicalSizeBytes,
			BlockSizeBytes: LogicalBlockSize,
			MappingRoot: MappingRootLocator{
				Version:    options.formatVersion(),
				RootDigest: digest.FromBytes(generation.root.payload).String(),
				Object:     generation.root.object,
			},
		}
		payload, err := EncodeDescriptor(descriptor)
		if err != nil {
			return BatchBuildResult{}, fmt.Errorf("encode generation %q descriptor: %w", generation.id, err)
		}
		references := sortedBatchObjectReferences(builder.owners[index])
		var publishedBytes int64
		for _, reference := range references {
			publishedBytes += reference.Size
		}
		results[generation.id] = BuildResult{
			Descriptor: descriptor, Payload: payload,
			Objects: len(references), Bytes: publishedBytes, References: references,
		}
	}
	return BatchBuildResult{
		Results: results, Objects: builder.objects, Bytes: builder.bytes,
		References: sortedBatchObjectReferences(builder.references),
	}, nil
}

func prepareBatchIncrementalGeneration(
	ctx context.Context,
	source RangeSource,
	input BatchIncrementalInput,
	owner int,
	cache *ReadCache,
) (batchPreparedGeneration, []batchDataItem, error) {
	if err := input.Descriptor.Validate(); err != nil {
		return batchPreparedGeneration{}, nil, err
	}
	if input.Descriptor.CompositeTail == nil {
		return batchPreparedGeneration{}, nil, fmt.Errorf("descriptor has no composite tail")
	}
	base := input.Descriptor
	base.CompositeTail = nil
	reader, err := NewReaderWithCacheContext(ctx, source, base, cache)
	if err != nil {
		return batchPreparedGeneration{}, nil, err
	}
	totalBlocks := uint64(input.Descriptor.LogicalSizeBytes / LogicalBlockSize)
	tail, _, err := DecodeCompositeTail(*input.Descriptor.CompositeTail, totalBlocks)
	if err != nil {
		return batchPreparedGeneration{}, nil, err
	}
	updates := mergeFinalBlockUpdates(tail, nil)
	blocks := make([]uint64, len(updates))
	for index, update := range updates {
		blocks[index] = update.Block
	}
	plan, err := prepareMappingEdits(ctx, reader, blocks)
	if err != nil {
		return batchPreparedGeneration{}, nil, err
	}
	dirty := make([]batchDataItem, 0, len(updates))
	for _, update := range updates {
		if allZero(update.Data) {
			continue
		}
		payload := append([]byte(nil), update.Data...)
		dirty = append(dirty, batchDataItem{
			owner: owner,
			entry: MappingEntry{
				LogicalStart: update.Block, BlockCount: 1, Kind: MappingEntryData,
				Object: ObjectRange{Length: LogicalBlockSize, Checksum: digest.FromBytes(payload).String()},
			},
			data: payload,
		})
	}
	return batchPreparedGeneration{
		id: input.ID, descriptor: input.Descriptor, plan: plan,
	}, dirty, nil
}

func (b *batchObjectBuilder) publishDataItems(generations []batchPreparedGeneration, items []batchDataItem) error {
	payloads := make([]batchPayload, len(items))
	for index, item := range items {
		payloads[index] = batchPayload{owner: item.owner, payload: item.data}
	}
	locators, err := b.publishPayloads("packs", ObjectKindDataPack, payloads)
	if err != nil {
		return err
	}
	for index, item := range items {
		item.entry.Object = locators[index]
		generations[item.owner].entries = append(generations[item.owner].entries, item.entry)
	}
	for index := range generations {
		sort.Slice(generations[index].entries, func(left, right int) bool {
			return generations[index].entries[left].LogicalStart < generations[index].entries[right].LogicalStart
		})
		if err := generations[index].plan.addData(generations[index].entries); err != nil {
			return err
		}
		generations[index].entries = nil
	}
	return nil
}

func (b *batchObjectBuilder) publishMappingTrees(generations []batchPreparedGeneration) error {
	plans := make([]*mappingEditPlan, len(generations))
	for index := range generations {
		plans[index] = generations[index].plan
	}
	err := publishMappingEdits(b.ctx, plans, b.options.PageEntries, func(pages []mappingEditPage) ([]publishedPage, error) {
		payloads := make([]batchPayload, len(pages))
		for index, page := range pages {
			page.page.Version = b.options.FormatVersion
			payload, err := EncodeMappingPage(page.page)
			if err != nil {
				return nil, err
			}
			payloads[index] = batchPayload{owner: page.owner, payload: payload}
		}
		locators, err := b.publishPayloads("map-packs", ObjectKindMappingPage, payloads)
		if err != nil {
			return nil, err
		}
		published := make([]publishedPage, len(pages))
		for index, page := range pages {
			published[index] = publishedPage{
				start: page.page.StartBlock, count: page.page.BlockCount, level: page.page.Level,
				object: locators[index], payload: payloads[index].payload,
			}
		}
		return published, nil
	})
	if err != nil {
		return err
	}
	for index := range generations {
		generations[index].root = &plans[index].root.replacements[0]
	}
	return nil
}

func (b *batchObjectBuilder) publishPayloads(kind, objectKind string, items []batchPayload) ([]ObjectRange, error) {
	locators := make([]ObjectRange, len(items))
	stored := make([]batchPayload, len(items))
	for index, item := range items {
		stored[index] = item
		locators[index] = ObjectRange{Length: int64(len(item.payload)), Checksum: digest.FromBytes(item.payload).String()}
		if b.options.formatVersion() == CompressedFormatVersion {
			var err error
			stored[index].payload, locators[index], err = b.encoder.encode(b.ctx, item.payload)
			if err != nil {
				return nil, err
			}
		}
	}
	items = stored
	for start := 0; start < len(items); {
		if err := b.ctx.Err(); err != nil {
			return nil, err
		}
		end := start
		size := 0
		for end < len(items) {
			itemBytes := len(items[end].payload)
			if end > start && size+itemBytes > b.options.PackBytes {
				break
			}
			size += itemBytes
			end++
			if size >= b.options.PackBytes {
				break
			}
		}
		payload := make([]byte, 0, size)
		for index := start; index < end; index++ {
			payload = append(payload, items[index].payload...)
		}
		objectDigest := digest.FromBytes(payload)
		key := fmt.Sprintf("%s/%s/sha256/%s", b.options.ObjectPrefix, kind, objectDigest.Encoded())
		if err := b.publisher.PutImmutable(b.ctx, key, payload); err != nil {
			return nil, fmt.Errorf("publish immutable %s object: %w", kind, err)
		}
		reference := ObjectReference{
			Key: key, Kind: objectKind, Size: int64(len(payload)), Checksum: objectDigest.String(),
		}
		b.reference(reference)
		b.objects++
		b.bytes += int64(len(payload))
		offset := int64(0)
		for index := start; index < end; index++ {
			item := items[index]
			locators[index].Key, locators[index].Offset = key, offset
			b.referenceOwner(item.owner, reference)
			offset += int64(len(item.payload))
		}
		start = end
	}
	return locators, nil
}

func (b *batchObjectBuilder) reference(reference ObjectReference) {
	if reference.Key != "" {
		b.references[reference.Key] = reference
	}
}

func (b *batchObjectBuilder) referenceOwner(owner int, reference ObjectReference) {
	if owner < 0 || owner >= len(b.owners) || reference.Key == "" {
		return
	}
	if b.owners[owner] == nil {
		b.owners[owner] = make(map[string]ObjectReference)
	}
	b.owners[owner][reference.Key] = reference
}

func sortedBatchObjectReferences(references map[string]ObjectReference) []ObjectReference {
	result := make([]ObjectReference, 0, len(references))
	for _, reference := range references {
		result = append(result, reference)
	}
	sort.Slice(result, func(left, right int) bool { return result[left].Key < result[right].Key })
	return result
}
