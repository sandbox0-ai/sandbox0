package rootfsblock

import (
	"context"
	"fmt"
	"math"
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
	id          string
	descriptor  Descriptor
	totalBlocks uint64
	entries     []MappingEntry
	root        *publishedPage
	pages       []publishedPage
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
	options, err := NormalizeBuildOptions(options)
	if err != nil {
		return BatchBuildResult{}, err
	}
	builder := batchObjectBuilder{
		ctx: ctx, publisher: publisher, options: options,
		references: make(map[string]ObjectReference),
		owners:     make([]map[string]ObjectReference, len(inputs)),
	}
	prepared := make([]batchPreparedGeneration, len(inputs))
	dirty := make([]batchDataItem, 0)
	seenIDs := make(map[string]struct{}, len(inputs))
	for index, input := range inputs {
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
		generation, items, err := prepareBatchIncrementalGeneration(ctx, source, input, index)
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
			Version: DescriptorVersion, LogicalSizeBytes: generation.descriptor.LogicalSizeBytes,
			BlockSizeBytes: LogicalBlockSize,
			MappingRoot: MappingRootLocator{
				Version:    MappingPageVersion,
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
) (batchPreparedGeneration, []batchDataItem, error) {
	if err := input.Descriptor.Validate(); err != nil {
		return batchPreparedGeneration{}, nil, err
	}
	if input.Descriptor.CompositeTail == nil {
		return batchPreparedGeneration{}, nil, fmt.Errorf("descriptor has no composite tail")
	}
	base := input.Descriptor
	base.CompositeTail = nil
	reader, err := NewReader(source, base, DefaultReadCacheBytes)
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
	baseEntries, err := reader.dataEntries(ctx)
	if err != nil {
		return batchPreparedGeneration{}, nil, err
	}
	unchanged, err := splitUnchangedEntries(reader, baseEntries, blocks)
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
		id: input.ID, descriptor: input.Descriptor, totalBlocks: totalBlocks,
		entries: unchanged,
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
		if len(generations[index].entries) > MaxMappingEntriesPerGeneration {
			return fmt.Errorf("generation %q has too many mapping entries", generations[index].id)
		}
	}
	return nil
}

type batchPagePlan struct {
	owner int
	root  bool
	page  MappingPage
}

func (b *batchObjectBuilder) publishMappingTrees(generations []batchPreparedGeneration) error {
	plans := make([]batchPagePlan, 0, len(generations))
	for owner := range generations {
		generation := &generations[owner]
		if len(generation.entries) <= b.options.PageEntries {
			plans = append(plans, batchPagePlan{owner: owner, root: true, page: MappingPage{
				StartBlock: 0, BlockCount: generation.totalBlocks, Entries: generation.entries,
			}})
			continue
		}
		for start := 0; start < len(generation.entries); start += b.options.PageEntries {
			end := min(start+b.options.PageEntries, len(generation.entries))
			first := generation.entries[start].LogicalStart
			last := generation.entries[end-1].LogicalStart + uint64(generation.entries[end-1].BlockCount)
			plans = append(plans, batchPagePlan{owner: owner, page: MappingPage{
				StartBlock: first, BlockCount: last - first, Entries: generation.entries[start:end],
			}})
		}
	}
	if err := b.publishPagePlans(generations, plans); err != nil {
		return err
	}
	for {
		plans = plans[:0]
		pending := 0
		for owner := range generations {
			generation := &generations[owner]
			if generation.root != nil {
				continue
			}
			pending++
			pages := generation.pages
			if len(pages) == 0 {
				return fmt.Errorf("generation %q has no mapping pages", generation.id)
			}
			if len(pages) <= b.options.PageEntries {
				page, err := batchMappingParentPage(pages, pages[0].level+1, true, generation.totalBlocks)
				if err != nil {
					return err
				}
				plans = append(plans, batchPagePlan{owner: owner, root: true, page: page})
				continue
			}
			for start := 0; start < len(pages); start += b.options.PageEntries {
				end := min(start+b.options.PageEntries, len(pages))
				page, err := batchMappingParentPage(pages[start:end], pages[start].level+1, false, generation.totalBlocks)
				if err != nil {
					return err
				}
				plans = append(plans, batchPagePlan{owner: owner, page: page})
			}
		}
		if pending == 0 {
			return nil
		}
		for index := range generations {
			if generations[index].root == nil {
				generations[index].pages = nil
			}
		}
		if err := b.publishPagePlans(generations, plans); err != nil {
			return err
		}
	}
}

func batchMappingParentPage(children []publishedPage, level uint8, root bool, totalBlocks uint64) (MappingPage, error) {
	if len(children) == 0 || level == 0 {
		return MappingPage{}, fmt.Errorf("mapping page children or level are invalid")
	}
	entries := make([]MappingEntry, 0, len(children))
	for _, child := range children {
		if child.count > math.MaxUint32 {
			return MappingPage{}, fmt.Errorf("mapping child covers too many blocks")
		}
		entries = append(entries, MappingEntry{
			LogicalStart: child.start, BlockCount: uint32(child.count), Kind: MappingEntryChild, Object: child.object,
		})
	}
	start := children[0].start
	count := children[len(children)-1].start + children[len(children)-1].count - start
	if root {
		start = 0
		count = totalBlocks
	}
	return MappingPage{Level: level, StartBlock: start, BlockCount: count, Entries: entries}, nil
}

func (b *batchObjectBuilder) publishPagePlans(generations []batchPreparedGeneration, plans []batchPagePlan) error {
	payloads := make([]batchPayload, len(plans))
	encoded := make([][]byte, len(plans))
	for index, plan := range plans {
		payload, err := EncodeMappingPage(plan.page)
		if err != nil {
			return err
		}
		encoded[index] = payload
		payloads[index] = batchPayload{owner: plan.owner, payload: payload}
	}
	locators, err := b.publishPayloads("map-packs", ObjectKindMappingPage, payloads)
	if err != nil {
		return err
	}
	for index, plan := range plans {
		page := publishedPage{
			start: plan.page.StartBlock, count: plan.page.BlockCount, level: plan.page.Level,
			object: locators[index], payload: encoded[index],
		}
		if plan.root {
			generations[plan.owner].root = &page
		} else {
			generations[plan.owner].pages = append(generations[plan.owner].pages, page)
		}
	}
	return nil
}

func (b *batchObjectBuilder) publishPayloads(kind, objectKind string, items []batchPayload) ([]ObjectRange, error) {
	locators := make([]ObjectRange, len(items))
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
			locators[index] = ObjectRange{
				Key: key, Offset: offset, Length: int64(len(item.payload)),
				Checksum: digest.FromBytes(item.payload).String(),
			}
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
