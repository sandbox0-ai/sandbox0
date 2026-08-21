package rootfsblock

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math"
	"strings"

	"github.com/opencontainers/go-digest"
)

const (
	DefaultDataRangeBytes = 8 << 20
	DefaultPackBytes      = 64 << 20
	DefaultPageEntries    = 1024
)

// ImmutableObjectPublisher creates a content-addressed object. Exact retries
// with identical bytes must succeed; an existing key with different bytes
// must fail.
type ImmutableObjectPublisher interface {
	PutImmutable(context.Context, string, []byte) error
}

type BuildOptions struct {
	DataRangeBytes int
	PackBytes      int
	PageEntries    int
	ObjectPrefix   string
}

type BuildResult struct {
	Descriptor Descriptor
	Payload    []byte
	Objects    int
	Bytes      int64
	// References contains every object published by a batch build and
	// reachable from this generation. Single-generation builders leave it
	// empty until their publication path needs durable object inventory.
	References []ObjectReference
}

// ObjectReference identifies one complete immutable object published while
// building a generation. Checksum covers the complete object, not one range.
type ObjectReference struct {
	Key      string
	Kind     string
	Size     int64
	Checksum string
}

const (
	ObjectKindDataPack    = "data_pack"
	ObjectKindMappingPage = "mapping_page"
)

type pendingDataEntry struct {
	entry MappingEntry
	data  []byte
}

// BuildMaterializedGeneration publishes one immutable complete block map from
// a logical disk image. Zero ranges remain implicit and therefore create no
// data object. Data and mapping objects use content-addressed keys.
func BuildMaterializedGeneration(
	ctx context.Context,
	reader io.ReaderAt,
	logicalSize int64,
	publisher ImmutableObjectPublisher,
	options BuildOptions,
) (BuildResult, error) {
	if reader == nil || publisher == nil {
		return BuildResult{}, fmt.Errorf("reader and publisher are required")
	}
	if logicalSize <= 0 || logicalSize%LogicalBlockSize != 0 {
		return BuildResult{}, fmt.Errorf("logical size must be a positive multiple of %d", LogicalBlockSize)
	}
	options, err := normalizeBuildOptions(options)
	if err != nil {
		return BuildResult{}, err
	}
	state := generationBuilder{ctx: ctx, publisher: publisher, options: options}
	entries := make([]MappingEntry, 0)
	pack := make([]pendingDataEntry, 0, options.PackBytes/options.DataRangeBytes)
	packBytes := 0
	for offset := int64(0); offset < logicalSize; {
		if err := ctx.Err(); err != nil {
			return BuildResult{}, err
		}
		length := min(int64(options.DataRangeBytes), logicalSize-offset)
		payload := make([]byte, int(length))
		n, readErr := reader.ReadAt(payload, offset)
		if readErr != nil && readErr != io.EOF {
			return BuildResult{}, fmt.Errorf("read logical disk at %d: %w", offset, readErr)
		}
		if n != len(payload) {
			return BuildResult{}, fmt.Errorf("logical disk ended at %d after %d of %d bytes", offset, n, len(payload))
		}
		if !allZero(payload) {
			if packBytes+len(payload) > options.PackBytes && len(pack) > 0 {
				published, err := state.publishPack(pack)
				if err != nil {
					return BuildResult{}, err
				}
				entries = append(entries, published...)
				pack = pack[:0]
				packBytes = 0
			}
			blocks := length / LogicalBlockSize
			if blocks > math.MaxUint32 {
				return BuildResult{}, fmt.Errorf("data range has too many logical blocks")
			}
			pack = append(pack, pendingDataEntry{
				entry: MappingEntry{
					LogicalStart: uint64(offset / LogicalBlockSize), BlockCount: uint32(blocks), Kind: MappingEntryData,
					Object: ObjectRange{Offset: int64(packBytes), Length: length, Checksum: digest.FromBytes(payload).String()},
				},
				data: payload,
			})
			packBytes += len(payload)
		}
		offset += length
	}
	if len(pack) > 0 {
		published, err := state.publishPack(pack)
		if err != nil {
			return BuildResult{}, err
		}
		entries = append(entries, published...)
	}
	root, rootPayload, err := state.publishMappingTree(entries, uint64(logicalSize/LogicalBlockSize))
	if err != nil {
		return BuildResult{}, err
	}
	descriptor := Descriptor{
		Version: DescriptorVersion, LogicalSizeBytes: logicalSize, BlockSizeBytes: LogicalBlockSize,
		MappingRoot: MappingRootLocator{Version: MappingPageVersion, RootDigest: digest.FromBytes(rootPayload).String(), Object: root},
	}
	descriptorPayload, err := EncodeDescriptor(descriptor)
	if err != nil {
		return BuildResult{}, err
	}
	return BuildResult{Descriptor: descriptor, Payload: descriptorPayload, Objects: state.objects, Bytes: state.bytes}, nil
}

type generationBuilder struct {
	ctx       context.Context
	publisher ImmutableObjectPublisher
	options   BuildOptions
	objects   int
	bytes     int64
}

func (b *generationBuilder) publishPack(pending []pendingDataEntry) ([]MappingEntry, error) {
	size := 0
	for _, item := range pending {
		size += len(item.data)
	}
	payload := make([]byte, 0, size)
	for _, item := range pending {
		payload = append(payload, item.data...)
	}
	key, err := b.publish("packs", payload)
	if err != nil {
		return nil, err
	}
	entries := make([]MappingEntry, len(pending))
	for index, item := range pending {
		item.entry.Object.Key = key
		entries[index] = item.entry
	}
	return entries, nil
}

type publishedPage struct {
	start   uint64
	count   uint64
	level   uint8
	object  ObjectRange
	payload []byte
}

func (b *generationBuilder) publishMappingTree(entries []MappingEntry, totalBlocks uint64) (ObjectRange, []byte, error) {
	if len(entries) <= b.options.PageEntries {
		return b.publishRootPage(MappingPage{StartBlock: 0, BlockCount: totalBlocks, Entries: entries})
	}
	pages := make([]publishedPage, 0, (len(entries)+b.options.PageEntries-1)/b.options.PageEntries)
	for start := 0; start < len(entries); start += b.options.PageEntries {
		end := min(start+b.options.PageEntries, len(entries))
		first := entries[start].LogicalStart
		last := entries[end-1].LogicalStart + uint64(entries[end-1].BlockCount)
		page, err := b.publishPage(MappingPage{StartBlock: first, BlockCount: last - first, Entries: entries[start:end]})
		if err != nil {
			return ObjectRange{}, nil, err
		}
		pages = append(pages, page)
	}
	for len(pages) > b.options.PageEntries {
		if pages[0].level == math.MaxUint8 {
			return ObjectRange{}, nil, fmt.Errorf("mapping tree is too deep")
		}
		next := make([]publishedPage, 0, (len(pages)+b.options.PageEntries-1)/b.options.PageEntries)
		for start := 0; start < len(pages); start += b.options.PageEntries {
			end := min(start+b.options.PageEntries, len(pages))
			page, err := b.publishInternalPage(pages[start:end], pages[start].level+1, false, totalBlocks)
			if err != nil {
				return ObjectRange{}, nil, err
			}
			next = append(next, page)
		}
		pages = next
	}
	root, err := b.publishInternalPage(pages, pages[0].level+1, true, totalBlocks)
	if err != nil {
		return ObjectRange{}, nil, err
	}
	return root.object, root.payload, nil
}

func (b *generationBuilder) publishInternalPage(children []publishedPage, level uint8, root bool, totalBlocks uint64) (publishedPage, error) {
	entries := make([]MappingEntry, 0, len(children))
	for _, child := range children {
		if child.count > math.MaxUint32 {
			return publishedPage{}, fmt.Errorf("mapping child covers too many blocks")
		}
		entries = append(entries, MappingEntry{LogicalStart: child.start, BlockCount: uint32(child.count), Kind: MappingEntryChild, Object: child.object})
	}
	start := children[0].start
	count := children[len(children)-1].start + children[len(children)-1].count - start
	if root {
		start = 0
		count = totalBlocks
	}
	return b.publishPage(MappingPage{Level: level, StartBlock: start, BlockCount: count, Entries: entries})
}

func (b *generationBuilder) publishRootPage(page MappingPage) (ObjectRange, []byte, error) {
	published, err := b.publishPage(page)
	return published.object, published.payload, err
}

func (b *generationBuilder) publishPage(page MappingPage) (publishedPage, error) {
	payload, err := EncodeMappingPage(page)
	if err != nil {
		return publishedPage{}, err
	}
	key, err := b.publish("maps", payload)
	if err != nil {
		return publishedPage{}, err
	}
	return publishedPage{
		start: page.StartBlock, count: page.BlockCount, level: page.Level, payload: payload,
		object: ObjectRange{Key: key, Length: int64(len(payload)), Checksum: digest.FromBytes(payload).String()},
	}, nil
}

func (b *generationBuilder) publish(kind string, payload []byte) (string, error) {
	value := digest.FromBytes(payload)
	key := fmt.Sprintf("%s/%s/sha256/%s", b.options.ObjectPrefix, kind, value.Encoded())
	if err := b.publisher.PutImmutable(b.ctx, key, payload); err != nil {
		return "", fmt.Errorf("publish immutable %s object: %w", kind, err)
	}
	b.objects++
	b.bytes += int64(len(payload))
	return key, nil
}

func normalizeBuildOptions(options BuildOptions) (BuildOptions, error) {
	if options.DataRangeBytes == 0 {
		options.DataRangeBytes = DefaultDataRangeBytes
	}
	if options.PackBytes == 0 {
		options.PackBytes = DefaultPackBytes
	}
	if options.PageEntries == 0 {
		options.PageEntries = DefaultPageEntries
	}
	options.ObjectPrefix = strings.Trim(strings.TrimSpace(options.ObjectPrefix), "/")
	if options.ObjectPrefix == "" {
		options.ObjectPrefix = "rootfs/v1"
	}
	if options.DataRangeBytes <= 0 || options.DataRangeBytes > MaxDataRangeBytes || options.DataRangeBytes%LogicalBlockSize != 0 {
		return BuildOptions{}, fmt.Errorf("data range must be a positive block-aligned value no greater than %d", MaxDataRangeBytes)
	}
	if options.PackBytes < options.DataRangeBytes || options.PackBytes%options.DataRangeBytes != 0 {
		return BuildOptions{}, fmt.Errorf("pack size must be a positive multiple of the data range")
	}
	if options.PageEntries < 2 || options.PageEntries > MaxMappingPageEntries {
		return BuildOptions{}, fmt.Errorf("mapping page entry limit is invalid")
	}
	return options, nil
}

func allZero(payload []byte) bool {
	return len(bytes.Trim(payload, "\x00")) == 0
}
