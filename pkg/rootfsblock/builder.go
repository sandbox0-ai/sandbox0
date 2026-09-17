package rootfsblock

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math"
	"path"
	"strings"

	"github.com/opencontainers/go-digest"
)

const (
	DefaultDataRangeBytes = CompressedDataRangeBytes
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
	// Zero selects the current durable Format2 defaults.
	FormatVersion      int `json:",omitempty"`
	DataRangeBytes     int
	PackBytes          int
	PageEntries        int
	ObjectPrefix       string
	MappingGroupPolicy string `json:",omitempty"`
}

type BuildResult struct {
	Descriptor Descriptor
	Payload    []byte
	Objects    int
	Bytes      int64
	// References contains every immutable object published by this build and
	// reachable from the resulting generation. The list is deduplicated and sorted so a
	// caller can durably inventory an exact retry before publishing metadata.
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

// ValidateObjectReference rejects unbounded or non-canonical immutable object
// identities before they enter a durable publication journal.
func ValidateObjectReference(reference ObjectReference) error {
	if reference.Key == "" || strings.TrimSpace(reference.Key) != reference.Key ||
		len(reference.Key) > MaxObjectKeyBytes || strings.HasPrefix(reference.Key, "/") ||
		strings.Contains(reference.Key, "\\") || path.Clean(reference.Key) != reference.Key ||
		reference.Key == "." || strings.HasPrefix(reference.Key, "../") ||
		reference.Size <= 0 || reference.Size > DefaultPackBytes {
		return fmt.Errorf("rootfs immutable object identity is invalid")
	}
	if reference.Kind != ObjectKindDataPack && reference.Kind != ObjectKindMappingPage {
		return fmt.Errorf("rootfs immutable object kind is invalid")
	}
	parsed, err := digest.Parse(reference.Checksum)
	if err != nil || parsed.Algorithm() != digest.SHA256 || parsed.String() != reference.Checksum {
		return fmt.Errorf("rootfs immutable object checksum must be canonical sha256")
	}
	return nil
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
	return buildMaterializedGeneration(ctx, reader, logicalSize, publisher, options, nil)
}

func buildMaterializedGeneration(
	ctx context.Context,
	reader io.ReaderAt,
	logicalSize int64,
	publisher ImmutableObjectPublisher,
	options BuildOptions,
	layout *DataRangeLayout,
) (BuildResult, error) {
	if reader == nil || publisher == nil {
		return BuildResult{}, fmt.Errorf("reader and publisher are required")
	}
	if logicalSize <= 0 || logicalSize%LogicalBlockSize != 0 {
		return BuildResult{}, fmt.Errorf("logical size must be a positive multiple of %d", LogicalBlockSize)
	}
	options, err := NormalizeBuildOptions(options)
	if err != nil {
		return BuildResult{}, err
	}
	if layout != nil && (options.formatVersion() != CompressedFormatVersion ||
		layout.logicalSize != logicalSize || layout.rangeBytes != int64(options.DataRangeBytes)) {
		return BuildResult{}, fmt.Errorf("data range layout must bind the same format-two image size and data unit")
	}
	state := generationBuilder{
		ctx: ctx, publisher: publisher, options: options,
		references: make(map[string]ObjectReference),
	}
	defer state.encoder.close()
	tree := newMappingStream(&state, uint64(logicalSize/LogicalBlockSize))
	pack := make([]pendingDataEntry, 0, options.PackBytes/options.DataRangeBytes)
	packBytes := 0
	payload := make([]byte, int(min(int64(options.DataRangeBytes), logicalSize)))
	cursor := dataRangeCursor{layout: layout}
	dataReader, sparse := reader.(materializedDataReader)
	for offset := int64(0); offset < logicalSize; {
		if err := ctx.Err(); err != nil {
			return BuildResult{}, err
		}
		if sparse {
			next, err := dataReader.nextDataOffset(ctx, offset)
			if err != nil {
				return BuildResult{}, fmt.Errorf("locate logical disk data at %d: %w", offset, err)
			}
			if next < offset || next > logicalSize {
				return BuildResult{}, fmt.Errorf("logical disk data offset is outside the remaining image")
			}
			if next == logicalSize {
				break
			}
			// Read the original complete unit containing the next data byte.
			// Re-basing a unit at an extent boundary would change publication.
			offset = cursor.rangeStart(next, options.DataRangeBytes)
		}
		length := cursor.nextLength(offset, logicalSize, options.DataRangeBytes)
		payload = payload[:int(length)]
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
				if err := tree.addEntries(published); err != nil {
					return BuildResult{}, err
				}
				clear(pack)
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
			// Nonzero bytes belong to the pending pack. Zero ranges reuse the
			// read buffer instead of allocating for every sparse image range.
			if offset+length < logicalSize {
				payload = make([]byte, int(min(int64(options.DataRangeBytes), logicalSize-offset-length)))
			}
		}
		offset += length
	}
	if len(pack) > 0 {
		published, err := state.publishPack(pack)
		if err != nil {
			return BuildResult{}, err
		}
		if err := tree.addEntries(published); err != nil {
			return BuildResult{}, err
		}
		clear(pack)
	}
	root, rootPayload, err := tree.finish()
	if err != nil {
		return BuildResult{}, err
	}
	descriptor := Descriptor{
		Version: options.formatVersion(), LogicalSizeBytes: logicalSize, BlockSizeBytes: LogicalBlockSize,
		MappingRoot: MappingRootLocator{Version: options.formatVersion(), RootDigest: digest.FromBytes(rootPayload).String(), Object: root},
	}
	descriptorPayload, err := EncodeDescriptor(descriptor)
	if err != nil {
		return BuildResult{}, err
	}
	return BuildResult{
		Descriptor: descriptor, Payload: descriptorPayload,
		Objects: state.objects, Bytes: state.bytes,
		References: sortedBatchObjectReferences(state.references),
	}, nil
}

type generationBuilder struct {
	ctx        context.Context
	publisher  ImmutableObjectPublisher
	options    BuildOptions
	objects    int
	bytes      int64
	references map[string]ObjectReference
	encoder    rangeEncoder
}

func (b *generationBuilder) publishPack(pending []pendingDataEntry) ([]MappingEntry, error) {
	size := 0
	for _, item := range pending {
		size += len(item.data)
	}
	payload := make([]byte, 0, size)
	entries := make([]MappingEntry, len(pending))
	for index, item := range pending {
		data, object, err := b.encoder.encode(b.ctx, item.data)
		if err != nil {
			return nil, err
		}
		item.entry.Object = object
		item.entry.Object.Offset = int64(len(payload))
		entries[index] = item.entry
		payload = append(payload, data...)
	}
	key, err := b.publish("packs", payload)
	if err != nil {
		return nil, err
	}
	for index := range entries {
		entries[index].Object.Key = key
	}
	return entries, nil
}

type publishedPage struct {
	start   uint64
	count   uint64
	level   uint8
	object  ObjectRange
	payload []byte
	stored  []byte
}

func (b *generationBuilder) publishMappingTree(entries []MappingEntry, totalBlocks uint64) (ObjectRange, []byte, error) {
	tree := newMappingStream(b, totalBlocks)
	if err := tree.addEntries(entries); err != nil {
		return ObjectRange{}, nil, err
	}
	return tree.finish()
}

func (b *generationBuilder) publishInternalPage(children []publishedPage, level uint8, root bool, totalBlocks uint64) (publishedPage, error) {
	page, err := internalMappingPage(children, level, root, totalBlocks)
	if err != nil {
		return publishedPage{}, err
	}
	return b.publishPage(page)
}

func internalMappingPage(children []publishedPage, level uint8, root bool, totalBlocks uint64) (MappingPage, error) {
	entries := make([]MappingEntry, 0, len(children))
	for _, child := range children {
		if child.count > math.MaxUint32 {
			return MappingPage{}, fmt.Errorf("mapping child covers too many blocks")
		}
		entries = append(entries, MappingEntry{LogicalStart: child.start, BlockCount: uint32(child.count), Kind: MappingEntryChild, Object: child.object})
	}
	start := children[0].start
	count := children[len(children)-1].start + children[len(children)-1].count - start
	if root {
		start = 0
		count = totalBlocks
	}
	return MappingPage{Level: level, StartBlock: start, BlockCount: count, Entries: entries}, nil
}

func (b *generationBuilder) publishRootPage(page MappingPage) (ObjectRange, []byte, error) {
	published, err := b.publishPage(page)
	return published.object, published.payload, err
}

func (b *generationBuilder) publishPage(page MappingPage) (publishedPage, error) {
	prepared, err := b.prepareMappingPage(page)
	if err != nil {
		return publishedPage{}, err
	}
	pages, err := b.publishMappingGroup([]publishedPage{prepared})
	if err != nil {
		return publishedPage{}, err
	}
	return pages[0], nil
}

func (b *generationBuilder) prepareMappingPage(page MappingPage) (publishedPage, error) {
	page.Version = b.options.FormatVersion
	payload, err := EncodeMappingPage(page)
	if err != nil {
		return publishedPage{}, err
	}
	stored, object, err := b.encoder.encode(b.ctx, payload)
	if err != nil {
		return publishedPage{}, err
	}
	return publishedPage{
		start: page.StartBlock, count: page.BlockCount, level: page.Level, payload: payload,
		object: object, stored: stored,
	}, nil
}

func (b *generationBuilder) publish(kind string, payload []byte) (string, error) {
	if err := b.ctx.Err(); err != nil {
		return "", err
	}
	var objectKind string
	switch kind {
	case "packs":
		objectKind = ObjectKindDataPack
	case "maps":
		objectKind = ObjectKindMappingPage
	default:
		return "", fmt.Errorf("unsupported immutable object kind %q", kind)
	}
	value := digest.FromBytes(payload)
	key := fmt.Sprintf("%s/%s/sha256/%s", b.options.ObjectPrefix, kind, value.Encoded())
	if err := b.publisher.PutImmutable(b.ctx, key, payload); err != nil {
		return "", fmt.Errorf("publish immutable %s object: %w", kind, err)
	}
	b.objects++
	b.bytes += int64(len(payload))
	if b.references == nil {
		b.references = make(map[string]ObjectReference)
	}
	b.references[key] = ObjectReference{
		Key: key, Kind: objectKind, Size: int64(len(payload)), Checksum: value.String(),
	}
	return key, nil
}

// NormalizeBuildOptions applies production defaults and validates immutable
// block publication bounds.
func NormalizeBuildOptions(options BuildOptions) (BuildOptions, error) {
	if options.FormatVersion == 0 {
		options.FormatVersion = DescriptorVersion
	}
	if options.MappingGroupPolicy != "" && (options.MappingGroupPolicy != ContiguousMappingV1 || options.FormatVersion != CompressedFormatVersion) {
		return BuildOptions{}, fmt.Errorf("unsupported mapping group policy or format")
	}
	if options.FormatVersion != DescriptorVersion {
		return BuildOptions{}, fmt.Errorf("unsupported build format version %d", options.FormatVersion)
	}
	if options.DataRangeBytes == 0 {
		options.DataRangeBytes = DefaultDataRangeBytes
	}
	if options.PackBytes == 0 {
		options.PackBytes = DefaultPackBytes
	}
	if options.PageEntries == 0 {
		options.PageEntries = DefaultPageEntries
	}
	if options.ObjectPrefix == "" {
		options.ObjectPrefix = fmt.Sprintf("rootfs/v%d", options.formatVersion())
	}
	if err := ValidateObjectPrefix(options.ObjectPrefix); err != nil {
		return BuildOptions{}, err
	}
	if options.DataRangeBytes <= 0 || options.DataRangeBytes > MaxDataRangeBytes || options.DataRangeBytes%LogicalBlockSize != 0 {
		return BuildOptions{}, fmt.Errorf("data range must be a positive block-aligned value no greater than %d", MaxDataRangeBytes)
	}
	if options.DataRangeBytes > CompressedDataRangeBytes {
		return BuildOptions{}, fmt.Errorf("compressed data range exceeds %d bytes", CompressedDataRangeBytes)
	}
	if options.PackBytes < options.DataRangeBytes || options.PackBytes > DefaultPackBytes ||
		options.PackBytes%options.DataRangeBytes != 0 {
		return BuildOptions{}, fmt.Errorf("pack size must be a positive multiple of the data range and no greater than %d", DefaultPackBytes)
	}
	if options.PageEntries < 2 || options.PageEntries > MaxMappingPageEntries {
		return BuildOptions{}, fmt.Errorf("mapping page entry limit is invalid")
	}
	return options, nil
}

func (o BuildOptions) formatVersion() int {
	if o.FormatVersion == 0 {
		return DescriptorVersion
	}
	return o.FormatVersion
}

func inheritBuildFormat(options BuildOptions, base Descriptor) (BuildOptions, error) {
	if options.MappingGroupPolicy != "" {
		return BuildOptions{}, fmt.Errorf("mapping group policy applies to materialized image imports only")
	}
	if options.FormatVersion == 0 {
		options.FormatVersion = base.Version
	}
	if options.formatVersion() != base.Version {
		return BuildOptions{}, fmt.Errorf("incremental publication cannot change RootFS format")
	}
	return NormalizeBuildOptions(options)
}

// ValidateObjectPrefix rejects paths that could escape or alias a caller's
// immutable object namespace. Prefixes are accepted only in their canonical
// form because silently trimming a durable operation input would make retries
// ambiguous.
func ValidateObjectPrefix(value string) error {
	if value == "" || value != strings.TrimSpace(value) || value != strings.Trim(value, "/") || len(value) > 512 {
		return fmt.Errorf("rootfs object prefix must be a canonical non-empty path within 512 bytes")
	}
	for _, segment := range strings.Split(value, "/") {
		if segment == "" || segment == "." || segment == ".." || len(segment) > 128 {
			return fmt.Errorf("rootfs object prefix contains an invalid path segment")
		}
		for _, character := range segment {
			if character >= 'a' && character <= 'z' || character >= 'A' && character <= 'Z' ||
				character >= '0' && character <= '9' || strings.ContainsRune("._:-", character) {
				continue
			}
			return fmt.Errorf("rootfs object prefix contains an invalid character")
		}
	}
	return nil
}

func allZero(payload []byte) bool {
	return len(bytes.Trim(payload, "\x00")) == 0
}
