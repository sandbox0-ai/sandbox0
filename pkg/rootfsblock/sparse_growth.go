package rootfsblock

import (
	"context"
	"fmt"
)

// GrowSparseGeneration extends only the mapping root's zero-filled address
// space. Existing data and child pages remain immutable and are not copied.
// This does not grow the filesystem stored inside the virtual block device.
func GrowSparseGeneration(ctx context.Context, source RangeSource, base Descriptor, logicalSize int64, publisher ImmutableObjectPublisher, options BuildOptions) (BuildResult, error) {
	if err := base.Validate(); err != nil {
		return BuildResult{}, err
	}
	if source == nil || publisher == nil {
		return BuildResult{}, fmt.Errorf("range source and publisher are required")
	}
	if logicalSize < base.LogicalSizeBytes || logicalSize%LogicalBlockSize != 0 {
		return BuildResult{}, fmt.Errorf("sparse device growth must be block-aligned and cannot shrink")
	}
	if logicalSize == base.LogicalSizeBytes {
		payload, err := EncodeDescriptor(base)
		return BuildResult{Descriptor: base, Payload: payload}, err
	}
	options, err := inheritBuildFormat(options, base)
	if err != nil {
		return BuildResult{}, err
	}
	reader, err := NewReader(source, base, 0)
	if err != nil {
		return BuildResult{}, err
	}
	defer reader.cache.Close()
	root := reader.root
	root.BlockCount = uint64(logicalSize / LogicalBlockSize)
	state := generationBuilder{ctx: ctx, publisher: publisher, options: options, references: make(map[string]ObjectReference)}
	defer state.encoder.close()
	published, err := state.publishPage(root)
	if err != nil {
		return BuildResult{}, err
	}
	descriptor := base
	descriptor.LogicalSizeBytes = logicalSize
	descriptor.MappingRoot = MappingRootLocator{Version: descriptor.Version, RootDigest: published.object.Checksum, Object: published.object}
	payload, err := EncodeDescriptor(descriptor)
	if err != nil {
		return BuildResult{}, err
	}
	return BuildResult{Descriptor: descriptor, Payload: payload, Objects: state.objects, Bytes: state.bytes, References: sortedBatchObjectReferences(state.references)}, nil
}
