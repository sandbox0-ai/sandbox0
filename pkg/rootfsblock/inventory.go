package rootfsblock

import (
	"context"
	"fmt"
)

// InspectInventoryMappingPage validates one persisted traversal step without
// reading data packs or accumulating a whole generation's reference graph.
// expectedLevel is -1 only for the descriptor's root; children bind the exact
// level and logical coverage authenticated by their parent mapping entry.
func InspectInventoryMappingPage(ctx context.Context, source RangeSource, version int, object ObjectRange, startBlock, blockCount uint64, expectedLevel int) (MappingPage, error) {
	if source == nil || expectedLevel < -1 || expectedLevel >= MaxChangedBlockMappingDepth || blockCount == 0 {
		return MappingPage{}, fmt.Errorf("invalid mapping inventory step")
	}
	if err := object.Validate(MaxMappingRootBytes); err != nil {
		return MappingPage{}, err
	}
	iterator := mappingExtentIterator{ctx: ctx, source: source, version: version}
	page, err := iterator.readPage(object, object.Checksum)
	if err != nil {
		return MappingPage{}, err
	}
	if page.StartBlock != startBlock || page.BlockCount != blockCount ||
		int(page.Level) >= MaxChangedBlockMappingDepth || (expectedLevel >= 0 && int(page.Level) != expectedLevel) {
		return MappingPage{}, fmt.Errorf("inventory mapping page does not match its parent binding")
	}
	return page, nil
}
