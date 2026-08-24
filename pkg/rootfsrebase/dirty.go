package rootfsrebase

import (
	"fmt"
	"math"
	"sort"
)

// ByteRange is a half-open byte interval represented as offset plus length.
type ByteRange struct {
	Offset uint64 `json:"offset"`
	Length uint64 `json:"length"`
}

// DirtyFileRanges maps exact branch LBAs to file-relative ranges through the
// source filesystem's FIEMAP. Metadata LBAs naturally map to no file.
func DirtyFileRanges(manifest Manifest, dirtyBlocks []uint64, blockSize uint64) (map[string][]ByteRange, error) {
	if err := manifest.Validate(); err != nil {
		return nil, err
	}
	dirty, err := dirtyByteRanges(dirtyBlocks, blockSize)
	if err != nil {
		return nil, err
	}
	if len(dirty) == 0 {
		return map[string][]ByteRange{}, nil
	}
	type inodeKey struct{ device, inode uint64 }
	byInode := make(map[inodeKey][]ByteRange)
	result := make(map[string][]ByteRange)
	for _, node := range manifest.Nodes {
		if node.Type != NodeRegular {
			continue
		}
		key := inodeKey{node.Device, node.Inode}
		ranges, exists := byInode[key]
		if !exists {
			ranges = intersectDirtyExtents(node.Extents, dirty, uint64(node.Size))
			byInode[key] = ranges
		}
		if len(ranges) != 0 {
			result[node.Path] = append([]ByteRange(nil), ranges...)
		}
	}
	return result, nil
}

func dirtyByteRanges(blocks []uint64, blockSize uint64) ([]ByteRange, error) {
	if blockSize == 0 {
		return nil, fmt.Errorf("dirty block size must be positive")
	}
	blocks = append([]uint64(nil), blocks...)
	sort.Slice(blocks, func(i, j int) bool { return blocks[i] < blocks[j] })
	var result []ByteRange
	for _, block := range blocks {
		if block > math.MaxUint64/blockSize {
			return nil, fmt.Errorf("dirty block offset overflows")
		}
		offset := block * blockSize
		if offset > math.MaxUint64-blockSize {
			return nil, fmt.Errorf("dirty block range overflows")
		}
		if len(result) != 0 {
			last := &result[len(result)-1]
			end := last.Offset + last.Length
			if offset < end {
				continue
			}
			if offset == end && last.Length <= math.MaxUint64-blockSize {
				last.Length += blockSize
				continue
			}
		}
		result = append(result, ByteRange{Offset: offset, Length: blockSize})
	}
	return result, nil
}

func intersectDirtyExtents(extents []Extent, dirty []ByteRange, logicalSize uint64) []ByteRange {
	var result []ByteRange
	for _, extent := range extents {
		if extent.Logical >= logicalSize {
			continue
		}
		extentEnd := extent.Physical + extent.Length
		index := sort.Search(len(dirty), func(index int) bool {
			return dirty[index].Offset+dirty[index].Length > extent.Physical
		})
		for ; index < len(dirty); index++ {
			dirtyEnd := dirty[index].Offset + dirty[index].Length
			if dirty[index].Offset >= extentEnd {
				break
			}
			start := max(extent.Physical, dirty[index].Offset)
			end := min(extentEnd, dirtyEnd)
			if start < end {
				logicalStart := extent.Logical + start - extent.Physical
				logicalEnd := min(extent.Logical+end-extent.Physical, logicalSize)
				if logicalStart >= logicalEnd {
					continue
				}
				result = append(result, ByteRange{
					Offset: logicalStart, Length: logicalEnd - logicalStart,
				})
			}
		}
	}
	return mergeByteRanges(result)
}

func mergeByteRanges(ranges []ByteRange) []ByteRange {
	if len(ranges) < 2 {
		return ranges
	}
	sort.Slice(ranges, func(i, j int) bool { return ranges[i].Offset < ranges[j].Offset })
	result := ranges[:1]
	for _, current := range ranges[1:] {
		last := &result[len(result)-1]
		lastEnd := last.Offset + last.Length
		currentEnd := current.Offset + current.Length
		if current.Offset <= lastEnd {
			if currentEnd > lastEnd {
				last.Length = currentEnd - last.Offset
			}
			continue
		}
		result = append(result, current)
	}
	return result
}
