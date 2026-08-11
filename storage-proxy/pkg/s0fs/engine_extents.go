package s0fs

import (
	"fmt"
	"slices"
	"time"

	"golang.org/x/sys/unix"
)

func (e *Engine) usesExtentLayoutLocked() bool {
	return e != nil && e.materializer != nil && e.materializer.Enabled()
}

func (e *Engine) applyExtentWrite(record walRecord) error {
	node, err := e.fileNodeLocked(record.Inode)
	if err != nil {
		return err
	}
	oldSize := node.Size
	extents, err := e.extentsForMutationLocked(record.Inode, record.Seq)
	if err != nil {
		return err
	}

	writeEnd := record.Offset + uint64(len(record.Data))
	var next []FileExtent
	next = append(next, sliceExtents(extents, 0, minUint64(record.Offset, oldSize))...)
	if record.Offset > oldSize {
		next = append(next, FileExtent{Length: record.Offset - oldSize})
	}
	if len(record.Data) > 0 {
		next = append(next, e.appendInlineSegmentLocked(record.Seq, "write", record.Data))
	}
	if writeEnd < oldSize {
		next = append(next, sliceExtents(extents, writeEnd, oldSize)...)
	}

	e.metadata.DeleteData(record.Inode)
	next = coalesceExtents(next)
	if len(next) == 0 {
		e.metadata.DeleteColdFile(record.Inode)
	} else {
		e.metadata.PutColdFile(record.Inode, next)
	}
	node.Size = maxUint64(oldSize, writeEnd)
	now := time.Unix(0, record.TimeUnix).UTC()
	node.Mtime = now
	node.Ctime = now
	e.metadata.PutNode(record.Inode, node)
	return nil
}

func (e *Engine) applyExtentTruncate(record walRecord) error {
	node, err := e.fileNodeLocked(record.Inode)
	if err != nil {
		return err
	}
	oldSize := node.Size
	target := record.Offset
	if target == 0 {
		e.metadata.DeleteData(record.Inode)
		e.metadata.DeleteColdFile(record.Inode)
		node.Size = 0
		now := time.Unix(0, record.TimeUnix).UTC()
		node.Mtime = now
		node.Ctime = now
		e.metadata.PutNode(record.Inode, node)
		return nil
	}

	extents, err := e.extentsForMutationLocked(record.Inode, record.Seq)
	if err != nil {
		return err
	}
	var next []FileExtent
	switch {
	case target < oldSize:
		next = sliceExtents(extents, 0, target)
	case target > oldSize:
		next = append(cloneExtents(extents), FileExtent{Length: target - oldSize})
	default:
		next = cloneExtents(extents)
	}
	e.metadata.DeleteData(record.Inode)
	next = coalesceExtents(next)
	if len(next) == 0 {
		e.metadata.DeleteColdFile(record.Inode)
	} else {
		e.metadata.PutColdFile(record.Inode, next)
	}
	node.Size = target
	now := time.Unix(0, record.TimeUnix).UTC()
	node.Mtime = now
	node.Ctime = now
	e.metadata.PutNode(record.Inode, node)
	return nil
}

func (e *Engine) applyFallocate(record walRecord) error {
	if !e.usesExtentLayoutLocked() {
		return e.applyDenseFallocate(record)
	}
	node, err := e.fileNodeLocked(record.Inode)
	if err != nil {
		return err
	}
	end := record.Offset + record.Length
	keepSize := record.Mode&uint32(unix.FALLOC_FL_KEEP_SIZE) != 0
	zeroRange := record.Mode&uint32(unix.FALLOC_FL_ZERO_RANGE) != 0
	punchHole := record.Mode&uint32(unix.FALLOC_FL_PUNCH_HOLE) != 0
	if !zeroRange && !punchHole {
		if !keepSize && end > node.Size {
			record.Offset = end
			return e.applyExtentTruncate(record)
		}
		node.Ctime = time.Unix(0, record.TimeUnix).UTC()
		e.metadata.PutNode(record.Inode, node)
		return nil
	}
	extents, err := e.extentsForMutationLocked(record.Inode, record.Seq)
	if err != nil {
		return err
	}
	visibleEnd := minUint64(end, node.Size)
	next := sliceExtents(extents, 0, minUint64(record.Offset, node.Size))
	if zeroRange && !keepSize && end > node.Size {
		visibleEnd = end
	}
	if visibleEnd > record.Offset {
		next = append(next, FileExtent{Length: visibleEnd - record.Offset})
	}
	if end < node.Size {
		next = append(next, sliceExtents(extents, end, node.Size)...)
	}
	next = coalesceExtents(next)
	e.metadata.DeleteData(record.Inode)
	if len(next) == 0 {
		e.metadata.DeleteColdFile(record.Inode)
	} else {
		e.metadata.PutColdFile(record.Inode, next)
	}
	if zeroRange && !keepSize && end > node.Size {
		node.Size = end
	}
	now := time.Unix(0, record.TimeUnix).UTC()
	node.Mtime, node.Ctime = now, now
	e.metadata.PutNode(record.Inode, node)
	return nil
}

func (e *Engine) applyDenseFallocate(record walRecord) error {
	node, err := e.fileNodeLocked(record.Inode)
	if err != nil {
		return err
	}
	end := record.Offset + record.Length
	keepSize := record.Mode&uint32(unix.FALLOC_FL_KEEP_SIZE) != 0
	zeroRange := record.Mode&uint32(unix.FALLOC_FL_ZERO_RANGE) != 0
	punchHole := record.Mode&uint32(unix.FALLOC_FL_PUNCH_HOLE) != 0
	current, err := e.mutableFileDataLocked(record.Inode)
	if err != nil {
		return err
	}
	targetSize := node.Size
	if !keepSize && end > targetSize {
		targetSize = end
	}
	if targetSize > uint64(maxInt()) {
		return fmt.Errorf("%w: dense file is too large", ErrInvalidInput)
	}
	if targetSize > uint64(len(current)) {
		grown := make([]byte, int(targetSize))
		copy(grown, current)
		current = grown
	} else {
		current = slices.Clone(current)
	}
	if zeroRange || punchHole {
		zeroEnd := minUint64(end, node.Size)
		if zeroRange && !keepSize {
			zeroEnd = end
		}
		for index := record.Offset; index < zeroEnd; index++ {
			current[int(index)] = 0
		}
	}
	e.metadata.PutData(record.Inode, current)
	e.metadata.DeleteColdFile(record.Inode)
	node.Size = targetSize
	now := time.Unix(0, record.TimeUnix).UTC()
	node.Mtime, node.Ctime = now, now
	e.metadata.PutNode(record.Inode, node)
	return nil
}

func (e *Engine) applyCopyFileRange(record walRecord) error {
	if !e.usesExtentLayoutLocked() {
		return e.applyDenseCopyFileRange(record)
	}
	source, err := e.fileNodeLocked(record.SourceInode)
	if err != nil {
		return err
	}
	destination, err := e.fileNodeLocked(record.Inode)
	if err != nil {
		return err
	}
	if record.SourceOffset >= source.Size || record.Length == 0 {
		return nil
	}
	copyLength := minUint64(record.Length, source.Size-record.SourceOffset)
	sourceExtents, err := e.extentsForMutationLocked(record.SourceInode, record.Seq)
	if err != nil {
		return err
	}
	// Capture source extents before rewriting the destination so overlapping
	// copies within one inode preserve copy_file_range snapshot semantics.
	copied := sliceExtents(sourceExtents, record.SourceOffset, record.SourceOffset+copyLength)
	destinationExtents, err := e.extentsForMutationLocked(record.Inode, record.Seq)
	if err != nil {
		return err
	}
	oldSize := destination.Size
	next := sliceExtents(destinationExtents, 0, minUint64(record.Offset, oldSize))
	if record.Offset > oldSize {
		next = append(next, FileExtent{Length: record.Offset - oldSize})
	}
	next = append(next, copied...)
	copyEnd := record.Offset + copyLength
	if copyEnd < oldSize {
		next = append(next, sliceExtents(destinationExtents, copyEnd, oldSize)...)
	}
	next = coalesceExtents(next)
	e.metadata.DeleteData(record.Inode)
	e.metadata.PutColdFile(record.Inode, next)
	destination.Size = maxUint64(oldSize, copyEnd)
	now := time.Unix(0, record.TimeUnix).UTC()
	destination.Mtime, destination.Ctime = now, now
	e.metadata.PutNode(record.Inode, destination)
	return nil
}

func (e *Engine) applyDenseCopyFileRange(record walRecord) error {
	source, err := e.fileNodeLocked(record.SourceInode)
	if err != nil {
		return err
	}
	destination, err := e.fileNodeLocked(record.Inode)
	if err != nil {
		return err
	}
	if record.SourceOffset >= source.Size || record.Length == 0 {
		return nil
	}
	copyLength := minUint64(record.Length, source.Size-record.SourceOffset)
	sourceData, err := e.mutableFileDataLocked(record.SourceInode)
	if err != nil {
		return err
	}
	sourceEnd := record.SourceOffset + copyLength
	copied := slices.Clone(sourceData[int(record.SourceOffset):int(sourceEnd)])
	destinationData, err := e.mutableFileDataLocked(record.Inode)
	if err != nil {
		return err
	}
	copyEnd := record.Offset + copyLength
	if copyEnd > uint64(maxInt()) {
		return fmt.Errorf("%w: dense file is too large", ErrInvalidInput)
	}
	if copyEnd > uint64(len(destinationData)) {
		grown := make([]byte, int(copyEnd))
		copy(grown, destinationData)
		destinationData = grown
	} else {
		destinationData = slices.Clone(destinationData)
	}
	copy(destinationData[int(record.Offset):int(copyEnd)], copied)
	e.metadata.PutData(record.Inode, destinationData)
	e.metadata.DeleteColdFile(record.Inode)
	destination.Size = maxUint64(destination.Size, copyEnd)
	now := time.Unix(0, record.TimeUnix).UTC()
	destination.Mtime, destination.Ctime = now, now
	e.metadata.PutNode(record.Inode, destination)
	return nil
}

func maxInt() int {
	return int(^uint(0) >> 1)
}

func (e *Engine) extentsForMutationLocked(inode uint64, seq uint64) ([]FileExtent, error) {
	if payload, ok := e.metadata.Data(inode); ok {
		e.metadata.DeleteData(inode)
		if len(payload) == 0 {
			if node, ok := e.metadata.Node(inode); ok && node.Size > 0 {
				return []FileExtent{{Length: node.Size}}, nil
			}
			return nil, nil
		}
		return []FileExtent{e.appendInlineSegmentLocked(seq, "base", payload)}, nil
	}
	if extents, _ := e.metadata.ColdFile(inode); len(extents) > 0 {
		return cloneExtents(extents), nil
	}
	if node, ok := e.metadata.Node(inode); ok && node.Size > 0 {
		return []FileExtent{{Length: node.Size}}, nil
	}
	return nil, nil
}

func (e *Engine) appendInlineSegmentLocked(seq uint64, suffix string, payload []byte) FileExtent {
	segmentID := fmt.Sprintf("inline-%020d-%s", seq, suffix)
	if _, exists := e.metadata.Segment(segmentID); exists {
		for i := 1; ; i++ {
			candidate := fmt.Sprintf("inline-%020d-%s-%d", seq, suffix, i)
			if _, exists := e.metadata.Segment(candidate); !exists {
				segmentID = candidate
				break
			}
		}
	}
	e.metadata.PutSegment(segmentID, &Segment{
		ID:         segmentID,
		VolumeID:   e.volumeID,
		Length:     uint64(len(payload)),
		InlineData: slices.Clone(payload),
	})
	return FileExtent{
		SegmentID: segmentID,
		Offset:    0,
		Length:    uint64(len(payload)),
	}
}
