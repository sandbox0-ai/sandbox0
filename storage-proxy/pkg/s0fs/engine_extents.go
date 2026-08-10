package s0fs

import (
	"fmt"
	"slices"
	"time"
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
