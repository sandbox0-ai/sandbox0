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

	delete(e.data, record.Inode)
	next = coalesceExtents(next)
	if len(next) == 0 {
		delete(e.coldFiles, record.Inode)
	} else {
		e.coldFiles[record.Inode] = next
	}
	node.Size = maxUint64(oldSize, writeEnd)
	now := time.Unix(0, record.TimeUnix).UTC()
	node.Mtime = now
	node.Ctime = now
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
		delete(e.data, record.Inode)
		delete(e.coldFiles, record.Inode)
		node.Size = 0
		now := time.Unix(0, record.TimeUnix).UTC()
		node.Mtime = now
		node.Ctime = now
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
	delete(e.data, record.Inode)
	next = coalesceExtents(next)
	if len(next) == 0 {
		delete(e.coldFiles, record.Inode)
	} else {
		e.coldFiles[record.Inode] = next
	}
	node.Size = target
	now := time.Unix(0, record.TimeUnix).UTC()
	node.Mtime = now
	node.Ctime = now
	return nil
}

func (e *Engine) extentsForMutationLocked(inode uint64, seq uint64) ([]FileExtent, error) {
	if payload, ok := e.data[inode]; ok {
		delete(e.data, inode)
		if len(payload) == 0 {
			if node := e.nodes[inode]; node != nil && node.Size > 0 {
				return []FileExtent{{Length: node.Size}}, nil
			}
			return nil, nil
		}
		return []FileExtent{e.appendInlineSegmentLocked(seq, "base", payload)}, nil
	}
	if extents := e.coldFiles[inode]; len(extents) > 0 {
		return cloneExtents(extents), nil
	}
	if node := e.nodes[inode]; node != nil && node.Size > 0 {
		return []FileExtent{{Length: node.Size}}, nil
	}
	return nil, nil
}

func (e *Engine) appendInlineSegmentLocked(seq uint64, suffix string, payload []byte) FileExtent {
	segmentID := fmt.Sprintf("inline-%020d-%s", seq, suffix)
	if existing := e.segments[segmentID]; existing != nil {
		for i := 1; ; i++ {
			candidate := fmt.Sprintf("inline-%020d-%s-%d", seq, suffix, i)
			if e.segments[candidate] == nil {
				segmentID = candidate
				break
			}
		}
	}
	e.segments[segmentID] = &Segment{
		ID:         segmentID,
		VolumeID:   e.volumeID,
		Length:     uint64(len(payload)),
		InlineData: slices.Clone(payload),
	}
	return FileExtent{
		SegmentID: segmentID,
		Offset:    0,
		Length:    uint64(len(payload)),
	}
}
