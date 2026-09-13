package rootfsblock

import (
	"fmt"
	"math"
)

// mappingStream retains one leaf and at most one fanout of child locators per
// level. A full frontier is published only when a successor arrives: until
// then it may still be the root, whose coverage includes leading/trailing
// sparse space. This preserves the existing tree and content-addressed keys
// without retaining every extent or every published page payload.
type mappingStream struct {
	builder     *generationBuilder
	totalBlocks uint64
	leaf        []MappingEntry
	levels      [][]publishedPage
	lastEnd     uint64
	finished    bool
	pending     [][]publishedPage
}

func newMappingStream(builder *generationBuilder, totalBlocks uint64) *mappingStream {
	return &mappingStream{builder: builder, totalBlocks: totalBlocks, leaf: make([]MappingEntry, 0, builder.options.PageEntries)}
}

func (s *mappingStream) addEntries(entries []MappingEntry) error {
	for _, entry := range entries {
		if err := s.add(entry); err != nil {
			return err
		}
	}
	return nil
}

func (s *mappingStream) add(entry MappingEntry) error {
	if s.finished {
		return fmt.Errorf("mapping stream is finished")
	}
	if err := s.builder.ctx.Err(); err != nil {
		return err
	}
	end := entry.LogicalStart + uint64(entry.BlockCount)
	if entry.BlockCount == 0 || end < entry.LogicalStart || end > s.totalBlocks || entry.LogicalStart < s.lastEnd {
		return fmt.Errorf("mapping stream entry is unordered or outside the image")
	}
	if len(s.leaf) == s.builder.options.PageEntries {
		if err := s.flushLeaf(); err != nil {
			return err
		}
	}
	s.leaf = append(s.leaf, entry)
	s.lastEnd = end
	return nil
}

func (s *mappingStream) flushLeaf() error {
	first := s.leaf[0].LogicalStart
	last := s.leaf[len(s.leaf)-1]
	err := s.queueMappingPage(0, MappingPage{StartBlock: first, BlockCount: last.LogicalStart + uint64(last.BlockCount) - first, Entries: s.leaf})
	if err != nil {
		return err
	}
	clear(s.leaf)
	s.leaf = s.leaf[:0]
	return nil
}

func (s *mappingStream) addPage(level int, page publishedPage) error {
	if level >= math.MaxUint8 {
		return fmt.Errorf("mapping tree is too deep")
	}
	for len(s.levels) <= level {
		s.levels = append(s.levels, make([]publishedPage, 0, s.builder.options.PageEntries))
	}
	children := s.levels[level]
	if len(children) == s.builder.options.PageEntries {
		parent, err := internalMappingPage(children, uint8(level+1), false, s.totalBlocks)
		if err != nil {
			return err
		}
		clear(children)
		s.levels[level] = children[:0]
		if err := s.queueMappingPage(level+1, parent); err != nil {
			return err
		}
	}
	page.payload, page.stored = nil, nil
	s.levels[level] = append(s.levels[level], page)
	return nil
}

func (s *mappingStream) finish() (ObjectRange, []byte, error) {
	if s.finished {
		return ObjectRange{}, nil, fmt.Errorf("mapping stream is finished")
	}
	s.finished = true
	if err := s.builder.ctx.Err(); err != nil {
		return ObjectRange{}, nil, err
	}
	if len(s.levels) == 0 && len(s.pending) == 0 {
		return s.builder.publishRootPage(MappingPage{StartBlock: 0, BlockCount: s.totalBlocks, Entries: s.leaf})
	}
	if len(s.leaf) > 0 {
		if err := s.flushLeaf(); err != nil {
			return ObjectRange{}, nil, err
		}
	}
	if err := s.flushPendingMappingGroups(); err != nil {
		return ObjectRange{}, nil, err
	}
	for level := 0; level < len(s.levels); level++ {
		children := s.levels[level]
		if len(children) == 0 {
			continue
		}
		root := level == len(s.levels)-1
		page, err := s.builder.publishInternalPage(children, uint8(level+1), root, s.totalBlocks)
		if err != nil {
			return ObjectRange{}, nil, err
		}
		clear(children)
		s.levels[level] = children[:0]
		if root {
			return page.object, page.payload, nil
		}
		if err := s.addPage(level+1, page); err != nil {
			return ObjectRange{}, nil, err
		}
		if err := s.flushPendingMappingGroups(); err != nil {
			return ObjectRange{}, nil, err
		}
	}
	return ObjectRange{}, nil, fmt.Errorf("mapping stream has no root")
}
