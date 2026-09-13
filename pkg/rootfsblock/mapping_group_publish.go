package rootfsblock

import (
	"fmt"
	"math"
)

// ContiguousMappingV1 groups adjacent, same-level metadata at import time.
// It is an immutable build policy, not a tenant/runtime compatibility class.
const ContiguousMappingV1 = "contiguous-mapping-v1"

const (
	mappingPublishPages        = 4
	mappingPublishStoredBytes  = 240 << 10
	mappingPublishDecodedBytes = 1 << 20
)

// publishMappingGroup inventories the complete stored object exactly once;
// locators retain independent decoded checksums and physical byte ranges.
func (b *generationBuilder) publishMappingGroup(pages []publishedPage) ([]publishedPage, error) {
	if len(pages) == 0 {
		return nil, fmt.Errorf("empty mapping group")
	}
	var stored []byte
	if len(pages) == 1 {
		stored = pages[0].stored
	} else {
		var size, decoded int
		for _, page := range pages {
			size += len(page.stored)
			decoded += len(page.payload)
		}
		if len(pages) > mappingPublishPages || size > mappingPublishStoredBytes || decoded > mappingPublishDecodedBytes {
			return nil, fmt.Errorf("mapping publication group exceeds bounds")
		}
		stored = make([]byte, 0, size)
		for _, page := range pages {
			stored = append(stored, page.stored...)
		}
	}
	key, err := b.publish("maps", stored)
	if err != nil {
		return nil, err
	}
	var offset int64
	for i := range pages {
		pages[i].object.Key = key
		pages[i].object.Offset = offset
		offset += int64(len(pages[i].stored))
		pages[i].stored = nil
	}
	return pages, nil
}

// Pending buffers are bounded per tree level, never by the full image size.
// Oversized individual pages retain their exact legacy publication path.
func (s *mappingStream) queueMappingPage(level int, page MappingPage) error {
	if level >= math.MaxUint8 {
		return fmt.Errorf("mapping tree is too deep")
	}
	if s.builder.options.MappingGroupPolicy == "" {
		published, err := s.builder.publishPage(page)
		if err != nil {
			return err
		}
		return s.addPage(level, published)
	}
	prepared, err := s.builder.prepareMappingPage(page)
	if err != nil {
		return err
	}
	for len(s.pending) <= level {
		s.pending = append(s.pending, make([]publishedPage, 0, mappingPublishPages))
	}
	var stored, decoded int
	for _, p := range s.pending[level] {
		stored += len(p.stored)
		decoded += len(p.payload)
	}
	if stored+len(prepared.stored) > mappingPublishStoredBytes || decoded+len(prepared.payload) > mappingPublishDecodedBytes {
		if err := s.flushMappingGroup(level); err != nil {
			return err
		}
	}
	if len(prepared.stored) > mappingPublishStoredBytes || len(prepared.payload) > mappingPublishDecodedBytes {
		pages, err := s.builder.publishMappingGroup([]publishedPage{prepared})
		if err != nil {
			return err
		}
		return s.addPage(level, pages[0])
	}
	s.pending[level] = append(s.pending[level], prepared)
	remaining := s.builder.options.PageEntries
	if level < len(s.levels) && len(s.levels[level]) < remaining {
		remaining -= len(s.levels[level])
	}
	// Keep all group locators inside one parent wherever the fanout allows it;
	// otherwise one parent cannot describe enough members to share a read.
	if len(s.pending[level]) >= min(mappingPublishPages, remaining) {
		return s.flushMappingGroup(level)
	}
	return nil
}

func (s *mappingStream) flushMappingGroup(level int) error {
	if level >= len(s.pending) || len(s.pending[level]) == 0 {
		return nil
	}
	pages, err := s.builder.publishMappingGroup(s.pending[level])
	if err != nil {
		return err
	}
	// Higher-level recursive flushes may grow the outer pending slice. Keep
	// ownership of this bounded inner buffer until all locators are attached.
	s.pending[level] = nil
	for _, page := range pages {
		if err := s.addPage(level, page); err != nil {
			return err
		}
	}
	clear(pages)
	s.pending[level] = pages[:0]
	return nil
}

func (s *mappingStream) flushPendingMappingGroups() error {
	for level := 0; level < len(s.pending); level++ {
		if err := s.flushMappingGroup(level); err != nil {
			return err
		}
	}
	return nil
}
