package rootfsblock

import (
	"context"
	"fmt"
	"math"
	"sort"
)

const MaxEditedMappingEntries = 4 << 20

// A plan owns only pages on dirty paths. Untouched children remain immutable
// locators, not decoded subtrees. The limit bounds one edit's working set,
// rather than the total number of extents in a durable generation.
type mappingEditPlan struct {
	root    *mappingEditNode
	leaves  []*mappingEditNode
	levels  [][]*mappingEditNode
	entries int
}

type mappingEditNode struct {
	page         MappingPage
	children     []mappingEditChild
	replacements []publishedPage
}

type mappingEditChild struct {
	entry MappingEntry
	edit  *mappingEditNode
}

func prepareMappingEdits(ctx context.Context, reader *Reader, blocks []uint64) (*mappingEditPlan, error) {
	plan := &mappingEditPlan{entries: len(blocks)}
	root, err := plan.prepare(ctx, reader, reader.root, blocks)
	if err != nil {
		return nil, err
	}
	plan.root = root
	return plan, nil
}

func (p *mappingEditPlan) add(node *mappingEditNode) {
	level := int(node.page.Level)
	for len(p.levels) <= level {
		p.levels = append(p.levels, nil)
	}
	p.levels[level] = append(p.levels[level], node)
}

func (p *mappingEditPlan) prepare(ctx context.Context, reader *Reader, page MappingPage, blocks []uint64) (*mappingEditNode, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	p.entries += len(page.Entries)
	if p.entries > MaxEditedMappingEntries {
		return nil, fmt.Errorf("incremental edit contains too many mapping entries")
	}
	node := &mappingEditNode{page: page}
	p.add(node)
	if page.Level == 0 {
		entries, err := splitUnchangedEntries(reader, page.Entries, blocks)
		if err != nil {
			return nil, err
		}
		node.page.Entries = entries
		p.leaves = append(p.leaves, node)
		return node, nil
	}
	remaining := blocks
	takeBefore := func(end uint64) []uint64 {
		index := sort.Search(len(remaining), func(i int) bool { return remaining[i] >= end })
		part := remaining[:index]
		remaining = remaining[index:]
		return part
	}
	gap := func(start, end uint64) error {
		part := takeBefore(end)
		if len(part) == 0 {
			return nil
		}
		child, err := p.prepare(ctx, reader, MappingPage{
			Version: page.Version, Level: page.Level - 1, StartBlock: start, BlockCount: end - start,
		}, part)
		if err == nil {
			node.children = append(node.children, mappingEditChild{edit: child})
		}
		return err
	}
	cursor := page.StartBlock
	for _, entry := range page.Entries {
		if err := gap(cursor, entry.LogicalStart); err != nil {
			return nil, err
		}
		end := mappingEntryEnd(entry)
		part := takeBefore(end)
		child := mappingEditChild{entry: entry}
		if len(part) > 0 {
			loaded, err := reader.readMappingPage(entry.Object)
			if err != nil {
				return nil, fmt.Errorf("read edited mapping child: %w", err)
			}
			if loaded.formatVersion() != page.formatVersion() || loaded.Level+1 != page.Level ||
				loaded.StartBlock != entry.LogicalStart || loaded.BlockCount != uint64(entry.BlockCount) {
				return nil, fmt.Errorf("mapping child does not match its parent entry")
			}
			child.edit, err = p.prepare(ctx, reader, loaded, part)
			if err != nil {
				return nil, err
			}
		}
		node.children = append(node.children, child)
		cursor = end
	}
	if err := gap(cursor, page.StartBlock+page.BlockCount); err != nil {
		return nil, err
	}
	// The child locators now own this level; do not retain a second entry slice.
	node.page.Entries = nil
	return node, nil
}

// New raw ranges must not straddle leaf boundaries: their checksum describes
// the complete raw view. Grouping dirty payloads still shares bounded packs.
func (p *mappingEditPlan) leafEnd(block uint64) uint64 {
	index := sort.Search(len(p.leaves), func(i int) bool {
		return p.leaves[i].page.StartBlock+p.leaves[i].page.BlockCount > block
	})
	if index == len(p.leaves) || p.leaves[index].page.StartBlock > block {
		return block
	}
	return p.leaves[index].page.StartBlock + p.leaves[index].page.BlockCount
}

func (p *mappingEditPlan) addData(entries []MappingEntry) error {
	index := 0
	for _, entry := range entries {
		for index < len(p.leaves) && p.leaves[index].page.StartBlock+p.leaves[index].page.BlockCount <= entry.LogicalStart {
			index++
		}
		if index == len(p.leaves) || entry.LogicalStart < p.leaves[index].page.StartBlock ||
			mappingEntryEnd(entry) > p.leaves[index].page.StartBlock+p.leaves[index].page.BlockCount {
			return fmt.Errorf("dirty data range is outside its edited mapping leaf")
		}
		p.leaves[index].page.Entries = append(p.leaves[index].page.Entries, entry)
	}
	for _, leaf := range p.leaves {
		sort.Slice(leaf.page.Entries, func(i, j int) bool { return leaf.page.Entries[i].LogicalStart < leaf.page.Entries[j].LogicalStart })
	}
	return nil
}

type mappingEditPage struct {
	owner int
	node  *mappingEditNode
	page  MappingPage
}

// Both single and batch publication use the same path-copy algorithm. The
// batch callback packs pages at each dependency level across owners, retaining
// the existing shared-pack economics without expanding their base trees.
func publishMappingEdits(ctx context.Context, plans []*mappingEditPlan, pageEntries int,
	publish func([]mappingEditPage) ([]publishedPage, error),
) error {
	for _, plan := range plans {
		plan.leaves = nil
	}
	for level := 0; ; level++ {
		pending := false
		pages := make([]mappingEditPage, 0)
		for owner, plan := range plans {
			if level >= len(plan.levels) {
				continue
			}
			pending = true
			for _, node := range plan.levels[level] {
				if err := ctx.Err(); err != nil {
					return err
				}
				page := node.page
				for _, child := range node.children {
					if child.edit == nil {
						page.Entries = append(page.Entries, child.entry)
						continue
					}
					for _, replacement := range child.edit.replacements {
						if replacement.count > math.MaxUint32 {
							return fmt.Errorf("mapping child covers too many blocks")
						}
						page.Entries = append(page.Entries, MappingEntry{
							LogicalStart: replacement.start, BlockCount: uint32(replacement.count),
							Kind: MappingEntryChild, Object: replacement.object,
						})
					}
					child.edit.replacements = nil
				}
				if len(page.Entries) == 0 && node != plan.root {
					continue
				}
				if len(page.Entries) <= pageEntries {
					if len(page.Entries) == 0 {
						page.Level = 0
					}
					pages = append(pages, mappingEditPage{owner: owner, node: node, page: page})
					continue
				}
				for start := 0; start < len(page.Entries); start += pageEntries {
					end := min(start+pageEntries, len(page.Entries))
					part := page
					part.Entries = page.Entries[start:end]
					part.StartBlock = part.Entries[0].LogicalStart
					part.BlockCount = mappingEntryEnd(part.Entries[len(part.Entries)-1]) - part.StartBlock
					pages = append(pages, mappingEditPage{owner: owner, node: node, page: part})
				}
				if node == plan.root {
					if page.Level == math.MaxUint8 {
						return fmt.Errorf("mapping tree is too deep")
					}
					plan.root = &mappingEditNode{
						page:     MappingPage{Version: page.Version, Level: page.Level + 1, StartBlock: 0, BlockCount: page.BlockCount},
						children: []mappingEditChild{{edit: node}},
					}
					plan.add(plan.root)
				}
			}
		}
		if !pending {
			return nil
		}
		published, err := publish(pages)
		if err != nil {
			return err
		}
		if len(published) != len(pages) {
			return fmt.Errorf("mapping publisher returned inconsistent page count")
		}
		for index, page := range pages {
			if page.node != plans[page.owner].root {
				published[index].payload = nil
			}
			page.node.replacements = append(page.node.replacements, published[index])
		}
		// Once a level is published only its locators are needed by parents.
		for _, plan := range plans {
			if level < len(plan.levels) {
				for _, node := range plan.levels[level] {
					node.page.Entries, node.children = nil, nil
				}
				plan.levels[level] = nil
			}
		}
	}
}
