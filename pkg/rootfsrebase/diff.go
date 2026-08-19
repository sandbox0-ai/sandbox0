package rootfsrebase

import (
	"bytes"
	"fmt"
	"sort"
	"strings"
)

const DiffVersion = 1

type ChangeKind string

const (
	ChangeAdd     ChangeKind = "add"
	ChangeRemove  ChangeKind = "remove"
	ChangeReplace ChangeKind = "replace"
	ChangeModify  ChangeKind = "modify"
	ChangeRename  ChangeKind = "rename"
)

// DataChange describes the only file bytes a rebase worker may need to read
// from the source. SourceData=false represents a hole or truncated tail.
type DataChange struct {
	Offset     uint64 `json:"offset"`
	Length     uint64 `json:"length"`
	SourceData bool   `json:"source_data"`
}

type MetadataChange struct {
	Fields       []string          `json:"fields,omitempty"`
	SetXattrs    map[string][]byte `json:"set_xattrs,omitempty"`
	RemoveXattrs []string          `json:"remove_xattrs,omitempty"`
}

type Change struct {
	Kind           ChangeKind     `json:"kind"`
	Path           string         `json:"path"`
	FromPath       string         `json:"from_path,omitempty"`
	HardlinkTarget string         `json:"hardlink_target,omitempty"`
	Metadata       MetadataChange `json:"metadata,omitempty"`
	Data           []DataChange   `json:"data,omitempty"`
}

type DiffResult struct {
	Version   int      `json:"version"`
	Changes   []Change `json:"changes"`
	ReadBytes uint64   `json:"read_bytes"`
}

func (r DiffResult) Validate() error {
	if r.Version != DiffVersion {
		return fmt.Errorf("unsupported RootFS diff version %d", r.Version)
	}
	var previous string
	var readBytes uint64
	for index, change := range r.Changes {
		if !validManifestPath(change.Path) || index > 0 && change.Path <= previous {
			return fmt.Errorf("changes must contain unique canonical sorted paths")
		}
		previous = change.Path
		switch change.Kind {
		case ChangeAdd, ChangeRemove, ChangeReplace, ChangeModify, ChangeRename:
		default:
			return fmt.Errorf("unsupported change kind %q", change.Kind)
		}
		if change.Kind == ChangeRename {
			if !validManifestPath(change.FromPath) || change.FromPath == change.Path {
				return fmt.Errorf("rename source is invalid")
			}
		} else if change.FromPath != "" {
			return fmt.Errorf("only rename changes may name a source path")
		}
		if change.HardlinkTarget != "" &&
			(!validManifestPath(change.HardlinkTarget) || change.HardlinkTarget == change.Path) {
			return fmt.Errorf("hardlink target is invalid")
		}
		if err := validateMetadataChange(change.Metadata); err != nil {
			return fmt.Errorf("change %q: %w", change.Path, err)
		}
		var previousDataEnd uint64
		for dataIndex, data := range change.Data {
			if data.Length == 0 || data.Offset > ^uint64(0)-data.Length {
				return fmt.Errorf("change %q has an invalid data range", change.Path)
			}
			if dataIndex > 0 && data.Offset < previousDataEnd {
				return fmt.Errorf("change %q has overlapping or unsorted data ranges", change.Path)
			}
			previousDataEnd = data.Offset + data.Length
			if data.SourceData {
				if readBytes > ^uint64(0)-data.Length {
					return fmt.Errorf("read byte count overflows")
				}
				readBytes += data.Length
			}
		}
	}
	if readBytes != r.ReadBytes {
		return fmt.Errorf("read byte count does not match data ranges")
	}
	return nil
}

func validateMetadataChange(change MetadataChange) error {
	previous := ""
	for index, field := range change.Fields {
		switch field {
		case "gid", "link_target", "mode", "mtime", "rdev", "uid":
		default:
			return fmt.Errorf("unsupported metadata field %q", field)
		}
		if index > 0 && field <= previous {
			return fmt.Errorf("metadata fields must be unique and sorted")
		}
		previous = field
	}
	for name := range change.SetXattrs {
		if strings.TrimSpace(name) == "" || strings.ContainsRune(name, '\x00') {
			return fmt.Errorf("xattr name is invalid")
		}
	}
	previous = ""
	for index, name := range change.RemoveXattrs {
		if strings.TrimSpace(name) == "" || strings.ContainsRune(name, '\x00') {
			return fmt.Errorf("xattr name is invalid")
		}
		if index > 0 && name <= previous {
			return fmt.Errorf("removed xattrs must be unique and sorted")
		}
		if _, exists := change.SetXattrs[name]; exists {
			return fmt.Errorf("xattr %q cannot be set and removed", name)
		}
		previous = name
	}
	return nil
}

// Diff computes the user delta between two mounts of the same logical
// filesystem. Dirty ranges come from branch-LBA/FIEMAP attribution; extent
// allocation differences add hole-punch and sparse-growth semantics without
// reading unchanged file payloads.
func Diff(old, source Manifest, dirty map[string][]ByteRange) (*DiffResult, error) {
	if err := old.Validate(); err != nil {
		return nil, fmt.Errorf("old manifest: %w", err)
	}
	if err := source.Validate(); err != nil {
		return nil, fmt.Errorf("source manifest: %w", err)
	}
	oldByPath := nodesByPath(old)
	sourceByPath := nodesByPath(source)
	for path, ranges := range dirty {
		node, exists := sourceByPath[path]
		if !exists || node.Type != NodeRegular {
			return nil, fmt.Errorf("dirty file range names an unknown regular file %q", path)
		}
		for _, value := range ranges {
			if value.Length == 0 || value.Offset > ^uint64(0)-value.Length {
				return nil, fmt.Errorf("dirty file range for %q is invalid", path)
			}
		}
	}
	result := &DiffResult{Version: DiffVersion}
	deleted := make(map[string]Node)
	added := make(map[string]Node)
	for path, oldNode := range oldByPath {
		if path == "." {
			continue
		}
		if _, exists := sourceByPath[path]; !exists {
			deleted[path] = oldNode
		}
	}
	for path, sourceNode := range sourceByPath {
		if path == "." {
			continue
		}
		if _, exists := oldByPath[path]; !exists {
			added[path] = sourceNode
		}
	}

	if old.LineageID != "" && old.LineageID == source.LineageID {
		detectRenames(result, deleted, added, dirty)
	}
	for path, node := range deleted {
		result.Changes = append(result.Changes, Change{Kind: ChangeRemove, Path: path})
		_ = node
	}
	for path, node := range added {
		change := Change{Kind: ChangeAdd, Path: path, Metadata: fullMetadata(node)}
		if node.Type == NodeRegular {
			change.Data = fullSourceData(node)
		}
		result.Changes = append(result.Changes, change)
	}
	for path, sourceNode := range sourceByPath {
		oldNode, exists := oldByPath[path]
		if !exists {
			continue
		}
		if oldNode.Type != sourceNode.Type {
			change := Change{Kind: ChangeReplace, Path: path, Metadata: fullMetadata(sourceNode)}
			if sourceNode.Type == NodeRegular {
				change.Data = fullSourceData(sourceNode)
			}
			result.Changes = append(result.Changes, change)
			continue
		}
		metadata := metadataDelta(oldNode, sourceNode)
		var data []DataChange
		if sourceNode.Type == NodeRegular {
			data = regularDataChanges(oldNode, sourceNode, dirty[path])
		}
		if metadataChanged(metadata) || len(data) != 0 {
			result.Changes = append(result.Changes, Change{
				Kind: ChangeModify, Path: path, Metadata: metadata, Data: data,
			})
		}
	}
	setHardlinkTargets(result, old, source)
	sort.Slice(result.Changes, func(i, j int) bool {
		if result.Changes[i].Path == result.Changes[j].Path {
			return result.Changes[i].Kind < result.Changes[j].Kind
		}
		return result.Changes[i].Path < result.Changes[j].Path
	})
	for _, change := range result.Changes {
		for _, data := range change.Data {
			if data.SourceData {
				if result.ReadBytes > ^uint64(0)-data.Length {
					return nil, fmt.Errorf("RootFS diff read bytes overflow")
				}
				result.ReadBytes += data.Length
			}
		}
	}
	if err := result.Validate(); err != nil {
		return nil, err
	}
	return result, nil
}

func nodesByPath(manifest Manifest) map[string]Node {
	result := make(map[string]Node, len(manifest.Nodes))
	for _, node := range manifest.Nodes {
		result[node.Path] = node
	}
	return result
}

func detectRenames(result *DiffResult, deleted, added map[string]Node, dirty map[string][]ByteRange) {
	type key struct {
		inode      uint64
		generation uint32
		typeID     NodeType
	}
	oldByInode := make(map[key][]string)
	newByInode := make(map[key][]string)
	for path, node := range deleted {
		if node.Inode == 0 || !node.GenerationKnown {
			continue
		}
		identity := key{inode: node.Inode, generation: node.Generation, typeID: node.Type}
		oldByInode[identity] = append(oldByInode[identity], path)
	}
	for path, node := range added {
		if node.Inode == 0 || !node.GenerationKnown {
			continue
		}
		identity := key{inode: node.Inode, generation: node.Generation, typeID: node.Type}
		newByInode[identity] = append(newByInode[identity], path)
	}
	for identity, oldPaths := range oldByInode {
		newPaths := newByInode[identity]
		sort.Strings(oldPaths)
		sort.Strings(newPaths)
		for index := 0; index < min(len(oldPaths), len(newPaths)); index++ {
			from, target := oldPaths[index], newPaths[index]
			oldNode, sourceNode := deleted[from], added[target]
			change := Change{
				Kind: ChangeRename, Path: target, FromPath: from,
				Metadata: metadataDelta(oldNode, sourceNode),
			}
			if sourceNode.Type == NodeRegular {
				change.Data = regularDataChanges(oldNode, sourceNode, dirty[target])
			}
			result.Changes = append(result.Changes, change)
			delete(deleted, from)
			delete(added, target)
		}
	}
}

func metadataDelta(old, source Node) MetadataChange {
	var fields []string
	if old.Mode != source.Mode {
		fields = append(fields, "mode")
	}
	if old.UID != source.UID {
		fields = append(fields, "uid")
	}
	if old.GID != source.GID {
		fields = append(fields, "gid")
	}
	if old.ModTimeNS != source.ModTimeNS {
		fields = append(fields, "mtime")
	}
	if old.Rdev != source.Rdev {
		fields = append(fields, "rdev")
	}
	if old.LinkTarget != source.LinkTarget {
		fields = append(fields, "link_target")
	}
	set := make(map[string][]byte)
	var remove []string
	for name, value := range source.Xattrs {
		oldValue, exists := old.Xattrs[name]
		if !exists || !bytes.Equal(oldValue, value) {
			set[name] = append([]byte(nil), value...)
		}
	}
	for name := range old.Xattrs {
		if _, exists := source.Xattrs[name]; !exists {
			remove = append(remove, name)
		}
	}
	sort.Strings(fields)
	sort.Strings(remove)
	if len(set) == 0 {
		set = nil
	}
	return MetadataChange{Fields: fields, SetXattrs: set, RemoveXattrs: remove}
}

func fullMetadata(source Node) MetadataChange {
	fields := []string{"gid", "mode", "mtime", "uid"}
	if source.Type == NodeSymlink {
		fields = append(fields, "link_target")
	}
	if source.Type == NodeCharDevice || source.Type == NodeBlockDevice {
		fields = append(fields, "rdev")
	}
	sort.Strings(fields)
	return MetadataChange{Fields: fields, SetXattrs: cloneXattrs(source.Xattrs)}
}

func metadataChanged(change MetadataChange) bool {
	return len(change.Fields) != 0 || len(change.SetXattrs) != 0 || len(change.RemoveXattrs) != 0
}

func fullSourceData(source Node) []DataChange {
	var result []DataChange
	for _, extent := range allocatedRanges(source) {
		result = append(result, DataChange{Offset: extent.Offset, Length: extent.Length, SourceData: true})
	}
	return result
}

func regularDataChanges(old, source Node, dirty []ByteRange) []DataChange {
	var changedRanges []ByteRange
	for _, value := range allocationDifferences(old, source) {
		changedRanges = append(changedRanges, ByteRange{Offset: value.Offset, Length: value.Length})
	}
	changedRanges = append(changedRanges, dirty...)
	if old.Size != source.Size {
		start := uint64(min(old.Size, source.Size))
		end := uint64(max(old.Size, source.Size))
		changedRanges = append(changedRanges, ByteRange{Offset: start, Length: end - start})
	}
	var result []DataChange
	for _, value := range mergeByteRanges(changedRanges) {
		result = append(result, splitBySourceAllocation(source, value)...)
	}
	return mergeAdjacentDataChanges(result)
}

func allocationDifferences(old, source Node) []DataChange {
	oldAllocated := allocatedRanges(old)
	sourceAllocated := allocatedRanges(source)
	boundaries := []uint64{0, uint64(max(old.Size, source.Size))}
	for _, value := range append(append([]ByteRange(nil), oldAllocated...), sourceAllocated...) {
		boundaries = append(boundaries, value.Offset, value.Offset+value.Length)
	}
	sort.Slice(boundaries, func(i, j int) bool { return boundaries[i] < boundaries[j] })
	boundaries = uniqueUint64(boundaries)
	var result []DataChange
	for index := 0; index+1 < len(boundaries); index++ {
		start, end := boundaries[index], boundaries[index+1]
		if start == end {
			continue
		}
		oldData := rangeContains(oldAllocated, start)
		sourceData := rangeContains(sourceAllocated, start)
		if oldData != sourceData {
			result = append(result, DataChange{Offset: start, Length: end - start, SourceData: sourceData})
		}
	}
	return result
}

func allocatedRanges(node Node) []ByteRange {
	if node.Type != NodeRegular || node.Size <= 0 {
		return nil
	}
	limit := uint64(node.Size)
	var result []ByteRange
	for _, extent := range node.Extents {
		if extent.Flags&0x800 != 0 || extent.Logical >= limit {
			continue
		}
		end := min(extent.Logical+extent.Length, limit)
		result = append(result, ByteRange{Offset: extent.Logical, Length: end - extent.Logical})
	}
	return mergeByteRanges(result)
}

func splitBySourceAllocation(source Node, value ByteRange) []DataChange {
	if value.Length == 0 {
		return nil
	}
	limit := uint64(max(source.Size, int64(0)))
	end := value.Offset + value.Length
	boundaries := []uint64{value.Offset, end}
	for _, allocated := range allocatedRanges(source) {
		if allocated.Offset < end && allocated.Offset+allocated.Length > value.Offset {
			boundaries = append(boundaries, max(value.Offset, allocated.Offset), min(end, allocated.Offset+allocated.Length))
		}
	}
	if value.Offset < limit && end > limit {
		boundaries = append(boundaries, limit)
	}
	sort.Slice(boundaries, func(i, j int) bool { return boundaries[i] < boundaries[j] })
	boundaries = uniqueUint64(boundaries)
	allocated := allocatedRanges(source)
	result := make([]DataChange, 0, len(boundaries)-1)
	for index := 0; index+1 < len(boundaries); index++ {
		start, segmentEnd := boundaries[index], boundaries[index+1]
		result = append(result, DataChange{
			Offset: start, Length: segmentEnd - start,
			SourceData: start < limit && rangeContains(allocated, start),
		})
	}
	return result
}

func mergeAdjacentDataChanges(changes []DataChange) []DataChange {
	if len(changes) == 0 {
		return nil
	}
	var result []DataChange
	for _, current := range changes {
		if current.Length == 0 {
			continue
		}
		if len(result) == 0 {
			result = append(result, current)
			continue
		}
		last := &result[len(result)-1]
		lastEnd := last.Offset + last.Length
		if current.Offset == lastEnd && current.SourceData == last.SourceData {
			last.Length += current.Length
			continue
		}
		if current.Offset < lastEnd {
			// Keep the inconsistent overlap so DiffResult.Validate returns a
			// controlled error instead of panicking in the worker.
			result = append(result, current)
			continue
		}
		result = append(result, current)
	}
	return result
}

func rangeContains(ranges []ByteRange, offset uint64) bool {
	index := sort.Search(len(ranges), func(index int) bool {
		return ranges[index].Offset+ranges[index].Length > offset
	})
	return index < len(ranges) && ranges[index].Offset <= offset
}

func uniqueUint64(values []uint64) []uint64 {
	if len(values) == 0 {
		return nil
	}
	result := values[:1]
	for _, value := range values[1:] {
		if value != result[len(result)-1] {
			result = append(result, value)
		}
	}
	return result
}

func setHardlinkTargets(result *DiffResult, old, source Manifest) {
	type key struct{ device, inode uint64 }
	groups := make(map[key][]string)
	oldByPath := nodesByPath(old)
	sourceByPath := nodesByPath(source)
	for _, node := range source.Nodes {
		if node.Type == NodeRegular && node.LinkCount > 1 && node.Inode != 0 {
			groups[key{node.Device, node.Inode}] = append(groups[key{node.Device, node.Inode}], node.Path)
		}
	}
	targets := make(map[string]string)
	for _, paths := range groups {
		sort.Strings(paths)
		representative := paths[0]
		for _, path := range paths {
			oldNode, exists := oldByPath[path]
			sourceNode := sourceByPath[path]
			if exists && oldNode.Type == NodeRegular && oldNode.Inode == sourceNode.Inode {
				representative = path
				break
			}
		}
		for _, path := range paths {
			if path != representative {
				targets[path] = representative
			}
		}
	}
	filtered := result.Changes[:0]
	for _, change := range result.Changes {
		change.HardlinkTarget = targets[change.Path]
		if change.HardlinkTarget != "" {
			change.Data = nil
			change.Metadata = MetadataChange{}
			if change.Kind == ChangeModify {
				continue
			}
		}
		filtered = append(filtered, change)
	}
	result.Changes = filtered
}
