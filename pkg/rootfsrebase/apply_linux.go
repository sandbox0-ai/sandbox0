//go:build linux

package rootfsrebase

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"

	"golang.org/x/sys/unix"
)

const rebaseIOBufferSize = 1 << 20

type structureAction uint8

const (
	structureNone structureAction = iota
	structureCreate
	structureRemove
	structureReplace
	structureRename
	structureHardlink
)

type changePlan struct {
	change       Change
	old          Node
	source       Node
	target       Node
	targetExists bool
	currentPath  string
	structure    structureAction
	fields       map[string]bool
	setXattrs    map[string][]byte
	removeXattrs []string
	data         []DataChange
	created      bool
}

func (p changePlan) mutates() bool {
	return p.structure != structureNone || len(p.fields) != 0 || len(p.setXattrs) != 0 ||
		len(p.removeXattrs) != 0 || len(p.data) != 0
}

type rootKind uint8

const (
	rootOld rootKind = iota
	rootSource
	rootTarget
)

type applyWorker struct {
	request  ApplyRequest
	oldRoot  *secureRoot
	source   *secureRoot
	target   *secureRoot
	targetMF Manifest
	oldBy    map[string]Node
	sourceBy map[string]Node
	targetBy map[string]Node
	stats    ApplyIOStats
	conflict conflictCollector
}

// Apply performs an offline, conflict-detecting three-way merge and returns a
// digest-bound health proof. All conflict checks finish before TargetRoot is
// mutated. A later operational error may leave the unpublished target partly
// changed, so callers must discard it rather than publish or retry in place.
func Apply(ctx context.Context, request ApplyRequest) (_ *ApplyResult, returnErr error) {
	if err := validateApplyRequest(request); err != nil {
		return nil, err
	}
	worker := &applyWorker{
		request: request,
		oldBy:   nodesByPath(request.Old), sourceBy: nodesByPath(request.Source),
	}
	worker.oldRoot, returnErr = openSecureRoot(request.OldRoot, false)
	if returnErr != nil {
		return nil, fmt.Errorf("open old RootFS: %w", returnErr)
	}
	defer worker.oldRoot.close()
	worker.source, returnErr = openSecureRoot(request.SourceRoot, false)
	if returnErr != nil {
		return nil, fmt.Errorf("open source RootFS: %w", returnErr)
	}
	defer worker.source.close()
	worker.target, returnErr = openSecureRoot(request.TargetRoot, true)
	if returnErr != nil {
		return nil, fmt.Errorf("open target RootFS: %w", returnErr)
	}
	defer worker.target.close()
	if sameRoot(worker.target, worker.oldRoot) || sameRoot(worker.target, worker.source) {
		return nil, fmt.Errorf("target RootFS must be distinct from old and source roots")
	}

	targetManifest, err := Scan(request.TargetRoot)
	if err != nil {
		return nil, fmt.Errorf("scan target RootFS: %w", err)
	}
	worker.targetMF = *targetManifest
	worker.targetBy = nodesByPath(*targetManifest)
	for _, root := range []*secureRoot{worker.oldRoot, worker.source, worker.target} {
		if err := root.verifyIdentity(); err != nil {
			return nil, err
		}
	}

	plans, err := worker.preflight(ctx)
	if err != nil {
		return nil, err
	}
	if err := worker.execute(ctx, plans); err != nil {
		return nil, err
	}
	syncFD, err := worker.target.open(".", unix.O_RDONLY|unix.O_DIRECTORY, 0)
	if err != nil {
		return nil, fmt.Errorf("open target RootFS for sync: %w", err)
	}
	syncErr := unix.Syncfs(syncFD)
	closeErr := unix.Close(syncFD)
	if syncErr != nil {
		return nil, fmt.Errorf("sync target RootFS: %w", syncErr)
	}
	if closeErr != nil {
		return nil, fmt.Errorf("close target RootFS sync descriptor: %w", closeErr)
	}
	if err := worker.target.verifyIdentity(); err != nil {
		return nil, err
	}
	finalManifest, err := Scan(request.TargetRoot)
	if err != nil {
		return nil, fmt.Errorf("scan rebased RootFS: %w", err)
	}
	return worker.result(plans, *finalManifest)
}

func sameRoot(left, right *secureRoot) bool {
	return left.stat.Dev == right.stat.Dev && left.stat.Ino == right.stat.Ino
}

func validateApplyRequest(request ApplyRequest) error {
	if err := request.Old.Validate(); err != nil {
		return fmt.Errorf("old manifest: %w", err)
	}
	if err := request.Source.Validate(); err != nil {
		return fmt.Errorf("source manifest: %w", err)
	}
	if err := request.Diff.Validate(); err != nil {
		return fmt.Errorf("RootFS diff: %w", err)
	}
	oldBy := nodesByPath(request.Old)
	sourceBy := nodesByPath(request.Source)
	trustedLineage := request.Old.LineageID != "" && request.Old.LineageID == request.Source.LineageID
	for _, change := range request.Diff.Changes {
		oldNode, oldExists := oldBy[change.Path]
		sourceNode, sourceExists := sourceBy[change.Path]
		switch change.Kind {
		case ChangeAdd:
			if oldExists || !sourceExists {
				return fmt.Errorf("add change %q does not match manifests", change.Path)
			}
		case ChangeRemove:
			if !oldExists || sourceExists {
				return fmt.Errorf("remove change %q does not match manifests", change.Path)
			}
		case ChangeReplace:
			if !oldExists || !sourceExists ||
				(oldNode.Type == sourceNode.Type && !replacementRequired(oldNode, sourceNode, trustedLineage)) {
				return fmt.Errorf("replace change %q does not match manifests", change.Path)
			}
		case ChangeModify:
			if !oldExists || !sourceExists || oldNode.Type != sourceNode.Type {
				return fmt.Errorf("modify change %q does not match manifests", change.Path)
			}
		case ChangeRename:
			from, fromExists := oldBy[change.FromPath]
			_, sourceFromExists := sourceBy[change.FromPath]
			if oldExists || !sourceExists || !fromExists || sourceFromExists || from.Type != sourceNode.Type {
				return fmt.Errorf("rename change %q does not match manifests", change.Path)
			}
			oldNode = from
		}
		if sourceExists && sourceNode.Type == NodeSocket {
			return fmt.Errorf("persistent socket %q cannot be rebased", change.Path)
		}
		if change.HardlinkTarget != "" {
			target, exists := sourceBy[change.HardlinkTarget]
			if !exists || !sourceExists || sourceNode.Type != NodeRegular || target.Type != NodeRegular ||
				sourceNode.Device != target.Device || sourceNode.Inode != target.Inode {
				return fmt.Errorf("hardlink target for %q does not match source inode", change.Path)
			}
		}
		if len(change.Data) != 0 {
			if !sourceExists || sourceNode.Type != NodeRegular {
				return fmt.Errorf("data change %q does not name a source regular file", change.Path)
			}
			allocated := allocatedRanges(sourceNode)
			for _, data := range change.Data {
				covered := rangeCovered(allocated, data.Offset, data.Length)
				overlaps := rangeOverlaps(allocated, data.Offset, data.Length)
				withinSize := data.Offset < uint64(sourceNode.Size) &&
					data.Length <= uint64(sourceNode.Size)-data.Offset
				if data.SourceData && (!withinSize || !covered) {
					return fmt.Errorf("source data range for %q is not allocated", change.Path)
				}
				if !data.SourceData && overlaps {
					return fmt.Errorf("source hole range for %q overlaps allocated data", change.Path)
				}
			}
		}
	}
	return nil
}

func rangeCovered(ranges []ByteRange, offset, length uint64) bool {
	if length == 0 {
		return false
	}
	end := offset + length
	for _, value := range ranges {
		if value.Offset > offset {
			return false
		}
		if value.Offset <= offset && value.Offset+value.Length > offset {
			offset = min(end, value.Offset+value.Length)
			if offset == end {
				return true
			}
		}
	}
	return false
}

func rangeOverlaps(ranges []ByteRange, offset, length uint64) bool {
	end := offset + length
	for _, value := range ranges {
		if value.Offset >= end {
			return false
		}
		if value.Offset+value.Length > offset {
			return true
		}
	}
	return false
}

func (w *applyWorker) preflight(ctx context.Context) ([]changePlan, error) {
	w.checkRemovedDirectorySubtrees()
	plans := make([]changePlan, 0, len(w.request.Diff.Changes))
	for _, change := range w.request.Diff.Changes {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		plan := changePlan{change: change, currentPath: change.Path}
		var err error
		switch change.Kind {
		case ChangeAdd:
			err = w.preflightAdd(ctx, &plan)
		case ChangeRemove:
			err = w.preflightRemove(ctx, &plan)
		case ChangeReplace:
			err = w.preflightReplace(ctx, &plan)
		case ChangeModify:
			err = w.preflightModify(ctx, &plan, change.Path)
		case ChangeRename:
			err = w.preflightRename(ctx, &plan)
		}
		if err != nil {
			return nil, err
		}
		plans = append(plans, plan)
	}
	if err := w.conflict.err(); err != nil {
		return nil, err
	}
	return plans, nil
}

func (w *applyWorker) checkRemovedDirectorySubtrees() {
	for _, change := range w.request.Diff.Changes {
		oldPath := change.Path
		oldNode, exists := w.oldBy[oldPath]
		if change.Kind == ChangeRename {
			continue
		}
		if !exists || oldNode.Type != NodeDirectory ||
			(change.Kind != ChangeRemove && change.Kind != ChangeReplace) {
			continue
		}
		prefix := oldPath + "/"
		for _, targetNode := range w.targetMF.Nodes {
			if strings.HasPrefix(targetNode.Path, prefix) {
				if _, existed := w.oldBy[targetNode.Path]; !existed {
					w.conflict.add(targetNode.Path, "directory", "new base added an entry under a removed directory")
				}
			}
		}
	}
}

func (w *applyWorker) preflightAdd(ctx context.Context, plan *changePlan) error {
	plan.source = w.sourceBy[plan.change.Path]
	plan.target, plan.targetExists = w.targetBy[plan.change.Path]
	if plan.change.HardlinkTarget != "" {
		if !plan.targetExists {
			plan.structure = structureHardlink
			return nil
		}
		target, exists := w.targetBy[plan.change.HardlinkTarget]
		if exists && plan.target.Type == NodeRegular && target.Type == NodeRegular &&
			plan.target.Device == target.Device && plan.target.Inode == target.Inode {
			return nil
		}
		w.conflict.add(plan.change.Path, "hardlink", "new base already contains a different entry")
		return nil
	}
	if !plan.targetExists {
		plan.structure = structureCreate
		return nil
	}
	equal, err := w.nodesEqual(ctx, w.source, plan.change.Path, plan.source, rootSource,
		w.target, plan.change.Path, plan.target, rootTarget)
	if err != nil {
		return err
	}
	if !equal {
		w.conflict.add(plan.change.Path, "add", "old and new bases both added different entries")
	}
	return nil
}

func (w *applyWorker) preflightRemove(ctx context.Context, plan *changePlan) error {
	plan.old = w.oldBy[plan.change.Path]
	plan.target, plan.targetExists = w.targetBy[plan.change.Path]
	if !plan.targetExists {
		return nil
	}
	if plan.target.Type != plan.old.Type {
		w.conflict.add(plan.change.Path, "remove", "new base replaced the entry")
		return nil
	}
	if plan.old.Type == NodeRegular && w.inodeSurvives(plan.old) {
		plan.structure = structureRemove
		return nil
	}
	equal, err := w.nodesEqual(ctx, w.oldRoot, plan.change.Path, plan.old, rootOld,
		w.target, plan.change.Path, plan.target, rootTarget)
	if err != nil {
		return err
	}
	if !equal {
		w.conflict.add(plan.change.Path, "remove", "new base modified the entry deleted by the source")
		return nil
	}
	plan.structure = structureRemove
	return nil
}

func (w *applyWorker) preflightReplace(ctx context.Context, plan *changePlan) error {
	plan.old = w.oldBy[plan.change.Path]
	plan.source = w.sourceBy[plan.change.Path]
	plan.target, plan.targetExists = w.targetBy[plan.change.Path]
	if !plan.targetExists {
		w.conflict.add(plan.change.Path, "replace", "new base removed the entry")
		return nil
	}
	if plan.target.Type == plan.source.Type {
		equal, err := w.nodesEqual(ctx, w.source, plan.change.Path, plan.source, rootSource,
			w.target, plan.change.Path, plan.target, rootTarget)
		if err != nil {
			return err
		}
		if equal {
			return nil
		}
	}
	if plan.target.Type != plan.old.Type {
		w.conflict.add(plan.change.Path, "replace", "new base independently replaced the entry")
		return nil
	}
	equal, err := w.nodesEqual(ctx, w.oldRoot, plan.change.Path, plan.old, rootOld,
		w.target, plan.change.Path, plan.target, rootTarget)
	if err != nil {
		return err
	}
	if !equal {
		w.conflict.add(plan.change.Path, "replace", "new base modified the replaced entry")
		return nil
	}
	if plan.change.HardlinkTarget != "" {
		plan.structure = structureHardlink
	} else {
		plan.structure = structureReplace
	}
	return nil
}

func (w *applyWorker) preflightModify(ctx context.Context, plan *changePlan, currentPath string) error {
	plan.old = w.oldBy[plan.change.Path]
	plan.source = w.sourceBy[plan.change.Path]
	plan.target, plan.targetExists = w.targetBy[currentPath]
	plan.currentPath = currentPath
	if !plan.targetExists || plan.target.Type != plan.old.Type {
		w.conflict.add(plan.change.Path, "modify", "new base removed or replaced the modified entry")
		return nil
	}
	w.preflightMetadata(plan)
	if plan.source.Type == NodeRegular {
		if err := w.preflightData(ctx, plan); err != nil {
			return err
		}
	}
	return nil
}

func (w *applyWorker) preflightRename(ctx context.Context, plan *changePlan) error {
	plan.old = w.oldBy[plan.change.FromPath]
	plan.source = w.sourceBy[plan.change.Path]
	destination, destinationExists := w.targetBy[plan.change.Path]
	from, fromExists := w.targetBy[plan.change.FromPath]
	if destinationExists {
		if fromExists {
			w.conflict.add(plan.change.Path, "rename", "new base still has the source and occupies the destination")
			return nil
		}
		equal, err := w.nodesEqual(ctx, w.source, plan.change.Path, plan.source, rootSource,
			w.target, plan.change.Path, destination, rootTarget)
		if err != nil {
			return err
		}
		if !equal {
			w.conflict.add(plan.change.Path, "rename", "new base occupies the rename destination")
		}
		return nil
	}
	if !fromExists || from.Type != plan.old.Type {
		w.conflict.add(plan.change.FromPath, "rename", "new base removed or replaced the rename source")
		return nil
	}
	plan.target = from
	plan.targetExists = true
	plan.currentPath = plan.change.FromPath
	plan.structure = structureRename
	w.preflightMetadata(plan)
	if plan.source.Type == NodeRegular {
		if err := w.preflightData(ctx, plan); err != nil {
			return err
		}
	}
	return nil
}

func (w *applyWorker) inodeSurvives(old Node) bool {
	for _, source := range w.request.Source.Nodes {
		oldPeer, existed := w.oldBy[source.Path]
		if existed && oldPeer.Type == NodeRegular && oldPeer.Inode == old.Inode &&
			source.Type == NodeRegular && source.Inode == old.Inode &&
			(!old.GenerationKnown || !source.GenerationKnown || source.Generation == old.Generation) {
			return true
		}
	}
	return false
}

func (w *applyWorker) preflightMetadata(plan *changePlan) {
	plan.fields = make(map[string]bool)
	for _, field := range plan.change.Metadata.Fields {
		if field == "mtime" {
			if !fieldEqual(field, plan.target, plan.source) {
				plan.fields[field] = true
			}
			continue
		}
		if fieldEqual(field, plan.target, plan.source) {
			continue
		}
		if fieldEqual(field, plan.target, plan.old) {
			plan.fields[field] = true
			continue
		}
		w.conflict.add(plan.change.Path, field, "old and new bases both changed the field")
	}
	setNames := make([]string, 0, len(plan.change.Metadata.SetXattrs))
	for name := range plan.change.Metadata.SetXattrs {
		setNames = append(setNames, name)
	}
	sort.Strings(setNames)
	for _, name := range setNames {
		sourceValue := plan.change.Metadata.SetXattrs[name]
		targetValue, targetExists := plan.target.Xattrs[name]
		oldValue, oldExists := plan.old.Xattrs[name]
		if optionalBytesEqual(targetValue, targetExists, sourceValue, true) {
			continue
		}
		if optionalBytesEqual(targetValue, targetExists, oldValue, oldExists) {
			if plan.setXattrs == nil {
				plan.setXattrs = make(map[string][]byte)
			}
			plan.setXattrs[name] = append([]byte(nil), sourceValue...)
			continue
		}
		w.conflict.add(plan.change.Path, "xattr:"+name, "old and new bases both changed the xattr")
	}
	for _, name := range plan.change.Metadata.RemoveXattrs {
		_, targetExists := plan.target.Xattrs[name]
		oldValue, oldExists := plan.old.Xattrs[name]
		if !targetExists {
			continue
		}
		targetValue := plan.target.Xattrs[name]
		if optionalBytesEqual(targetValue, true, oldValue, oldExists) {
			plan.removeXattrs = append(plan.removeXattrs, name)
			continue
		}
		w.conflict.add(plan.change.Path, "xattr:"+name, "new base changed the xattr removed by the source")
	}
	if len(plan.fields) == 0 {
		plan.fields = nil
	}
}

func fieldEqual(field string, left, right Node) bool {
	switch field {
	case "mode":
		return left.Mode == right.Mode
	case "uid":
		return left.UID == right.UID
	case "gid":
		return left.GID == right.GID
	case "mtime":
		return left.ModTimeNS == right.ModTimeNS
	case "rdev":
		return left.Rdev == right.Rdev
	case "size":
		return left.Size == right.Size
	case "link_target":
		return left.LinkTarget == right.LinkTarget
	default:
		return false
	}
}

func optionalBytesEqual(left []byte, leftExists bool, right []byte, rightExists bool) bool {
	return leftExists == rightExists && (!leftExists || bytes.Equal(left, right))
}

func (w *applyWorker) preflightData(ctx context.Context, plan *changePlan) error {
	for _, data := range plan.change.Data {
		if data.Offset >= uint64(plan.source.Size) {
			continue // The size merge owns a truncated tail.
		}
		length := min(data.Length, uint64(plan.source.Size)-data.Offset)
		targetMatchesOld, err := w.rangesEqual(ctx,
			w.target, plan.currentPath, plan.target, rootTarget,
			w.oldRoot, oldDataPath(plan.change), plan.old, rootOld,
			data.Offset, length)
		if err != nil {
			return err
		}
		if targetMatchesOld {
			plan.data = append(plan.data, DataChange{Offset: data.Offset, Length: length, SourceData: data.SourceData})
			continue
		}
		targetMatchesSource, err := w.rangesEqual(ctx,
			w.target, plan.currentPath, plan.target, rootTarget,
			w.source, plan.change.Path, plan.source, rootSource,
			data.Offset, length)
		if err != nil {
			return err
		}
		if !targetMatchesSource {
			w.conflict.add(plan.change.Path,
				fmt.Sprintf("data:%d-%d", data.Offset, data.Offset+length),
				"old and new bases both changed the byte range")
		}
	}
	return nil
}

func oldDataPath(change Change) string {
	if change.Kind == ChangeRename {
		return change.FromPath
	}
	return change.Path
}

func (w *applyWorker) nodesEqual(
	ctx context.Context,
	leftRoot *secureRoot, leftPath string, left Node, leftKind rootKind,
	rightRoot *secureRoot, rightPath string, right Node, rightKind rootKind,
) (bool, error) {
	if left.Type != right.Type || left.Mode != right.Mode || left.UID != right.UID ||
		left.GID != right.GID || left.ModTimeNS != right.ModTimeNS || left.Rdev != right.Rdev ||
		left.LinkTarget != right.LinkTarget || !xattrsEqual(left.Xattrs, right.Xattrs) {
		return false, nil
	}
	if left.Type != NodeDirectory && left.Size != right.Size {
		return false, nil
	}
	if left.Type != NodeRegular {
		return true, nil
	}
	leftAllocated := allocatedRanges(left)
	rightAllocated := allocatedRanges(right)
	if !byteRangesEqual(leftAllocated, rightAllocated) {
		return false, nil
	}
	for _, value := range leftAllocated {
		equal, err := w.rangesEqual(ctx,
			leftRoot, leftPath, left, leftKind,
			rightRoot, rightPath, right, rightKind,
			value.Offset, value.Length)
		if err != nil || !equal {
			return equal, err
		}
	}
	return true, nil
}

func xattrsEqual(left, right map[string][]byte) bool {
	if len(left) != len(right) {
		return false
	}
	for name, value := range left {
		rightValue, exists := right[name]
		if !exists || !bytes.Equal(value, rightValue) {
			return false
		}
	}
	return true
}

func byteRangesEqual(left, right []ByteRange) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func (w *applyWorker) rangesEqual(
	ctx context.Context,
	leftRoot *secureRoot, leftPath string, left Node, leftKind rootKind,
	rightRoot *secureRoot, rightPath string, right Node, rightKind rootKind,
	offset, length uint64,
) (bool, error) {
	segments := allocatedUnion(left, right, offset, length)
	if len(segments) == 0 {
		return true, nil
	}
	leftFD, err := leftRoot.open(leftPath, unix.O_RDONLY, 0)
	if err != nil {
		return false, fmt.Errorf("open %s for comparison: %w", leftPath, err)
	}
	defer unix.Close(leftFD)
	rightFD, err := rightRoot.open(rightPath, unix.O_RDONLY, 0)
	if err != nil {
		return false, fmt.Errorf("open %s for comparison: %w", rightPath, err)
	}
	defer unix.Close(rightFD)
	leftBuffer := make([]byte, rebaseIOBufferSize)
	rightBuffer := make([]byte, rebaseIOBufferSize)
	for _, segment := range segments {
		for position := segment.Offset; position < segment.Offset+segment.Length; {
			if err := ctx.Err(); err != nil {
				return false, err
			}
			chunk := min(uint64(len(leftBuffer)), segment.Offset+segment.Length-position)
			leftBytes := leftBuffer[:chunk]
			rightBytes := rightBuffer[:chunk]
			if err := preadVirtual(leftFD, left.Size, position, leftBytes); err != nil {
				return false, err
			}
			if err := preadVirtual(rightFD, right.Size, position, rightBytes); err != nil {
				return false, err
			}
			w.addRead(leftKind, chunk)
			w.addRead(rightKind, chunk)
			if !bytes.Equal(leftBytes, rightBytes) {
				return false, nil
			}
			position += chunk
		}
	}
	return true, nil
}

func allocatedUnion(left, right Node, offset, length uint64) []ByteRange {
	end := offset + length
	var values []ByteRange
	for _, ranges := range [][]ByteRange{allocatedRanges(left), allocatedRanges(right)} {
		for _, value := range ranges {
			start := max(offset, value.Offset)
			valueEnd := min(end, value.Offset+value.Length)
			if start < valueEnd {
				values = append(values, ByteRange{Offset: start, Length: valueEnd - start})
			}
		}
	}
	return mergeByteRanges(values)
}

func preadVirtual(fd int, size int64, offset uint64, buffer []byte) error {
	clear(buffer)
	if offset >= uint64(size) {
		return nil
	}
	available := min(uint64(len(buffer)), uint64(size)-offset)
	read := 0
	for read < int(available) {
		count, err := unix.Pread(fd, buffer[read:available], int64(offset)+int64(read))
		if err != nil {
			return err
		}
		if count == 0 {
			return fmt.Errorf("unexpected EOF at byte %d", offset+uint64(read))
		}
		read += count
	}
	return nil
}

func (w *applyWorker) addRead(kind rootKind, count uint64) {
	switch kind {
	case rootOld:
		w.stats.OldReadBytes += count
	case rootSource:
		w.stats.SourceReadBytes += count
	case rootTarget:
		w.stats.TargetReadBytes += count
	}
}

func (w *applyWorker) execute(ctx context.Context, plans []changePlan) error {
	ordered := append([]changePlan(nil), plans...)
	sort.Slice(ordered, func(i, j int) bool {
		leftDepth := strings.Count(ordered[i].change.Path, "/")
		rightDepth := strings.Count(ordered[j].change.Path, "/")
		if leftDepth == rightDepth {
			return ordered[i].change.Path < ordered[j].change.Path
		}
		return leftDepth < rightDepth
	})

	// Directory targets must exist before a rename or child creation uses them.
	for index := range ordered {
		plan := &ordered[index]
		if (plan.structure == structureCreate || plan.structure == structureReplace) &&
			plan.source.Type == NodeDirectory {
			if err := w.replaceIfNeeded(*plan); err != nil {
				return err
			}
			if err := w.createNode(ctx, *plan); err != nil {
				return err
			}
			plan.created = true
			plan.structure = structureNone
		}
	}
	for index := range ordered {
		plan := &ordered[index]
		if plan.structure == structureRename {
			if err := w.renameNode(plan.change.FromPath, plan.change.Path); err != nil {
				return err
			}
			plan.currentPath = plan.change.Path
			plan.structure = structureNone
		}
	}

	// Remove leaves before parents, after files have moved out of removed trees.
	for index := len(ordered) - 1; index >= 0; index-- {
		plan := &ordered[index]
		if plan.structure == structureRemove {
			if err := w.removeNode(plan.change.Path, plan.target.Type); err != nil {
				return err
			}
			plan.structure = structureNone
		}
	}
	for index := range ordered {
		plan := &ordered[index]
		if (plan.structure == structureCreate || plan.structure == structureReplace) &&
			plan.source.Type != NodeDirectory && plan.change.HardlinkTarget == "" {
			if err := w.replaceIfNeeded(*plan); err != nil {
				return err
			}
			if err := w.createNode(ctx, *plan); err != nil {
				return err
			}
			plan.created = true
			plan.structure = structureNone
		}
	}
	for index := range ordered {
		plan := &ordered[index]
		if plan.structure == structureHardlink {
			if plan.change.Kind == ChangeReplace {
				if err := w.removeNode(plan.change.Path, plan.target.Type); err != nil {
					return err
				}
			}
			if err := w.createHardlink(plan.change.HardlinkTarget, plan.change.Path); err != nil {
				return err
			}
			plan.structure = structureNone
		}
	}

	// Data precedes metadata so writes cannot perturb the final mode or mtime.
	for index := range ordered {
		plan := &ordered[index]
		if len(plan.data) == 0 && !plan.fields["size"] {
			continue
		}
		if err := w.applyRegularData(ctx, *plan); err != nil {
			return err
		}
		delete(plan.fields, "size")
	}
	for index := range ordered {
		plan := &ordered[index]
		if plan.change.Kind == ChangeAdd || plan.change.Kind == ChangeReplace {
			if plan.change.HardlinkTarget == "" && plan.created {
				if err := w.applyMetadata(plan.change.Path, plan.source, fullMetadata(plan.source)); err != nil {
					return err
				}
			}
			continue
		}
		if len(plan.fields) != 0 || len(plan.setXattrs) != 0 || len(plan.removeXattrs) != 0 {
			metadata := MetadataChange{SetXattrs: plan.setXattrs, RemoveXattrs: plan.removeXattrs}
			for field := range plan.fields {
				metadata.Fields = append(metadata.Fields, field)
			}
			sort.Strings(metadata.Fields)
			if err := w.applyMetadata(plan.change.Path, plan.source, metadata); err != nil {
				return err
			}
		}
	}
	return nil
}

func (w *applyWorker) replaceIfNeeded(plan changePlan) error {
	if plan.structure != structureReplace {
		return nil
	}
	return w.removeNode(plan.change.Path, plan.target.Type)
}

func (w *applyWorker) removeNode(relative string, nodeType NodeType) error {
	parent, leaf, err := w.target.parent(relative)
	if err != nil {
		return fmt.Errorf("open parent for removal %s: %w", relative, err)
	}
	defer unix.Close(parent)
	flags := 0
	if nodeType == NodeDirectory {
		flags = unix.AT_REMOVEDIR
	}
	if err := unix.Unlinkat(parent, leaf, flags); err != nil {
		return fmt.Errorf("remove %s: %w", relative, err)
	}
	return nil
}

func (w *applyWorker) renameNode(from, target string) error {
	fromParent, fromLeaf, err := w.target.parent(from)
	if err != nil {
		return fmt.Errorf("open rename source %s: %w", from, err)
	}
	defer unix.Close(fromParent)
	targetParent, targetLeaf, err := w.target.parent(target)
	if err != nil {
		return fmt.Errorf("open rename target %s: %w", target, err)
	}
	defer unix.Close(targetParent)
	if err := unix.Renameat2(fromParent, fromLeaf, targetParent, targetLeaf, unix.RENAME_NOREPLACE); err != nil {
		return fmt.Errorf("rename %s to %s: %w", from, target, err)
	}
	return nil
}

func (w *applyWorker) createHardlink(target, relative string) error {
	targetParent, targetLeaf, err := w.target.parent(target)
	if err != nil {
		return fmt.Errorf("open hardlink target %s: %w", target, err)
	}
	defer unix.Close(targetParent)
	parent, leaf, err := w.target.parent(relative)
	if err != nil {
		return fmt.Errorf("open hardlink parent %s: %w", relative, err)
	}
	defer unix.Close(parent)
	if err := unix.Linkat(targetParent, targetLeaf, parent, leaf, 0); err != nil {
		return fmt.Errorf("link %s to %s: %w", relative, target, err)
	}
	return nil
}

func (w *applyWorker) createNode(ctx context.Context, plan changePlan) error {
	parent, leaf, err := w.target.parent(plan.change.Path)
	if err != nil {
		return fmt.Errorf("open create parent %s: %w", plan.change.Path, err)
	}
	defer unix.Close(parent)
	switch plan.source.Type {
	case NodeDirectory:
		if err := unix.Mkdirat(parent, leaf, 0o700); err != nil {
			return fmt.Errorf("create directory %s: %w", plan.change.Path, err)
		}
	case NodeRegular:
		fd, err := w.target.open(plan.change.Path, unix.O_CREAT|unix.O_EXCL|unix.O_RDWR, 0o600)
		if err != nil {
			return fmt.Errorf("create regular file %s: %w", plan.change.Path, err)
		}
		if err := unix.Ftruncate(fd, plan.source.Size); err != nil {
			unix.Close(fd)
			return fmt.Errorf("size regular file %s: %w", plan.change.Path, err)
		}
		for _, value := range allocatedRanges(plan.source) {
			if err := w.copySourceRange(ctx, plan.change.Path, fd, value.Offset, value.Length); err != nil {
				unix.Close(fd)
				return err
			}
		}
		if err := unix.Fsync(fd); err != nil {
			unix.Close(fd)
			return fmt.Errorf("fsync %s: %w", plan.change.Path, err)
		}
		if err := unix.Close(fd); err != nil {
			return err
		}
	case NodeSymlink:
		if err := unix.Symlinkat(plan.source.LinkTarget, parent, leaf); err != nil {
			return fmt.Errorf("create symlink %s: %w", plan.change.Path, err)
		}
	case NodeCharDevice, NodeBlockDevice, NodeFIFO:
		mode := plan.source.Mode&unix.S_IFMT | 0o600
		if err := unix.Mknodat(parent, leaf, mode, int(plan.source.Rdev)); err != nil {
			return fmt.Errorf("create special file %s: %w", plan.change.Path, err)
		}
	default:
		return fmt.Errorf("cannot create RootFS node type %q", plan.source.Type)
	}
	return nil
}

func (w *applyWorker) applyRegularData(ctx context.Context, plan changePlan) error {
	fd, err := w.target.open(plan.change.Path, unix.O_RDWR, 0)
	if err != nil {
		return fmt.Errorf("open target data %s: %w", plan.change.Path, err)
	}
	defer unix.Close(fd)
	if plan.fields["size"] && plan.source.Size > plan.target.Size {
		if err := unix.Ftruncate(fd, plan.source.Size); err != nil {
			return err
		}
	}
	for _, data := range plan.data {
		if data.SourceData {
			if err := w.copySourceRange(ctx, plan.change.Path, fd, data.Offset, data.Length); err != nil {
				return err
			}
			continue
		}
		if err := unix.Fallocate(fd, unix.FALLOC_FL_PUNCH_HOLE|unix.FALLOC_FL_KEEP_SIZE,
			int64(data.Offset), int64(data.Length)); err != nil {
			return fmt.Errorf("punch target hole %s at %d: %w", plan.change.Path, data.Offset, err)
		}
		w.stats.PunchedBytes += data.Length
	}
	if plan.fields["size"] && plan.source.Size <= plan.target.Size {
		if err := unix.Ftruncate(fd, plan.source.Size); err != nil {
			return err
		}
	}
	if err := unix.Fsync(fd); err != nil {
		return fmt.Errorf("fsync target data %s: %w", plan.change.Path, err)
	}
	return nil
}

func (w *applyWorker) copySourceRange(ctx context.Context, relative string, targetFD int, offset, length uint64) error {
	if offset > math.MaxInt64 || length > math.MaxInt64 || offset+length > math.MaxInt64 {
		return fmt.Errorf("source range for %s exceeds signed file offsets", relative)
	}
	sourceFD, err := w.source.open(relative, unix.O_RDONLY, 0)
	if err != nil {
		return fmt.Errorf("open source data %s: %w", relative, err)
	}
	defer unix.Close(sourceFD)
	buffer := make([]byte, rebaseIOBufferSize)
	for position := offset; position < offset+length; {
		if err := ctx.Err(); err != nil {
			return err
		}
		chunk := min(uint64(len(buffer)), offset+length-position)
		read := 0
		for read < int(chunk) {
			count, err := unix.Pread(sourceFD, buffer[read:chunk], int64(position)+int64(read))
			if err != nil {
				return fmt.Errorf("read source data %s: %w", relative, err)
			}
			if count == 0 {
				return fmt.Errorf("unexpected source EOF in %s at %d", relative, position+uint64(read))
			}
			read += count
		}
		written := 0
		for written < int(chunk) {
			count, err := unix.Pwrite(targetFD, buffer[written:chunk], int64(position)+int64(written))
			if err != nil {
				return fmt.Errorf("write target data %s: %w", relative, err)
			}
			if count == 0 {
				return fmt.Errorf("short target write in %s at %d", relative, position+uint64(written))
			}
			written += count
		}
		w.stats.SourceReadBytes += chunk
		w.stats.WrittenBytes += chunk
		position += chunk
	}
	return nil
}

func (w *applyWorker) applyMetadata(relative string, source Node, metadata MetadataChange) error {
	fields := make(map[string]bool, len(metadata.Fields))
	for _, field := range metadata.Fields {
		fields[field] = true
	}
	if fields["uid"] || fields["gid"] {
		uid, gid := -1, -1
		if fields["uid"] {
			uid = int(source.UID)
		}
		if fields["gid"] {
			gid = int(source.GID)
		}
		if err := w.chown(relative, uid, gid); err != nil {
			return fmt.Errorf("chown %s: %w", relative, err)
		}
	}
	if fields["mode"] && source.Type != NodeSymlink {
		if err := w.chmod(relative, source.Mode&0o7777); err != nil {
			return fmt.Errorf("chmod %s: %w", relative, err)
		}
	}
	for _, name := range metadata.RemoveXattrs {
		if err := w.removeXattr(relative, name); err != nil && !errors.Is(err, unix.ENODATA) {
			return fmt.Errorf("remove xattr %s on %s: %w", name, relative, err)
		}
	}
	names := make([]string, 0, len(metadata.SetXattrs))
	for name := range metadata.SetXattrs {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		if err := w.setXattr(relative, name, metadata.SetXattrs[name]); err != nil {
			return fmt.Errorf("set xattr %s on %s: %w", name, relative, err)
		}
	}
	if fields["mtime"] {
		if err := w.setMtime(relative, source.ModTimeNS); err != nil {
			return fmt.Errorf("set mtime on %s: %w", relative, err)
		}
	}
	return nil
}

func (w *applyWorker) chown(relative string, uid, gid int) error {
	if relative == "." {
		fd, err := w.target.open(".", unix.O_RDONLY|unix.O_DIRECTORY, 0)
		if err != nil {
			return err
		}
		defer unix.Close(fd)
		return unix.Fchown(fd, uid, gid)
	}
	parent, leaf, err := w.target.parent(relative)
	if err != nil {
		return err
	}
	defer unix.Close(parent)
	return unix.Fchownat(parent, leaf, uid, gid, unix.AT_SYMLINK_NOFOLLOW)
}

func (w *applyWorker) chmod(relative string, mode uint32) error {
	if relative == "." {
		fd, err := w.target.open(".", unix.O_RDONLY|unix.O_DIRECTORY, 0)
		if err != nil {
			return err
		}
		defer unix.Close(fd)
		return unix.Fchmod(fd, mode)
	}
	parent, leaf, err := w.target.parent(relative)
	if err != nil {
		return err
	}
	defer unix.Close(parent)
	return unix.Fchmodat(parent, leaf, mode, 0)
}

func (w *applyWorker) removeXattr(relative, name string) error {
	if relative == "." {
		fd, err := w.target.open(".", unix.O_RDONLY|unix.O_DIRECTORY, 0)
		if err != nil {
			return err
		}
		defer unix.Close(fd)
		return unix.Fremovexattr(fd, name)
	}
	parent, leaf, err := w.target.parent(relative)
	if err != nil {
		return err
	}
	defer unix.Close(parent)
	return unix.Lremovexattr(procLeafPath(parent, leaf), name)
}

func (w *applyWorker) setXattr(relative, name string, value []byte) error {
	if relative == "." {
		fd, err := w.target.open(".", unix.O_RDONLY|unix.O_DIRECTORY, 0)
		if err != nil {
			return err
		}
		defer unix.Close(fd)
		return unix.Fsetxattr(fd, name, value, 0)
	}
	parent, leaf, err := w.target.parent(relative)
	if err != nil {
		return err
	}
	defer unix.Close(parent)
	return unix.Lsetxattr(procLeafPath(parent, leaf), name, value, 0)
}

func (w *applyWorker) setMtime(relative string, nanoseconds int64) error {
	times := []unix.Timespec{{Nsec: unix.UTIME_OMIT}, unix.NsecToTimespec(nanoseconds)}
	if relative == "." {
		return unix.UtimesNanoAt(w.target.fd, ".", times, 0)
	}
	parent, leaf, err := w.target.parent(relative)
	if err != nil {
		return err
	}
	defer unix.Close(parent)
	return unix.UtimesNanoAt(parent, leaf, times, unix.AT_SYMLINK_NOFOLLOW)
}

func (w *applyWorker) result(plans []changePlan, target Manifest) (*ApplyResult, error) {
	oldDigest, err := w.request.Old.Digest()
	if err != nil {
		return nil, err
	}
	sourceDigest, err := w.request.Source.Digest()
	if err != nil {
		return nil, err
	}
	diffDigest, err := w.request.Diff.Digest()
	if err != nil {
		return nil, err
	}
	targetDigest, err := target.Digest()
	if err != nil {
		return nil, err
	}
	result := &ApplyResult{
		Version: ApplyResultVersion, TargetNodeCount: len(target.Nodes),
		OldManifestDigest: oldDigest, SourceManifestDigest: sourceDigest,
		DiffDigest: diffDigest, TargetManifestDigest: targetDigest, IO: w.stats,
	}
	for _, plan := range plans {
		if plan.mutates() {
			result.AppliedChanges++
		} else {
			result.ConvergedChanges++
		}
	}
	proofPayload := struct {
		Version              int    `json:"version"`
		TargetNodeCount      int    `json:"target_node_count"`
		OldManifestDigest    string `json:"old_manifest_digest"`
		SourceManifestDigest string `json:"source_manifest_digest"`
		DiffDigest           string `json:"diff_digest"`
		TargetManifestDigest string `json:"target_manifest_digest"`
	}{
		Version: result.Version, TargetNodeCount: result.TargetNodeCount,
		OldManifestDigest: result.OldManifestDigest, SourceManifestDigest: result.SourceManifestDigest,
		DiffDigest: result.DiffDigest, TargetManifestDigest: result.TargetManifestDigest,
	}
	payload, err := json.Marshal(proofPayload)
	if err != nil {
		return nil, err
	}
	proof := sha256.Sum256(payload)
	result.HealthProof = hex.EncodeToString(proof[:])
	return result, nil
}
