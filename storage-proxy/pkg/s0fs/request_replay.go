package s0fs

import (
	"fmt"
	"slices"
	"sort"
	"time"
)

const (
	// RecoverableFUSEMaxBackground is the maximum number of requests the ctld
	// recoverable connection permits the kernel to leave unanswered.
	RecoverableFUSEMaxBackground = 128
	// DefaultRequestReplayCapacity leaves ample headroom over max_background
	// while keeping the node-local response ledger bounded.
	DefaultRequestReplayCapacity = 4096

	requestAckWALOperation    = "request_ack"
	requestResultWALOperation = "request_result"

	requestSetAttrMode uint32 = 1 << 0
	requestSetAttrUID  uint32 = 1 << 1
	requestSetAttrGID  uint32 = 1 << 2
	requestSetAttrSize uint32 = 1 << 3

	xattrCreate  = uint32(1)
	xattrReplace = uint32(2)
)

// Fail compilation if the default ever stops covering every in-flight FUSE
// request that the configured kernel connection can resend.
var _ [DefaultRequestReplayCapacity - RecoverableFUSEMaxBackground]struct{}

// RequestKey is stable for the lifetime of one recoverable FUSE connection.
type RequestKey struct {
	Scope  string
	Unique uint64
}

type requestLedgerKey struct {
	Scope  string `json:"scope"`
	Unique uint64 `json:"unique"`
}

func ledgerKey(key RequestKey) requestLedgerKey {
	return requestLedgerKey(key)
}

// RequestMutationKind identifies one logical FUSE mutation. Several metadata
// changes from one SetAttr request intentionally share one WAL record.
type RequestMutationKind string

const (
	RequestCreate      RequestMutationKind = "create"
	RequestMkdir       RequestMutationKind = "mkdir"
	RequestMknod       RequestMutationKind = "mknod"
	RequestUnlink      RequestMutationKind = "unlink"
	RequestRmdir       RequestMutationKind = "rmdir"
	RequestRename      RequestMutationKind = "rename"
	RequestLink        RequestMutationKind = "link"
	RequestSymlink     RequestMutationKind = "symlink"
	RequestWrite       RequestMutationKind = "write"
	RequestTruncate    RequestMutationKind = "truncate"
	RequestSetAttr     RequestMutationKind = "setattr"
	RequestSetXattr    RequestMutationKind = "setxattr"
	RequestRemoveXattr RequestMutationKind = "removexattr"
)

// RequestMutation contains the deterministic input of one FUSE mutation. Once
// a request identity is committed, later calls with that identity return the
// original result without consulting replacement mutation input.
type RequestMutation struct {
	Kind         RequestMutationKind `json:"kind"`
	Inode        uint64              `json:"inode,omitempty"`
	Parent       uint64              `json:"parent,omitempty"`
	Name         string              `json:"name,omitempty"`
	NewParent    uint64              `json:"new_parent,omitempty"`
	NewName      string              `json:"new_name,omitempty"`
	Type         FileType            `json:"type,omitempty"`
	Mode         uint32              `json:"mode,omitempty"`
	UID          uint32              `json:"uid,omitempty"`
	GID          uint32              `json:"gid,omitempty"`
	Offset       uint64              `json:"offset,omitempty"`
	Data         []byte              `json:"data,omitempty"`
	Target       string              `json:"target,omitempty"`
	SetAttrValid uint32              `json:"setattr_valid,omitempty"`
}

// RequestMutationResult is the response material needed to reproduce the
// original FUSE reply after a ctld crash. Handle IDs are inode-derived and do
// not need a separate durable field.
type RequestMutationResult struct {
	Node         *Node  `json:"node,omitempty"`
	Inode        uint64 `json:"inode,omitempty"`
	BytesWritten uint64 `json:"bytes_written,omitempty"`
}

type requestReplayEntry struct {
	result   RequestMutationResult
	timeUnix int64
}

// ApplyRequestMutation atomically appends the mutation identity, operation,
// and deterministic result in the existing S0FS WAL record. A repeated key
// returns the original result without applying the operation again.
func (e *Engine) ApplyRequestMutation(key RequestKey, mutation RequestMutation) (RequestMutationResult, bool, error) {
	return e.ApplyRecoverableRequestMutation(key, mutation, false)
}

// ApplyRecoverableRequestMutation fails closed when resend is true and the
// original durable result is absent. Applying such a request as new could
// duplicate a mutation after ledger corruption or an incompatible restore.
func (e *Engine) ApplyRecoverableRequestMutation(key RequestKey, mutation RequestMutation, resend bool) (RequestMutationResult, bool, error) {
	if key.Scope == "" || key.Unique == 0 {
		return RequestMutationResult{}, false, fmt.Errorf("%w: request scope and unique are required", ErrInvalidInput)
	}
	internalKey := ledgerKey(key)
	e.mutationMu.Lock()
	defer e.mutationMu.Unlock()
	e.mu.Lock()
	defer e.mu.Unlock()
	if err := e.checkOpen(); err != nil {
		return RequestMutationResult{}, false, err
	}
	if existing, ok := e.requestResults[internalKey]; ok {
		return cloneRequestResult(existing.result), true, nil
	}
	if resend {
		return RequestMutationResult{}, false, fmt.Errorf("%w: scope %q unique %d", ErrRequestResultMissing, key.Scope, key.Unique)
	}
	if err := e.reserveRequestResultLocked(); err != nil {
		return RequestMutationResult{}, false, err
	}

	record, projectedBytes, err := e.prepareRequestRecordLocked(internalKey, mutation)
	if err != nil {
		return RequestMutationResult{}, false, err
	}
	if err := e.appendAndApplyLocked(record, projectedBytes); err != nil {
		return RequestMutationResult{}, false, err
	}
	e.storeRequestResultLocked(record)
	return requestResultFromRecord(record), false, nil
}

// AcknowledgeRequest marks that the kernel accepted the original reply. The
// completion path only changes bounded memory state; acknowledgements are
// batched into the WAL by an existing fsync, at capacity pressure, or removed
// atomically by the existing materialize/reset path.
func (e *Engine) AcknowledgeRequest(key RequestKey) error {
	e.mutationMu.Lock()
	defer e.mutationMu.Unlock()
	e.mu.Lock()
	defer e.mu.Unlock()
	if err := e.checkOpen(); err != nil {
		return err
	}
	internalKey := ledgerKey(key)
	_, ok := e.requestResults[internalKey]
	if !ok {
		return nil
	}
	delete(e.requestResults, internalKey)
	e.pendingRequestAcks = append(e.pendingRequestAcks, internalKey)
	e.advanceRequestOrderHeadLocked()
	e.compactRequestOrderLocked()
	if len(e.pendingRequestAcks) >= e.requestAckFlushThresholdLocked() {
		return e.persistAcknowledgedRequestsLocked()
	}
	return nil
}

// RequestReplayCounts exposes bounded-ledger state for health checks and
// tests. It does not expose response contents.
func (e *Engine) RequestReplayCounts() (unacked, pendingAcknowledgements int) {
	if e == nil {
		return 0, 0
	}
	e.mu.RLock()
	defer e.mu.RUnlock()
	return len(e.requestResults), len(e.pendingRequestAcks)
}

func cloneRequestResult(result RequestMutationResult) RequestMutationResult {
	result.Node = cloneNode(result.Node)
	return result
}

func (e *Engine) reserveRequestResultLocked() error {
	// A failed acknowledgement append must stop later mutations instead of
	// allowing the in-memory acknowledgement batch to grow without bound.
	if len(e.pendingRequestAcks) >= e.requestAckFlushThresholdLocked() {
		if err := e.persistAcknowledgedRequestsLocked(); err != nil {
			return err
		}
	}
	if len(e.requestResults) < e.requestReplayCapacity {
		return nil
	}
	return fmt.Errorf("%w: capacity %d", ErrRequestCapacity, e.requestReplayCapacity)
}

func (e *Engine) advanceRequestOrderHeadLocked() {
	for e.requestOrderHead < len(e.requestOrder) {
		key := e.requestOrder[e.requestOrderHead]
		if _, ok := e.requestResults[key]; key.Scope != "" && ok {
			return
		}
		e.requestOrderHead++
	}
}

func (e *Engine) compactRequestOrderLocked() {
	activeSpan := len(e.requestOrder) - e.requestOrderHead
	if e.requestOrderHead < RecoverableFUSEMaxBackground && activeSpan <= len(e.requestResults)*2+RecoverableFUSEMaxBackground {
		return
	}
	retained := e.requestOrder[:0]
	for _, key := range e.requestOrder[e.requestOrderHead:] {
		if _, ok := e.requestResults[key]; key.Scope != "" && ok {
			retained = append(retained, key)
		}
	}
	e.requestOrder = retained
	e.requestOrderHead = 0
}

func (e *Engine) storeRequestResultLocked(record walRecord) {
	key := requestLedgerKey{Scope: record.RequestScope, Unique: record.RequestUnique}
	result := requestResultFromRecord(record)
	e.requestResults[key] = requestReplayEntry{
		result:   result,
		timeUnix: record.TimeUnix,
	}
	e.requestOrder = append(e.requestOrder, key)
}

func compactRequestResultRecord(key requestLedgerKey, entry requestReplayEntry) walRecord {
	return walRecord{
		Op:                 requestResultWALOperation,
		TimeUnix:           entry.timeUnix,
		RequestScope:       key.Scope,
		RequestUnique:      key.Unique,
		RequestResultNode:  cloneNode(entry.result.Node),
		RequestResultInode: entry.result.Inode,
		RequestResultBytes: entry.result.BytesWritten,
	}
}

func requestResultFromRecord(record walRecord) RequestMutationResult {
	bytesWritten := record.RequestResultBytes
	if record.Op == "write" {
		bytesWritten = uint64(len(record.Data))
	}
	node := record.RequestResultNode
	if record.Op == "create" && node == nil {
		node = nodeFromCreateRecord(record)
	}
	return RequestMutationResult{
		Node:         cloneNode(node),
		Inode:        record.RequestResultInode,
		BytesWritten: bytesWritten,
	}
}

func (e *Engine) replayRequestResultLocked(record walRecord) error {
	if record.RequestScope == "" || record.RequestUnique == 0 {
		return fmt.Errorf("%w: incomplete request result at wal seq %d", ErrInvalidInput, record.Seq)
	}
	key := requestLedgerKey{Scope: record.RequestScope, Unique: record.RequestUnique}
	if _, ok := e.requestResults[key]; ok {
		return nil
	}
	if err := e.reserveRequestResultLocked(); err != nil {
		return err
	}
	e.storeRequestResultLocked(record)
	return nil
}

func (e *Engine) replayRequestAckLocked(record walRecord) error {
	keys := record.RequestAcks
	if len(keys) == 0 && record.RequestScope != "" && record.RequestUnique != 0 {
		keys = []requestLedgerKey{{Scope: record.RequestScope, Unique: record.RequestUnique}}
	}
	if len(keys) == 0 {
		return fmt.Errorf("%w: incomplete request acknowledgement", ErrInvalidInput)
	}
	for _, key := range keys {
		if key.Scope == "" || key.Unique == 0 {
			return fmt.Errorf("%w: incomplete request acknowledgement", ErrInvalidInput)
		}
		// Acknowledgements are idempotent so retrying an append after an
		// ambiguous filesystem error cannot make the WAL unrecoverable.
		delete(e.requestResults, key)
	}
	e.advanceRequestOrderHeadLocked()
	e.compactRequestOrderLocked()
	return nil
}

func (e *Engine) persistAcknowledgedRequestsLocked() error {
	if len(e.pendingRequestAcks) == 0 {
		return nil
	}
	keys := slices.Clone(e.pendingRequestAcks)
	record := walRecord{
		Op:          requestAckWALOperation,
		RequestAcks: keys,
		TimeUnix:    time.Now().UTC().UnixNano(),
	}
	payload, err := e.wal.prepare(record)
	if err != nil {
		return err
	}
	if err := e.wal.appendPrepared(payload); err != nil {
		return err
	}
	e.pendingRequestAcks = e.pendingRequestAcks[:0]
	return nil
}

func (e *Engine) requestAckFlushThresholdLocked() int {
	// Keep one max_background window free. After a process crash, durable
	// mutation records whose in-memory acknowledgements were not flushed plus
	// every genuinely in-flight kernel request still fit in the replay map.
	headroom := RecoverableFUSEMaxBackground
	if headroom >= e.requestReplayCapacity {
		headroom = e.requestReplayCapacity - 1
	}
	threshold := e.requestReplayCapacity - headroom
	if threshold < 1 {
		return 1
	}
	return threshold
}

func (e *Engine) unackedRequestRecordsLocked() []walRecord {
	records := make([]walRecord, 0, len(e.requestResults))
	for _, key := range e.requestOrder[e.requestOrderHead:] {
		entry, ok := e.requestResults[key]
		if !ok {
			continue
		}
		records = append(records, compactRequestResultRecord(key, entry))
	}
	return records
}

func (e *Engine) resetWALLocked() error {
	records := e.unackedRequestRecordsLocked()
	if err := e.wal.reset(records...); err != nil {
		return err
	}
	e.requestResults = make(map[requestLedgerKey]requestReplayEntry, len(records))
	e.requestOrder = e.requestOrder[:0]
	e.requestOrderHead = 0
	e.pendingRequestAcks = e.pendingRequestAcks[:0]
	for _, record := range records {
		e.storeRequestResultLocked(record)
	}
	return nil
}

func (e *Engine) prepareRequestRecordLocked(key requestLedgerKey, mutation RequestMutation) (walRecord, int64, error) {
	record := e.newRecord("")
	record.RequestScope = key.Scope
	record.RequestUnique = key.Unique
	projectedBytes := int64(0)

	switch mutation.Kind {
	case RequestCreate, RequestMkdir, RequestMknod, RequestSymlink:
		if mutation.Parent == 0 || mutation.Name == "" {
			return walRecord{}, 0, fmt.Errorf("%w: parent and name are required", ErrInvalidInput)
		}
		if err := e.ensureDirLocked(mutation.Parent); err != nil {
			return walRecord{}, 0, err
		}
		if _, exists := e.children[mutation.Parent][mutation.Name]; exists {
			return walRecord{}, 0, ErrExists
		}
		typ := mutation.Type
		if typ == "" {
			typ = TypeFile
		}
		if mutation.Kind == RequestSymlink && mutation.Target == "" {
			return walRecord{}, 0, fmt.Errorf("%w: symlink target is required", ErrInvalidInput)
		}
		record.Op = "create"
		record.Inode = e.nextInode
		record.Parent = mutation.Parent
		record.Name = mutation.Name
		record.Type = typ
		record.Mode = mutation.Mode
		record.UID = mutation.UID
		record.GID = mutation.GID
		record.Target = mutation.Target
	case RequestLink:
		node := e.nodes[mutation.Inode]
		if node == nil {
			return walRecord{}, 0, ErrNotFound
		}
		if node.Type == TypeDirectory {
			return walRecord{}, 0, ErrIsDir
		}
		if mutation.NewName == "" {
			return walRecord{}, 0, fmt.Errorf("%w: empty link name", ErrInvalidInput)
		}
		if err := e.ensureDirLocked(mutation.NewParent); err != nil {
			return walRecord{}, 0, err
		}
		if _, exists := e.children[mutation.NewParent][mutation.NewName]; exists {
			return walRecord{}, 0, ErrExists
		}
		record.Op = "link"
		record.Inode = mutation.Inode
		record.NewParent = mutation.NewParent
		record.NewName = mutation.NewName
		resultNode := cloneNode(node)
		resultNode.Nlink++
		resultNode.Ctime = time.Unix(0, record.TimeUnix).UTC()
		record.RequestResultNode = resultNode
	case RequestWrite:
		node, err := e.fileNodeLocked(mutation.Inode)
		if err != nil {
			return walRecord{}, 0, err
		}
		record.Op = "write"
		record.Inode = mutation.Inode
		record.Offset = mutation.Offset
		record.Data = mutation.Data
		end := mutation.Offset + uint64(len(mutation.Data))
		if end > node.Size {
			projectedBytes += int64(end - node.Size)
		}
	case RequestTruncate:
		node, err := e.fileNodeLocked(mutation.Inode)
		if err != nil {
			return walRecord{}, 0, err
		}
		record.Op = "truncate"
		record.Inode = mutation.Inode
		record.Offset = mutation.Offset
		record.RequestResultInode = mutation.Inode
		if mutation.Offset > node.Size {
			projectedBytes += int64(mutation.Offset - node.Size)
		}
	case RequestRename:
		if mutation.Name == "" || mutation.NewName == "" {
			return walRecord{}, 0, fmt.Errorf("%w: empty rename component", ErrInvalidInput)
		}
		if _, err := e.lookupLocked(mutation.Parent, mutation.Name); err != nil {
			return walRecord{}, 0, err
		}
		if err := e.ensureDirLocked(mutation.NewParent); err != nil {
			return walRecord{}, 0, err
		}
		record.Op = "rename"
		record.Parent = mutation.Parent
		record.Name = mutation.Name
		record.NewParent = mutation.NewParent
		record.NewName = mutation.NewName
	case RequestUnlink:
		inode, err := e.lookupLocked(mutation.Parent, mutation.Name)
		if err != nil {
			return walRecord{}, 0, err
		}
		if node := e.nodes[inode]; node != nil && node.Type == TypeDirectory {
			return walRecord{}, 0, ErrIsDir
		}
		record.Op = "unlink"
		record.Parent = mutation.Parent
		record.Name = mutation.Name
		record.RequestResultInode = inode
	case RequestRmdir:
		inode, err := e.lookupLocked(mutation.Parent, mutation.Name)
		if err != nil {
			return walRecord{}, 0, err
		}
		node := e.nodes[inode]
		if node == nil {
			return walRecord{}, 0, ErrNotFound
		}
		if node.Type != TypeDirectory {
			return walRecord{}, 0, ErrNotDir
		}
		if len(e.children[inode]) != 0 {
			return walRecord{}, 0, ErrNotEmpty
		}
		record.Op = "rmdir"
		record.Parent = mutation.Parent
		record.Name = mutation.Name
	case RequestSetAttr:
		node := e.nodes[mutation.Inode]
		if node == nil {
			return walRecord{}, 0, ErrNotFound
		}
		if mutation.SetAttrValid&requestSetAttrSize != 0 && node.Type != TypeFile {
			return walRecord{}, 0, ErrIsDir
		}
		record.Op = "setattr"
		record.Inode = mutation.Inode
		record.Mode = mutation.Mode
		record.UID = mutation.UID
		record.GID = mutation.GID
		record.Offset = mutation.Offset
		record.SetAttrValid = mutation.SetAttrValid
		resultNode := cloneNode(node)
		now := time.Unix(0, record.TimeUnix).UTC()
		if mutation.SetAttrValid&requestSetAttrMode != 0 {
			resultNode.Mode = mutation.Mode
			resultNode.Ctime = now
		}
		if mutation.SetAttrValid&(requestSetAttrUID|requestSetAttrGID) != 0 {
			if mutation.SetAttrValid&requestSetAttrUID != 0 {
				resultNode.UID = mutation.UID
			}
			if mutation.SetAttrValid&requestSetAttrGID != 0 {
				resultNode.GID = mutation.GID
			}
			resultNode.Ctime = now
		}
		if mutation.SetAttrValid&requestSetAttrSize != 0 {
			if mutation.Offset > node.Size {
				projectedBytes += int64(mutation.Offset - node.Size)
			}
			resultNode.Size = mutation.Offset
			resultNode.Mtime = now
			resultNode.Ctime = now
		}
		record.RequestResultNode = resultNode
	case RequestSetXattr:
		if e.nodes[mutation.Inode] == nil {
			return walRecord{}, 0, ErrNotFound
		}
		if mutation.Name == "" || mutation.Mode&^(xattrCreate|xattrReplace) != 0 || mutation.Mode == xattrCreate|xattrReplace {
			return walRecord{}, 0, fmt.Errorf("%w: invalid extended attribute request", ErrInvalidInput)
		}
		_, exists := e.xattrs[mutation.Inode][mutation.Name]
		if mutation.Mode&xattrCreate != 0 && exists {
			return walRecord{}, 0, ErrExists
		}
		if mutation.Mode&xattrReplace != 0 && !exists {
			return walRecord{}, 0, ErrXattrNotFound
		}
		record.Op = "setxattr"
		record.Inode = mutation.Inode
		record.Name = mutation.Name
		record.Data = mutation.Data
		record.Mode = mutation.Mode
	case RequestRemoveXattr:
		if e.nodes[mutation.Inode] == nil {
			return walRecord{}, 0, ErrNotFound
		}
		if mutation.Name == "" {
			return walRecord{}, 0, fmt.Errorf("%w: extended attribute name is required", ErrInvalidInput)
		}
		if _, exists := e.xattrs[mutation.Inode][mutation.Name]; !exists {
			return walRecord{}, 0, ErrXattrNotFound
		}
		record.Op = "removexattr"
		record.Inode = mutation.Inode
		record.Name = mutation.Name
	default:
		return walRecord{}, 0, fmt.Errorf("%w: unknown request mutation %q", ErrInvalidInput, mutation.Kind)
	}

	projectedBytes += estimatedWALRecordBytes(record)
	return record, projectedBytes, nil
}

func (e *Engine) applyRequestSetAttr(record walRecord) error {
	node := e.nodes[record.Inode]
	if node == nil {
		return ErrNotFound
	}
	now := time.Unix(0, record.TimeUnix).UTC()
	if record.SetAttrValid&requestSetAttrMode != 0 {
		node.Mode = record.Mode
		node.Ctime = now
	}
	if record.SetAttrValid&requestSetAttrUID != 0 {
		node.UID = record.UID
		node.Ctime = now
	}
	if record.SetAttrValid&requestSetAttrGID != 0 {
		node.GID = record.GID
		node.Ctime = now
	}
	if record.SetAttrValid&requestSetAttrSize != 0 {
		if err := e.applyTruncate(record); err != nil {
			return err
		}
	}
	return nil
}

func (e *Engine) applySetXattr(record walRecord) error {
	if e.nodes[record.Inode] == nil {
		return ErrNotFound
	}
	attrs := e.xattrs[record.Inode]
	if attrs == nil {
		attrs = make(map[string][]byte)
		e.xattrs[record.Inode] = attrs
	}
	attrs[record.Name] = slices.Clone(record.Data)
	e.nodes[record.Inode].Ctime = time.Unix(0, record.TimeUnix).UTC()
	return nil
}

func (e *Engine) applyRemoveXattr(record walRecord) error {
	if e.nodes[record.Inode] == nil {
		return ErrNotFound
	}
	delete(e.xattrs[record.Inode], record.Name)
	if len(e.xattrs[record.Inode]) == 0 {
		delete(e.xattrs, record.Inode)
	}
	e.nodes[record.Inode].Ctime = time.Unix(0, record.TimeUnix).UTC()
	return nil
}

// GetXattr returns a copy of one extended attribute value.
func (e *Engine) GetXattr(inode uint64, name string) ([]byte, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if err := e.checkOpen(); err != nil {
		return nil, err
	}
	if e.nodes[inode] == nil {
		return nil, ErrNotFound
	}
	value, ok := e.xattrs[inode][name]
	if !ok {
		return nil, ErrXattrNotFound
	}
	return slices.Clone(value), nil
}

// ListXattrs returns sorted names for stable FUSE listxattr replies.
func (e *Engine) ListXattrs(inode uint64) ([]string, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if err := e.checkOpen(); err != nil {
		return nil, err
	}
	if e.nodes[inode] == nil {
		return nil, ErrNotFound
	}
	names := make([]string, 0, len(e.xattrs[inode]))
	for name := range e.xattrs[inode] {
		names = append(names, name)
	}
	sort.Strings(names)
	return names, nil
}
