package portal

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"unsafe"

	"github.com/sandbox0-ai/sandbox0/pkg/volumefuse"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/fserror"
	pb "github.com/sandbox0-ai/sandbox0/storage-proxy/proto/fs"
	"golang.org/x/sys/unix"
	"google.golang.org/protobuf/proto"
)

const (
	rootFSResendLedgerFileName   = "resend.ledger"
	rootFSResendLedgerVersion    = 1
	rootFSResendLedgerHeaderSize = 4096
	rootFSResendLedgerCapacity   = 128
	rootFSResendScopeBytes       = 128
	rootFSResendErrorBytes       = 256
	rootFSResendInlineDataBytes  = 4096
)

var rootFSResendLedgerMagic = [8]byte{'S', '0', 'R', 'F', 'S', 'R', 'E', '1'}

type rootFSMutationOperation uint16

const (
	rootFSMutationSetAttr rootFSMutationOperation = iota + 1
	rootFSMutationMkdir
	rootFSMutationCreate
	rootFSMutationUnlink
	rootFSMutationRmdir
	rootFSMutationRename
	rootFSMutationLink
	rootFSMutationSymlink
	rootFSMutationWrite
	rootFSMutationFallocate
	rootFSMutationSetXattr
	rootFSMutationRemoveXattr
	rootFSMutationMknod
	rootFSMutationRelease
	rootFSMutationReleaseDir
)

type rootFSResendState uint32

const (
	rootFSResendEmpty rootFSResendState = iota
	rootFSResendIntent
	rootFSResendApplied
	rootFSResendAcked
)

const (
	rootFSResendDataNone uint32 = iota
	rootFSResendDataInline
	rootFSResendDataHash
)

type rootFSResendErrorKind uint16

const (
	rootFSResendNoError rootFSResendErrorKind = iota
	rootFSResendFSError
	rootFSResendErrno
	rootFSResendGenericError
)

const (
	rootFSObjectExists uint32 = 1 << iota
	rootFSObjectValueExists
)

// rootFSObjectState is deliberately pointer-free so it can be copied into a
// fixed mmap record before a host filesystem mutation starts.
type rootFSObjectState struct {
	Flags       uint32
	Mode        uint32
	AttrMode    uint32
	UID         uint32
	GID         uint32
	Reserved    uint32
	Device      uint64
	Inode       uint64
	Rdev        uint64
	Size        int64
	AtimeSec    int64
	AtimeNsec   int64
	MtimeSec    int64
	MtimeNsec   int64
	ValueLength int64
	ValueHash   rootFSDataHash
}

func (s rootFSObjectState) exists() bool {
	return s.Flags&rootFSObjectExists != 0
}

func (s rootFSObjectState) valueExists() bool {
	return s.Flags&rootFSObjectValueExists != 0
}

func (s rootFSObjectState) identity() rootFSHostIdentity {
	return rootFSHostIdentity{Device: s.Device, Inode: s.Inode, Mode: s.Mode}
}

func (s rootFSObjectState) sameIdentity(other rootFSObjectState) bool {
	return s.exists() == other.exists() && (!s.exists() || s.identity() == other.identity())
}

// rootFSMutationGuard contains only reconciliation facts. The request itself
// is resent by the kernel and is protected by Fingerprint in the ledger.
type rootFSMutationGuard struct {
	BeforePrimary   rootFSObjectState
	BeforeSecondary rootFSObjectState
	AfterPrimary    rootFSObjectState
	AfterSecondary  rootFSObjectState
	Offset          int64
	Length          int64
	DataHash        rootFSDataHash
	Valid           uint32
	Mode            uint32
	UID             uint32
	GID             uint32
	AtimeSec        int64
	AtimeNsec       int64
	MtimeSec        int64
	MtimeNsec       int64
}

type rootFSResendLedgerHeader struct {
	Magic      [8]byte
	Version    uint32
	Capacity   uint32
	RecordSize uint32
	Reserved   uint32
}

// rootFSResendRecord has no Go pointers. State and GuardReady are published
// last with atomic stores so a replacement process never observes a partially
// initialized intent or result.
type rootFSResendRecord struct {
	State        uint32
	GuardReady   uint32
	Operation    uint16
	ScopeLength  uint16
	ErrorKind    uint16
	Reserved     uint16
	ErrorCode    int32
	ErrorLength  uint32
	DataKind     uint32
	DataLength   uint32
	Unique       uint64
	Fingerprint  rootFSRequestFingerprint
	Scope        [rootFSResendScopeBytes]byte
	Guard        rootFSMutationGuard
	Result       rootFSResendResult
	DataHash     rootFSDataHash
	InlineData   [rootFSResendInlineDataBytes]byte
	ErrorMessage [rootFSResendErrorBytes]byte
}

type rootFSRequestFingerprint [16]byte
type rootFSDataHash [16]byte

type rootFSStoredAttr struct {
	Ino       uint64
	Mode      uint32
	Nlink     uint32
	UID       uint32
	GID       uint32
	Rdev      uint64
	Size      uint64
	Blocks    uint64
	AtimeSec  int64
	AtimeNsec int64
	MtimeSec  int64
	MtimeNsec int64
	CtimeSec  int64
	CtimeNsec int64
}

type rootFSResendResult struct {
	Inode        uint64
	Generation   uint64
	HandleID     uint64
	BytesWritten int64
	Attr         rootFSStoredAttr
}

type rootFSResendLedger struct {
	mu      sync.Mutex
	file    *os.File
	mapping []byte
	records []rootFSResendRecord
	tokens  [rootFSResendLedgerCapacity]rootFSCompletionToken
	next    int
	prefer  atomic.Uint32
	closed  atomic.Bool
	ackRefs atomic.Int64
}

type rootFSCompletionToken struct {
	ledger *rootFSResendLedger
	slot   int
}

func (t *rootFSCompletionToken) RequestAcknowledged() {
	if t != nil && t.ledger != nil {
		t.ledger.acknowledgeSlot(t.slot)
	}
}

type rootFSResendTransaction struct {
	ledger      *rootFSResendLedger
	slot        int
	identity    volumefuse.RequestIdentity
	operation   rootFSMutationOperation
	fingerprint rootFSRequestFingerprint
	state       rootFSResendState
}

func openRootFSResendLedger(path string) (*rootFSResendLedger, error) {
	recordSize := int(unsafe.Sizeof(rootFSResendRecord{}))
	if recordSize <= 0 {
		return nil, errors.New("rootfs resend ledger has an invalid record size")
	}
	fileSize := rootFSResendLedgerHeaderSize + rootFSResendLedgerCapacity*recordSize
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, err
	}
	closeOnError := func(openErr error) (*rootFSResendLedger, error) {
		_ = file.Close()
		return nil, openErr
	}
	if err := file.Chmod(0o600); err != nil {
		return closeOnError(err)
	}
	info, err := file.Stat()
	if err != nil {
		return closeOnError(err)
	}
	newFile := info.Size() == 0
	if newFile {
		if err := file.Truncate(int64(fileSize)); err != nil {
			return closeOnError(err)
		}
	} else if info.Size() != int64(fileSize) {
		return closeOnError(fmt.Errorf("rootfs resend ledger size is %d, want %d", info.Size(), fileSize))
	}
	mapping, err := unix.Mmap(int(file.Fd()), 0, fileSize, unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
	if err != nil {
		return closeOnError(err)
	}
	header := (*rootFSResendLedgerHeader)(unsafe.Pointer(&mapping[0]))
	if newFile {
		*header = rootFSResendLedgerHeader{
			Magic:      rootFSResendLedgerMagic,
			Version:    rootFSResendLedgerVersion,
			Capacity:   rootFSResendLedgerCapacity,
			RecordSize: uint32(recordSize),
		}
		if err := unix.Msync(mapping[:rootFSResendLedgerHeaderSize], unix.MS_SYNC); err != nil {
			_ = unix.Munmap(mapping)
			return closeOnError(err)
		}
		if err := file.Sync(); err != nil {
			_ = unix.Munmap(mapping)
			return closeOnError(err)
		}
	} else if header.Magic != rootFSResendLedgerMagic ||
		header.Version != rootFSResendLedgerVersion ||
		header.Capacity != rootFSResendLedgerCapacity ||
		header.RecordSize != uint32(recordSize) {
		_ = unix.Munmap(mapping)
		return closeOnError(errors.New("rootfs resend ledger header is incompatible"))
	}
	records := unsafe.Slice((*rootFSResendRecord)(unsafe.Pointer(&mapping[rootFSResendLedgerHeaderSize])), rootFSResendLedgerCapacity)
	for i := range records {
		state := rootFSResendState(atomic.LoadUint32(&records[i].State))
		if state > rootFSResendAcked {
			_ = unix.Munmap(mapping)
			return closeOnError(fmt.Errorf("rootfs resend ledger slot %d has invalid state %d", i, state))
		}
		if state == rootFSResendEmpty {
			continue
		}
		if records[i].ScopeLength == 0 || int(records[i].ScopeLength) > len(records[i].Scope) || records[i].Unique == 0 {
			_ = unix.Munmap(mapping)
			return closeOnError(fmt.Errorf("rootfs resend ledger slot %d has an invalid identity", i))
		}
		if records[i].Operation < uint16(rootFSMutationSetAttr) || records[i].Operation > uint16(rootFSMutationReleaseDir) {
			_ = unix.Munmap(mapping)
			return closeOnError(fmt.Errorf("rootfs resend ledger slot %d has an invalid operation", i))
		}
		if state >= rootFSResendApplied && (atomic.LoadUint32(&records[i].GuardReady) == 0 ||
			records[i].ErrorLength > rootFSResendErrorBytes ||
			rootFSResendErrorKind(records[i].ErrorKind) > rootFSResendGenericError) {
			_ = unix.Munmap(mapping)
			return closeOnError(fmt.Errorf("rootfs resend ledger slot %d has an invalid response", i))
		}
		if records[i].DataKind > rootFSResendDataHash ||
			records[i].DataKind == rootFSResendDataInline && records[i].DataLength > rootFSResendInlineDataBytes {
			_ = unix.Munmap(mapping)
			return closeOnError(fmt.Errorf("rootfs resend ledger slot %d has invalid request data", i))
		}
	}
	ledger := &rootFSResendLedger{file: file, mapping: mapping, records: records}
	for i := range ledger.tokens {
		ledger.tokens[i] = rootFSCompletionToken{ledger: ledger, slot: i}
	}
	return ledger, nil
}

func (l *rootFSResendLedger) Close() error {
	if l == nil {
		return nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed.Load() {
		return nil
	}
	l.closed.Store(true)
	for l.ackRefs.Load() != 0 {
		runtime.Gosched()
	}
	var errs []error
	if l.mapping != nil {
		errs = append(errs, unix.Munmap(l.mapping))
	}
	if l.file != nil {
		errs = append(errs, l.file.Close())
	}
	l.mapping = nil
	l.records = nil
	return errors.Join(errs...)
}

func (l *rootFSResendLedger) begin(identity volumefuse.RequestIdentity, operation rootFSMutationOperation, fingerprint rootFSRequestFingerprint) (rootFSResendTransaction, error) {
	if l == nil {
		return rootFSResendTransaction{}, fserror.New(fserror.FailedPrecondition, "rootfs resend ledger is unavailable")
	}
	if len(identity.Scope) == 0 || len(identity.Scope) > rootFSResendScopeBytes || identity.Unique == 0 {
		return rootFSResendTransaction{}, fserror.New(fserror.FailedPrecondition, "rootfs request identity is invalid")
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed.Load() {
		return rootFSResendTransaction{}, fserror.New(fserror.FailedPrecondition, "rootfs resend ledger is closed")
	}
	if identity.Resend {
		matching := -1
		for i := range l.records {
			record := &l.records[i]
			if rootFSResendState(atomic.LoadUint32(&record.State)) == rootFSResendEmpty || record.Unique != identity.Unique {
				continue
			}
			if int(record.ScopeLength) == len(identity.Scope) && bytes.Equal(record.Scope[:record.ScopeLength], []byte(identity.Scope)) {
				matching = i
				break
			}
		}
		if matching < 0 {
			return rootFSResendTransaction{}, fserror.New(fserror.FailedPrecondition, "resent rootfs mutation has no durable ledger record")
		}
		record := &l.records[matching]
		if rootFSMutationOperation(record.Operation) != operation || record.Fingerprint != fingerprint {
			return rootFSResendTransaction{}, fserror.New(fserror.FailedPrecondition, "resent rootfs mutation does not match its durable intent")
		}
		return rootFSResendTransaction{
			ledger: l, slot: matching, identity: identity, operation: operation,
			fingerprint: fingerprint, state: rootFSResendState(atomic.LoadUint32(&record.State)),
		}, nil
	}
	start := l.next
	if preferred := l.prefer.Swap(0); preferred != 0 {
		start = int(preferred - 1)
	}
	for offset := 0; offset < len(l.records); offset++ {
		i := (start + offset) % len(l.records)
		record := &l.records[i]
		state := rootFSResendState(atomic.LoadUint32(&record.State))
		if state != rootFSResendEmpty && state != rootFSResendAcked {
			continue
		}
		atomic.StoreUint32(&record.State, uint32(rootFSResendEmpty))
		atomic.StoreUint32(&record.GuardReady, 0)
		record.Operation = uint16(operation)
		record.ScopeLength = uint16(len(identity.Scope))
		record.ErrorKind = 0
		record.ErrorCode = 0
		record.ErrorLength = 0
		record.DataKind = rootFSResendDataNone
		record.DataLength = 0
		record.Unique = identity.Unique
		record.Fingerprint = fingerprint
		copy(record.Scope[:], identity.Scope)
		atomic.StoreUint32(&record.State, uint32(rootFSResendIntent))
		l.next = (i + 1) % len(l.records)
		return rootFSResendTransaction{
			ledger: l, slot: i, identity: identity, operation: operation,
			fingerprint: fingerprint, state: rootFSResendIntent,
		}, nil
	}
	return rootFSResendTransaction{}, fserror.New(fserror.ResourceExhausted, "rootfs resend ledger has no acknowledged slot available")
}

func (t *rootFSResendTransaction) storeRequestData(data []byte) error {
	record, err := t.lockedRecord()
	if err != nil {
		return err
	}
	record.DataLength = uint32(len(data))
	if len(data) <= len(record.InlineData) {
		copy(record.InlineData[:], data)
		record.DataKind = rootFSResendDataInline
		return nil
	}
	record.DataHash = rootFSHashData(data)
	record.DataKind = rootFSResendDataHash
	return nil
}

func (t *rootFSResendTransaction) requestDataMatches(data []byte) bool {
	if t == nil || t.ledger == nil || t.slot < 0 || t.slot >= len(t.ledger.records) {
		return false
	}
	record := &t.ledger.records[t.slot]
	if uint32(len(data)) != record.DataLength {
		return false
	}
	switch record.DataKind {
	case rootFSResendDataInline:
		return bytes.Equal(record.InlineData[:len(data)], data)
	case rootFSResendDataHash:
		return rootFSHashData(data) == record.DataHash
	default:
		return false
	}
}

func (t *rootFSResendTransaction) guardReady() bool {
	if t == nil || t.ledger == nil {
		return false
	}
	if t.ledger.closed.Load() || t.slot < 0 || t.slot >= len(t.ledger.records) {
		return false
	}
	record := &t.ledger.records[t.slot]
	return atomic.LoadUint32(&record.GuardReady) != 0
}

func (t *rootFSResendTransaction) loadGuard() rootFSMutationGuard {
	if t == nil || t.ledger == nil || t.slot < 0 || t.slot >= len(t.ledger.records) {
		return rootFSMutationGuard{}
	}
	return t.ledger.records[t.slot].Guard
}

func (t *rootFSResendTransaction) storeGuard(guard rootFSMutationGuard) error {
	if t == nil || t.ledger == nil {
		return fserror.New(fserror.FailedPrecondition, "rootfs resend transaction is unavailable")
	}
	record, err := t.lockedRecord()
	if err != nil {
		return err
	}
	storeRootFSMutationGuard(&record.Guard, &guard, t.operation, false)
	atomic.StoreUint32(&record.GuardReady, 1)
	return nil
}

func (t *rootFSResendTransaction) commit(guard rootFSMutationGuard, response any, responseErr error) error {
	if t == nil || t.ledger == nil {
		return fserror.New(fserror.FailedPrecondition, "rootfs resend transaction is unavailable")
	}
	record, err := t.lockedRecord()
	if err != nil {
		return err
	}
	storeRootFSMutationGuard(&record.Guard, &guard, t.operation, true)
	return t.commitResult(response, responseErr)
}

func (t *rootFSResendTransaction) commitResult(response any, responseErr error) error {
	if t == nil || t.ledger == nil {
		return fserror.New(fserror.FailedPrecondition, "rootfs resend transaction is unavailable")
	}
	errKind, errCode, errMessage := encodeRootFSResendError(responseErr)
	if len(errMessage) > rootFSResendErrorBytes {
		errMessage = errMessage[:rootFSResendErrorBytes]
	}
	record, err := t.lockedRecord()
	if err != nil {
		return err
	}
	record.Result = rootFSResultFromResponse(response)
	record.ErrorKind = uint16(errKind)
	record.ErrorCode = errCode
	record.ErrorLength = uint32(len(errMessage))
	copy(record.ErrorMessage[:], errMessage)
	atomic.StoreUint32(&record.GuardReady, 1)
	atomic.StoreUint32(&record.State, uint32(rootFSResendApplied))
	t.state = rootFSResendApplied
	return nil
}

func (t *rootFSResendTransaction) storeWriteGuard(object rootFSObjectState, offset, length int64) error {
	record, err := t.lockedRecord()
	if err != nil {
		return err
	}
	record.Guard.BeforePrimary = object
	record.Guard.Offset = offset
	record.Guard.Length = length
	atomic.StoreUint32(&record.GuardReady, 1)
	return nil
}

func (t *rootFSResendTransaction) loadWriteGuard() (rootFSObjectState, int64, int64) {
	if t == nil || t.ledger == nil || t.slot < 0 || t.slot >= len(t.ledger.records) {
		return rootFSObjectState{}, 0, 0
	}
	guard := &t.ledger.records[t.slot].Guard
	return guard.BeforePrimary, guard.Offset, guard.Length
}

func storeRootFSMutationGuard(destination, source *rootFSMutationGuard, operation rootFSMutationOperation, after bool) {
	if destination == nil || source == nil {
		return
	}
	if after {
		switch operation {
		case rootFSMutationMkdir, rootFSMutationCreate, rootFSMutationLink, rootFSMutationSymlink, rootFSMutationMknod:
			destination.AfterPrimary = source.AfterPrimary
		case rootFSMutationUnlink, rootFSMutationRmdir, rootFSMutationRename:
			destination.AfterPrimary = source.AfterPrimary
			destination.AfterSecondary = source.AfterSecondary
		case rootFSMutationRelease, rootFSMutationReleaseDir:
			destination.AfterSecondary = source.AfterSecondary
		}
		return
	}
	destination.BeforePrimary = source.BeforePrimary
	switch operation {
	case rootFSMutationRename, rootFSMutationLink, rootFSMutationSetXattr, rootFSMutationRemoveXattr,
		rootFSMutationRelease, rootFSMutationReleaseDir:
		destination.BeforeSecondary = source.BeforeSecondary
	}
	switch operation {
	case rootFSMutationFallocate, rootFSMutationSymlink, rootFSMutationSetXattr, rootFSMutationMknod:
		destination.Offset = source.Offset
		destination.Length = source.Length
		destination.DataHash = source.DataHash
		destination.Valid = source.Valid
		destination.Mode = source.Mode
	case rootFSMutationSetAttr:
		destination.Length = source.Length
		destination.Valid = source.Valid
		destination.Mode = source.Mode
		destination.UID = source.UID
		destination.GID = source.GID
		destination.AtimeSec = source.AtimeSec
		destination.AtimeNsec = source.AtimeNsec
		destination.MtimeSec = source.MtimeSec
		destination.MtimeNsec = source.MtimeNsec
	}
}

func (t *rootFSResendTransaction) result(response any) error {
	if t == nil || t.ledger == nil {
		return fserror.New(fserror.FailedPrecondition, "rootfs resend transaction is unavailable")
	}
	record, err := t.lockedRecord()
	if err != nil {
		return err
	}
	state := rootFSResendState(atomic.LoadUint32(&record.State))
	if state != rootFSResendApplied && state != rootFSResendAcked {
		return fserror.New(fserror.FailedPrecondition, "rootfs resend transaction has no committed result")
	}
	if record.ErrorLength > rootFSResendErrorBytes ||
		rootFSResendErrorKind(record.ErrorKind) > rootFSResendGenericError {
		return fserror.New(fserror.FailedPrecondition, "rootfs resend transaction result is corrupt")
	}
	populateRootFSResponse(response, record.Result)
	return decodeRootFSResendError(rootFSResendErrorKind(record.ErrorKind), record.ErrorCode, string(record.ErrorMessage[:record.ErrorLength]))
}

func (t *rootFSResendTransaction) completionToken() volumefuse.RequestCompletionToken {
	if t == nil || t.ledger == nil {
		return nil
	}
	return &t.ledger.tokens[t.slot]
}

func (l *rootFSResendLedger) acknowledgeSlot(slot int) {
	if l == nil {
		return
	}
	l.ackRefs.Add(1)
	defer l.ackRefs.Add(-1)
	if l.closed.Load() || slot < 0 || slot >= len(l.records) {
		return
	}
	record := &l.records[slot]
	if rootFSResendState(atomic.LoadUint32(&record.State)) == rootFSResendApplied {
		if atomic.CompareAndSwapUint32(&record.State, uint32(rootFSResendApplied), uint32(rootFSResendAcked)) {
			l.prefer.Store(uint32(slot + 1))
		}
	}
}

func (t *rootFSResendTransaction) lockedRecord() (*rootFSResendRecord, error) {
	if t.ledger.closed.Load() || t.slot < 0 || t.slot >= len(t.ledger.records) {
		return nil, fserror.New(fserror.FailedPrecondition, "rootfs resend transaction ledger is closed")
	}
	record := &t.ledger.records[t.slot]
	if record.Unique != t.identity.Unique || rootFSMutationOperation(record.Operation) != t.operation || record.Fingerprint != t.fingerprint ||
		int(record.ScopeLength) != len(t.identity.Scope) || !bytes.Equal(record.Scope[:record.ScopeLength], []byte(t.identity.Scope)) {
		return nil, fserror.New(fserror.FailedPrecondition, "rootfs resend transaction slot was replaced")
	}
	return record, nil
}

func encodeRootFSResendError(err error) (rootFSResendErrorKind, int32, string) {
	if err == nil {
		return rootFSResendNoError, 0, ""
	}
	if fsErr, ok := fserror.FromError(err); ok {
		return rootFSResendFSError, int32(fsErr.Code()), fsErr.Message()
	}
	var errno syscall.Errno
	if errors.As(err, &errno) {
		return rootFSResendErrno, int32(errno), err.Error()
	}
	return rootFSResendGenericError, 0, err.Error()
}

func decodeRootFSResendError(kind rootFSResendErrorKind, code int32, message string) error {
	switch kind {
	case rootFSResendNoError:
		return nil
	case rootFSResendFSError:
		return fserror.New(fserror.Code(code), message)
	case rootFSResendErrno:
		return syscall.Errno(code)
	case rootFSResendGenericError:
		return errors.New(message)
	default:
		return fserror.New(fserror.FailedPrecondition, "rootfs resend ledger contains an invalid error kind")
	}
}

func newRootFSResponse(operation rootFSMutationOperation) proto.Message {
	switch operation {
	case rootFSMutationSetAttr:
		return &pb.SetAttrResponse{}
	case rootFSMutationMkdir, rootFSMutationCreate, rootFSMutationLink, rootFSMutationSymlink, rootFSMutationMknod:
		return &pb.NodeResponse{}
	case rootFSMutationUnlink, rootFSMutationRmdir, rootFSMutationRename, rootFSMutationFallocate,
		rootFSMutationSetXattr, rootFSMutationRemoveXattr, rootFSMutationRelease, rootFSMutationReleaseDir:
		return &pb.Empty{}
	default:
		return nil
	}
}

func rootFSResultFromResponse(response any) rootFSResendResult {
	var result rootFSResendResult
	switch typed := response.(type) {
	case *pb.NodeResponse:
		if typed != nil {
			result.Inode = typed.GetInode()
			result.Generation = typed.GetGeneration()
			result.HandleID = typed.GetHandleId()
			result.Attr = storeRootFSAttr(typed.GetAttr())
		}
	case *pb.SetAttrResponse:
		if typed != nil {
			result.Attr = storeRootFSAttr(typed.GetAttr())
		}
	case *pb.WriteResponse:
		if typed != nil {
			result.BytesWritten = typed.GetBytesWritten()
		}
	}
	return result
}

func populateRootFSResponse(response any, result rootFSResendResult) {
	switch typed := response.(type) {
	case *pb.NodeResponse:
		typed.Inode = result.Inode
		typed.Generation = result.Generation
		typed.HandleId = result.HandleID
		typed.Attr = loadRootFSAttr(result.Attr)
	case *pb.SetAttrResponse:
		typed.Attr = loadRootFSAttr(result.Attr)
	case *pb.WriteResponse:
		typed.BytesWritten = result.BytesWritten
	}
}

func storeRootFSAttr(attr *pb.GetAttrResponse) rootFSStoredAttr {
	if attr == nil {
		return rootFSStoredAttr{}
	}
	return rootFSStoredAttr{
		Ino: attr.GetIno(), Mode: attr.GetMode(), Nlink: attr.GetNlink(), UID: attr.GetUid(), GID: attr.GetGid(),
		Rdev: attr.GetRdev(), Size: attr.GetSize(), Blocks: attr.GetBlocks(),
		AtimeSec: attr.GetAtimeSec(), AtimeNsec: attr.GetAtimeNsec(), MtimeSec: attr.GetMtimeSec(), MtimeNsec: attr.GetMtimeNsec(),
		CtimeSec: attr.GetCtimeSec(), CtimeNsec: attr.GetCtimeNsec(),
	}
}

func loadRootFSAttr(attr rootFSStoredAttr) *pb.GetAttrResponse {
	return &pb.GetAttrResponse{
		Ino: attr.Ino, Mode: attr.Mode, Nlink: attr.Nlink, Uid: attr.UID, Gid: attr.GID,
		Rdev: attr.Rdev, Size: attr.Size, Blocks: attr.Blocks,
		AtimeSec: attr.AtimeSec, AtimeNsec: attr.AtimeNsec, MtimeSec: attr.MtimeSec, MtimeNsec: attr.MtimeNsec,
		CtimeSec: attr.CtimeSec, CtimeNsec: attr.CtimeNsec,
	}
}
