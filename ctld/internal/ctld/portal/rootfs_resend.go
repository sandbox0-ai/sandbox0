package portal

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math/bits"
	"os"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/sandbox0-ai/sandbox0/pkg/volumefuse"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/fserror"
	pb "github.com/sandbox0-ai/sandbox0/storage-proxy/proto/fs"
	"golang.org/x/sys/unix"
	"google.golang.org/protobuf/proto"
)

type rootFSIntentDisposition uint8

const (
	rootFSIntentRetry rootFSIntentDisposition = iota
	rootFSIntentApplied
)

type rootFSFingerprintBuilder struct {
	left  uint64
	right uint64
	count uint64
}

func newRootFSFingerprintBuilder(operation rootFSMutationOperation) rootFSFingerprintBuilder {
	builder := rootFSFingerprintBuilder{left: 14695981039346656037, right: 7809847782465536322}
	builder.uint64(uint64(operation))
	return builder
}

func (b *rootFSFingerprintBuilder) uint64(value uint64) {
	b.count++
	mixed := value + 0x9e3779b97f4a7c15*b.count
	mixed = (mixed ^ (mixed >> 30)) * 0xbf58476d1ce4e5b9
	mixed = (mixed ^ (mixed >> 27)) * 0x94d049bb133111eb
	mixed ^= mixed >> 31
	b.left = bits.RotateLeft64(b.left^mixed, 27)*0x3c79ac492ba7b653 + 0x1c69b3f74ac4ae35
	b.right = bits.RotateLeft64(b.right+mixed, 31)*0x9ddfea08eb382d69 + value
}

func (b *rootFSFingerprintBuilder) string(value string) {
	b.uint64(uint64(len(value)))
	for len(value) >= 8 {
		b.uint64(binary.LittleEndian.Uint64([]byte(value[:8])))
		value = value[8:]
	}
	if len(value) != 0 {
		var tail uint64
		for i := range len(value) {
			tail |= uint64(value[i]) << (8 * i)
		}
		b.uint64(tail)
	}
}

func (b *rootFSFingerprintBuilder) actor(actor *pb.PosixActor) {
	if actor == nil {
		b.uint64(0)
		return
	}
	b.uint64(uint64(actor.GetPid()) + 1)
	b.uint64(uint64(actor.GetUid()))
	b.uint64(uint64(len(actor.GetGids())))
	for _, gid := range actor.GetGids() {
		b.uint64(uint64(gid))
	}
}

func (b *rootFSFingerprintBuilder) sum() rootFSRequestFingerprint {
	var result rootFSRequestFingerprint
	binary.LittleEndian.PutUint64(result[:8], b.left)
	binary.LittleEndian.PutUint64(result[8:], b.right)
	return result
}

func rootFSWriteFingerprint(request *pb.WriteRequest) rootFSRequestFingerprint {
	left := rootFSHashMix(
		request.GetInode()^uint64(request.GetOffset()),
		request.GetHandleId()^uint64(len(request.GetData()))^0xa0761d6478bd642f,
	)
	right := rootFSHashMix(
		bits.RotateLeft64(request.GetInode(), 29)^request.GetHandleId(),
		bits.RotateLeft64(uint64(request.GetOffset()), 17)^uint64(len(request.GetData()))^0xe7037ed1a0b428db,
	)
	var result rootFSRequestFingerprint
	binary.LittleEndian.PutUint64(result[:8], left)
	binary.LittleEndian.PutUint64(result[8:], right)
	return result
}

func rootFSMutationFingerprint(operation rootFSMutationOperation, request proto.Message) (rootFSRequestFingerprint, error) {
	builder := newRootFSFingerprintBuilder(operation)
	switch req := request.(type) {
	case *pb.SetAttrRequest:
		builder.string(req.GetVolumeId())
		builder.uint64(req.GetInode())
		builder.uint64(uint64(req.GetValid()))
		builder.uint64(req.GetHandleId())
		attr := req.GetAttr()
		builder.uint64(attr.GetIno())
		builder.uint64(uint64(attr.GetMode()))
		builder.uint64(uint64(attr.GetUid()))
		builder.uint64(uint64(attr.GetGid()))
		builder.uint64(attr.GetSize())
		builder.uint64(uint64(attr.GetAtimeSec()))
		builder.uint64(uint64(attr.GetAtimeNsec()))
		builder.uint64(uint64(attr.GetMtimeSec()))
		builder.uint64(uint64(attr.GetMtimeNsec()))
		builder.actor(req.GetActor())
	case *pb.MkdirRequest:
		builder.string(req.GetVolumeId())
		builder.uint64(req.GetParent())
		builder.string(req.GetName())
		builder.uint64(uint64(req.GetMode()))
		builder.uint64(uint64(req.GetUmask()))
		builder.actor(req.GetActor())
	case *pb.CreateRequest:
		builder.string(req.GetVolumeId())
		builder.uint64(req.GetParent())
		builder.string(req.GetName())
		builder.uint64(uint64(req.GetMode()))
		builder.uint64(uint64(req.GetFlags()))
		builder.uint64(uint64(req.GetUmask()))
		builder.actor(req.GetActor())
	case *pb.UnlinkRequest:
		builder.string(req.GetVolumeId())
		builder.uint64(req.GetParent())
		builder.string(req.GetName())
		builder.actor(req.GetActor())
	case *pb.RmdirRequest:
		builder.string(req.GetVolumeId())
		builder.uint64(req.GetParent())
		builder.string(req.GetName())
		builder.actor(req.GetActor())
	case *pb.RenameRequest:
		builder.string(req.GetVolumeId())
		builder.uint64(req.GetOldParent())
		builder.string(req.GetOldName())
		builder.uint64(req.GetNewParent())
		builder.string(req.GetNewName())
		builder.uint64(uint64(req.GetFlags()))
		builder.actor(req.GetActor())
	case *pb.LinkRequest:
		builder.string(req.GetVolumeId())
		builder.uint64(req.GetInode())
		builder.uint64(req.GetNewParent())
		builder.string(req.GetNewName())
		builder.actor(req.GetActor())
	case *pb.SymlinkRequest:
		builder.string(req.GetVolumeId())
		builder.uint64(req.GetParent())
		builder.string(req.GetName())
		builder.uint64(uint64(len(req.GetTarget())))
		builder.actor(req.GetActor())
	case *pb.FallocateRequest:
		builder.string(req.GetVolumeId())
		builder.uint64(req.GetInode())
		builder.uint64(uint64(req.GetMode()))
		builder.uint64(uint64(req.GetOffset()))
		builder.uint64(uint64(req.GetLength()))
		builder.uint64(req.GetHandleId())
		builder.actor(req.GetActor())
	case *pb.SetXattrRequest:
		builder.string(req.GetVolumeId())
		builder.uint64(req.GetInode())
		builder.string(req.GetName())
		builder.uint64(uint64(req.GetFlags()))
		builder.uint64(uint64(len(req.GetValue())))
		builder.actor(req.GetActor())
	case *pb.RemoveXattrRequest:
		builder.string(req.GetVolumeId())
		builder.uint64(req.GetInode())
		builder.string(req.GetName())
		builder.actor(req.GetActor())
	case *pb.MknodRequest:
		builder.string(req.GetVolumeId())
		builder.uint64(req.GetParent())
		builder.string(req.GetName())
		builder.uint64(uint64(req.GetMode()))
		builder.uint64(uint64(req.GetRdev()))
		builder.uint64(uint64(req.GetUmask()))
		builder.actor(req.GetActor())
	case *pb.ReleaseRequest:
		builder.string(req.GetVolumeId())
		builder.uint64(req.GetInode())
		builder.uint64(req.GetHandleId())
		builder.actor(req.GetActor())
	case *pb.ReleaseDirRequest:
		builder.string(req.GetVolumeId())
		builder.uint64(req.GetInode())
		builder.uint64(req.GetHandleId())
		builder.actor(req.GetActor())
	default:
		return rootFSRequestFingerprint{}, fserror.New(fserror.Unimplemented, "rootfs mutation fingerprint is not implemented")
	}
	return builder.sum(), nil
}

func rootFSHashData(data []byte) rootFSDataHash {
	left := uint64(len(data)) ^ 0xa0761d6478bd642f
	right := uint64(len(data)) ^ 0xe7037ed1a0b428db
	for len(data) >= 16 {
		first := binary.LittleEndian.Uint64(data[:8])
		second := binary.LittleEndian.Uint64(data[8:16])
		left = rootFSHashMix(left^first, 0x8ebc6af09c88c6e3^second)
		right = rootFSHashMix(right^second, 0x589965cc75374cc3^first)
		data = data[16:]
	}
	var firstTail, secondTail uint64
	for i, value := range data {
		if i < 8 {
			firstTail |= uint64(value) << (8 * i)
		} else {
			secondTail |= uint64(value) << (8 * (i - 8))
		}
	}
	left = rootFSHashMix(left^firstTail, 0x1d8e4e27c47d124f^uint64(len(data)))
	right = rootFSHashMix(right^secondTail, 0xeb44accab455d165^uint64(len(data)))
	left ^= rootFSHashMix(right, 0x9e3779b97f4a7c15)
	right ^= rootFSHashMix(left, 0xd6e8feb86659fd93)
	var result rootFSDataHash
	binary.LittleEndian.PutUint64(result[:8], left)
	binary.LittleEndian.PutUint64(result[8:], right)
	return result
}

func rootFSHashMix(left, right uint64) uint64 {
	high, low := bits.Mul64(left, right)
	return high ^ low
}

func rootFSMutationRequestData(operation rootFSMutationOperation, request proto.Message) ([]byte, bool) {
	switch operation {
	case rootFSMutationSetXattr:
		return request.(*pb.SetXattrRequest).GetValue(), true
	case rootFSMutationSymlink:
		return []byte(request.(*pb.SymlinkRequest).GetTarget()), true
	default:
		return nil, false
	}
}

func (s *rootFSBackedSession) executeMutation(
	ctx context.Context,
	operation rootFSMutationOperation,
	request proto.Message,
) (proto.Message, error) {
	s.mutationMu.Lock()
	defer s.mutationMu.Unlock()

	identity, durable := volumefuse.RequestIdentityFromContext(ctx)
	if !durable {
		return s.applyMutation(operation, request, &rootFSMutationGuard{})
	}
	fingerprint, err := rootFSMutationFingerprint(operation, request)
	if err != nil {
		return nil, err
	}
	transaction, err := s.resendLedger.begin(identity, operation, fingerprint)
	if err != nil {
		return nil, err
	}
	requestData, hasRequestData := rootFSMutationRequestData(operation, request)
	if transaction.state == rootFSResendApplied || transaction.state == rootFSResendAcked {
		if hasRequestData && !transaction.requestDataMatches(requestData) {
			return nil, fserror.New(fserror.FailedPrecondition, "resent rootfs mutation data does not match its durable intent")
		}
		response := newRootFSResponse(operation)
		resultErr := transaction.result(response)
		if resultErr == nil {
			guard := transaction.loadGuard()
			if err := s.validateAppliedMutation(operation, request, &guard); err != nil {
				return nil, err
			}
		}
		if err := attachRootFSCompletion(ctx, &transaction); err != nil {
			return nil, err
		}
		return response, resultErr
	}

	guard := transaction.loadGuard()
	if !transaction.guardReady() {
		if hasRequestData {
			if err := transaction.storeRequestData(requestData); err != nil {
				return nil, err
			}
		}
		guard, err = s.prepareMutation(operation, request)
		if err != nil {
			if commitErr := transaction.commit(guard, nil, err); commitErr != nil {
				return nil, errors.Join(err, commitErr)
			}
			if err := attachRootFSCompletion(ctx, &transaction); err != nil {
				return nil, err
			}
			return nil, err
		}
		if err := transaction.storeGuard(guard); err != nil {
			return nil, err
		}
	} else if identity.Resend {
		if hasRequestData && !transaction.requestDataMatches(requestData) {
			return nil, fserror.New(fserror.FailedPrecondition, "resent rootfs mutation data does not match its durable intent")
		}
		disposition, reconcileErr := s.reconcileIntent(operation, request, &guard)
		if reconcileErr != nil {
			return nil, reconcileErr
		}
		if disposition == rootFSIntentApplied {
			response, recoverErr := s.recoverMutationResponse(operation, request, &guard)
			if recoverErr != nil {
				return nil, recoverErr
			}
			if err := s.captureMutationAfter(operation, request, &guard); err != nil {
				return nil, err
			}
			if err := transaction.commit(guard, response, nil); err != nil {
				return nil, err
			}
			if hook := s.afterMutationCommit; hook != nil {
				hook(operation)
			}
			if err := attachRootFSCompletion(ctx, &transaction); err != nil {
				return nil, err
			}
			return response, nil
		}
	}

	response, responseErr := s.applyMutation(operation, request, &guard)
	if responseErr == nil {
		if hook := s.afterMutationApply; hook != nil {
			hook(operation)
		}
		if err := s.captureMutationAfter(operation, request, &guard); err != nil {
			return nil, err
		}
	}
	if err := transaction.commit(guard, response, responseErr); err != nil {
		return nil, err
	}
	if hook := s.afterMutationCommit; hook != nil {
		hook(operation)
	}
	if err := attachRootFSCompletion(ctx, &transaction); err != nil {
		return nil, err
	}
	return response, responseErr
}

func (s *rootFSBackedSession) executeWriteMutation(ctx context.Context, request *pb.WriteRequest) (*pb.WriteResponse, error) {
	s.mutationMu.Lock()
	defer s.mutationMu.Unlock()

	identity, durable := volumefuse.RequestIdentityFromContext(ctx)
	if !durable {
		return s.writeOnce(request)
	}
	fingerprint := rootFSWriteFingerprint(request)
	transaction, err := s.resendLedger.begin(identity, rootFSMutationWrite, fingerprint)
	if err != nil {
		return nil, err
	}
	if transaction.state == rootFSResendApplied || transaction.state == rootFSResendAcked {
		if !transaction.requestDataMatches(request.GetData()) {
			return nil, fserror.New(fserror.FailedPrecondition, "resent rootfs write data does not match its durable intent")
		}
		response := &pb.WriteResponse{}
		resultErr := transaction.result(response)
		if resultErr == nil {
			object, offset, _ := transaction.loadWriteGuard()
			current, currentErr := s.actualObjectForInode(request.GetInode())
			if currentErr != nil || !current.sameIdentity(object) {
				return nil, fserror.New(fserror.FailedPrecondition, "rootfs committed write host object was replaced")
			}
			matches, matchErr := s.fileRangeMatches(request.GetInode(), request.GetHandleId(), offset, request.GetData())
			if matchErr != nil || !matches {
				return nil, fserror.New(fserror.FailedPrecondition, "rootfs committed write data no longer matches")
			}
		}
		if err := attachRootFSCompletion(ctx, &transaction); err != nil {
			return nil, err
		}
		return response, resultErr
	}

	var prepared *os.File
	if !transaction.guardReady() {
		if err := transaction.storeRequestData(request.GetData()); err != nil {
			return nil, err
		}
		var object rootFSObjectState
		prepared, object, err = s.prepareWrite(request.GetInode(), request.GetHandleId())
		prepareErr := err
		if prepareErr != nil {
			if err := transaction.commitResult(nil, prepareErr); err != nil {
				return nil, err
			}
			if err := attachRootFSCompletion(ctx, &transaction); err != nil {
				return nil, err
			}
			return nil, prepareErr
		}
		if err := transaction.storeWriteGuard(object, request.GetOffset(), int64(len(request.GetData()))); err != nil {
			return nil, err
		}
	} else if identity.Resend {
		if !transaction.requestDataMatches(request.GetData()) {
			return nil, fserror.New(fserror.FailedPrecondition, "resent rootfs write data does not match its durable intent")
		}
		object, offset, length := transaction.loadWriteGuard()
		if offset != request.GetOffset() || length != int64(len(request.GetData())) {
			return nil, fserror.New(fserror.FailedPrecondition, "resent rootfs write range does not match its durable intent")
		}
		current, currentErr := s.actualObjectForInode(request.GetInode())
		if currentErr != nil || !current.sameIdentity(object) {
			return nil, fserror.New(fserror.FailedPrecondition, "rootfs write host object was replaced during recovery")
		}
		matches, matchErr := s.fileRangeMatches(request.GetInode(), request.GetHandleId(), offset, request.GetData())
		if matchErr != nil {
			return nil, matchErr
		}
		if matches {
			response := &pb.WriteResponse{BytesWritten: length}
			if err := transaction.commitResult(response, nil); err != nil {
				return nil, err
			}
			if hook := s.afterMutationCommit; hook != nil {
				hook(rootFSMutationWrite)
			}
			if err := attachRootFSCompletion(ctx, &transaction); err != nil {
				return nil, err
			}
			return response, nil
		}
	}

	if prepared == nil {
		prepared, _, err = s.prepareWrite(request.GetInode(), request.GetHandleId())
		if err != nil {
			return nil, err
		}
	}
	written, responseErr := prepared.WriteAt(request.GetData(), request.GetOffset())
	var response *pb.WriteResponse
	if responseErr == nil {
		response = &pb.WriteResponse{BytesWritten: int64(written)}
	} else {
		responseErr = mapRootFSBackedError(responseErr)
	}
	if responseErr == nil {
		if hook := s.afterMutationApply; hook != nil {
			hook(rootFSMutationWrite)
		}
	}
	if err := transaction.commitResult(response, responseErr); err != nil {
		return nil, err
	}
	if hook := s.afterMutationCommit; hook != nil {
		hook(rootFSMutationWrite)
	}
	if err := attachRootFSCompletion(ctx, &transaction); err != nil {
		return nil, err
	}
	return response, responseErr
}

func (s *rootFSBackedSession) prepareWrite(inode, handleID uint64) (*os.File, rootFSObjectState, error) {
	inode = normalizeRootFSInode(inode)
	if handleID != 0 && handleID != inode {
		return nil, rootFSObjectState{}, fserror.New(fserror.NotFound, fmt.Sprintf("file handle %d not found", handleID))
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.ensureOpenLocked(); err != nil {
		return nil, rootFSObjectState{}, err
	}
	identity, ok := s.identityByInode[inode]
	if !ok {
		return nil, rootFSObjectState{}, fserror.New(fserror.NotFound, fmt.Sprintf("inode %d not found", inode))
	}
	if handle := s.handles[inode]; handle != nil && handle.writeFile != nil {
		return handle.writeFile, objectStateFromIdentity(identity), nil
	}
	rel, err := s.relForInodeLocked(inode)
	if err != nil {
		return nil, rootFSObjectState{}, err
	}
	file, err := os.OpenFile(s.hostPath(rel), os.O_WRONLY, 0)
	if err != nil {
		return nil, rootFSObjectState{}, mapRootFSBackedError(err)
	}
	handle := s.handles[inode]
	if handle == nil {
		handle = &rootFSOpenHandle{}
		s.handles[inode] = handle
	}
	cacheRootFSFile(handle, file, os.O_WRONLY)
	return handle.writeFile, objectStateFromIdentity(identity), nil
}

func attachRootFSCompletion(ctx context.Context, transaction *rootFSResendTransaction) error {
	if volumefuse.AttachRequestCompletionToken(ctx, transaction.completionToken()) {
		return nil
	}
	return fserror.New(fserror.FailedPrecondition, "rootfs mutation has no FUSE reply acknowledgement route")
}

func (s *rootFSBackedSession) prepareMutation(operation rootFSMutationOperation, request proto.Message) (rootFSMutationGuard, error) {
	var guard rootFSMutationGuard
	var err error
	switch operation {
	case rootFSMutationSetAttr:
		req := request.(*pb.SetAttrRequest)
		guard.BeforePrimary, err = s.cachedObjectForInode(req.GetInode())
		if err != nil {
			return guard, err
		}
		guard.Valid = req.GetValid()
		guard.Mode = req.GetAttr().GetMode()
		guard.UID = req.GetAttr().GetUid()
		guard.GID = req.GetAttr().GetGid()
		now := time.Now()
		guard.AtimeSec, guard.AtimeNsec = now.Unix(), int64(now.Nanosecond())
		guard.MtimeSec, guard.MtimeNsec = guard.AtimeSec, guard.AtimeNsec
		if guard.Valid&fuse.FATTR_ATIME != 0 {
			guard.AtimeSec, guard.AtimeNsec = req.GetAttr().GetAtimeSec(), req.GetAttr().GetAtimeNsec()
		}
		if guard.Valid&fuse.FATTR_MTIME != 0 {
			guard.MtimeSec, guard.MtimeNsec = req.GetAttr().GetMtimeSec(), req.GetAttr().GetMtimeNsec()
		}
		guard.Length = int64(req.GetAttr().GetSize())
	case rootFSMutationMkdir:
		req := request.(*pb.MkdirRequest)
		_, rel, pathErr := s.childPath(req.GetParent(), req.GetName())
		if pathErr != nil {
			return guard, pathErr
		}
		guard.BeforePrimary = s.cachedObjectAtRel(rel)
	case rootFSMutationCreate:
		req := request.(*pb.CreateRequest)
		_, rel, pathErr := s.childPath(req.GetParent(), req.GetName())
		if pathErr != nil {
			return guard, pathErr
		}
		guard.BeforePrimary = s.cachedObjectAtRel(rel)
	case rootFSMutationUnlink:
		req := request.(*pb.UnlinkRequest)
		_, rel, pathErr := s.childPath(req.GetParent(), req.GetName())
		if pathErr != nil {
			return guard, pathErr
		}
		guard.BeforePrimary = s.cachedObjectAtRel(rel)
	case rootFSMutationRmdir:
		req := request.(*pb.RmdirRequest)
		_, rel, pathErr := s.childPath(req.GetParent(), req.GetName())
		if pathErr != nil {
			return guard, pathErr
		}
		guard.BeforePrimary = s.cachedObjectAtRel(rel)
	case rootFSMutationRename:
		req := request.(*pb.RenameRequest)
		_, oldRel, pathErr := s.childPath(req.GetOldParent(), req.GetOldName())
		if pathErr != nil {
			return guard, pathErr
		}
		_, newRel, pathErr := s.childPath(req.GetNewParent(), req.GetNewName())
		if pathErr != nil {
			return guard, pathErr
		}
		guard.BeforePrimary = s.cachedObjectAtRel(oldRel)
		guard.BeforeSecondary = s.cachedObjectAtRel(newRel)
	case rootFSMutationLink:
		req := request.(*pb.LinkRequest)
		guard.BeforePrimary, err = s.cachedObjectForInode(req.GetInode())
		if err != nil {
			return guard, err
		}
		_, newRel, pathErr := s.childPath(req.GetNewParent(), req.GetNewName())
		if pathErr != nil {
			return guard, pathErr
		}
		guard.BeforeSecondary = s.cachedObjectAtRel(newRel)
	case rootFSMutationSymlink:
		req := request.(*pb.SymlinkRequest)
		_, rel, pathErr := s.childPath(req.GetParent(), req.GetName())
		if pathErr != nil {
			return guard, pathErr
		}
		guard.BeforePrimary = s.cachedObjectAtRel(rel)
		guard.Length = int64(len(req.GetTarget()))
	case rootFSMutationFallocate:
		req := request.(*pb.FallocateRequest)
		guard.BeforePrimary, err = s.objectForWritableHandle(req.GetInode(), req.GetHandleId())
		if err != nil {
			return guard, err
		}
		guard.Offset, guard.Length = req.GetOffset(), req.GetLength()
		guard.Valid = req.GetMode()
	case rootFSMutationSetXattr:
		req := request.(*pb.SetXattrRequest)
		guard.BeforePrimary, err = s.cachedObjectForInode(req.GetInode())
		if err != nil {
			return guard, err
		}
		guard.BeforeSecondary, err = s.xattrState(req.GetInode(), req.GetName())
		if err != nil {
			return guard, err
		}
		guard.Length = int64(len(req.GetValue()))
		guard.Valid = req.GetFlags()
	case rootFSMutationRemoveXattr:
		req := request.(*pb.RemoveXattrRequest)
		guard.BeforePrimary, err = s.cachedObjectForInode(req.GetInode())
		if err != nil {
			return guard, err
		}
		guard.BeforeSecondary, err = s.xattrState(req.GetInode(), req.GetName())
	case rootFSMutationMknod:
		req := request.(*pb.MknodRequest)
		_, rel, pathErr := s.childPath(req.GetParent(), req.GetName())
		if pathErr != nil {
			return guard, pathErr
		}
		guard.BeforePrimary = s.cachedObjectAtRel(rel)
		guard.Mode = req.GetMode()
		guard.Length = int64(req.GetRdev())
	case rootFSMutationRelease:
		req := request.(*pb.ReleaseRequest)
		guard.BeforePrimary, err = s.cachedObjectForInode(req.GetHandleId())
		if err != nil {
			// A recovered ordinary RELEASE may no longer have a host path. Its
			// releaseHandleLocked path is still safely idempotent.
			guard.BeforePrimary = rootFSObjectState{}
			err = nil
		}
		guard.BeforeSecondary = s.cachedOrphanReleaseState(req.GetHandleId())
	case rootFSMutationReleaseDir:
		req := request.(*pb.ReleaseDirRequest)
		guard.BeforePrimary, err = s.cachedObjectForInode(req.GetHandleId())
		if err != nil {
			guard.BeforePrimary = rootFSObjectState{}
			err = nil
		}
		guard.BeforeSecondary = s.cachedOrphanReleaseState(req.GetHandleId())
	default:
		return guard, fserror.New(fserror.Unimplemented, "rootfs mutation is not covered by the resend ledger")
	}
	return guard, err
}

func (s *rootFSBackedSession) applyMutation(operation rootFSMutationOperation, request proto.Message, guard *rootFSMutationGuard) (proto.Message, error) {
	switch operation {
	case rootFSMutationSetAttr:
		var fixedAtime, fixedMtime time.Time
		if guard.AtimeSec != 0 || guard.AtimeNsec != 0 {
			fixedAtime = time.Unix(guard.AtimeSec, guard.AtimeNsec)
		}
		if guard.MtimeSec != 0 || guard.MtimeNsec != 0 {
			fixedMtime = time.Unix(guard.MtimeSec, guard.MtimeNsec)
		}
		return s.setAttrOnce(
			request.(*pb.SetAttrRequest),
			fixedAtime,
			fixedMtime,
		)
	case rootFSMutationMkdir:
		return s.mkdirOnce(request.(*pb.MkdirRequest))
	case rootFSMutationCreate:
		return s.createOnce(request.(*pb.CreateRequest))
	case rootFSMutationUnlink:
		return s.unlinkOnce(request.(*pb.UnlinkRequest))
	case rootFSMutationRmdir:
		return s.rmdirOnce(request.(*pb.RmdirRequest))
	case rootFSMutationRename:
		return s.renameOnce(request.(*pb.RenameRequest))
	case rootFSMutationLink:
		return s.linkOnce(request.(*pb.LinkRequest))
	case rootFSMutationSymlink:
		return s.symlinkOnce(request.(*pb.SymlinkRequest))
	case rootFSMutationFallocate:
		return s.fallocateOnce(request.(*pb.FallocateRequest))
	case rootFSMutationSetXattr:
		return s.setXattrOnce(request.(*pb.SetXattrRequest))
	case rootFSMutationRemoveXattr:
		return s.removeXattrOnce(request.(*pb.RemoveXattrRequest))
	case rootFSMutationMknod:
		return s.mknodOnce(request.(*pb.MknodRequest))
	case rootFSMutationRelease:
		return s.releaseOnce(request.(*pb.ReleaseRequest))
	case rootFSMutationReleaseDir:
		return s.releaseDirOnce(request.(*pb.ReleaseDirRequest))
	default:
		return nil, fserror.New(fserror.Unimplemented, "rootfs mutation is not implemented")
	}
}

func (s *rootFSBackedSession) captureMutationAfter(operation rootFSMutationOperation, request proto.Message, guard *rootFSMutationGuard) error {
	switch operation {
	case rootFSMutationSetAttr, rootFSMutationFallocate,
		rootFSMutationSetXattr, rootFSMutationRemoveXattr:
		// The initial guard already contains the stable host identity and the
		// desired overwrite. Recovery validates the live postcondition only on
		// the rare resend path.
		return nil
	case rootFSMutationMkdir:
		req := request.(*pb.MkdirRequest)
		_, rel, pathErr := s.childPath(req.GetParent(), req.GetName())
		if pathErr != nil {
			return pathErr
		}
		guard.AfterPrimary = s.cachedObjectAtRel(rel)
	case rootFSMutationCreate:
		req := request.(*pb.CreateRequest)
		_, rel, pathErr := s.childPath(req.GetParent(), req.GetName())
		if pathErr != nil {
			return pathErr
		}
		guard.AfterPrimary = s.cachedObjectAtRel(rel)
	case rootFSMutationUnlink:
		req := request.(*pb.UnlinkRequest)
		_, rel, pathErr := s.childPath(req.GetParent(), req.GetName())
		if pathErr != nil {
			return pathErr
		}
		guard.AfterPrimary = s.cachedObjectAtRel(rel)
		if guard.BeforePrimary.exists() {
			guard.AfterSecondary = s.cachedObjectAtRel(rootFSOrphanPath(guard.BeforePrimary.Inode))
		}
	case rootFSMutationRmdir:
		req := request.(*pb.RmdirRequest)
		_, rel, pathErr := s.childPath(req.GetParent(), req.GetName())
		if pathErr != nil {
			return pathErr
		}
		guard.AfterPrimary = s.cachedObjectAtRel(rel)
		if guard.BeforePrimary.exists() {
			guard.AfterSecondary = s.cachedObjectAtRel(rootFSOrphanPath(guard.BeforePrimary.Inode))
		}
	case rootFSMutationRename:
		req := request.(*pb.RenameRequest)
		_, oldRel, pathErr := s.childPath(req.GetOldParent(), req.GetOldName())
		if pathErr != nil {
			return pathErr
		}
		_, newRel, pathErr := s.childPath(req.GetNewParent(), req.GetNewName())
		if pathErr != nil {
			return pathErr
		}
		guard.AfterSecondary = s.cachedObjectAtRel(oldRel)
		guard.AfterPrimary = s.cachedObjectAtRel(newRel)
	case rootFSMutationLink:
		req := request.(*pb.LinkRequest)
		_, rel, pathErr := s.childPath(req.GetNewParent(), req.GetNewName())
		if pathErr != nil {
			return pathErr
		}
		guard.AfterPrimary = s.cachedObjectAtRel(rel)
	case rootFSMutationSymlink:
		req := request.(*pb.SymlinkRequest)
		_, rel, pathErr := s.childPath(req.GetParent(), req.GetName())
		if pathErr != nil {
			return pathErr
		}
		guard.AfterPrimary = s.cachedObjectAtRel(rel)
	case rootFSMutationMknod:
		req := request.(*pb.MknodRequest)
		_, rel, pathErr := s.childPath(req.GetParent(), req.GetName())
		if pathErr != nil {
			return pathErr
		}
		guard.AfterPrimary = s.cachedObjectAtRel(rel)
	case rootFSMutationRelease:
		req := request.(*pb.ReleaseRequest)
		guard.AfterSecondary = s.cachedOrphanReleaseState(req.GetHandleId())
	case rootFSMutationReleaseDir:
		req := request.(*pb.ReleaseDirRequest)
		guard.AfterSecondary = s.cachedOrphanReleaseState(req.GetHandleId())
	}
	return nil
}

func (s *rootFSBackedSession) reconcileIntent(operation rootFSMutationOperation, request proto.Message, guard *rootFSMutationGuard) (rootFSIntentDisposition, error) {
	failReplacement := func() (rootFSIntentDisposition, error) {
		return rootFSIntentRetry, fserror.New(fserror.FailedPrecondition, "rootfs mutation reconciliation found a replacement host object")
	}
	switch operation {
	case rootFSMutationSetAttr:
		current, err := s.actualObjectForInode(request.(*pb.SetAttrRequest).GetInode())
		if err != nil {
			return rootFSIntentRetry, err
		}
		if !current.sameIdentity(guard.BeforePrimary) {
			return failReplacement()
		}
		// Every SetAttr field is an overwrite. Reapplying it with the fixed NOW
		// timestamp completes any partially applied sequence safely.
		return rootFSIntentRetry, nil
	case rootFSMutationMkdir:
		req := request.(*pb.MkdirRequest)
		return s.reconcileCreateLike(req.GetParent(), req.GetName(), guard, func(path string) bool {
			info, err := os.Lstat(path)
			return err == nil && info.IsDir()
		})
	case rootFSMutationCreate:
		req := request.(*pb.CreateRequest)
		return s.reconcileCreateLike(req.GetParent(), req.GetName(), guard, func(path string) bool {
			info, err := os.Lstat(path)
			return err == nil && info.Mode().IsRegular()
		})
	case rootFSMutationMknod:
		req := request.(*pb.MknodRequest)
		return s.reconcileCreateLike(req.GetParent(), req.GetName(), guard, func(path string) bool {
			current, err := rootFSObjectAtPath(path)
			return err == nil && current.exists() && current.Mode == req.GetMode()&syscall.S_IFMT
		})
	case rootFSMutationSymlink:
		req := request.(*pb.SymlinkRequest)
		return s.reconcileCreateLike(req.GetParent(), req.GetName(), guard, func(path string) bool {
			target, err := os.Readlink(path)
			return err == nil && target == req.GetTarget()
		})
	case rootFSMutationUnlink:
		req := request.(*pb.UnlinkRequest)
		return s.reconcileRemoval(req.GetParent(), req.GetName(), guard)
	case rootFSMutationRmdir:
		req := request.(*pb.RmdirRequest)
		return s.reconcileRemoval(req.GetParent(), req.GetName(), guard)
	case rootFSMutationRename:
		return s.reconcileRename(request.(*pb.RenameRequest), guard)
	case rootFSMutationLink:
		return s.reconcileLink(request.(*pb.LinkRequest), guard)
	case rootFSMutationFallocate:
		req := request.(*pb.FallocateRequest)
		current, err := s.objectForWritableHandle(req.GetInode(), req.GetHandleId())
		if err != nil {
			return rootFSIntentRetry, err
		}
		if !current.sameIdentity(guard.BeforePrimary) {
			return failReplacement()
		}
		preSize, postSize, sizeSensitive := fallocateSizes(guard.BeforePrimary.Size, req.GetMode(), req.GetOffset(), req.GetLength())
		if sizeSensitive {
			switch current.Size {
			case postSize:
				return rootFSIntentApplied, nil
			case preSize:
				return rootFSIntentRetry, nil
			default:
				return failReplacement()
			}
		}
		return rootFSIntentRetry, nil
	case rootFSMutationSetXattr:
		req := request.(*pb.SetXattrRequest)
		return s.reconcileSetXattr(req.GetInode(), req.GetName(), req.GetValue(), guard)
	case rootFSMutationRemoveXattr:
		req := request.(*pb.RemoveXattrRequest)
		current, err := s.xattrState(req.GetInode(), req.GetName())
		if err != nil {
			return rootFSIntentRetry, err
		}
		if !current.valueExists() {
			if !guard.BeforeSecondary.valueExists() {
				return rootFSIntentRetry, nil
			}
			return rootFSIntentApplied, nil
		}
		if sameRootFSValue(current, guard.BeforeSecondary) {
			return rootFSIntentRetry, nil
		}
		return failReplacement()
	case rootFSMutationRelease:
		return s.reconcileRelease(request.(*pb.ReleaseRequest).GetHandleId(), guard)
	case rootFSMutationReleaseDir:
		return s.reconcileRelease(request.(*pb.ReleaseDirRequest).GetHandleId(), guard)
	default:
		return rootFSIntentRetry, fserror.New(fserror.Unimplemented, "rootfs mutation reconciliation is not implemented")
	}
}

func (s *rootFSBackedSession) validateAppliedMutation(operation rootFSMutationOperation, request proto.Message, guard *rootFSMutationGuard) error {
	replacement := func() error {
		return fserror.New(fserror.FailedPrecondition, "rootfs committed mutation no longer matches its host object")
	}
	switch operation {
	case rootFSMutationMkdir:
		req := request.(*pb.MkdirRequest)
		current, err := s.actualChildObject(req.GetParent(), req.GetName())
		if err != nil || !current.sameIdentity(guard.AfterPrimary) {
			return replacement()
		}
	case rootFSMutationCreate:
		req := request.(*pb.CreateRequest)
		current, err := s.actualChildObject(req.GetParent(), req.GetName())
		if err != nil || !current.sameIdentity(guard.AfterPrimary) {
			return replacement()
		}
	case rootFSMutationMknod:
		req := request.(*pb.MknodRequest)
		current, err := s.actualChildObject(req.GetParent(), req.GetName())
		if err != nil || !current.sameIdentity(guard.AfterPrimary) {
			return replacement()
		}
	case rootFSMutationSymlink:
		req := request.(*pb.SymlinkRequest)
		current, err := s.actualChildObject(req.GetParent(), req.GetName())
		if err != nil || !current.sameIdentity(guard.AfterPrimary) {
			return replacement()
		}
		target, err := os.Readlink(s.hostPathForChild(req.GetParent(), req.GetName()))
		if err != nil || target != req.GetTarget() {
			return replacement()
		}
	case rootFSMutationLink:
		req := request.(*pb.LinkRequest)
		current, err := s.actualChildObject(req.GetNewParent(), req.GetNewName())
		if err != nil || !current.sameIdentity(guard.AfterPrimary) {
			return replacement()
		}
	case rootFSMutationUnlink:
		req := request.(*pb.UnlinkRequest)
		if current, err := s.actualChildObject(req.GetParent(), req.GetName()); err != nil || current.exists() ||
			guard.AfterSecondary.exists() && !s.actualOrphanMatches(guard.AfterSecondary) {
			return replacement()
		}
	case rootFSMutationRmdir:
		req := request.(*pb.RmdirRequest)
		if current, err := s.actualChildObject(req.GetParent(), req.GetName()); err != nil || current.exists() ||
			guard.AfterSecondary.exists() && !s.actualOrphanMatches(guard.AfterSecondary) {
			return replacement()
		}
	case rootFSMutationRename:
		req := request.(*pb.RenameRequest)
		oldObject, oldErr := s.actualChildObject(req.GetOldParent(), req.GetOldName())
		newObject, newErr := s.actualChildObject(req.GetNewParent(), req.GetNewName())
		if oldErr != nil || newErr != nil || !oldObject.sameIdentity(guard.AfterSecondary) || !newObject.sameIdentity(guard.AfterPrimary) {
			return replacement()
		}
	case rootFSMutationSetAttr:
		req := request.(*pb.SetAttrRequest)
		current, err := s.actualObjectForInode(req.GetInode())
		if err != nil || !current.sameIdentity(guard.BeforePrimary) || !setAttrMatches(current, req, guard) {
			return replacement()
		}
	case rootFSMutationFallocate:
		req := request.(*pb.FallocateRequest)
		current, err := s.objectForWritableHandle(req.GetInode(), req.GetHandleId())
		if err != nil || !current.sameIdentity(guard.BeforePrimary) {
			return replacement()
		}
		_, postSize, sizeSensitive := fallocateSizes(guard.BeforePrimary.Size, req.GetMode(), req.GetOffset(), req.GetLength())
		if sizeSensitive && current.Size != postSize {
			return replacement()
		}
	case rootFSMutationSetXattr:
		req := request.(*pb.SetXattrRequest)
		current, err := s.xattrState(req.GetInode(), req.GetName())
		if err != nil || !current.valueExists() || current.ValueLength != int64(len(req.GetValue())) || current.ValueHash != rootFSHashData(req.GetValue()) {
			return replacement()
		}
	case rootFSMutationRemoveXattr:
		req := request.(*pb.RemoveXattrRequest)
		current, err := s.xattrState(req.GetInode(), req.GetName())
		if err != nil || current.valueExists() {
			return replacement()
		}
	case rootFSMutationRelease:
		if !s.cachedOrphanReleaseState(request.(*pb.ReleaseRequest).GetHandleId()).sameValueState(guard.AfterSecondary) {
			return replacement()
		}
	case rootFSMutationReleaseDir:
		if !s.cachedOrphanReleaseState(request.(*pb.ReleaseDirRequest).GetHandleId()).sameValueState(guard.AfterSecondary) {
			return replacement()
		}
	}
	return nil
}

func (s *rootFSBackedSession) recoverMutationResponse(operation rootFSMutationOperation, request proto.Message, guard *rootFSMutationGuard) (proto.Message, error) {
	switch operation {
	case rootFSMutationSetAttr:
		req := request.(*pb.SetAttrRequest)
		info, err := os.Lstat(s.hostPathForInode(req.GetInode()))
		if err != nil {
			return nil, mapRootFSBackedError(err)
		}
		return &pb.SetAttrResponse{Attr: attrFromFileInfo(req.GetInode(), info)}, nil
	case rootFSMutationMkdir:
		req := request.(*pb.MkdirRequest)
		return s.nodeResponseForChild(req.GetParent(), req.GetName(), false)
	case rootFSMutationCreate:
		req := request.(*pb.CreateRequest)
		return s.nodeResponseForChild(req.GetParent(), req.GetName(), true)
	case rootFSMutationLink:
		req := request.(*pb.LinkRequest)
		return s.nodeResponseForChild(req.GetNewParent(), req.GetNewName(), false)
	case rootFSMutationSymlink:
		req := request.(*pb.SymlinkRequest)
		return s.nodeResponseForChild(req.GetParent(), req.GetName(), false)
	case rootFSMutationMknod:
		req := request.(*pb.MknodRequest)
		return s.nodeResponseForChild(req.GetParent(), req.GetName(), false)
	case rootFSMutationUnlink, rootFSMutationRmdir, rootFSMutationRename, rootFSMutationFallocate,
		rootFSMutationSetXattr, rootFSMutationRemoveXattr, rootFSMutationRelease, rootFSMutationReleaseDir:
		return &pb.Empty{}, nil
	default:
		return nil, fserror.New(fserror.Unimplemented, "rootfs mutation response recovery is not implemented")
	}
}

func (s *rootFSBackedSession) reconcileCreateLike(parent uint64, name string, guard *rootFSMutationGuard, matches func(string) bool) (rootFSIntentDisposition, error) {
	current, err := s.actualChildObject(parent, name)
	if err != nil {
		return rootFSIntentRetry, err
	}
	if guard.BeforePrimary.exists() {
		if !current.sameIdentity(guard.BeforePrimary) {
			return rootFSIntentRetry, fserror.New(fserror.FailedPrecondition, "rootfs create target identity changed during recovery")
		}
		// Existing-file Create and create errors are safe to retry. O_TRUNC is
		// an overwrite and O_EXCL deterministically returns EEXIST.
		return rootFSIntentRetry, nil
	}
	if !current.exists() {
		return rootFSIntentRetry, nil
	}
	path := s.hostPathForChild(parent, name)
	if guard.AfterPrimary.exists() {
		if !current.sameIdentity(guard.AfterPrimary) {
			return rootFSIntentRetry, fserror.New(fserror.FailedPrecondition, "rootfs create target was replaced during recovery")
		}
	} else if !matches(path) {
		return rootFSIntentRetry, fserror.New(fserror.FailedPrecondition, "rootfs create target does not match its durable intent")
	}
	return rootFSIntentApplied, nil
}

func (s *rootFSBackedSession) reconcileRemoval(parent uint64, name string, guard *rootFSMutationGuard) (rootFSIntentDisposition, error) {
	current, err := s.actualChildObject(parent, name)
	if err != nil {
		return rootFSIntentRetry, err
	}
	if current.exists() {
		if !current.sameIdentity(guard.BeforePrimary) {
			return rootFSIntentRetry, fserror.New(fserror.FailedPrecondition, "rootfs removal target was replaced during recovery")
		}
		return rootFSIntentRetry, nil
	}
	if !guard.BeforePrimary.exists() {
		return rootFSIntentRetry, nil
	}
	return rootFSIntentApplied, nil
}

func (s *rootFSBackedSession) reconcileRename(req *pb.RenameRequest, guard *rootFSMutationGuard) (rootFSIntentDisposition, error) {
	oldObject, err := s.actualChildObject(req.GetOldParent(), req.GetOldName())
	if err != nil {
		return rootFSIntentRetry, err
	}
	newObject, err := s.actualChildObject(req.GetNewParent(), req.GetNewName())
	if err != nil {
		return rootFSIntentRetry, err
	}
	if oldObject.exists() {
		if !oldObject.sameIdentity(guard.BeforePrimary) {
			return rootFSIntentRetry, fserror.New(fserror.FailedPrecondition, "rootfs rename source was replaced during recovery")
		}
		if !newObject.sameIdentity(guard.BeforeSecondary) {
			return rootFSIntentRetry, fserror.New(fserror.FailedPrecondition, "rootfs rename target was replaced during recovery")
		}
		return rootFSIntentRetry, nil
	}
	if !guard.BeforePrimary.exists() {
		if newObject.sameIdentity(guard.BeforeSecondary) {
			return rootFSIntentRetry, nil
		}
		return rootFSIntentRetry, fserror.New(fserror.FailedPrecondition, "rootfs missing rename source has a changed target")
	}
	if newObject.exists() && newObject.identity() == guard.BeforePrimary.identity() {
		return rootFSIntentApplied, nil
	}
	return rootFSIntentRetry, fserror.New(fserror.FailedPrecondition, "rootfs rename result does not match its durable identities")
}

func (s *rootFSBackedSession) reconcileLink(req *pb.LinkRequest, guard *rootFSMutationGuard) (rootFSIntentDisposition, error) {
	source, err := s.actualObjectForInode(req.GetInode())
	if err != nil || !source.sameIdentity(guard.BeforePrimary) {
		return rootFSIntentRetry, fserror.New(fserror.FailedPrecondition, "rootfs hard-link source was replaced during recovery")
	}
	target, err := s.actualChildObject(req.GetNewParent(), req.GetNewName())
	if err != nil {
		return rootFSIntentRetry, err
	}
	if !target.exists() {
		return rootFSIntentRetry, nil
	}
	if guard.BeforeSecondary.exists() {
		if !target.sameIdentity(guard.BeforeSecondary) {
			return rootFSIntentRetry, fserror.New(fserror.FailedPrecondition, "rootfs hard-link target was replaced during recovery")
		}
		return rootFSIntentRetry, nil
	}
	if target.identity() != source.identity() {
		return rootFSIntentRetry, fserror.New(fserror.FailedPrecondition, "rootfs hard-link target was replaced during recovery")
	}
	return rootFSIntentApplied, nil
}

func (s *rootFSBackedSession) reconcileSetXattr(inode uint64, name string, value []byte, guard *rootFSMutationGuard) (rootFSIntentDisposition, error) {
	current, err := s.xattrState(inode, name)
	if err != nil {
		return rootFSIntentRetry, err
	}
	if (guard.Valid&unix.XATTR_CREATE != 0 && guard.BeforeSecondary.valueExists()) ||
		(guard.Valid&unix.XATTR_REPLACE != 0 && !guard.BeforeSecondary.valueExists()) {
		if sameRootFSValue(current, guard.BeforeSecondary) {
			return rootFSIntentRetry, nil
		}
		return rootFSIntentRetry, fserror.New(fserror.FailedPrecondition, "rootfs xattr precondition target changed during recovery")
	}
	if current.valueExists() && current.ValueLength == int64(len(value)) && current.ValueHash == rootFSHashData(value) {
		return rootFSIntentApplied, nil
	}
	if sameRootFSValue(current, guard.BeforeSecondary) {
		return rootFSIntentRetry, nil
	}
	return rootFSIntentRetry, fserror.New(fserror.FailedPrecondition, "rootfs xattr was replaced during recovery")
}

func (s *rootFSBackedSession) cachedObjectForInode(inode uint64) (rootFSObjectState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	inode = normalizeRootFSInode(inode)
	if err := s.ensureOpenLocked(); err != nil {
		return rootFSObjectState{}, err
	}
	identity, ok := s.identityByInode[inode]
	if !ok {
		return rootFSObjectState{}, fserror.New(fserror.NotFound, fmt.Sprintf("inode %d not found", inode))
	}
	return objectStateFromIdentity(identity), nil
}

func (s *rootFSBackedSession) cachedObjectAtRel(rel string) rootFSObjectState {
	s.mu.Lock()
	defer s.mu.Unlock()
	inode, ok := s.inodeByPath[cleanRootFSBackedRel(rel)]
	if !ok {
		return rootFSObjectState{}
	}
	return objectStateFromIdentity(s.identityByInode[inode])
}

func objectStateFromIdentity(identity rootFSHostIdentity) rootFSObjectState {
	return rootFSObjectState{Flags: rootFSObjectExists, Mode: identity.Mode, Device: identity.Device, Inode: identity.Inode}
}

func rootFSObjectFromInfo(info os.FileInfo) (rootFSObjectState, error) {
	identity, err := rootFSIdentityFromInfo(info)
	if err != nil {
		return rootFSObjectState{}, err
	}
	state := objectStateFromIdentity(identity)
	state.Size = info.Size()
	if stat, ok := info.Sys().(*syscall.Stat_t); ok {
		state.AttrMode = stat.Mode
		state.UID = stat.Uid
		state.GID = stat.Gid
		state.Rdev = uint64(stat.Rdev)
		state.AtimeSec = stat.Atim.Sec
		state.AtimeNsec = stat.Atim.Nsec
		state.MtimeSec = stat.Mtim.Sec
		state.MtimeNsec = stat.Mtim.Nsec
	}
	return state, nil
}

func rootFSObjectAtPath(path string) (rootFSObjectState, error) {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return rootFSObjectState{}, nil
	}
	if err != nil {
		return rootFSObjectState{}, mapRootFSBackedError(err)
	}
	return rootFSObjectFromInfo(info)
}

func (s *rootFSBackedSession) actualChildObject(parent uint64, name string) (rootFSObjectState, error) {
	return rootFSObjectAtPath(s.hostPathForChild(parent, name))
}

func (s *rootFSBackedSession) actualObjectForInode(inode uint64) (rootFSObjectState, error) {
	return rootFSObjectAtPath(s.hostPathForInode(inode))
}

func (s *rootFSBackedSession) objectForWritableHandle(inode, handleID uint64) (rootFSObjectState, error) {
	file, release, err := s.handleForWrite(inode, handleID)
	if err != nil {
		return rootFSObjectState{}, err
	}
	defer release()
	info, err := file.Stat()
	if err != nil {
		return rootFSObjectState{}, mapRootFSBackedError(err)
	}
	return rootFSObjectFromInfo(info)
}

func (s *rootFSBackedSession) childPath(parent uint64, name string) (string, string, error) {
	parentRel, err := s.relForInode(parent)
	if err != nil {
		return "", "", err
	}
	rel, err := childRel(parentRel, name)
	if err != nil {
		return "", "", err
	}
	return s.hostPath(rel), rel, nil
}

func (s *rootFSBackedSession) hostPathForChild(parent uint64, name string) string {
	path, _, err := s.childPath(parent, name)
	if err != nil {
		return ""
	}
	return path
}

func (s *rootFSBackedSession) hostPathForInode(inode uint64) string {
	rel, err := s.relForInode(inode)
	if err != nil {
		return ""
	}
	return s.hostPath(rel)
}

func (s *rootFSBackedSession) nodeResponseForChild(parent uint64, name string, withHandle bool) (*pb.NodeResponse, error) {
	_, rel, err := s.childPath(parent, name)
	if err != nil {
		return nil, err
	}
	info, err := os.Lstat(s.hostPath(rel))
	if err != nil {
		return nil, mapRootFSBackedError(err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	inode, err := s.inodeForExistingPathLocked(rel, info)
	if err != nil {
		return nil, err
	}
	response := &pb.NodeResponse{Inode: inode, Generation: 1, Attr: attrFromFileInfo(inode, info)}
	if withHandle {
		response.HandleId = inode
	}
	return response, nil
}

func (s *rootFSBackedSession) xattrState(inode uint64, name string) (rootFSObjectState, error) {
	path := s.hostPathForInode(inode)
	if path == "" {
		return rootFSObjectState{}, fserror.New(fserror.NotFound, fmt.Sprintf("inode %d not found", inode))
	}
	size, err := unix.Lgetxattr(path, name, nil)
	if errors.Is(err, unix.ENODATA) {
		return rootFSObjectState{}, nil
	}
	if err != nil {
		return rootFSObjectState{}, mapRootFSBackedError(err)
	}
	value := make([]byte, size)
	if size > 0 {
		if _, err := unix.Lgetxattr(path, name, value); err != nil {
			return rootFSObjectState{}, mapRootFSBackedError(err)
		}
	}
	return rootFSObjectState{
		Flags: rootFSObjectValueExists, ValueLength: int64(size), ValueHash: rootFSHashData(value),
	}, nil
}

func sameRootFSValue(left, right rootFSObjectState) bool {
	return left.valueExists() == right.valueExists() && (!left.valueExists() ||
		left.ValueLength == right.ValueLength && left.ValueHash == right.ValueHash)
}

func (s rootFSObjectState) sameValueState(other rootFSObjectState) bool {
	return s.valueExists() == other.valueExists() && (!s.valueExists() ||
		s.identity() == other.identity() && s.ValueLength == other.ValueLength && s.ValueHash == other.ValueHash)
}

func (s *rootFSBackedSession) cachedOrphanReleaseState(inode uint64) rootFSObjectState {
	s.mu.Lock()
	defer s.mu.Unlock()
	orphan, ok := s.orphans[normalizeRootFSInode(inode)]
	if !ok {
		return rootFSObjectState{}
	}
	return rootFSObjectState{
		Flags: rootFSObjectValueExists,
		Mode:  orphan.Identity.Mode, Device: orphan.Identity.Device, Inode: orphan.Identity.Inode,
		ValueLength: int64(orphan.OpenCount),
	}
}

func (s *rootFSBackedSession) reconcileRelease(inode uint64, guard *rootFSMutationGuard) (rootFSIntentDisposition, error) {
	before := guard.BeforeSecondary
	current := s.cachedOrphanReleaseState(inode)
	if !before.valueExists() {
		// Ordinary handle counts are process-local. After replacement the old
		// count is gone, so one replay restores the single RELEASE effect.
		return rootFSIntentRetry, nil
	}
	if before.ValueLength <= 1 {
		if !current.valueExists() {
			return rootFSIntentApplied, nil
		}
		if current.sameValueState(before) {
			return rootFSIntentRetry, nil
		}
	} else {
		expected := before
		expected.ValueLength--
		if current.sameValueState(expected) {
			return rootFSIntentApplied, nil
		}
		if current.sameValueState(before) {
			return rootFSIntentRetry, nil
		}
	}
	return rootFSIntentRetry, fserror.New(fserror.FailedPrecondition, "rootfs orphan release count changed during recovery")
}

func (s *rootFSBackedSession) fileRangeMatches(inode, handleID uint64, offset int64, expected []byte) (bool, error) {
	length := int64(len(expected))
	if length < 0 || length > int64(int(^uint(0)>>1)) {
		return false, fserror.New(fserror.InvalidArgument, "rootfs write recovery range is invalid")
	}
	file, release, err := s.handleForRead(inode, handleID)
	if err != nil {
		return false, err
	}
	defer release()
	data := make([]byte, int(length))
	n, err := file.ReadAt(data, offset)
	if err != nil && !errors.Is(err, io.EOF) {
		return false, mapRootFSBackedError(err)
	}
	return int64(n) == length && bytes.Equal(data[:n], expected), nil
}

func fallocateSizes(preSize int64, mode uint32, offset, length int64) (int64, int64, bool) {
	switch {
	case mode&unix.FALLOC_FL_COLLAPSE_RANGE != 0:
		return preSize, preSize - length, true
	case mode&unix.FALLOC_FL_INSERT_RANGE != 0:
		return preSize, preSize + length, true
	case mode&unix.FALLOC_FL_KEEP_SIZE == 0 && mode&unix.FALLOC_FL_PUNCH_HOLE == 0:
		post := preSize
		if offset+length > post {
			post = offset + length
		}
		return preSize, post, post != preSize
	default:
		return preSize, preSize, false
	}
}

func setAttrMatches(current rootFSObjectState, req *pb.SetAttrRequest, guard *rootFSMutationGuard) bool {
	valid := req.GetValid()
	if valid&fuse.FATTR_MODE != 0 && current.AttrMode&0o7777 != req.GetAttr().GetMode()&0o7777 {
		return false
	}
	if valid&fuse.FATTR_UID != 0 && current.UID != req.GetAttr().GetUid() {
		return false
	}
	if valid&fuse.FATTR_GID != 0 && current.GID != req.GetAttr().GetGid() {
		return false
	}
	if valid&fuse.FATTR_SIZE != 0 && current.Size != int64(req.GetAttr().GetSize()) {
		return false
	}
	if valid&(fuse.FATTR_ATIME|fuse.FATTR_ATIME_NOW) != 0 &&
		(current.AtimeSec != guard.AtimeSec || current.AtimeNsec != guard.AtimeNsec) {
		return false
	}
	if valid&(fuse.FATTR_MTIME|fuse.FATTR_MTIME_NOW) != 0 &&
		(current.MtimeSec != guard.MtimeSec || current.MtimeNsec != guard.MtimeNsec) {
		return false
	}
	return true
}

func (s *rootFSBackedSession) actualOrphanMatches(expected rootFSObjectState) bool {
	current, err := rootFSObjectAtPath(s.hostPath(rootFSOrphanPath(expected.Inode)))
	return err == nil && current.sameIdentity(expected)
}

func (s *rootFSBackedSession) SetAttr(ctx context.Context, req *pb.SetAttrRequest) (*pb.SetAttrResponse, error) {
	response, err := s.executeMutation(ctx, rootFSMutationSetAttr, req)
	if response == nil {
		return nil, err
	}
	return response.(*pb.SetAttrResponse), err
}

func (s *rootFSBackedSession) Mkdir(ctx context.Context, req *pb.MkdirRequest) (*pb.NodeResponse, error) {
	response, err := s.executeMutation(ctx, rootFSMutationMkdir, req)
	if response == nil {
		return nil, err
	}
	return response.(*pb.NodeResponse), err
}

func (s *rootFSBackedSession) Create(ctx context.Context, req *pb.CreateRequest) (*pb.NodeResponse, error) {
	response, err := s.executeMutation(ctx, rootFSMutationCreate, req)
	if response == nil {
		return nil, err
	}
	return response.(*pb.NodeResponse), err
}

func (s *rootFSBackedSession) Unlink(ctx context.Context, req *pb.UnlinkRequest) (*pb.Empty, error) {
	response, err := s.executeMutation(ctx, rootFSMutationUnlink, req)
	if response == nil {
		return nil, err
	}
	return response.(*pb.Empty), err
}

func (s *rootFSBackedSession) Rmdir(ctx context.Context, req *pb.RmdirRequest) (*pb.Empty, error) {
	response, err := s.executeMutation(ctx, rootFSMutationRmdir, req)
	if response == nil {
		return nil, err
	}
	return response.(*pb.Empty), err
}

func (s *rootFSBackedSession) Rename(ctx context.Context, req *pb.RenameRequest) (*pb.Empty, error) {
	response, err := s.executeMutation(ctx, rootFSMutationRename, req)
	if response == nil {
		return nil, err
	}
	return response.(*pb.Empty), err
}

func (s *rootFSBackedSession) Link(ctx context.Context, req *pb.LinkRequest) (*pb.NodeResponse, error) {
	response, err := s.executeMutation(ctx, rootFSMutationLink, req)
	if response == nil {
		return nil, err
	}
	return response.(*pb.NodeResponse), err
}

func (s *rootFSBackedSession) Symlink(ctx context.Context, req *pb.SymlinkRequest) (*pb.NodeResponse, error) {
	response, err := s.executeMutation(ctx, rootFSMutationSymlink, req)
	if response == nil {
		return nil, err
	}
	return response.(*pb.NodeResponse), err
}

func (s *rootFSBackedSession) Write(ctx context.Context, req *pb.WriteRequest) (*pb.WriteResponse, error) {
	return s.executeWriteMutation(ctx, req)
}

func (s *rootFSBackedSession) Fallocate(ctx context.Context, req *pb.FallocateRequest) (*pb.Empty, error) {
	response, err := s.executeMutation(ctx, rootFSMutationFallocate, req)
	if response == nil {
		return nil, err
	}
	return response.(*pb.Empty), err
}

func (s *rootFSBackedSession) SetXattr(ctx context.Context, req *pb.SetXattrRequest) (*pb.Empty, error) {
	response, err := s.executeMutation(ctx, rootFSMutationSetXattr, req)
	if response == nil {
		return nil, err
	}
	return response.(*pb.Empty), err
}

func (s *rootFSBackedSession) RemoveXattr(ctx context.Context, req *pb.RemoveXattrRequest) (*pb.Empty, error) {
	response, err := s.executeMutation(ctx, rootFSMutationRemoveXattr, req)
	if response == nil {
		return nil, err
	}
	return response.(*pb.Empty), err
}

func (s *rootFSBackedSession) Mknod(ctx context.Context, req *pb.MknodRequest) (*pb.NodeResponse, error) {
	response, err := s.executeMutation(ctx, rootFSMutationMknod, req)
	if response == nil {
		return nil, err
	}
	return response.(*pb.NodeResponse), err
}

func (s *rootFSBackedSession) Release(ctx context.Context, req *pb.ReleaseRequest) (*pb.Empty, error) {
	response, err := s.executeMutation(ctx, rootFSMutationRelease, req)
	if response == nil {
		return nil, err
	}
	return response.(*pb.Empty), err
}

func (s *rootFSBackedSession) ReleaseDir(ctx context.Context, req *pb.ReleaseDirRequest) (*pb.Empty, error) {
	response, err := s.executeMutation(ctx, rootFSMutationReleaseDir, req)
	if response == nil {
		return nil, err
	}
	return response.(*pb.Empty), err
}
