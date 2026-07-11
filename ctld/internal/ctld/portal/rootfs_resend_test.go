package portal

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/sandbox0-ai/sandbox0/pkg/volumefuse"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/fserror"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/s0fs"
	pb "github.com/sandbox0-ai/sandbox0/storage-proxy/proto/fs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
	"google.golang.org/protobuf/proto"
)

const rootFSResendCrashExitCode = 91

func rootFSRequestContext(scope string, unique uint64, resend bool, token *volumefuse.RequestCompletionToken) context.Context {
	return volumefuse.ContextWithRequestIdentity(
		context.Background(),
		volumefuse.RequestIdentity{Scope: scope, Unique: unique, Resend: resend},
		func(completion volumefuse.RequestCompletionToken) {
			if token != nil {
				*token = completion
			}
		},
	)
}

func requireRootFSCompletion(t *testing.T, token volumefuse.RequestCompletionToken) {
	t.Helper()
	require.NotNil(t, token)
	token.RequestAcknowledged()
}

func requireRootFSMutationPanic(t *testing.T, call func()) {
	t.Helper()
	deferred := false
	func() {
		defer func() {
			deferred = recover() != nil
		}()
		call()
	}()
	require.True(t, deferred, "mutation did not reach the crash hook")
}

func TestRootFSResendReplaysCommittedResponse(t *testing.T) {
	backing := t.TempDir()
	first := newRootFSBackedSession(backing)
	require.NoError(t, first.InitError())
	var firstToken volumefuse.RequestCompletionToken
	request := &pb.MkdirRequest{Parent: s0fs.RootInode, Name: "committed", Mode: 0o700}
	created, err := first.Mkdir(rootFSRequestContext("shard-a", 41, false, &firstToken), request)
	require.NoError(t, err)
	require.NotNil(t, firstToken)
	first.Close() // Simulate replacement before the kernel reply acknowledgement.

	second := newRootFSBackedSession(backing)
	require.NoError(t, second.InitError())
	defer second.Close()
	var replayToken volumefuse.RequestCompletionToken
	replayed, err := second.Mkdir(rootFSRequestContext("shard-a", 41, true, &replayToken), request)
	require.NoError(t, err)
	assert.True(t, proto.Equal(created, replayed), "replayed response differs from committed response")
	requireRootFSCompletion(t, replayToken)

	second.resendLedger.mu.Lock()
	defer second.resendLedger.mu.Unlock()
	foundAcked := false
	for i := range second.resendLedger.records {
		record := &second.resendLedger.records[i]
		if record.Unique == 41 && rootFSResendState(record.State) == rootFSResendAcked {
			foundAcked = true
		}
	}
	assert.True(t, foundAcked)
}

func TestRootFSResendRejectsFingerprintAndReplacement(t *testing.T) {
	backing := t.TempDir()
	first := newRootFSBackedSession(backing)
	require.NoError(t, first.InitError())
	request := &pb.MkdirRequest{Parent: s0fs.RootInode, Name: "guarded", Mode: 0o700}
	_, err := first.Mkdir(rootFSRequestContext("shard-a", 52, false, new(volumefuse.RequestCompletionToken)), request)
	require.NoError(t, err)
	first.Close()

	second := newRootFSBackedSession(backing)
	require.NoError(t, second.InitError())
	_, err = second.Mkdir(rootFSRequestContext("shard-a", 52, true, nil), &pb.MkdirRequest{
		Parent: s0fs.RootInode, Name: "different", Mode: 0o700,
	})
	require.Error(t, err)
	assert.Equal(t, fserror.FailedPrecondition, fserror.CodeOf(err))
	second.Close()

	require.NoError(t, os.Rename(filepath.Join(backing, "guarded"), filepath.Join(backing, "original-object")))
	require.NoError(t, os.Mkdir(filepath.Join(backing, "guarded"), 0o700))
	third := newRootFSBackedSession(backing)
	require.NoError(t, third.InitError())
	defer third.Close()
	_, err = third.Mkdir(rootFSRequestContext("shard-a", 52, true, nil), request)
	require.Error(t, err)
	assert.Equal(t, fserror.FailedPrecondition, fserror.CodeOf(err))
}

func TestRootFSResendRejectsChangedWriteData(t *testing.T) {
	backing := t.TempDir()
	first := newRootFSBackedSession(backing)
	require.NoError(t, first.InitError())
	file, err := first.Create(context.Background(), &pb.CreateRequest{
		Parent: s0fs.RootInode, Name: "data", Mode: 0o600, Flags: uint32(os.O_RDWR),
	})
	require.NoError(t, err)
	_, err = first.Write(rootFSRequestContext("write-data", 53, false, new(volumefuse.RequestCompletionToken)), &pb.WriteRequest{
		Inode: file.Inode, HandleId: file.HandleId, Data: []byte("original"),
	})
	require.NoError(t, err)
	first.Close()

	second := newRootFSBackedSession(backing)
	require.NoError(t, second.InitError())
	defer second.Close()
	_, err = second.Write(rootFSRequestContext("write-data", 53, true, nil), &pb.WriteRequest{
		Inode: file.Inode, HandleId: file.HandleId, Data: []byte("modified"),
	})
	require.Error(t, err)
	assert.Equal(t, fserror.FailedPrecondition, fserror.CodeOf(err))
	assertFileContentForPortalTest(t, filepath.Join(backing, "data"), "original")
}

func TestRootFSResendLedgerCorruptionFailsClosed(t *testing.T) {
	backing := t.TempDir()
	first := newRootFSBackedSession(backing)
	require.NoError(t, first.InitError())
	ledgerPath := first.resendLedgerPath()
	first.Close()
	ledger, err := os.OpenFile(ledgerPath, os.O_WRONLY, 0)
	require.NoError(t, err)
	_, err = ledger.WriteAt([]byte("X"), 0)
	require.NoError(t, err)
	require.NoError(t, ledger.Close())

	second := newRootFSBackedSession(backing)
	defer second.Close()
	require.Error(t, second.InitError())
	_, err = second.Mkdir(context.Background(), &pb.MkdirRequest{Parent: s0fs.RootInode, Name: "blocked"})
	require.Error(t, err)
	require.NoDirExists(t, filepath.Join(backing, "blocked"))
}

func TestRootFSResendIntentReconcilesNamespaceMutations(t *testing.T) {
	tests := []struct {
		name      string
		operation rootFSMutationOperation
		prepare   func(*testing.T, string) func(*rootFSBackedSession, context.Context) error
	}{
		{
			name: "mkdir", operation: rootFSMutationMkdir,
			prepare: func(_ *testing.T, _ string) func(*rootFSBackedSession, context.Context) error {
				req := &pb.MkdirRequest{Parent: s0fs.RootInode, Name: "directory", Mode: 0o700}
				return func(session *rootFSBackedSession, ctx context.Context) error {
					_, err := session.Mkdir(ctx, req)
					return err
				}
			},
		},
		{
			name: "create", operation: rootFSMutationCreate,
			prepare: func(_ *testing.T, _ string) func(*rootFSBackedSession, context.Context) error {
				req := &pb.CreateRequest{Parent: s0fs.RootInode, Name: "created", Mode: 0o600, Flags: uint32(os.O_RDWR | os.O_EXCL)}
				return func(session *rootFSBackedSession, ctx context.Context) error {
					_, err := session.Create(ctx, req)
					return err
				}
			},
		},
		{
			name: "symlink", operation: rootFSMutationSymlink,
			prepare: func(_ *testing.T, _ string) func(*rootFSBackedSession, context.Context) error {
				req := &pb.SymlinkRequest{Parent: s0fs.RootInode, Name: "link", Target: "target-value"}
				return func(session *rootFSBackedSession, ctx context.Context) error {
					_, err := session.Symlink(ctx, req)
					return err
				}
			},
		},
		{
			name: "mknod", operation: rootFSMutationMknod,
			prepare: func(_ *testing.T, _ string) func(*rootFSBackedSession, context.Context) error {
				req := &pb.MknodRequest{Parent: s0fs.RootInode, Name: "node", Mode: syscall.S_IFREG | 0o600}
				return func(session *rootFSBackedSession, ctx context.Context) error {
					_, err := session.Mknod(ctx, req)
					return err
				}
			},
		},
		{
			name: "link", operation: rootFSMutationLink,
			prepare: func(t *testing.T, backing string) func(*rootFSBackedSession, context.Context) error {
				require.NoError(t, os.WriteFile(filepath.Join(backing, "source"), []byte("source"), 0o600))
				info, err := os.Lstat(filepath.Join(backing, "source"))
				require.NoError(t, err)
				identity, err := rootFSIdentityFromInfo(info)
				require.NoError(t, err)
				req := &pb.LinkRequest{Inode: identity.Inode, NewParent: s0fs.RootInode, NewName: "alias"}
				return func(session *rootFSBackedSession, ctx context.Context) error {
					_, err := session.Link(ctx, req)
					return err
				}
			},
		},
		{
			name: "unlink", operation: rootFSMutationUnlink,
			prepare: func(t *testing.T, backing string) func(*rootFSBackedSession, context.Context) error {
				require.NoError(t, os.WriteFile(filepath.Join(backing, "removed"), []byte("data"), 0o600))
				req := &pb.UnlinkRequest{Parent: s0fs.RootInode, Name: "removed"}
				return func(session *rootFSBackedSession, ctx context.Context) error {
					_, err := session.Unlink(ctx, req)
					return err
				}
			},
		},
		{
			name: "rmdir", operation: rootFSMutationRmdir,
			prepare: func(t *testing.T, backing string) func(*rootFSBackedSession, context.Context) error {
				require.NoError(t, os.Mkdir(filepath.Join(backing, "removed-dir"), 0o700))
				req := &pb.RmdirRequest{Parent: s0fs.RootInode, Name: "removed-dir"}
				return func(session *rootFSBackedSession, ctx context.Context) error {
					_, err := session.Rmdir(ctx, req)
					return err
				}
			},
		},
		{
			name: "rename", operation: rootFSMutationRename,
			prepare: func(t *testing.T, backing string) func(*rootFSBackedSession, context.Context) error {
				require.NoError(t, os.WriteFile(filepath.Join(backing, "before"), []byte("data"), 0o600))
				req := &pb.RenameRequest{OldParent: s0fs.RootInode, OldName: "before", NewParent: s0fs.RootInode, NewName: "after"}
				return func(session *rootFSBackedSession, ctx context.Context) error {
					_, err := session.Rename(ctx, req)
					return err
				}
			},
		},
	}

	for i, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			backing := t.TempDir()
			call := test.prepare(t, backing)
			first := newRootFSBackedSession(backing)
			require.NoError(t, first.InitError())
			first.afterMutationApply = func(operation rootFSMutationOperation) {
				if operation == test.operation {
					panic("simulated process exit after host mutation")
				}
			}
			requireRootFSMutationPanic(t, func() {
				_ = call(first, rootFSRequestContext("shard-intent", uint64(100+i), false, nil))
			})
			first.Close()

			second := newRootFSBackedSession(backing)
			require.NoError(t, second.InitError())
			var token volumefuse.RequestCompletionToken
			err := call(second, rootFSRequestContext("shard-intent", uint64(100+i), true, &token))
			require.NoError(t, err)
			requireRootFSCompletion(t, token)
			second.Close()
		})
	}
}

func TestRootFSResendIntentReconcilesWriteAndXattr(t *testing.T) {
	backing := t.TempDir()
	first := newRootFSBackedSession(backing)
	require.NoError(t, first.InitError())
	file, err := first.Create(context.Background(), &pb.CreateRequest{
		Parent: s0fs.RootInode, Name: "mutable", Mode: 0o600, Flags: uint32(os.O_RDWR),
	})
	require.NoError(t, err)
	first.afterMutationApply = func(operation rootFSMutationOperation) {
		if operation == rootFSMutationWrite {
			panic("crash after write")
		}
	}
	write := &pb.WriteRequest{Inode: file.Inode, HandleId: file.HandleId, Offset: 3, Data: []byte("once")}
	requireRootFSMutationPanic(t, func() {
		_, _ = first.Write(rootFSRequestContext("shard-data", 201, false, nil), write)
	})
	first.Close()

	second := newRootFSBackedSession(backing)
	require.NoError(t, second.InitError())
	var writeToken volumefuse.RequestCompletionToken
	response, err := second.Write(rootFSRequestContext("shard-data", 201, true, &writeToken), write)
	require.NoError(t, err)
	assert.Equal(t, int64(4), response.GetBytesWritten())
	requireRootFSCompletion(t, writeToken)
	assertFileContentForPortalTest(t, filepath.Join(backing, "mutable"), "\x00\x00\x00once")

	second.afterMutationApply = func(operation rootFSMutationOperation) {
		if operation == rootFSMutationSetXattr {
			panic("crash after setxattr")
		}
	}
	setRequest := &pb.SetXattrRequest{Inode: file.Inode, Name: "user.sandbox0", Value: []byte("value")}
	requireRootFSMutationPanic(t, func() {
		_, _ = second.SetXattr(rootFSRequestContext("shard-data", 202, false, nil), setRequest)
	})
	second.Close()

	third := newRootFSBackedSession(backing)
	require.NoError(t, third.InitError())
	defer third.Close()
	var xattrToken volumefuse.RequestCompletionToken
	_, err = third.SetXattr(rootFSRequestContext("shard-data", 202, true, &xattrToken), setRequest)
	require.NoError(t, err)
	requireRootFSCompletion(t, xattrToken)
	value, err := third.GetXattr(context.Background(), &pb.GetXattrRequest{Inode: file.Inode, Name: "user.sandbox0"})
	require.NoError(t, err)
	assert.Equal(t, []byte("value"), value.GetValue())

	third.afterMutationApply = func(operation rootFSMutationOperation) {
		if operation == rootFSMutationRemoveXattr {
			panic("crash after removexattr")
		}
	}
	removeRequest := &pb.RemoveXattrRequest{Inode: file.Inode, Name: "user.sandbox0"}
	requireRootFSMutationPanic(t, func() {
		_, _ = third.RemoveXattr(rootFSRequestContext("shard-data", 203, false, nil), removeRequest)
	})
	third.Close()

	fourth := newRootFSBackedSession(backing)
	require.NoError(t, fourth.InitError())
	defer fourth.Close()
	var removeToken volumefuse.RequestCompletionToken
	_, err = fourth.RemoveXattr(rootFSRequestContext("shard-data", 203, true, &removeToken), removeRequest)
	require.NoError(t, err)
	requireRootFSCompletion(t, removeToken)
	_, err = fourth.GetXattr(context.Background(), &pb.GetXattrRequest{Inode: file.Inode, Name: "user.sandbox0"})
	require.Error(t, err)
}

func TestRootFSResendSetAttrNowIsStable(t *testing.T) {
	backing := t.TempDir()
	first := newRootFSBackedSession(backing)
	require.NoError(t, first.InitError())
	file, err := first.Create(context.Background(), &pb.CreateRequest{Parent: s0fs.RootInode, Name: "times", Mode: 0o600})
	require.NoError(t, err)
	request := &pb.SetAttrRequest{
		Inode: file.Inode,
		Valid: fuse.FATTR_ATIME_NOW | fuse.FATTR_MTIME_NOW,
		Attr:  &pb.GetAttrResponse{},
	}
	first.afterMutationApply = func(operation rootFSMutationOperation) {
		if operation == rootFSMutationSetAttr {
			panic("crash after setattr")
		}
	}
	requireRootFSMutationPanic(t, func() {
		_, _ = first.SetAttr(rootFSRequestContext("shard-time", 301, false, nil), request)
	})
	before, err := os.Lstat(filepath.Join(backing, "times"))
	require.NoError(t, err)
	beforeAttr := attrFromFileInfo(file.Inode, before)
	first.Close()

	time.Sleep(20 * time.Millisecond)
	second := newRootFSBackedSession(backing)
	require.NoError(t, second.InitError())
	defer second.Close()
	var token volumefuse.RequestCompletionToken
	response, err := second.SetAttr(rootFSRequestContext("shard-time", 301, true, &token), request)
	require.NoError(t, err)
	requireRootFSCompletion(t, token)
	assert.Equal(t, beforeAttr.GetAtimeSec(), response.GetAttr().GetAtimeSec())
	assert.Equal(t, beforeAttr.GetAtimeNsec(), response.GetAttr().GetAtimeNsec())
	assert.Equal(t, beforeAttr.GetMtimeSec(), response.GetAttr().GetMtimeSec())
	assert.Equal(t, beforeAttr.GetMtimeNsec(), response.GetAttr().GetMtimeNsec())
}

func TestRootFSResendIntentDoesNotDoubleReleaseOrphan(t *testing.T) {
	backing := t.TempDir()
	first := newRootFSBackedSession(backing)
	require.NoError(t, first.InitError())
	file, err := first.Create(context.Background(), &pb.CreateRequest{
		Parent: s0fs.RootInode, Name: "open", Mode: 0o600, Flags: uint32(os.O_RDWR),
	})
	require.NoError(t, err)
	_, err = first.Open(context.Background(), &pb.OpenRequest{Inode: file.Inode, Flags: uint32(os.O_RDWR)})
	require.NoError(t, err)
	_, err = first.Unlink(context.Background(), &pb.UnlinkRequest{Parent: s0fs.RootInode, Name: "open"})
	require.NoError(t, err)
	require.Equal(t, uint64(2), first.orphans[file.Inode].OpenCount)

	first.afterMutationApply = func(operation rootFSMutationOperation) {
		if operation == rootFSMutationRelease {
			panic("crash after release")
		}
	}
	request := &pb.ReleaseRequest{Inode: file.Inode, HandleId: file.HandleId}
	requireRootFSMutationPanic(t, func() {
		_, _ = first.Release(rootFSRequestContext("release", 350, false, nil), request)
	})
	require.Equal(t, uint64(1), first.orphans[file.Inode].OpenCount)
	first.Close()

	second := newRootFSBackedSession(backing)
	require.NoError(t, second.InitError())
	defer second.Close()
	var token volumefuse.RequestCompletionToken
	_, err = second.Release(rootFSRequestContext("release", 350, true, &token), request)
	require.NoError(t, err)
	requireRootFSCompletion(t, token)
	orphan, ok := second.orphans[file.Inode]
	require.True(t, ok, "resent RELEASE removed the orphan a second time")
	assert.Equal(t, uint64(1), orphan.OpenCount)
	require.FileExists(t, second.hostPath(rootFSOrphanPath(file.Inode)))
}

func TestRootFSResendLedgerFailsClosedUntilAcknowledged(t *testing.T) {
	backing := t.TempDir()
	session := newRootFSBackedSession(backing)
	require.NoError(t, session.InitError())
	defer session.Close()
	tokens := make([]volumefuse.RequestCompletionToken, rootFSResendLedgerCapacity)
	for i := range rootFSResendLedgerCapacity {
		_, err := session.Mkdir(rootFSRequestContext("capacity", uint64(i+1), false, &tokens[i]), &pb.MkdirRequest{
			Parent: s0fs.RootInode, Name: fmt.Sprintf("d-%03d", i), Mode: 0o700,
		})
		require.NoError(t, err)
		require.NotNil(t, tokens[i])
	}
	_, err := session.Mkdir(rootFSRequestContext("capacity", 1000, false, nil), &pb.MkdirRequest{
		Parent: s0fs.RootInode, Name: "full", Mode: 0o700,
	})
	require.Error(t, err)
	assert.Equal(t, fserror.ResourceExhausted, fserror.CodeOf(err))
	require.NoDirExists(t, filepath.Join(backing, "full"))

	tokens[0].RequestAcknowledged()
	var reused volumefuse.RequestCompletionToken
	_, err = session.Mkdir(rootFSRequestContext("capacity", 1001, false, &reused), &pb.MkdirRequest{
		Parent: s0fs.RootInode, Name: "reused", Mode: 0o700,
	})
	require.NoError(t, err)
	requireRootFSCompletion(t, reused)
}

func TestRootFSResendSurvivesProcessExit(t *testing.T) {
	for _, crashPoint := range []string{"intent", "applied"} {
		t.Run(crashPoint, func(t *testing.T) {
			backing := t.TempDir()
			require.NoError(t, os.WriteFile(filepath.Join(backing, "before"), []byte("payload"), 0o600))
			command := exec.Command(os.Args[0], "-test.run=^TestRootFSResendCrashHelper$")
			command.Env = append(os.Environ(),
				"SANDBOX0_ROOTFS_RESEND_HELPER=1",
				"SANDBOX0_ROOTFS_RESEND_ROOT="+backing,
				"SANDBOX0_ROOTFS_RESEND_CRASH_POINT="+crashPoint,
			)
			err := command.Run()
			var exitErr *exec.ExitError
			require.ErrorAs(t, err, &exitErr)
			assert.Equal(t, rootFSResendCrashExitCode, exitErr.ExitCode())
			require.NoFileExists(t, filepath.Join(backing, "before"))
			require.FileExists(t, filepath.Join(backing, "after"))

			session := newRootFSBackedSession(backing)
			require.NoError(t, session.InitError())
			var token volumefuse.RequestCompletionToken
			_, err = session.Rename(rootFSRequestContext("process-exit", 404, true, &token), &pb.RenameRequest{
				OldParent: s0fs.RootInode, OldName: "before", NewParent: s0fs.RootInode, NewName: "after",
			})
			require.NoError(t, err)
			requireRootFSCompletion(t, token)
			session.Close()
			assertFileContentForPortalTest(t, filepath.Join(backing, "after"), "payload")
		})
	}
}

func TestRootFSResendCrashHelper(t *testing.T) {
	if os.Getenv("SANDBOX0_ROOTFS_RESEND_HELPER") != "1" {
		t.Skip("subprocess helper")
	}
	root := os.Getenv("SANDBOX0_ROOTFS_RESEND_ROOT")
	session := newRootFSBackedSession(root)
	if err := session.InitError(); err != nil {
		t.Fatal(err)
	}
	crash := func(operation rootFSMutationOperation) {
		if operation == rootFSMutationRename {
			os.Exit(rootFSResendCrashExitCode)
		}
	}
	switch os.Getenv("SANDBOX0_ROOTFS_RESEND_CRASH_POINT") {
	case "intent":
		session.afterMutationApply = crash
	case "applied":
		session.afterMutationCommit = crash
	default:
		t.Fatal("invalid crash point")
	}
	_, err := session.Rename(rootFSRequestContext("process-exit", 404, false, nil), &pb.RenameRequest{
		OldParent: s0fs.RootInode, OldName: "before", NewParent: s0fs.RootInode, NewName: "after",
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Fatal("crash hook did not exit")
}

func TestRootFSKernelFUSEAcknowledgesLedgerSlots(t *testing.T) {
	backing := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(backing, "ack"), make([]byte, 4096), 0o600))
	session := newRootFSBackedSession(backing)
	require.NoError(t, session.InitError())
	defer session.Close()
	filesystem := volumefuse.NewWithRequestScope("ack-test", "ack-test-scope", 0, session)
	mountpoint := t.TempDir()
	server, err := fuse.NewServer(filesystem, mountpoint, &fuse.MountOptions{
		DirectMount: true,
		FsName:      "sandbox0-rootfs-ack-test",
		Name:        "sandbox0-rootfs-ack-test",
	})
	if err != nil {
		t.Skipf("mount FUSE acknowledgement test: %v", err)
	}
	go server.Serve()
	require.NoError(t, server.WaitMount())
	defer func() { require.NoError(t, server.Unmount()) }()
	file, err := os.OpenFile(filepath.Join(mountpoint, "ack"), os.O_WRONLY, 0)
	require.NoError(t, err)
	defer file.Close()
	data := make([]byte, 4096)
	for i := 0; i < rootFSResendLedgerCapacity*3; i++ {
		_, err := file.WriteAt(data, 0)
		require.NoError(t, err, "write %d exceeded acknowledged ledger capacity", i)
	}
	session.resendLedger.mu.Lock()
	defer session.resendLedger.mu.Unlock()
	for i := range session.resendLedger.records {
		state := rootFSResendState(atomic.LoadUint32(&session.resendLedger.records[i].State))
		assert.NotEqual(t, rootFSResendIntent, state, "slot %d remains Intent", i)
		assert.NotEqual(t, rootFSResendApplied, state, "slot %d did not receive the kernel reply acknowledgement", i)
	}
}

func TestRootFSFallocateSizeReconciliation(t *testing.T) {
	pre := int64(64 << 10)
	_, post, sensitive := fallocateSizes(pre, unix.FALLOC_FL_COLLAPSE_RANGE, 4096, 4096)
	assert.True(t, sensitive)
	assert.Equal(t, pre-4096, post)
	_, post, sensitive = fallocateSizes(pre, unix.FALLOC_FL_INSERT_RANGE, 4096, 4096)
	assert.True(t, sensitive)
	assert.Equal(t, pre+4096, post)
}

func TestRootFSResendIntentReconcilesFallocateSizeModes(t *testing.T) {
	for i, mode := range []uint32{unix.FALLOC_FL_COLLAPSE_RANGE, unix.FALLOC_FL_INSERT_RANGE} {
		t.Run(fmt.Sprintf("mode=%d", mode), func(t *testing.T) {
			backing := t.TempDir()
			path := filepath.Join(backing, "range")
			data := make([]byte, 4*4096)
			for i := range data {
				data[i] = byte(i)
			}
			require.NoError(t, os.WriteFile(path, data, 0o600))
			first := newRootFSBackedSession(backing)
			require.NoError(t, first.InitError())
			lookup, err := first.Lookup(context.Background(), &pb.LookupRequest{Parent: s0fs.RootInode, Name: "range"})
			require.NoError(t, err)
			request := &pb.FallocateRequest{
				Inode: lookup.Inode, HandleId: lookup.Inode, Mode: mode, Offset: 4096, Length: 4096,
			}
			first.afterMutationApply = func(operation rootFSMutationOperation) {
				if operation == rootFSMutationFallocate {
					panic("crash after fallocate")
				}
			}
			var firstToken volumefuse.RequestCompletionToken
			panicked := false
			var callErr error
			func() {
				defer func() { panicked = recover() != nil }()
				_, callErr = first.Fallocate(rootFSRequestContext("fallocate", uint64(500+i), false, &firstToken), request)
			}()
			if !panicked {
				if firstToken != nil {
					firstToken.RequestAcknowledged()
				}
				first.Close()
				t.Skipf("backing filesystem does not support fallocate mode %d: %v", mode, callErr)
			}
			first.Close()
			beforeSize := int64(len(data))
			_, expectedSize, _ := fallocateSizes(beforeSize, mode, request.GetOffset(), request.GetLength())
			info, err := os.Stat(path)
			require.NoError(t, err)
			require.Equal(t, expectedSize, info.Size())

			second := newRootFSBackedSession(backing)
			require.NoError(t, second.InitError())
			var token volumefuse.RequestCompletionToken
			_, err = second.Fallocate(rootFSRequestContext("fallocate", uint64(500+i), true, &token), request)
			require.NoError(t, err)
			requireRootFSCompletion(t, token)
			second.Close()
			info, err = os.Stat(path)
			require.NoError(t, err)
			assert.Equal(t, expectedSize, info.Size(), "resend applied a size-changing fallocate twice")
		})
	}
}

func BenchmarkRootFSBackedSessionWrite(b *testing.B) {
	for _, durable := range []bool{false, true} {
		b.Run(fmt.Sprintf("durable=%t", durable), func(b *testing.B) {
			backing := b.TempDir()
			session := newRootFSBackedSession(backing)
			if err := session.InitError(); err != nil {
				b.Fatal(err)
			}
			defer session.Close()
			file, err := session.Create(context.Background(), &pb.CreateRequest{
				Parent: s0fs.RootInode, Name: "bench", Mode: 0o600, Flags: uint32(os.O_RDWR),
			})
			if err != nil {
				b.Fatal(err)
			}
			data := make([]byte, 4096)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				ctx := context.Background()
				var token volumefuse.RequestCompletionToken
				if durable {
					ctx = rootFSRequestContext("benchmark", uint64(i+1), false, &token)
				}
				if _, err := session.Write(ctx, &pb.WriteRequest{
					Inode: file.Inode, HandleId: file.HandleId, Offset: 0, Data: data,
				}); err != nil {
					b.Fatal(err)
				}
				if token != nil {
					token.RequestAcknowledged()
				}
			}
		})
	}
}

type rootFSLegacyWriteBenchmarkSession struct {
	*rootFSBackedSession
}

func (s *rootFSLegacyWriteBenchmarkSession) Write(_ context.Context, req *pb.WriteRequest) (*pb.WriteResponse, error) {
	return s.writeOnce(req)
}

func BenchmarkRootFSFileSystemWrite(b *testing.B) {
	for _, durable := range []bool{false, true} {
		b.Run(fmt.Sprintf("durable=%t", durable), func(b *testing.B) {
			backing := b.TempDir()
			session := newRootFSBackedSession(backing)
			if err := session.InitError(); err != nil {
				b.Fatal(err)
			}
			defer session.Close()
			file, err := session.Create(context.Background(), &pb.CreateRequest{
				Parent: s0fs.RootInode, Name: "bench", Mode: 0o600, Flags: uint32(os.O_RDWR),
			})
			if err != nil {
				b.Fatal(err)
			}
			var backend volumefuse.Session = session
			if !durable {
				backend = &rootFSLegacyWriteBenchmarkSession{rootFSBackedSession: session}
			}
			filesystem := volumefuse.NewWithRequestScope("benchmark", "benchmark-scope", 0, backend)
			input := fuse.WriteIn{Fh: file.HandleId}
			input.NodeId = file.Inode
			data := make([]byte, 4096)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				input.Unique = uint64(i + 1)
				written, status := filesystem.Write(nil, &input, data)
				if status != fuse.OK || written != uint32(len(data)) {
					b.Fatalf("Write() = (%d, %v)", written, status)
				}
				filesystem.RequestAcknowledged(&input.InHeader)
			}
		})
	}
}

func BenchmarkRootFSKernelFUSEWrite(b *testing.B) {
	for _, durable := range []bool{false, true} {
		b.Run(fmt.Sprintf("durable=%t", durable), func(b *testing.B) {
			backing := b.TempDir()
			if err := os.WriteFile(filepath.Join(backing, "bench"), make([]byte, 4096), 0o600); err != nil {
				b.Fatal(err)
			}
			session := newRootFSBackedSession(backing)
			if err := session.InitError(); err != nil {
				b.Fatal(err)
			}
			defer session.Close()
			var backend volumefuse.Session = session
			if !durable {
				backend = &rootFSLegacyWriteBenchmarkSession{rootFSBackedSession: session}
			}
			filesystem := volumefuse.NewWithRequestScope("benchmark", "kernel-benchmark", 0, backend)
			mountpoint := b.TempDir()
			server, err := fuse.NewServer(filesystem, mountpoint, &fuse.MountOptions{
				DirectMount: true,
				FsName:      "sandbox0-rootfs-benchmark",
				Name:        "sandbox0-rootfs-benchmark",
			})
			if err != nil {
				b.Skipf("mount FUSE benchmark: %v", err)
			}
			go server.Serve()
			if err := server.WaitMount(); err != nil {
				b.Fatal(err)
			}
			defer func() { _ = server.Unmount() }()
			file, err := os.OpenFile(filepath.Join(mountpoint, "bench"), os.O_WRONLY, 0)
			if err != nil {
				b.Fatal(err)
			}
			defer file.Close()
			data := make([]byte, 4096)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := file.WriteAt(data, 0); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
