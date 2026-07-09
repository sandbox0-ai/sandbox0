package portal

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sort"
	"strings"
	"syscall"
	"testing"

	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/fserror"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/objectstore"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/volume"
	pb "github.com/sandbox0-ai/sandbox0/storage-proxy/proto/fs"
)

func TestS3SessionProjectsObjectsAsDirectoriesAndFiles(t *testing.T) {
	ctx := context.Background()
	store := objectstore.NewMemoryStore(t.Name())
	putS3TestObject(t, store, "docs/readme.txt", "hello from s3")
	putS3TestObject(t, store, "blue", "file-shadowed-by-directory")
	putS3TestObject(t, store, "blue/nested.txt", "nested")
	session := newS3Session("vol-s3", store, volume.AccessModeRWO, nil)

	docs, err := session.Lookup(ctx, &pb.LookupRequest{Parent: s3RootInode, Name: "docs"})
	if err != nil {
		t.Fatalf("Lookup(docs) error = %v", err)
	}
	if docs.Attr.Mode&uint32(syscall.S_IFMT) != uint32(syscall.S_IFDIR) {
		t.Fatalf("docs mode = %#o, want directory", docs.Attr.Mode)
	}

	readme, err := session.Lookup(ctx, &pb.LookupRequest{Parent: docs.Inode, Name: "readme.txt"})
	if err != nil {
		t.Fatalf("Lookup(readme.txt) error = %v", err)
	}
	openResp, err := session.Open(ctx, &pb.OpenRequest{Inode: readme.Inode})
	if err != nil {
		t.Fatalf("Open(readme.txt) error = %v", err)
	}
	readResp, err := session.Read(ctx, &pb.ReadRequest{
		Inode:    readme.Inode,
		HandleId: openResp.HandleId,
		Offset:   0,
		Size:     64,
	})
	if err != nil {
		t.Fatalf("Read(readme.txt) error = %v", err)
	}
	if string(readResp.Data) != "hello from s3" || !readResp.Eof {
		t.Fatalf("Read(readme.txt) = %q eof=%v, want payload and eof", string(readResp.Data), readResp.Eof)
	}

	blue, err := session.Lookup(ctx, &pb.LookupRequest{Parent: s3RootInode, Name: "blue"})
	if err != nil {
		t.Fatalf("Lookup(blue) error = %v", err)
	}
	if blue.Attr.Mode&uint32(syscall.S_IFMT) != uint32(syscall.S_IFDIR) {
		t.Fatalf("blue mode = %#o, want directory because directory prefixes shadow files", blue.Attr.Mode)
	}
}

func TestS3SessionReadDirShadowsFileWhenBackendOmitsCommonPrefix(t *testing.T) {
	ctx := context.Background()
	base := objectstore.NewMemoryStore(t.Name())
	putS3TestObject(t, base, "blue", "file-shadowed-by-directory")
	putS3TestObject(t, base, "blue/image.jpg", "nested")
	store := commonPrefixBlindStore{Store: base}
	session := newS3Session("vol-s3", store, volume.AccessModeRWO, nil)

	entriesResp, err := session.ReadDir(ctx, &pb.ReadDirRequest{Inode: s3RootInode, Plus: true})
	if err != nil {
		t.Fatalf("ReadDir(root) error = %v", err)
	}
	got := make([]string, 0, len(entriesResp.Entries))
	for _, entry := range entriesResp.Entries {
		kind := "file"
		if entry.Type&uint32(syscall.S_IFMT) == uint32(syscall.S_IFDIR) {
			kind = "dir"
		}
		got = append(got, kind+":"+entry.Name)
	}
	sort.Strings(got)
	want := []string{"dir:blue"}
	if !equalPortalStringSlices(got, want) {
		t.Fatalf("ReadDir(root) = %#v, want %#v", got, want)
	}
}

func TestS3SessionSeesExternalObjectsAndWritesBackNewFiles(t *testing.T) {
	ctx := context.Background()
	store := objectstore.NewMemoryStore(t.Name())
	session := newS3Session("vol-s3", store, volume.AccessModeRWO, nil)

	putS3TestObject(t, store, "external/created-before-lookup.txt", "external")
	externalDir, err := session.Lookup(ctx, &pb.LookupRequest{Parent: s3RootInode, Name: "external"})
	if err != nil {
		t.Fatalf("Lookup(external) error = %v", err)
	}
	externalFile, err := session.Lookup(ctx, &pb.LookupRequest{Parent: externalDir.Inode, Name: "created-before-lookup.txt"})
	if err != nil {
		t.Fatalf("Lookup(created-before-lookup.txt) error = %v", err)
	}
	buf := bytes.Repeat([]byte{0xff}, 16)
	n, eof, err := session.ReadInto(ctx, &pb.ReadRequest{Inode: externalFile.Inode, Size: 8}, buf)
	if err != nil {
		t.Fatalf("ReadInto(external) error = %v", err)
	}
	if !eof || string(buf[:n]) != "external" {
		t.Fatalf("ReadInto(external) = %q eof=%v, want external and eof", string(buf[:n]), eof)
	}
	if !bytes.Equal(buf[n:], bytes.Repeat([]byte{0xff}, len(buf)-n)) {
		t.Fatalf("ReadInto modified bytes past read size: %#v", buf)
	}

	if _, err := session.Mkdir(ctx, &pb.MkdirRequest{Parent: s3RootInode, Name: "from-sandbox"}); err != nil {
		t.Fatalf("Mkdir(from-sandbox) error = %v", err)
	}
	assertS3TestObjectMissing(t, store, "from-sandbox/")
	fromSandbox, err := session.Lookup(ctx, &pb.LookupRequest{Parent: s3RootInode, Name: "from-sandbox"})
	if err != nil {
		t.Fatalf("Lookup(from-sandbox after mkdir) error = %v", err)
	}
	created, err := session.Create(ctx, &pb.CreateRequest{Parent: fromSandbox.Inode, Name: "new.txt"})
	if err != nil {
		t.Fatalf("Create(new.txt) error = %v", err)
	}
	assertS3TestObjectMissing(t, store, "from-sandbox/new.txt")
	if _, err := session.Write(ctx, &pb.WriteRequest{HandleId: created.HandleId, Offset: 0, Data: []byte("first ")}); err != nil {
		t.Fatalf("Write(first) error = %v", err)
	}
	if _, err := session.Write(ctx, &pb.WriteRequest{HandleId: created.HandleId, Offset: 6, Data: []byte("second")}); err != nil {
		t.Fatalf("Write(second) error = %v", err)
	}
	assertS3TestObjectMissing(t, store, "from-sandbox/new.txt")
	if _, err := session.Flush(ctx, &pb.FlushRequest{HandleId: created.HandleId}); err != nil {
		t.Fatalf("Flush(new.txt) error = %v", err)
	}
	assertS3TestObjectMissing(t, store, "from-sandbox/new.txt")
	lookedUp, err := session.Lookup(ctx, &pb.LookupRequest{Parent: fromSandbox.Inode, Name: "new.txt"})
	if err != nil {
		t.Fatalf("Lookup(uncommitted new.txt) error = %v", err)
	}
	if lookedUp.Inode != created.Inode {
		t.Fatalf("Lookup(uncommitted new.txt) inode = %d, want %d", lookedUp.Inode, created.Inode)
	}
	entriesDuringWrite, err := session.ReadDir(ctx, &pb.ReadDirRequest{Inode: fromSandbox.Inode})
	if err != nil {
		t.Fatalf("ReadDir(from-sandbox during write) error = %v", err)
	}
	if got := portalDirEntryNames(entriesDuringWrite.Entries); !equalPortalStringSlices(got, []string{"new.txt"}) {
		t.Fatalf("ReadDir(from-sandbox during write) = %#v, want new.txt", got)
	}
	if _, err := session.Open(ctx, &pb.OpenRequest{Inode: created.Inode}); !errors.Is(err, syscall.EPERM) {
		t.Fatalf("Open(reader while writer open) error = %v, want EPERM", err)
	}
	if _, err := session.Unlink(ctx, &pb.UnlinkRequest{Parent: fromSandbox.Inode, Name: "new.txt"}); !errors.Is(err, syscall.EPERM) {
		t.Fatalf("Unlink(writer open) error = %v, want EPERM", err)
	}
	if _, err := session.Release(ctx, &pb.ReleaseRequest{HandleId: created.HandleId}); err != nil {
		t.Fatalf("Release(new.txt) error = %v", err)
	}
	assertS3TestObject(t, store, "from-sandbox/new.txt", "first second")

	fsynced, err := session.Create(ctx, &pb.CreateRequest{Parent: fromSandbox.Inode, Name: "fsynced.txt"})
	if err != nil {
		t.Fatalf("Create(fsynced.txt) error = %v", err)
	}
	if _, err := session.Write(ctx, &pb.WriteRequest{HandleId: fsynced.HandleId, Offset: 0, Data: []byte("first")}); err != nil {
		t.Fatalf("Write(fsynced first) error = %v", err)
	}
	if _, err := session.Fsync(ctx, &pb.FsyncRequest{HandleId: fsynced.HandleId}); err != nil {
		t.Fatalf("Fsync(fsynced.txt) error = %v", err)
	}
	assertS3TestObject(t, store, "from-sandbox/fsynced.txt", "first")
	if _, err := session.Write(ctx, &pb.WriteRequest{HandleId: fsynced.HandleId, Offset: 5, Data: []byte(" second")}); err != nil {
		t.Fatalf("Write(fsynced second) error = %v", err)
	}
	assertS3TestObject(t, store, "from-sandbox/fsynced.txt", "first")
	if _, err := session.Release(ctx, &pb.ReleaseRequest{HandleId: fsynced.HandleId}); err != nil {
		t.Fatalf("Release(fsynced.txt) error = %v", err)
	}
	assertS3TestObject(t, store, "from-sandbox/fsynced.txt", "first second")

	reopened, err := session.Open(ctx, &pb.OpenRequest{
		Inode: created.Inode,
		Flags: uint32(syscall.O_WRONLY | syscall.O_TRUNC),
	})
	if err != nil {
		t.Fatalf("Open(new.txt writable) error = %v", err)
	}
	if _, err := session.SetAttr(ctx, &pb.SetAttrRequest{
		Inode:    created.Inode,
		HandleId: reopened.HandleId,
		Valid:    s3MetadataSetAttrNoopMask,
		Attr:     &pb.GetAttrResponse{Mode: s3FileMode | 0o600},
	}); err != nil {
		t.Fatalf("SetAttr(metadata with writable handle) error = %v", err)
	}
	if _, err := session.SetAttr(ctx, &pb.SetAttrRequest{
		Inode:    created.Inode,
		HandleId: reopened.HandleId,
		Valid:    fuseFattrSize | fuseFattrNoopMask | s3MetadataSetAttrNoopMask,
		Attr:     &pb.GetAttrResponse{Size: 0, Mode: s3FileMode},
	}); err != nil {
		t.Fatalf("SetAttr(truncate with writable handle) error = %v", err)
	}
	assertS3TestObject(t, store, "from-sandbox/new.txt", "first second")
	if _, err := session.Write(ctx, &pb.WriteRequest{HandleId: reopened.HandleId, Offset: 0, Data: []byte("replacement")}); err != nil {
		t.Fatalf("Write(replacement) error = %v", err)
	}
	if _, err := session.Release(ctx, &pb.ReleaseRequest{HandleId: reopened.HandleId}); err != nil {
		t.Fatalf("Release(replacement) error = %v", err)
	}
	assertS3TestObject(t, store, "from-sandbox/new.txt", "replacement")

	failedAppend, err := session.Open(ctx, &pb.OpenRequest{
		Inode: created.Inode,
		Flags: uint32(syscall.O_WRONLY),
	})
	if err != nil {
		t.Fatalf("Open(new.txt failed append simulation) error = %v", err)
	}
	if _, err := session.Write(ctx, &pb.WriteRequest{
		Inode:    created.Inode,
		HandleId: failedAppend.HandleId,
		Offset:   int64(len("replacement")),
		Data:     []byte("append"),
	}); fserror.CodeOf(err) != fserror.InvalidArgument {
		t.Fatalf("Write(failed append simulation) error = %v, want InvalidArgument", err)
	}
	afterFailedAppend, err := session.GetAttr(ctx, &pb.GetAttrRequest{Inode: created.Inode})
	if err != nil {
		t.Fatalf("GetAttr(new.txt after failed append simulation) error = %v", err)
	}
	if afterFailedAppend.Size != uint64(len("replacement")) {
		t.Fatalf("new.txt size after failed append simulation = %d, want %d", afterFailedAppend.Size, len("replacement"))
	}
	assertS3TestObject(t, store, "from-sandbox/new.txt", "replacement")

	_, err = session.Open(ctx, &pb.OpenRequest{
		Inode: created.Inode,
		Flags: uint32(syscall.O_WRONLY | syscall.O_APPEND),
	})
	if fserror.CodeOf(err) != fserror.InvalidArgument {
		t.Fatalf("Open(new.txt O_APPEND) error = %v, want InvalidArgument", err)
	}
	afterAppendOpen, err := session.GetAttr(ctx, &pb.GetAttrRequest{Inode: created.Inode})
	if err != nil {
		t.Fatalf("GetAttr(new.txt after O_APPEND) error = %v", err)
	}
	if afterAppendOpen.Size != uint64(len("replacement")) {
		t.Fatalf("new.txt size after O_APPEND = %d, want %d", afterAppendOpen.Size, len("replacement"))
	}
	assertS3TestObject(t, store, "from-sandbox/new.txt", "replacement")

	_, err = session.Create(ctx, &pb.CreateRequest{
		Parent: fromSandbox.Inode,
		Name:   "new.txt",
		Flags:  uint32(syscall.O_WRONLY | syscall.O_CREAT | syscall.O_APPEND),
	})
	if fserror.CodeOf(err) != fserror.InvalidArgument {
		t.Fatalf("Create(existing new.txt O_APPEND) error = %v, want InvalidArgument", err)
	}
	_, err = session.Create(ctx, &pb.CreateRequest{
		Parent: fromSandbox.Inode,
		Name:   "append-new.txt",
		Flags:  uint32(syscall.O_WRONLY | syscall.O_CREAT | syscall.O_APPEND),
	})
	if fserror.CodeOf(err) != fserror.InvalidArgument {
		t.Fatalf("Create(new append-new.txt O_APPEND) error = %v, want InvalidArgument", err)
	}
	assertS3TestObject(t, store, "from-sandbox/new.txt", "replacement")

	blockingReader, err := session.Open(ctx, &pb.OpenRequest{Inode: created.Inode})
	if err != nil {
		t.Fatalf("Open(blocking reader) error = %v", err)
	}
	if _, err := session.Read(ctx, &pb.ReadRequest{
		Inode:    created.Inode,
		HandleId: blockingReader.HandleId,
		Size:     1,
	}); err != nil {
		t.Fatalf("Read(blocking reader) error = %v", err)
	}
	_, err = session.Open(ctx, &pb.OpenRequest{
		Inode: created.Inode,
		Flags: uint32(syscall.O_WRONLY | syscall.O_TRUNC),
	})
	if !errors.Is(err, syscall.EPERM) {
		t.Fatalf("Open(writer while reader open) error = %v, want EPERM", err)
	}
	if _, err := session.Release(ctx, &pb.ReleaseRequest{HandleId: blockingReader.HandleId}); err != nil {
		t.Fatalf("Release(blocking reader) error = %v", err)
	}
	assertS3TestObject(t, store, "from-sandbox/new.txt", "replacement")

	stalePathReader := session.newHandle(&s3Handle{
		inode: created.Inode,
		path:  "from-sandbox/stale-reader.txt",
		actor: &pb.PosixActor{Pid: 300, Uid: 1000, Gids: []uint32{1000}},
	})
	writerAfterStalePath, err := session.Open(ctx, &pb.OpenRequest{
		Inode: created.Inode,
		Flags: uint32(syscall.O_WRONLY | syscall.O_TRUNC),
		Actor: &pb.PosixActor{Pid: 400, Uid: 1000, Gids: []uint32{1000}},
	})
	if err != nil {
		t.Fatalf("Open(writer with stale different-path reader) error = %v", err)
	}
	if _, err := session.Release(ctx, &pb.ReleaseRequest{HandleId: writerAfterStalePath.HandleId}); err != nil {
		t.Fatalf("Release(writer with stale different-path reader) error = %v", err)
	}
	if _, err := session.Release(ctx, &pb.ReleaseRequest{HandleId: stalePathReader}); err != nil {
		t.Fatalf("Release(stale different-path reader) error = %v", err)
	}
	assertS3TestObject(t, store, "from-sandbox/new.txt", "")

	putS3TestObject(t, store, "from-sandbox/actor-truncate.txt", "actor-base")
	actorNode, err := session.Lookup(ctx, &pb.LookupRequest{Parent: fromSandbox.Inode, Name: "actor-truncate.txt"})
	if err != nil {
		t.Fatalf("Lookup(actor-truncate.txt) error = %v", err)
	}
	gvisorActor := &pb.PosixActor{Pid: 42, Uid: 1000, Gids: []uint32{1000}}
	actorReader, err := session.Open(ctx, &pb.OpenRequest{Inode: actorNode.Inode, Actor: gvisorActor})
	if err != nil {
		t.Fatalf("Open(actor reader) error = %v", err)
	}
	actorWriter, err := session.Open(ctx, &pb.OpenRequest{
		Inode: actorNode.Inode,
		Flags: uint32(syscall.O_WRONLY | syscall.O_TRUNC),
		Actor: gvisorActor,
	})
	if err != nil {
		t.Fatalf("Open(same actor writer while reader open) error = %v", err)
	}
	duplicateActorWriter, err := session.Open(ctx, &pb.OpenRequest{
		Inode: actorNode.Inode,
		Flags: uint32(syscall.O_WRONLY | syscall.O_TRUNC),
		Actor: gvisorActor,
	})
	if err != nil {
		t.Fatalf("Open(same actor duplicate writer) error = %v", err)
	}
	if duplicateActorWriter.HandleId != actorWriter.HandleId {
		t.Fatalf("Open(same actor duplicate writer) handle = %d, want %d", duplicateActorWriter.HandleId, actorWriter.HandleId)
	}
	if _, err := session.Release(ctx, &pb.ReleaseRequest{HandleId: actorWriter.HandleId}); err != nil {
		t.Fatalf("Release(same actor writer) error = %v", err)
	}
	if _, err := session.Release(ctx, &pb.ReleaseRequest{HandleId: duplicateActorWriter.HandleId}); err != nil {
		t.Fatalf("Release(same actor duplicate writer) error = %v", err)
	}
	if _, err := session.Release(ctx, &pb.ReleaseRequest{HandleId: actorReader.HandleId}); err != nil {
		t.Fatalf("Release(actor reader) error = %v", err)
	}
	assertS3TestObject(t, store, "from-sandbox/actor-truncate.txt", "")

	putS3TestObject(t, store, "from-sandbox/actor-read-blocked.txt", "actor-read-blocked")
	readBlockedNode, err := session.Lookup(ctx, &pb.LookupRequest{Parent: fromSandbox.Inode, Name: "actor-read-blocked.txt"})
	if err != nil {
		t.Fatalf("Lookup(actor-read-blocked.txt) error = %v", err)
	}
	readBlockedReader, err := session.Open(ctx, &pb.OpenRequest{Inode: readBlockedNode.Inode, Actor: gvisorActor})
	if err != nil {
		t.Fatalf("Open(same actor active reader) error = %v", err)
	}
	if _, err := session.Read(ctx, &pb.ReadRequest{
		Inode:    readBlockedNode.Inode,
		HandleId: readBlockedReader.HandleId,
		Size:     1,
		Actor:    gvisorActor,
	}); err != nil {
		t.Fatalf("Read(same actor active reader) error = %v", err)
	}
	if _, err := session.Open(ctx, &pb.OpenRequest{
		Inode: readBlockedNode.Inode,
		Flags: uint32(syscall.O_WRONLY | syscall.O_TRUNC),
		Actor: gvisorActor,
	}); !errors.Is(err, syscall.EPERM) {
		t.Fatalf("Open(same actor writer while active reader open) error = %v, want EPERM", err)
	}
	if _, err := session.Release(ctx, &pb.ReleaseRequest{HandleId: readBlockedReader.HandleId}); err != nil {
		t.Fatalf("Release(same actor active reader) error = %v", err)
	}
	assertS3TestObject(t, store, "from-sandbox/actor-read-blocked.txt", "actor-read-blocked")

	putS3TestObject(t, store, "from-sandbox/actor-blocked.txt", "actor-blocked")
	blockedNode, err := session.Lookup(ctx, &pb.LookupRequest{Parent: fromSandbox.Inode, Name: "actor-blocked.txt"})
	if err != nil {
		t.Fatalf("Lookup(actor-blocked.txt) error = %v", err)
	}
	otherReader, err := session.Open(ctx, &pb.OpenRequest{
		Inode: blockedNode.Inode,
		Actor: &pb.PosixActor{Pid: 100, Uid: 1000, Gids: []uint32{1000}},
	})
	if err != nil {
		t.Fatalf("Open(other actor reader) error = %v", err)
	}
	otherWriter, err := session.Open(ctx, &pb.OpenRequest{
		Inode: blockedNode.Inode,
		Flags: uint32(syscall.O_WRONLY | syscall.O_TRUNC),
		Actor: &pb.PosixActor{Pid: 200, Uid: 1000, Gids: []uint32{1000}},
	})
	if err != nil {
		t.Fatalf("Open(different actor writer while unread reader open) error = %v", err)
	}
	if _, err := session.Release(ctx, &pb.ReleaseRequest{HandleId: otherWriter.HandleId}); err != nil {
		t.Fatalf("Release(other actor writer) error = %v", err)
	}
	if _, err := session.Release(ctx, &pb.ReleaseRequest{HandleId: otherReader.HandleId}); err != nil {
		t.Fatalf("Release(other actor reader) error = %v", err)
	}
	assertS3TestObject(t, store, "from-sandbox/actor-blocked.txt", "")

	putS3TestObject(t, store, "from-sandbox/actor-active-blocked.txt", "actor-active-blocked")
	activeBlockedNode, err := session.Lookup(ctx, &pb.LookupRequest{Parent: fromSandbox.Inode, Name: "actor-active-blocked.txt"})
	if err != nil {
		t.Fatalf("Lookup(actor-active-blocked.txt) error = %v", err)
	}
	activeOtherReader, err := session.Open(ctx, &pb.OpenRequest{
		Inode: activeBlockedNode.Inode,
		Actor: &pb.PosixActor{Pid: 300, Uid: 1000, Gids: []uint32{1000}},
	})
	if err != nil {
		t.Fatalf("Open(other actor active reader) error = %v", err)
	}
	if _, err := session.Read(ctx, &pb.ReadRequest{
		Inode:    activeBlockedNode.Inode,
		HandleId: activeOtherReader.HandleId,
		Size:     1,
		Actor:    &pb.PosixActor{Pid: 300, Uid: 1000, Gids: []uint32{1000}},
	}); err != nil {
		t.Fatalf("Read(other actor active reader) error = %v", err)
	}
	if _, err := session.Open(ctx, &pb.OpenRequest{
		Inode: activeBlockedNode.Inode,
		Flags: uint32(syscall.O_WRONLY | syscall.O_TRUNC),
		Actor: &pb.PosixActor{Pid: 400, Uid: 1000, Gids: []uint32{1000}},
	}); !errors.Is(err, syscall.EPERM) {
		t.Fatalf("Open(different actor writer while active reader open) error = %v, want EPERM", err)
	}
	if _, err := session.Release(ctx, &pb.ReleaseRequest{HandleId: activeOtherReader.HandleId}); err != nil {
		t.Fatalf("Release(other actor active reader) error = %v", err)
	}
	assertS3TestObject(t, store, "from-sandbox/actor-active-blocked.txt", "actor-active-blocked")

	if _, err := session.SetAttr(ctx, &pb.SetAttrRequest{
		Inode: created.Inode,
		Valid: fuseFattrSize,
		Attr:  &pb.GetAttrResponse{Size: 0},
	}); err != nil {
		t.Fatalf("SetAttr(truncate) error = %v", err)
	}
	assertS3TestObject(t, store, "from-sandbox/new.txt", "")

	modeNoop, err := session.SetAttr(ctx, &pb.SetAttrRequest{
		Inode: created.Inode,
		Valid: 1,
		Attr:  &pb.GetAttrResponse{Mode: (s3FileMode &^ 0o777) | 0o600},
	})
	if err != nil {
		t.Fatalf("SetAttr(mode no-op) error = %v", err)
	}
	if modeNoop.Attr.Mode != s3FileMode {
		t.Fatalf("SetAttr(mode no-op) mode = %#o, want %#o", modeNoop.Attr.Mode, s3FileMode)
	}
	if _, err := session.SetAttr(ctx, &pb.SetAttrRequest{
		Inode: created.Inode,
		Valid: fuseFattrNoopMask,
		Attr:  &pb.GetAttrResponse{Mode: (s3FileMode &^ 0o777) | 0o600},
	}); err != nil {
		t.Fatalf("SetAttr(mode without valid mode bit) error = %v", err)
	}
	xattrs, err := session.ListXattr(ctx, &pb.ListXattrRequest{Inode: created.Inode})
	if err != nil {
		t.Fatalf("ListXattr() error = %v", err)
	}
	if len(xattrs.GetData()) != 0 {
		t.Fatalf("ListXattr() data = %q, want empty", xattrs.GetData())
	}
	if _, err := session.Rename(ctx, &pb.RenameRequest{
		OldParent: fromSandbox.Inode,
		OldName:   "new.txt",
		NewParent: fromSandbox.Inode,
		NewName:   "renamed.txt",
	}); !errors.Is(err, syscall.EPERM) {
		t.Fatalf("Rename() error = %v, want EPERM", err)
	}
	assertS3TestObject(t, store, "from-sandbox/new.txt", "")
	assertS3TestObjectMissing(t, store, "from-sandbox/renamed.txt")

	if _, err := session.Write(ctx, &pb.WriteRequest{
		Inode:    created.Inode,
		HandleId: 0,
		Offset:   1,
		Data:     []byte("x"),
	}); fserror.CodeOf(err) != fserror.InvalidArgument {
		t.Fatalf("Write(handle-less non-sequential existing object) error = %v, want InvalidArgument", err)
	}
	if _, err := session.Lookup(ctx, &pb.LookupRequest{Parent: fromSandbox.Inode, Name: "new.txt"}); err != nil {
		t.Fatalf("Lookup(new.txt after failed handle-less write) error = %v", err)
	}
	assertS3TestObject(t, store, "from-sandbox/new.txt", "")

	implicitCreated, err := session.Create(ctx, &pb.CreateRequest{Parent: fromSandbox.Inode, Name: "implicit.txt"})
	if err != nil {
		t.Fatalf("Create(implicit.txt) error = %v", err)
	}
	if _, err := session.SetAttr(ctx, &pb.SetAttrRequest{
		Inode: implicitCreated.Inode,
		Valid: fuseFattrSize,
		Attr:  &pb.GetAttrResponse{Size: 0},
	}); err != nil {
		t.Fatalf("SetAttr(implicit truncate) error = %v", err)
	}
	if _, err := session.Write(ctx, &pb.WriteRequest{
		Inode:    implicitCreated.Inode,
		HandleId: 0,
		Offset:   0,
		Data:     []byte("implicit"),
	}); err != nil {
		t.Fatalf("Write(implicit handle) error = %v", err)
	}
	if _, err := session.Release(ctx, &pb.ReleaseRequest{HandleId: implicitCreated.HandleId}); err != nil {
		t.Fatalf("Release(implicit explicit handle) error = %v", err)
	}
	assertS3TestObject(t, store, "from-sandbox/implicit.txt", "implicit")

	nonSequential, err := session.Create(ctx, &pb.CreateRequest{Parent: fromSandbox.Inode, Name: "non-sequential.txt"})
	if err != nil {
		t.Fatalf("Create(non-sequential.txt) error = %v", err)
	}
	if _, err := session.Write(ctx, &pb.WriteRequest{HandleId: nonSequential.HandleId, Offset: 1, Data: []byte("x")}); fserror.CodeOf(err) != fserror.InvalidArgument {
		t.Fatalf("Write(non-sequential) error = %v, want InvalidArgument", err)
	}
	if _, err := session.Release(ctx, &pb.ReleaseRequest{HandleId: nonSequential.HandleId}); err != nil {
		t.Fatalf("Release(non-sequential.txt) error = %v", err)
	}
	assertS3TestObjectMissing(t, store, "from-sandbox/non-sequential.txt")
	if _, err := session.Lookup(ctx, &pb.LookupRequest{Parent: fromSandbox.Inode, Name: "non-sequential.txt"}); fserror.CodeOf(err) != fserror.NotFound {
		t.Fatalf("Lookup(non-sequential.txt after failed write) error = %v, want NotFound", err)
	}

	handlelessPending, err := session.Mknod(ctx, &pb.MknodRequest{
		Parent: fromSandbox.Inode,
		Name:   "handleless-non-sequential.txt",
		Mode:   0o644,
	})
	if err != nil {
		t.Fatalf("Mknod(handleless-non-sequential.txt) error = %v", err)
	}
	if _, err := session.Write(ctx, &pb.WriteRequest{
		Inode:    handlelessPending.Inode,
		HandleId: 0,
		Offset:   1,
		Data:     []byte("x"),
	}); fserror.CodeOf(err) != fserror.InvalidArgument {
		t.Fatalf("Write(handle-less non-sequential pending file) error = %v, want InvalidArgument", err)
	}
	if _, err := session.Lookup(ctx, &pb.LookupRequest{Parent: fromSandbox.Inode, Name: "handleless-non-sequential.txt"}); fserror.CodeOf(err) != fserror.NotFound {
		t.Fatalf("Lookup(handleless-non-sequential.txt after failed write) error = %v, want NotFound", err)
	}
	assertS3TestObjectMissing(t, store, "from-sandbox/handleless-non-sequential.txt")

	entriesResp, err := session.ReadDir(ctx, &pb.ReadDirRequest{Inode: s3RootInode, Plus: true})
	if err != nil {
		t.Fatalf("ReadDir(root) error = %v", err)
	}
	got := make([]string, 0, len(entriesResp.Entries))
	for _, entry := range entriesResp.Entries {
		kind := "file"
		if entry.Type&uint32(syscall.S_IFMT) == uint32(syscall.S_IFDIR) {
			kind = "dir"
		}
		got = append(got, kind+":"+entry.Name)
	}
	sort.Strings(got)
	want := []string{"dir:external", "dir:from-sandbox"}
	if !equalPortalStringSlices(got, want) {
		t.Fatalf("ReadDir(root) = %#v, want %#v", got, want)
	}
}

func TestS3SessionMkdirUsesMountpointLocalDirectorySemantics(t *testing.T) {
	ctx := context.Background()
	store := objectstore.NewMemoryStore(t.Name())
	putS3TestObject(t, store, "marker/", "")
	putS3TestObject(t, store, "implicit/file.txt", "payload")
	session := newS3Session("vol-s3", store, volume.AccessModeRWO, nil)

	local, err := session.Mkdir(ctx, &pb.MkdirRequest{Parent: s3RootInode, Name: "local"})
	if err != nil {
		t.Fatalf("Mkdir(local) error = %v", err)
	}
	assertS3TestObjectMissing(t, store, "local/")
	if _, err := session.Lookup(ctx, &pb.LookupRequest{Parent: s3RootInode, Name: "local"}); err != nil {
		t.Fatalf("Lookup(local) error = %v", err)
	}
	if _, err := session.ReadDir(ctx, &pb.ReadDirRequest{Inode: s3RootInode, Plus: true}); err != nil {
		t.Fatalf("ReadDir(root after local mkdir) error = %v", err)
	}
	if _, err := session.Rmdir(ctx, &pb.RmdirRequest{Parent: s3RootInode, Name: "local"}); err != nil {
		t.Fatalf("Rmdir(local) error = %v", err)
	}
	if _, err := session.Lookup(ctx, &pb.LookupRequest{Parent: s3RootInode, Name: "local"}); fserror.CodeOf(err) != fserror.NotFound {
		t.Fatalf("Lookup(local after rmdir) error = %v, want NotFound", err)
	}

	mknodLocal, err := session.Mknod(ctx, &pb.MknodRequest{
		Parent: s3RootInode,
		Name:   "mknod-local",
		Mode:   uint32(syscall.S_IFDIR | 0o755),
	})
	if err != nil {
		t.Fatalf("Mknod(mknod-local dir) error = %v", err)
	}
	if mknodLocal.Attr.Mode&uint32(syscall.S_IFMT) != uint32(syscall.S_IFDIR) {
		t.Fatalf("mknod-local mode = %#o, want directory", mknodLocal.Attr.Mode)
	}
	assertS3TestObjectMissing(t, store, "mknod-local/")
	if _, err := session.Lookup(ctx, &pb.LookupRequest{Parent: s3RootInode, Name: "mknod-local"}); err != nil {
		t.Fatalf("Lookup(mknod-local) error = %v", err)
	}
	if _, err := session.SetAttr(ctx, &pb.SetAttrRequest{
		Inode: mknodLocal.Inode,
		Valid: s3MetadataSetAttrNoopMask,
		Attr:  &pb.GetAttrResponse{Mode: uint32(syscall.S_IFDIR | 0o700)},
	}); err != nil {
		t.Fatalf("SetAttr(mknod-local mode no-op) error = %v", err)
	}
	mknodFile, err := session.Mknod(ctx, &pb.MknodRequest{
		Parent: s3RootInode,
		Name:   "mknod-file-no-type",
		Mode:   0o755,
	})
	if err != nil {
		t.Fatalf("Mknod(mknod-file-no-type) error = %v", err)
	}
	if mknodFile.Attr.Mode&uint32(syscall.S_IFMT) != uint32(syscall.S_IFREG) {
		t.Fatalf("mknod-file-no-type mode = %#o, want file", mknodFile.Attr.Mode)
	}
	assertS3TestObjectMissing(t, store, "mknod-file-no-type")
	putS3TestObject(t, store, "mknod-file-no-type", "external overwrite")
	promoted, err := session.Lookup(ctx, &pb.LookupRequest{Parent: s3RootInode, Name: "mknod-file-no-type"})
	if err != nil {
		t.Fatalf("Lookup(mknod-file-no-type promoted from S3) error = %v", err)
	}
	if promoted.Attr.Size != uint64(len("external overwrite")) {
		t.Fatalf("promoted mknod-file-no-type size = %d, want %d", promoted.Attr.Size, len("external overwrite"))
	}
	if _, err := session.Unlink(ctx, &pb.UnlinkRequest{Parent: s3RootInode, Name: "mknod-file-no-type"}); err != nil {
		t.Fatalf("Unlink(promoted mknod-file-no-type) error = %v", err)
	}
	mknodFile, err = session.Mknod(ctx, &pb.MknodRequest{
		Parent: s3RootInode,
		Name:   "mknod-file-no-type",
		Mode:   0o755,
	})
	if err != nil {
		t.Fatalf("Mknod(mknod-file-no-type after promoted unlink) error = %v", err)
	}
	mknodOpen, err := session.Open(ctx, &pb.OpenRequest{Inode: mknodFile.Inode, Flags: uint32(syscall.O_WRONLY)})
	if err != nil {
		t.Fatalf("Open(mknod-file-no-type) error = %v", err)
	}
	if _, err := session.Write(ctx, &pb.WriteRequest{
		Inode:    mknodFile.Inode,
		HandleId: mknodOpen.HandleId,
		Data:     []byte("mknod payload"),
	}); err != nil {
		t.Fatalf("Write(mknod-file-no-type) error = %v", err)
	}
	assertS3TestObjectMissing(t, store, "mknod-file-no-type")
	if _, err := session.Release(ctx, &pb.ReleaseRequest{Inode: mknodFile.Inode, HandleId: mknodOpen.HandleId}); err != nil {
		t.Fatalf("Release(mknod-file-no-type) error = %v", err)
	}
	assertS3TestObject(t, store, "mknod-file-no-type", "mknod payload")
	if _, err := session.Mknod(ctx, &pb.MknodRequest{
		Parent: s3RootInode,
		Name:   "special-node",
		Mode:   uint32(syscall.S_IFCHR | 0o600),
	}); !errors.Is(err, syscall.EOPNOTSUPP) {
		t.Fatalf("Mknod(special-node) error = %v, want EOPNOTSUPP", err)
	}

	nested, err := session.Mkdir(ctx, &pb.MkdirRequest{Parent: s3RootInode, Name: "nested"})
	if err != nil {
		t.Fatalf("Mkdir(nested) error = %v", err)
	}
	if _, err := session.Mkdir(ctx, &pb.MkdirRequest{Parent: nested.Inode, Name: "child"}); err != nil {
		t.Fatalf("Mkdir(nested/child) error = %v", err)
	}
	if _, err := session.Rmdir(ctx, &pb.RmdirRequest{Parent: s3RootInode, Name: "nested"}); !errors.Is(err, syscall.ENOTEMPTY) {
		t.Fatalf("Rmdir(nested) error = %v, want ENOTEMPTY", err)
	}

	if _, err := session.Rmdir(ctx, &pb.RmdirRequest{Parent: s3RootInode, Name: "marker"}); !errors.Is(err, syscall.ENOTEMPTY) {
		t.Fatalf("Rmdir(marker) error = %v, want ENOTEMPTY", err)
	}
	if _, err := session.Rmdir(ctx, &pb.RmdirRequest{Parent: s3RootInode, Name: "implicit"}); !errors.Is(err, syscall.ENOTEMPTY) {
		t.Fatalf("Rmdir(implicit) error = %v, want ENOTEMPTY", err)
	}
	if _, err := session.Create(ctx, &pb.CreateRequest{Parent: local.Inode, Name: "after-rmdir.txt"}); fserror.CodeOf(err) != fserror.NotFound {
		t.Fatalf("Create(using removed local inode) error = %v, want NotFound", err)
	}
}

func TestS3SessionHidesObjectKeysWithEmptyPathSegments(t *testing.T) {
	ctx := context.Background()
	store := objectstore.NewMemoryStore(t.Name())
	putS3TestObject(t, store, "invalid//hidden.txt", "hidden")
	putS3TestObject(t, store, "dots/./hidden.txt", "hidden")
	putS3TestObject(t, store, "parents/../hidden.txt", "hidden")
	session := newS3Session("vol-s3", store, volume.AccessModeRWO, nil)

	for _, name := range []string{"invalid", "dots", "parents"} {
		dir, err := session.Lookup(ctx, &pb.LookupRequest{Parent: s3RootInode, Name: name})
		if err != nil {
			t.Fatalf("Lookup(%s) error = %v", name, err)
		}
		entries, err := session.ReadDir(ctx, &pb.ReadDirRequest{Inode: dir.Inode, Plus: true})
		if err != nil {
			t.Fatalf("ReadDir(%s) error = %v", name, err)
		}
		if len(entries.Entries) != 0 {
			t.Fatalf("ReadDir(%s) entries = %#v, want none for invalid path segment object", name, entries.Entries)
		}
		if _, err := session.Lookup(ctx, &pb.LookupRequest{Parent: dir.Inode, Name: "hidden.txt"}); fserror.CodeOf(err) != fserror.NotFound {
			t.Fatalf("Lookup(%s/hidden.txt) error = %v, want NotFound", name, err)
		}
	}
}

func TestS3SessionDirectoryLookupFallsBackToListWhenMarkerHeadFails(t *testing.T) {
	ctx := context.Background()
	base := objectstore.NewMemoryStore(t.Name())
	putS3TestObject(t, base, "external/file.txt", "payload")
	store := headErrorForDirectoryMarkersStore{Store: base}
	session := newS3Session("vol-s3", store, volume.AccessModeRWO, nil)

	externalDir, err := session.Lookup(ctx, &pb.LookupRequest{Parent: s3RootInode, Name: "external"})
	if err != nil {
		t.Fatalf("Lookup(external) error = %v", err)
	}
	if externalDir.Attr.Mode&uint32(syscall.S_IFMT) != uint32(syscall.S_IFDIR) {
		t.Fatalf("external mode = %#o, want directory", externalDir.Attr.Mode)
	}
}

func TestS3SessionReadOnlyAccessRejectsWrites(t *testing.T) {
	ctx := context.Background()
	store := objectstore.NewMemoryStore(t.Name())
	session := newS3Session("vol-s3", store, volume.AccessModeROX, nil)

	_, err := session.Create(ctx, &pb.CreateRequest{Parent: s3RootInode, Name: "blocked.txt"})
	if !errors.Is(err, syscall.EROFS) {
		t.Fatalf("Create() error = %v, want EROFS", err)
	}
	_, err = session.Mkdir(ctx, &pb.MkdirRequest{Parent: s3RootInode, Name: "blocked"})
	if !errors.Is(err, syscall.EROFS) {
		t.Fatalf("Mkdir() error = %v, want EROFS", err)
	}
}

type headErrorForDirectoryMarkersStore struct {
	objectstore.Store
}

func (s headErrorForDirectoryMarkersStore) Head(key string) (objectstore.Info, error) {
	if strings.HasSuffix(strings.TrimSpace(key), "/") {
		return objectstore.Info{}, errors.New("bad request")
	}
	return s.Store.Head(key)
}

type commonPrefixBlindStore struct {
	objectstore.Store
}

func (s commonPrefixBlindStore) List(prefix, startAfter, token, delimiter string, limit int64) ([]objectstore.Info, bool, string, error) {
	infos, more, next, err := s.Store.List(prefix, startAfter, token, delimiter, limit)
	if err != nil || delimiter == "" {
		return infos, more, next, err
	}
	filtered := infos[:0]
	for _, info := range infos {
		if !info.IsPrefix {
			filtered = append(filtered, info)
		}
	}
	return filtered, more, next, nil
}

func putS3TestObject(t *testing.T, store objectstore.Store, key, value string) {
	t.Helper()
	if err := store.Put(key, bytes.NewReader([]byte(value))); err != nil {
		t.Fatalf("Put(%q) error = %v", key, err)
	}
}

func assertS3TestObject(t *testing.T, store objectstore.Store, key, want string) {
	t.Helper()
	reader, err := store.Get(key, 0, -1)
	if err != nil {
		t.Fatalf("Get(%q) error = %v", key, err)
	}
	defer reader.Close()
	got, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("ReadAll(%q) error = %v", key, err)
	}
	if string(got) != want {
		t.Fatalf("object %q = %q, want %q", key, string(got), want)
	}
}

func assertS3TestObjectMissing(t *testing.T, store objectstore.Store, key string) {
	t.Helper()
	reader, err := store.Get(key, 0, -1)
	if err == nil {
		_ = reader.Close()
		t.Fatalf("Get(%q) succeeded, want missing object", key)
	}
	if !objectstore.IsNotFound(err) {
		t.Fatalf("Get(%q) error = %v, want not found", key, err)
	}
}

func equalPortalStringSlices(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func portalDirEntryNames(entries []*pb.DirEntry) []string {
	out := make([]string, 0, len(entries))
	for _, entry := range entries {
		out = append(out, entry.Name)
	}
	sort.Strings(out)
	return out
}
