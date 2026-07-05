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
	fromSandbox, err := session.Lookup(ctx, &pb.LookupRequest{Parent: s3RootInode, Name: "from-sandbox"})
	if err != nil {
		t.Fatalf("Lookup(from-sandbox after mkdir) error = %v", err)
	}
	created, err := session.Create(ctx, &pb.CreateRequest{Parent: fromSandbox.Inode, Name: "new.txt"})
	if err != nil {
		t.Fatalf("Create(new.txt) error = %v", err)
	}
	if _, err := session.Write(ctx, &pb.WriteRequest{HandleId: created.HandleId, Offset: 0, Data: []byte("first ")}); err != nil {
		t.Fatalf("Write(first) error = %v", err)
	}
	if _, err := session.Write(ctx, &pb.WriteRequest{HandleId: created.HandleId, Offset: 6, Data: []byte("second")}); err != nil {
		t.Fatalf("Write(second) error = %v", err)
	}
	if _, err := session.Release(ctx, &pb.ReleaseRequest{HandleId: created.HandleId}); err != nil {
		t.Fatalf("Release(new.txt) error = %v", err)
	}
	assertS3TestObject(t, store, "from-sandbox/new.txt", "first second")

	reopened, err := session.Open(ctx, &pb.OpenRequest{
		Inode: created.Inode,
		Flags: uint32(syscall.O_WRONLY | syscall.O_TRUNC),
	})
	if err != nil {
		t.Fatalf("Open(new.txt writable) error = %v", err)
	}
	if _, err := session.Write(ctx, &pb.WriteRequest{HandleId: reopened.HandleId, Offset: 0, Data: []byte("replacement")}); err != nil {
		t.Fatalf("Write(replacement) error = %v", err)
	}
	if _, err := session.Release(ctx, &pb.ReleaseRequest{HandleId: reopened.HandleId}); err != nil {
		t.Fatalf("Release(replacement) error = %v", err)
	}
	assertS3TestObject(t, store, "from-sandbox/new.txt", "replacement")

	if _, err := session.SetAttr(ctx, &pb.SetAttrRequest{
		Inode: created.Inode,
		Valid: fuseFattrSize,
		Attr:  &pb.GetAttrResponse{Size: 0},
	}); err != nil {
		t.Fatalf("SetAttr(truncate) error = %v", err)
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
