package s0fs

import (
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"syscall"
	"testing"
)

func TestEngineSmallFileReadWriteReplay(t *testing.T) {
	walPath := filepath.Join(t.TempDir(), "volume.wal")

	engine, err := Open(context.Background(), Config{VolumeID: "vol-1", WALPath: walPath})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	node, err := engine.CreateFile(RootInode, "hello.txt", 0o644)
	if err != nil {
		t.Fatalf("CreateFile() error = %v", err)
	}
	if _, err := engine.Write(node.Inode, 0, []byte("hello")); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	if err := engine.Fsync(node.Inode); err != nil {
		t.Fatalf("Fsync() error = %v", err)
	}
	if err := engine.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	replayed, err := Open(context.Background(), Config{VolumeID: "vol-1", WALPath: walPath})
	if err != nil {
		t.Fatalf("Open(replay) error = %v", err)
	}
	defer replayed.Close()

	replayedNode, err := replayed.Lookup(RootInode, "hello.txt")
	if err != nil {
		t.Fatalf("Lookup() after replay error = %v", err)
	}
	data, err := replayed.Read(replayedNode.Inode, 0, 1024)
	if err != nil {
		t.Fatalf("Read() after replay error = %v", err)
	}
	if !bytes.Equal(data, []byte("hello")) {
		t.Fatalf("replayed data = %q, want hello", data)
	}
}

func TestEngineLocalDiskGuardRejectsProjectedCacheGrowth(t *testing.T) {
	dir := t.TempDir()
	engine, err := Open(context.Background(), Config{
		VolumeID: "vol-1",
		WALPath:  filepath.Join(dir, "volume.wal"),
		LocalDiskGuard: &LocalDiskGuard{
			Path:     dir,
			MaxBytes: 2048,
		},
	})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer engine.Close()

	node, err := engine.CreateFile(RootInode, "limited.txt", 0o644)
	if err != nil {
		t.Fatalf("CreateFile() error = %v", err)
	}
	if _, err := engine.Write(node.Inode, 0, []byte("ok")); err != nil {
		t.Fatalf("small Write() error = %v", err)
	}
	if _, err := engine.Write(node.Inode, 0, bytes.Repeat([]byte("x"), 2048)); !errors.Is(err, ErrNoSpace) {
		t.Fatalf("large Write() error = %v, want ErrNoSpace", err)
	}
}

func TestEngineWriteExtendsWithZeros(t *testing.T) {
	engine, err := Open(context.Background(), Config{VolumeID: "vol-1", WALPath: filepath.Join(t.TempDir(), "volume.wal")})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer engine.Close()

	node, err := engine.CreateFile(RootInode, "sparse.txt", 0o644)
	if err != nil {
		t.Fatalf("CreateFile() error = %v", err)
	}
	if _, err := engine.Write(node.Inode, 4, []byte("x")); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	data, err := engine.Read(node.Inode, 0, 8)
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}
	if !bytes.Equal(data, []byte{0, 0, 0, 0, 'x'}) {
		t.Fatalf("sparse data = %#v", data)
	}
}

func TestEngineReadIntoSmallFile(t *testing.T) {
	engine, err := Open(context.Background(), Config{VolumeID: "vol-1", WALPath: filepath.Join(t.TempDir(), "volume.wal")})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer engine.Close()

	node, err := engine.CreateFile(RootInode, "data.txt", 0o644)
	if err != nil {
		t.Fatalf("CreateFile() error = %v", err)
	}
	if _, err := engine.Write(node.Inode, 0, []byte("abcdef")); err != nil {
		t.Fatalf("Write() error = %v", err)
	}

	buf := bytes.Repeat([]byte{0xff}, 8)
	n, err := engine.ReadInto(node.Inode, 2, buf[:3])
	if err != nil {
		t.Fatalf("ReadInto() error = %v", err)
	}
	if n != 3 {
		t.Fatalf("ReadInto() n = %d, want 3", n)
	}
	if !bytes.Equal(buf[:3], []byte("cde")) {
		t.Fatalf("ReadInto() data = %q, want cde", buf[:3])
	}
	if !bytes.Equal(buf[3:], bytes.Repeat([]byte{0xff}, 5)) {
		t.Fatalf("ReadInto() modified bytes past destination: %#v", buf)
	}
}

func TestEngineRenameAndUnlinkReplay(t *testing.T) {
	walPath := filepath.Join(t.TempDir(), "volume.wal")
	engine, err := Open(context.Background(), Config{VolumeID: "vol-1", WALPath: walPath})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	node, err := engine.CreateFile(RootInode, "before.txt", 0o644)
	if err != nil {
		t.Fatalf("CreateFile() error = %v", err)
	}
	if _, err := engine.Write(node.Inode, 0, []byte("payload")); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	if err := engine.Rename(RootInode, "before.txt", RootInode, "after.txt"); err != nil {
		t.Fatalf("Rename() error = %v", err)
	}
	if err := engine.Fsync(node.Inode); err != nil {
		t.Fatalf("Fsync() error = %v", err)
	}
	if err := engine.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	replayed, err := Open(context.Background(), Config{VolumeID: "vol-1", WALPath: walPath})
	if err != nil {
		t.Fatalf("Open(replay) error = %v", err)
	}
	replayedNode, err := replayed.Lookup(RootInode, "after.txt")
	if err != nil {
		t.Fatalf("Lookup(after) error = %v", err)
	}
	if data, err := replayed.Read(replayedNode.Inode, 0, 1024); err != nil || !bytes.Equal(data, []byte("payload")) {
		t.Fatalf("Read(after) = %q, %v", data, err)
	}
	if err := replayed.Unlink(RootInode, "after.txt"); err != nil {
		t.Fatalf("Unlink() error = %v", err)
	}
	if err := replayed.Fsync(replayedNode.Inode); err != nil {
		t.Fatalf("Fsync() after unlink error = %v", err)
	}
	if err := replayed.Close(); err != nil {
		t.Fatalf("Close(replayed) error = %v", err)
	}

	replayedAgain, err := Open(context.Background(), Config{VolumeID: "vol-1", WALPath: walPath})
	if err != nil {
		t.Fatalf("Open(replay again) error = %v", err)
	}
	defer replayedAgain.Close()
	if _, err := replayedAgain.Lookup(RootInode, "after.txt"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Lookup(after) err = %v, want ErrNotFound", err)
	}
}

func TestEngineRejectsDuplicateDentry(t *testing.T) {
	engine, err := Open(context.Background(), Config{VolumeID: "vol-1", WALPath: filepath.Join(t.TempDir(), "volume.wal")})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer engine.Close()

	if _, err := engine.CreateFile(RootInode, "dup.txt", 0o644); err != nil {
		t.Fatalf("CreateFile() error = %v", err)
	}
	if _, err := engine.CreateFile(RootInode, "dup.txt", 0o644); !errors.Is(err, ErrExists) {
		t.Fatalf("CreateFile(duplicate) err = %v, want ErrExists", err)
	}
}

func TestEngineLinkReplay(t *testing.T) {
	walPath := filepath.Join(t.TempDir(), "volume.wal")
	engine, err := Open(context.Background(), Config{VolumeID: "vol-1", WALPath: walPath})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	node, err := engine.CreateFile(RootInode, "source.txt", 0o644)
	if err != nil {
		t.Fatalf("CreateFile() error = %v", err)
	}
	if _, err := engine.Write(node.Inode, 0, []byte("payload")); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	linked, err := engine.Link(node.Inode, RootInode, "linked.txt")
	if err != nil {
		t.Fatalf("Link() error = %v", err)
	}
	if linked.Inode != node.Inode || linked.Nlink != 2 {
		t.Fatalf("Link() node = %#v, want same inode with nlink 2", linked)
	}
	if err := engine.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	replayed, err := Open(context.Background(), Config{VolumeID: "vol-1", WALPath: walPath})
	if err != nil {
		t.Fatalf("Open(replay) error = %v", err)
	}
	defer replayed.Close()
	replayedNode, err := replayed.Lookup(RootInode, "linked.txt")
	if err != nil {
		t.Fatalf("Lookup(linked) error = %v", err)
	}
	if replayedNode.Inode != node.Inode || replayedNode.Nlink != 2 {
		t.Fatalf("Lookup(linked) node = %#v, want same inode with nlink 2", replayedNode)
	}
	data, err := replayed.Read(replayedNode.Inode, 0, 1024)
	if err != nil {
		t.Fatalf("Read(linked) error = %v", err)
	}
	if !bytes.Equal(data, []byte("payload")) {
		t.Fatalf("Read(linked) data = %q, want payload", data)
	}
}

func TestEngineDirectoryOperationsReplay(t *testing.T) {
	walPath := filepath.Join(t.TempDir(), "volume.wal")
	engine, err := Open(context.Background(), Config{VolumeID: "vol-1", WALPath: walPath})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}

	dir, err := engine.Mkdir(RootInode, "dir", 0o755)
	if err != nil {
		t.Fatalf("Mkdir() error = %v", err)
	}
	if _, err := engine.CreateFile(dir.Inode, "child.txt", 0o644); err != nil {
		t.Fatalf("CreateFile(child) error = %v", err)
	}
	entries, err := engine.ReadDir(dir.Inode)
	if err != nil {
		t.Fatalf("ReadDir() error = %v", err)
	}
	if len(entries) != 1 || entries[0].Name != "child.txt" || entries[0].Type != TypeFile {
		t.Fatalf("ReadDir() entries = %+v", entries)
	}
	if err := engine.RemoveDir(RootInode, "dir"); !errors.Is(err, ErrNotEmpty) {
		t.Fatalf("RemoveDir(non-empty) err = %v, want ErrNotEmpty", err)
	}
	if err := engine.Unlink(dir.Inode, "child.txt"); err != nil {
		t.Fatalf("Unlink(child) error = %v", err)
	}
	if err := engine.RemoveDir(RootInode, "dir"); err != nil {
		t.Fatalf("RemoveDir() error = %v", err)
	}
	if err := engine.Fsync(RootInode); err != nil {
		t.Fatalf("Fsync() error = %v", err)
	}
	if err := engine.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	replayed, err := Open(context.Background(), Config{VolumeID: "vol-1", WALPath: walPath})
	if err != nil {
		t.Fatalf("Open(replay) error = %v", err)
	}
	defer replayed.Close()
	if _, err := replayed.Lookup(RootInode, "dir"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Lookup(dir) err = %v, want ErrNotFound", err)
	}
}

func TestEngineMetadataMutationReplay(t *testing.T) {
	walPath := filepath.Join(t.TempDir(), "volume.wal")
	engine, err := Open(context.Background(), Config{VolumeID: "vol-1", WALPath: walPath})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}

	node, err := engine.CreateFile(RootInode, "meta.txt", 0o644)
	if err != nil {
		t.Fatalf("CreateFile() error = %v", err)
	}
	if _, err := engine.Write(node.Inode, 0, []byte("abcdef")); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	if err := engine.Truncate(node.Inode, 3); err != nil {
		t.Fatalf("Truncate() error = %v", err)
	}
	if err := engine.SetMode(node.Inode, 0o600); err != nil {
		t.Fatalf("SetMode() error = %v", err)
	}
	if err := engine.SetOwner(node.Inode, 1000, 1001); err != nil {
		t.Fatalf("SetOwner() error = %v", err)
	}
	if err := engine.Fsync(node.Inode); err != nil {
		t.Fatalf("Fsync() error = %v", err)
	}
	if err := engine.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	replayed, err := Open(context.Background(), Config{VolumeID: "vol-1", WALPath: walPath})
	if err != nil {
		t.Fatalf("Open(replay) error = %v", err)
	}
	defer replayed.Close()
	attr, err := replayed.Lookup(RootInode, "meta.txt")
	if err != nil {
		t.Fatalf("Lookup() error = %v", err)
	}
	if attr.Mode != 0o600 || attr.UID != 1000 || attr.GID != 1001 || attr.Size != 3 {
		t.Fatalf("attr after replay = %+v", attr)
	}
	data, err := replayed.Read(attr.Inode, 0, 16)
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}
	if !bytes.Equal(data, []byte("abc")) {
		t.Fatalf("data after replay = %q, want abc", data)
	}
}

func TestEngineMknodSpecialNodeReplay(t *testing.T) {
	walPath := filepath.Join(t.TempDir(), "volume.wal")
	engine, err := Open(context.Background(), Config{VolumeID: "vol-1", WALPath: walPath})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}

	node, err := engine.Mknod(RootInode, "console", syscall.S_IFCHR|0o600, 0x0501)
	if err != nil {
		t.Fatalf("Mknod() error = %v", err)
	}
	if node.Type != TypeCharDevice || node.Mode != 0o600 || node.Rdev != 0x0501 {
		t.Fatalf("Mknod() node = %+v, want char device mode 0600 rdev 0x501", node)
	}
	if _, err := engine.Read(node.Inode, 0, 1); !errors.Is(err, ErrNotFile) {
		t.Fatalf("Read(char device) err = %v, want ErrNotFile", err)
	}
	if _, err := engine.Write(node.Inode, 0, []byte("x")); !errors.Is(err, ErrNotFile) {
		t.Fatalf("Write(char device) err = %v, want ErrNotFile", err)
	}
	if err := engine.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	replayed, err := Open(context.Background(), Config{VolumeID: "vol-1", WALPath: walPath})
	if err != nil {
		t.Fatalf("Open(replay) error = %v", err)
	}
	defer replayed.Close()

	replayedNode, err := replayed.Lookup(RootInode, "console")
	if err != nil {
		t.Fatalf("Lookup(console) error = %v", err)
	}
	if replayedNode.Type != TypeCharDevice || replayedNode.Mode != 0o600 || replayedNode.Rdev != 0x0501 {
		t.Fatalf("replayed node = %+v, want char device mode 0600 rdev 0x501", replayedNode)
	}
}

func TestEngineXattrReplayAndFlags(t *testing.T) {
	walPath := filepath.Join(t.TempDir(), "volume.wal")
	engine, err := Open(context.Background(), Config{VolumeID: "vol-1", WALPath: walPath})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}

	node, err := engine.CreateFile(RootInode, "meta.txt", 0o644)
	if err != nil {
		t.Fatalf("CreateFile() error = %v", err)
	}
	if err := engine.SetXattr(node.Inode, "trusted.overlay.opaque", []byte("y"), XattrCreateFlag); err != nil {
		t.Fatalf("SetXattr(create) error = %v", err)
	}
	if err := engine.SetXattr(node.Inode, "trusted.overlay.opaque", []byte("z"), XattrCreateFlag); !errors.Is(err, ErrExists) {
		t.Fatalf("SetXattr(create existing) err = %v, want ErrExists", err)
	}
	if err := engine.SetXattr(node.Inode, "trusted.overlay.opaque", []byte(""), XattrReplaceFlag); err != nil {
		t.Fatalf("SetXattr(replace empty) error = %v", err)
	}
	if err := engine.SetXattr(node.Inode, "user.note", []byte("note"), 0); err != nil {
		t.Fatalf("SetXattr(user.note) error = %v", err)
	}
	if err := engine.SetXattr(node.Inode, "user.missing", []byte("missing"), XattrReplaceFlag); !errors.Is(err, ErrNoAttr) {
		t.Fatalf("SetXattr(replace missing) err = %v, want ErrNoAttr", err)
	}
	if err := engine.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	replayed, err := Open(context.Background(), Config{VolumeID: "vol-1", WALPath: walPath})
	if err != nil {
		t.Fatalf("Open(replay) error = %v", err)
	}
	defer replayed.Close()

	replayedNode, err := replayed.Lookup(RootInode, "meta.txt")
	if err != nil {
		t.Fatalf("Lookup(meta) error = %v", err)
	}
	value, err := replayed.GetXattr(replayedNode.Inode, "trusted.overlay.opaque")
	if err != nil {
		t.Fatalf("GetXattr(opaque) error = %v", err)
	}
	if len(value) != 0 {
		t.Fatalf("GetXattr(opaque) = %q, want empty value", value)
	}
	names, err := replayed.ListXattrs(replayedNode.Inode)
	if err != nil {
		t.Fatalf("ListXattrs() error = %v", err)
	}
	wantNames := []string{"trusted.overlay.opaque", "user.note"}
	if len(names) != len(wantNames) || names[0] != wantNames[0] || names[1] != wantNames[1] {
		t.Fatalf("ListXattrs() = %v, want %v", names, wantNames)
	}
	if err := replayed.RemoveXattr(replayedNode.Inode, "trusted.overlay.opaque"); err != nil {
		t.Fatalf("RemoveXattr() error = %v", err)
	}
	if _, err := replayed.GetXattr(replayedNode.Inode, "trusted.overlay.opaque"); !errors.Is(err, ErrNoAttr) {
		t.Fatalf("GetXattr(removed) err = %v, want ErrNoAttr", err)
	}
	if err := replayed.RemoveXattr(replayedNode.Inode, "trusted.overlay.opaque"); !errors.Is(err, ErrNoAttr) {
		t.Fatalf("RemoveXattr(removed) err = %v, want ErrNoAttr", err)
	}
}

func TestEngineUnlinkThenForget(t *testing.T) {
	engine, err := Open(context.Background(), Config{VolumeID: "vol-1", WALPath: filepath.Join(t.TempDir(), "volume.wal")})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer engine.Close()

	node, err := engine.CreateFile(RootInode, "open.txt", 0o644)
	if err != nil {
		t.Fatalf("CreateFile() error = %v", err)
	}
	if _, err := engine.Write(node.Inode, 0, []byte("payload")); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	unlinkedInode, err := engine.UnlinkWithInode(RootInode, "open.txt")
	if err != nil {
		t.Fatalf("UnlinkWithInode() error = %v", err)
	}
	if unlinkedInode != node.Inode {
		t.Fatalf("UnlinkWithInode() inode = %d, want %d", unlinkedInode, node.Inode)
	}
	if _, err := engine.Lookup(RootInode, "open.txt"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Lookup() after unlink err = %v, want ErrNotFound", err)
	}
	data, err := engine.Read(node.Inode, 0, 16)
	if err != nil {
		t.Fatalf("Read(unlinked inode) error = %v", err)
	}
	if !bytes.Equal(data, []byte("payload")) {
		t.Fatalf("Read(unlinked inode) = %q, want payload", data)
	}
	if err := engine.Forget(node.Inode); err != nil {
		t.Fatalf("Forget() error = %v", err)
	}
	if _, err := engine.GetAttr(node.Inode); !errors.Is(err, ErrNotFound) {
		t.Fatalf("GetAttr() after forget err = %v, want ErrNotFound", err)
	}
}
