package http

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"syscall"
	"testing"

	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/fserror"
	pb "github.com/sandbox0-ai/sandbox0/storage-proxy/proto/fs"
)

type archiveTestNode struct {
	inode  uint64
	parent uint64
	name   string
	mode   uint32
	data   []byte
	target string
}

type archiveTestFS struct {
	nextInode uint64
	nodes     map[uint64]*archiveTestNode
	children  map[uint64]map[string]uint64
}

func newArchiveTestFS() *archiveTestFS {
	fs := &archiveTestFS{
		nextInode: 2,
		nodes: map[uint64]*archiveTestNode{
			1: {inode: 1, mode: syscall.S_IFDIR | 0o755},
		},
		children: map[uint64]map[string]uint64{1: {}},
	}
	return fs
}

func (fs *archiveTestFS) addDir(parent uint64, name string) uint64 {
	return fs.addNode(parent, name, syscall.S_IFDIR|0o755, nil, "")
}

func (fs *archiveTestFS) addFile(parent uint64, name string, data []byte) uint64 {
	return fs.addNode(parent, name, syscall.S_IFREG|0o644, data, "")
}

func (fs *archiveTestFS) addNode(parent uint64, name string, mode uint32, data []byte, target string) uint64 {
	inode := fs.nextInode
	fs.nextInode++
	fs.nodes[inode] = &archiveTestNode{inode: inode, parent: parent, name: name, mode: mode, data: data, target: target}
	if fs.children[parent] == nil {
		fs.children[parent] = map[string]uint64{}
	}
	fs.children[parent][name] = inode
	if mode&syscall.S_IFMT == syscall.S_IFDIR {
		fs.children[inode] = map[string]uint64{}
	}
	return inode
}

func archiveTestRPC(fs *archiveTestFS) *fakeHTTPVolumeFileRPC {
	var handleInode uint64
	return &fakeHTTPVolumeFileRPC{
		getAttrFunc: func(_ context.Context, req *pb.GetAttrRequest) (*pb.GetAttrResponse, error) {
			node := fs.nodes[req.Inode]
			if node == nil {
				return nil, fserror.New(fserror.NotFound, "missing")
			}
			return &pb.GetAttrResponse{Mode: node.mode, Size: uint64(len(node.data))}, nil
		},
		lookupFunc: func(_ context.Context, req *pb.LookupRequest) (*pb.NodeResponse, error) {
			child := fs.children[req.Parent][req.Name]
			node := fs.nodes[child]
			if node == nil {
				return nil, fserror.New(fserror.NotFound, "missing")
			}
			return &pb.NodeResponse{Inode: node.inode, Attr: &pb.GetAttrResponse{Mode: node.mode, Size: uint64(len(node.data))}}, nil
		},
		mkdirFunc: func(_ context.Context, req *pb.MkdirRequest) (*pb.NodeResponse, error) {
			if child := fs.children[req.Parent][req.Name]; child != 0 {
				node := fs.nodes[child]
				return &pb.NodeResponse{Inode: node.inode, Attr: &pb.GetAttrResponse{Mode: node.mode}}, nil
			}
			inode := fs.addNode(req.Parent, req.Name, syscall.S_IFDIR|req.Mode, nil, "")
			return &pb.NodeResponse{Inode: inode, Attr: &pb.GetAttrResponse{Mode: fs.nodes[inode].mode}}, nil
		},
		createFunc: func(_ context.Context, req *pb.CreateRequest) (*pb.NodeResponse, error) {
			inode := fs.addNode(req.Parent, req.Name, syscall.S_IFREG|req.Mode, nil, "")
			handleInode = inode
			return &pb.NodeResponse{Inode: inode, HandleId: inode + 1000, Attr: &pb.GetAttrResponse{Mode: fs.nodes[inode].mode}}, nil
		},
		writeFunc: func(_ context.Context, req *pb.WriteRequest) (*pb.WriteResponse, error) {
			node := fs.nodes[handleInode]
			if node == nil {
				return nil, fserror.New(fserror.NotFound, "missing handle")
			}
			end := int(req.Offset) + len(req.Data)
			if len(node.data) < end {
				grown := make([]byte, end)
				copy(grown, node.data)
				node.data = grown
			}
			copy(node.data[int(req.Offset):], req.Data)
			return &pb.WriteResponse{BytesWritten: int64(len(req.Data))}, nil
		},
		releaseFunc: func(_ context.Context, req *pb.ReleaseRequest) (*pb.Empty, error) {
			handleInode = 0
			return &pb.Empty{}, nil
		},
		symlinkFunc: func(_ context.Context, req *pb.SymlinkRequest) (*pb.NodeResponse, error) {
			inode := fs.addNode(req.Parent, req.Name, syscall.S_IFLNK|0o777, nil, req.Target)
			return &pb.NodeResponse{Inode: inode, Attr: &pb.GetAttrResponse{Mode: fs.nodes[inode].mode}}, nil
		},
	}
}

func TestVolumeArchiveUploadImportsTarGz(t *testing.T) {
	fs := newArchiveTestFS()
	server, volMgr := newVolumeFileTestServerWithBarrier(archiveTestRPC(fs), &fakeHTTPSharedVolumeBarrier{})
	body := makeTarGz(t, []tarEntry{
		{name: "app/", mode: 0o755, typ: tar.TypeDir},
		{name: "app/server.js", mode: 0o644, typ: tar.TypeReg, body: "console.log('ok')"},
		{name: "app/current", typ: tar.TypeSymlink, link: "server.js"},
	})
	req := httptest.NewRequest(http.MethodPost, "/sandboxvolumes/vol-1/files/archive?path=/workspace", bytes.NewReader(body))
	req.SetPathValue("id", "vol-1")
	req = req.WithContext(internalauth.WithClaims(req.Context(), &internalauth.Claims{TeamID: "team-a"}))
	recorder := httptest.NewRecorder()

	server.handleVolumeFileArchiveUpload(recorder, req)

	if recorder.Code != http.StatusCreated {
		t.Fatalf("status = %d, want %d, body = %s", recorder.Code, http.StatusCreated, recorder.Body.String())
	}
	result, apiErr, err := spec.DecodeResponse[volumeArchiveUploadResult](recorder.Body)
	if err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if apiErr != nil {
		t.Fatalf("api error: %+v", apiErr)
	}
	if result.Files != 1 || result.Dirs != 1 || result.Symlinks != 1 || result.Bytes != int64(len("console.log('ok')")) {
		t.Fatalf("result = %+v, want imported file, dirs, symlink", result)
	}
	workspace := fs.children[1]["workspace"]
	app := fs.children[workspace]["app"]
	serverJS := fs.nodes[fs.children[app]["server.js"]]
	if string(serverJS.data) != "console.log('ok')" {
		t.Fatalf("server.js = %q, want archive content", string(serverJS.data))
	}
	current := fs.nodes[fs.children[app]["current"]]
	if current.target != "server.js" {
		t.Fatalf("symlink target = %q, want server.js", current.target)
	}
	if volMgr.syncCalls != 1 {
		t.Fatalf("sync calls = %d, want 1", volMgr.syncCalls)
	}
}

func TestVolumeArchiveUploadRejectsTraversal(t *testing.T) {
	fs := newArchiveTestFS()
	server, _ := newVolumeFileTestServer(archiveTestRPC(fs))
	body := makeTarGz(t, []tarEntry{{name: "../escape.txt", mode: 0o644, typ: tar.TypeReg, body: "no"}})
	req := httptest.NewRequest(http.MethodPost, "/sandboxvolumes/vol-1/files/archive", bytes.NewReader(body))
	req.SetPathValue("id", "vol-1")
	req = req.WithContext(internalauth.WithClaims(req.Context(), &internalauth.Claims{TeamID: "team-a"}))
	recorder := httptest.NewRecorder()

	server.handleVolumeFileArchiveUpload(recorder, req)

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusBadRequest)
	}
}

func TestVolumeArchiveUploadRejectsExistingPathWithoutOverwrite(t *testing.T) {
	fs := newArchiveTestFS()
	app := fs.addDir(1, "app")
	fs.addFile(app, "server.js", []byte("old"))
	server, _ := newVolumeFileTestServer(archiveTestRPC(fs))
	body := makeTarGz(t, []tarEntry{{name: "server.js", mode: 0o644, typ: tar.TypeReg, body: "new"}})
	req := httptest.NewRequest(http.MethodPost, "/sandboxvolumes/vol-1/files/archive?path=/app", bytes.NewReader(body))
	req.SetPathValue("id", "vol-1")
	req = req.WithContext(internalauth.WithClaims(req.Context(), &internalauth.Claims{TeamID: "team-a"}))
	recorder := httptest.NewRecorder()

	server.handleVolumeFileArchiveUpload(recorder, req)

	if recorder.Code != http.StatusConflict {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusConflict)
	}
	if node := fs.nodes[fs.children[app]["server.js"]]; string(node.data) != "old" {
		t.Fatalf("server.js = %q, want unchanged content", string(node.data))
	}
}

func TestVolumeArchiveUploadStreamsLargeFilesInChunks(t *testing.T) {
	fs := newArchiveTestFS()
	var writes int
	var maxWrite int
	rpc := archiveTestRPC(fs)
	originalWrite := rpc.writeFunc
	rpc.writeFunc = func(ctx context.Context, req *pb.WriteRequest) (*pb.WriteResponse, error) {
		writes++
		if len(req.Data) > maxWrite {
			maxWrite = len(req.Data)
		}
		return originalWrite(ctx, req)
	}
	server, _ := newVolumeFileTestServer(rpc)
	content := strings.Repeat("a", volumeArchiveWriteChunkSize+17)
	body := makeTarGz(t, []tarEntry{{name: "large.bin", mode: 0o644, typ: tar.TypeReg, body: content}})
	req := httptest.NewRequest(http.MethodPost, "/sandboxvolumes/vol-1/files/archive", bytes.NewReader(body))
	req.SetPathValue("id", "vol-1")
	req = req.WithContext(internalauth.WithClaims(req.Context(), &internalauth.Claims{TeamID: "team-a"}))
	recorder := httptest.NewRecorder()

	server.handleVolumeFileArchiveUpload(recorder, req)

	if recorder.Code != http.StatusCreated {
		t.Fatalf("status = %d, want %d, body = %s", recorder.Code, http.StatusCreated, recorder.Body.String())
	}
	if writes < 2 {
		t.Fatalf("writes = %d, want multiple chunked writes", writes)
	}
	if maxWrite > volumeArchiveWriteChunkSize {
		t.Fatalf("max write = %d, want <= %d", maxWrite, volumeArchiveWriteChunkSize)
	}
	node := fs.nodes[fs.children[1]["large.bin"]]
	if string(node.data) != content {
		t.Fatalf("large.bin content mismatch: got %d bytes, want %d", len(node.data), len(content))
	}
}

func TestVolumeArchiveUploadCheckpointsLargeImports(t *testing.T) {
	fs := newArchiveTestFS()
	server, volMgr := newVolumeFileTestServerWithBarrier(archiveTestRPC(fs), &fakeHTTPSharedVolumeBarrier{})
	content := strings.Repeat("a", volumeArchiveCheckpointBytes+1)
	body := makeTarGz(t, []tarEntry{{name: "large.bin", mode: 0o644, typ: tar.TypeReg, body: content}})
	req := httptest.NewRequest(http.MethodPost, "/sandboxvolumes/vol-1/files/archive", bytes.NewReader(body))
	req.SetPathValue("id", "vol-1")
	req = req.WithContext(internalauth.WithClaims(req.Context(), &internalauth.Claims{TeamID: "team-a"}))
	recorder := httptest.NewRecorder()

	server.handleVolumeFileArchiveUpload(recorder, req)

	if recorder.Code != http.StatusCreated {
		t.Fatalf("status = %d, want %d, body = %s", recorder.Code, http.StatusCreated, recorder.Body.String())
	}
	if volMgr.syncCalls < 2 {
		t.Fatalf("sync calls = %d, want archive checkpoint plus final sync", volMgr.syncCalls)
	}
}

type tarEntry struct {
	name string
	mode int64
	typ  byte
	body string
	link string
}

func makeTarGz(t *testing.T, entries []tarEntry) []byte {
	t.Helper()
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gz)
	for _, entry := range entries {
		header := &tar.Header{
			Name:     entry.name,
			Mode:     entry.mode,
			Typeflag: entry.typ,
			Linkname: entry.link,
			Size:     int64(len(entry.body)),
		}
		if entry.typ == tar.TypeDir {
			header.Size = 0
		}
		if err := tw.WriteHeader(header); err != nil {
			t.Fatalf("write tar header: %v", err)
		}
		if entry.body != "" {
			if _, err := tw.Write([]byte(entry.body)); err != nil {
				t.Fatalf("write tar body: %v", err)
			}
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatalf("close tar: %v", err)
	}
	if err := gz.Close(); err != nil {
		t.Fatalf("close gzip: %v", err)
	}
	if strings.TrimSpace(buf.String()) == "" {
		t.Fatal("empty archive")
	}
	return buf.Bytes()
}
