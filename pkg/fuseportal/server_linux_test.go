//go:build linux

package fuseportal

import (
	"encoding/binary"
	"errors"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
	"unsafe"

	"github.com/hanwen/go-fuse/v2/fuse"
	"golang.org/x/sys/unix"
)

type takeoverTestFS struct {
	fuse.RawFileSystem
	mu   sync.Mutex
	data []byte
}

func newTakeoverTestFS(data string) *takeoverTestFS {
	return &takeoverTestFS{
		RawFileSystem: fuse.NewDefaultRawFileSystem(),
		data:          []byte(data),
	}
}

func (fs *takeoverTestFS) String() string { return "takeover-test" }

func (fs *takeoverTestFS) Lookup(_ <-chan struct{}, header *fuse.InHeader, name string, out *fuse.EntryOut) fuse.Status {
	if header.NodeId != fuseRootID || name != "value" {
		return fuse.ENOENT
	}
	fs.mu.Lock()
	defer fs.mu.Unlock()
	out.NodeId = 2
	out.Generation = 1
	out.Ino = 2
	out.Mode = syscall.S_IFREG | 0o644
	out.Size = uint64(len(fs.data))
	return fuse.OK
}

func (fs *takeoverTestFS) GetAttr(_ <-chan struct{}, input *fuse.GetAttrIn, out *fuse.AttrOut) fuse.Status {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	out.Ino = input.NodeId
	if input.NodeId == fuseRootID {
		out.Mode = syscall.S_IFDIR | 0o755
		out.Nlink = 2
		return fuse.OK
	}
	if input.NodeId != 2 {
		return fuse.ENOENT
	}
	out.Mode = syscall.S_IFREG | 0o644
	out.Nlink = 1
	out.Size = uint64(len(fs.data))
	return fuse.OK
}

func (fs *takeoverTestFS) Open(_ <-chan struct{}, input *fuse.OpenIn, out *fuse.OpenOut) fuse.Status {
	if input.NodeId != 2 {
		return fuse.ENOENT
	}
	out.Fh = 1
	out.OpenFlags = fuse.FOPEN_DIRECT_IO
	return fuse.OK
}

func (fs *takeoverTestFS) Read(_ <-chan struct{}, input *fuse.ReadIn, _ []byte) (fuse.ReadResult, fuse.Status) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if input.NodeId != 2 {
		return nil, fuse.ENOENT
	}
	if input.Offset >= uint64(len(fs.data)) {
		return fuse.ReadResultData(nil), fuse.OK
	}
	end := input.Offset + uint64(input.Size)
	if end > uint64(len(fs.data)) {
		end = uint64(len(fs.data))
	}
	return fuse.ReadResultData(append([]byte(nil), fs.data[input.Offset:end]...)), fuse.OK
}

func (fs *takeoverTestFS) setData(data string) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	fs.data = []byte(data)
}

func TestClonedChannelTakesOverExistingMount(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("requires root")
	}
	if _, err := os.Stat(fuseDevicePath); err != nil {
		t.Skipf("FUSE device unavailable: %v", err)
	}

	mountPoint := t.TempDir()
	fs := newTakeoverTestFS("primary")
	opts := &fuse.MountOptions{
		Name:          "sandbox0-takeover-test",
		FsName:        "sandbox0-takeover-test",
		MaxWrite:      128 * 1024,
		MaxBackground: 16,
	}
	primary, err := Mount(fs, mountPoint, opts)
	if err != nil {
		t.Skipf("mount FUSE test filesystem: %v", err)
	}
	primaryDone := make(chan error, 1)
	go func() { primaryDone <- primary.Serve() }()
	t.Cleanup(func() {
		_ = primary.Unmount()
	})

	clone, err := primary.CloneChannel()
	if err != nil {
		t.Fatalf("CloneChannel() error = %v", err)
	}
	standby, err := Attach(fs, clone, mountPoint, primary.InitRequest(), opts)
	if err != nil {
		_ = clone.Close()
		t.Fatalf("Attach() error = %v", err)
	}
	_ = clone.Close()
	t.Cleanup(func() { _ = standby.Unmount() })

	assertFileContents(t, mountPoint+"/value", "primary")
	var reads sync.WaitGroup
	readErrors := make(chan error, 64)
	for range 64 {
		reads.Add(1)
		go func() {
			defer reads.Done()
			payload, err := os.ReadFile(mountPoint + "/value")
			if err != nil {
				readErrors <- err
				return
			}
			if string(payload) != "primary" {
				readErrors <- syscall.EIO
			}
		}()
	}
	reads.Wait()
	close(readErrors)
	for err := range readErrors {
		t.Fatalf("read with idle cloned channel: %v", err)
	}
	if err := primary.Detach(); err != nil {
		t.Fatalf("primary.Detach() error = %v", err)
	}
	select {
	case err := <-primaryDone:
		if err != nil {
			t.Fatalf("primary.Serve() error = %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("primary Serve did not stop")
	}

	fs.setData("standby")
	standbyDone := make(chan error, 1)
	go func() { standbyDone <- standby.Serve() }()
	assertFileContents(t, mountPoint+"/value", "standby")

	if err := standby.Unmount(); err != nil {
		t.Fatalf("standby.Unmount() error = %v", err)
	}
	select {
	case err := <-standbyDone:
		if err != nil {
			t.Fatalf("standby.Serve() error = %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("standby Serve did not stop")
	}
}

func assertFileContents(t *testing.T, path, want string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		got, err := os.ReadFile(path)
		if err == nil && string(got) == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("ReadFile(%q) = %q, %v; want %q", path, got, err, want)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestResponseBuffersUsesIoctlOutSize(t *testing.T) {
	request := make([]byte, 72)
	binary.LittleEndian.PutUint32(request[0:4], uint32(len(request)))
	binary.LittleEndian.PutUint32(request[4:8], opIoctl)
	binary.LittleEndian.PutUint32(request[68:72], 123)

	buffers, suppress, err := responseBuffers(request)
	if err != nil {
		t.Fatalf("responseBuffers() error = %v", err)
	}
	if suppress {
		t.Fatal("responseBuffers() suppress = true, want false")
	}
	want := []int{fuseOutSize, int(unsafe.Sizeof(fuse.IoctlOut{})), 123}
	if len(buffers) != len(want) {
		t.Fatalf("response buffer count = %d, want %d", len(buffers), len(want))
	}
	for i := range want {
		if len(buffers[i]) != want[i] {
			t.Fatalf("response buffer %d size = %d, want %d", i, len(buffers[i]), want[i])
		}
	}
}

func TestStructuredOutputSizeCoversLinuxProtocolExtensions(t *testing.T) {
	tests := map[uint32]int{
		opBmap:            8,
		opStatx:           int(unsafe.Sizeof(fuse.StatxOut{})),
		opCopyFileRange64: int(unsafe.Sizeof(fuse.CopyFileRangeOut{})),
	}
	for opcode, want := range tests {
		if got := structuredOutputSize(opcode); got != want {
			t.Errorf("structuredOutputSize(%d) = %d, want %d", opcode, got, want)
		}
	}
}

func TestHandleAndReplyReturnsProtocolErrorToKernel(t *testing.T) {
	fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		t.Fatalf("Socketpair() error = %v", err)
	}
	defer unix.Close(fds[1])
	server, err := newServer(fuse.NewDefaultRawFileSystem(), fds[0], "", &fuse.MountOptions{})
	if err != nil {
		_ = unix.Close(fds[0])
		t.Fatalf("newServer() error = %v", err)
	}
	defer server.closeDescriptors()
	request := make([]byte, fuseHeaderSize)
	binary.LittleEndian.PutUint32(request[0:4], uint32(len(request)))
	binary.LittleEndian.PutUint32(request[4:8], 999)
	binary.LittleEndian.PutUint64(request[8:16], 42)

	if err := server.handleAndReply(request); err != nil {
		t.Fatalf("handleAndReply() error = %v", err)
	}
	response := make([]byte, fuseOutSize)
	if _, err := unix.Read(fds[1], response); err != nil {
		t.Fatalf("Read(response) error = %v", err)
	}
	if got := int32(binary.LittleEndian.Uint32(response[4:8])); got != -int32(fuse.ENOSYS) {
		t.Fatalf("response error = %d, want %d", got, -int32(fuse.ENOSYS))
	}
}

func TestSharedEpollMultiplexesPortalChannels(t *testing.T) {
	mux, err := sharedEpollMultiplexer()
	if err != nil {
		t.Fatalf("sharedEpollMultiplexer() error = %v", err)
	}
	baseline := mux.activeCount()
	baselineThreads, threadCountErr := processThreadCount()
	const serverCount = 128
	portalFS := newTakeoverTestFS("")
	type portalPair struct {
		server *Server
		peerFD int
		done   chan error
	}
	pairs := make([]portalPair, 0, serverCount)
	for i := 0; i < serverCount; i++ {
		fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM|unix.SOCK_CLOEXEC, 0)
		if err != nil {
			t.Fatalf("Socketpair(%d) error = %v", i, err)
		}
		server, err := newServer(portalFS, fds[0], "", &fuse.MountOptions{})
		if err != nil {
			_ = unix.Close(fds[0])
			_ = unix.Close(fds[1])
			t.Fatalf("newServer(%d) error = %v", i, err)
		}
		readTimeout := unix.NsecToTimeval((3 * time.Second).Nanoseconds())
		if err := unix.SetsockoptTimeval(fds[1], unix.SOL_SOCKET, unix.SO_RCVTIMEO, &readTimeout); err != nil {
			_ = server.Detach()
			_ = unix.Close(fds[1])
			t.Fatalf("SetsockoptTimeval(%d) error = %v", i, err)
		}
		done := make(chan error, 1)
		go func() { done <- server.Serve() }()
		pairs = append(pairs, portalPair{server: server, peerFD: fds[1], done: done})
	}
	t.Cleanup(func() {
		for _, pair := range pairs {
			_ = pair.server.Detach()
			if pair.peerFD >= 0 {
				_ = unix.Close(pair.peerFD)
			}
		}
	})

	deadline := time.Now().Add(3 * time.Second)
	for mux.activeCount() != baseline+serverCount {
		if time.Now().After(deadline) {
			t.Fatalf("active epoll registrations = %d, want %d", mux.activeCount(), baseline+serverCount)
		}
		time.Sleep(time.Millisecond)
	}
	if threadCountErr == nil {
		threads, err := processThreadCount()
		if err != nil {
			t.Fatalf("processThreadCount() error = %v", err)
		}
		if delta := threads - baselineThreads; delta > 32 {
			t.Fatalf("OS thread count grew by %d for %d idle portals; want at most 32", delta, serverCount)
		}
	}

	request := make([]byte, int(unsafe.Sizeof(fuse.GetAttrIn{})))
	binary.LittleEndian.PutUint32(request[0:4], uint32(len(request)))
	binary.LittleEndian.PutUint32(request[4:8], opGetattr)
	binary.LittleEndian.PutUint64(request[16:24], fuseRootID)
	for i, pair := range pairs {
		binary.LittleEndian.PutUint64(request[8:16], uint64(i+1))
		if _, err := unix.Write(pair.peerFD, request); err != nil {
			t.Fatalf("Write(request %d) error = %v", i, err)
		}
	}
	for i, pair := range pairs {
		response, err := readTestResponse(pair.peerFD)
		if err != nil {
			t.Fatalf("Read(response %d) error = %v", i, err)
		}
		if got := binary.LittleEndian.Uint64(response[8:16]); got != uint64(i+1) {
			t.Fatalf("response %d unique = %d, want %d", i, got, i+1)
		}
		if got := int32(binary.LittleEndian.Uint32(response[4:8])); got != 0 {
			t.Fatalf("response %d error = %d, want 0", i, got)
		}
	}
	for i, pair := range pairs {
		if err := pair.server.Detach(); err != nil {
			t.Fatalf("Detach(%d) error = %v", i, err)
		}
		if err := <-pair.done; err != nil {
			t.Fatalf("Serve(%d) error = %v", i, err)
		}
		_ = unix.Close(pair.peerFD)
		pairs[i].peerFD = -1
	}
	if got := mux.activeCount(); got != baseline {
		t.Fatalf("active epoll registrations after detach = %d, want %d", got, baseline)
	}
}

func readTestResponse(fd int) ([]byte, error) {
	header := make([]byte, fuseOutSize)
	if err := readFullFD(fd, header); err != nil {
		return nil, err
	}
	length := int(binary.LittleEndian.Uint32(header[:4]))
	if length < fuseOutSize || length > 1<<20 {
		return nil, syscall.EPROTO
	}
	response := make([]byte, length)
	copy(response, header)
	if err := readFullFD(fd, response[fuseOutSize:]); err != nil {
		return nil, err
	}
	return response, nil
}

func readFullFD(fd int, buffer []byte) error {
	for len(buffer) > 0 {
		count, err := unix.Read(fd, buffer)
		if errors.Is(err, unix.EINTR) {
			continue
		}
		if err != nil {
			return err
		}
		if count == 0 {
			return io.ErrUnexpectedEOF
		}
		buffer = buffer[count:]
	}
	return nil
}

func processThreadCount() (int, error) {
	payload, err := os.ReadFile("/proc/self/status")
	if err != nil {
		return 0, err
	}
	for _, line := range strings.Split(string(payload), "\n") {
		if !strings.HasPrefix(line, "Threads:") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) != 2 {
			return 0, syscall.EINVAL
		}
		return strconv.Atoi(fields[1])
	}
	return 0, syscall.ENOENT
}
