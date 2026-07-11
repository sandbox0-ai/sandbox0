//go:build linux

package fuseportal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"unsafe"

	"github.com/hanwen/go-fuse/v2/fuse"
	"golang.org/x/sys/unix"
)

const (
	fuseDevicePath = "/dev/fuse"
	fuseRootID     = 1
	fuseHeaderSize = 40
	fuseOutSize    = 16

	// _IOR(FUSE_DEV_IOC_MAGIC, 0, uint32_t), from include/uapi/linux/fuse.h.
	fuseDevIOClone = uintptr(0x8004e500)

	opLookup          = 1
	opForget          = 2
	opGetattr         = 3
	opSetattr         = 4
	opReadlink        = 5
	opSymlink         = 6
	opMknod           = 8
	opMkdir           = 9
	opRename          = 12
	opLink            = 13
	opOpen            = 14
	opRead            = 15
	opWrite           = 16
	opStatfs          = 17
	opGetxattr        = 22
	opListxattr       = 23
	opInit            = 26
	opOpendir         = 27
	opReaddir         = 28
	opGetlk           = 31
	opCreate          = 35
	opBmap            = 37
	opIoctl           = 39
	opPoll            = 40
	opNotifyReply     = 41
	opBatchForget     = 42
	opReaddirplus     = 44
	opRename2         = 45
	opLseek           = 46
	opCopyFileRange   = 47
	opStatx           = 52
	opCopyFileRange64 = 53
)

// Server owns one userspace channel for a kernel FUSE connection. A connection
// can have multiple Servers, but exactly one should call Serve at a time.
type Server struct {
	fs          fuse.RawFileSystem
	protocol    *fuse.ProtocolServer
	opts        fuse.MountOptions
	mountPoint  string
	initRequest []byte
	fd          int
	wakeFD      int
	fdMu        sync.Mutex
	serving     bool
	serveMu     sync.Mutex
	requests    sync.WaitGroup
	done        chan struct{}
	doneOnce    sync.Once
}

// Mount creates a new kernel FUSE connection and processes its INIT request.
// The caller must call Serve to start processing regular filesystem requests.
func Mount(fs fuse.RawFileSystem, mountPoint string, opts *fuse.MountOptions) (*Server, error) {
	if fs == nil {
		return nil, fmt.Errorf("fuse filesystem is required")
	}
	cleanMountPoint, err := absoluteMountPoint(mountPoint)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(cleanMountPoint, 0o755); err != nil {
		return nil, fmt.Errorf("create FUSE mount point: %w", err)
	}

	fd, err := unix.Open(fuseDevicePath, unix.O_RDWR|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", fuseDevicePath, err)
	}
	configured := normalizeOptions(opts)
	if err := mountDirect(fd, cleanMountPoint, &configured); err != nil {
		_ = unix.Close(fd)
		return nil, err
	}

	server, err := newServer(fs, fd, cleanMountPoint, &configured)
	if err != nil {
		_ = unix.Unmount(cleanMountPoint, unix.MNT_DETACH)
		_ = unix.Close(fd)
		return nil, err
	}
	initRequest, err := readRequest(fd, configured.MaxWrite)
	if err != nil {
		_ = server.closeDescriptors()
		_ = unix.Unmount(cleanMountPoint, unix.MNT_DETACH)
		return nil, fmt.Errorf("read FUSE INIT request: %w", err)
	}
	if requestOpcode(initRequest) != opInit {
		_ = server.closeDescriptors()
		_ = unix.Unmount(cleanMountPoint, unix.MNT_DETACH)
		return nil, fmt.Errorf("first FUSE request opcode is %d, want INIT", requestOpcode(initRequest))
	}
	if err := server.handleAndReply(initRequest); err != nil {
		_ = server.closeDescriptors()
		_ = unix.Unmount(cleanMountPoint, unix.MNT_DETACH)
		return nil, fmt.Errorf("process FUSE INIT request: %w", err)
	}
	server.initRequest = append([]byte(nil), initRequest...)
	return server, nil
}

// Attach creates an idle Server for an already initialized cloned FUSE
// channel. initRequest must be the original request received for the mount.
// The caller retains ownership of channel.
func Attach(fs fuse.RawFileSystem, channel *os.File, mountPoint string, initRequest []byte, opts *fuse.MountOptions) (*Server, error) {
	if fs == nil {
		return nil, fmt.Errorf("fuse filesystem is required")
	}
	if channel == nil {
		return nil, fmt.Errorf("FUSE channel is required")
	}
	if requestOpcode(initRequest) != opInit {
		return nil, fmt.Errorf("valid FUSE INIT request is required")
	}
	configured := normalizeOptions(opts)
	fd, err := unix.Dup(int(channel.Fd()))
	if err != nil {
		return nil, fmt.Errorf("duplicate attached FUSE channel: %w", err)
	}
	unix.CloseOnExec(fd)
	server, err := newServer(fs, fd, filepath.Clean(mountPoint), &configured)
	if err != nil {
		_ = unix.Close(fd)
		return nil, err
	}
	// The cloned kernel channel has already completed INIT. Replaying the input
	// only initializes go-fuse's protocol parser; its generated reply is dropped.
	if err := server.initializeProtocol(initRequest); err != nil {
		_ = server.closeDescriptors()
		return nil, fmt.Errorf("initialize attached FUSE protocol: %w", err)
	}
	server.initRequest = append([]byte(nil), initRequest...)
	return server, nil
}

func newServer(fs fuse.RawFileSystem, fd int, mountPoint string, opts *fuse.MountOptions) (*Server, error) {
	wakeFD, err := unix.Eventfd(0, unix.EFD_CLOEXEC|unix.EFD_NONBLOCK)
	if err != nil {
		return nil, fmt.Errorf("create FUSE wake event: %w", err)
	}
	return &Server{
		fs:         fs,
		protocol:   fuse.NewProtocolServer(fs, opts),
		opts:       *opts,
		mountPoint: mountPoint,
		fd:         fd,
		wakeFD:     wakeFD,
		done:       make(chan struct{}),
	}, nil
}

// Serve processes requests until Detach or Unmount is called, or the channel
// becomes unavailable.
func (s *Server) Serve() error {
	if s == nil {
		return fmt.Errorf("FUSE server is nil")
	}
	s.serveMu.Lock()
	if s.serving {
		s.serveMu.Unlock()
		return fmt.Errorf("FUSE server is already serving")
	}
	s.serving = true
	s.serveMu.Unlock()

	defer s.finish()
	s.fdMu.Lock()
	channelFD := s.fd
	wakeFD := s.wakeFD
	s.fdMu.Unlock()
	pollFDs := []unix.PollFd{
		{Fd: int32(channelFD), Events: unix.POLLIN},
		{Fd: int32(wakeFD), Events: unix.POLLIN},
	}
	for {
		_, err := unix.Poll(pollFDs, -1)
		if errors.Is(err, unix.EINTR) {
			continue
		}
		if err != nil {
			return fmt.Errorf("poll FUSE channel: %w", err)
		}
		if pollFDs[1].Revents != 0 {
			return nil
		}
		if pollFDs[0].Revents&(unix.POLLERR|unix.POLLHUP|unix.POLLNVAL) != 0 {
			return nil
		}
		if pollFDs[0].Revents&unix.POLLIN == 0 {
			continue
		}
		request, err := readRequest(channelFD, s.opts.MaxWrite)
		if errors.Is(err, unix.ENODEV) || errors.Is(err, unix.EBADF) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("read FUSE request: %w", err)
		}
		s.requests.Add(1)
		go func() {
			defer s.requests.Done()
			if err := s.handleAndReply(request); err != nil {
				if s.opts.Logger != nil {
					s.opts.Logger.Printf("handle FUSE request: %v", err)
				}
				_ = s.replyError(request, fuse.EIO)
			}
		}()
	}
}

// CloneChannel creates a second device channel attached to the same kernel
// FUSE connection. The returned descriptor may be transferred with SCM_RIGHTS.
func (s *Server) CloneChannel() (*os.File, error) {
	if s == nil {
		return nil, fmt.Errorf("FUSE channel is unavailable")
	}
	cloneFD, err := unix.Open(fuseDevicePath, unix.O_RDWR|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, fmt.Errorf("open cloned FUSE device: %w", err)
	}
	s.fdMu.Lock()
	defer s.fdMu.Unlock()
	if s.fd < 0 {
		_ = unix.Close(cloneFD)
		return nil, fmt.Errorf("FUSE channel is unavailable")
	}
	oldFD := uint32(s.fd)
	_, _, errno := unix.Syscall(unix.SYS_IOCTL, uintptr(cloneFD), fuseDevIOClone, uintptr(unsafe.Pointer(&oldFD)))
	if errno != 0 {
		_ = unix.Close(cloneFD)
		return nil, fmt.Errorf("clone FUSE device channel: %w", errno)
	}
	return os.NewFile(uintptr(cloneFD), "fuse-clone"), nil
}

// InitRequest returns the original FUSE INIT request needed to initialize a
// ProtocolServer attached to a cloned channel.
func (s *Server) InitRequest() []byte {
	if s == nil {
		return nil
	}
	return append([]byte(nil), s.initRequest...)
}

// Detach stops serving and closes this channel without unmounting the kernel
// filesystem. Another cloned channel must remain open for the mount to survive.
func (s *Server) Detach() error {
	if s == nil {
		return nil
	}
	if s.stopIdle() {
		return nil
	}
	s.wake()
	<-s.done
	return nil
}

// Unmount detaches the filesystem mount and stops this channel.
func (s *Server) Unmount() error {
	if s == nil {
		return nil
	}
	var unmountErr error
	if s.mountPoint != "" {
		if err := unix.Unmount(s.mountPoint, 0); err != nil && !errors.Is(err, unix.EINVAL) && !errors.Is(err, unix.ENOENT) {
			unmountErr = fmt.Errorf("unmount FUSE portal: %w", err)
		}
	}
	if s.stopIdle() {
		return unmountErr
	}
	s.wake()
	<-s.done
	return unmountErr
}

func (s *Server) stopIdle() bool {
	s.serveMu.Lock()
	defer s.serveMu.Unlock()
	if s.serving {
		return false
	}
	s.serving = true
	s.finish()
	return true
}

func (s *Server) wake() {
	if s == nil {
		return
	}
	s.fdMu.Lock()
	defer s.fdMu.Unlock()
	if s.wakeFD < 0 {
		return
	}
	var value [8]byte
	binary.LittleEndian.PutUint64(value[:], 1)
	_, _ = unix.Write(s.wakeFD, value[:])
}

func (s *Server) finish() {
	s.requests.Wait()
	_ = s.closeDescriptors()
	s.fs.OnUnmount()
	s.doneOnce.Do(func() { close(s.done) })
}

func (s *Server) closeDescriptors() error {
	s.fdMu.Lock()
	defer s.fdMu.Unlock()
	var firstErr error
	if s.fd >= 0 {
		if err := unix.Close(s.fd); err != nil && !errors.Is(err, unix.EBADF) {
			firstErr = err
		}
		s.fd = -1
	}
	if s.wakeFD >= 0 {
		if err := unix.Close(s.wakeFD); err != nil && !errors.Is(err, unix.EBADF) && firstErr == nil {
			firstErr = err
		}
		s.wakeFD = -1
	}
	return firstErr
}

func (s *Server) initializeProtocol(request []byte) error {
	out, suppress, err := responseBuffers(request)
	if err != nil {
		return err
	}
	if suppress {
		return fmt.Errorf("FUSE INIT request unexpectedly suppresses replies")
	}
	_, status := s.protocol.HandleRequest([][]byte{request}, out)
	if status != fuse.OK {
		return fmt.Errorf("FUSE protocol status: %s", status)
	}
	return nil
}

func (s *Server) handleAndReply(request []byte) error {
	out, suppress, err := responseBuffers(request)
	if err != nil {
		return err
	}
	written, status := s.protocol.HandleRequest([][]byte{request}, out)
	if status != fuse.OK {
		return s.replyError(request, status)
	}
	if suppress {
		return nil
	}
	iov := trimResponse(out, written)
	if len(iov) == 0 {
		return fmt.Errorf("empty FUSE response for opcode %d", requestOpcode(request))
	}
	for len(iov) > 0 {
		s.fdMu.Lock()
		fd := s.fd
		if fd < 0 {
			s.fdMu.Unlock()
			return unix.EBADF
		}
		n, err := unix.Writev(fd, iov)
		s.fdMu.Unlock()
		if errors.Is(err, unix.EINTR) {
			continue
		}
		if errors.Is(err, unix.ENOENT) || errors.Is(err, unix.ENODEV) || errors.Is(err, unix.EBADF) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("write FUSE response: %w", err)
		}
		iov = consumeIOV(iov, n)
	}
	return nil
}

func (s *Server) replyError(request []byte, status fuse.Status) error {
	if len(request) < fuseOutSize {
		return io.ErrUnexpectedEOF
	}
	response := make([]byte, fuseOutSize)
	binary.LittleEndian.PutUint32(response[0:4], fuseOutSize)
	binary.LittleEndian.PutUint32(response[4:8], uint32(int32(-status)))
	copy(response[8:16], request[8:16])
	s.fdMu.Lock()
	defer s.fdMu.Unlock()
	if s.fd < 0 {
		return unix.EBADF
	}
	_, err := unix.Write(s.fd, response)
	if errors.Is(err, unix.ENOENT) || errors.Is(err, unix.ENODEV) || errors.Is(err, unix.EBADF) {
		return nil
	}
	return err
}

func normalizeOptions(opts *fuse.MountOptions) fuse.MountOptions {
	configured := fuse.MountOptions{}
	if opts != nil {
		configured = *opts
	}
	if configured.Name == "" {
		configured.Name = "sandbox0-volume"
	}
	if configured.FsName == "" {
		configured.FsName = configured.Name
	}
	if configured.MaxWrite <= 0 {
		configured.MaxWrite = 128 * 1024
	}
	if configured.MaxBackground <= 0 {
		configured.MaxBackground = 12
	}
	return configured
}

func absoluteMountPoint(mountPoint string) (string, error) {
	cleaned := filepath.Clean(mountPoint)
	if filepath.IsAbs(cleaned) {
		return cleaned, nil
	}
	cwd, err := os.Getwd()
	if err != nil {
		return "", fmt.Errorf("resolve FUSE mount point: %w", err)
	}
	return filepath.Clean(filepath.Join(cwd, cleaned)), nil
}

func mountDirect(fd int, mountPoint string, opts *fuse.MountOptions) error {
	var st unix.Stat_t
	if err := unix.Stat(mountPoint, &st); err != nil {
		return fmt.Errorf("stat FUSE mount point: %w", err)
	}
	flags := uintptr(syscall.MS_NOSUID | syscall.MS_NODEV)
	if opts.DirectMountFlags != 0 {
		flags = opts.DirectMountFlags
	}
	options := []string{
		fmt.Sprintf("fd=%d", fd),
		fmt.Sprintf("rootmode=%o", st.Mode&unix.S_IFMT),
		fmt.Sprintf("user_id=%d", os.Geteuid()),
		fmt.Sprintf("group_id=%d", os.Getegid()),
		fmt.Sprintf("max_read=%d", opts.MaxWrite),
	}
	if opts.AllowOther {
		options = append(options, "allow_other")
	}
	options = append(options, opts.Options...)
	if err := unix.Mount(opts.FsName, mountPoint, "fuse."+opts.Name, flags, joinMountOptions(options)); err != nil {
		return fmt.Errorf("mount FUSE portal: %w", err)
	}
	return nil
}

func joinMountOptions(options []string) string {
	if len(options) == 0 {
		return ""
	}
	result := options[0]
	for _, option := range options[1:] {
		result += "," + option
	}
	return result
}

func readRequest(fd, maxWrite int) ([]byte, error) {
	if maxWrite < 128*1024 {
		maxWrite = 128 * 1024
	}
	buffer := make([]byte, maxWrite+64*1024)
	for {
		n, err := unix.Read(fd, buffer)
		if errors.Is(err, unix.EINTR) {
			continue
		}
		if err != nil {
			return nil, err
		}
		if n < fuseHeaderSize {
			return nil, io.ErrUnexpectedEOF
		}
		length := int(binary.LittleEndian.Uint32(buffer[:4]))
		if length < fuseHeaderSize || length > n {
			return nil, fmt.Errorf("invalid FUSE request length %d from read of %d", length, n)
		}
		return append([]byte(nil), buffer[:length]...), nil
	}
}

func requestOpcode(request []byte) uint32 {
	if len(request) < 8 {
		return 0
	}
	return binary.LittleEndian.Uint32(request[4:8])
}

func responseBuffers(request []byte) ([][]byte, bool, error) {
	if len(request) < fuseHeaderSize {
		return nil, false, io.ErrUnexpectedEOF
	}
	opcode := requestOpcode(request)
	suppress := opcode == opForget || opcode == opBatchForget || opcode == opNotifyReply
	if suppress {
		return nil, true, nil
	}
	outSize := structuredOutputSize(opcode)
	payloadSize, err := responsePayloadSize(opcode, request)
	if err != nil {
		return nil, false, err
	}
	if (opcode == opGetxattr || opcode == opListxattr) && payloadSize > 0 {
		outSize = 0
	}
	buffers := [][]byte{make([]byte, fuseOutSize)}
	if outSize > 0 {
		buffers = append(buffers, make([]byte, outSize))
	}
	if payloadSize > 0 {
		buffers = append(buffers, make([]byte, payloadSize))
	}
	return buffers, false, nil
}

func structuredOutputSize(opcode uint32) int {
	switch opcode {
	case opLookup, opSymlink, opMknod, opMkdir, opLink:
		return int(unsafe.Sizeof(fuse.EntryOut{}))
	case opGetattr, opSetattr:
		return int(unsafe.Sizeof(fuse.AttrOut{}))
	case opOpen, opOpendir:
		return int(unsafe.Sizeof(fuse.OpenOut{}))
	case opWrite, opCopyFileRange:
		return int(unsafe.Sizeof(fuse.WriteOut{}))
	case opStatfs:
		return int(unsafe.Sizeof(fuse.StatfsOut{}))
	case opGetxattr, opListxattr:
		return int(unsafe.Sizeof(fuse.GetXAttrOut{}))
	case opInit:
		return int(unsafe.Sizeof(fuse.InitOut{}))
	case opGetlk:
		return int(unsafe.Sizeof(fuse.LkOut{}))
	case opCreate:
		return int(unsafe.Sizeof(fuse.CreateOut{}))
	case opIoctl:
		return int(unsafe.Sizeof(fuse.IoctlOut{}))
	case opPoll:
		return 8
	case opBmap:
		return 8
	case opLseek:
		return int(unsafe.Sizeof(fuse.LseekOut{}))
	case opStatx:
		return int(unsafe.Sizeof(fuse.StatxOut{}))
	case opCopyFileRange64:
		return int(unsafe.Sizeof(fuse.CopyFileRangeOut{}))
	default:
		return 0
	}
}

func responsePayloadSize(opcode uint32, request []byte) (int, error) {
	switch opcode {
	case opRead, opReaddir, opReaddirplus:
		return requestUint32(request, 56)
	case opGetxattr, opListxattr:
		return requestUint32(request, 40)
	case opIoctl:
		return requestUint32(request, 68)
	case opReadlink:
		return 64 * 1024, nil
	default:
		return 0, nil
	}
}

func requestUint32(request []byte, offset int) (int, error) {
	if offset < 0 || len(request) < offset+4 {
		return 0, io.ErrUnexpectedEOF
	}
	return int(binary.LittleEndian.Uint32(request[offset : offset+4])), nil
}

func trimResponse(iov [][]byte, length int) [][]byte {
	if length <= 0 {
		return nil
	}
	trimmed := make([][]byte, 0, len(iov))
	remaining := length
	for _, part := range iov {
		if remaining <= 0 {
			break
		}
		if len(part) > remaining {
			part = part[:remaining]
		}
		trimmed = append(trimmed, part)
		remaining -= len(part)
	}
	return trimmed
}

func consumeIOV(iov [][]byte, consumed int) [][]byte {
	for len(iov) > 0 && consumed > 0 {
		if consumed < len(iov[0]) {
			iov[0] = iov[0][consumed:]
			return iov
		}
		consumed -= len(iov[0])
		iov = iov[1:]
	}
	return iov
}
