//go:build linux

package rootfsblock

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
	"unsafe"

	"golang.org/x/sys/unix"
)

const (
	nbdSetSock       = 0xab00
	nbdSetBlockSize  = 0xab01
	nbdDoIt          = 0xab03
	nbdClearSock     = 0xab04
	nbdClearQueue    = 0xab05
	nbdSetSizeBlocks = 0xab07
	nbdDisconnect    = 0xab08
	nbdSetTimeout    = 0xab09
	nbdSetFlags      = 0xab0a

	nbdFlagHasFlags       = 1 << 0
	nbdFlagSendFlush      = 1 << 2
	nbdFlagSendFUA        = 1 << 3
	nbdFlagSendTrim       = 1 << 5
	nbdFlagSendWriteZeros = 1 << 6

	defaultNBDReadyTimeout = 5 * time.Second
)

var nbdDeviceName = regexp.MustCompile(`^nbd[0-9]+$`)

type KernelNBDOptions struct {
	DevicePath      string
	RequestTimeout  time.Duration
	ReadyTimeout    time.Duration
	MaxRequestBytes uint32
	SysBlockRoot    string
}

// KernelNBDDevice binds one WritableBlockDevice to an existing /dev/nbdN
// device. The caller owns exclusive device allocation and must keep this
// object alive until every filesystem and runtime reference is gone.
type KernelNBDDevice struct {
	path       string
	file       *os.File
	connection net.Conn
	cancel     context.CancelFunc
	done       chan struct{}
	closeOnce  sync.Once
	closing    atomic.Bool
	resultMu   sync.RWMutex
	runErr     error
	requestErr error
	closeErr   error
}

func StartKernelNBD(lifetime, readyContext context.Context, backend WritableBlockDevice, options KernelNBDOptions) (*KernelNBDDevice, error) {
	if backend == nil {
		return nil, fmt.Errorf("NBD backend is required")
	}
	if lifetime == nil || readyContext == nil {
		return nil, fmt.Errorf("NBD lifetime and readiness contexts are required")
	}
	geometry, err := kernelNBDGeometry(backend.Size())
	if err != nil {
		return nil, err
	}
	devicePath, deviceName, err := validateNBDDevicePath(options.DevicePath)
	if err != nil {
		return nil, err
	}
	if options.RequestTimeout < 0 || options.ReadyTimeout < 0 {
		return nil, fmt.Errorf("NBD timeouts must be non-negative")
	}
	readyTimeout := options.ReadyTimeout
	if readyTimeout == 0 {
		readyTimeout = defaultNBDReadyTimeout
	}
	sysBlockRoot := strings.TrimSpace(options.SysBlockRoot)
	if sysBlockRoot == "" {
		sysBlockRoot = "/sys/block"
	}
	if err := requireUnusedNBD(filepath.Join(sysBlockRoot, deviceName, "pid")); err != nil {
		return nil, err
	}

	file, err := os.OpenFile(devicePath, os.O_RDWR|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return nil, fmt.Errorf("open NBD device %s: %w", devicePath, err)
	}
	info, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("inspect NBD device %s: %w", devicePath, err)
	}
	if info.Mode()&os.ModeDevice == 0 || info.Mode()&os.ModeCharDevice != 0 {
		file.Close()
		return nil, fmt.Errorf("NBD path %s is not a block device", devicePath)
	}

	sockets, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("create NBD socket pair: %w", err)
	}
	kernelSocket := os.NewFile(uintptr(sockets[0]), "nbd-kernel")
	userSocket := os.NewFile(uintptr(sockets[1]), "nbd-userspace")
	closeSockets := func() {
		_ = kernelSocket.Close()
		_ = userSocket.Close()
	}
	if err := ioctlSetInt(file, nbdSetSock, sockets[0]); err != nil {
		closeSockets()
		file.Close()
		return nil, fmt.Errorf("set NBD socket: %w", err)
	}
	if err := ioctlSetInt(file, nbdSetBlockSize, geometry.sectorSize); err != nil {
		_ = ioctlSetInt(file, nbdClearSock, 0)
		closeSockets()
		file.Close()
		return nil, fmt.Errorf("set NBD block size: %w", err)
	}
	if err := ioctlSetInt(file, nbdSetSizeBlocks, geometry.sectorCount); err != nil {
		_ = ioctlSetInt(file, nbdClearSock, 0)
		closeSockets()
		file.Close()
		return nil, fmt.Errorf("set NBD logical size: %w", err)
	}
	flags := nbdFlagHasFlags | nbdFlagSendFlush | nbdFlagSendFUA | nbdFlagSendTrim | nbdFlagSendWriteZeros
	if err := ioctlSetInt(file, nbdSetFlags, flags); err != nil {
		_ = ioctlSetInt(file, nbdClearSock, 0)
		closeSockets()
		file.Close()
		return nil, fmt.Errorf("set NBD transmission flags: %w", err)
	}
	if options.RequestTimeout > 0 {
		seconds := int((options.RequestTimeout + time.Second - 1) / time.Second)
		if err := ioctlSetInt(file, nbdSetTimeout, seconds); err != nil {
			_ = ioctlSetInt(file, nbdClearSock, 0)
			closeSockets()
			file.Close()
			return nil, fmt.Errorf("set NBD request timeout: %w", err)
		}
	}
	// NBD_SET_SOCK retains the kernel endpoint. Drop our duplicate so an
	// accidental userspace FD leak cannot keep the transmission alive.
	if err := kernelSocket.Close(); err != nil {
		_ = ioctlSetInt(file, nbdClearSock, 0)
		_ = userSocket.Close()
		file.Close()
		return nil, fmt.Errorf("close duplicate NBD kernel socket: %w", err)
	}
	connection, err := net.FileConn(userSocket)
	_ = userSocket.Close()
	if err != nil {
		_ = ioctlSetInt(file, nbdClearSock, 0)
		file.Close()
		return nil, fmt.Errorf("adopt NBD userspace socket: %w", err)
	}

	lifetime, cancel := context.WithCancel(lifetime)
	device := &KernelNBDDevice{
		path: devicePath, file: file, connection: connection, cancel: cancel, done: make(chan struct{}),
	}
	serverDone := make(chan error, 1)
	go func() {
		err := (NBDTransmissionServer{
			Backend: backend, MaxRequestBytes: options.MaxRequestBytes,
			OnBackendError: device.recordRequestError,
		}).Serve(lifetime, connection)
		serverDone <- err
		if err != nil && !errors.Is(err, context.Canceled) {
			_ = ioctlSetInt(file, nbdDisconnect, 0)
		}
	}()
	go func() {
		doErr := runKernelNBD(file)
		cancel()
		_ = connection.Close()
		serverErr := <-serverDone
		clearQueueErr := ignoreNBDStopped(ioctlSetInt(file, nbdClearQueue, 0))
		clearSockErr := ignoreNBDStopped(ioctlSetInt(file, nbdClearSock, 0))
		if device.closing.Load() {
			doErr = ignoreNBDStopped(doErr)
			serverErr = ignoreNBDServerStopped(serverErr)
		}
		device.resultMu.Lock()
		device.runErr = errors.Join(doErr, serverErr, clearQueueErr, clearSockErr)
		device.resultMu.Unlock()
		close(device.done)
	}()

	readyCtx, stopReady := context.WithTimeout(readyContext, readyTimeout)
	defer stopReady()
	if err := waitNBDReady(readyCtx, filepath.Join(sysBlockRoot, deviceName, "pid"), device.done, device.result); err != nil {
		_ = device.Close()
		return nil, err
	}
	return device, nil
}

// runKernelNBD prevents asynchronous signals from interrupting NBD_DO_IT. The
// kernel treats any signal interruption as a request to tear down every NBD
// socket, so the wait must own one OS thread and mask signals for its entire
// lifetime. Process-directed shutdown signals remain deliverable to the other
// Go runtime threads, which cancel the NBD lifetime through the normal path.
func runKernelNBD(file *os.File) error {
	return withKernelNBDSignalMask(func() error { return ioctlSetInt(file, nbdDoIt, 0) })
}

func withKernelNBDSignalMask(operation func() error) (result error) {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	mask := unix.Sigset_t{}
	fillKernelNBDSignalMask(&mask)
	var oldMask unix.Sigset_t
	if err := unix.PthreadSigmask(unix.SIG_BLOCK, &mask, &oldMask); err != nil {
		return fmt.Errorf("block NBD interrupt signal: %w", err)
	}
	defer func() {
		if err := unix.PthreadSigmask(unix.SIG_SETMASK, &oldMask, nil); err != nil {
			result = errors.Join(result, fmt.Errorf("restore NBD signal mask: %w", err))
		}
	}()
	return operation()
}

func fillKernelNBDSignalMask(mask *unix.Sigset_t) {
	for signal := unix.Signal(1); signal <= 64; signal++ {
		setSignalMaskBit(mask, signal)
	}
}

func setSignalMaskBit(mask *unix.Sigset_t, signal unix.Signal) {
	index := int(signal) - 1
	bitsPerWord := int(8 * unsafe.Sizeof(uintptr(0)))
	words := unsafe.Slice((*uintptr)(unsafe.Pointer(mask)), int(unsafe.Sizeof(*mask)/unsafe.Sizeof(uintptr(0))))
	if index < 0 || index/bitsPerWord >= len(words) {
		panic("signal is outside the native signal set")
	}
	words[index/bitsPerWord] |= uintptr(1) << uint(index%bitsPerWord)
}

func signalMaskContains(mask *unix.Sigset_t, signal unix.Signal) bool {
	index := int(signal) - 1
	bitsPerWord := int(8 * unsafe.Sizeof(uintptr(0)))
	words := unsafe.Slice((*uintptr)(unsafe.Pointer(mask)), int(unsafe.Sizeof(*mask)/unsafe.Sizeof(uintptr(0))))
	if index < 0 || index/bitsPerWord >= len(words) {
		return false
	}
	return words[index/bitsPerWord]&(uintptr(1)<<uint(index%bitsPerWord)) != 0
}

func (d *KernelNBDDevice) Path() string { return d.path }

func (d *KernelNBDDevice) Wait() error {
	<-d.done
	return d.result()
}

func (d *KernelNBDDevice) Close() error {
	d.closeOnce.Do(func() {
		d.closing.Store(true)
		disconnectErr := ignoreNBDStopped(ioctlSetInt(d.file, nbdDisconnect, 0))
		d.cancel()
		connectionErr := ignoreNBDServerStopped(d.connection.Close())
		runErr := d.Wait()
		fileErr := d.file.Close()
		d.resultMu.Lock()
		d.closeErr = errors.Join(disconnectErr, connectionErr, runErr, d.requestErr, fileErr)
		d.resultMu.Unlock()
	})
	d.resultMu.RLock()
	defer d.resultMu.RUnlock()
	return d.closeErr
}

// RecoverOrphanKernelNBD clears an exact kernel attachment after its userspace
// NBD owner has disappeared. Callers must already have fenced the allocation
// and removed every filesystem and runtime reference to the device.
func RecoverOrphanKernelNBD(ctx context.Context, devicePath, sysBlockRoot string) error {
	if ctx == nil {
		return fmt.Errorf("NBD recovery context is required")
	}
	devicePath, deviceName, err := validateNBDDevicePath(devicePath)
	if err != nil {
		return err
	}
	sysBlockRoot = strings.TrimSpace(sysBlockRoot)
	if sysBlockRoot == "" {
		sysBlockRoot = "/sys/block"
	}
	if !filepath.IsAbs(sysBlockRoot) || filepath.Clean(sysBlockRoot) == "/" {
		return fmt.Errorf("sys block root must be a non-root absolute path")
	}
	pidPath := filepath.Join(filepath.Clean(sysBlockRoot), deviceName, "pid")
	sizePath := filepath.Join(filepath.Clean(sysBlockRoot), deviceName, "size")
	if _, statErr := os.Stat(filepath.Dir(pidPath)); errors.Is(statErr, os.ErrNotExist) {
		// If the kernel endpoint itself is absent, no stale userspace process,
		// mount, or open block node can keep this NBD allocation attached.
		return nil
	} else if statErr != nil {
		return fmt.Errorf("inspect orphan NBD endpoint %s: %w", devicePath, statErr)
	}
	unused, observeErr := orphanNBDIsUnused(pidPath, sizePath)
	if observeErr != nil {
		return fmt.Errorf("observe orphan NBD device %s: %w", devicePath, observeErr)
	}
	if unused {
		return nil
	}
	file, err := os.OpenFile(devicePath, os.O_RDWR|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return fmt.Errorf("open orphan NBD device %s: %w", devicePath, err)
	}
	info, statErr := file.Stat()
	if statErr != nil || info.Mode()&os.ModeDevice == 0 || info.Mode()&os.ModeCharDevice != 0 {
		_ = file.Close()
		if statErr != nil {
			return fmt.Errorf("inspect orphan NBD device %s: %w", devicePath, statErr)
		}
		return fmt.Errorf("orphan NBD path %s is not a block device", devicePath)
	}
	disconnectErr := ignoreNBDStopped(ioctlSetInt(file, nbdDisconnect, 0))
	clearQueueErr := ignoreNBDStopped(ioctlSetInt(file, nbdClearQueue, 0))
	clearSockErr := ignoreNBDStopped(ioctlSetInt(file, nbdClearSock, 0))
	clearSizeErr := ignoreNBDStopped(ioctlSetInt(file, nbdSetSizeBlocks, 0))
	closeErr := file.Close()
	if err := errors.Join(disconnectErr, clearQueueErr, clearSockErr, clearSizeErr, closeErr); err != nil {
		return fmt.Errorf("disconnect orphan NBD device %s: %w", devicePath, err)
	}

	waitCtx, cancel := context.WithTimeout(ctx, defaultNBDReadyTimeout)
	defer cancel()
	poll := time.NewTicker(time.Millisecond)
	defer poll.Stop()
	for {
		unused, observeErr := orphanNBDIsUnused(pidPath, sizePath)
		if observeErr != nil {
			return fmt.Errorf("observe orphan NBD device %s: %w", devicePath, observeErr)
		}
		if unused {
			return nil
		}
		select {
		case <-waitCtx.Done():
			return fmt.Errorf("wait for orphan NBD device %s to disconnect: %w", devicePath, waitCtx.Err())
		case <-poll.C:
		}
	}
}

func orphanNBDIsUnused(pidPath, sizePath string) (bool, error) {
	payload, err := os.ReadFile(pidPath)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return false, err
	}
	if err == nil {
		pid, parseErr := strconv.Atoi(strings.TrimSpace(string(payload)))
		if parseErr != nil {
			return false, fmt.Errorf("NBD owner %s is invalid", pidPath)
		}
		if pid != 0 {
			return false, nil
		}
	}
	sizePayload, err := os.ReadFile(sizePath)
	if err != nil {
		return false, err
	}
	sectors, err := strconv.ParseUint(strings.TrimSpace(string(sizePayload)), 10, 64)
	if err != nil {
		return false, fmt.Errorf("NBD size %s is invalid", sizePath)
	}
	return sectors == 0, nil
}

func (d *KernelNBDDevice) recordRequestError(err error) {
	if err == nil {
		return
	}
	// ENOSPC was already returned for the exact request and is retained in the
	// branch admission diagnostics. Linux NBD maps every remote errno to
	// block-layer EIO, so the caller's sync/unmount path still observes any XFS
	// consequence; duplicating it as a sticky transport failure adds no proof.
	// Integrity and backend I/O errors remain sticky and fail terminal cleanup.
	if errors.Is(err, syscall.ENOSPC) {
		return
	}
	d.resultMu.Lock()
	defer d.resultMu.Unlock()
	if d.requestErr == nil {
		d.requestErr = err
	}
}

func (d *KernelNBDDevice) result() error {
	d.resultMu.RLock()
	defer d.resultMu.RUnlock()
	return d.runErr
}

type nbdGeometry struct {
	sectorSize  int
	sectorCount int
}

func kernelNBDGeometry(size int64) (nbdGeometry, error) {
	if size <= 0 || size%LogicalBlockSize != 0 {
		return nbdGeometry{}, fmt.Errorf("NBD backend size must be a positive multiple of %d", LogicalBlockSize)
	}
	if size%NBDDeviceSectorSize != 0 {
		return nbdGeometry{}, fmt.Errorf("NBD backend size must be a multiple of the %d-byte device sector", NBDDeviceSectorSize)
	}
	sectors := size / NBDDeviceSectorSize
	if int64(int(sectors)) != sectors {
		return nbdGeometry{}, fmt.Errorf("NBD backend has too many device sectors: %d", sectors)
	}
	return nbdGeometry{sectorSize: NBDDeviceSectorSize, sectorCount: int(sectors)}, nil
}

func validateNBDDevicePath(path string) (string, string, error) {
	path = filepath.Clean(strings.TrimSpace(path))
	if !filepath.IsAbs(path) || filepath.Dir(path) != "/dev" {
		return "", "", fmt.Errorf("NBD device must be an absolute /dev/nbdN path")
	}
	name := filepath.Base(path)
	if !nbdDeviceName.MatchString(name) {
		return "", "", fmt.Errorf("NBD device name %q is invalid", name)
	}
	return path, name, nil
}

func requireUnusedNBD(pidPath string) error {
	payload, err := os.ReadFile(pidPath)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("inspect NBD ownership %s: %w", pidPath, err)
	}
	if strings.TrimSpace(string(payload)) != "" && strings.TrimSpace(string(payload)) != "0" {
		return fmt.Errorf("NBD device is already owned by PID %s", strings.TrimSpace(string(payload)))
	}
	return nil
}

func waitNBDReady(ctx context.Context, pidPath string, done <-chan struct{}, result func() error) error {
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		payload, err := os.ReadFile(pidPath)
		if err == nil {
			pid, parseErr := strconv.Atoi(strings.TrimSpace(string(payload)))
			if parseErr == nil && pid > 1 {
				return nil
			}
		} else if !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("observe NBD readiness: %w", err)
		}
		select {
		case <-done:
			err := result()
			if err == nil {
				return fmt.Errorf("NBD worker exited before the device became ready")
			}
			return fmt.Errorf("NBD worker exited before readiness: %w", err)
		case <-ctx.Done():
			return fmt.Errorf("wait for NBD readiness: %w", ctx.Err())
		case <-ticker.C:
		}
	}
}

func ioctlSetInt(file *os.File, request uint, value int) error {
	return unix.IoctlSetInt(int(file.Fd()), request, value)
}

func ignoreNBDStopped(err error) error {
	if errors.Is(err, unix.EINVAL) || errors.Is(err, unix.ENOTCONN) || errors.Is(err, unix.EPIPE) {
		return nil
	}
	return err
}

func ignoreNBDServerStopped(err error) error {
	if errors.Is(err, context.Canceled) || errors.Is(err, net.ErrClosed) || errors.Is(err, os.ErrClosed) ||
		errors.Is(err, syscall.EPIPE) || errors.Is(err, syscall.ECONNRESET) {
		return nil
	}
	return err
}
