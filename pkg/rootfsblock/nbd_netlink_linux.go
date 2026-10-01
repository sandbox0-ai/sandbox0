//go:build linux

package rootfsblock

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	nl "github.com/mdlayher/netlink"
	"golang.org/x/sys/unix"
)

// The Linux generic-netlink NBD ABI is independent of the userspace control
// process. Preserve the connected userspace endpoint during planned handoff.
func nbdNetlink(command uint8, attributes []nl.Attribute) error {
	connection, err := nl.Dial(unix.NETLINK_GENERIC, nil)
	if err != nil {
		return err
	}
	defer connection.Close()
	if err := connection.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		return err
	}
	name, err := nl.MarshalAttributes([]nl.Attribute{{Type: 2, Data: append([]byte("nbd"), 0)}})
	if err != nil {
		return err
	}
	replies, err := connection.Execute(nl.Message{Header: nl.Header{Type: unix.GENL_ID_CTRL, Flags: nl.Request}, Data: append([]byte{3, 1, 0, 0}, name...)})
	if err != nil {
		return fmt.Errorf("resolve NBD netlink family: %w", err)
	}
	var family uint16
	for _, reply := range replies {
		if len(reply.Data) < 4 {
			continue
		}
		attrs, err := nl.UnmarshalAttributes(reply.Data[4:])
		if err != nil {
			return err
		}
		for _, attr := range attrs {
			if attr.Type == 1 && len(attr.Data) == 2 {
				family = binary.NativeEndian.Uint16(attr.Data)
			}
		}
	}
	if family == 0 {
		return fmt.Errorf("kernel NBD generic-netlink family unavailable")
	}
	payload, err := nl.MarshalAttributes(attributes)
	if err != nil {
		return err
	}
	_, err = connection.Execute(nl.Message{Header: nl.Header{Type: nl.HeaderType(family), Flags: nl.Request | nl.Acknowledge}, Data: append([]byte{command, 1, 0, 0}, payload...)})
	return err
}

func nbdUint32(value uint32) []byte {
	result := make([]byte, 4)
	binary.NativeEndian.PutUint32(result, value)
	return result
}
func nbdUint64(value uint64) []byte {
	result := make([]byte, 8)
	binary.NativeEndian.PutUint64(result, value)
	return result
}

func nbdIndex(path string) (uint32, error) {
	_, name, err := validateNBDDevicePath(path)
	if err != nil {
		return 0, err
	}
	index, err := strconv.ParseUint(strings.TrimPrefix(name, "nbd"), 10, 32)
	return uint32(index), err
}

func disconnectNetlinkNBD(path string) error {
	index, err := nbdIndex(path)
	if err != nil {
		return err
	}
	return nbdNetlink(2, []nl.Attribute{{Type: 1, Data: nbdUint32(index)}})
}

func startTransferableKernelNBD(lifetime, readyContext context.Context, backend WritableBlockDevice, options KernelNBDOptions) (*KernelNBDDevice, error) {
	if lifetime == nil || readyContext == nil || backend == nil {
		return nil, fmt.Errorf("NBD contexts and backend required")
	}
	geometry, err := kernelNBDGeometry(backend.Size())
	if err != nil {
		return nil, err
	}
	path, name, err := validateNBDDevicePath(options.DevicePath)
	if err != nil {
		return nil, err
	}
	if options.RequestTimeout < 0 || options.ReadyTimeout < 0 {
		return nil, fmt.Errorf("NBD timeouts must be non-negative")
	}
	sysRoot := options.SysBlockRoot
	if sysRoot == "" {
		sysRoot = "/sys/block"
	}
	if err := requireUnusedNBD(filepath.Join(sysRoot, name, "pid")); err != nil {
		return nil, err
	}
	index, err := nbdIndex(path)
	if err != nil {
		return nil, err
	}
	sockets, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	kernelSocket, userSocket := os.NewFile(uintptr(sockets[0]), "nbd-kernel"), os.NewFile(uintptr(sockets[1]), "nbd-userspace")
	defer kernelSocket.Close()
	defer userSocket.Close()
	fd, err := nl.MarshalAttributes([]nl.Attribute{{Type: 1, Data: nbdUint32(uint32(sockets[0]))}})
	if err != nil {
		return nil, err
	}
	item, err := nl.MarshalAttributes([]nl.Attribute{{Type: 1 | nl.Nested, Data: fd}})
	if err != nil {
		return nil, err
	}
	flags := uint64(nbdFlagHasFlags | nbdFlagSendFlush | nbdFlagSendFUA | nbdFlagSendTrim | nbdFlagSendWriteZeros)
	attrs := []nl.Attribute{{Type: 1, Data: nbdUint32(index)}, {Type: 2, Data: nbdUint64(uint64(backend.Size()))}, {Type: 3, Data: nbdUint64(uint64(geometry.sectorSize))}, {Type: 5, Data: nbdUint64(flags)}, {Type: 6, Data: nbdUint64(0)}, {Type: 7 | nl.Nested, Data: item}}
	if options.RequestTimeout > 0 {
		attrs = append(attrs, nl.Attribute{Type: 4, Data: nbdUint64(uint64((options.RequestTimeout + time.Second - 1) / time.Second))})
	}
	if err := nbdNetlink(1, attrs); err != nil {
		return nil, fmt.Errorf("connect kernel-managed NBD: %w", err)
	}
	device, err := adoptTransferableNBD(lifetime, path, userSocket, backend, options.MaxRequestBytes)
	if err != nil {
		_ = disconnectNetlinkNBD(path)
		return nil, err
	}
	readyTimeout := options.ReadyTimeout
	if readyTimeout == 0 {
		readyTimeout = defaultNBDReadyTimeout
	}
	readyCtx, cancel := context.WithTimeout(readyContext, readyTimeout)
	defer cancel()
	if err := waitNBDReady(readyCtx, filepath.Join(sysRoot, name, "pid"), device.done, device.result); err != nil {
		_ = device.Close()
		return nil, err
	}
	return device, nil
}

// AdoptKernelNBD accepts only a planned connected endpoint, never an orphan
// reconstruction. The session manager must validate its exact durable binding.
func AdoptKernelNBD(lifetime context.Context, path string, socket *os.File, backend WritableBlockDevice) (*KernelNBDDevice, error) {
	return adoptTransferableNBD(lifetime, path, socket, backend, 0)
}

func adoptTransferableNBD(lifetime context.Context, path string, socket *os.File, backend WritableBlockDevice, maximum uint32) (*KernelNBDDevice, error) {
	if lifetime == nil || socket == nil || backend == nil {
		return nil, fmt.Errorf("NBD adoption context, socket and backend required")
	}
	if _, _, err := validateNBDDevicePath(path); err != nil {
		return nil, err
	}
	connection, err := net.FileConn(socket)
	if err != nil {
		return nil, err
	}
	file, err := os.OpenFile(path, os.O_RDWR|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		connection.Close()
		return nil, err
	}
	ctx, cancel := context.WithCancel(lifetime)
	device := &KernelNBDDevice{path: path, file: file, connection: connection, cancel: cancel, done: make(chan struct{}), handoff: &NBDTransmissionControl{}, netlink: true}
	go func() {
		err := (NBDTransmissionServer{Backend: backend, MaxRequestBytes: maximum, Handoff: device.handoff, OnBackendError: device.recordRequestError}).Serve(ctx, connection)
		if errors.Is(err, ErrNBDHandoff) {
			err = nil
		} else if !device.closing.Load() {
			_ = disconnectNetlinkNBD(path)
		}
		device.resultMu.Lock()
		device.runErr = ignoreNBDServerStopped(err)
		device.resultMu.Unlock()
		close(device.done)
	}()
	return device, nil
}

func (d *KernelNBDDevice) PrepareHandoff(ctx context.Context) (*os.File, error) {
	if !d.netlink || d.handoff == nil {
		return nil, fmt.Errorf("legacy ioctl-owned NBD device cannot transfer")
	}
	if err := d.handoff.Pause(ctx); err != nil {
		return nil, err
	}
	file, err := d.handoff.File()
	if err != nil {
		d.handoff.Resume()
	}
	return file, err
}

func (d *KernelNBDDevice) AbortHandoff() {
	if d.handoff != nil {
		d.handoff.Resume()
	}
}

func (d *KernelNBDDevice) CommitHandoff() error {
	if !d.netlink || d.handoff == nil {
		return fmt.Errorf("legacy NBD ownership cannot transfer")
	}
	d.detached.Store(true)
	if err := d.handoff.Detach(); err != nil {
		d.detached.Store(false)
		return err
	}
	return d.Wait()
}
