package rootfsblock

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"syscall"
)

const (
	nbdRequestMagic = 0x25609513
	nbdReplyMagic   = 0x67446698

	nbdCommandRead       = 0
	nbdCommandWrite      = 1
	nbdCommandDisconnect = 2
	nbdCommandFlush      = 3
	nbdCommandTrim       = 4
	nbdCommandWriteZero  = 6

	nbdCommandMask       = 0x0000ffff
	nbdCommandFlagFUA    = 1 << 16
	nbdCommandFlagNoHole = 1 << 17

	nbdRequestHeaderBytes = 28
	nbdReplyHeaderBytes   = 16
	DefaultNBDMaxRequest  = 8 << 20

	// NBDDeviceSectorSize is the logical sector size advertised to Linux. It
	// is deliberately independent from LogicalBlockSize, which is the
	// persistence mapping granularity. The canonical XFS artifacts use
	// 512-byte sectors while the block map records 4 KiB updates.
	NBDDeviceSectorSize = 512
)

// WritableBlockDevice is the persistence boundary exported to the Linux NBD
// transport. Flush makes all previously completed writes durable on the
// current node; it does not imply regional durability.
type WritableBlockDevice interface {
	io.ReaderAt
	io.WriterAt
	Size() int64
	Flush() error
	Trim(offset, length int64) error
	WriteZeroes(offset, length int64) error
}

// NBDTransmissionServer serves the kernel's simple NBD transmission protocol
// over an already-connected Unix socket. Requests are executed in wire order,
// which provides an intentionally conservative ordering baseline for XFS.
type NBDTransmissionServer struct {
	Backend         WritableBlockDevice
	MaxRequestBytes uint32
	// OnBackendError observes the original storage error before it is reduced
	// to an NBD errno. It must not block the transmission loop.
	OnBackendError func(error)
}

func (s NBDTransmissionServer) Serve(ctx context.Context, connection net.Conn) error {
	if s.Backend == nil || connection == nil {
		return fmt.Errorf("NBD backend and connection are required")
	}
	maximum := s.MaxRequestBytes
	if maximum == 0 {
		maximum = DefaultNBDMaxRequest
	}
	if maximum < LogicalBlockSize {
		return fmt.Errorf("NBD maximum request must be at least %d bytes", LogicalBlockSize)
	}
	server := nbdTransmission{
		backend: s.Backend, connection: connection, maximum: maximum,
		onBackendError: s.OnBackendError,
	}
	stop := context.AfterFunc(ctx, func() { _ = connection.Close() })
	defer stop()
	for {
		disconnect, err := server.serveOne()
		if err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return ctxErr
			}
			if errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) {
				return nil
			}
			return err
		}
		if disconnect {
			return nil
		}
	}
}

type nbdTransmission struct {
	backend        WritableBlockDevice
	connection     net.Conn
	maximum        uint32
	onBackendError func(error)
	replyMu        sync.Mutex
}

type nbdRequest struct {
	typeAndFlags uint32
	handle       [8]byte
	offset       uint64
	length       uint32
}

func (s *nbdTransmission) serveOne() (bool, error) {
	request, err := readNBDRequest(s.connection)
	if err != nil {
		return false, err
	}
	command := request.typeAndFlags & nbdCommandMask
	flags := request.typeAndFlags &^ nbdCommandMask
	if request.length > s.maximum && (command == nbdCommandRead || command == nbdCommandWrite) {
		// A WRITE body follows the header on the same stream. Terminating is the
		// only safe response to an impossible size because replying without
		// consuming it would desynchronize every subsequent request. Commands
		// without payloads may legally cover much larger ranges than the maximum
		// payload size and are handled without allocating that range.
		if command == nbdCommandWrite {
			return false, fmt.Errorf("NBD request length %d exceeds limit %d", request.length, s.maximum)
		}
		return false, s.reply(request.handle, syscall.EINVAL, nil)
	}
	if request.offset > uint64(s.backend.Size()) || uint64(request.length) > uint64(s.backend.Size())-request.offset {
		if command == nbdCommandWrite {
			return false, fmt.Errorf("out-of-range NBD write would desynchronize the connection")
		}
		return false, s.reply(request.handle, syscall.EINVAL, nil)
	}

	switch command {
	case nbdCommandRead:
		if flags != 0 || request.length == 0 {
			return false, s.reply(request.handle, syscall.EINVAL, nil)
		}
		payload := make([]byte, request.length)
		n, readErr := s.backend.ReadAt(payload, int64(request.offset))
		if readErr != nil || n != len(payload) {
			if readErr == nil {
				readErr = io.ErrUnexpectedEOF
			}
			s.observeBackendError(fmt.Errorf(
				"read offset %d length %d: %w", request.offset, request.length, readErr,
			))
			return false, s.reply(request.handle, nbdErrno(readErr), nil)
		}
		return false, s.reply(request.handle, 0, payload)

	case nbdCommandWrite:
		if flags & ^uint32(nbdCommandFlagFUA) != 0 || request.length == 0 {
			return false, fmt.Errorf("invalid NBD write flags or length")
		}
		payload := make([]byte, request.length)
		if _, err := io.ReadFull(s.connection, payload); err != nil {
			return false, fmt.Errorf("read NBD write payload: %w", err)
		}
		n, writeErr := s.backend.WriteAt(payload, int64(request.offset))
		if writeErr == nil && n != len(payload) {
			writeErr = io.ErrShortWrite
		}
		if writeErr == nil && flags&nbdCommandFlagFUA != 0 {
			writeErr = s.backend.Flush()
		}
		if writeErr != nil {
			s.observeBackendError(fmt.Errorf(
				"write offset %d length %d: %w", request.offset, request.length, writeErr,
			))
		}
		return false, s.reply(request.handle, nbdErrno(writeErr), nil)

	case nbdCommandFlush:
		if flags != 0 || request.offset != 0 || request.length != 0 {
			return false, s.reply(request.handle, syscall.EINVAL, nil)
		}
		flushErr := s.backend.Flush()
		if flushErr != nil {
			s.observeBackendError(fmt.Errorf("flush: %w", flushErr))
		}
		return false, s.reply(request.handle, nbdErrno(flushErr), nil)

	case nbdCommandTrim, nbdCommandWriteZero:
		allowedFlags := uint32(nbdCommandFlagFUA)
		if command == nbdCommandWriteZero {
			allowedFlags |= nbdCommandFlagNoHole
		}
		if flags & ^allowedFlags != 0 || request.length == 0 {
			return false, s.reply(request.handle, syscall.EINVAL, nil)
		}
		var err error
		if command == nbdCommandTrim {
			err = s.backend.Trim(int64(request.offset), int64(request.length))
		} else {
			err = s.backend.WriteZeroes(int64(request.offset), int64(request.length))
		}
		if err == nil && flags&nbdCommandFlagFUA != 0 {
			err = s.backend.Flush()
		}
		if err != nil {
			s.observeBackendError(fmt.Errorf(
				"zero offset %d length %d: %w", request.offset, request.length, err,
			))
		}
		return false, s.reply(request.handle, nbdErrno(err), nil)

	case nbdCommandDisconnect:
		if flags != 0 || request.offset != 0 || request.length != 0 {
			return false, s.reply(request.handle, syscall.EINVAL, nil)
		}
		return true, nil

	default:
		return false, s.reply(request.handle, syscall.EINVAL, nil)
	}
}

func (s *nbdTransmission) observeBackendError(err error) {
	if err != nil && s.onBackendError != nil {
		s.onBackendError(err)
	}
}

func readNBDRequest(reader io.Reader) (nbdRequest, error) {
	header := make([]byte, nbdRequestHeaderBytes)
	if _, err := io.ReadFull(reader, header); err != nil {
		return nbdRequest{}, err
	}
	if binary.BigEndian.Uint32(header[:4]) != nbdRequestMagic {
		return nbdRequest{}, fmt.Errorf("invalid NBD request magic")
	}
	request := nbdRequest{
		typeAndFlags: binary.BigEndian.Uint32(header[4:8]),
		offset:       binary.BigEndian.Uint64(header[16:24]),
		length:       binary.BigEndian.Uint32(header[24:28]),
	}
	copy(request.handle[:], header[8:16])
	return request, nil
}

func (s *nbdTransmission) reply(handle [8]byte, errno syscall.Errno, payload []byte) error {
	header := make([]byte, nbdReplyHeaderBytes)
	binary.BigEndian.PutUint32(header[:4], nbdReplyMagic)
	binary.BigEndian.PutUint32(header[4:8], uint32(errno))
	copy(header[8:16], handle[:])
	s.replyMu.Lock()
	defer s.replyMu.Unlock()
	if err := writeFull(s.connection, header); err != nil {
		return fmt.Errorf("write NBD reply: %w", err)
	}
	if len(payload) > 0 {
		if err := writeFull(s.connection, payload); err != nil {
			return fmt.Errorf("write NBD read payload: %w", err)
		}
	}
	return nil
}

func writeFull(writer io.Writer, payload []byte) error {
	for len(payload) > 0 {
		written, err := writer.Write(payload)
		if err != nil {
			return err
		}
		if written == 0 {
			return io.ErrShortWrite
		}
		payload = payload[written:]
	}
	return nil
}

func nbdErrno(err error) syscall.Errno {
	if err == nil {
		return 0
	}
	var errno syscall.Errno
	if errors.As(err, &errno) && errno != 0 {
		return errno
	}
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.ErrShortWrite) {
		return syscall.EIO
	}
	return syscall.EIO
}
