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

	"golang.org/x/sync/semaphore"
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

	// Small kernel reads can all wait on one larger immutable range. A wider
	// demand window can reach independent ranges, while separately retaining
	// the original eight-maximum-payload budget through the complete reply.
	nbdMaxConcurrentReads = 64
	nbdReadBufferSlots    = 8

	// NBDDeviceSectorSize is the logical sector size advertised to Linux. It
	// is deliberately independent from LogicalBlockSize, which is the
	// persistence mapping granularity. The canonical XFS artifacts use
	// 512-byte sectors while the block map records 4 KiB updates.
	NBDDeviceSectorSize = 512
)

var errNBDRequestMagic = errors.New("invalid NBD request magic")

// WritableBlockDevice is the persistence boundary exported to the Linux NBD
// transport. Flush makes all previously completed writes durable on the
// current node; it does not imply regional durability. ReadAt follows the
// io.ReaderAt contract, including support for concurrent calls.
type WritableBlockDevice interface {
	io.ReaderAt
	io.WriterAt
	Size() int64
	Flush() error
	Trim(offset, length int64) error
	WriteZeroes(offset, length int64) error
}

// NBDTransmissionServer serves the kernel's simple NBD transmission protocol
// over an already-connected Unix socket. Consecutive valid reads may execute
// and reply out of order. Every other request is a barrier: earlier reads and
// their replies finish before it executes, and its reply precedes later work.
type NBDTransmissionServer struct {
	Backend         WritableBlockDevice
	MaxRequestBytes uint32
	// OnBackendError observes the original storage error before it is reduced
	// to an NBD errno. Calls are serialized, but concurrent read errors may be
	// observed out of wire order. It must not block the transmission loop.
	OnBackendError func(error)
}

// Once transmission starts, Serve closes the connection on termination and
// joins all backend calls and error observers before returning, including on
// cancellation or socket error.
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
	runCtx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	server := nbdTransmission{
		backend: s.Backend, connection: connection, maximum: maximum,
		onBackendError: s.OnBackendError,
		ctx:            runCtx, fail: cancel, readSlots: make(chan struct{}, nbdMaxConcurrentReads),
		readBytes: semaphore.NewWeighted(int64(maximum) * nbdReadBufferSlots),
	}
	stop := context.AfterFunc(runCtx, func() { _ = connection.Close() })
	defer stop()
	var err error
	for {
		var disconnect bool
		disconnect, err = server.serveOne()
		if err != nil || disconnect {
			break
		}
	}
	// A reply failure cancels admission and closes the socket even if the
	// parser is blocked reading a header or waiting for read admission. Preserve
	// that original failure rather than the resulting closed-socket error.
	cancel(err)
	if err != nil {
		err = context.Cause(runCtx)
	}
	_ = connection.Close()
	// ReaderAt has no cancellation method. Returning before every call and
	// error observer finishes would let KernelNBDDevice.Close proceed into
	// terminal branch cleanup with backend operations still active.
	server.reads.Wait()
	// Closing a failed transmission wakes NBD_DO_IT, whose waiter cancels
	// our parent lifetime while reads may still be draining. Keep an earlier
	// transport failure; only report parent cancellation when it caused the
	// shutdown (or followed a clean disconnect).
	if ctxErr := ctx.Err(); ctxErr != nil && (err == nil || errors.Is(err, context.Cause(ctx))) {
		return ctxErr
	}
	if errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) {
		return nil
	}
	return err
}

type nbdTransmission struct {
	backend        WritableBlockDevice
	connection     net.Conn
	maximum        uint32
	onBackendError func(error)
	replyMu        sync.Mutex
	backendErrorMu sync.Mutex
	ctx            context.Context
	fail           context.CancelCauseFunc
	readSlots      chan struct{}
	readBytes      *semaphore.Weighted
	reads          sync.WaitGroup
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
		if errors.Is(err, errNBDRequestMagic) {
			// A malformed header is still an ordering barrier. Transport errors
			// instead abort immediately so blocked replies can be joined.
			s.reads.Wait()
		}
		return false, err
	}
	if request.typeAndFlags == nbdCommandRead && request.length > 0 &&
		request.length <= s.maximum && request.inRange(s.backend.Size()) {
		select {
		case s.readSlots <- struct{}{}:
		case <-s.ctx.Done():
			return false, s.ctx.Err()
		}
		if err := s.readBytes.Acquire(s.ctx, int64(request.length)); err != nil {
			<-s.readSlots
			return false, err
		}
		if err := s.ctx.Err(); err != nil {
			s.readBytes.Release(int64(request.length))
			<-s.readSlots
			return false, err
		}
		s.reads.Add(1)
		go func() {
			defer s.reads.Done()
			defer func() { <-s.readSlots }()
			defer s.readBytes.Release(int64(request.length))
			if s.ctx.Err() != nil {
				return
			}
			s.serveRead(request)
		}()
		return false, nil
	}

	// Only valid READs bypass this barrier. In particular, an invalid READ or
	// unknown command must not reply ahead of earlier reads or let later work
	// pass it. WRITE payloads are not allocated or consumed until reads drain.
	s.reads.Wait()
	if err := s.ctx.Err(); err != nil {
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
	if !request.inRange(s.backend.Size()) {
		if command == nbdCommandWrite {
			return false, fmt.Errorf("out-of-range NBD write would desynchronize the connection")
		}
		return false, s.reply(request.handle, syscall.EINVAL, nil)
	}

	switch command {
	case nbdCommandRead:
		return false, s.reply(request.handle, syscall.EINVAL, nil)

	case nbdCommandWrite:
		if flags & ^uint32(nbdCommandFlagFUA) != 0 || request.length == 0 {
			return false, fmt.Errorf("invalid NBD write flags or length")
		}
		payload := make([]byte, request.length)
		if _, err := io.ReadFull(s.connection, payload); err != nil {
			return false, fmt.Errorf("read NBD write payload: %w", err)
		}
		if err := s.ctx.Err(); err != nil {
			return false, err
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

func (r nbdRequest) inRange(size int64) bool {
	return size >= 0 && r.offset <= uint64(size) && uint64(r.length) <= uint64(size)-r.offset
}

func (s *nbdTransmission) serveRead(request nbdRequest) {
	payload := make([]byte, request.length)
	n, readErr := s.backend.ReadAt(payload, int64(request.offset))
	if readErr != nil || n != len(payload) {
		if readErr == nil {
			readErr = io.ErrUnexpectedEOF
		}
		s.observeBackendError(fmt.Errorf(
			"read offset %d length %d: %w", request.offset, request.length, readErr,
		))
		payload = nil
	}
	if s.ctx.Err() != nil {
		return
	}
	_ = s.reply(request.handle, nbdErrno(readErr), payload)
}

func (s *nbdTransmission) observeBackendError(err error) {
	if err != nil && s.onBackendError != nil {
		s.backendErrorMu.Lock()
		defer s.backendErrorMu.Unlock()
		s.onBackendError(err)
	}
}

func readNBDRequest(reader io.Reader) (nbdRequest, error) {
	header := make([]byte, nbdRequestHeaderBytes)
	if _, err := io.ReadFull(reader, header); err != nil {
		return nbdRequest{}, err
	}
	if binary.BigEndian.Uint32(header[:4]) != nbdRequestMagic {
		return nbdRequest{}, errNBDRequestMagic
	}
	request := nbdRequest{
		typeAndFlags: binary.BigEndian.Uint32(header[4:8]),
		offset:       binary.BigEndian.Uint64(header[16:24]),
		length:       binary.BigEndian.Uint32(header[24:28]),
	}
	copy(request.handle[:], header[8:16])
	return request, nil
}

func (s *nbdTransmission) reply(handle [8]byte, errno syscall.Errno, payload []byte) (err error) {
	header := make([]byte, nbdReplyHeaderBytes)
	binary.BigEndian.PutUint32(header[:4], nbdReplyMagic)
	binary.BigEndian.PutUint32(header[4:8], uint32(errno))
	copy(header[8:16], handle[:])
	s.replyMu.Lock()
	defer s.replyMu.Unlock()
	defer func() {
		// Cancel while still owning the frame lock so no other worker can
		// append a reply after a partially written frame.
		if err != nil {
			s.fail(err)
		}
	}()
	if err := s.ctx.Err(); err != nil {
		return err
	}
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
