package rootfsblock

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestNBDTransmissionReportsOriginalBackendError(t *testing.T) {
	backendErr := errors.New("immutable object checksum mismatch")
	backend := &failingReadBlockBackend{err: backendErr}
	client, server := net.Pipe()
	done := make(chan error, 1)
	observed := make(chan error, 1)
	go func() {
		done <- (NBDTransmissionServer{
			Backend:        backend,
			OnBackendError: func(err error) { observed <- err },
		}).Serve(t.Context(), server)
	}()

	handle := [8]byte{1}
	writeNBDRequest(t, client, nbdCommandRead, handle, 4096, make([]byte, LogicalBlockSize))
	require.Equal(t, uint32(syscall.EIO), readNBDReply(t, client, handle, 0))
	require.ErrorIs(t, <-observed, backendErr)
	require.ErrorContains(t, backendErr, "checksum mismatch")

	writeNBDRequest(t, client, nbdCommandDisconnect, [8]byte{2}, 0, nil)
	require.NoError(t, <-done)
}

func TestNBDTransmissionReadWriteFlushTrimAndFUA(t *testing.T) {
	backend := &memoryBlockBackend{payload: bytes.Repeat([]byte{0x11}, 4*LogicalBlockSize)}
	client, server := net.Pipe()
	done := make(chan error, 1)
	go func() {
		done <- (NBDTransmissionServer{Backend: backend}).Serve(t.Context(), server)
	}()

	writePayload := bytes.Repeat([]byte{0x22}, LogicalBlockSize)
	handle := [8]byte{1, 2, 3}
	writeNBDRequest(t, client, nbdCommandWrite|nbdCommandFlagFUA, handle, LogicalBlockSize, writePayload)
	require.Equal(t, uint32(0), readNBDReply(t, client, handle, 0))
	require.Equal(t, 1, backend.flushes)

	readHandle := [8]byte{4, 5, 6}
	writeNBDRequest(t, client, nbdCommandRead, readHandle, LogicalBlockSize, make([]byte, LogicalBlockSize))
	require.Equal(t, uint32(0), readNBDReply(t, client, readHandle, LogicalBlockSize))
	actual := make([]byte, LogicalBlockSize)
	_, err := io.ReadFull(client, actual)
	require.NoError(t, err)
	require.Equal(t, writePayload, actual)

	zeroHandle := [8]byte{7}
	writeNBDRequest(t, client, nbdCommandWriteZero|nbdCommandFlagNoHole, zeroHandle, LogicalBlockSize, make([]byte, LogicalBlockSize))
	require.Equal(t, uint32(0), readNBDReply(t, client, zeroHandle, 0))
	require.Equal(t, make([]byte, LogicalBlockSize), backend.payload[LogicalBlockSize:2*LogicalBlockSize])

	trimHandle := [8]byte{8}
	writeNBDRequest(t, client, nbdCommandTrim|nbdCommandFlagFUA, trimHandle, 2*LogicalBlockSize, make([]byte, LogicalBlockSize))
	require.Equal(t, uint32(0), readNBDReply(t, client, trimHandle, 0))
	require.Equal(t, 1, backend.trims)
	require.Equal(t, bytes.Repeat([]byte{0x11}, LogicalBlockSize), backend.payload[2*LogicalBlockSize:3*LogicalBlockSize])
	require.Equal(t, 2, backend.flushes)

	flushHandle := [8]byte{9}
	writeNBDRequest(t, client, nbdCommandFlush, flushHandle, 0, nil)
	require.Equal(t, uint32(0), readNBDReply(t, client, flushHandle, 0))
	require.Equal(t, 3, backend.flushes)

	writeNBDRequest(t, client, nbdCommandDisconnect, [8]byte{10}, 0, nil)
	require.NoError(t, <-done)
}

func TestNBDTransmissionAcceptsTrimLargerThanPayloadLimit(t *testing.T) {
	backend := &memoryBlockBackend{payload: bytes.Repeat([]byte{0x11}, 4*LogicalBlockSize)}
	client, server := net.Pipe()
	done := make(chan error, 1)
	go func() {
		done <- (NBDTransmissionServer{
			Backend: backend, MaxRequestBytes: LogicalBlockSize,
		}).Serve(t.Context(), server)
	}()

	trimHandle := [8]byte{31}
	require.NoError(t, writeFull(client, makeNBDRequest(
		nbdCommandTrim, trimHandle, 0, 4*LogicalBlockSize,
	)))
	require.Equal(t, uint32(0), readNBDReply(t, client, trimHandle, 0))
	require.Equal(t, 1, backend.trims)

	readHandle := [8]byte{32}
	writeNBDRequest(t, client, nbdCommandRead, readHandle, 0, make([]byte, LogicalBlockSize))
	require.Equal(t, uint32(0), readNBDReply(t, client, readHandle, LogicalBlockSize))
	payload := make([]byte, LogicalBlockSize)
	_, err := io.ReadFull(client, payload)
	require.NoError(t, err)
	require.Equal(t, bytes.Repeat([]byte{0x11}, LogicalBlockSize), payload)

	writeNBDRequest(t, client, nbdCommandDisconnect, [8]byte{33}, 0, nil)
	require.NoError(t, <-done)
}

func TestNBDTransmissionPreservesBackendPartialWriteAndFUAErrorSemantics(t *testing.T) {
	t.Run("partial write", func(t *testing.T) {
		backend := &partialWriteBlockBackend{
			memoryBlockBackend: memoryBlockBackend{payload: bytes.Repeat([]byte{0x11}, 3*LogicalBlockSize)},
			err:                syscall.ENOSPC,
		}
		client, server := net.Pipe()
		done := make(chan error, 1)
		go func() { done <- (NBDTransmissionServer{Backend: backend}).Serve(t.Context(), server) }()

		payload := bytes.Repeat([]byte{0x22}, 2*LogicalBlockSize)
		handle := [8]byte{11}
		writeNBDRequest(t, client, nbdCommandWrite, handle, 0, payload)
		require.Equal(t, uint32(syscall.ENOSPC), readNBDReply(t, client, handle, 0))
		require.Equal(t, payload[:LogicalBlockSize], backend.payload[:LogicalBlockSize])
		require.Equal(t, bytes.Repeat([]byte{0x11}, LogicalBlockSize), backend.payload[LogicalBlockSize:2*LogicalBlockSize])

		writeNBDRequest(t, client, nbdCommandDisconnect, [8]byte{12}, 0, nil)
		require.NoError(t, <-done)
	})

	t.Run("FUA flush failure", func(t *testing.T) {
		backend := &memoryBlockBackend{
			payload:  bytes.Repeat([]byte{0x11}, 2*LogicalBlockSize),
			flushErr: syscall.EIO,
		}
		client, server := net.Pipe()
		done := make(chan error, 1)
		go func() { done <- (NBDTransmissionServer{Backend: backend}).Serve(t.Context(), server) }()

		payload := bytes.Repeat([]byte{0x33}, LogicalBlockSize)
		handle := [8]byte{13}
		writeNBDRequest(t, client, nbdCommandWrite|nbdCommandFlagFUA, handle, 0, payload)
		require.Equal(t, uint32(syscall.EIO), readNBDReply(t, client, handle, 0))
		require.Equal(t, payload, backend.payload[:LogicalBlockSize])

		writeNBDRequest(t, client, nbdCommandDisconnect, [8]byte{14}, 0, nil)
		require.NoError(t, <-done)
	})
}

func TestNBDTransmissionEncodesDirtyTailCapacityAsENOSPCReply(t *testing.T) {
	base := make([]byte, 2*LogicalBlockSize)
	branch, err := OpenBranchWithOptions(
		filepath.Join(t.TempDir(), "branch.log"), testBranchIdentity(int64(len(base))),
		bytes.NewReader(base), BranchOptions{MaxDirtyTailBytes: LogicalBlockSize},
	)
	require.NoError(t, err)
	defer branch.Close()
	_, err = branch.WriteAt(bytes.Repeat([]byte{1}, LogicalBlockSize), 0)
	require.NoError(t, err)

	client, server := net.Pipe()
	done := make(chan error, 1)
	observed := make(chan error, 1)
	go func() {
		done <- (NBDTransmissionServer{
			Backend: branch, OnBackendError: func(err error) { observed <- err },
		}).Serve(t.Context(), server)
	}()

	handle := [8]byte{21}
	writeNBDRequest(t, client, nbdCommandWrite, handle, LogicalBlockSize, bytes.Repeat([]byte{2}, LogicalBlockSize))
	require.Equal(t, uint32(syscall.ENOSPC), readNBDReply(t, client, handle, 0))
	require.ErrorIs(t, <-observed, syscall.ENOSPC)
	actual := make([]byte, LogicalBlockSize)
	_, err = branch.ReadAt(actual, LogicalBlockSize)
	require.NoError(t, err)
	require.Equal(t, make([]byte, LogicalBlockSize), actual)

	writeNBDRequest(t, client, nbdCommandDisconnect, [8]byte{22}, 0, nil)
	require.NoError(t, <-done)
}

func TestNBDTransmissionRejectsBadMagicAndOversizedWrite(t *testing.T) {
	backend := &memoryBlockBackend{payload: make([]byte, 4*LogicalBlockSize)}
	for name, request := range map[string][]byte{
		"magic": func() []byte {
			value := makeNBDRequest(nbdCommandRead, [8]byte{}, 0, LogicalBlockSize)
			value[0] = 0
			return value
		}(),
		"oversized": makeNBDRequest(nbdCommandWrite, [8]byte{}, 0, 2*LogicalBlockSize),
	} {
		t.Run(name, func(t *testing.T) {
			client, server := net.Pipe()
			done := make(chan error, 1)
			go func() {
				done <- (NBDTransmissionServer{Backend: backend, MaxRequestBytes: LogicalBlockSize}).Serve(t.Context(), server)
			}()
			require.NoError(t, writeFull(client, request))
			require.NoError(t, client.Close())
			require.Error(t, <-done)
		})
	}
}

func TestNBDTransmissionContextCancellationClosesConnection(t *testing.T) {
	backend := &memoryBlockBackend{payload: make([]byte, LogicalBlockSize)}
	client, server := net.Pipe()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- (NBDTransmissionServer{Backend: backend}).Serve(ctx, server) }()
	cancel()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("NBD server did not stop after cancellation")
	}
	require.NoError(t, client.Close())
}

type memoryBlockBackend struct {
	mu       sync.Mutex
	payload  []byte
	flushes  int
	trims    int
	flushErr error
}

type partialWriteBlockBackend struct {
	memoryBlockBackend
	err error
}

func (b *partialWriteBlockBackend) WriteAt(payload []byte, offset int64) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	written := min(len(payload), LogicalBlockSize)
	copy(b.payload[offset:], payload[:written])
	return written, b.err
}

type failingReadBlockBackend struct{ err error }

func (b *failingReadBlockBackend) Size() int64                       { return 4 * LogicalBlockSize }
func (b *failingReadBlockBackend) ReadAt([]byte, int64) (int, error) { return 0, b.err }
func (*failingReadBlockBackend) WriteAt(payload []byte, _ int64) (int, error) {
	return len(payload), nil
}
func (*failingReadBlockBackend) Flush() error                   { return nil }
func (*failingReadBlockBackend) Trim(int64, int64) error        { return nil }
func (*failingReadBlockBackend) WriteZeroes(int64, int64) error { return nil }

func (b *memoryBlockBackend) Size() int64 { return int64(len(b.payload)) }

func (b *memoryBlockBackend) ReadAt(target []byte, offset int64) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return bytes.NewReader(b.payload).ReadAt(target, offset)
}

func (b *memoryBlockBackend) WriteAt(payload []byte, offset int64) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return copy(b.payload[offset:], payload), nil
}

func (b *memoryBlockBackend) Flush() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.flushes++
	return b.flushErr
}

func (b *memoryBlockBackend) Trim(_, _ int64) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.trims++
	return nil
}

func (b *memoryBlockBackend) WriteZeroes(offset, length int64) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	clear(b.payload[offset : offset+length])
	return nil
}

func writeNBDRequest(t *testing.T, writer io.Writer, command uint32, handle [8]byte, offset int, payload []byte) {
	t.Helper()
	length := len(payload)
	if command&nbdCommandMask != nbdCommandWrite {
		length = len(payload)
	}
	header := makeNBDRequest(command, handle, uint64(offset), uint32(length))
	require.NoError(t, writeFull(writer, header))
	if command&nbdCommandMask == nbdCommandWrite {
		require.NoError(t, writeFull(writer, payload))
	}
}

func makeNBDRequest(command uint32, handle [8]byte, offset uint64, length uint32) []byte {
	header := make([]byte, nbdRequestHeaderBytes)
	binary.BigEndian.PutUint32(header[:4], nbdRequestMagic)
	binary.BigEndian.PutUint32(header[4:8], command)
	copy(header[8:16], handle[:])
	binary.BigEndian.PutUint64(header[16:24], offset)
	binary.BigEndian.PutUint32(header[24:28], length)
	return header
}

func readNBDReply(t *testing.T, reader io.Reader, expectedHandle [8]byte, payloadLength int) uint32 {
	t.Helper()
	header := make([]byte, nbdReplyHeaderBytes)
	_, err := io.ReadFull(reader, header)
	require.NoError(t, err)
	require.Equal(t, uint32(nbdReplyMagic), binary.BigEndian.Uint32(header[:4]))
	require.Equal(t, expectedHandle[:], header[8:16])
	_ = payloadLength
	return binary.BigEndian.Uint32(header[4:8])
}
