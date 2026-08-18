package rootfsblock

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
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
	require.Equal(t, 2, backend.flushes)

	flushHandle := [8]byte{9}
	writeNBDRequest(t, client, nbdCommandFlush, flushHandle, 0, nil)
	require.Equal(t, uint32(0), readNBDReply(t, client, flushHandle, 0))
	require.Equal(t, 3, backend.flushes)

	writeNBDRequest(t, client, nbdCommandDisconnect, [8]byte{10}, 0, nil)
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
	mu      sync.Mutex
	payload []byte
	flushes int
}

type failingReadBlockBackend struct{ err error }

func (b *failingReadBlockBackend) Size() int64                       { return 4 * LogicalBlockSize }
func (b *failingReadBlockBackend) ReadAt([]byte, int64) (int, error) { return 0, b.err }
func (*failingReadBlockBackend) WriteAt(payload []byte, _ int64) (int, error) {
	return len(payload), nil
}
func (*failingReadBlockBackend) Flush() error                   { return nil }
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
