package rootfsblock

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"path/filepath"
	"syscall"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/require"
)

// Each backend operation waits on its own gate, without holding a backend
// lock. Tests control completion order independently of goroutine scheduling.
type nbdControlledCall struct {
	command uint32
	offset  int64
	length  int
	release chan struct{}
	n       int
	err     error
}

type nbdControlledBackend struct {
	memoryBlockBackend
	calls chan *nbdControlledCall
	stop  chan struct{}
}

func (b *nbdControlledBackend) enter(command uint32, offset int64, length int) *nbdControlledCall {
	call := &nbdControlledCall{
		command: command, offset: offset, length: length, n: length, release: make(chan struct{}),
	}
	b.calls <- call
	select {
	case <-call.release:
	case <-b.stop:
	}
	return call
}

func (b *nbdControlledBackend) ReadAt(target []byte, offset int64) (int, error) {
	call := b.enter(nbdCommandRead, offset, len(target))
	n, err := b.memoryBlockBackend.ReadAt(target[:call.n], offset)
	return n, errors.Join(err, call.err)
}

func (b *nbdControlledBackend) WriteAt(payload []byte, offset int64) (int, error) {
	call := b.enter(nbdCommandWrite, offset, len(payload))
	n, err := b.memoryBlockBackend.WriteAt(payload[:call.n], offset)
	return n, errors.Join(err, call.err)
}

func (b *nbdControlledBackend) Flush() error {
	call := b.enter(nbdCommandFlush, 0, 0)
	return errors.Join(b.memoryBlockBackend.Flush(), call.err)
}

func (b *nbdControlledBackend) Trim(offset, length int64) error {
	call := b.enter(nbdCommandTrim, offset, int(length))
	return errors.Join(b.memoryBlockBackend.Trim(offset, length), call.err)
}

func (b *nbdControlledBackend) WriteZeroes(offset, length int64) error {
	call := b.enter(nbdCommandWriteZero, offset, int(length))
	return errors.Join(b.memoryBlockBackend.WriteZeroes(offset, length), call.err)
}

type nbdConcurrencyHarness struct {
	t       *testing.T
	backend *nbdControlledBackend
	client  net.Conn
	cancel  context.CancelFunc
	done    chan struct{}
	err     error
}

const nbdTestDeviceBlocks = 128

type nbdGatedRangeSource struct {
	RangeSource
	pack    string
	started chan int64
	release <-chan struct{}
}

func (s nbdGatedRangeSource) Get(key string, offset, length int64) (io.ReadCloser, error) {
	if key == s.pack {
		s.started <- offset
		<-s.release
	}
	return s.RangeSource.Get(key, offset, length)
}

func TestNBDConcurrentCoalescedWaitersDoNotHideIndependentRange(t *testing.T) {
	// Sixteen small demands share the first immutable range. The following
	// already-demanded range must reach storage before that first GET ends.
	// This is demand admission, not speculative reading beyond the wire.
	synctest.Test(t, func(t *testing.T) {
		store, descriptor, expected, pack := coalescingFixture(t)
		started := make(chan int64, 4)
		release := make(chan struct{})
		reader, err := NewReader(nbdGatedRangeSource{store, pack, started, release}, descriptor, DefaultReadCacheBytes)
		require.NoError(t, err)
		branch, err := OpenBranch(filepath.Join(t.TempDir(), "branch"), testBranchIdentity(int64(len(expected))), reader)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, branch.Close()) })
		h := newNBDConcurrencyHarness(t, func(s *NBDTransmissionServer, c net.Conn) net.Conn {
			s.Backend = branch
			s.MaxRequestBytes = DefaultNBDMaxRequest
			return c
		})
		// Release blocked source calls even when a later assertion fails.
		t.Cleanup(func() {
			select {
			case <-release:
			default:
				close(release)
			}
		})
		for index := 0; index < 16; index++ {
			require.NoError(t, <-h.send(nbdCommandRead, byte(index+1), uint64(index*LogicalBlockSize), LogicalBlockSize))
		}
		require.Zero(t, <-started)
		require.NoError(t, <-h.send(nbdCommandRead, 17, CompressedDataRangeBytes, LogicalBlockSize))
		synctest.Wait()
		require.Len(t, started, 1, "independent demanded range must not wait behind coalesced readers")
		require.Positive(t, <-started, "the second compressed range has its own physical offset")
		close(release)
		seen := make(map[byte]bool)
		for index := 0; index < 17; index++ {
			header := make([]byte, nbdReplyHeaderBytes)
			_, err := io.ReadFull(h.client, header)
			require.NoError(t, err)
			require.Equal(t, uint32(nbdReplyMagic), binary.BigEndian.Uint32(header[:4]))
			require.Zero(t, binary.BigEndian.Uint32(header[4:8]))
			id := header[8]
			require.True(t, id >= 1 && id <= 17)
			require.False(t, seen[id])
			seen[id] = true
			payload := make([]byte, LogicalBlockSize)
			_, err = io.ReadFull(h.client, payload)
			require.NoError(t, err)
			value := byte(1)
			if id == 17 {
				value = 2
			}
			require.Equal(t, bytes.Repeat([]byte{value}, LogicalBlockSize), payload)
		}
		require.Equal(t, 2, store.count(pack), "coalesced demands still fetch each immutable range once")
		h.disconnect()
	})
}

func newNBDConcurrencyHarness(t *testing.T, configure func(*NBDTransmissionServer, net.Conn) net.Conn) *nbdConcurrencyHarness {
	t.Helper()
	client, connection := net.Pipe()
	ctx, cancel := context.WithCancel(t.Context())
	h := &nbdConcurrencyHarness{
		t: t, client: client, cancel: cancel, done: make(chan struct{}),
		backend: &nbdControlledBackend{
			memoryBlockBackend: memoryBlockBackend{payload: bytes.Repeat([]byte{0x11}, nbdTestDeviceBlocks*LogicalBlockSize)},
			calls:              make(chan *nbdControlledCall, 64), stop: make(chan struct{}),
		},
	}
	server := NBDTransmissionServer{Backend: h.backend, MaxRequestBytes: LogicalBlockSize}
	if configure != nil {
		connection = configure(&server, connection)
	}
	go func() {
		h.err = server.Serve(ctx, connection)
		close(h.done)
	}()
	t.Cleanup(func() {
		cancel()
		_ = client.Close()
		close(h.backend.stop)
		<-h.done
	})
	return h
}

func (h *nbdConcurrencyHarness) send(command uint32, id byte, offset uint64, length uint32) <-chan error {
	h.t.Helper()
	payload := makeNBDRequest(command, [8]byte{id}, offset, length)
	if command&nbdCommandMask == nbdCommandWrite {
		payload = append(payload, bytes.Repeat([]byte{0x22}, int(length))...)
	}
	return h.sendRaw(payload)
}

func (h *nbdConcurrencyHarness) sendRaw(payload []byte) <-chan error {
	done := make(chan error, 1)
	go func() { done <- writeFull(h.client, payload) }()
	return done
}

func (h *nbdConcurrencyHarness) nextCall(command uint32, offset int64) *nbdControlledCall {
	h.t.Helper()
	call := <-h.backend.calls
	require.Equal(h.t, command, call.command)
	require.Equal(h.t, offset, call.offset)
	return call
}

func (h *nbdConcurrencyHarness) reply(id byte, errno syscall.Errno, payload []byte) {
	h.t.Helper()
	require.Equal(h.t, uint32(errno), readNBDReply(h.t, h.client, [8]byte{id}, len(payload)))
	if len(payload) != 0 {
		actual := make([]byte, len(payload))
		_, err := io.ReadFull(h.client, actual)
		require.NoError(h.t, err)
		require.Equal(h.t, payload, actual)
	}
}

func (h *nbdConcurrencyHarness) noBackendCall() {
	h.t.Helper()
	synctest.Wait()
	require.Empty(h.t, h.backend.calls, "a request passed the ordering/admission barrier")
}

func (h *nbdConcurrencyHarness) stillServing() {
	h.t.Helper()
	synctest.Wait()
	select {
	case <-h.done:
		h.t.Fatalf("Serve returned with an active backend operation: %v", h.err)
	default:
	}
}

func (h *nbdConcurrencyHarness) disconnect() {
	h.t.Helper()
	require.NoError(h.t, <-h.send(nbdCommandDisconnect, 255, 0, 0))
	<-h.done
	require.NoError(h.t, h.err)
}

func TestNBDConcurrentIndependentReadsReplyOutOfOrder(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newNBDConcurrencyHarness(t, nil)
		require.NoError(t, <-h.send(nbdCommandRead, 1, 0, LogicalBlockSize))
		first := h.nextCall(nbdCommandRead, 0)
		require.NoError(t, <-h.send(nbdCommandRead, 2, LogicalBlockSize, LogicalBlockSize))
		second := h.nextCall(nbdCommandRead, LogicalBlockSize)

		close(second.release)
		h.reply(2, 0, bytes.Repeat([]byte{0x11}, LogicalBlockSize))
		close(first.release)
		h.reply(1, 0, bytes.Repeat([]byte{0x11}, LogicalBlockSize))
		h.disconnect()
	})
}

func TestNBDConcurrentBranchReadsReachIndependentBaseRanges(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newNBDConcurrencyHarness(t, func(s *NBDTransmissionServer, c net.Conn) net.Conn {
			branch, err := OpenBranch(
				filepath.Join(t.TempDir(), "branch.log"), testBranchIdentity(s.Backend.Size()), s.Backend,
			)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, branch.Close()) })
			s.Backend = branch
			return c
		})
		// Exercise the actual writable Branch's read locks, not just a test
		// backend, with two independent immutable base misses held in flight.
		require.NoError(t, <-h.send(nbdCommandRead, 1, 0, LogicalBlockSize))
		first := h.nextCall(nbdCommandRead, 0)
		require.NoError(t, <-h.send(nbdCommandRead, 2, LogicalBlockSize, LogicalBlockSize))
		second := h.nextCall(nbdCommandRead, LogicalBlockSize)
		close(second.release)
		h.reply(2, 0, bytes.Repeat([]byte{0x11}, LogicalBlockSize))
		close(first.release)
		h.reply(1, 0, bytes.Repeat([]byte{0x11}, LogicalBlockSize))
		h.disconnect()
	})
}

func TestNBDConcurrentReadsBracketMutationAndFlushBarriers(t *testing.T) {
	for _, command := range []uint32{
		nbdCommandWrite, nbdCommandWrite | nbdCommandFlagFUA,
		nbdCommandFlush, nbdCommandTrim, nbdCommandTrim | nbdCommandFlagFUA,
		nbdCommandWriteZero, nbdCommandWriteZero | nbdCommandFlagNoHole | nbdCommandFlagFUA,
	} {
		t.Run(nbdTestCommandName(command), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				h := newNBDConcurrencyHarness(t, nil)
				require.NoError(t, <-h.send(nbdCommandRead, 1, 0, LogicalBlockSize))
				first := h.nextCall(nbdCommandRead, 0)
				require.NoError(t, <-h.send(nbdCommandRead, 2, LogicalBlockSize, LogicalBlockSize))
				second := h.nextCall(nbdCommandRead, LogicalBlockSize)
				length := uint32(LogicalBlockSize)
				if command == nbdCommandFlush {
					length = 0
				}
				barrierSent := h.send(command, 3, 0, length)
				h.noBackendCall()
				close(second.release)
				h.reply(2, 0, bytes.Repeat([]byte{0x11}, LogicalBlockSize))
				h.noBackendCall()
				close(first.release)
				// The barrier also waits for the READ payload, not just ReadAt.
				require.Zero(t, readNBDReply(t, h.client, [8]byte{1}, LogicalBlockSize))
				h.noBackendCall()
				payload := make([]byte, LogicalBlockSize)
				_, err := io.ReadFull(h.client, payload)
				require.NoError(t, err)
				require.Equal(t, bytes.Repeat([]byte{0x11}, LogicalBlockSize), payload)
				require.NoError(t, <-barrierSent)
				barrier := h.nextCall(command&nbdCommandMask, 0)
				laterSent := h.send(nbdCommandRead, 4, 0, LogicalBlockSize)
				h.noBackendCall()
				close(barrier.release)
				if command&nbdCommandFlagFUA != 0 {
					flush := h.nextCall(nbdCommandFlush, 0)
					h.noBackendCall()
					close(flush.release)
				}
				h.noBackendCall() // Later READ also waits for the barrier's reply.
				h.reply(3, 0, nil)
				require.NoError(t, <-laterSent)
				later := h.nextCall(nbdCommandRead, 0)
				close(later.release)
				expected := byte(0x11)
				switch command & nbdCommandMask {
				case nbdCommandWrite:
					expected = 0x22
				case nbdCommandWriteZero:
					expected = 0
				}
				h.reply(4, 0, bytes.Repeat([]byte{expected}, LogicalBlockSize))
				h.disconnect()
			})
		})
	}
}

func nbdTestCommandName(command uint32) string {
	name := map[uint32]string{
		nbdCommandWrite: "write", nbdCommandFlush: "flush", nbdCommandTrim: "trim", nbdCommandWriteZero: "write zero",
	}[command&nbdCommandMask]
	if command&nbdCommandFlagFUA != 0 {
		name += " FUA"
	}
	return name
}

func TestNBDConcurrentReadLimitIncludesPendingReplies(t *testing.T) {
	for _, bound := range []struct {
		name    string
		maximum uint32
		calls   int
	}{
		{"count", nbdMaxConcurrentReads * LogicalBlockSize, nbdMaxConcurrentReads},
		{"bytes", LogicalBlockSize, nbdReadBufferSlots},
	} {
		t.Run(bound.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				h := newNBDConcurrencyHarness(t, func(s *NBDTransmissionServer, c net.Conn) net.Conn {
					s.MaxRequestBytes = bound.maximum
					return c
				})
				calls := make([]*nbdControlledCall, bound.calls)
				for i := range calls {
					require.NoError(t, <-h.send(nbdCommandRead, byte(i+1), uint64(i*LogicalBlockSize), LogicalBlockSize))
					calls[i] = h.nextCall(nbdCommandRead, int64(i*LogicalBlockSize))
					require.Equal(t, LogicalBlockSize, calls[i].length)
				}
				require.NoError(t, <-h.send(nbdCommandRead, 100, 0, LogicalBlockSize))
				h.noBackendCall()
				close(calls[0].release)
				h.noBackendCall() // The full reply is blocked on the client.
				require.Zero(t, readNBDReply(t, h.client, [8]byte{1}, LogicalBlockSize))
				payload := make([]byte, LogicalBlockSize)
				_, err := io.ReadFull(h.client, payload[:LogicalBlockSize-1])
				require.NoError(t, err)
				h.noBackendCall() // Even one pending payload byte retains the slot.
				_, err = io.ReadFull(h.client, payload[LogicalBlockSize-1:])
				require.NoError(t, err)
				next := h.nextCall(nbdCommandRead, 0)
				close(next.release)
				h.reply(100, 0, bytes.Repeat([]byte{0x11}, LogicalBlockSize))
				for i := 1; i < len(calls); i++ {
					close(calls[i].release)
					h.reply(byte(i+1), 0, bytes.Repeat([]byte{0x11}, LogicalBlockSize))
				}
				h.disconnect()
			})
		})
	}
}

func TestNBDConcurrentInvalidRequestsAreBarriers(t *testing.T) {
	for name, request := range map[string][]byte{
		"read flags":       makeNBDRequest(nbdCommandRead|nbdCommandFlagFUA, [8]byte{2}, 0, 1),
		"read empty":       makeNBDRequest(nbdCommandRead, [8]byte{2}, 0, 0),
		"read oversized":   makeNBDRequest(nbdCommandRead, [8]byte{2}, 0, LogicalBlockSize+1),
		"read past end":    makeNBDRequest(nbdCommandRead, [8]byte{2}, nbdTestDeviceBlocks*LogicalBlockSize, 1),
		"read overflow":    makeNBDRequest(nbdCommandRead, [8]byte{2}, ^uint64(0), 2),
		"flush flags":      makeNBDRequest(nbdCommandFlush|nbdCommandFlagFUA, [8]byte{2}, 0, 0),
		"flush offset":     makeNBDRequest(nbdCommandFlush, [8]byte{2}, 1, 0),
		"flush length":     makeNBDRequest(nbdCommandFlush, [8]byte{2}, 0, 1),
		"trim flags":       makeNBDRequest(nbdCommandTrim|nbdCommandFlagNoHole, [8]byte{2}, 0, 1),
		"trim empty":       makeNBDRequest(nbdCommandTrim, [8]byte{2}, 0, 0),
		"trim range":       makeNBDRequest(nbdCommandTrim, [8]byte{2}, nbdTestDeviceBlocks*LogicalBlockSize, 1),
		"zero flags":       makeNBDRequest(nbdCommandWriteZero|1<<18, [8]byte{2}, 0, 1),
		"zero empty":       makeNBDRequest(nbdCommandWriteZero, [8]byte{2}, 0, 0),
		"zero range":       makeNBDRequest(nbdCommandWriteZero, [8]byte{2}, nbdTestDeviceBlocks*LogicalBlockSize, 1),
		"disconnect flags": makeNBDRequest(nbdCommandDisconnect|nbdCommandFlagFUA, [8]byte{2}, 0, 0),
		"disconnect range": makeNBDRequest(nbdCommandDisconnect, [8]byte{2}, 1, 1),
		"unknown":          makeNBDRequest(99, [8]byte{2}, 0, 0),
	} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				h := newNBDConcurrencyHarness(t, nil)
				require.NoError(t, <-h.send(nbdCommandRead, 1, 0, LogicalBlockSize))
				first := h.nextCall(nbdCommandRead, 0)
				require.NoError(t, <-h.sendRaw(request))
				laterSent := h.send(nbdCommandRead, 3, LogicalBlockSize, LogicalBlockSize)
				h.noBackendCall()
				close(first.release)
				h.reply(1, 0, bytes.Repeat([]byte{0x11}, LogicalBlockSize))
				h.noBackendCall()
				h.reply(2, syscall.EINVAL, nil)
				require.NoError(t, <-laterSent)
				later := h.nextCall(nbdCommandRead, LogicalBlockSize)
				close(later.release)
				h.reply(3, 0, bytes.Repeat([]byte{0x11}, LogicalBlockSize))
				h.disconnect()
			})
		})
	}
}

func TestNBDConcurrentMalformedRequestsDrainBeforeTermination(t *testing.T) {
	badMagic := makeNBDRequest(nbdCommandRead, [8]byte{2}, 0, 1)
	badMagic[0] = 0
	for name, request := range map[string][]byte{
		"bad magic":       badMagic,
		"write oversized": makeNBDRequest(nbdCommandWrite, [8]byte{2}, 0, LogicalBlockSize+1),
		"write range":     makeNBDRequest(nbdCommandWrite, [8]byte{2}, nbdTestDeviceBlocks*LogicalBlockSize, 1),
		"write overflow":  makeNBDRequest(nbdCommandWrite, [8]byte{2}, ^uint64(0), 1),
		"write flags":     makeNBDRequest(nbdCommandWrite|nbdCommandFlagNoHole, [8]byte{2}, 0, 1),
		"write empty":     makeNBDRequest(nbdCommandWrite, [8]byte{2}, 0, 0),
	} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				h := newNBDConcurrencyHarness(t, nil)
				require.NoError(t, <-h.send(nbdCommandRead, 1, 0, LogicalBlockSize))
				first := h.nextCall(nbdCommandRead, 0)
				require.NoError(t, <-h.sendRaw(request))
				// A body that resembles a valid header must never become a request.
				trailing := h.send(nbdCommandRead, 3, 0, LogicalBlockSize)
				h.noBackendCall()
				h.stillServing()
				close(first.release)
				h.reply(1, 0, bytes.Repeat([]byte{0x11}, LogicalBlockSize))
				<-h.done
				require.Error(t, h.err)
				require.Error(t, <-trailing)
				require.Empty(t, h.backend.calls)
			})
		})
	}
}

func TestNBDConcurrentDisconnectWaitsForReadsAndReplies(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newNBDConcurrencyHarness(t, nil)
		require.NoError(t, <-h.send(nbdCommandRead, 1, 0, LogicalBlockSize))
		first := h.nextCall(nbdCommandRead, 0)
		require.NoError(t, <-h.send(nbdCommandDisconnect, 2, 0, 0))
		trailing := h.send(nbdCommandWrite, 3, 0, LogicalBlockSize)
		h.stillServing()
		close(first.release)
		require.Zero(t, readNBDReply(t, h.client, [8]byte{1}, LogicalBlockSize))
		h.stillServing()
		_, err := io.ReadFull(h.client, make([]byte, LogicalBlockSize))
		require.NoError(t, err)
		<-h.done
		require.NoError(t, h.err)
		require.Error(t, <-trailing)
		require.Empty(t, h.backend.calls)
		_, err = h.client.Read(make([]byte, 1))
		require.ErrorIs(t, err, io.EOF) // DISC never receives a reply.
	})
}

func TestNBDConcurrentCancellationAndPeerCloseJoinBackendCalls(t *testing.T) {
	for _, bound := range []struct {
		name    string
		maximum uint32
		calls   int
	}{
		{"count", nbdMaxConcurrentReads * LogicalBlockSize, nbdMaxConcurrentReads},
		{"bytes", LogicalBlockSize, nbdReadBufferSlots},
	} {
		t.Run(bound.name, func(t *testing.T) {
			for _, cancel := range []bool{false, true} {
				for _, barrier := range []bool{false, true} {
					t.Run(map[bool]string{false: "peer close", true: "cancel"}[cancel]+map[bool]string{false: " at read limit", true: " at barrier"}[barrier], func(t *testing.T) {
						synctest.Test(t, func(t *testing.T) {
							observed := make(chan error, nbdMaxConcurrentReads)
							h := newNBDConcurrencyHarness(t, func(s *NBDTransmissionServer, c net.Conn) net.Conn {
								s.MaxRequestBytes = bound.maximum
								s.OnBackendError = func(err error) { observed <- err }
								return c
							})
							calls := make([]*nbdControlledCall, bound.calls)
							for i := range calls {
								require.NoError(t, <-h.send(nbdCommandRead, byte(i+1), uint64(i*LogicalBlockSize), LogicalBlockSize))
								calls[i] = h.nextCall(nbdCommandRead, int64(i*LogicalBlockSize))
							}
							command := uint32(nbdCommandRead)
							if barrier {
								command = nbdCommandWrite
							}
							queued := h.send(command, 100, 0, LogicalBlockSize)
							h.noBackendCall()
							if cancel {
								h.cancel()
							} else {
								require.NoError(t, h.client.Close())
							}
							h.stillServing()
							// A storage error discovered during teardown must still reach
							// the terminal error observer before Serve returns.
							calls[0].err = syscall.EIO
							for _, call := range calls {
								close(call.release)
							}
							<-h.done
							if cancel {
								require.ErrorIs(t, h.err, context.Canceled)
							}
							require.ErrorIs(t, <-observed, syscall.EIO)
							<-queued
							require.Empty(t, h.backend.calls)
						})
					})
				}
			}
		})
	}
}

func TestNBDConcurrentReadErrorsDoNotExposePartialPayloads(t *testing.T) {
	for name, result := range map[string]struct {
		n     int
		err   error
		errno syscall.Errno
	}{
		"short without error": {1, nil, syscall.EIO},
		"EOF":                 {1, io.EOF, syscall.EIO},
		"full with error":     {LogicalBlockSize, syscall.EACCES, syscall.EACCES},
	} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				observed := make(chan error, 1)
				h := newNBDConcurrencyHarness(t, func(s *NBDTransmissionServer, c net.Conn) net.Conn {
					s.OnBackendError = func(err error) { observed <- err }
					return c
				})
				require.NoError(t, <-h.send(nbdCommandRead, 1, 0, LogicalBlockSize))
				first := h.nextCall(nbdCommandRead, 0)
				require.NoError(t, <-h.send(nbdCommandRead, 2, LogicalBlockSize, LogicalBlockSize))
				second := h.nextCall(nbdCommandRead, LogicalBlockSize)
				first.n, first.err = result.n, result.err
				close(first.release)
				h.reply(1, result.errno, nil)
				original := result.err
				if original == nil {
					original = io.ErrUnexpectedEOF
				}
				require.ErrorIs(t, <-observed, original)
				close(second.release)
				h.reply(2, 0, bytes.Repeat([]byte{0x11}, LogicalBlockSize))
				h.disconnect()
			})
		})
	}
}

func TestNBDConcurrentRepliesKeepHandlesAndPayloadsTogether(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		// Exercise writeFull and frame locking with short socket writes while
		// all read workers race to reply and report errors.
		var observed []error // The server must serialize this callback.
		h := newNBDConcurrencyHarness(t, func(s *NBDTransmissionServer, c net.Conn) net.Conn {
			s.MaxRequestBytes = nbdMaxConcurrentReads * LogicalBlockSize
			s.OnBackendError = func(err error) { observed = append(observed, err) }
			return &nbdShortWriteConn{Conn: c}
		})
		for i := 0; i < nbdMaxConcurrentReads; i++ {
			copy(h.backend.payload[i*LogicalBlockSize:], bytes.Repeat([]byte{byte(i)}, LogicalBlockSize))
		}
		for round := 0; round < 8; round++ {
			calls := make([]*nbdControlledCall, nbdMaxConcurrentReads)
			for i := range calls {
				require.NoError(t, <-h.send(nbdCommandRead, byte(i+1), uint64(i*LogicalBlockSize), LogicalBlockSize))
				calls[i] = h.nextCall(nbdCommandRead, int64(i*LogicalBlockSize))
				if i%2 == 0 {
					calls[i].err = syscall.EIO
				}
			}
			for _, call := range calls {
				close(call.release)
			}
			seen := make(map[byte]bool)
			for range calls {
				header := make([]byte, nbdReplyHeaderBytes)
				_, err := io.ReadFull(h.client, header)
				require.NoError(t, err)
				require.Equal(t, uint32(nbdReplyMagic), binary.BigEndian.Uint32(header[:4]))
				id := header[8]
				require.True(t, id >= 1 && int(id) <= len(calls))
				require.Equal(t, make([]byte, 7), header[9:])
				require.False(t, seen[id])
				seen[id] = true
				errno := binary.BigEndian.Uint32(header[4:8])
				if (id-1)%2 == 0 {
					require.Equal(t, uint32(syscall.EIO), errno)
				} else {
					require.Zero(t, errno)
					payload := make([]byte, LogicalBlockSize)
					_, err = io.ReadFull(h.client, payload)
					require.NoError(t, err)
					require.Equal(t, bytes.Repeat([]byte{id - 1}, LogicalBlockSize), payload)
				}
			}
		}
		h.disconnect()
		require.Len(t, observed, 8*((nbdMaxConcurrentReads+1)/2))
	})
}

type nbdShortWriteConn struct{ net.Conn }

func (c *nbdShortWriteConn) Write(payload []byte) (int, error) {
	return c.Conn.Write(payload[:min(7, len(payload))])
}

// A read-side transport failure must close a blocked reply before joining it.
// Unlike net.Pipe.Close, this wrapper leaves Write blocked until Serve closes.
type nbdReadFailureConn struct {
	net.Conn
	fail      <-chan struct{}
	err       error
	remaining int
}

func (c *nbdReadFailureConn) Read(payload []byte) (int, error) {
	if c.remaining == 0 {
		<-c.fail
		return 0, c.err
	}
	n, err := c.Conn.Read(payload[:min(c.remaining, len(payload))])
	c.remaining -= n
	return n, err
}

type nbdReplyFailureConn struct {
	net.Conn
	remaining int
	err       error
}

func (c *nbdReplyFailureConn) Write(payload []byte) (int, error) {
	if c.remaining == 0 {
		return 0, c.err
	}
	n, err := c.Conn.Write(payload[:min(c.remaining, len(payload))])
	c.remaining -= n
	return n, err
}

func TestNBDConcurrentReplyFailureStopsAdmissionAndJoinsReads(t *testing.T) {
	for _, remaining := range []int{0, 3, nbdReplyHeaderBytes + 7} {
		synctest.Test(t, func(t *testing.T) {
			failure := errors.New("injected reply failure")
			h := newNBDConcurrencyHarness(t, func(_ *NBDTransmissionServer, c net.Conn) net.Conn {
				return &nbdReplyFailureConn{Conn: c, remaining: remaining, err: failure}
			})
			require.NoError(t, <-h.send(nbdCommandRead, 1, 0, LogicalBlockSize))
			first := h.nextCall(nbdCommandRead, 0)
			require.NoError(t, <-h.send(nbdCommandRead, 2, LogicalBlockSize, LogicalBlockSize))
			second := h.nextCall(nbdCommandRead, LogicalBlockSize)
			// Leave the parser blocked in a partial header.
			require.NoError(t, <-h.sendRaw(makeNBDRequest(nbdCommandRead, [8]byte{3}, 0, LogicalBlockSize)[:5]))
			close(first.release)
			_, err := io.ReadFull(h.client, make([]byte, remaining))
			require.NoError(t, err)
			h.stillServing()
			close(second.release)
			<-h.done
			require.ErrorIs(t, h.err, failure)
			require.Empty(t, h.backend.calls)
			_, err = h.client.Read(make([]byte, 1))
			require.ErrorIs(t, err, io.EOF)
		})
	}
}

func TestNBDConcurrentReplyFailureSurvivesKernelLifetimeCancellation(t *testing.T) {
	for _, barrier := range []bool{false, true} {
		synctest.Test(t, func(t *testing.T) {
			failure := errors.New("injected reply failure before kernel exit")
			h := newNBDConcurrencyHarness(t, func(_ *NBDTransmissionServer, c net.Conn) net.Conn {
				return &nbdReplyFailureConn{Conn: c, err: failure}
			})
			require.NoError(t, <-h.send(nbdCommandRead, 1, 0, LogicalBlockSize))
			first := h.nextCall(nbdCommandRead, 0)
			require.NoError(t, <-h.send(nbdCommandRead, 2, LogicalBlockSize, LogicalBlockSize))
			second := h.nextCall(nbdCommandRead, LogicalBlockSize)
			if barrier {
				require.NoError(t, <-h.send(nbdCommandFlush, 3, 0, 0))
			}
			close(first.release)
			_, err := h.client.Read(make([]byte, 1))
			require.ErrorIs(t, err, io.EOF)
			// StartKernelNBD cancels Serve's parent lifetime when NBD_DO_IT
			// returns after the failed transmission closes the socket. That
			// secondary cancellation must not replace the original I/O error.
			h.cancel()
			h.stillServing()
			close(second.release)
			<-h.done
			require.ErrorIs(t, h.err, failure)
			require.NotErrorIs(t, h.err, context.Canceled)
			require.Empty(t, h.backend.calls)
		})
	}
}

func TestNBDConcurrentReadTransportErrorClosesBlockedReplyBeforeJoin(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		fail := make(chan struct{})
		failure := errors.New("injected request stream failure")
		h := newNBDConcurrencyHarness(t, func(_ *NBDTransmissionServer, c net.Conn) net.Conn {
			return &nbdReadFailureConn{Conn: c, fail: fail, err: failure, remaining: nbdRequestHeaderBytes}
		})
		require.NoError(t, <-h.send(nbdCommandRead, 1, 0, LogicalBlockSize))
		first := h.nextCall(nbdCommandRead, 0)
		close(first.release)
		require.Zero(t, readNBDReply(t, h.client, [8]byte{1}, LogicalBlockSize))
		// Leave the payload blocked while failing the independent request stream.
		close(fail)
		<-h.done
		require.ErrorIs(t, h.err, failure)
		require.Empty(t, h.backend.calls)
	})
}

func TestNBDConcurrentCancelJoinsMutationsAndFUA(t *testing.T) {
	for _, command := range []uint32{
		nbdCommandWrite, nbdCommandWrite | nbdCommandFlagFUA, nbdCommandFlush,
		nbdCommandTrim | nbdCommandFlagFUA, nbdCommandWriteZero | nbdCommandFlagFUA,
	} {
		t.Run(nbdTestCommandName(command), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				h := newNBDConcurrencyHarness(t, nil)
				length := uint32(LogicalBlockSize)
				if command == nbdCommandFlush {
					length = 0
				}
				require.NoError(t, <-h.send(command, 1, 0, length))
				operation := h.nextCall(command&nbdCommandMask, 0)
				later := h.send(nbdCommandRead, 2, 0, LogicalBlockSize)
				h.cancel()
				h.stillServing()
				close(operation.release)
				if command&nbdCommandFlagFUA != 0 {
					flush := h.nextCall(nbdCommandFlush, 0)
					h.stillServing()
					close(flush.release)
				}
				<-h.done
				require.ErrorIs(t, h.err, context.Canceled)
				require.Error(t, <-later)
				require.Empty(t, h.backend.calls)
			})
		})
	}
}

func TestNBDConcurrentCancelDuringWritePayloadDoesNotWrite(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newNBDConcurrencyHarness(t, nil)
		request := append(makeNBDRequest(nbdCommandWrite|nbdCommandFlagFUA, [8]byte{1}, 0, LogicalBlockSize), 1, 2)
		require.NoError(t, <-h.sendRaw(request))
		h.cancel()
		<-h.done
		require.ErrorIs(t, h.err, context.Canceled)
		require.Empty(t, h.backend.calls)
	})
}

func TestNBDConcurrentFUAFailureOrdersLaterReads(t *testing.T) {
	for _, command := range []uint32{
		nbdCommandWrite | nbdCommandFlagFUA, nbdCommandTrim | nbdCommandFlagFUA,
		nbdCommandWriteZero | nbdCommandFlagFUA,
	} {
		for _, failFlush := range []bool{false, true} {
			t.Run(nbdTestCommandName(command)+map[bool]string{false: " operation error", true: " flush error"}[failFlush], func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					observed := make(chan error, 1)
					h := newNBDConcurrencyHarness(t, func(s *NBDTransmissionServer, c net.Conn) net.Conn {
						s.OnBackendError = func(err error) { observed <- err }
						return c
					})
					require.NoError(t, <-h.send(command, 1, 0, LogicalBlockSize))
					operation := h.nextCall(command&nbdCommandMask, 0)
					later := h.send(nbdCommandRead, 2, 0, LogicalBlockSize)
					if !failFlush {
						operation.err = syscall.ENOSPC
					}
					close(operation.release)
					if failFlush {
						flush := h.nextCall(nbdCommandFlush, 0)
						h.noBackendCall()
						flush.err = syscall.ENOSPC
						close(flush.release)
					}
					h.noBackendCall() // Failed mutations skip FUA; failed FUA still orders replies.
					h.reply(1, syscall.ENOSPC, nil)
					require.ErrorIs(t, <-observed, syscall.ENOSPC)
					require.NoError(t, <-later)
					read := h.nextCall(nbdCommandRead, 0)
					close(read.release)
					expected := byte(0x11)
					switch command & nbdCommandMask {
					case nbdCommandWrite:
						expected = 0x22
					case nbdCommandWriteZero:
						expected = 0
					}
					h.reply(2, 0, bytes.Repeat([]byte{expected}, LogicalBlockSize))
					h.disconnect()
				})
			})
		}
	}
}

func TestNBDConcurrentTruncatedHeaderAndWritePayload(t *testing.T) {
	for _, write := range []bool{false, true} {
		synctest.Test(t, func(t *testing.T) {
			h := newNBDConcurrencyHarness(t, nil)
			require.NoError(t, <-h.send(nbdCommandRead, 1, 0, LogicalBlockSize))
			first := h.nextCall(nbdCommandRead, 0)
			request := makeNBDRequest(nbdCommandRead, [8]byte{2}, 0, LogicalBlockSize)[:5]
			if write {
				request = append(makeNBDRequest(nbdCommandWrite, [8]byte{2}, 0, LogicalBlockSize), 1, 2, 3)
			}
			sent := h.sendRaw(request)
			if write {
				close(first.release)
				h.reply(1, 0, bytes.Repeat([]byte{0x11}, LogicalBlockSize))
			}
			require.NoError(t, <-sent)
			require.NoError(t, h.client.Close())
			if !write {
				h.stillServing()
				close(first.release)
			}
			<-h.done
			require.ErrorIs(t, h.err, io.ErrUnexpectedEOF)
			require.Empty(t, h.backend.calls)
		})
	}
}
