package rootfsblock

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func handoffSocket(t *testing.T) (net.Conn, net.Conn) {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "s0-nbd-")
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	listener, err := net.Listen("unix", filepath.Join(dir, "nbd.sock"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })
	client, err := net.Dial("unix", listener.Addr().String())
	require.NoError(t, err)
	server, err := listener.Accept()
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close(); _ = server.Close() })
	require.NoError(t, client.SetDeadline(time.Now().Add(10*time.Second)))
	return client, server
}

func TestNBDHandoffRepeatedOwnershipTransfer(t *testing.T) {
	client, server := handoffSocket(t)
	backend := &memoryBlockBackend{payload: make([]byte, 4*LogicalBlockSize)}
	for i := byte(1); i <= 12; i++ {
		control := &NBDTransmissionControl{}
		done := make(chan error, 1)
		go func() { done <- (NBDTransmissionServer{Backend: backend, Handoff: control}).Serve(t.Context(), server) }()
		payload := bytes.Repeat([]byte{i}, LogicalBlockSize)
		writeNBDRequest(t, client, nbdCommandWrite|nbdCommandFlagFUA, [8]byte{i}, 0, payload)
		require.Zero(t, readNBDReply(t, client, [8]byte{i}, 0))
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		require.NoError(t, control.Pause(ctx))
		cancel()
		file, err := control.File()
		require.NoError(t, err)
		require.NoError(t, control.Detach())
		require.ErrorIs(t, <-done, ErrNBDHandoff)
		require.NoError(t, server.Close())
		server, err = net.FileConn(file)
		require.NoError(t, err)
		require.NoError(t, file.Close())
		require.Equal(t, payload, backend.payload[:LogicalBlockSize])
	}
	done := make(chan error, 1)
	go func() { done <- (NBDTransmissionServer{Backend: backend}).Serve(t.Context(), server) }()
	writeNBDRequest(t, client, nbdCommandRead, [8]byte{90}, 0, make([]byte, LogicalBlockSize))
	require.Zero(t, readNBDReply(t, client, [8]byte{90}, LogicalBlockSize))
	actual := make([]byte, LogicalBlockSize)
	_, err := io.ReadFull(client, actual)
	require.NoError(t, err)
	require.Equal(t, bytes.Repeat([]byte{12}, LogicalBlockSize), actual)
	writeNBDRequest(t, client, nbdCommandDisconnect, [8]byte{91}, 0, nil)
	require.NoError(t, <-done)
}

func TestNBDHandoffPartialFrameTimeoutResumesSource(t *testing.T) {
	for _, prefix := range []int{1, 13, 27, 28 + 512} {
		t.Run(strconv.Itoa(prefix), func(t *testing.T) {
			client, server := handoffSocket(t)
			backend := &memoryBlockBackend{payload: make([]byte, 2*LogicalBlockSize)}
			control := &NBDTransmissionControl{}
			done := make(chan error, 1)
			go func() { done <- (NBDTransmissionServer{Backend: backend, Handoff: control}).Serve(t.Context(), server) }()
			payload := bytes.Repeat([]byte{0x53}, LogicalBlockSize)
			frame := append(makeNBDRequest(nbdCommandWrite|nbdCommandFlagFUA, [8]byte{1}, 0, LogicalBlockSize), payload...)
			require.NoError(t, writeFull(client, frame[:prefix]))
			// Wait until the parser consumed the prefix; an idle boundary before
			// consumption is transferable and deliberately has different semantics.
			require.Eventually(t, func() bool {
				control.mu.Lock()
				defer control.mu.Unlock()
				return control.connection != nil && (control.headerRead || prefix > 28)
			}, time.Second, time.Millisecond)
			ctx, cancel := context.WithTimeout(t.Context(), 30*time.Millisecond)
			err := control.Pause(ctx)
			cancel()
			if err == nil {
				control.Resume() // Prefix was still queued on the socket.
			} else {
				require.ErrorIs(t, err, context.DeadlineExceeded)
			}
			require.NoError(t, writeFull(client, frame[prefix:]))
			require.Zero(t, readNBDReply(t, client, [8]byte{1}, 0))
			require.Equal(t, payload, backend.payload[:LogicalBlockSize])
			require.Equal(t, 1, backend.flushes)
			ctx, cancel = context.WithTimeout(t.Context(), time.Second)
			require.NoError(t, control.Pause(ctx))
			cancel()
			control.Resume()
			writeNBDRequest(t, client, nbdCommandDisconnect, [8]byte{2}, 0, nil)
			require.NoError(t, <-done)
		})
	}
}

func TestNBDHandoffWaitsForReplyAndCanAbort(t *testing.T) {
	client, server := net.Pipe() // Replies cannot complete until the peer reads.
	t.Cleanup(func() { _ = client.Close(); _ = server.Close() })
	backend := &memoryBlockBackend{payload: make([]byte, 2*LogicalBlockSize)}
	control := &NBDTransmissionControl{}
	done := make(chan error, 1)
	go func() { done <- (NBDTransmissionServer{Backend: backend, Handoff: control}).Serve(t.Context(), server) }()
	writeNBDRequest(t, client, nbdCommandRead, [8]byte{1}, 0, make([]byte, LogicalBlockSize))
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Millisecond)
	require.ErrorIs(t, control.Pause(ctx), context.DeadlineExceeded)
	cancel()
	require.Zero(t, readNBDReply(t, client, [8]byte{1}, LogicalBlockSize))
	_, err := io.CopyN(io.Discard, client, LogicalBlockSize)
	require.NoError(t, err)
	ctx, cancel = context.WithTimeout(t.Context(), time.Second)
	require.NoError(t, control.Pause(ctx))
	cancel()
	_, err = control.File()
	require.ErrorContains(t, err, "export descriptors")
	control.Resume()
	writeNBDRequest(t, client, nbdCommandDisconnect, [8]byte{2}, 0, nil)
	require.NoError(t, <-done)
}

func TestNBDHandoffClosedTransportDoesNotHang(t *testing.T) {
	client, server := handoffSocket(t)
	control := &NBDTransmissionControl{}
	done := make(chan error, 1)
	go func() {
		done <- (NBDTransmissionServer{Backend: &memoryBlockBackend{payload: make([]byte, LogicalBlockSize)}, Handoff: control}).Serve(t.Context(), server)
	}()
	require.NoError(t, client.Close())
	require.NoError(t, <-done)
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	err := control.Pause(ctx)
	require.Error(t, err)
	require.False(t, errors.Is(err, context.DeadlineExceeded))
}
