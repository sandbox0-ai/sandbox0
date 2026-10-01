//go:build linux

package livehandoff

import (
	"encoding/binary"
	"net"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

func testSocketPair(t *testing.T) (*net.UnixConn, *net.UnixConn) {
	t.Helper()
	fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM|unix.SOCK_CLOEXEC, 0)
	require.NoError(t, err)
	connections := make([]*net.UnixConn, 2)
	for i, fd := range fds {
		file := os.NewFile(uintptr(fd), "test-handoff")
		conn, err := net.FileConn(file)
		require.NoError(t, err)
		require.NoError(t, file.Close())
		connections[i] = conn.(*net.UnixConn)
		require.NoError(t, conn.SetDeadline(time.Now().Add(3*time.Second)))
		t.Cleanup(func() { _ = conn.Close() })
	}
	return connections[0], connections[1]
}

func TestLiveTransportBatchesManyDescriptorsAndPreservesOpenDescriptions(t *testing.T) {
	source, candidate := testSocketPair(t)
	if os.Getuid() == 0 {
		require.NoError(t, RequireRoot(candidate))
	} else {
		require.Error(t, RequireRoot(candidate))
	}
	file, err := os.CreateTemp(t.TempDir(), "payload")
	require.NoError(t, err)
	defer file.Close()
	_, err = file.Write([]byte("survives source close"))
	require.NoError(t, err)
	files := make([]*os.File, 1029)
	for i := range files {
		files[i] = file
	}
	done := make(chan error, 1)
	go func() { done <- Send(source, map[string]string{"operation": "release"}, files) }()
	var payload map[string]string
	received, err := Receive(candidate, &payload)
	require.NoError(t, err)
	defer CloseFiles(received)
	require.NoError(t, <-done)
	require.Len(t, received, len(files))
	require.Equal(t, "release", payload["operation"])
	require.NoError(t, file.Close())
	for _, fd := range received {
		flags, err := unix.FcntlInt(fd.Fd(), unix.F_GETFD, 0)
		require.NoError(t, err)
		require.NotZero(t, flags&unix.FD_CLOEXEC)
		data := make([]byte, 21)
		_, err = fd.ReadAt(data, 0)
		require.NoError(t, err)
		require.Equal(t, "survives source close", string(data))
	}
}

func TestLiveTransportRejectsInvalidFramesWithoutLeakingDescriptors(t *testing.T) {
	for _, scenario := range []string{"too many", "truncated", "bad marker", "partial transfer"} {
		t.Run(scenario, func(t *testing.T) {
			source, candidate := testSocketPair(t)
			file, err := os.Open("/dev/null")
			require.NoError(t, err)
			defer file.Close()
			before, err := os.ReadDir("/proc/self/fd")
			require.NoError(t, err)
			count, sent, marker := 1, 2, byte(1)
			switch scenario {
			case "truncated":
				count, sent = 200, 200
			case "bad marker":
				sent, marker = 1, 9
			case "partial transfer":
				count, sent = 129, 128
			}
			header := make([]byte, 8)
			binary.BigEndian.PutUint32(header, 2)
			binary.BigEndian.PutUint32(header[4:], uint32(count))
			require.NoError(t, write(source, append(header, []byte("{}")...)))
			descriptors := make([]int, sent)
			for i := range descriptors {
				descriptors[i] = int(file.Fd())
			}
			_, _, err = source.WriteMsgUnix([]byte{marker}, unix.UnixRights(descriptors...), nil)
			require.NoError(t, err)
			require.NoError(t, source.Close())
			var payload any
			received, err := Receive(candidate, &payload)
			require.Error(t, err)
			require.Empty(t, received)
			after, err := os.ReadDir("/proc/self/fd")
			require.NoError(t, err)
			require.Equal(t, len(before)-1, len(after), "all SCM_RIGHTS copies must close on rejection")
		})
	}
}
