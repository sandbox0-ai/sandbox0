//go:build linux

// Package livehandoff implements a bounded, root-only local process handoff.
// It carries private node state; it must never be exposed as a tenant API.
package livehandoff

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"

	"golang.org/x/sys/unix"
)

const maxPayload = 16 << 20
const maxFiles = 2048
const batchFiles = 128

// RequireRoot checks the kernel's peer credentials, independently of pathname
// permissions. Call before receiving state or descriptors.
func RequireRoot(connection *net.UnixConn) error {
	raw, err := connection.SyscallConn()
	if err != nil {
		return err
	}
	var credential *unix.Ucred
	var credentialErr error
	if err := raw.Control(func(fd uintptr) {
		credential, credentialErr = unix.GetsockoptUcred(int(fd), unix.SOL_SOCKET, unix.SO_PEERCRED)
	}); err != nil {
		return err
	}
	if credentialErr != nil {
		return credentialErr
	}
	if credential == nil || credential.Uid != 0 {
		return fmt.Errorf("live handoff requires a root peer")
	}
	return nil
}

func Send(connection *net.UnixConn, payload any, files []*os.File) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	if len(data) > maxPayload || len(files) > maxFiles {
		return fmt.Errorf("live handoff exceeds envelope limit")
	}
	header := make([]byte, 8)
	binary.BigEndian.PutUint32(header, uint32(len(data)))
	binary.BigEndian.PutUint32(header[4:], uint32(len(files)))
	if err := write(connection, header); err != nil {
		return err
	}
	if err := write(connection, data); err != nil {
		return err
	}
	for offset := 0; offset < len(files); offset += batchFiles {
		count := min(batchFiles, len(files)-offset)
		descriptors := make([]int, count)
		for i, file := range files[offset : offset+count] {
			if file == nil {
				return fmt.Errorf("live handoff contains a nil descriptor")
			}
			descriptors[i] = int(file.Fd())
		}
		n, _, err := connection.WriteMsgUnix([]byte{1}, unix.UnixRights(descriptors...), nil)
		if err != nil {
			return err
		}
		if n != 1 {
			return io.ErrShortWrite
		}
	}
	return nil
}

func Receive(connection *net.UnixConn, payload any) (files []*os.File, result error) {
	defer func() {
		if result != nil {
			CloseFiles(files)
			files = nil
		}
	}()
	header := make([]byte, 8)
	if _, err := io.ReadFull(connection, header); err != nil {
		return nil, err
	}
	length, count := int(binary.BigEndian.Uint32(header)), int(binary.BigEndian.Uint32(header[4:]))
	if length <= 0 || length > maxPayload || count > maxFiles {
		return nil, fmt.Errorf("invalid live handoff envelope size")
	}
	data := make([]byte, length)
	if _, err := io.ReadFull(connection, data); err != nil {
		return nil, err
	}
	if err := json.Unmarshal(data, payload); err != nil {
		return nil, err
	}
	for len(files) < count {
		oob := make([]byte, unix.CmsgSpace(batchFiles*4))
		marker := make([]byte, 1)
		n, oobn, flags, _, err := connection.ReadMsgUnix(marker, oob)
		if err != nil {
			return files, err
		}
		messages, err := unix.ParseSocketControlMessage(oob[:oobn])
		if err != nil {
			return files, err
		}
		before := len(files)
		for _, message := range messages {
			descriptors, err := unix.ParseUnixRights(&message)
			if err != nil {
				return files, err
			}
			for _, fd := range descriptors {
				unix.CloseOnExec(fd)
				files = append(files, os.NewFile(uintptr(fd), "live-handoff"))
			}
		}
		if n != 1 || marker[0] != 1 || flags&unix.MSG_CTRUNC != 0 || len(files) == before || len(files) > count {
			return files, fmt.Errorf("invalid live handoff descriptor frame")
		}
	}
	return files, nil
}

func CloseFiles(files []*os.File) error {
	var result error
	for _, file := range files {
		if file != nil {
			result = errors.Join(result, file.Close())
		}
	}
	return result
}

func write(writer io.Writer, data []byte) error {
	for len(data) > 0 {
		n, err := writer.Write(data)
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		data = data[n:]
	}
	return nil
}
