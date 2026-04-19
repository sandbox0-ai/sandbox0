package volproto

import (
	"encoding/binary"
	"fmt"
	"io"
)

const (
	Magic       uint32 = 0x50563053 // "S0VP" in little-endian order.
	Version     uint16 = 1
	HeaderBytes        = 24

	FlagError uint16 = 1 << 0
	FlagEvent uint16 = 1 << 1

	MaxPayloadBytes = 64 << 20
)

type Op uint16

const (
	OpMountVolume Op = iota + 1
	OpUnmountVolume
	OpHello
	OpHeartbeat
	OpWatchEvent

	OpLookup
	OpGetAttr
	OpSetAttr
	OpMkdir
	OpCreate
	OpUnlink
	OpRmdir
	OpRename
	OpLink
	OpSymlink
	OpReadlink
	OpAccess

	OpOpen
	OpRead
	OpWrite
	OpRelease
	OpFlush
	OpFsync
	OpFallocate
	OpCopyFileRange

	OpOpenDir
	OpReadDir
	OpReleaseDir
	OpStatFs

	OpGetXattr
	OpSetXattr
	OpListXattr
	OpRemoveXattr
	OpMknod

	OpGetLk
	OpSetLk
	OpSetLkw
	OpFlock
)

type Frame struct {
	RequestID uint64
	Op        Op
	Flags     uint16
	Payload   []byte
}

func WriteFrame(w io.Writer, frame Frame) error {
	if len(frame.Payload) > MaxPayloadBytes {
		return fmt.Errorf("s0vp payload too large: %d", len(frame.Payload))
	}
	var header [HeaderBytes]byte
	binary.LittleEndian.PutUint32(header[0:4], Magic)
	binary.LittleEndian.PutUint16(header[4:6], Version)
	binary.LittleEndian.PutUint16(header[6:8], uint16(frame.Op))
	binary.LittleEndian.PutUint16(header[8:10], frame.Flags)
	binary.LittleEndian.PutUint64(header[12:20], frame.RequestID)
	binary.LittleEndian.PutUint32(header[20:24], uint32(len(frame.Payload)))
	if _, err := w.Write(header[:]); err != nil {
		return err
	}
	if len(frame.Payload) == 0 {
		return nil
	}
	_, err := w.Write(frame.Payload)
	return err
}

func ReadFrame(r io.Reader) (Frame, error) {
	var header [HeaderBytes]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return Frame{}, err
	}
	if got := binary.LittleEndian.Uint32(header[0:4]); got != Magic {
		return Frame{}, fmt.Errorf("invalid s0vp magic: 0x%x", got)
	}
	if got := binary.LittleEndian.Uint16(header[4:6]); got != Version {
		return Frame{}, fmt.Errorf("unsupported s0vp version: %d", got)
	}
	payloadLen := binary.LittleEndian.Uint32(header[20:24])
	if payloadLen > MaxPayloadBytes {
		return Frame{}, fmt.Errorf("s0vp payload too large: %d", payloadLen)
	}
	payload := make([]byte, payloadLen)
	if payloadLen > 0 {
		if _, err := io.ReadFull(r, payload); err != nil {
			return Frame{}, err
		}
	}
	return Frame{
		Op:        Op(binary.LittleEndian.Uint16(header[6:8])),
		Flags:     binary.LittleEndian.Uint16(header[8:10]),
		RequestID: binary.LittleEndian.Uint64(header[12:20]),
		Payload:   payload,
	}, nil
}
