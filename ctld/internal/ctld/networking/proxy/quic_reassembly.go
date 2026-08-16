package proxy

import (
	"bytes"
	"crypto"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"strings"
	"sync"
	"time"

	"github.com/cuonglm/quicsni"
	"github.com/quic-go/quic-go/quicvarint"
	"golang.org/x/crypto/hkdf"
	"src.agwa.name/tlshacks"
)

const (
	maxCryptoBuffer = 256 * 1024
	reassemblyTTL   = 20 * time.Second
	maxEntries      = 2048
)

type quicReassembler struct {
	mu      sync.Mutex
	streams map[string]*cryptoStream
}

type cryptoStream struct {
	nextOffset int64
	buffer     []byte
	pending    map[int64][]byte
	updatedAt  time.Time
}

func newQuicReassembler() *quicReassembler {
	return &quicReassembler{
		streams: make(map[string]*cryptoStream),
	}
}

func (r *quicReassembler) ParseSNI(packet []byte, srcIP, dstIP string) string {
	if len(packet) == 0 {
		return ""
	}
	if sni := parseSNIFromTLSHandshake(packet); sni != "" {
		return sni
	}
	hdr, offset, err := quicsni.ParseInitialHeader(packet)
	if err != nil {
		return ""
	}
	if offset+hdr.Length > int64(len(packet)) {
		return ""
	}
	key := buildStreamKey(srcIP, dstIP, hdr.DestConnectionID)
	unprotected, err := unprotectInitial(packet[:offset+hdr.Length], offset, hdr)
	if err != nil {
		return ""
	}

	frames, err := parseCryptoFrames(unprotected)
	if err != nil {
		return ""
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	r.cleanupLocked()

	stream := r.streams[key]
	if stream == nil {
		if len(r.streams) >= maxEntries {
			r.evictOldestLocked()
		}
		stream = &cryptoStream{
			nextOffset: 0,
			buffer:     []byte{},
			pending:    make(map[int64][]byte),
			updatedAt:  time.Now(),
		}
		r.streams[key] = stream
	}

	for _, frame := range frames {
		stream.add(frame.Offset, frame.Data)
	}
	stream.updatedAt = time.Now()

	if len(stream.buffer) == 0 {
		return ""
	}
	if ch := tlshacks.UnmarshalClientHello(stream.buffer); ch != nil && ch.Info.ServerName != nil {
		return strings.ToLower(*ch.Info.ServerName)
	}
	return ""
}

func buildStreamKey(srcIP, dstIP string, dcid []byte) string {
	return srcIP + "|" + dstIP + "|" + hex.EncodeToString(dcid)
}

func unprotectInitial(packet []byte, pnOffset int64, hdr *quicsni.Header) (out []byte, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			out = nil
			err = fmt.Errorf("unprotect quic initial: %v", recovered)
		}
	}()
	if len(packet) == 0 || packet[0]&0x80 == 0 {
		return nil, fmt.Errorf("quic initial packet is not a long-header packet")
	}
	if pnOffset < 0 || pnOffset+4+16 > int64(len(packet)) {
		return nil, fmt.Errorf("quic initial packet is too short")
	}
	initialSecret := hkdf.Extract(crypto.SHA256.New, hdr.DestConnectionID, getSalt(hdr.Version))
	clientSecret := hkdfExpandLabel(crypto.SHA256.New, initialSecret, "client in", []byte{}, crypto.SHA256.Size())
	key, err := quicsni.NewInitialProtectionKey(clientSecret, hdr.Version)
	if err != nil {
		return nil, err
	}
	pp := quicsni.NewPacketProtector(key)
	return pp.UnProtect(packet, pnOffset, 2)
}

type cryptoFrame struct {
	Offset int64
	Data   []byte
}

func parseCryptoFrames(payload []byte) ([]cryptoFrame, error) {
	reader := bytes.NewReader(payload)
	frames := []cryptoFrame{}
	for reader.Len() > 0 {
		typ, err := quicvarint.Read(reader)
		if err != nil {
			return nil, err
		}
		if typ == quicsni.PaddingFrameType {
			continue
		}
		if typ != quicsni.CryptoFrameType {
			return frames, nil
		}
		offset, err := quicvarint.Read(reader)
		if err != nil {
			return nil, err
		}
		length, err := quicvarint.Read(reader)
		if err != nil {
			return nil, err
		}
		if length > uint64(reader.Len()) {
			return nil, errors.New("invalid crypto frame length")
		}
		data := make([]byte, length)
		if _, err := reader.Read(data); err != nil {
			return nil, err
		}
		frames = append(frames, cryptoFrame{Offset: int64(offset), Data: data})
	}
	return frames, nil
}

func (s *cryptoStream) add(offset int64, data []byte) {
	if len(data) == 0 {
		return
	}
	if offset < s.nextOffset {
		trim := int(s.nextOffset - offset)
		if trim >= len(data) {
			return
		}
		data = data[trim:]
		offset = s.nextOffset
	}
	if offset == s.nextOffset {
		s.buffer = append(s.buffer, data...)
		s.nextOffset += int64(len(data))
		s.appendPending()
		s.trim()
		return
	}
	if s.pending == nil {
		s.pending = make(map[int64][]byte)
	}
	s.pending[offset] = data
}

func (s *cryptoStream) appendPending() {
	for {
		next, ok := s.pending[s.nextOffset]
		if !ok {
			return
		}
		delete(s.pending, s.nextOffset)
		s.buffer = append(s.buffer, next...)
		s.nextOffset += int64(len(next))
	}
}

func (s *cryptoStream) trim() {
	if len(s.buffer) <= maxCryptoBuffer {
		return
	}
	s.buffer = s.buffer[:maxCryptoBuffer]
	s.pending = make(map[int64][]byte)
}

func (r *quicReassembler) cleanupLocked() {
	cutoff := time.Now().Add(-reassemblyTTL)
	for key, stream := range r.streams {
		if stream.updatedAt.Before(cutoff) {
			delete(r.streams, key)
		}
	}
}

func (r *quicReassembler) evictOldestLocked() {
	var oldestKey string
	var oldest time.Time
	for key, stream := range r.streams {
		if oldestKey == "" || stream.updatedAt.Before(oldest) {
			oldestKey = key
			oldest = stream.updatedAt
		}
	}
	if oldestKey != "" {
		delete(r.streams, oldestKey)
	}
}

func getSalt(v uint32) []byte {
	switch v {
	case quicsni.V1:
		return []byte{0x38, 0x76, 0x2c, 0xf7, 0xf5, 0x59, 0x34, 0xb3, 0x4d, 0x17, 0x9a, 0xe6, 0xa4, 0xc8, 0x0c, 0xad, 0xcc, 0xbb, 0x7f, 0x0a}
	case quicsni.V2:
		return []byte{0x0d, 0xed, 0xe3, 0xde, 0xf7, 0x00, 0xa6, 0xdb, 0x81, 0x93, 0x81, 0xbe, 0x6e, 0x26, 0x9d, 0xcb, 0xf9, 0xbd, 0x2e, 0xd9}
	default:
		return []byte{0xaf, 0xbf, 0xec, 0x28, 0x99, 0x93, 0xd2, 0x4c, 0x9e, 0x97, 0x86, 0xf1, 0x9c, 0x61, 0x11, 0xe0, 0x43, 0x90, 0xa8, 0x99}
	}
}

func hkdfExpandLabel(hashFn func() hash.Hash, secret []byte, label string, context []byte, length int) []byte {
	// quicsni uses cryptobyte to build TLS 1.3 label; reuse the same format.
	builder := new(bytes.Buffer)
	_ = builder.WriteByte(byte(length >> 8))
	_ = builder.WriteByte(byte(length))
	writeUint8Vector(builder, []byte("tls13 "+label))
	writeUint8Vector(builder, context)
	out := make([]byte, length)
	n, err := hkdf.Expand(hashFn, secret, builder.Bytes()).Read(out)
	if err != nil || n != length {
		panic(fmt.Sprintf("hkdf expand failed: %v", err))
	}
	return out
}

func writeUint8Vector(buf *bytes.Buffer, value []byte) {
	if len(value) > 255 {
		value = value[:255]
	}
	_ = buf.WriteByte(byte(len(value)))
	_, _ = buf.Write(value)
}
