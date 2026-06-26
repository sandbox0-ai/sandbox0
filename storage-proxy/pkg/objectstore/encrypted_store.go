package objectstore

import (
	"bufio"
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"

	"golang.org/x/crypto/chacha20poly1305"
)

const (
	defaultEncryptedObjectChunkSize = 1 << 20
	encryptedObjectMagic            = "s0.object.encrypted.v1\n"
	encryptedObjectVersion          = 1
	maxUint32                       = int64(^uint32(0))
	maxInt64                        = int64(^uint64(0) >> 1)
	maxInt                          = int64(^uint(0) >> 1)
)

// EncryptionConfig configures object-level envelope encryption for Store.
type EncryptionConfig struct {
	Enabled      bool
	Algorithm    string
	KeyEncryptor Encryptor
	ChunkSize    int64
}

type encryptedStore struct {
	store Store
	cfg   EncryptionConfig
}

type encryptedObjectHeader struct {
	Version     int    `json:"version"`
	Algorithm   string `json:"algorithm"`
	WrappedKey  []byte `json:"wrapped_key"`
	NoncePrefix []byte `json:"nonce_prefix"`
	ChunkSize   int64  `json:"chunk_size"`
}

// Encrypting wraps a Store with streaming object encryption when enabled.
func Encrypting(store Store, cfg EncryptionConfig) Store {
	if store == nil || !cfg.enabled() {
		return store
	}
	return &encryptedStore{store: store, cfg: cfg}
}

func (c EncryptionConfig) enabled() bool {
	return c.Enabled && c.KeyEncryptor != nil
}

func (c EncryptionConfig) normalizedAlgorithm() string {
	if strings.TrimSpace(c.Algorithm) == "" {
		return EncryptionAlgoAES256GCMRSA
	}
	return strings.TrimSpace(c.Algorithm)
}

func (c EncryptionConfig) chunkSize() int64 {
	if c.ChunkSize <= 0 {
		return defaultEncryptedObjectChunkSize
	}
	return c.ChunkSize
}

func (s *encryptedStore) String() string {
	if s == nil || s.store == nil {
		return "encrypted(<nil>)"
	}
	return "encrypted(" + s.store.String() + ")"
}

func (s *encryptedStore) Create() error {
	return s.store.Create()
}

func (s *encryptedStore) Put(key string, in io.Reader) error {
	if in == nil {
		in = bytes.NewReader(nil)
	}
	tmp, err := os.CreateTemp("", "s0-object-encrypted-*")
	if err != nil {
		return fmt.Errorf("create encrypted object temp file: %w", err)
	}
	defer func() {
		_ = tmp.Close()
		_ = os.Remove(tmp.Name())
	}()
	if err := s.encryptTo(tmp, key, in); err != nil {
		return err
	}
	if _, err := tmp.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("seek encrypted object temp file: %w", err)
	}
	return s.store.Put(key, tmp)
}

func (s *encryptedStore) Get(key string, off, limit int64) (io.ReadCloser, error) {
	if limit == 0 {
		return io.NopCloser(bytes.NewReader(nil)), nil
	}
	reader, err := s.store.Get(key, 0, -1)
	if err != nil {
		return nil, err
	}
	pr, pw := io.Pipe()
	go func() {
		defer reader.Close()
		err := s.decryptTo(pw, key, reader, off, limit)
		_ = pw.CloseWithError(err)
	}()
	return pr, nil
}

func (s *encryptedStore) Delete(key string) error {
	return s.store.Delete(key)
}

func (s *encryptedStore) Head(key string) (Info, error) {
	return s.store.Head(key)
}

func (s *encryptedStore) List(prefix, startAfter, token, delimiter string, limit int64) ([]Info, bool, string, error) {
	return s.store.List(prefix, startAfter, token, delimiter, limit)
}

func (s *encryptedStore) encryptTo(out io.Writer, key string, in io.Reader) error {
	dataKey := make([]byte, 32)
	if _, err := io.ReadFull(rand.Reader, dataKey); err != nil {
		return fmt.Errorf("generate object data key: %w", err)
	}
	wrappedKey, err := s.cfg.KeyEncryptor.Encrypt(dataKey)
	if err != nil {
		return fmt.Errorf("wrap object data key: %w", err)
	}
	aead, err := newObjectAEAD(s.cfg.normalizedAlgorithm(), dataKey)
	if err != nil {
		return err
	}
	noncePrefix := make([]byte, aead.NonceSize()-8)
	if _, err := io.ReadFull(rand.Reader, noncePrefix); err != nil {
		return fmt.Errorf("generate object nonce prefix: %w", err)
	}
	header := encryptedObjectHeader{
		Version:     encryptedObjectVersion,
		Algorithm:   s.cfg.normalizedAlgorithm(),
		WrappedKey:  wrappedKey,
		NoncePrefix: noncePrefix,
		ChunkSize:   s.cfg.chunkSize(),
	}
	if header.ChunkSize > maxInt {
		return fmt.Errorf("encrypted object chunk size is too large: %d", header.ChunkSize)
	}
	if err := writeEncryptedObjectHeader(out, header); err != nil {
		return err
	}
	buf := make([]byte, int(header.ChunkSize))
	for chunkIndex := uint64(0); ; chunkIndex++ {
		n, readErr := io.ReadFull(in, buf)
		if readErr == io.EOF {
			return nil
		}
		if readErr == io.ErrUnexpectedEOF {
			readErr = nil
		}
		if readErr != nil {
			return readErr
		}
		nonce := encryptedObjectNonce(aead.NonceSize(), noncePrefix, chunkIndex)
		ciphertext := aead.Seal(nil, nonce, buf[:n], encryptedObjectChunkAAD(key, chunkIndex, header.Algorithm))
		if int64(len(ciphertext)) > maxUint32 {
			return fmt.Errorf("encrypted object chunk is too large: %d", len(ciphertext))
		}
		var lenBuf [4]byte
		binary.BigEndian.PutUint32(lenBuf[:], uint32(len(ciphertext)))
		if _, err := out.Write(lenBuf[:]); err != nil {
			return err
		}
		if _, err := out.Write(ciphertext); err != nil {
			return err
		}
		if n < len(buf) {
			return nil
		}
	}
}

func (s *encryptedStore) decryptTo(out io.Writer, key string, in io.Reader, off, limit int64) error {
	if off < 0 {
		return fmt.Errorf("negative object read offset: %d", off)
	}
	buffered := bufio.NewReader(in)
	magic, err := buffered.Peek(len(encryptedObjectMagic))
	if err != nil || string(magic) != encryptedObjectMagic {
		return copyPlainRange(out, buffered, off, limit)
	}
	if _, err := buffered.Discard(len(encryptedObjectMagic)); err != nil {
		return err
	}
	header, err := readEncryptedObjectHeader(buffered)
	if err != nil {
		return err
	}
	if header.Version != encryptedObjectVersion {
		return fmt.Errorf("unsupported encrypted object version %d", header.Version)
	}
	if header.ChunkSize <= 0 {
		return fmt.Errorf("encrypted object chunk size is required")
	}
	dataKey, err := s.cfg.KeyEncryptor.Decrypt(header.WrappedKey)
	if err != nil {
		return fmt.Errorf("unwrap object data key: %w", err)
	}
	aead, err := newObjectAEAD(header.Algorithm, dataKey)
	if err != nil {
		return err
	}
	if len(header.NoncePrefix) != aead.NonceSize()-8 {
		return fmt.Errorf("invalid encrypted object nonce prefix size %d", len(header.NoncePrefix))
	}
	rangeEnd := int64(-1)
	if limit >= 0 {
		rangeEnd = off + limit
		if rangeEnd < off {
			rangeEnd = maxInt64
		}
	}
	var plainOffset int64
	for chunkIndex := uint64(0); ; chunkIndex++ {
		var lenBuf [4]byte
		if _, err := io.ReadFull(buffered, lenBuf[:]); err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
		cipherLen := binary.BigEndian.Uint32(lenBuf[:])
		if cipherLen == 0 || int64(cipherLen) > header.ChunkSize+int64(aead.Overhead()) {
			return fmt.Errorf("invalid encrypted object chunk size %d", cipherLen)
		}
		ciphertext := make([]byte, cipherLen)
		if _, err := io.ReadFull(buffered, ciphertext); err != nil {
			return err
		}
		nonce := encryptedObjectNonce(aead.NonceSize(), header.NoncePrefix, chunkIndex)
		plaintext, err := aead.Open(nil, nonce, ciphertext, encryptedObjectChunkAAD(key, chunkIndex, header.Algorithm))
		if err != nil {
			return fmt.Errorf("decrypt object chunk %d: %w", chunkIndex, err)
		}
		if err := writeRangeChunk(out, plaintext, plainOffset, off, rangeEnd); err != nil {
			return err
		}
		plainOffset += int64(len(plaintext))
		if rangeEnd >= 0 && plainOffset >= rangeEnd {
			return nil
		}
	}
}

func writeEncryptedObjectHeader(out io.Writer, header encryptedObjectHeader) error {
	headerBytes, err := json.Marshal(header)
	if err != nil {
		return fmt.Errorf("marshal encrypted object header: %w", err)
	}
	if int64(len(headerBytes)) > maxUint32 {
		return fmt.Errorf("encrypted object header is too large: %d", len(headerBytes))
	}
	if _, err := out.Write([]byte(encryptedObjectMagic)); err != nil {
		return err
	}
	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(headerBytes)))
	if _, err := out.Write(lenBuf[:]); err != nil {
		return err
	}
	_, err = out.Write(headerBytes)
	return err
}

func readEncryptedObjectHeader(in io.Reader) (encryptedObjectHeader, error) {
	var lenBuf [4]byte
	if _, err := io.ReadFull(in, lenBuf[:]); err != nil {
		return encryptedObjectHeader{}, err
	}
	headerLen := binary.BigEndian.Uint32(lenBuf[:])
	if headerLen == 0 || headerLen > 1<<20 {
		return encryptedObjectHeader{}, fmt.Errorf("invalid encrypted object header size %d", headerLen)
	}
	headerBytes := make([]byte, headerLen)
	if _, err := io.ReadFull(in, headerBytes); err != nil {
		return encryptedObjectHeader{}, err
	}
	var header encryptedObjectHeader
	if err := json.Unmarshal(headerBytes, &header); err != nil {
		return encryptedObjectHeader{}, fmt.Errorf("unmarshal encrypted object header: %w", err)
	}
	return header, nil
}

func newObjectAEAD(algorithm string, key []byte) (cipher.AEAD, error) {
	algorithm = strings.TrimSpace(algorithm)
	if algorithm == "" {
		algorithm = EncryptionAlgoAES256GCMRSA
	}
	switch algorithm {
	case EncryptionAlgoAES256GCMRSA:
		block, err := aes.NewCipher(key)
		if err != nil {
			return nil, err
		}
		return cipher.NewGCM(block)
	case EncryptionAlgoCHACHA20RSA:
		return chacha20poly1305.New(key)
	default:
		return nil, fmt.Errorf("unsupported object encryption algorithm: %s", algorithm)
	}
}

func encryptedObjectNonce(nonceSize int, prefix []byte, chunkIndex uint64) []byte {
	nonce := make([]byte, nonceSize)
	copy(nonce, prefix)
	binary.BigEndian.PutUint64(nonce[nonceSize-8:], chunkIndex)
	return nonce
}

func encryptedObjectChunkAAD(key string, chunkIndex uint64, algorithm string) []byte {
	return []byte(fmt.Sprintf("s0.object.encrypted.v1|%s|%d|%s", key, chunkIndex, algorithm))
}

func copyPlainRange(out io.Writer, in io.Reader, off, limit int64) error {
	if limit == 0 {
		return nil
	}
	if off > 0 {
		if _, err := io.CopyN(io.Discard, in, off); err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
	}
	if limit < 0 {
		_, err := io.Copy(out, in)
		return err
	}
	if _, err := io.CopyN(out, in, limit); err != nil && err != io.EOF {
		return err
	}
	return nil
}

func writeRangeChunk(out io.Writer, chunk []byte, chunkStart, rangeStart, rangeEnd int64) error {
	chunkEnd := chunkStart + int64(len(chunk))
	if chunkEnd <= rangeStart {
		return nil
	}
	start := int64(0)
	if rangeStart > chunkStart {
		start = rangeStart - chunkStart
	}
	end := int64(len(chunk))
	if rangeEnd >= 0 && chunkEnd > rangeEnd {
		end = rangeEnd - chunkStart
	}
	if start >= end {
		return nil
	}
	_, err := out.Write(chunk[int(start):int(end)])
	return err
}
