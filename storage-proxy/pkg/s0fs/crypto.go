package s0fs

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/objectstore"
	"golang.org/x/crypto/chacha20poly1305"
)

const (
	defaultEncryptionChunkSize uint64 = 1 << 20

	encryptedBlobMarker = "s0fs.encrypted.v1"

	segmentEncryptionVersion = 1
)

type EncryptionConfig struct {
	Enabled          bool
	Algorithm        string
	KeyEncryptor     objectstore.Encryptor
	SegmentChunkSize uint64
}

type SegmentEncryption struct {
	Version        int    `json:"version"`
	Algorithm      string `json:"algorithm"`
	ChunkSize      uint64 `json:"chunk_size"`
	PlaintextSize  uint64 `json:"plaintext_size"`
	CiphertextSize uint64 `json:"ciphertext_size"`
	WrappedKey     []byte `json:"wrapped_key"`
	NoncePrefix    []byte `json:"nonce_prefix"`
}

type encryptedBlob struct {
	Marker     string `json:"s0fs_encrypted"`
	Version    int    `json:"version"`
	Algorithm  string `json:"algorithm"`
	WrappedKey []byte `json:"wrapped_key"`
	Nonce      []byte `json:"nonce"`
	Ciphertext []byte `json:"ciphertext"`
}

func (c *EncryptionConfig) enabled() bool {
	return c != nil && c.Enabled && c.KeyEncryptor != nil
}

func (c *EncryptionConfig) normalizedAlgorithm() string {
	if c == nil || strings.TrimSpace(c.Algorithm) == "" {
		return objectstore.EncryptionAlgoAES256GCMRSA
	}
	return strings.TrimSpace(c.Algorithm)
}

func (c *EncryptionConfig) chunkSize() uint64 {
	if c == nil || c.SegmentChunkSize == 0 {
		return defaultEncryptionChunkSize
	}
	return c.SegmentChunkSize
}

func (c *EncryptionConfig) newAEAD(key []byte) (cipher.AEAD, error) {
	return newAEADForAlgorithm(c.normalizedAlgorithm(), key)
}

func newAEADForAlgorithm(algorithm string, key []byte) (cipher.AEAD, error) {
	algorithm = strings.TrimSpace(algorithm)
	if algorithm == "" {
		algorithm = objectstore.EncryptionAlgoAES256GCMRSA
	}
	switch algorithm {
	case objectstore.EncryptionAlgoAES256GCMRSA:
		block, err := aes.NewCipher(key)
		if err != nil {
			return nil, err
		}
		return cipher.NewGCM(block)
	case objectstore.EncryptionAlgoCHACHA20RSA:
		return chacha20poly1305.New(key)
	default:
		return nil, fmt.Errorf("unsupported s0fs encryption algorithm: %s", algorithm)
	}
}

func (c *EncryptionConfig) newDataKey() ([]byte, error) {
	key := make([]byte, 32)
	if _, err := io.ReadFull(rand.Reader, key); err != nil {
		return nil, err
	}
	return key, nil
}

func (c *EncryptionConfig) encryptBlob(plaintext []byte, aad []byte) ([]byte, error) {
	if !c.enabled() {
		return plaintext, nil
	}
	key, err := c.newDataKey()
	if err != nil {
		return nil, err
	}
	wrappedKey, err := c.KeyEncryptor.Encrypt(key)
	if err != nil {
		return nil, fmt.Errorf("wrap data key: %w", err)
	}
	aead, err := c.newAEAD(key)
	if err != nil {
		return nil, err
	}
	nonce := make([]byte, aead.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, err
	}
	blob := encryptedBlob{
		Marker:     encryptedBlobMarker,
		Version:    1,
		Algorithm:  c.normalizedAlgorithm(),
		WrappedKey: wrappedKey,
		Nonce:      nonce,
		Ciphertext: aead.Seal(nil, nonce, plaintext, aad),
	}
	payload, err := json.Marshal(blob)
	if err != nil {
		return nil, fmt.Errorf("marshal encrypted blob: %w", err)
	}
	return payload, nil
}

func (c *EncryptionConfig) decryptBlobIfEncrypted(payload []byte, aad []byte) ([]byte, bool, error) {
	var blob encryptedBlob
	if err := json.Unmarshal(payload, &blob); err != nil || blob.Marker != encryptedBlobMarker {
		return payload, false, nil
	}
	if !c.enabled() {
		return nil, true, fmt.Errorf("%w: encrypted state requires encryption config", ErrInvalidInput)
	}
	if blob.Version != 1 {
		return nil, true, fmt.Errorf("%w: unsupported encrypted blob version %d", ErrInvalidInput, blob.Version)
	}
	key, err := c.KeyEncryptor.Decrypt(blob.WrappedKey)
	if err != nil {
		return nil, true, fmt.Errorf("unwrap data key: %w", err)
	}
	aead, err := newAEADForAlgorithm(blob.Algorithm, key)
	if err != nil {
		return nil, true, err
	}
	plaintext, err := aead.Open(nil, blob.Nonce, blob.Ciphertext, aad)
	if err != nil {
		return nil, true, fmt.Errorf("decrypt encrypted blob: %w", err)
	}
	return plaintext, true, nil
}

func (c *EncryptionConfig) encryptSegment(volumeID string, segment *materializedSegment) ([]byte, *SegmentEncryption, error) {
	if !c.enabled() || segment == nil || len(segment.Payload) == 0 {
		if segment == nil {
			return nil, nil, nil
		}
		return segment.Payload, nil, nil
	}
	key, err := c.newDataKey()
	if err != nil {
		return nil, nil, err
	}
	wrappedKey, err := c.KeyEncryptor.Encrypt(key)
	if err != nil {
		return nil, nil, fmt.Errorf("wrap segment data key: %w", err)
	}
	aead, err := c.newAEAD(key)
	if err != nil {
		return nil, nil, err
	}
	noncePrefix := make([]byte, aead.NonceSize()-8)
	if _, err := io.ReadFull(rand.Reader, noncePrefix); err != nil {
		return nil, nil, err
	}
	chunkSize := c.chunkSize()
	var out bytes.Buffer
	for chunkIndex, offset := uint64(0), uint64(0); offset < uint64(len(segment.Payload)); chunkIndex, offset = chunkIndex+1, offset+chunkSize {
		end := offset + chunkSize
		if end > uint64(len(segment.Payload)) {
			end = uint64(len(segment.Payload))
		}
		chunk := segment.Payload[offset:end]
		nonce := segmentChunkNonce(aead.NonceSize(), noncePrefix, chunkIndex)
		aad := segmentChunkAAD(volumeID, segment.ID, segment.Key, chunkIndex, uint64(len(chunk)), c.normalizedAlgorithm())
		out.Write(aead.Seal(nil, nonce, chunk, aad))
	}
	meta := &SegmentEncryption{
		Version:        segmentEncryptionVersion,
		Algorithm:      c.normalizedAlgorithm(),
		ChunkSize:      chunkSize,
		PlaintextSize:  uint64(len(segment.Payload)),
		CiphertextSize: uint64(out.Len()),
		WrappedKey:     wrappedKey,
		NoncePrefix:    noncePrefix,
	}
	return out.Bytes(), meta, nil
}

func (c *EncryptionConfig) decryptSegmentRange(store objectstore.Store, volumeID string, segment *Segment, off, limit int64) ([]byte, error) {
	if store == nil {
		return nil, fmt.Errorf("%w: object store is not configured", ErrInvalidInput)
	}
	if segment == nil || segment.Encryption == nil {
		return nil, fmt.Errorf("%w: segment encryption metadata is required", ErrInvalidInput)
	}
	if !c.enabled() {
		return nil, fmt.Errorf("%w: encrypted segment requires encryption config", ErrInvalidInput)
	}
	enc := segment.Encryption
	if enc.Version != segmentEncryptionVersion {
		return nil, fmt.Errorf("%w: unsupported segment encryption version %d", ErrInvalidInput, enc.Version)
	}
	if enc.ChunkSize == 0 {
		return nil, fmt.Errorf("%w: encrypted segment chunk size is required", ErrInvalidInput)
	}
	if off < 0 {
		return nil, fmt.Errorf("%w: negative segment offset", ErrInvalidInput)
	}
	plaintextSize := enc.PlaintextSize
	if plaintextSize == 0 {
		plaintextSize = segment.Length
	}
	if uint64(off) >= plaintextSize || limit == 0 {
		return nil, nil
	}
	end := plaintextSize
	if limit >= 0 && uint64(off)+uint64(limit) < end {
		end = uint64(off) + uint64(limit)
	}
	key, err := c.KeyEncryptor.Decrypt(enc.WrappedKey)
	if err != nil {
		return nil, fmt.Errorf("unwrap segment data key: %w", err)
	}
	aead, err := newAEADForAlgorithm(enc.Algorithm, key)
	if err != nil {
		return nil, err
	}
	startChunk := uint64(off) / enc.ChunkSize
	endChunk := (end - 1) / enc.ChunkSize
	var out bytes.Buffer
	for chunkIndex := startChunk; chunkIndex <= endChunk; chunkIndex++ {
		plainStart := chunkIndex * enc.ChunkSize
		plainLen := enc.ChunkSize
		if plainStart+plainLen > plaintextSize {
			plainLen = plaintextSize - plainStart
		}
		cipherOffset := int64(chunkIndex * (enc.ChunkSize + uint64(aead.Overhead())))
		cipherLen := int64(plainLen + uint64(aead.Overhead()))
		reader, err := store.Get(segment.Key, cipherOffset, cipherLen)
		if err != nil {
			return nil, err
		}
		ciphertext, readErr := io.ReadAll(reader)
		closeErr := reader.Close()
		if readErr != nil {
			return nil, readErr
		}
		if closeErr != nil {
			return nil, closeErr
		}
		nonce := segmentChunkNonce(aead.NonceSize(), enc.NoncePrefix, chunkIndex)
		aad := segmentChunkAAD(volumeID, segment.ID, segment.Key, chunkIndex, plainLen, enc.Algorithm)
		chunk, err := aead.Open(nil, nonce, ciphertext, aad)
		if err != nil {
			return nil, fmt.Errorf("decrypt segment chunk %d: %w", chunkIndex, err)
		}
		chunkFrom := uint64(0)
		if uint64(off) > plainStart {
			chunkFrom = uint64(off) - plainStart
		}
		chunkTo := plainLen
		if end < plainStart+plainLen {
			chunkTo = end - plainStart
		}
		out.Write(chunk[chunkFrom:chunkTo])
	}
	return out.Bytes(), nil
}

func segmentChunkNonce(nonceSize int, prefix []byte, chunkIndex uint64) []byte {
	nonce := make([]byte, nonceSize)
	copy(nonce, prefix)
	binary.BigEndian.PutUint64(nonce[nonceSize-8:], chunkIndex)
	return nonce
}

func segmentChunkAAD(volumeID, segmentID, key string, chunkIndex, plainLen uint64, algorithm string) []byte {
	return []byte(fmt.Sprintf("s0fs-segment-v1\nvolume=%s\nsegment=%s\nkey=%s\nchunk=%d\nplain_len=%d\nalgorithm=%s",
		volumeID, segmentID, key, chunkIndex, plainLen, algorithm))
}

func stateBlobAAD(volumeID, role string) []byte {
	return []byte("s0fs-state-v1\nvolume=" + volumeID + "\nrole=" + role)
}

func walRecordAAD(volumeID string) []byte {
	return []byte("s0fs-wal-v1\nvolume=" + volumeID)
}
