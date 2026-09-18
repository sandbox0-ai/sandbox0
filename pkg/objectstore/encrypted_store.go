package objectstore

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"

	"golang.org/x/crypto/chacha20poly1305"
)

const (
	defaultEncryptedObjectChunkSize = 1 << 20
	encryptedObjectMagic            = "s0.object.encrypted.v1\n"
	encryptedObjectVersion          = 1
	maxEncryptedObjectHeaderBytes   = 1 << 20
	encryptedObjectHeaderProbeBytes = 1 << 10
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
	store             Store
	cfg               EncryptionConfig
	headerCache       *encryptedHeaderCache
	parallelReadsOnce sync.Once
	parallelReads     chan struct{}
}

type encryptedObjectHeader struct {
	Version     int    `json:"version"`
	Algorithm   string `json:"algorithm"`
	WrappedKey  []byte `json:"wrapped_key"`
	NoncePrefix []byte `json:"nonce_prefix"`
	ChunkSize   int64  `json:"chunk_size"`
}

var _ ContextConditionalStore = (*encryptedStore)(nil)

// Encrypting wraps a Store with streaming object encryption when enabled.
func Encrypting(store Store, cfg EncryptionConfig) Store {
	if store == nil || !cfg.enabled() {
		return store
	}
	return &encryptedStore{store: store, cfg: cfg}
}

// EncryptingImmutable opts into bounded, on-demand header and AEAD caching.
// The caller must guarantee that canonical keys identify immutable plaintext.
// Recreating a deleted key with fresh envelope encryption is supported: an
// unauthenticated first frame can trigger one header refresh before any plaintext
// delivery. Data is retried only when the encryption header has actually changed.
// Use conditional creates and verify collisions; plaintext digests still need
// independent verification. Mutations through this wrapper invalidate its cache,
// but cannot invalidate other wrappers. Generic stores must use Encrypting.
func EncryptingImmutable(store Store, cfg EncryptionConfig, cache EncryptedHeaderCacheConfig) Store {
	if store == nil || !cfg.enabled() {
		return store
	}
	return &encryptedStore{store: store, cfg: cfg, headerCache: newEncryptedHeaderCache(cache)}
}

func (c EncryptionConfig) enabled() bool {
	return c.Enabled && c.KeyEncryptor != nil
}

// IsEnabled reports whether the configuration can encrypt and decrypt objects.
func (c EncryptionConfig) IsEnabled() bool {
	return c.enabled()
}

// HasEncryptedObjectHeader reports whether a reader starts with the Sandbox0
// encrypted-object format marker. The consumed bytes are not restored.
func HasEncryptedObjectHeader(in io.Reader) (bool, error) {
	if in == nil {
		return false, nil
	}
	prefix := make([]byte, len(encryptedObjectMagic))
	if _, err := io.ReadFull(in, prefix); err != nil {
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return false, nil
		}
		return false, err
	}
	return string(prefix) == encryptedObjectMagic, nil
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
	_, err := s.put(context.Background(), key, in, false, false)
	return err
}

func (s *encryptedStore) PutIfAbsent(key string, in io.Reader) (bool, error) {
	return s.put(context.Background(), key, in, true, false)
}

func (s *encryptedStore) PutIfAbsentContext(ctx context.Context, key string, in io.Reader) (bool, error) {
	return s.put(ctx, key, in, true, true)
}

func (s *encryptedStore) supportsConditionalCreate() bool {
	return s != nil && SupportsConditionalCreate(s.store)
}

func (s *encryptedStore) supportsContextConditionalCreate() bool {
	return s != nil && SupportsContextConditionalCreate(s.store)
}

func (s *encryptedStore) put(
	ctx context.Context,
	key string,
	in io.Reader,
	conditional bool,
	requireContext bool,
) (bool, error) {
	if ctx == nil {
		return false, fmt.Errorf("object write context is required")
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if in == nil {
		in = bytes.NewReader(nil)
	}
	in = withObjectReadContext(ctx, in)
	tmp, err := os.CreateTemp("", "s0-object-encrypted-*")
	if err != nil {
		return false, fmt.Errorf("create encrypted object temp file: %w", err)
	}
	defer func() {
		_ = tmp.Close()
		_ = os.Remove(tmp.Name())
	}()
	if err := s.encryptTo(tmp, key, in); err != nil {
		return false, err
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if _, err := tmp.Seek(0, io.SeekStart); err != nil {
		return false, fmt.Errorf("seek encrypted object temp file: %w", err)
	}
	if s.headerCache != nil {
		finish := s.headerCache.beginMutation(key)
		defer finish()
	}
	if !conditional {
		return true, s.store.Put(key, tmp)
	}
	if requireContext {
		store, ok := s.store.(ContextConditionalStore)
		if !ok || !SupportsContextConditionalCreate(s.store) {
			return false, fmt.Errorf("underlying object store does not support contextual conditional access")
		}
		return store.PutIfAbsentContext(ctx, key, tmp)
	}
	store, ok := s.store.(ConditionalStore)
	if !ok {
		return false, fmt.Errorf("underlying object store does not support conditional create")
	}
	return store.PutIfAbsent(key, tmp)
}

func (s *encryptedStore) Get(key string, off, limit int64) (io.ReadCloser, error) {
	return s.getContext(context.Background(), key, off, limit, false)
}

func (s *encryptedStore) GetContext(ctx context.Context, key string, off, limit int64) (io.ReadCloser, error) {
	return s.getContext(ctx, key, off, limit, true)
}

func (s *encryptedStore) getContext(
	ctx context.Context,
	key string,
	off, limit int64,
	requireContext bool,
) (io.ReadCloser, error) {
	if ctx == nil {
		return nil, fmt.Errorf("object read context is required")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if limit == 0 {
		return io.NopCloser(bytes.NewReader(nil)), nil
	}
	if off < 0 {
		return nil, fmt.Errorf("negative object read offset: %d", off)
	}
	// A cache hit must not invent contextual access on an unsupported provider.
	if requireContext && !SupportsContextConditionalCreate(s.store) {
		return nil, fmt.Errorf("underlying object store does not support contextual conditional access")
	}
	metadata, prefix, err := s.encryptedObjectMetadataForRange(ctx, key, off, limit, requireContext)
	if err != nil {
		return nil, err
	}
	if metadata == nil {
		return s.getUnderlying(ctx, key, off, limit, requireContext)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	reader, startChunk, rangeEnd, err := s.openEncryptedObjectRange(ctx, key, off, limit, requireContext, metadata, prefix)
	refreshed := false
	if err != nil && s.headerCache != nil && ctx.Err() == nil {
		// A changed header can also make the old ciphertext offset out of range.
		next, refreshErr := s.refreshEncryptedObjectMetadata(ctx, key, requireContext, metadata, err)
		refreshed = true
		if refreshErr != nil {
			return nil, refreshErr
		}
		metadata = next
		reader, startChunk, rangeEnd, err = s.openEncryptedObjectRange(ctx, key, off, limit, requireContext, metadata, nil)
	}
	if err != nil {
		s.headerCache.forget(key, metadata)
		return nil, err
	}
	pr, pw := io.Pipe()
	// Cancellation must also release a decryptor blocked on the consumer's
	// pipe, after the independently shared header load has already completed.
	stopCancellation := context.AfterFunc(ctx, func() { _ = pw.CloseWithError(ctx.Err()) })
	go func() {
		defer stopCancellation()
		for {
			authenticated, readErr := decryptEncryptedObjectFrames(
				pw, key, withObjectReadContext(ctx, reader), metadata.header, metadata.aead,
				uint64(startChunk), startChunk*metadata.header.ChunkSize, off, rangeEnd,
			)
			_ = reader.Close()
			// No retry is allowed once any frame has authenticated, even if the
			// consumer has not read its plaintext yet. Identical headers preserve
			// the original corruption error; a second failure is always terminal.
			if !authenticated && !refreshed && s.headerCache != nil && ctx.Err() == nil && !errors.Is(readErr, io.ErrClosedPipe) {
				refreshed = true
				next, refreshErr := s.refreshEncryptedObjectMetadata(ctx, key, requireContext, metadata, readErr)
				if refreshErr != nil {
					readErr = refreshErr
				} else if next != nil {
					metadata = next
					reader, startChunk, rangeEnd, readErr = s.openEncryptedObjectRange(ctx, key, off, limit, requireContext, metadata, nil)
					if readErr == nil {
						continue
					}
				}
			}
			if readErr != nil && ctx.Err() == nil && !errors.Is(readErr, io.ErrClosedPipe) {
				s.headerCache.forget(key, metadata)
			}
			_ = pw.CloseWithError(readErr)
			return
		}
	}()
	return pr, nil
}

func (s *encryptedStore) openEncryptedObjectRange(ctx context.Context, key string, off, limit int64, requireContext bool, metadata *encryptedObjectMetadata, prefix *encryptedObjectPrefix) (io.ReadCloser, int64, int64, error) {
	header, headerEnd, aead := metadata.header, metadata.headerEnd, metadata.aead
	frameBytes := int64(4) + header.ChunkSize + int64(aead.Overhead())
	startChunk := off / header.ChunkSize
	if startChunk > (maxInt64-headerEnd)/frameBytes {
		return nil, 0, 0, fmt.Errorf("encrypted object range offset is too large: %d", off)
	}
	cipherOffset := headerEnd + startChunk*frameBytes
	cipherLimit := int64(-1)
	rangeEnd := int64(-1)
	if limit >= 0 {
		if off > maxInt64-limit {
			return nil, 0, 0, fmt.Errorf("encrypted object range overflows: offset %d limit %d", off, limit)
		}
		rangeEnd = off + limit
		endChunk := (rangeEnd-1)/header.ChunkSize + 1
		if endChunk < startChunk || endChunk-startChunk > maxInt64/frameBytes {
			return nil, 0, 0, fmt.Errorf("encrypted object range is too large: offset %d limit %d", off, limit)
		}
		cipherLimit = (endChunk - startChunk) * frameBytes
	}
	if prefix != nil && prefix.metadata == metadata && startChunk == prefix.startChunk && cipherLimit > 0 {
		payload := prefix.ciphertext
		if int64(len(payload)) >= cipherLimit || prefix.eof || prefix.containsRange(rangeEnd) {
			payload = payload[:min(int64(len(payload)), cipherLimit)]
			return io.NopCloser(bytes.NewReader(payload)), startChunk, rangeEnd, nil
		}
		if len(payload) > 0 {
			// Stored frame/header geometry can differ from the writer's current
			// defaults. Continue the exact ciphertext range without re-fetching
			// its prefix; a frame may straddle the two bodies. Authentication is
			// still performed by the ordinary frame decryptor below.
			reader, err := s.getUnderlying(ctx, key, cipherOffset+int64(len(payload)), cipherLimit-int64(len(payload)), requireContext)
			if err != nil {
				return nil, 0, 0, err
			}
			return &encryptedPrefixRangeReader{Reader: io.MultiReader(bytes.NewReader(payload), reader), Closer: reader}, startChunk, rangeEnd, nil
		}
	}
	reader, err := s.getUnderlying(ctx, key, cipherOffset, cipherLimit, requireContext)
	return reader, startChunk, rangeEnd, err
}

func (s *encryptedStore) encryptedObjectMetadata(ctx context.Context, key string, requireContext bool) (*encryptedObjectMetadata, error) {
	if s.headerCache == nil {
		return s.loadEncryptedObjectMetadata(ctx, key, requireContext)
	}
	return s.headerCache.get(ctx, key, func(loadCtx context.Context) (*encryptedObjectMetadata, error) {
		return s.loadEncryptedObjectMetadata(loadCtx, key, requireContext || SupportsContextConditionalCreate(s.store))
	})
}

// Refresh is bounded by the caller to one attempt before any authenticated frame.
// A changed envelope is evidence of re-encryption, never evidence of plaintext
// integrity: the replacement must authenticate and pass the caller's checksum.
func (s *encryptedStore) refreshEncryptedObjectMetadata(ctx context.Context, key string, requireContext bool, previous *encryptedObjectMetadata, readErr error) (*encryptedObjectMetadata, error) {
	s.headerCache.forget(key, previous)
	next, err := s.encryptedObjectMetadata(ctx, key, requireContext)
	if err != nil {
		return nil, errors.Join(readErr, fmt.Errorf("refresh encrypted object header: %w", err))
	}
	if next.identity == previous.identity && next.headerEnd == previous.headerEnd {
		if readErr != nil {
			s.headerCache.forget(key, next)
		}
		return nil, readErr
	}
	return next, nil
}

func (s *encryptedStore) loadEncryptedObjectMetadata(ctx context.Context, key string, requireContext bool) (*encryptedObjectMetadata, error) {
	return s.loadEncryptedObjectMetadataWithProbe(ctx, key, requireContext, encryptedObjectHeaderProbeBytes, nil)
}

func (s *encryptedStore) loadEncryptedObjectMetadataWithProbe(ctx context.Context, key string, requireContext bool, probeBytes int64, capture *encryptedObjectPrefix) (*encryptedObjectMetadata, error) {
	header, headerEnd, encrypted, err := s.readEncryptedObjectHeaderWithProbe(ctx, key, requireContext, probeBytes, capture)
	if err != nil {
		return nil, err
	}
	if !encrypted {
		return nil, fmt.Errorf("object %q is missing the required encrypted-object header", key)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	aead, err := s.objectAEAD(header)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	// The expanded AEAD owns its key; retaining the wrapped key is unnecessary.
	encoded, err := json.Marshal(header)
	if err != nil {
		return nil, fmt.Errorf("encode encrypted object header identity: %w", err)
	}
	identity := sha256.Sum256(encoded)
	header.WrappedKey = nil
	return &encryptedObjectMetadata{header: header, headerEnd: headerEnd, aead: aead, identity: identity}, nil
}

func (s *encryptedStore) getUnderlying(
	ctx context.Context,
	key string,
	off, limit int64,
	requireContext bool,
) (io.ReadCloser, error) {
	if requireContext {
		store, ok := s.store.(ContextConditionalStore)
		if !ok || !SupportsContextConditionalCreate(s.store) {
			return nil, fmt.Errorf("underlying object store does not support contextual conditional access")
		}
		return store.GetContext(ctx, key, off, limit)
	}
	return s.store.Get(key, off, limit)
}

// readEncryptedObjectHeader combines the prefix and ordinary JSON header into
// one bounded demand read. Unusually large headers require one exact remainder
// read. The probe may include at most 1 KiB of incidental ciphertext, which is
// discarded, never decrypted or cached as data. A range near the end of a large
// pack still fetches its data frames directly, not every preceding frame.
func (s *encryptedStore) readEncryptedObjectHeader(
	ctx context.Context,
	key string,
	requireContext bool,
) (encryptedObjectHeader, int64, bool, error) {
	return s.readEncryptedObjectHeaderWithProbe(ctx, key, requireContext, encryptedObjectHeaderProbeBytes, nil)
}

func (s *encryptedStore) readEncryptedObjectHeaderWithProbe(
	ctx context.Context, key string, requireContext bool, probeBytes int64, capture *encryptedObjectPrefix,
) (encryptedObjectHeader, int64, bool, error) {
	if probeBytes < encryptedObjectHeaderProbeBytes || probeBytes > maxEncryptedObjectHeaderBytes {
		return encryptedObjectHeader{}, 0, false, fmt.Errorf("invalid encrypted object prefix read size %d", probeBytes)
	}
	prefixBytes := int64(len(encryptedObjectMagic) + 4)
	reader, err := s.getUnderlying(ctx, key, 0, probeBytes, requireContext)
	if err != nil {
		return encryptedObjectHeader{}, 0, false, err
	}
	probe, readErr := io.ReadAll(io.LimitReader(withObjectReadContext(ctx, reader), probeBytes+1))
	closeErr := reader.Close()
	if err := errors.Join(readErr, closeErr); err != nil {
		return encryptedObjectHeader{}, 0, false, err
	}
	if int64(len(probe)) > probeBytes {
		return encryptedObjectHeader{}, 0, false, fmt.Errorf("encrypted object header probe exceeded its range")
	}
	if len(probe) < len(encryptedObjectMagic) || string(probe[:len(encryptedObjectMagic)]) != encryptedObjectMagic {
		return encryptedObjectHeader{}, 0, false, nil
	}
	if int64(len(probe)) < prefixBytes {
		return encryptedObjectHeader{}, 0, true, fmt.Errorf("read encrypted object header length: %w", io.ErrUnexpectedEOF)
	}
	headerBytes := int64(binary.BigEndian.Uint32(probe[len(encryptedObjectMagic):prefixBytes]))
	if headerBytes <= 0 || headerBytes > maxEncryptedObjectHeaderBytes {
		return encryptedObjectHeader{}, 0, true, fmt.Errorf("invalid encrypted object header size %d", headerBytes)
	}
	headerEnd := prefixBytes + headerBytes
	payload := probe[prefixBytes:]
	if headerEnd > int64(len(probe)) {
		// A short response is EOF, not permission to turn a malformed object
		// into an unbounded sequence of small range requests.
		if int64(len(probe)) != probeBytes {
			return encryptedObjectHeader{}, 0, true, fmt.Errorf("read encrypted object header: %w", io.ErrUnexpectedEOF)
		}
		remaining := headerEnd - int64(len(probe))
		reader, err = s.getUnderlying(ctx, key, int64(len(probe)), remaining, requireContext)
		if err != nil {
			return encryptedObjectHeader{}, 0, true, err
		}
		remainder, readErr := io.ReadAll(io.LimitReader(withObjectReadContext(ctx, reader), remaining+1))
		closeErr = reader.Close()
		if err := errors.Join(readErr, closeErr); err != nil {
			return encryptedObjectHeader{}, 0, true, err
		}
		if int64(len(remainder)) != remaining {
			return encryptedObjectHeader{}, 0, true, fmt.Errorf("read encrypted object header remainder: %w", io.ErrUnexpectedEOF)
		}
		payload = append(payload, remainder...)
	} else {
		payload = payload[:headerBytes]
	}
	var header encryptedObjectHeader
	if err := json.Unmarshal(payload, &header); err != nil {
		return encryptedObjectHeader{}, 0, true, fmt.Errorf("unmarshal encrypted object header: %w", err)
	}
	if capture != nil && headerEnd <= int64(len(probe)) {
		capture.ciphertext = probe[headerEnd:]
		capture.eof = int64(len(probe)) < probeBytes
	}
	return header, headerEnd, true, nil
}

func (s *encryptedStore) objectAEAD(header encryptedObjectHeader) (cipher.AEAD, error) {
	if header.Version != encryptedObjectVersion {
		return nil, fmt.Errorf("unsupported encrypted object version %d", header.Version)
	}
	if header.ChunkSize <= 0 || header.ChunkSize > maxUint32 {
		return nil, fmt.Errorf("invalid encrypted object chunk size %d", header.ChunkSize)
	}
	dataKey, err := s.cfg.KeyEncryptor.Decrypt(header.WrappedKey)
	if err != nil {
		return nil, fmt.Errorf("unwrap object data key: %w", err)
	}
	aead, err := newObjectAEAD(header.Algorithm, dataKey)
	if err != nil {
		return nil, err
	}
	if len(header.NoncePrefix) != aead.NonceSize()-8 {
		return nil, fmt.Errorf("invalid encrypted object nonce prefix size %d", len(header.NoncePrefix))
	}
	if header.ChunkSize > maxUint32-int64(aead.Overhead()) {
		return nil, fmt.Errorf("encrypted object chunk size is too large: %d", header.ChunkSize)
	}
	return aead, nil
}

func (s *encryptedStore) Delete(key string) error {
	if s.headerCache != nil {
		finish := s.headerCache.beginMutation(key)
		defer finish()
	}
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

func decryptEncryptedObjectFrames(
	out io.Writer,
	key string,
	in io.Reader,
	header encryptedObjectHeader,
	aead cipher.AEAD,
	startChunk uint64,
	plainOffset, rangeStart, rangeEnd int64,
) (authenticated bool, err error) {
	for chunkIndex := startChunk; ; chunkIndex++ {
		var lenBuf [4]byte
		if _, err := io.ReadFull(in, lenBuf[:]); err != nil {
			if err == io.EOF {
				return authenticated, nil
			}
			return authenticated, err
		}
		cipherLen := binary.BigEndian.Uint32(lenBuf[:])
		if cipherLen == 0 || int64(cipherLen) > header.ChunkSize+int64(aead.Overhead()) {
			return authenticated, fmt.Errorf("invalid encrypted object chunk size %d", cipherLen)
		}
		ciphertext := make([]byte, cipherLen)
		if _, err := io.ReadFull(in, ciphertext); err != nil {
			return authenticated, err
		}
		nonce := encryptedObjectNonce(aead.NonceSize(), header.NoncePrefix, chunkIndex)
		plaintext, err := aead.Open(nil, nonce, ciphertext, encryptedObjectChunkAAD(key, chunkIndex, header.Algorithm))
		if err != nil {
			return authenticated, fmt.Errorf("decrypt object chunk %d: %w", chunkIndex, err)
		}
		authenticated = true
		if err := writeRangeChunk(out, plaintext, plainOffset, rangeStart, rangeEnd); err != nil {
			return authenticated, err
		}
		plainOffset += int64(len(plaintext))
		if rangeEnd >= 0 && plainOffset >= rangeEnd {
			return authenticated, nil
		}
		if int64(len(plaintext)) < header.ChunkSize {
			return authenticated, nil
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
