package objectstore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestHeadContentMatchesGetAndPreservesPhysicalMetadata(t *testing.T) {
	for _, algorithm := range []string{EncryptionAlgoAES256GCMRSA, EncryptionAlgoCHACHA20RSA} {
		for _, chunk := range []int64{8, 64 << 10, 1 << 20} {
			for _, size := range []int64{0, 1, chunk - 1, chunk, chunk + 1, 2 * chunk, 2*chunk + 11} {
				t.Run(fmt.Sprintf("%s/chunk%d/size%d", algorithm, chunk, size), func(t *testing.T) {
					base := NewMemoryStore(t.Name())
					recording := &recordingRangeStore{ContextConditionalStore: base.(ContextConditionalStore)}
					store := Encrypting(recording, EncryptionConfig{Enabled: true, Algorithm: algorithm, KeyEncryptor: reversibleTestEncryptor{}, ChunkSize: chunk})
					payload := bytes.Repeat([]byte{0xa5}, int(size))
					require.NoError(t, store.Put("pack", bytes.NewReader(payload)))
					physical, err := base.Head("pack")
					require.NoError(t, err)
					info, err := HeadContent(store, "pack")
					require.NoError(t, err)
					require.Equal(t, size, info.Size)
					require.Equal(t, physical.Key, info.Key)
					require.Equal(t, physical.Modified, info.Modified)
					require.Equal(t, []rangeReadCall{{off: 0, limit: encryptedObjectHeaderProbeBytes}}, recording.gets)
					head, err := store.Head("pack")
					require.NoError(t, err)
					require.Equal(t, physical, head)
					objects, _, _, err := store.List("", "", "", "", 10)
					require.NoError(t, err)
					require.Len(t, objects, 1)
					require.Equal(t, physical.Size, objects[0].Size)
					require.Greater(t, physical.Size, info.Size)
					reader, err := store.Get("pack", 0, -1)
					require.NoError(t, err)
					got, err := io.ReadAll(reader)
					require.NoError(t, errors.Join(err, reader.Close()))
					require.Equal(t, payload, got)
				})
			}
		}
	}
}

func TestHeadContentFollowsPrefixAndNestedEncryption(t *testing.T) {
	cfg := EncryptionConfig{Enabled: true, KeyEncryptor: reversibleTestEncryptor{}, ChunkSize: 17}
	for _, order := range []string{"plain", "prefix-outer", "encryption-outer", "nested-encryption"} {
		t.Run(order, func(t *testing.T) {
			base := NewMemoryStore(t.Name())
			var store Store
			switch order {
			case "plain":
				store = Prefix(base, "tenant/rootfs")
			case "prefix-outer":
				store = Prefix(Encrypting(base, cfg), "tenant/rootfs")
			case "encryption-outer":
				store = Encrypting(Prefix(base, "tenant/rootfs"), cfg)
			case "nested-encryption":
				store = Encrypting(Prefix(Encrypting(base, cfg), "tenant/rootfs"), EncryptionConfig{
					Enabled: true, Algorithm: EncryptionAlgoCHACHA20RSA, KeyEncryptor: reversibleTestEncryptor{}, ChunkSize: 31,
				})
			}
			payload := bytes.Repeat([]byte("payload"), 19)
			require.NoError(t, store.Put("map", bytes.NewReader(payload)))
			info, err := HeadContent(store, "map")
			require.NoError(t, err)
			require.Equal(t, int64(len(payload)), info.Size)
			require.Equal(t, "map", info.Key)
			physical, err := base.Head("tenant/rootfs/map")
			require.NoError(t, err)
			head, err := store.Head("map")
			require.NoError(t, err)
			require.Equal(t, physical.Size, head.Size)
		})
	}
}

func TestHeadContentRequiresFreshEnvelopeAfterReencryption(t *testing.T) {
	base := NewMemoryStore(t.Name())
	recording := &recordingRangeStore{ContextConditionalStore: base.(ContextConditionalStore)}
	cfg := EncryptionConfig{Enabled: true, KeyEncryptor: reversibleTestEncryptor{}, ChunkSize: 8}
	store := EncryptingImmutable(recording, cfg, EncryptedHeaderCacheConfig{MaxEntries: 4, MaxBytes: 1 << 20})
	payload := bytes.Repeat([]byte("same immutable bytes"), 21)
	require.NoError(t, store.Put("pack", bytes.NewReader(payload)))
	reader, err := store.Get("pack", 0, -1)
	require.NoError(t, err)
	_, err = io.Copy(io.Discard, reader)
	require.NoError(t, errors.Join(err, reader.Close()))
	require.True(t, store.(*encryptedStore).headerCache.contains("pack"))
	cfg.ChunkSize = 31
	require.NoError(t, Encrypting(base, cfg).Put("pack", bytes.NewReader(payload)))
	recording.gets = nil
	info, err := HeadContent(store, "pack")
	require.NoError(t, err)
	require.Equal(t, int64(len(payload)), info.Size)
	require.Equal(t, []rangeReadCall{{off: 0, limit: encryptedObjectHeaderProbeBytes}}, recording.gets)
}

func TestHeadContentIsMetadataNotPayloadAuthentication(t *testing.T) {
	base := NewMemoryStore(t.Name())
	store := Encrypting(base, EncryptionConfig{Enabled: true, KeyEncryptor: reversibleTestEncryptor{}, ChunkSize: 8})
	payload := bytes.Repeat([]byte("payload"), 4)
	require.NoError(t, store.Put("pack", bytes.NewReader(payload)))
	raw, err := base.Get("pack", 0, -1)
	require.NoError(t, err)
	encoded, err := io.ReadAll(raw)
	require.NoError(t, errors.Join(err, raw.Close()))
	encoded[len(encoded)-1] ^= 1
	require.NoError(t, base.Put("pack", bytes.NewReader(encoded)))
	info, err := HeadContent(store, "pack")
	require.NoError(t, err)
	require.Equal(t, int64(len(payload)), info.Size)
	reader, err := store.Get("pack", 0, -1)
	require.NoError(t, err)
	_, err = io.ReadAll(reader)
	_ = reader.Close()
	require.ErrorContains(t, err, "decrypt object chunk")
}

func TestHeadContentRejectsMalformedEnvelopeAndHonorsLegacyMode(t *testing.T) {
	base := NewMemoryStore(t.Name())
	cfg := EncryptionConfig{Enabled: true, KeyEncryptor: reversibleTestEncryptor{}, ChunkSize: 8}
	store := Encrypting(base, cfg).(*encryptedStore)
	_, err := HeadContent(nil, "pack")
	require.Error(t, err)
	_, err = HeadContent(store, "missing")
	require.Error(t, err)
	require.NoError(t, base.Put("plain", bytes.NewReader([]byte("legacy payload"))))
	_, err = HeadContent(store, "plain")
	require.ErrorContains(t, err, "required encrypted-object header")
	info, err := HeadContent(EncryptingLegacyReadCompatible(base, cfg), "plain")
	require.NoError(t, err)
	require.Equal(t, int64(len("legacy payload")), info.Size)
	require.NoError(t, store.Put("valid", bytes.NewReader([]byte("payload"))))
	header, _, _, err := store.readEncryptedObjectHeader(context.Background(), "valid", false)
	require.NoError(t, err)
	for _, mutate := range []func(*encryptedObjectHeader){
		func(h *encryptedObjectHeader) { h.Version++ },
		func(h *encryptedObjectHeader) { h.Algorithm = "unsupported" },
		func(h *encryptedObjectHeader) { h.ChunkSize = 0 },
		func(h *encryptedObjectHeader) { h.ChunkSize = maxUint32 },
		func(h *encryptedObjectHeader) { h.NoncePrefix = nil },
		func(h *encryptedObjectHeader) { h.WrappedKey = []byte("invalid AES key") },
	} {
		bad := header
		mutate(&bad)
		var encoded bytes.Buffer
		require.NoError(t, writeEncryptedObjectHeader(&encoded, bad))
		require.NoError(t, base.Put("invalid", &encoded))
		_, err := HeadContent(store, "invalid")
		require.Error(t, err)
	}
}

func TestEncryptedContentSizeBoundaries(t *testing.T) {
	const header, tag = int64(197), int64(16)
	for _, chunk := range []int64{1, 8, 31, 64 << 10, 1 << 20, maxUint32 - tag} {
		for _, size := range []int64{0, 1, chunk - 1, chunk, chunk + 1, 2 * chunk, maxInt64 / 4} {
			frames := size / chunk
			if size%chunk != 0 {
				frames++
			}
			if frames > (maxInt64-header-size)/(4+tag) {
				continue
			}
			stored := header + size + frames*(4+tag)
			got, err := encryptedContentSize(stored, header, chunk, tag)
			require.NoError(t, err)
			require.Equal(t, size, got)
		}
	}
	for _, values := range [][4]int64{{-1, header, 8, tag}, {header - 1, header, 8, tag}, {header, 0, 8, tag}, {header, header, 0, tag}, {header, header, maxUint32, tag}, {header, header, 8, 0}} {
		_, err := encryptedContentSize(values[0], values[1], values[2], values[3])
		require.Error(t, err)
	}
	for tail := int64(1); tail <= 4+tag; tail++ {
		_, err := encryptedContentSize(header+tail, header, 64<<10, tag)
		require.Error(t, err)
	}
}

type contentHeadFaultStore struct {
	ContextConditionalStore
	size int64
	err  error
}

func (s contentHeadFaultStore) Head(key string) (Info, error) {
	return Info{Key: key, Size: s.size}, s.err
}

func TestHeadContentLargeObjectDoesNotReadBody(t *testing.T) {
	base := NewMemoryStore(t.Name())
	cfg := EncryptionConfig{Enabled: true, KeyEncryptor: reversibleTestEncryptor{}, ChunkSize: 64 << 10}
	writer := Encrypting(base, cfg).(*encryptedStore)
	require.NoError(t, writer.Put("pack", bytes.NewReader(nil)))
	_, headerEnd, _, err := writer.readEncryptedObjectHeader(context.Background(), "pack", false)
	require.NoError(t, err)
	const size = int64(1 << 40)
	physical := headerEnd + size + (size/cfg.ChunkSize)*20
	// Only a header exists in this fake provider. This deliberately verifies
	// bounded metadata work, not the integrity or existence of a TiB payload.
	fake := contentHeadFaultStore{ContextConditionalStore: base.(ContextConditionalStore), size: physical}
	recording := &recordingRangeStore{ContextConditionalStore: fake}
	info, err := HeadContent(Encrypting(recording, cfg), "pack")
	require.NoError(t, err)
	require.Equal(t, size, info.Size)
	require.Equal(t, []rangeReadCall{{off: 0, limit: encryptedObjectHeaderProbeBytes}}, recording.gets)
	_, err = HeadContent(Encrypting(contentHeadFaultStore{ContextConditionalStore: base.(ContextConditionalStore), err: io.ErrUnexpectedEOF}, cfg), "pack")
	require.ErrorIs(t, err, io.ErrUnexpectedEOF)
}

type contentHeaderReadFaultStore struct {
	Store
	read func(int64, int64) (io.ReadCloser, error)
}

func (s contentHeaderReadFaultStore) Get(_ string, off, limit int64) (io.ReadCloser, error) {
	return s.read(off, limit)
}

func TestHeadContentRejectsFailedOrOversizedHeaderReads(t *testing.T) {
	for _, kind := range []string{"read-error", "close-error", "oversized-probe"} {
		t.Run(kind, func(t *testing.T) {
			base := NewMemoryStore(t.Name())
			cfg := EncryptionConfig{Enabled: true, KeyEncryptor: reversibleTestEncryptor{}, ChunkSize: 8}
			require.NoError(t, Encrypting(base, cfg).Put("pack", bytes.NewReader([]byte("payload"))))
			closes, calls := 0, 0
			broken := contentHeaderReadFaultStore{Store: base, read: func(off, limit int64) (io.ReadCloser, error) {
				calls++
				require.Equal(t, int64(0), off)
				require.Equal(t, int64(encryptedObjectHeaderProbeBytes), limit)
				reader := headerProbeErrorReader{Reader: bytes.NewReader(make([]byte, encryptedObjectHeaderProbeBytes+1)), closed: &closes}
				if kind == "read-error" {
					reader.readErr = io.ErrUnexpectedEOF
				}
				if kind == "close-error" {
					reader.closeErr = io.ErrClosedPipe
				}
				return reader, nil
			}}
			_, err := HeadContent(Encrypting(broken, cfg), "pack")
			require.Error(t, err)
			require.Equal(t, 1, calls)
			require.Equal(t, 1, closes)
		})
	}
}

func TestHeadContentBoundsLargeHeaderRemainder(t *testing.T) {
	base := NewMemoryStore(t.Name())
	recording := &recordingRangeStore{ContextConditionalStore: base.(ContextConditionalStore)}
	store := Encrypting(recording, EncryptionConfig{Enabled: true, KeyEncryptor: reversibleTestEncryptor{}, ChunkSize: 8})
	payload := []byte("payload")
	require.NoError(t, store.Put("pack", bytes.NewReader(payload)))
	reader, err := base.Get("pack", 0, -1)
	require.NoError(t, err)
	raw, err := io.ReadAll(reader)
	require.NoError(t, errors.Join(err, reader.Close()))
	const end = maxEncryptedObjectHeaderBytes + len(encryptedObjectMagic) + 4
	raw = paddedHeaderProbeObject(t, raw, end)
	require.NoError(t, base.Put("pack", bytes.NewReader(raw)))
	info, err := HeadContent(store, "pack")
	require.NoError(t, err)
	require.Equal(t, int64(len(payload)), info.Size)
	require.Equal(t, []rangeReadCall{
		{off: 0, limit: encryptedObjectHeaderProbeBytes},
		{off: encryptedObjectHeaderProbeBytes, limit: int64(end - encryptedObjectHeaderProbeBytes)},
	}, recording.gets)
}

func FuzzEncryptedContentSize(f *testing.F) {
	for _, size := range []uint64{0, 1, 8, 9, 65535, 65536, 1 << 40, uint64(maxInt64 / 2)} {
		f.Add(size, uint32(8))
		f.Add(size, uint32(65536))
	}
	f.Fuzz(func(t *testing.T, input uint64, inputChunk uint32) {
		const header, tag = int64(197), int64(16)
		if input > uint64(maxInt64-header) {
			return
		}
		size, chunk := int64(input), int64(inputChunk)%(maxUint32-tag)+1
		frames := size / chunk
		if size%chunk != 0 {
			frames++
		}
		if frames > (maxInt64-header-size)/(4+tag) {
			return
		}
		got, err := encryptedContentSize(header+size+frames*(4+tag), header, chunk, tag)
		require.NoError(t, err)
		require.Equal(t, size, got)
	})
}
