package objectstore

import (
	"context"
	"encoding/binary"
	"io"
)

// encryptedObjectPrefix is transient ciphertext owned by one range request.
// It is never part of encryptedObjectMetadata or the shared header cache.
type encryptedObjectPrefix struct {
	metadata   *encryptedObjectMetadata
	ciphertext []byte
	eof        bool
	startChunk int64
}

type encryptedPrefixRangeReader struct {
	io.Reader
	io.Closer
}

// containsRange recognizes a complete short final frame even when the object
// ends exactly on the probe boundary (so transport length cannot prove EOF).
// This examines framing only. The ordinary decryptor must still authenticate
// every returned frame; malformed framing is sent there to fail closed too.
func (p *encryptedObjectPrefix) containsRange(rangeEnd int64) bool {
	payload := p.ciphertext
	chunk, overhead := p.metadata.header.ChunkSize, int64(p.metadata.aead.Overhead())
	for covered := p.startChunk * chunk; covered < rangeEnd; {
		if len(payload) < 4 {
			return false
		}
		length := int64(binary.BigEndian.Uint32(payload[:4]))
		payload = payload[4:]
		if length < overhead || length > chunk+overhead {
			return true
		}
		if length > int64(len(payload)) {
			return false
		}
		covered += length - overhead
		payload = payload[length:]
		if length-overhead < chunk {
			return true
		}
	}
	return true
}

// prefixProbeBytes is only a bounded transport hint based on writer defaults.
// Decryption and any continuation always use the stored, validated geometry.
// Nonzero offsets never download preceding pack data. Large/full reads keep
// their ordinary streaming path, and a disabled cache disables co-reading too.
func (s *encryptedStore) prefixProbeBytes(off, limit int64) int64 {
	if s.headerCache == nil || off != 0 || limit <= 0 {
		return encryptedObjectHeaderProbeBytes
	}
	maximum := min(s.headerCache.cfg.MaxPrefixBytes, int64(maxEncryptedObjectHeaderBytes))
	chunk := s.cfg.chunkSize()
	if maximum <= encryptedObjectHeaderProbeBytes || limit > maximum || chunk <= 0 || chunk > maximum {
		return encryptedObjectHeaderProbeBytes
	}
	// Both supported AEADs have 16-byte tags; every frame has a 4-byte length.
	// Operands are bounded above by 1 MiB, including tiny legacy chunk sizes.
	frames := (limit-1)/chunk + 1
	return min(maximum, encryptedObjectHeaderProbeBytes+frames*(chunk+20))
}

func (s *encryptedStore) encryptedObjectMetadataForRange(ctx context.Context, key string, off, limit int64, requireContext bool) (*encryptedObjectMetadata, *encryptedObjectPrefix, error) {
	if probe := s.startParallelEncryptedRead(ctx, key, off, limit); probe != nil {
		defer probe.cancel()
		metadata, err := s.encryptedObjectMetadata(ctx, key, requireContext)
		if err != nil {
			return nil, nil, err
		}
		return metadata, probe.take(ctx, metadata, off, limit), nil
	}
	probeBytes := s.prefixProbeBytes(off, limit)
	if probeBytes == encryptedObjectHeaderProbeBytes {
		metadata, err := s.encryptedObjectMetadata(ctx, key, requireContext)
		return metadata, nil, err
	}
	var prefix encryptedObjectPrefix
	metadata, err := s.headerCache.get(ctx, key, func(loadCtx context.Context) (*encryptedObjectMetadata, error) {
		var loaded encryptedObjectPrefix
		metadata, err := s.loadEncryptedObjectMetadataWithProbe(loadCtx, key,
			requireContext || SupportsContextConditionalCreate(s.store), probeBytes, &loaded)
		loaded.metadata = metadata
		prefix = loaded
		return metadata, err
	})
	if err != nil {
		// The caller may have stopped waiting while another waiter keeps the
		// shared loader alive. Do not inspect its captured state on this path.
		return nil, nil, err
	}
	// A cache hit or another caller's flight has no private prefix. Mutation
	// invalidation may also have replaced the load; never combine its old
	// ciphertext with a different envelope's metadata.
	if prefix.metadata != metadata || metadata == nil || (len(prefix.ciphertext) == 0 && !prefix.eof) {
		return metadata, nil, nil
	}
	return metadata, &prefix, nil
}
