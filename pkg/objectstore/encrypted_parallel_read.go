package objectstore

import (
	"context"
	"errors"
	"io"
	"sync"
)

const maxParallelEncryptedReads = 8

type parallelEncryptedReadResult struct {
	payload []byte
	err     error
}

type parallelEncryptedRead struct {
	offset, length, chunk int64
	cancel                context.CancelFunc
	done                  chan parallelEncryptedReadResult
}

// parallelEncryptedReadHint covers the demanded frames for the current writer
// geometry, allowing any header ending within the ordinary 1 KiB probe. This
// is not trusted object metadata: old geometry or larger headers must fall back
// to the validated exact range. At most 1 KiB of incidental bytes precedes the
// demanded frame; the intervening pack prefix is never downloaded.
func (s *encryptedStore) parallelEncryptedReadHint(off, limit int64) (offset, length int64, ok bool) {
	if s.headerCache == nil || limit <= 0 {
		return 0, 0, false
	}
	maximum := min(s.headerCache.cfg.MaxParallelReadBytes, int64(maxEncryptedObjectHeaderBytes))
	chunk := s.cfg.chunkSize()
	if maximum <= encryptedObjectHeaderProbeBytes || chunk <= 0 || chunk > maximum || off < chunk || limit > maximum || off > maxInt64-limit {
		return 0, 0, false
	}
	// The supported AEADs have 16-byte tags and a four-byte frame length.
	stride := chunk + 20
	first := off / chunk
	last := (off+limit-1)/chunk + 1
	prefix := int64(len(encryptedObjectMagic) + 4)
	if first > (maxInt64-prefix)/stride || last-first > maximum/stride {
		return 0, 0, false
	}
	offset = prefix + first*stride
	length = encryptedObjectHeaderProbeBytes - prefix + (last-first)*stride
	if length > maximum || offset > maxInt64-length {
		return 0, 0, false
	}
	return offset, length, true
}

// startParallelEncryptedRead does not join or delay the shared header flight.
// Every probe belongs to its own caller; a canceled leader cannot cancel other
// readers. Slots remain charged until the provider body actually exits, even
// if cancellation is ignored. Saturation simply uses the ordinary read path.
func (s *encryptedStore) startParallelEncryptedRead(ctx context.Context, key string, off, limit int64) *parallelEncryptedRead {
	offset, length, ok := s.parallelEncryptedReadHint(off, limit)
	if !ok || ctx.Err() != nil || !SupportsContextConditionalCreate(s.store) || s.headerCache.contains(key) {
		return nil
	}
	s.parallelReadsOnce.Do(func() { s.parallelReads = make(chan struct{}, maxParallelEncryptedReads) })
	select {
	case s.parallelReads <- struct{}{}:
	default:
		return nil
	}
	probeCtx, cancel := context.WithCancel(ctx)
	released := make(chan struct{})
	var releaseOnce sync.Once
	probe := &parallelEncryptedRead{offset: offset, length: length, chunk: s.cfg.chunkSize(), done: make(chan parallelEncryptedReadResult, 1)}
	probe.cancel = func() { cancel(); releaseOnce.Do(func() { close(released) }) }
	go func() {
		defer func() { <-s.parallelReads }()
		defer cancel()
		reader, err := s.getUnderlying(probeCtx, key, offset, length, true)
		result := parallelEncryptedReadResult{err: err}
		if err == nil {
			result.payload, result.err = io.ReadAll(io.LimitReader(withObjectReadContext(probeCtx, reader), length+1))
			result.err = errors.Join(result.err, reader.Close())
			if int64(len(result.payload)) > length {
				result.err = errors.New("parallel encrypted read exceeded its range")
			}
		}
		probe.done <- result
		// A completed ciphertext body still consumes admission while its
		// caller waits for the shared header. Otherwise one stalled header
		// could accumulate unbounded completed private buffers from waiters.
		select {
		case <-released:
		case <-ctx.Done():
		}
	}()
	return probe
}

func (p *parallelEncryptedRead) take(ctx context.Context, metadata *encryptedObjectMetadata, off, limit int64) *encryptedObjectPrefix {
	if metadata == nil || metadata.header.ChunkSize != p.chunk || metadata.aead.Overhead() != 16 || metadata.headerEnd > encryptedObjectHeaderProbeBytes {
		return nil
	}
	first := off / p.chunk
	stride := p.chunk + 20
	// The hint already checked these arithmetic bounds. Actual header length
	// only shifts the wanted interval forward within that bounded window.
	cipherOffset := metadata.headerEnd + first*stride
	length := ((off+limit-1)/p.chunk + 1 - first) * stride
	shift := cipherOffset - p.offset
	if shift < 0 || shift > p.length || length > p.length-shift {
		return nil
	}
	var result parallelEncryptedReadResult
	select {
	case <-ctx.Done():
		return nil
	case result = <-p.done:
	}
	if result.err != nil || shift > int64(len(result.payload)) {
		return nil
	}
	return &encryptedObjectPrefix{
		metadata: metadata, startChunk: first,
		ciphertext: result.payload[shift:min(int64(len(result.payload)), shift+length)],
		eof:        int64(len(result.payload)) < p.length,
	}
}
