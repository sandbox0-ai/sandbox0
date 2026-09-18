package objectstore

import (
	"context"
	"fmt"
	"strings"
)

// ContentHeaderStore reports the size in the same coordinate space as Get.
// Transforming stores and wrappers around them must preserve this capability.
// Head and List continue to report the underlying physical object size.
type ContentHeaderStore interface {
	Store
	HeadContent(key string) (Info, error)
}

// HeadContent reports content metadata without streaming the object body.
// Plain stores use Head; transforming stores translate the stored size using
// their format metadata. Like Head, success is not a content-integrity proof:
// callers must still authenticate reads and verify their expected digest.
func HeadContent(store Store, key string) (Info, error) {
	if store == nil {
		return Info{}, fmt.Errorf("object store is required")
	}
	if content, ok := store.(ContentHeaderStore); ok {
		return content.HeadContent(key)
	}
	return store.Head(key)
}

var (
	_ ContentHeaderStore = (*encryptedStore)(nil)
	_ ContentHeaderStore = (*prefixedStore)(nil)
)

func (s *prefixedStore) HeadContent(key string) (Info, error) {
	info, err := HeadContent(s.store, s.prefixed(key))
	if err != nil {
		return Info{}, err
	}
	info.Key = strings.TrimPrefix(info.Key, s.prefix)
	return info, nil
}

func (s *encryptedStore) HeadContent(key string) (Info, error) {
	info, err := HeadContent(s.store, key)
	if err != nil {
		return Info{}, err
	}
	if info.Size < 0 || info.IsPrefix {
		return Info{}, fmt.Errorf("invalid encrypted object metadata for %q", key)
	}
	// Head is fresh metadata, so never combine it with a possibly stale cached
	// envelope after GC deleted and re-encrypted the same immutable plaintext.
	// The existing bounded header loader validates version, algorithm, key and
	// frame geometry. Its fixed-size probe may include incidental ciphertext;
	// it does not scan or decrypt the payload.
	metadata, err := s.loadEncryptedObjectMetadata(context.Background(), key, false)
	if err != nil {
		return Info{}, err
	}
	size, err := encryptedContentSize(info.Size, metadata.headerEnd, metadata.header.ChunkSize, int64(metadata.aead.Overhead()))
	if err != nil {
		return Info{}, fmt.Errorf("stat encrypted object %q: %w", key, err)
	}
	info.Size = size
	return info, nil
}

// V1 writes full frames followed by at most one nonempty short frame, with no
// trailer. Each frame has a four-byte length and one AEAD tag. Computing size
// from this geometry is bounded even for very large objects; it does not check
// frame contents, frame-length prefixes, or the object's plaintext checksum.
func encryptedContentSize(stored, headerEnd, chunk, tag int64) (int64, error) {
	if headerEnd <= 0 || stored < headerEnd || chunk <= 0 || tag <= 0 || tag > maxUint32 || chunk > maxUint32-tag {
		return 0, fmt.Errorf("invalid encrypted object size or frame geometry")
	}
	frameOverhead := int64(4) + tag
	frameSize := chunk + frameOverhead
	body := stored - headerEnd
	fullFrames, remainder := body/frameSize, body%frameSize
	// An empty object has only a header. Writers never emit an empty frame.
	if remainder != 0 && remainder <= frameOverhead {
		return 0, fmt.Errorf("truncated encrypted object frame")
	}
	// fullFrames*chunk cannot overflow: it is at most body (an int64 size).
	size := fullFrames * chunk
	if remainder != 0 {
		size += remainder - frameOverhead
	}
	return size, nil
}
