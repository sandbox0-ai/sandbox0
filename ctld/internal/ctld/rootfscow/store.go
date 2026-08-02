package rootfscow

import (
	"bytes"
	"context"
	"fmt"
	"path"
	"sort"
	"strings"
	"sync"

	"github.com/opencontainers/go-digest"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshead"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/objectstore"
	"golang.org/x/sync/singleflight"
)

func LoadHead(ctx context.Context, store objectstore.Store, reference rootfshead.HeadReference) (rootfshead.Head, error) {
	if store == nil {
		return rootfshead.Head{}, fmt.Errorf("rootfs object store is required")
	}
	if err := reference.Validate(); err != nil {
		return rootfshead.Head{}, err
	}
	payload, err := readObject(ctx, store, reference.Manifest)
	if err != nil {
		return rootfshead.Head{}, err
	}
	head, err := rootfshead.DecodeHead(bytes.NewReader(payload))
	if err != nil {
		return rootfshead.Head{}, err
	}
	if head.HeadID != reference.HeadID {
		return rootfshead.Head{}, fmt.Errorf("rootfs head id %q does not match reference %q", head.HeadID, reference.HeadID)
	}
	return head, ctx.Err()
}

// ObjectWriter stores immutable plaintext objects and inventories every object
// produced or reused by this runtime generation. Upload metrics count only CAS
// misses, while the complete reference set protects shared objects during GC.
type ObjectWriter struct {
	store  objectstore.Store
	prefix string

	mu             sync.Mutex
	known          map[string]rootfshead.Object
	referenced     map[string]rootfshead.Object
	createdBytes   int64
	createdObjects int64
	putGroup       singleflight.Group
}

func NewObjectWriter(store objectstore.Store, prefix string) (*ObjectWriter, error) {
	if store == nil {
		return nil, fmt.Errorf("rootfs object store is required")
	}
	prefix = strings.Trim(strings.TrimSpace(prefix), "/")
	if prefix == "" {
		return nil, fmt.Errorf("rootfs object prefix is required")
	}
	return &ObjectWriter{
		store:      store,
		prefix:     prefix,
		known:      make(map[string]rootfshead.Object),
		referenced: make(map[string]rootfshead.Object),
	}, nil
}

func (w *ObjectWriter) Put(ctx context.Context, mediaType string, payload []byte) (rootfshead.Object, error) {
	if err := ctx.Err(); err != nil {
		return rootfshead.Object{}, err
	}
	digestValue := digest.FromBytes(payload)
	identity := mediaType + "\x00" + digestValue.String()
	result, err, _ := w.putGroup.Do(identity, func() (any, error) {
		w.mu.Lock()
		if object, ok := w.known[identity]; ok {
			w.mu.Unlock()
			return object, nil
		}
		w.mu.Unlock()

		object := rootfshead.Object{
			Key:       path.Join(w.prefix, objectKind(mediaType), digestValue.Algorithm().String(), digestValue.Encoded()),
			Digest:    digestValue.String(),
			Size:      int64(len(payload)),
			MediaType: mediaType,
		}
		if err := object.Validate(mediaType); err != nil {
			return rootfshead.Object{}, err
		}
		created := false
		if _, err := w.store.Head(object.Key); err != nil {
			if !objectstore.IsNotFound(err) {
				return rootfshead.Object{}, fmt.Errorf("inspect rootfs CAS object %s: %w", object.Key, err)
			}
			if err := w.store.Put(object.Key, bytes.NewReader(payload)); err != nil {
				return rootfshead.Object{}, fmt.Errorf("store rootfs CAS object %s: %w", object.Key, err)
			}
			created = true
		}
		w.mu.Lock()
		w.known[identity] = object
		w.referenced[object.Key] = object
		if created {
			w.createdBytes += object.Size
			w.createdObjects++
		}
		w.mu.Unlock()
		return object, nil
	})
	if err != nil {
		return rootfshead.Object{}, err
	}
	return result.(rootfshead.Object), nil
}

// Referenced returns every object produced or reused by this runtime session.
// Parent-only objects stay reachable through layer ancestry; recording reused
// objects prevents a restored branch from losing CAS data owned by an
// otherwise unrelated, garbage-collected branch of the same filesystem.
func (w *ObjectWriter) Referenced() []rootfshead.Object {
	if w == nil {
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	objects := make([]rootfshead.Object, 0, len(w.referenced))
	for _, object := range w.referenced {
		objects = append(objects, object)
	}
	sort.Slice(objects, func(i, j int) bool { return objects[i].Key < objects[j].Key })
	return objects
}

func (w *ObjectWriter) CreatedMetrics() (int64, int64) {
	if w == nil {
		return 0, 0
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.createdBytes, w.createdObjects
}

func objectKind(mediaType string) string {
	switch mediaType {
	case rootfshead.ChunkMediaType:
		return "chunks"
	case rootfshead.FileMediaType:
		return "files"
	case rootfshead.DirectoryShardMediaType:
		return "directory-shards"
	case rootfshead.DirectoryIndexMediaType:
		return "directories"
	case rootfshead.HeadMediaType:
		return "heads"
	default:
		return "metadata"
	}
}
