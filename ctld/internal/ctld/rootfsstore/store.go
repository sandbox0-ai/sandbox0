// Package rootfsstore owns immutable rootfs object I/O and integrity checks.
package rootfsstore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"

	"github.com/opencontainers/go-digest"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshead"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/objectstore"
	"golang.org/x/sync/singleflight"
)

type Writer struct {
	store  objectstore.Store
	prefix string

	mu             sync.Mutex
	known          map[string]rootfshead.Object
	pendingProtect map[string]rootfshead.Object
	protected      map[string]rootfshead.Object
	createdBytes   int64
	createdObjects int64
	puts           singleflight.Group
}

func NewTeamWriter(store objectstore.Store, teamID string) (*Writer, error) {
	prefix, err := rootfshead.TeamObjectPrefix(teamID)
	if err != nil {
		return nil, err
	}
	return NewWriter(store, prefix)
}

func NewWriter(store objectstore.Store, prefix string) (*Writer, error) {
	if store == nil {
		return nil, fmt.Errorf("rootfs object store is required")
	}
	prefix = strings.Trim(strings.TrimSpace(prefix), "/")
	if prefix == "" {
		return nil, fmt.Errorf("rootfs object prefix is required")
	}
	return &Writer{
		store:          store,
		prefix:         prefix,
		known:          make(map[string]rootfshead.Object),
		pendingProtect: make(map[string]rootfshead.Object),
		protected:      make(map[string]rootfshead.Object),
	}, nil
}

func (w *Writer) Prefix() string {
	if w == nil {
		return ""
	}
	return w.prefix
}

func (w *Writer) Put(ctx context.Context, mediaType string, payload []byte) (rootfshead.Object, error) {
	if w == nil || w.store == nil {
		return rootfshead.Object{}, fmt.Errorf("rootfs object writer is not configured")
	}
	if err := ctx.Err(); err != nil {
		return rootfshead.Object{}, err
	}
	digestValue := digest.FromBytes(payload)
	key, err := rootfshead.ObjectKey(w.prefix, mediaType, digestValue.String())
	if err != nil {
		return rootfshead.Object{}, err
	}
	object := rootfshead.Object{Key: key, Digest: digestValue.String(), Size: int64(len(payload)), MediaType: mediaType}
	return w.PutObject(ctx, object, payload)
}

func (w *Writer) PutObject(ctx context.Context, object rootfshead.Object, payload []byte) (rootfshead.Object, error) {
	if w == nil || w.store == nil {
		return rootfshead.Object{}, fmt.Errorf("rootfs object writer is not configured")
	}
	if err := ctx.Err(); err != nil {
		return rootfshead.Object{}, err
	}
	if err := rootfshead.ValidateObjectScope(w.prefix, object); err != nil {
		return rootfshead.Object{}, err
	}
	if int64(len(payload)) != object.Size || digest.FromBytes(payload).String() != object.Digest {
		return rootfshead.Object{}, fmt.Errorf("rootfs object %s payload does not match descriptor", object.Key)
	}
	identity := object.MediaType + "\x00" + object.Digest
	result, err, _ := w.puts.Do(identity, func() (any, error) {
		w.mu.Lock()
		known, ok := w.known[identity]
		if ok {
			if protected, protectedOK := w.protected[known.Key]; !protectedOK || protected != known {
				w.pendingProtect[known.Key] = known
			}
		}
		w.mu.Unlock()
		if ok {
			return known, nil
		}

		created, err := PutImmutable(ctx, w.store, w.prefix, object, payload)
		if err != nil {
			return rootfshead.Object{}, err
		}
		w.mu.Lock()
		w.known[identity] = object
		w.pendingProtect[object.Key] = object
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

// RotateGeneration starts object accounting for the next immutable Head while
// preserving the process-local CAS knowledge used to avoid duplicate uploads.
// The caller must ensure no Put is in flight while rotating.
func (w *Writer) RotateGeneration() {
	if w == nil {
		return
	}
	w.mu.Lock()
	w.pendingProtect = make(map[string]rootfshead.Object)
	w.protected = make(map[string]rootfshead.Object)
	w.createdBytes = 0
	w.createdObjects = 0
	w.mu.Unlock()
}

// PendingProtection returns references not yet checkpointed by the active
// capture lease. It is intentionally separate from per-Head accounting.
func (w *Writer) PendingProtection() []rootfshead.Object {
	if w == nil {
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	objects := make([]rootfshead.Object, 0, len(w.pendingProtect))
	for _, object := range w.pendingProtect {
		objects = append(objects, object)
	}
	sort.Slice(objects, func(i, j int) bool { return objects[i].Key < objects[j].Key })
	return objects
}

// MarkProtected clears only descriptors confirmed by one durable lease
// checkpoint, preserving references added concurrently after the snapshot.
func (w *Writer) MarkProtected(objects []rootfshead.Object) {
	if w == nil {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, object := range objects {
		if pending, ok := w.pendingProtect[object.Key]; ok && pending == object {
			delete(w.pendingProtect, object.Key)
			w.protected[object.Key] = object
		}
	}
}

func (w *Writer) CreatedMetrics() (bytes int64, objects int64) {
	if w == nil {
		return 0, 0
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.createdBytes, w.createdObjects
}

func PutImmutable(ctx context.Context, store objectstore.Store, prefix string, object rootfshead.Object, payload []byte) (bool, error) {
	if store == nil {
		return false, fmt.Errorf("rootfs object store is required")
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if err := rootfshead.ValidateObjectScope(prefix, object); err != nil {
		return false, err
	}
	if int64(len(payload)) != object.Size || digest.FromBytes(payload).String() != object.Digest {
		return false, fmt.Errorf("rootfs object %s payload does not match descriptor", object.Key)
	}
	_, err := store.Head(object.Key)
	if err == nil {
		// Store wrappers such as envelope encryption report physical object size
		// from Head. The canonical key already binds media type and plaintext
		// digest; every actual Read verifies plaintext size and digest.
		return false, ctx.Err()
	}
	if !objectstore.IsNotFound(err) {
		return false, fmt.Errorf("inspect rootfs object %s: %w", object.Key, err)
	}
	if err := store.Put(object.Key, bytes.NewReader(payload)); err != nil {
		return false, fmt.Errorf("store rootfs object %s: %w", object.Key, err)
	}
	if err := ctx.Err(); err != nil {
		return true, err
	}
	return true, nil
}

func Read(ctx context.Context, store objectstore.Store, prefix string, object rootfshead.Object) ([]byte, error) {
	if store == nil {
		return nil, fmt.Errorf("rootfs object store is required")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := rootfshead.ValidateObjectScope(prefix, object); err != nil {
		return nil, err
	}
	reader, err := store.Get(object.Key, 0, object.Size)
	if err != nil {
		return nil, fmt.Errorf("read rootfs object %s: %w", object.Key, err)
	}
	payload, readErr := io.ReadAll(io.LimitReader(reader, object.Size+1))
	closeErr := reader.Close()
	if readErr != nil || closeErr != nil {
		return nil, fmt.Errorf("read rootfs object %s: %w", object.Key, errors.Join(readErr, closeErr))
	}
	if int64(len(payload)) != object.Size || digest.FromBytes(payload).String() != object.Digest {
		return nil, fmt.Errorf("rootfs object %s failed size or digest validation", object.Key)
	}
	return payload, ctx.Err()
}

func LoadHead(ctx context.Context, store objectstore.Store, reference rootfshead.HeadReference) (rootfshead.Head, error) {
	if err := reference.Validate(); err != nil {
		return rootfshead.Head{}, err
	}
	prefix, err := PrefixFromObject(reference.Manifest)
	if err != nil {
		return rootfshead.Head{}, err
	}
	payload, err := Read(ctx, store, prefix, reference.Manifest)
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
	return head, nil
}

func PrefixFromObject(object rootfshead.Object) (string, error) {
	if err := object.Validate(""); err != nil {
		return "", err
	}
	marker := "/" + objectKindPath(object.MediaType) + "/"
	position := strings.LastIndex(object.Key, marker)
	if position <= 0 {
		return "", fmt.Errorf("rootfs object key %q has no canonical prefix", object.Key)
	}
	prefix := object.Key[:position]
	if err := rootfshead.ValidateObjectScope(prefix, object); err != nil {
		return "", err
	}
	return prefix, nil
}

func objectKindPath(mediaType string) string {
	switch mediaType {
	case rootfshead.ChunkMediaType:
		return "chunks"
	case rootfshead.FileMediaType:
		return "files"
	case rootfshead.DirectoryShardMediaType:
		return "directory-shards"
	case rootfshead.DirectoryIndexMediaType:
		return "directory-indexes"
	case rootfshead.HeadMediaType:
		return "heads"
	case rootfshead.MarkerMediaType:
		return "markers"
	case rootfshead.ImageEnvelopeMediaType:
		return "image-envelopes"
	case rootfshead.ExportLayerMediaType:
		return "exports"
	default:
		return ""
	}
}
