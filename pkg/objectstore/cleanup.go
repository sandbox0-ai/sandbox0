package objectstore

import (
	"context"
	"fmt"
	"strings"
)

// ContextCleanupStore lets bounded collectors cancel both listing and deletion.
// A wrapper cannot create this capability when its underlying provider lacks it.
type ContextCleanupStore interface {
	Store
	ListContext(context.Context, string, string, string, string, int64) ([]Info, bool, string, error)
	DeleteContext(context.Context, string) error
}

func SupportsContextCleanup(store Store) bool {
	if store == nil {
		return false
	}
	if capability, ok := store.(interface{ supportsContextCleanup() bool }); ok {
		return capability.supportsContextCleanup()
	}
	_, ok := store.(ContextCleanupStore)
	return ok
}

func cleanupContextError(ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf("object cleanup context is required")
	}
	return ctx.Err()
}

func cleanupStore(store Store) (ContextCleanupStore, error) {
	if !SupportsContextCleanup(store) {
		return nil, fmt.Errorf("underlying object store does not support contextual cleanup")
	}
	return store.(ContextCleanupStore), nil
}

func (s *prefixedStore) supportsContextCleanup() bool  { return SupportsContextCleanup(s.store) }
func (s *encryptedStore) supportsContextCleanup() bool { return SupportsContextCleanup(s.store) }

func (s *prefixedStore) DeleteContext(ctx context.Context, key string) error {
	base, err := cleanupStore(s.store)
	if err != nil {
		return err
	}
	return base.DeleteContext(ctx, s.prefixed(key))
}

func (s *prefixedStore) ListContext(ctx context.Context, prefix, after, token, delimiter string, limit int64) ([]Info, bool, string, error) {
	base, err := cleanupStore(s.store)
	if err != nil {
		return nil, false, "", err
	}
	listPrefix := s.prefixed(prefix)
	if strings.TrimLeft(strings.TrimSpace(prefix), "/") == "" {
		listPrefix = s.prefix
	}
	listAfter := ""
	if strings.TrimLeft(strings.TrimSpace(after), "/") != "" {
		listAfter = s.prefixed(after)
	}
	objects, more, next, err := base.ListContext(ctx, listPrefix, listAfter, token, delimiter, limit)
	if err != nil {
		return nil, false, "", err
	}
	for i := range objects {
		objects[i].Key = strings.TrimPrefix(objects[i].Key, s.prefix)
	}
	return objects, more, next, nil
}

func (s *encryptedStore) DeleteContext(ctx context.Context, key string) error {
	base, err := cleanupStore(s.store)
	if err != nil {
		return err
	}
	if s.headerCache != nil {
		finish := s.headerCache.beginMutation(key)
		defer finish()
	}
	return base.DeleteContext(ctx, key)
}

func (s *encryptedStore) ListContext(ctx context.Context, prefix, after, token, delimiter string, limit int64) ([]Info, bool, string, error) {
	base, err := cleanupStore(s.store)
	if err != nil {
		return nil, false, "", err
	}
	return base.ListContext(ctx, prefix, after, token, delimiter, limit)
}
