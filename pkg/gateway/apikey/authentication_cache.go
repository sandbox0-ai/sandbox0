package apikey

import (
	"container/list"
	"crypto/sha256"
	"errors"
	"time"
)

const (
	DefaultAuthenticationCachePositiveTTL = time.Second
	DefaultAuthenticationCacheNegativeTTL = 250 * time.Millisecond
	DefaultAuthenticationCacheMaxEntries  = 10_000
)

type authenticationDigest [sha256.Size]byte

type AuthenticationCacheConfig struct {
	PositiveTTL time.Duration
	NegativeTTL time.Duration
	MaxEntries  int
}

func DefaultAuthenticationCacheConfig() AuthenticationCacheConfig {
	return AuthenticationCacheConfig{
		PositiveTTL: DefaultAuthenticationCachePositiveTTL,
		NegativeTTL: DefaultAuthenticationCacheNegativeTTL,
		MaxEntries:  DefaultAuthenticationCacheMaxEntries,
	}
}

func normalizeAuthenticationCacheConfig(config AuthenticationCacheConfig) AuthenticationCacheConfig {
	defaults := DefaultAuthenticationCacheConfig()
	if config.PositiveTTL <= 0 {
		config.PositiveTTL = defaults.PositiveTTL
	}
	if config.NegativeTTL <= 0 {
		config.NegativeTTL = defaults.NegativeTTL
	}
	if config.MaxEntries <= 0 {
		config.MaxEntries = defaults.MaxEntries
	}
	return config
}

type authenticationCacheEntry struct {
	digest    authenticationDigest
	key       *APIKey
	err       error
	keyID     string
	expiresAt time.Time
}

// authenticationCache is an exact-size-bounded LRU. Entries never contain the
// raw API key: the map key is SHA-256(raw), and cached APIKey values have
// KeyValue cleared.
type authenticationCache struct {
	maxEntries int
	entries    map[authenticationDigest]*list.Element
	lru        *list.List
	byID       map[string]map[authenticationDigest]struct{}
}

func newAuthenticationCache(maxEntries int) *authenticationCache {
	if maxEntries <= 0 {
		maxEntries = DefaultAuthenticationCacheMaxEntries
	}
	return &authenticationCache{
		maxEntries: maxEntries,
		entries:    make(map[authenticationDigest]*list.Element, maxEntries),
		lru:        list.New(),
		byID:       make(map[string]map[authenticationDigest]struct{}),
	}
}

func (c *authenticationCache) get(
	digest authenticationDigest,
	now time.Time,
) (*APIKey, error, bool) {
	if c == nil {
		return nil, nil, false
	}
	element, ok := c.entries[digest]
	if !ok {
		return nil, nil, false
	}
	entry := element.Value.(*authenticationCacheEntry)
	if !now.Before(entry.expiresAt) {
		c.removeElement(element)
		return nil, nil, false
	}
	c.lru.MoveToFront(element)
	if entry.err != nil {
		return nil, entry.err, true
	}
	return cloneAPIKey(entry.key), nil, true
}

func (c *authenticationCache) putPositive(
	digest authenticationDigest,
	key *APIKey,
	expiresAt time.Time,
) {
	if c == nil || key == nil {
		return
	}
	c.put(&authenticationCacheEntry{
		digest:    digest,
		key:       cloneAPIKeyWithoutSecret(key),
		keyID:     key.ID,
		expiresAt: expiresAt,
	})
}

func (c *authenticationCache) putNegative(
	digest authenticationDigest,
	err error,
	keyID string,
	expiresAt time.Time,
) {
	if c == nil || !isCacheableAuthenticationError(err) {
		return
	}
	c.put(&authenticationCacheEntry{
		digest:    digest,
		err:       canonicalAuthenticationError(err),
		keyID:     keyID,
		expiresAt: expiresAt,
	})
}

func (c *authenticationCache) put(entry *authenticationCacheEntry) {
	if c == nil || entry == nil {
		return
	}
	if existing, ok := c.entries[entry.digest]; ok {
		c.removeElement(existing)
	}
	element := c.lru.PushFront(entry)
	c.entries[entry.digest] = element
	c.addIDIndex(entry)
	for len(c.entries) > c.maxEntries {
		c.removeElement(c.lru.Back())
	}
}

func (c *authenticationCache) invalidateDigest(digest authenticationDigest) {
	if c == nil {
		return
	}
	if element, ok := c.entries[digest]; ok {
		c.removeElement(element)
	}
}

func (c *authenticationCache) invalidateID(keyID string) []authenticationDigest {
	if c == nil || keyID == "" {
		return nil
	}
	indexed := c.byID[keyID]
	if len(indexed) == 0 {
		return nil
	}
	digests := make([]authenticationDigest, 0, len(indexed))
	for digest := range indexed {
		digests = append(digests, digest)
		if element, ok := c.entries[digest]; ok {
			c.removeElement(element)
		}
	}
	return digests
}

func (c *authenticationCache) len() int {
	if c == nil {
		return 0
	}
	return len(c.entries)
}

func (c *authenticationCache) removeElement(element *list.Element) {
	if c == nil || element == nil {
		return
	}
	entry, _ := element.Value.(*authenticationCacheEntry)
	c.lru.Remove(element)
	if entry == nil {
		return
	}
	delete(c.entries, entry.digest)
	if entry.keyID == "" {
		return
	}
	indexed := c.byID[entry.keyID]
	delete(indexed, entry.digest)
	if len(indexed) == 0 {
		delete(c.byID, entry.keyID)
	}
}

func (c *authenticationCache) addIDIndex(entry *authenticationCacheEntry) {
	if c == nil || entry == nil || entry.keyID == "" {
		return
	}
	indexed := c.byID[entry.keyID]
	if indexed == nil {
		indexed = make(map[authenticationDigest]struct{})
		c.byID[entry.keyID] = indexed
	}
	indexed[entry.digest] = struct{}{}
}

func isCacheableAuthenticationError(err error) bool {
	return errors.Is(err, ErrInvalidKey) ||
		errors.Is(err, ErrInactiveKey) ||
		errors.Is(err, ErrExpiredKey)
}

func canonicalAuthenticationError(err error) error {
	switch {
	case errors.Is(err, ErrInactiveKey):
		return ErrInactiveKey
	case errors.Is(err, ErrExpiredKey):
		return ErrExpiredKey
	default:
		return ErrInvalidKey
	}
}

func cloneAPIKeyWithoutSecret(key *APIKey) *APIKey {
	cloned := cloneAPIKey(key)
	if cloned != nil {
		cloned.KeyValue = ""
	}
	return cloned
}

func cloneAPIKey(key *APIKey) *APIKey {
	if key == nil {
		return nil
	}
	cloned := *key
	cloned.Roles = append([]string(nil), key.Roles...)
	if key.UserID != nil {
		userID := *key.UserID
		cloned.UserID = &userID
	}
	if key.LastUsed != nil {
		lastUsed := *key.LastUsed
		cloned.LastUsed = &lastUsed
	}
	return &cloned
}
