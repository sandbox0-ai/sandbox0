package http

import (
	"context"
	"crypto/ed25519"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/cluster-gateway/pkg/client"
	mgr "github.com/sandbox0-ai/sandbox0/manager/pkg/service"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"go.uber.org/zap"
)

func TestGetSandboxInternalCachedUsesCacheHit(t *testing.T) {
	cache := newTestSandboxInternalCache()
	cache.values["sb-1"] = &mgr.Sandbox{ID: "sb-1", TeamID: "team-1"}
	server := &Server{
		sandboxInternalCache: cache,
		logger:               zap.NewNop(),
	}

	sandbox, err := server.getSandboxInternalCached(context.Background(), "sb-1")
	if err != nil {
		t.Fatalf("get sandbox: %v", err)
	}
	if sandbox.ID != "sb-1" {
		t.Fatalf("sandbox ID = %q, want sb-1", sandbox.ID)
	}
	if cache.gets != 1 {
		t.Fatalf("cache gets = %d, want 1", cache.gets)
	}
}

func TestGetSandboxInternalCachedPopulatesCache(t *testing.T) {
	var managerGets atomic.Int32
	manager := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/internal/v1/sandboxes/sb-1" {
			t.Fatalf("unexpected manager request %s %s", r.Method, r.URL.Path)
		}
		managerGets.Add(1)
		_ = spec.WriteSuccess(w, http.StatusOK, &mgr.Sandbox{
			ID:           "sb-1",
			TeamID:       "team-1",
			InternalAddr: "http://127.0.0.1:7777",
		})
	}))
	defer manager.Close()

	cache := newTestSandboxInternalCache()
	server := &Server{
		managerClient:        newTestManagerClient(t, manager.URL),
		sandboxInternalCache: cache,
		logger:               zap.NewNop(),
	}

	for i := 0; i < 2; i++ {
		sandbox, err := server.getSandboxInternalCached(context.Background(), "sb-1")
		if err != nil {
			t.Fatalf("get sandbox: %v", err)
		}
		if sandbox.InternalAddr != "http://127.0.0.1:7777" {
			t.Fatalf("InternalAddr = %q", sandbox.InternalAddr)
		}
	}
	if got := managerGets.Load(); got != 1 {
		t.Fatalf("manager gets = %d, want 1", got)
	}
	if cache.sets != 1 {
		t.Fatalf("cache sets = %d, want 1", cache.sets)
	}
}

func TestInvalidateSandboxInternalCacheDeletesKey(t *testing.T) {
	cache := newTestSandboxInternalCache()
	cache.values["sb-1"] = &mgr.Sandbox{ID: "sb-1"}
	server := &Server{
		sandboxInternalCache: cache,
		logger:               zap.NewNop(),
	}

	server.invalidateSandboxInternalCache(context.Background(), "sb-1")

	if _, ok := cache.values["sb-1"]; ok {
		t.Fatal("sandbox cache entry was not deleted")
	}
	if cache.deletes != 1 {
		t.Fatalf("cache deletes = %d, want 1", cache.deletes)
	}
}

type testSandboxInternalCache struct {
	mu      sync.Mutex
	values  map[string]*mgr.Sandbox
	gets    int
	sets    int
	deletes int
}

func newTestSandboxInternalCache() *testSandboxInternalCache {
	return &testSandboxInternalCache{values: make(map[string]*mgr.Sandbox)}
}

func (c *testSandboxInternalCache) Get(_ context.Context, key string) (*mgr.Sandbox, bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.gets++
	value, ok := c.values[key]
	return value, ok, nil
}

func (c *testSandboxInternalCache) Set(_ context.Context, key string, value *mgr.Sandbox) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.sets++
	c.values[key] = value
	return nil
}

func (c *testSandboxInternalCache) Delete(_ context.Context, key string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.deletes++
	delete(c.values, key)
	return nil
}

func newTestManagerClient(t *testing.T, managerURL string) *client.ManagerClient {
	t.Helper()
	_, privateKey, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	gen := internalauth.NewGenerator(internalauth.GeneratorConfig{
		Caller:     "cluster-gateway",
		PrivateKey: privateKey,
		TTL:        time.Minute,
	})
	return client.NewManagerClient(managerURL, gen, zap.NewNop(), time.Second)
}
