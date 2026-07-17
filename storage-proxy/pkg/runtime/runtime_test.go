package runtime

import (
	"context"
	"crypto/ed25519"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"go.uber.org/zap"
)

func TestStorageAuthValidatorRejectsLegacyAudience(t *testing.T) {
	publicKey, privateKey, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	validator := newStorageAuthValidator(publicKey)
	generator := internalauth.NewGenerator(internalauth.DefaultGeneratorConfig(internalauth.ServiceClusterGateway, privateKey))

	canonicalToken, err := generator.Generate(internalauth.ServiceManagerStorage, "team-1", "user-1", internalauth.GenerateOptions{})
	if err != nil {
		t.Fatalf("generate canonical token: %v", err)
	}
	if _, err := validator.Validate(canonicalToken); err != nil {
		t.Fatalf("validate canonical manager-storage token: %v", err)
	}

	legacyToken, err := generator.Generate("storage-proxy", "team-1", "user-1", internalauth.GenerateOptions{})
	if err != nil {
		t.Fatalf("generate legacy token: %v", err)
	}
	if _, err := validator.Validate(legacyToken); !errors.Is(err, internalauth.ErrInvalidTarget) {
		t.Fatalf("validate legacy storage-proxy audience error = %v, want ErrInvalidTarget", err)
	}
}

func TestInternalHTTPClientUsesRuntimeHandlerWithoutSocket(t *testing.T) {
	r := &Runtime{
		httpHandler: http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			if got := req.Header.Get("X-Test"); got != "value" {
				t.Errorf("X-Test = %q, want value", got)
			}
			w.Header().Set("X-Result", "ok")
			w.WriteHeader(http.StatusCreated)
			_, _ = w.Write([]byte("created"))
		}),
		started: true,
	}
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, "http://manager-storage/internal", strings.NewReader("body"))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("X-Test", "value")

	resp, err := r.InternalHTTPClient().Do(req)
	if err != nil {
		t.Fatalf("Do() error = %v", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusCreated)
	}
	if got := resp.Header.Get("X-Result"); got != "ok" {
		t.Fatalf("X-Result = %q, want ok", got)
	}
	if got := string(body); got != "created" {
		t.Fatalf("body = %q, want created", got)
	}
}

func TestShutdownDrainsInternalHTTPRequestsAndRejectsNewOnes(t *testing.T) {
	requestStarted := make(chan struct{})
	releaseRequest := make(chan struct{})
	r := newHTTPOnlyRuntimeForTest(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		close(requestStarted)
		<-releaseRequest
		w.WriteHeader(http.StatusNoContent)
	}))
	if err := r.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	requestDone := make(chan error, 1)
	go func() {
		resp, err := r.InternalHTTPClient().Get("http://manager-storage/internal")
		if resp != nil {
			_ = resp.Body.Close()
		}
		requestDone <- err
	}()
	<-requestStarted

	shutdownDone := make(chan error, 1)
	go func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		shutdownDone <- r.Shutdown(shutdownCtx)
	}()
	select {
	case err := <-shutdownDone:
		t.Fatalf("Shutdown() returned before internal request drained: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	close(releaseRequest)
	if err := <-requestDone; err != nil {
		t.Fatalf("internal request error = %v", err)
	}
	if err := <-shutdownDone; err != nil {
		t.Fatalf("Shutdown() error = %v", err)
	}
	if _, err := r.InternalHTTPClient().Get("http://manager-storage/after-shutdown"); err == nil {
		t.Fatal("internal request after shutdown succeeded")
	}
}

func TestRuntimeStartStopsOnParentContext(t *testing.T) {
	r := newHTTPOnlyRuntimeForTest(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	ctx, cancel := context.WithCancel(context.Background())
	if err := r.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	resp, err := http.Get("http://" + r.Address())
	if err != nil {
		t.Fatalf("GET runtime: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusNoContent)
	}

	cancel()
	select {
	case <-r.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("runtime did not stop after parent context cancellation")
	}
	if err := r.Start(context.Background()); err == nil || !strings.Contains(err.Error(), "stopped") {
		t.Fatalf("Start() after shutdown error = %v, want stopped error", err)
	}
	if err := r.Shutdown(context.Background()); err != nil {
		t.Fatalf("second Shutdown() error = %v", err)
	}
}

func TestRuntimeStartReturnsListenError(t *testing.T) {
	wantErr := errors.New("bind failed")
	r := newHTTPOnlyRuntimeForTest(http.NotFoundHandler())
	r.listen = func(string, string) (net.Listener, error) { return nil, wantErr }

	err := r.Start(context.Background())
	if !errors.Is(err, wantErr) {
		t.Fatalf("Start() error = %v, want %v", err, wantErr)
	}
	if err := r.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown() error = %v", err)
	}
}

func TestBuildDirectVolumeFileCleanupInterval(t *testing.T) {
	cfg := &config.StorageProxyConfig{CleanupInterval: "2m"}
	if got := buildDirectVolumeFileCleanupInterval(cfg, 30*time.Second); got != 30*time.Second {
		t.Fatalf("cleanup interval = %s, want 30s", got)
	}
	cfg.CleanupInterval = "100ms"
	if got := buildDirectVolumeFileCleanupInterval(cfg, 30*time.Second); got != time.Second {
		t.Fatalf("cleanup interval = %s, want 1s", got)
	}
}

func newHTTPOnlyRuntimeForTest(handler http.Handler) *Runtime {
	return &Runtime{
		cfg:         &config.StorageProxyConfig{},
		logger:      zap.NewNop(),
		listen:      net.Listen,
		httpHandler: handler,
		httpServer: &http.Server{
			Addr:    "127.0.0.1:0",
			Handler: handler,
		},
		errors: make(chan error, 1),
		done:   make(chan struct{}),
	}
}
