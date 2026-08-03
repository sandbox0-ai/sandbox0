package service

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"sync"
	"testing"

	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"github.com/sandbox0-ai/sandbox0/pkg/naming"
)

type webhookRoundTripFunc func(*http.Request) (*http.Response, error)

func (f webhookRoundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func TestManagerStorageVolumeClientDefaultsClusterID(t *testing.T) {
	var gotClusterID string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/internal/v1/sandboxvolumes/owned" {
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		var req struct {
			ClusterID string `json:"cluster_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		gotClusterID = req.ClusterID
		_ = spec.WriteSuccess(w, http.StatusCreated, map[string]any{
			"volume": map[string]string{"id": "vol-1"},
		})
	}))
	defer server.Close()

	client := NewManagerStorageVolumeClient(ManagerStorageVolumeClientConfig{
		BaseURL:        server.URL,
		TokenGenerator: staticTokenGenerator{},
	})
	volumeID, err := client.Create(t.Context(), "team-1", "user-1", "sandbox-1", webhookStateVolumeKind)
	if err != nil {
		t.Fatalf("Create returned error: %v", err)
	}
	if volumeID != "vol-1" {
		t.Fatalf("volumeID = %q, want vol-1", volumeID)
	}
	if gotClusterID != naming.DefaultClusterID {
		t.Fatalf("clusterID = %q, want %q", gotClusterID, naming.DefaultClusterID)
	}
}

func TestPrepareWebhookStateVolumeReusesDurableVolume(t *testing.T) {
	volumeClient := &recordingSystemVolumeClient{}
	svc := &SandboxService{webhookStateVolumes: volumeClient}

	volume, err := svc.prepareWebhookStateVolume(context.Background(), &ClaimRequest{
		TeamID:               "team-1",
		UserID:               "user-1",
		WebhookStateVolumeID: "volume-existing",
		Config: &SandboxConfig{Webhook: &WebhookConfig{
			URL: "https://example.test/webhook",
		}},
	}, "sandbox-1")
	if err != nil {
		t.Fatalf("prepareWebhookStateVolume() error = %v", err)
	}
	if volume == nil || volume.VolumeID != "volume-existing" {
		t.Fatalf("volume = %#v, want volume-existing", volume)
	}
	if volume.Created {
		t.Fatal("reused webhook state volume was marked created")
	}
	if len(volumeClient.created) != 0 {
		t.Fatalf("created volumes = %#v, want none", volumeClient.created)
	}
}

func TestHTTPSandboxDeletionWebhookEmitterRetriesTransientFailure(t *testing.T) {
	var mu sync.Mutex
	var bodies [][]byte
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read request body: %v", err)
			http.Error(w, "read request", http.StatusInternalServerError)
			return
		}
		mu.Lock()
		bodies = append(bodies, body)
		attempt := len(bodies)
		mu.Unlock()
		if attempt < deletionWebhookMaxAttempts {
			http.Error(w, "try again", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	emitter := NewHTTPSandboxDeletionWebhookEmitter(server.Client())
	err := emitter.EmitSandboxDeleted(t.Context(), SandboxLifecycleInfo{
		SandboxID:     "sandbox-1",
		TeamID:        "team-1",
		WebhookURL:    server.URL,
		WebhookSecret: "secret",
	})
	if err != nil {
		t.Fatalf("EmitSandboxDeleted() error = %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(bodies) != deletionWebhookMaxAttempts {
		t.Fatalf("request count = %d, want %d", len(bodies), deletionWebhookMaxAttempts)
	}
	for i := 1; i < len(bodies); i++ {
		if !slices.Equal(bodies[0], bodies[i]) {
			t.Fatalf("retry body %d differs from the first attempt", i+1)
		}
	}
}

func TestHTTPSandboxDeletionWebhookEmitterDoesNotRetryPermanentFailure(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls++
		http.Error(w, "invalid", http.StatusBadRequest)
	}))
	defer server.Close()

	emitter := NewHTTPSandboxDeletionWebhookEmitter(server.Client())
	err := emitter.EmitSandboxDeleted(t.Context(), SandboxLifecycleInfo{
		SandboxID:  "sandbox-1",
		TeamID:     "team-1",
		WebhookURL: server.URL,
	})
	if err == nil {
		t.Fatal("EmitSandboxDeleted() error = nil, want permanent failure")
	}
	if calls != 1 {
		t.Fatalf("request count = %d, want 1", calls)
	}
	if isRetryableSandboxDeletionWebhookError(err) {
		t.Fatalf("EmitSandboxDeleted() error = %v, want permanent failure", err)
	}
}

func TestHTTPSandboxDeletionWebhookEmitterClassifiesExhaustedTransientFailure(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls++
		http.Error(w, "try again", http.StatusServiceUnavailable)
	}))
	defer server.Close()

	emitter := NewHTTPSandboxDeletionWebhookEmitter(server.Client())
	err := emitter.EmitSandboxDeleted(t.Context(), SandboxLifecycleInfo{
		SandboxID:  "sandbox-1",
		TeamID:     "team-1",
		WebhookURL: server.URL,
	})
	if err == nil {
		t.Fatal("EmitSandboxDeleted() error = nil, want transient failure")
	}
	if !isRetryableSandboxDeletionWebhookError(err) {
		t.Fatalf("EmitSandboxDeleted() error = %v, want retryable failure", err)
	}
	if calls != deletionWebhookMaxAttempts {
		t.Fatalf("request count = %d, want %d", calls, deletionWebhookMaxAttempts)
	}
}

func TestHTTPSandboxDeletionWebhookEmitterClassifiesClientTimeoutAsRetryable(t *testing.T) {
	calls := 0
	emitter := NewHTTPSandboxDeletionWebhookEmitter(&http.Client{Transport: webhookRoundTripFunc(func(*http.Request) (*http.Response, error) {
		calls++
		return nil, context.DeadlineExceeded
	})})
	err := emitter.EmitSandboxDeleted(t.Context(), SandboxLifecycleInfo{
		SandboxID:  "sandbox-1",
		TeamID:     "team-1",
		WebhookURL: "https://example.test/webhook",
	})
	if err == nil {
		t.Fatal("EmitSandboxDeleted() error = nil, want timeout")
	}
	if !isRetryableSandboxDeletionWebhookError(err) {
		t.Fatalf("EmitSandboxDeleted() error = %v, want retryable failure", err)
	}
	if calls != deletionWebhookMaxAttempts {
		t.Fatalf("request count = %d, want %d", calls, deletionWebhookMaxAttempts)
	}
}

func TestManagerStorageVolumeClientPrepareForPortalBind(t *testing.T) {
	var gotMethod, gotPath, gotToken string
	var gotReq PrepareVolumePortalBindRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotPath = r.URL.Path
		gotToken = r.Header.Get("X-Internal-Token")
		if err := json.NewDecoder(r.Body).Decode(&gotReq); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		_ = spec.WriteSuccess(w, http.StatusOK, map[string]any{"prepared": true})
	}))
	defer server.Close()

	client := NewManagerStorageVolumeClient(ManagerStorageVolumeClientConfig{
		BaseURL:        server.URL,
		TokenGenerator: staticTokenGenerator{},
	})
	if err := client.PrepareForVolumePortalBind(t.Context(), PrepareVolumePortalBindRequest{
		TeamID:    "team-1",
		UserID:    "user-1",
		VolumeID:  "vol-1",
		PodUID:    "pod-uid",
		MountPath: "/workspace",
	}); err != nil {
		t.Fatalf("PrepareForVolumePortalBind() error = %v", err)
	}
	if gotMethod != http.MethodPut {
		t.Fatalf("method = %q, want %q", gotMethod, http.MethodPut)
	}
	if gotPath != "/internal/v1/sandboxvolumes/vol-1/prepare-portal-bind" {
		t.Fatalf("path = %q, want %q", gotPath, "/internal/v1/sandboxvolumes/vol-1/prepare-portal-bind")
	}
	if gotToken == "" {
		t.Fatal("expected internal token header to be set")
	}
	if gotReq.PodUID != "pod-uid" {
		t.Fatalf("podUID = %q, want %q", gotReq.PodUID, "pod-uid")
	}
}

func TestManagerStorageVolumeClientPrepareForPortalBindConflict(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = spec.WriteError(w, http.StatusConflict, spec.CodeConflict, "volume has active mounts")
	}))
	defer server.Close()

	client := NewManagerStorageVolumeClient(ManagerStorageVolumeClientConfig{
		BaseURL:        server.URL,
		TokenGenerator: staticTokenGenerator{},
	})
	err := client.PrepareForVolumePortalBind(t.Context(), PrepareVolumePortalBindRequest{
		TeamID:   "team-1",
		UserID:   "user-1",
		VolumeID: "vol-1",
	})
	if err == nil {
		t.Fatal("PrepareForVolumePortalBind() error = nil, want conflict")
	}
	if !errors.Is(err, ErrVolumePortalBindConflict) {
		t.Fatalf("PrepareForVolumePortalBind() error = %v, want ErrVolumePortalBindConflict", err)
	}
}
