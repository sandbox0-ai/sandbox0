package internalgateway

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/sandbox0-ai/infra/pkg/auth"
)

func TestInternalGatewayIntegration_AuthRequired(t *testing.T) {
	keys := gatewayKeyPair{}
	keys.privateKey, keys.publicKey = writeInternalGatewayKeys(t)

	managerServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatalf("unexpected manager request: %s", r.URL.Path)
	}))
	t.Cleanup(managerServer.Close)

	storageServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatalf("unexpected storage-proxy request: %s", r.URL.Path)
	}))
	t.Cleanup(storageServer.Close)

	env := newGatewayTestEnv(t, managerServer.URL, storageServer.URL, nil, keys)

	resp, _ := doGatewayRequest(t, env.server.Client(), http.MethodGet, env.server.URL+"/api/v1/templates", "", nil)
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected unauthorized, got %d", resp.StatusCode)
	}
}

func TestInternalGatewayIntegration_PermissionDenied(t *testing.T) {
	keys := gatewayKeyPair{}
	keys.privateKey, keys.publicKey = writeInternalGatewayKeys(t)

	managerServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatalf("unexpected manager request: %s", r.URL.Path)
	}))
	t.Cleanup(managerServer.Close)

	storageServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatalf("unexpected storage-proxy request: %s", r.URL.Path)
	}))
	t.Cleanup(storageServer.Close)

	env := newGatewayTestEnv(t, managerServer.URL, storageServer.URL, nil, keys)
	token := newInternalToken(t, env.edgeGen, []string{auth.PermSandboxRead})

	resp, _ := doGatewayRequest(t, env.server.Client(), http.MethodGet, env.server.URL+"/api/v1/templates", token, nil)
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("expected forbidden, got %d", resp.StatusCode)
	}
}

func TestInternalGatewayIntegration_SandboxWriteRequired(t *testing.T) {
	keys := gatewayKeyPair{}
	keys.privateKey, keys.publicKey = writeInternalGatewayKeys(t)

	managerServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatalf("unexpected manager request: %s", r.URL.Path)
	}))
	t.Cleanup(managerServer.Close)

	storageServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatalf("unexpected storage-proxy request: %s", r.URL.Path)
	}))
	t.Cleanup(storageServer.Close)

	env := newGatewayTestEnv(t, managerServer.URL, storageServer.URL, nil, keys)
	token := newInternalToken(t, env.edgeGen, []string{auth.PermSandboxRead})

	resp, _ := doGatewayRequest(t, env.server.Client(), http.MethodPatch, env.server.URL+"/api/v1/sandboxes/sbx-1/bandwidth", token, map[string]any{
		"upload_bps":   1024,
		"download_bps": 1024,
	})
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("expected forbidden for sandbox bandwidth update, got %d", resp.StatusCode)
	}
}

func TestInternalGatewayIntegration_VolumeEndpointsRequirePermissions(t *testing.T) {
	keys := gatewayKeyPair{}
	keys.privateKey, keys.publicKey = writeInternalGatewayKeys(t)

	managerServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatalf("unexpected manager request: %s", r.URL.Path)
	}))
	t.Cleanup(managerServer.Close)

	storageServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatalf("unexpected storage-proxy request: %s", r.URL.Path)
	}))
	t.Cleanup(storageServer.Close)

	env := newGatewayTestEnv(t, managerServer.URL, storageServer.URL, nil, keys)
	readToken := newInternalToken(t, env.edgeGen, []string{auth.PermSandboxRead})
	writeToken := newInternalToken(t, env.edgeGen, []string{auth.PermSandboxWrite})

	resp, _ := doGatewayRequest(t, env.server.Client(), http.MethodPost, env.server.URL+"/api/v1/sandboxvolumes", readToken, map[string]any{
		"name": "vol-1",
	})
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("expected forbidden for create, got %d", resp.StatusCode)
	}

	resp, _ = doGatewayRequest(t, env.server.Client(), http.MethodDelete, env.server.URL+"/api/v1/sandboxvolumes/vol-1", readToken, nil)
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("expected forbidden for delete, got %d", resp.StatusCode)
	}

	resp, _ = doGatewayRequest(t, env.server.Client(), http.MethodGet, env.server.URL+"/api/v1/sandboxvolumes", writeToken, nil)
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("expected forbidden for list, got %d", resp.StatusCode)
	}
}
