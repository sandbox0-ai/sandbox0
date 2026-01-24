package internalgateway

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/sandbox0-ai/infra/infra-operator/api/config"
	gatewayhttp "github.com/sandbox0-ai/infra/internal-gateway/pkg/http"
	"github.com/sandbox0-ai/infra/pkg/internalauth"
	"go.uber.org/zap"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type gatewayTestEnv struct {
	server       *httptest.Server
	edgeGen      *internalauth.Generator
	schedulerGen *internalauth.Generator
	publicKey    internalauth.PublicKeyType
}

type gatewayKeyPair struct {
	privateKey internalauth.PrivateKeyType
	publicKey  internalauth.PublicKeyType
}

func newGatewayTestEnv(t *testing.T, managerURL, storageProxyURL string, schedulerPerms []string, keys gatewayKeyPair) *gatewayTestEnv {
	t.Helper()

	edgeGen := internalauth.NewGenerator(internalauth.GeneratorConfig{
		Caller:     "edge-gateway",
		PrivateKey: keys.privateKey,
		TTL:        time.Minute,
	})
	schedulerGen := internalauth.NewGenerator(internalauth.GeneratorConfig{
		Caller:     "scheduler",
		PrivateKey: keys.privateKey,
		TTL:        time.Minute,
	})

	cfg := &config.InternalGatewayConfig{
		ManagerURL:              managerURL,
		StorageProxyURL:         storageProxyURL,
		AllowedCallers:          []string{"edge-gateway", "scheduler"},
		ProxyTimeout:            metav1.Duration{Duration: 2 * time.Second},
		SchedulerPermissions:    schedulerPerms,
		ProcdStoragePermissions: []string{"sandboxvolume:read"},
	}

	server, err := gatewayhttp.NewServer(cfg, nil, zap.NewNop())
	if err != nil {
		t.Fatalf("create internal-gateway server: %v", err)
	}

	httpServer := httptest.NewServer(server.Handler())
	t.Cleanup(httpServer.Close)

	return &gatewayTestEnv{
		server:       httpServer,
		edgeGen:      edgeGen,
		schedulerGen: schedulerGen,
		publicKey:    keys.publicKey,
	}
}

func writeInternalGatewayKeys(t *testing.T) (internalauth.PrivateKeyType, internalauth.PublicKeyType) {
	t.Helper()

	privatePEM, publicPEM, err := internalauth.GenerateEd25519KeyPair()
	if err != nil {
		t.Fatalf("generate internal keypair: %v", err)
	}

	tempDir := t.TempDir()
	pubPath := tempDir + "/internal_jwt_public.key"
	privPath := tempDir + "/internal_jwt_private.key"

	if err := os.WriteFile(pubPath, publicPEM, 0o600); err != nil {
		t.Fatalf("write public key: %v", err)
	}
	if err := os.WriteFile(privPath, privatePEM, 0o600); err != nil {
		t.Fatalf("write private key: %v", err)
	}

	// Override default paths for testing
	internalauth.DefaultInternalJWTPublicKeyPath = pubPath
	internalauth.DefaultInternalJWTPrivateKeyPath = privPath

	privateKey, err := internalauth.LoadEd25519PrivateKey(privatePEM)
	if err != nil {
		t.Fatalf("load private key: %v", err)
	}
	publicKey, err := internalauth.LoadEd25519PublicKey(publicPEM)
	if err != nil {
		t.Fatalf("load public key: %v", err)
	}

	return privateKey, publicKey
}

func newInternalToken(t *testing.T, gen *internalauth.Generator, perms []string) string {
	t.Helper()

	token, err := gen.Generate("internal-gateway", "team-1", "user-1", internalauth.GenerateOptions{
		Permissions: perms,
	})
	if err != nil {
		t.Fatalf("generate token: %v", err)
	}
	return token
}

func newValidator(t *testing.T, target string, publicKey internalauth.PublicKeyType, allowedCallers []string) *internalauth.Validator {
	t.Helper()

	cfg := internalauth.ValidatorConfig{
		Target:             target,
		PublicKey:          publicKey,
		AllowedCallers:     allowedCallers,
		ClockSkewTolerance: 10 * time.Second,
	}
	return internalauth.NewValidator(cfg)
}

func doGatewayRequest(t *testing.T, client *http.Client, method, url, token string, body any) (*http.Response, []byte) {
	t.Helper()

	var payload io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("marshal request body: %v", err)
		}
		payload = bytes.NewReader(encoded)
	}

	req, err := http.NewRequest(method, url, payload)
	if err != nil {
		t.Fatalf("create request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set(internalauth.DefaultTokenHeader, token)
	}

	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("send request: %v", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}

	return resp, respBody
}
