package credentialstore

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	apiconfig "github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	infrav1alpha1 "github.com/sandbox0-ai/sandbox0/infra-operator/api/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/infra-operator/internal/controller/pkg/common"
)

func TestEnsureEncryptedPGKeyCreatesStableSecret(t *testing.T) {
	ctx := context.Background()
	infra, resources := testCredentialStoreResources(t)
	scope := common.NewObjectScope(infra)

	ref, err := EnsureEncryptedPGKey(ctx, resources, scope)
	if err != nil {
		t.Fatalf("ensure key: %v", err)
	}
	if ref.SecretName != "demo-credential-source-encryption" || ref.KeyID != defaultKeyID {
		t.Fatalf("unexpected ref: %#v", ref)
	}
	secret := &corev1.Secret{}
	if err := resources.Client.Get(ctx, types.NamespacedName{Name: ref.SecretName, Namespace: infra.Namespace}, secret); err != nil {
		t.Fatalf("get key secret: %v", err)
	}
	firstKey := append([]byte(nil), secret.Data[encryptedPGKeyKey]...)
	if len(firstKey) != encryptedPGKeyLength {
		t.Fatalf("key length = %d, want %d", len(firstKey), encryptedPGKeyLength)
	}

	if _, err := EnsureEncryptedPGKey(ctx, resources, scope); err != nil {
		t.Fatalf("ensure key again: %v", err)
	}
	if err := resources.Client.Get(ctx, types.NamespacedName{Name: ref.SecretName, Namespace: infra.Namespace}, secret); err != nil {
		t.Fatalf("get key secret again: %v", err)
	}
	if string(secret.Data[encryptedPGKeyKey]) != string(firstKey) {
		t.Fatal("encrypted_pg key changed across ensure calls")
	}
}

func TestApplyManagerCredentialStoreConfigForBuiltinVault(t *testing.T) {
	ctx := context.Background()
	infra, resources := testCredentialStoreResources(t)
	infra.Spec.CredentialVault = &infrav1alpha1.CredentialVaultConfig{
		Type: infrav1alpha1.CredentialVaultTypeBuiltin,
		Builtin: &infrav1alpha1.BuiltinCredentialVaultConfig{
			Enabled: true,
			Mount:   "sandbox0",
		},
	}
	cfg := &apiconfig.ManagerConfig{}
	if err := ApplyManagerCredentialStoreConfig(ctx, resources, common.NewObjectScope(infra), cfg); err != nil {
		t.Fatalf("apply manager config: %v", err)
	}
	if cfg.CredentialStore.DefaultStorageKind != "hashicorp_vault" {
		t.Fatalf("default storage kind = %q", cfg.CredentialStore.DefaultStorageKind)
	}
	if cfg.CredentialStore.EncryptedPG.KeyFile != EncryptedPGKeyFilePath {
		t.Fatalf("key file = %q", cfg.CredentialStore.EncryptedPG.KeyFile)
	}
	if len(cfg.CredentialStore.Vault.Connections) != 1 {
		t.Fatalf("vault connections = %d, want 1", len(cfg.CredentialStore.Vault.Connections))
	}
	conn := cfg.CredentialStore.Vault.Connections[0]
	if conn.Address != "http://demo-openbao.sandbox0-system.svc:8200" {
		t.Fatalf("vault address = %q", conn.Address)
	}
	if conn.DefaultMount != "sandbox0" || conn.TokenFile != VaultTokenFilePath("default") {
		t.Fatalf("unexpected vault connection: %#v", conn)
	}

	mounts, volumes := ManagerCredentialStoreVolumes(common.NewObjectScope(infra), cfg)
	if len(mounts) != 2 || len(volumes) != 2 {
		t.Fatalf("mounts=%d volumes=%d, want 2/2", len(mounts), len(volumes))
	}
}

func TestApplyManagerCredentialStoreConfigDefaultsToEncryptedPG(t *testing.T) {
	ctx := context.Background()
	infra, resources := testCredentialStoreResources(t)
	cfg := &apiconfig.ManagerConfig{}
	if err := ApplyManagerCredentialStoreConfig(ctx, resources, common.NewObjectScope(infra), cfg); err != nil {
		t.Fatalf("apply manager config: %v", err)
	}
	if cfg.CredentialStore.DefaultStorageKind != "encrypted_pg" {
		t.Fatalf("default storage kind = %q", cfg.CredentialStore.DefaultStorageKind)
	}
	if cfg.CredentialStore.EncryptedPG.KeyFile != EncryptedPGKeyFilePath {
		t.Fatalf("key file = %q", cfg.CredentialStore.EncryptedPG.KeyFile)
	}
	if len(cfg.CredentialStore.Vault.Connections) != 0 {
		t.Fatalf("vault connections = %d, want 0", len(cfg.CredentialStore.Vault.Connections))
	}
}

func TestValidateExternalCredentialVault(t *testing.T) {
	ctx := context.Background()
	infra, resources := testCredentialStoreResources(t)
	infra.Spec.CredentialVault = &infrav1alpha1.CredentialVaultConfig{
		Type: infrav1alpha1.CredentialVaultTypeExternal,
		External: &infrav1alpha1.ExternalCredentialVaultConfig{
			Address: "https://vault.example.com",
			TokenSecret: infrav1alpha1.CredentialVaultTokenSecretRef{
				Name: "vault-token",
			},
		},
	}
	if err := resources.Client.Create(ctx, &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "vault-token", Namespace: infra.Namespace},
		Data:       map[string][]byte{"token": []byte("test-token")},
	}); err != nil {
		t.Fatalf("seed token secret: %v", err)
	}
	if err := ValidateExternalCredentialVault(ctx, resources.Client, infra); err != nil {
		t.Fatalf("validate external vault: %v", err)
	}
}

func TestEnsureKV2MountCreatesMissingOpenBaoMount(t *testing.T) {
	ctx := context.Background()
	var created bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/sys/mounts/secret" && r.Method == http.MethodGet {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"errors":["No secret engine mount at secret/"]}`))
			return
		}
		if r.URL.Path == "/v1/sys/mounts/secret" && r.Method == http.MethodPost {
			var body struct {
				Type    string            `json:"type"`
				Options map[string]string `json:"options"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatalf("decode mount request: %v", err)
			}
			if body.Type != "kv" || body.Options["version"] != "2" {
				t.Fatalf("unexpected mount body: %#v", body)
			}
			created = true
			w.WriteHeader(http.StatusNoContent)
			return
		}
		t.Fatalf("unexpected request: %s %s", r.Method, r.URL.Path)
	}))
	defer server.Close()

	api := newOpenBaoAPI(server.URL)
	if err := api.ensureKV2Mount(ctx, "root-token", "secret"); err != nil {
		t.Fatalf("ensure mount: %v", err)
	}
	if !created {
		t.Fatal("expected missing mount to be created")
	}
}

func testCredentialStoreResources(t *testing.T) (*infrav1alpha1.Sandbox0Infra, *common.ResourceManager) {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("add core scheme: %v", err)
	}
	if err := infrav1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add infra scheme: %v", err)
	}
	infra := &infrav1alpha1.Sandbox0Infra{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "demo",
			Namespace: "sandbox0-system",
		},
	}
	client := fake.NewClientBuilder().WithScheme(scheme).WithObjects(infra).Build()
	return infra, common.NewResourceManager(client, scheme, nil, common.LocalDevConfig{})
}
