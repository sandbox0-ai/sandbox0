package license

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const featureMultiCluster = "multi_cluster"

func TestLoadFromFile(t *testing.T) {
	t.Parallel()

	now := time.Unix(1_700_000_000, 0)
	keyID := "test-key"
	fixture, licensePath := buildSignedLicenseFixture(t, keyID, Claims{
		Version:   "v1",
		Subject:   "sandbox0-enterprise",
		IssuedAt:  now.Add(-time.Hour).Unix(),
		NotBefore: now.Add(-time.Minute).Unix(),
		ExpiresAt: now.Add(time.Hour).Unix(),
		Features:  []string{featureMultiCluster},
	})

	checker, err := loadAt(map[string]ed25519.PublicKey{keyID: fixture.publicKey}, mustRead(t, licensePath), now)
	if err != nil {
		t.Fatalf("load license: %v", err)
	}
	if checker.KeyID() != keyID {
		t.Fatalf("unexpected key id: %q", checker.KeyID())
	}
	if !checker.HasFeature(featureMultiCluster) {
		t.Fatalf("expected %s feature to be enabled", featureMultiCluster)
	}
	if checker.Claims().Subject != "sandbox0-enterprise" {
		t.Fatalf("unexpected subject: %q", checker.Claims().Subject)
	}
}

func TestLoadFromFile_Expired(t *testing.T) {
	t.Parallel()

	now := time.Unix(1_700_000_000, 0)
	keyID := "test-key"
	fixture, licensePath := buildSignedLicenseFixture(t, keyID, Claims{
		Version:   "v1",
		Subject:   "sandbox0-enterprise",
		ExpiresAt: now.Add(-time.Minute).Unix(),
		Features:  []string{featureMultiCluster},
	})

	_, err := loadAt(map[string]ed25519.PublicKey{keyID: fixture.publicKey}, mustRead(t, licensePath), now)
	if err == nil {
		t.Fatalf("expected expired error")
	}
	if err != ErrLicenseExpired {
		t.Fatalf("expected ErrLicenseExpired, got: %v", err)
	}
}

func TestLoadFromFile_UnknownKeyID(t *testing.T) {
	t.Parallel()

	now := time.Unix(1_700_000_000, 0)
	fixture, licensePath := buildSignedLicenseFixture(t, "test-key", Claims{
		Version:   "v1",
		Subject:   "sandbox0-enterprise",
		ExpiresAt: now.Add(time.Hour).Unix(),
		Features:  []string{featureMultiCluster},
	})

	_, err := loadAt(map[string]ed25519.PublicKey{"other-key": fixture.publicKey}, mustRead(t, licensePath), now)
	if err == nil {
		t.Fatalf("expected unknown key id error")
	}
	if err.Error() == "" || !strings.Contains(err.Error(), ErrUnknownKeyID.Error()) {
		t.Fatalf("expected unknown key id error, got: %v", err)
	}
}

type fixture struct {
	publicKey ed25519.PublicKey
}

func buildSignedLicenseFixture(t *testing.T, keyID string, claims Claims) (fixture, string) {
	t.Helper()

	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate key pair: %v", err)
	}

	payloadBytes, err := json.Marshal(claims)
	if err != nil {
		t.Fatalf("marshal claims: %v", err)
	}
	signature := ed25519.Sign(privateKey, payloadBytes)

	env := Envelope{
		KeyID:     keyID,
		Payload:   base64.RawURLEncoding.EncodeToString(payloadBytes),
		Signature: base64.RawURLEncoding.EncodeToString(signature),
	}
	licenseBytes, err := json.Marshal(env)
	if err != nil {
		t.Fatalf("marshal license envelope: %v", err)
	}

	dir := t.TempDir()
	licensePath := filepath.Join(dir, "license.json")
	if err := os.WriteFile(licensePath, licenseBytes, 0o600); err != nil {
		t.Fatalf("write license file: %v", err)
	}

	return fixture{publicKey: publicKey}, licensePath
}

func mustRead(t *testing.T, path string) []byte {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read file %s: %v", path, err)
	}
	return raw
}
