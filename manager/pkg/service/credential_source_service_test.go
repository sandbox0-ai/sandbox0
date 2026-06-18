package service

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"math/big"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/egressauth"
	"go.uber.org/zap"
)

type memorySourceStore struct {
	records map[string]*egressauth.CredentialSourceMetadata
}

type nilListSourceStore struct {
	*memorySourceStore
}

func newMemorySourceStore() *memorySourceStore {
	return &memorySourceStore{records: make(map[string]*egressauth.CredentialSourceMetadata)}
}

func (s *memorySourceStore) ListSourceMetadata(_ context.Context, teamID string) ([]egressauth.CredentialSourceMetadata, error) {
	out := make([]egressauth.CredentialSourceMetadata, 0)
	for key, value := range s.records {
		if len(key) >= len(teamID)+1 && key[:len(teamID)+1] == teamID+"/" {
			out = append(out, *cloneSourceMetadata(value))
		}
	}
	return out, nil
}

func (s *memorySourceStore) GetSourceMetadata(_ context.Context, teamID, name string) (*egressauth.CredentialSourceMetadata, error) {
	return cloneSourceMetadata(s.records[teamID+"/"+name]), nil
}

func (s *memorySourceStore) PutSource(_ context.Context, teamID string, record *egressauth.CredentialSourceWriteRequest) (*egressauth.CredentialSourceMetadata, error) {
	key := teamID + "/" + record.Name
	current := s.records[key]
	cloned := &egressauth.CredentialSourceMetadata{
		Name:         record.Name,
		ResolverKind: record.ResolverKind,
		StorageKind:  record.StorageKind,
		Status:       "active",
	}
	if current == nil || current.CurrentVersion == 0 {
		cloned.CurrentVersion = 1
	} else {
		cloned.CurrentVersion = current.CurrentVersion + 1
	}
	s.records[key] = cloned
	return cloneSourceMetadata(cloned), nil
}

func (s *memorySourceStore) DeleteSource(_ context.Context, teamID, name string) error {
	delete(s.records, teamID+"/"+name)
	return nil
}

func (s *nilListSourceStore) ListSourceMetadata(context.Context, string) ([]egressauth.CredentialSourceMetadata, error) {
	return nil, nil
}

func TestCredentialSourceServiceListSourcesReturnsEmptySlice(t *testing.T) {
	svc := NewCredentialSourceService(&nilListSourceStore{memorySourceStore: newMemorySourceStore()}, zap.NewNop())

	records, err := svc.ListSources(context.Background(), "team-1")
	if err != nil {
		t.Fatalf("list sources: %v", err)
	}
	if records == nil {
		t.Fatal("records slice is nil, want empty slice")
	}
	if len(records) != 0 {
		t.Fatalf("records = %d, want 0", len(records))
	}
}

func TestCredentialSourceServicePutSource(t *testing.T) {
	store := newMemorySourceStore()
	svc := NewCredentialSourceService(store, zap.NewNop())

	record, err := svc.PutSource(context.Background(), "team-1", &egressauth.CredentialSourceWriteRequest{
		Name:         "github-api",
		ResolverKind: "static_headers",
		Spec: egressauth.CredentialSourceSecretSpec{
			StaticHeaders: &egressauth.StaticHeadersSourceSpec{
				Values: map[string]string{"token": "abc"},
			},
		},
	})
	if err != nil {
		t.Fatalf("put source: %v", err)
	}
	if record.CurrentVersion != 1 {
		t.Fatalf("current version = %d, want 1", record.CurrentVersion)
	}
}

func TestCredentialSourceServiceCreateSourceRejectsDuplicate(t *testing.T) {
	store := newMemorySourceStore()
	svc := NewCredentialSourceService(store, zap.NewNop())

	first, err := svc.CreateSource(context.Background(), "team-1", staticHeadersCredentialSourceRequest("github-api", "first"))
	if err != nil {
		t.Fatalf("create source: %v", err)
	}
	if first.CurrentVersion != 1 {
		t.Fatalf("current version = %d, want 1", first.CurrentVersion)
	}

	_, err = svc.CreateSource(context.Background(), "team-1", staticHeadersCredentialSourceRequest("github-api", "second"))
	if !errors.Is(err, ErrCredentialSourceAlreadyExists) {
		t.Fatalf("create duplicate error = %v, want ErrCredentialSourceAlreadyExists", err)
	}
	stored := store.records["team-1/github-api"]
	if stored == nil || stored.CurrentVersion != 1 {
		t.Fatalf("stored source = %#v, want unchanged version 1", stored)
	}
}

func TestCredentialSourceServiceUpdateSourceRejectsMissing(t *testing.T) {
	store := newMemorySourceStore()
	svc := NewCredentialSourceService(store, zap.NewNop())

	_, err := svc.UpdateSource(context.Background(), "team-1", staticHeadersCredentialSourceRequest("missing", "token"))
	if !errors.Is(err, ErrCredentialSourceNotFound) {
		t.Fatalf("update missing error = %v, want ErrCredentialSourceNotFound", err)
	}
	if len(store.records) != 0 {
		t.Fatalf("records = %d, want 0", len(store.records))
	}
}

func TestCredentialSourceServiceUpdateSourceUpdatesExisting(t *testing.T) {
	store := newMemorySourceStore()
	svc := NewCredentialSourceService(store, zap.NewNop())

	if _, err := svc.CreateSource(context.Background(), "team-1", staticHeadersCredentialSourceRequest("github-api", "first")); err != nil {
		t.Fatalf("create source: %v", err)
	}
	updated, err := svc.UpdateSource(context.Background(), "team-1", staticHeadersCredentialSourceRequest("github-api", "second"))
	if err != nil {
		t.Fatalf("update source: %v", err)
	}
	if updated.CurrentVersion != 2 {
		t.Fatalf("current version = %d, want 2", updated.CurrentVersion)
	}
}

func TestCredentialSourceServicePutTLSClientCertificateSource(t *testing.T) {
	store := newMemorySourceStore()
	svc := NewCredentialSourceService(store, zap.NewNop())

	certPEM, keyPEM, err := testTLSKeyPair(t)
	if err != nil {
		t.Fatalf("test tls keypair: %v", err)
	}

	record, err := svc.PutSource(context.Background(), "team-1", &egressauth.CredentialSourceWriteRequest{
		Name:         "db-cert",
		ResolverKind: "static_tls_client_certificate",
		Spec: egressauth.CredentialSourceSecretSpec{
			StaticTLSClientCertificate: &egressauth.StaticTLSClientCertificateSourceSpec{
				CertificatePEM: certPEM,
				PrivateKeyPEM:  keyPEM,
			},
		},
	})
	if err != nil {
		t.Fatalf("put tls client certificate source: %v", err)
	}
	if record.CurrentVersion != 1 {
		t.Fatalf("current version = %d, want 1", record.CurrentVersion)
	}
}

func TestCredentialSourceServicePutUsernamePasswordSource(t *testing.T) {
	store := newMemorySourceStore()
	svc := NewCredentialSourceService(store, zap.NewNop())

	record, err := svc.PutSource(context.Background(), "team-1", &egressauth.CredentialSourceWriteRequest{
		Name:         "corp-proxy",
		ResolverKind: "static_username_password",
		Spec: egressauth.CredentialSourceSecretSpec{
			StaticUsernamePassword: &egressauth.StaticUsernamePasswordSourceSpec{
				Username: "alice",
				Password: "secret",
			},
		},
	})
	if err != nil {
		t.Fatalf("put username/password source: %v", err)
	}
	if record.CurrentVersion != 1 {
		t.Fatalf("current version = %d, want 1", record.CurrentVersion)
	}
}

func TestCredentialSourceServicePutSSHPrivateKeySource(t *testing.T) {
	store := newMemorySourceStore()
	svc := NewCredentialSourceService(store, zap.NewNop())

	_, keyPEM, err := testTLSKeyPair(t)
	if err != nil {
		t.Fatalf("test private key: %v", err)
	}

	record, err := svc.PutSource(context.Background(), "team-1", &egressauth.CredentialSourceWriteRequest{
		Name:         "git-ssh",
		ResolverKind: "static_ssh_private_key",
		Spec: egressauth.CredentialSourceSecretSpec{
			StaticSSHPrivateKey: &egressauth.StaticSSHPrivateKeySourceSpec{
				PrivateKeyPEM: keyPEM,
			},
		},
	})
	if err != nil {
		t.Fatalf("put ssh private key source: %v", err)
	}
	if record.CurrentVersion != 1 {
		t.Fatalf("current version = %d, want 1", record.CurrentVersion)
	}
}

func TestCredentialSourceServiceRejectsWhitespaceName(t *testing.T) {
	store := newMemorySourceStore()
	svc := NewCredentialSourceService(store, zap.NewNop())

	_, err := svc.PutSource(context.Background(), "team-1", &egressauth.CredentialSourceWriteRequest{
		Name:         "   ",
		ResolverKind: "static_username_password",
		Spec: egressauth.CredentialSourceSecretSpec{
			StaticUsernamePassword: &egressauth.StaticUsernamePasswordSourceSpec{
				Username: "alice",
				Password: "secret",
			},
		},
	})
	if err == nil {
		t.Fatal("expected whitespace name to be rejected")
	}
	if len(store.records) != 0 {
		t.Fatalf("records = %d, want 0", len(store.records))
	}
}

func TestCredentialSourceServiceRejectsSlashName(t *testing.T) {
	store := newMemorySourceStore()
	svc := NewCredentialSourceService(store, zap.NewNop())

	_, err := svc.PutSource(context.Background(), "team-1", &egressauth.CredentialSourceWriteRequest{
		Name:         "manual/slash",
		ResolverKind: "static_headers",
		Spec: egressauth.CredentialSourceSecretSpec{
			StaticHeaders: &egressauth.StaticHeadersSourceSpec{
				Values: map[string]string{"token": "abc"},
			},
		},
	})
	if err == nil {
		t.Fatal("expected slash name to be rejected")
	}
	if len(store.records) != 0 {
		t.Fatalf("records = %d, want 0", len(store.records))
	}
}

func TestCredentialSourceServiceRejectsExplicitStorageKind(t *testing.T) {
	store := newMemorySourceStore()
	svc := NewCredentialSourceService(store, zap.NewNop())

	_, err := svc.PutSource(context.Background(), "team-1", &egressauth.CredentialSourceWriteRequest{
		Name:         "corp-proxy",
		ResolverKind: "static_username_password",
		StorageKind:  egressauth.CredentialSourceStorageKindHashiCorpVault,
		Spec: egressauth.CredentialSourceSecretSpec{
			StaticUsernamePassword: &egressauth.StaticUsernamePasswordSourceSpec{
				Username: "alice",
				Password: "secret",
			},
		},
	})
	if err == nil {
		t.Fatal("expected explicit storageKind to be rejected")
	}
}

func TestCredentialSourceServiceRejectsExternalRef(t *testing.T) {
	store := newMemorySourceStore()
	svc := NewCredentialSourceService(store, zap.NewNop())

	_, err := svc.PutSource(context.Background(), "team-1", &egressauth.CredentialSourceWriteRequest{
		Name:         "corp-proxy",
		ResolverKind: "static_username_password",
		ExternalRef: &egressauth.CredentialSourceExternalRefSpec{
			Path: "sandbox0/credential-sources/team-1/corp-proxy",
			Fields: map[string]string{
				"username": "user",
				"password": "pass",
			},
		},
	})
	if err == nil {
		t.Fatal("expected externalRef to be rejected")
	}
}

func cloneSourceMetadata(in *egressauth.CredentialSourceMetadata) *egressauth.CredentialSourceMetadata {
	if in == nil {
		return nil
	}
	cloned := *in
	return &cloned
}

func staticHeadersCredentialSourceRequest(name, token string) *egressauth.CredentialSourceWriteRequest {
	return &egressauth.CredentialSourceWriteRequest{
		Name:         name,
		ResolverKind: "static_headers",
		Spec: egressauth.CredentialSourceSecretSpec{
			StaticHeaders: &egressauth.StaticHeadersSourceSpec{
				Values: map[string]string{"token": token},
			},
		},
	}
}

func testTLSKeyPair(t *testing.T) (string, string, error) {
	t.Helper()

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return "", "", err
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			CommonName: "test-client",
		},
		NotBefore:             time.Now().Add(-time.Minute),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		return "", "", err
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
	return string(certPEM), string(keyPEM), nil
}
