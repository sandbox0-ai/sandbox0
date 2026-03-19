package service

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/egressauth"
	"go.uber.org/zap"
)

type memorySourceStore struct {
	records map[string]*egressauth.CredentialSourceMetadata
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

func cloneSourceMetadata(in *egressauth.CredentialSourceMetadata) *egressauth.CredentialSourceMetadata {
	if in == nil {
		return nil
	}
	cloned := *in
	return &cloned
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
