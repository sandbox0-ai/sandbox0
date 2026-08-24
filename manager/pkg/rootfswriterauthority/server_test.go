package rootfswriterauthority

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestAuthorityServerRequiresVerifiedClientCertificates(t *testing.T) {
	certFile, keyFile, caFile := writeAuthorityServerTestIdentity(t)
	server, err := NewServer(ServerConfig{
		Address: "127.0.0.1:0", CertFile: certFile, KeyFile: keyFile,
		ClientCAFile: caFile, Handler: http.NotFoundHandler(),
	})
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}
	if server.config.ClientAuth != tls.RequireAndVerifyClientCert {
		t.Fatalf("ClientAuth = %v, want RequireAndVerifyClientCert", server.config.ClientAuth)
	}
	if server.config.ClientCAs == nil || len(server.config.ClientCAs.Subjects()) != 1 {
		t.Fatal("client CA was not installed")
	}
}

func TestAuthorityServerRejectsMissingOrInvalidClientCA(t *testing.T) {
	certFile, keyFile, _ := writeAuthorityServerTestIdentity(t)
	_, err := NewServer(ServerConfig{
		Address: "127.0.0.1:0", CertFile: certFile, KeyFile: keyFile,
		Handler: http.NotFoundHandler(),
	})
	if err == nil {
		t.Fatal("missing client CA was accepted")
	}
	invalidCA := filepath.Join(t.TempDir(), "invalid-ca.pem")
	if err := os.WriteFile(invalidCA, []byte("not a certificate"), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err = NewServer(ServerConfig{
		Address: "127.0.0.1:0", CertFile: certFile, KeyFile: keyFile,
		ClientCAFile: invalidCA, Handler: http.NotFoundHandler(),
	})
	if err == nil {
		t.Fatal("invalid client CA was accepted")
	}
}

func TestAuthorityServerPublishesReadyAfterBinding(t *testing.T) {
	certFile, keyFile, caFile := writeAuthorityServerTestIdentity(t)
	server, err := NewServer(ServerConfig{
		Address: "127.0.0.1:0", CertFile: certFile, KeyFile: keyFile,
		ClientCAFile: caFile, Handler: http.NotFoundHandler(),
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- server.Start(ctx) }()
	select {
	case <-server.Ready():
	case <-time.After(5 * time.Second):
		t.Fatal("authority server did not publish readiness")
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Start() error = %v", err)
	}
}

func writeAuthorityServerTestIdentity(t *testing.T) (string, string, string) {
	t.Helper()
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "authority.test"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		KeyUsage:    x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IsCA:        true, BasicConstraintsValid: true, DNSNames: []string{"authority.test"},
	}
	raw, err := x509.CreateCertificate(rand.Reader, template, template, publicKey, privateKey)
	if err != nil {
		t.Fatal(err)
	}
	key, err := x509.MarshalPKCS8PrivateKey(privateKey)
	if err != nil {
		t.Fatal(err)
	}
	directory := t.TempDir()
	certFile := filepath.Join(directory, "tls.crt")
	keyFile := filepath.Join(directory, "tls.key")
	caFile := filepath.Join(directory, "ca.crt")
	certificatePEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: raw})
	if err := os.WriteFile(certFile, certificatePEM, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(caFile, certificatePEM, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyFile, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: key}), 0o600); err != nil {
		t.Fatal(err)
	}
	return certFile, keyFile, caFile
}
