package service

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"strings"

	"github.com/sandbox0-ai/sandbox0/pkg/egressauth"
	"go.uber.org/zap"
	"golang.org/x/crypto/ssh"
)

// CredentialSourceService manages team-scoped credential sources.
type CredentialSourceService struct {
	store  egressauth.SourceStore
	logger *zap.Logger
}

func NewCredentialSourceService(store egressauth.SourceStore, logger *zap.Logger) *CredentialSourceService {
	return &CredentialSourceService{
		store:  store,
		logger: logger,
	}
}

func (s *CredentialSourceService) ListSources(ctx context.Context, teamID string) ([]egressauth.CredentialSourceMetadata, error) {
	if s == nil || s.store == nil {
		return nil, fmt.Errorf("credential source store is not configured")
	}
	return s.store.ListSourceMetadata(ctx, teamID)
}

func (s *CredentialSourceService) GetSource(ctx context.Context, teamID, name string) (*egressauth.CredentialSourceMetadata, error) {
	if s == nil || s.store == nil {
		return nil, fmt.Errorf("credential source store is not configured")
	}
	return s.store.GetSourceMetadata(ctx, teamID, name)
}

func (s *CredentialSourceService) PutSource(ctx context.Context, teamID string, record *egressauth.CredentialSourceWriteRequest) (*egressauth.CredentialSourceMetadata, error) {
	if s == nil || s.store == nil {
		return nil, fmt.Errorf("credential source store is not configured")
	}
	if err := validateCredentialSourceWriteRequest(record); err != nil {
		return nil, err
	}
	return s.store.PutSource(ctx, teamID, record)
}

func (s *CredentialSourceService) DeleteSource(ctx context.Context, teamID, name string) error {
	if s == nil || s.store == nil {
		return fmt.Errorf("credential source store is not configured")
	}
	return s.store.DeleteSource(ctx, teamID, name)
}

func validateCredentialSourceWriteRequest(record *egressauth.CredentialSourceWriteRequest) error {
	if record == nil {
		return fmt.Errorf("credential source record is required")
	}
	if record.Name == "" {
		return fmt.Errorf("credential source name is required")
	}
	switch record.ResolverKind {
	case "static_headers":
		if record.Spec.StaticHeaders == nil {
			return fmt.Errorf("static_headers spec is required")
		}
	case "static_username_password":
		if record.Spec.StaticUsernamePassword == nil {
			return fmt.Errorf("static_username_password spec is required")
		}
		if strings.TrimSpace(record.Spec.StaticUsernamePassword.Username) == "" {
			return fmt.Errorf("static_username_password username is required")
		}
		if strings.TrimSpace(record.Spec.StaticUsernamePassword.Password) == "" {
			return fmt.Errorf("static_username_password password is required")
		}
	case "static_tls_client_certificate":
		if record.Spec.StaticTLSClientCertificate == nil {
			return fmt.Errorf("static_tls_client_certificate spec is required")
		}
		if strings.TrimSpace(record.Spec.StaticTLSClientCertificate.CertificatePEM) == "" {
			return fmt.Errorf("static_tls_client_certificate certificatePem is required")
		}
		if strings.TrimSpace(record.Spec.StaticTLSClientCertificate.PrivateKeyPEM) == "" {
			return fmt.Errorf("static_tls_client_certificate privateKeyPem is required")
		}
		if _, err := tls.X509KeyPair(
			[]byte(record.Spec.StaticTLSClientCertificate.CertificatePEM),
			[]byte(record.Spec.StaticTLSClientCertificate.PrivateKeyPEM),
		); err != nil {
			return fmt.Errorf("static_tls_client_certificate keypair is invalid: %w", err)
		}
		if caPEM := strings.TrimSpace(record.Spec.StaticTLSClientCertificate.CAPEM); caPEM != "" {
			pool := x509.NewCertPool()
			if !pool.AppendCertsFromPEM([]byte(caPEM)) {
				return fmt.Errorf("static_tls_client_certificate caPem is invalid")
			}
		}
	case "static_ssh_private_key":
		if record.Spec.StaticSSHPrivateKey == nil {
			return fmt.Errorf("static_ssh_private_key spec is required")
		}
		if strings.TrimSpace(record.Spec.StaticSSHPrivateKey.PrivateKeyPEM) == "" {
			return fmt.Errorf("static_ssh_private_key privateKeyPem is required")
		}
		if strings.TrimSpace(record.Spec.StaticSSHPrivateKey.Passphrase) != "" {
			if _, err := ssh.ParsePrivateKeyWithPassphrase([]byte(record.Spec.StaticSSHPrivateKey.PrivateKeyPEM), []byte(record.Spec.StaticSSHPrivateKey.Passphrase)); err != nil {
				return fmt.Errorf("static_ssh_private_key privateKeyPem is invalid: %w", err)
			}
		} else if _, err := ssh.ParsePrivateKey([]byte(record.Spec.StaticSSHPrivateKey.PrivateKeyPEM)); err != nil {
			return fmt.Errorf("static_ssh_private_key privateKeyPem is invalid: %w", err)
		}
	default:
		return fmt.Errorf("credential source resolver_kind %q is not supported", record.ResolverKind)
	}
	return nil
}
