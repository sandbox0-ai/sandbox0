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
	storageKind := strings.TrimSpace(record.StorageKind)
	if storageKind == "" {
		storageKind = egressauth.CredentialSourceStorageKindEncryptedPG
	}
	switch storageKind {
	case egressauth.CredentialSourceStorageKindEncryptedPG:
		if err := validateSecretBearingCredentialSource(record); err != nil {
			return err
		}
	case egressauth.CredentialSourceStorageKindHashiCorpVault:
		return validateHashiCorpVaultCredentialSource(record)
	default:
		return fmt.Errorf("credential source storageKind %q is not supported", record.StorageKind)
	}
	return nil
}

func validateHashiCorpVaultCredentialSource(record *egressauth.CredentialSourceWriteRequest) error {
	hasSpec := credentialSourceSpecPresent(record.Spec)
	if record.ExternalRef != nil {
		if err := validateExternalCredentialSourceRef(record, !hasSpec); err != nil {
			return err
		}
	}
	if !hasSpec {
		if record.ExternalRef == nil {
			return fmt.Errorf("hashicorp_vault credential source requires spec or externalRef")
		}
		return nil
	}
	return validateSecretBearingCredentialSource(record)
}

func validateSecretBearingCredentialSource(record *egressauth.CredentialSourceWriteRequest) error {
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

func validateExternalCredentialSourceRef(record *egressauth.CredentialSourceWriteRequest, requireFields bool) error {
	if record.ExternalRef == nil {
		return fmt.Errorf("hashicorp_vault credential source requires externalRef")
	}
	if record.ExternalRef.Provider == "" {
		record.ExternalRef.Provider = egressauth.CredentialSourceExternalProviderHashiCorpVault
	}
	if record.ExternalRef.Provider != egressauth.CredentialSourceExternalProviderHashiCorpVault {
		return fmt.Errorf("external credential source provider %q is not supported", record.ExternalRef.Provider)
	}
	if strings.TrimSpace(record.ExternalRef.Path) == "" {
		return fmt.Errorf("hashicorp_vault externalRef path is required")
	}
	if !requireFields {
		return nil
	}
	switch record.ResolverKind {
	case "static_headers":
		if len(record.ExternalRef.Fields) == 0 {
			return fmt.Errorf("static_headers hashicorp_vault externalRef fields are required")
		}
	case "static_username_password":
		if record.ExternalRef.Fields["username"] == "" || record.ExternalRef.Fields["password"] == "" {
			return fmt.Errorf("static_username_password hashicorp_vault externalRef requires username and password fields")
		}
	case "static_tls_client_certificate":
		if record.ExternalRef.Fields["certificatePem"] == "" || record.ExternalRef.Fields["privateKeyPem"] == "" {
			return fmt.Errorf("static_tls_client_certificate hashicorp_vault externalRef requires certificatePem and privateKeyPem fields")
		}
	case "static_ssh_private_key":
		if record.ExternalRef.Fields["privateKeyPem"] == "" {
			return fmt.Errorf("static_ssh_private_key hashicorp_vault externalRef requires privateKeyPem field")
		}
	default:
		return fmt.Errorf("credential source resolver_kind %q is not supported", record.ResolverKind)
	}
	return nil
}

func credentialSourceSpecPresent(spec egressauth.CredentialSourceSecretSpec) bool {
	return spec.StaticHeaders != nil ||
		spec.StaticTLSClientCertificate != nil ||
		spec.StaticUsernamePassword != nil ||
		spec.StaticSSHPrivateKey != nil
}
