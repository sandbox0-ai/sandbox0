package service

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"strings"

	"github.com/sandbox0-ai/sandbox0/pkg/egressauth"
	"go.uber.org/zap"
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

func (s *CredentialSourceService) ListSources(ctx context.Context, teamID string) ([]egressauth.CredentialSourceRecord, error) {
	if s == nil || s.store == nil {
		return nil, fmt.Errorf("credential source store is not configured")
	}
	return s.store.ListSourceRecords(ctx, teamID)
}

func (s *CredentialSourceService) GetSource(ctx context.Context, teamID, name string) (*egressauth.CredentialSourceRecord, error) {
	if s == nil || s.store == nil {
		return nil, fmt.Errorf("credential source store is not configured")
	}
	return s.store.GetSourceRecord(ctx, teamID, name)
}

func (s *CredentialSourceService) PutSource(ctx context.Context, teamID string, record *egressauth.CredentialSourceRecord) (*egressauth.CredentialSourceRecord, error) {
	if s == nil || s.store == nil {
		return nil, fmt.Errorf("credential source store is not configured")
	}
	if err := validateCredentialSourceRecord(record); err != nil {
		return nil, err
	}
	return s.store.PutSourceRecord(ctx, teamID, record)
}

func (s *CredentialSourceService) DeleteSource(ctx context.Context, teamID, name string) error {
	if s == nil || s.store == nil {
		return fmt.Errorf("credential source store is not configured")
	}
	return s.store.DeleteSourceRecord(ctx, teamID, name)
}

func validateCredentialSourceRecord(record *egressauth.CredentialSourceRecord) error {
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
	default:
		return fmt.Errorf("credential source resolver_kind %q is not supported", record.ResolverKind)
	}
	return nil
}
