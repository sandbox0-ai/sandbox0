package runtime

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

type fakeBindingStore struct {
	recordFn        func() *egressauth.BindingRecord
	sourceVersionFn func(int64, int64) *egressauth.CredentialSourceVersion
}

func (f *fakeBindingStore) GetBindings(context.Context, string, string) (*egressauth.BindingRecord, error) {
	if f.recordFn == nil {
		return nil, nil
	}
	return f.recordFn(), nil
}

func (f *fakeBindingStore) UpsertBindings(context.Context, *egressauth.BindingRecord) error {
	return errors.New("not implemented")
}

func (f *fakeBindingStore) DeleteBindings(context.Context, string, string) error {
	return errors.New("not implemented")
}

func (f *fakeBindingStore) GetSourceByRef(context.Context, string, string) (*egressauth.CredentialSource, error) {
	return nil, errors.New("not implemented")
}

func (f *fakeBindingStore) GetSourceVersion(_ context.Context, sourceID, version int64) (*egressauth.CredentialSourceVersion, error) {
	if f.sourceVersionFn == nil {
		return nil, nil
	}
	return f.sourceVersionFn(sourceID, version), nil
}

type countingProvider struct {
	calls int
}

func (p *countingProvider) Resolve(_ context.Context, req *egressauth.ResolveRequest, _ *egressauth.CredentialBinding, source *egressauth.CredentialSourceVersion, defaultTTL time.Duration) (*ResolveResult, error) {
	p.calls++
	expiresAt := time.Now().UTC().Add(defaultTTL)
	return &ResolveResult{
		Response: &egressauth.ResolveResponse{
			AuthRef:   req.AuthRef,
			Headers:   map[string]string{"Authorization": "Bearer " + source.Spec.StaticHeaders.Values["token"]},
			ExpiresAt: &expiresAt,
		},
		TTL: defaultTTL,
	}, nil
}

type ttlCountingProvider struct {
	calls int
	ttl   time.Duration
}

func (p *ttlCountingProvider) Resolve(_ context.Context, req *egressauth.ResolveRequest, _ *egressauth.CredentialBinding, source *egressauth.CredentialSourceVersion, defaultTTL time.Duration) (*ResolveResult, error) {
	p.calls++
	ttl := p.ttl
	if ttl <= 0 {
		ttl = defaultTTL
	}
	expiresAt := time.Now().UTC().Add(ttl)
	return &ResolveResult{
		Response: egressauth.NewHTTPHeadersResolveResponse(
			req.AuthRef,
			map[string]string{"Authorization": "Bearer " + source.Spec.StaticHeaders.Values["token"]},
			&expiresAt,
		),
		TTL: ttl,
	}, nil
}

func testStaticSourceVersion(token string) *egressauth.CredentialSourceVersion {
	return &egressauth.CredentialSourceVersion{
		SourceID:     1,
		Version:      1,
		ResolverKind: "static_headers",
		Spec: egressauth.CredentialSourceSecretSpec{
			StaticHeaders: &egressauth.StaticHeadersSourceSpec{
				Values: map[string]string{"token": token},
			},
		},
	}
}

func testStaticTLSClientCertificateSourceVersion(certPEM, keyPEM, caPEM string) *egressauth.CredentialSourceVersion {
	return &egressauth.CredentialSourceVersion{
		SourceID:     1,
		Version:      1,
		ResolverKind: "static_tls_client_certificate",
		Spec: egressauth.CredentialSourceSecretSpec{
			StaticTLSClientCertificate: &egressauth.StaticTLSClientCertificateSourceSpec{
				CertificatePEM: certPEM,
				PrivateKeyPEM:  keyPEM,
				CAPEM:          caPEM,
			},
		},
	}
}

func testStaticUsernamePasswordSourceVersion(username, password string) *egressauth.CredentialSourceVersion {
	return &egressauth.CredentialSourceVersion{
		SourceID:     1,
		Version:      1,
		ResolverKind: "static_username_password",
		Spec: egressauth.CredentialSourceSecretSpec{
			StaticUsernamePassword: &egressauth.StaticUsernamePasswordSourceSpec{
				Username: username,
				Password: password,
			},
		},
	}
}

func testStaticSSHPrivateKeySourceVersion(privateKeyPEM string) *egressauth.CredentialSourceVersion {
	return &egressauth.CredentialSourceVersion{
		SourceID:     1,
		Version:      1,
		ResolverKind: "static_ssh_private_key",
		Spec: egressauth.CredentialSourceSecretSpec{
			StaticSSHPrivateKey: &egressauth.StaticSSHPrivateKeySourceSpec{
				PrivateKeyPEM: privateKeyPEM,
			},
		},
	}
}

func testBindingRecord(updatedAt time.Time) *egressauth.BindingRecord {
	return &egressauth.BindingRecord{
		TeamID:    "team-1",
		SandboxID: "sbx-1",
		UpdatedAt: updatedAt,
		Bindings: []egressauth.CredentialBinding{{
			Ref:           "example-api",
			SourceRef:     "example-source",
			SourceID:      1,
			SourceVersion: 1,
			Projection: egressauth.ProjectionSpec{
				Type: egressauth.CredentialProjectionTypeHTTPHeaders,
			},
		}},
	}
}

func testUsernamePasswordBindingRecord(updatedAt time.Time) *egressauth.BindingRecord {
	return &egressauth.BindingRecord{
		TeamID:    "team-1",
		SandboxID: "sbx-1",
		UpdatedAt: updatedAt,
		Bindings: []egressauth.CredentialBinding{{
			Ref:           "corp-proxy",
			SourceRef:     "corp-proxy-source",
			SourceID:      1,
			SourceVersion: 1,
			Projection: egressauth.ProjectionSpec{
				Type:             egressauth.CredentialProjectionTypeUsernamePassword,
				UsernamePassword: &egressauth.UsernamePasswordProjection{},
			},
		}},
	}
}

func testSSHProxyBindingRecord(updatedAt time.Time) *egressauth.BindingRecord {
	return &egressauth.BindingRecord{
		TeamID:    "team-1",
		SandboxID: "sbx-1",
		UpdatedAt: updatedAt,
		Bindings: []egressauth.CredentialBinding{{
			Ref:           "git-ssh",
			SourceRef:     "git-ssh-source",
			SourceID:      1,
			SourceVersion: 1,
			Projection: egressauth.ProjectionSpec{
				Type: egressauth.CredentialProjectionTypeSSHProxy,
				SSHProxy: &egressauth.SSHProxyProjection{
					SandboxPublicKeys: []string{"ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAID////////////////////////////////////////// fake"},
					UpstreamUsername:  "git",
					KnownHosts:        []string{"github.com ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAID//////////////////////////////////////////"},
				},
			},
		}},
	}
}

func testPlaceholderSubstitutionBindingRecord(updatedAt time.Time) *egressauth.BindingRecord {
	return &egressauth.BindingRecord{
		TeamID:    "team-1",
		SandboxID: "sbx-1",
		UpdatedAt: updatedAt,
		Bindings: []egressauth.CredentialBinding{{
			Ref:           "example-api",
			SourceRef:     "example-source",
			SourceID:      1,
			SourceVersion: 1,
			Projection: egressauth.ProjectionSpec{
				Type: egressauth.CredentialProjectionTypePlaceholderSubstitution,
				PlaceholderSubstitution: &egressauth.PlaceholderSubstitutionProjection{
					Replacements: []egressauth.PlaceholderReplacement{{
						Placeholder:   "s0env_test_token",
						ValueTemplate: "{{ .token }}",
						Locations: []egressauth.PlaceholderSubstitutionLocation{
							egressauth.PlaceholderSubstitutionLocationHeader,
							egressauth.PlaceholderSubstitutionLocationQuery,
							egressauth.PlaceholderSubstitutionLocationBody,
						},
					}},
				},
			},
		}},
	}
}

func TestResolveReloadsBindingProviderResult(t *testing.T) {
	provider := &countingProvider{}
	token := "first"
	store := &fakeBindingStore{
		recordFn: func() *egressauth.BindingRecord {
			return testBindingRecord(time.Unix(10, 0).UTC())
		},
		sourceVersionFn: func(int64, int64) *egressauth.CredentialSourceVersion {
			return testStaticSourceVersion(token)
		},
	}

	service := NewService(Config{
		DefaultResolveTTL: time.Minute,
	}, store, zap.NewNop())
	service.RegisterProvider("static_headers", provider)

	req := &egressauth.ResolveRequest{TeamID: "team-1", SandboxID: "sbx-1", AuthRef: "example-api", Destination: "api.example.com", Protocol: "http"}
	first, err := service.Resolve(context.Background(), req)
	if err != nil {
		t.Fatalf("first resolve: %v", err)
	}
	token = "second"
	second, err := service.Resolve(context.Background(), req)
	if err != nil {
		t.Fatalf("second resolve: %v", err)
	}
	if provider.calls != 2 {
		t.Fatalf("provider calls = %d, want 2", provider.calls)
	}
	if first.Headers["Authorization"] != "Bearer first" || second.Headers["Authorization"] != "Bearer second" {
		t.Fatalf("unexpected headers: first=%v second=%v", first.Headers, second.Headers)
	}
}

func TestResolveUsesUpdatedBindingSourceVersion(t *testing.T) {
	provider := &countingProvider{}
	sourceVersion := int64(1)
	store := &fakeBindingStore{
		recordFn: func() *egressauth.BindingRecord {
			record := testBindingRecord(time.Unix(10, 0).UTC())
			record.Bindings[0].SourceVersion = sourceVersion
			return record
		},
		sourceVersionFn: func(_ int64, version int64) *egressauth.CredentialSourceVersion {
			source := testStaticSourceVersion("old")
			source.Version = version
			if version == 2 {
				source.Spec.StaticHeaders.Values["token"] = "new"
			}
			return source
		},
	}

	service := NewService(Config{
		DefaultResolveTTL: time.Minute,
	}, store, zap.NewNop())
	service.RegisterProvider("static_headers", provider)

	req := &egressauth.ResolveRequest{TeamID: "team-1", SandboxID: "sbx-1", AuthRef: "example-api"}
	if _, err := service.Resolve(context.Background(), req); err != nil {
		t.Fatalf("first resolve: %v", err)
	}
	sourceVersion = 2
	resp, err := service.Resolve(context.Background(), req)
	if err != nil {
		t.Fatalf("second resolve: %v", err)
	}
	if provider.calls != 2 {
		t.Fatalf("provider calls = %d, want 2", provider.calls)
	}
	if got := resp.Headers["Authorization"]; got != "Bearer new" {
		t.Fatalf("Authorization = %q, want Bearer new", got)
	}
}

func TestResolveIncludesSourceMetadata(t *testing.T) {
	store := &fakeBindingStore{
		recordFn: func() *egressauth.BindingRecord {
			record := testBindingRecord(time.Unix(10, 0).UTC())
			record.Bindings[0].SourceVersion = 3
			record.Bindings[0].Projection.HTTPHeaders = &egressauth.HTTPHeadersProjection{
				Headers: []egressauth.ProjectedHeader{{
					Name:          "Authorization",
					ValueTemplate: "Bearer {{ .token }}",
				}},
			}
			return record
		},
		sourceVersionFn: func(sourceID, version int64) *egressauth.CredentialSourceVersion {
			source := testStaticSourceVersion("token")
			source.SourceID = sourceID
			source.TeamID = "team-1"
			source.Version = version
			return source
		},
	}

	service := NewService(Config{DefaultResolveTTL: time.Minute}, store, zap.NewNop())
	resp, err := service.Resolve(context.Background(), &egressauth.ResolveRequest{
		TeamID:    "team-1",
		SandboxID: "sbx-1",
		AuthRef:   "example-api",
	})
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if resp.Source == nil {
		t.Fatal("source metadata is nil")
	}
	if resp.Source.TeamID != "team-1" || resp.Source.SourceRef != "example-source" || resp.Source.SourceID != 1 || resp.Source.SourceVersion != 3 {
		t.Fatalf("source metadata = %#v", resp.Source)
	}
}

func TestResolveReturnsPlaceholderSubstitutionDirective(t *testing.T) {
	store := &fakeBindingStore{
		recordFn: func() *egressauth.BindingRecord {
			return testPlaceholderSubstitutionBindingRecord(time.Unix(10, 0).UTC())
		},
		sourceVersionFn: func(int64, int64) *egressauth.CredentialSourceVersion {
			return testStaticSourceVersion("resolved-secret")
		},
	}

	service := NewService(Config{
		DefaultResolveTTL: time.Minute,
	}, store, zap.NewNop())

	resp, err := service.Resolve(context.Background(), &egressauth.ResolveRequest{
		TeamID:    "team-1",
		SandboxID: "sbx-1",
		AuthRef:   "example-api",
		Protocol:  "http",
	})
	if err != nil {
		t.Fatalf("resolve placeholder substitution: %v", err)
	}
	if len(resp.Headers) != 0 {
		t.Fatalf("legacy headers = %#v, want none", resp.Headers)
	}
	if len(resp.Directives) != 1 || resp.Directives[0].Kind != egressauth.ResolveDirectiveKindPlaceholderSubstitution {
		t.Fatalf("unexpected directives: %#v", resp.Directives)
	}
	directive := resp.Directives[0].PlaceholderSubstitution
	if directive == nil || len(directive.Replacements) != 1 {
		t.Fatalf("unexpected placeholder directive: %#v", directive)
	}
	replacement := directive.Replacements[0]
	if replacement.Placeholder != "s0env_test_token" {
		t.Fatalf("placeholder = %q", replacement.Placeholder)
	}
	if replacement.Value != "resolved-secret" {
		t.Fatalf("value = %q", replacement.Value)
	}
	if len(replacement.Locations) != 3 {
		t.Fatalf("locations = %#v", replacement.Locations)
	}
}

func TestResolveFallsBackToStaticAuth(t *testing.T) {
	service := NewService(Config{
		StaticAuth: []StaticAuthConfig{{
			AuthRef: "example-api",
			Headers: map[string]string{"Authorization": "Bearer static"},
			TTL:     time.Minute,
		}},
	}, nil, zap.NewNop())

	resp, err := service.Resolve(context.Background(), &egressauth.ResolveRequest{AuthRef: "example-api"})
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if got := resp.Headers["Authorization"]; got != "Bearer static" {
		t.Fatalf("authorization header = %q", got)
	}
}

func TestResolveReturnsProviderExpiresAt(t *testing.T) {
	provider := &ttlCountingProvider{ttl: 90 * time.Second}
	store := &fakeBindingStore{
		recordFn: func() *egressauth.BindingRecord {
			return testBindingRecord(time.Unix(10, 0).UTC())
		},
		sourceVersionFn: func(int64, int64) *egressauth.CredentialSourceVersion {
			return testStaticSourceVersion("binding")
		},
	}

	service := NewService(Config{
		DefaultResolveTTL: time.Minute,
	}, store, zap.NewNop())
	service.RegisterProvider("static_headers", provider)

	req := &egressauth.ResolveRequest{TeamID: "team-1", SandboxID: "sbx-1", AuthRef: "example-api", Destination: "api.example.com", Protocol: "http"}
	start := time.Now().UTC()
	resp, err := service.Resolve(context.Background(), req)
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if resp.ExpiresAt == nil {
		t.Fatal("expiresAt is nil")
	}
	if resp.ExpiresAt.Before(start.Add(89*time.Second)) || resp.ExpiresAt.After(start.Add(91*time.Second)) {
		t.Fatalf("expiresAt = %s, want approximately 90s from %s", resp.ExpiresAt, start)
	}
	if _, err := service.Resolve(context.Background(), req); err != nil {
		t.Fatalf("second resolve: %v", err)
	}
	if provider.calls != 2 {
		t.Fatalf("provider calls = %d, want 2", provider.calls)
	}
}

func TestResolveReturnsNotFoundWhenAuthRefMissing(t *testing.T) {
	service := NewService(Config{}, nil, zap.NewNop())
	_, err := service.Resolve(context.Background(), &egressauth.ResolveRequest{AuthRef: "missing"})
	if !errors.Is(err, ErrAuthRefNotFound) {
		t.Fatalf("err = %v, want ErrAuthRefNotFound", err)
	}
}

func TestResolveReturnsUsernamePasswordDirective(t *testing.T) {
	store := &fakeBindingStore{
		recordFn: func() *egressauth.BindingRecord {
			return testUsernamePasswordBindingRecord(time.Unix(10, 0).UTC())
		},
		sourceVersionFn: func(int64, int64) *egressauth.CredentialSourceVersion {
			return testStaticUsernamePasswordSourceVersion("alice", "secret")
		},
	}

	service := NewService(Config{
		DefaultResolveTTL: time.Minute,
	}, store, zap.NewNop())

	resp, err := service.Resolve(context.Background(), &egressauth.ResolveRequest{TeamID: "team-1", SandboxID: "sbx-1", AuthRef: "corp-proxy", Protocol: "socks5"})
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if len(resp.Directives) != 1 || resp.Directives[0].UsernamePassword == nil {
		t.Fatalf("unexpected directives: %#v", resp.Directives)
	}
	if resp.Directives[0].UsernamePassword.Username != "alice" {
		t.Fatalf("username = %q, want alice", resp.Directives[0].UsernamePassword.Username)
	}
}

func TestResolveUsesStaticTLSClientCertificateProvider(t *testing.T) {
	certPEM, keyPEM, err := testTLSKeyPair(t)
	if err != nil {
		t.Fatalf("test tls keypair: %v", err)
	}
	store := &fakeBindingStore{
		recordFn: func() *egressauth.BindingRecord {
			record := testBindingRecord(time.Unix(10, 0).UTC())
			record.Bindings[0].Projection = egressauth.ProjectionSpec{
				Type:                 egressauth.CredentialProjectionTypeTLSClientCertificate,
				TLSClientCertificate: &egressauth.TLSClientCertificateProjection{},
			}
			return record
		},
		sourceVersionFn: func(int64, int64) *egressauth.CredentialSourceVersion {
			return testStaticTLSClientCertificateSourceVersion(certPEM, keyPEM, "")
		},
	}

	service := NewService(Config{
		DefaultResolveTTL: time.Minute,
	}, store, zap.NewNop())

	resp, err := service.Resolve(context.Background(), &egressauth.ResolveRequest{
		TeamID:    "team-1",
		SandboxID: "sbx-1",
		AuthRef:   "example-api",
		Protocol:  "tls",
	})
	if err != nil {
		t.Fatalf("resolve tls client certificate: %v", err)
	}
	if len(resp.Directives) != 1 || resp.Directives[0].Kind != egressauth.ResolveDirectiveKindTLSClientCertificate {
		t.Fatalf("unexpected directives: %#v", resp.Directives)
	}
	if resp.Directives[0].TLSClientCertificate == nil || resp.Directives[0].TLSClientCertificate.CertificatePEM == "" {
		t.Fatalf("expected tls client certificate payload, got %#v", resp.Directives[0].TLSClientCertificate)
	}
}

func TestResolveReturnsSSHProxyDirective(t *testing.T) {
	privateKeyPEM, err := testSSHPrivateKeyPEM()
	if err != nil {
		t.Fatalf("test ssh private key: %v", err)
	}
	store := &fakeBindingStore{
		recordFn: func() *egressauth.BindingRecord {
			return testSSHProxyBindingRecord(time.Unix(10, 0).UTC())
		},
		sourceVersionFn: func(int64, int64) *egressauth.CredentialSourceVersion {
			return testStaticSSHPrivateKeySourceVersion(privateKeyPEM)
		},
	}

	service := NewService(Config{
		DefaultResolveTTL: time.Minute,
	}, store, zap.NewNop())

	resp, err := service.Resolve(context.Background(), &egressauth.ResolveRequest{
		TeamID:    "team-1",
		SandboxID: "sbx-1",
		AuthRef:   "git-ssh",
		Protocol:  "ssh",
	})
	if err != nil {
		t.Fatalf("resolve ssh proxy: %v", err)
	}
	if len(resp.Directives) != 1 || resp.Directives[0].Kind != egressauth.ResolveDirectiveKindSSHProxy {
		t.Fatalf("unexpected directives: %#v", resp.Directives)
	}
	directive := resp.Directives[0].SSHProxy
	if directive == nil {
		t.Fatal("expected ssh proxy directive")
	}
	if directive.UpstreamUsername != "git" {
		t.Fatalf("upstream username = %q, want git", directive.UpstreamUsername)
	}
	if directive.PrivateKeyPEM == "" {
		t.Fatal("expected private key pem")
	}
	if len(directive.SandboxPublicKeys) != 1 {
		t.Fatalf("sandbox public keys = %d, want 1", len(directive.SandboxPublicKeys))
	}
}

func testSSHPrivateKeyPEM() (string, error) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return "", err
	}
	return string(pem.EncodeToMemory(&pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(key),
	})), nil
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
