package resolver

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/pkg/egressauth"
	"go.uber.org/zap"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
		Spec: egressauth.CredentialSourceSpec{
			StaticHeaders: &egressauth.StaticHeadersSourceSpec{
				Values: map[string]string{"token": token},
			},
		},
	}
}

func testBindingRecord(updatedAt time.Time) *egressauth.BindingRecord {
	return &egressauth.BindingRecord{
		ClusterID: "cluster-a",
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

func TestResolveUsesBindingProviderAndCache(t *testing.T) {
	provider := &countingProvider{}
	store := &fakeBindingStore{
		recordFn: func() *egressauth.BindingRecord {
			return testBindingRecord(time.Unix(10, 0).UTC())
		},
		sourceVersionFn: func(int64, int64) *egressauth.CredentialSourceVersion {
			return testStaticSourceVersion("binding")
		},
	}

	service := NewService(&config.EgressBrokerConfig{
		ClusterID:         "cluster-a",
		DefaultResolveTTL: metav1.Duration{Duration: time.Minute},
	}, store, zap.NewNop())
	service.RegisterProvider("static_headers", provider)

	req := &egressauth.ResolveRequest{SandboxID: "sbx-1", AuthRef: "example-api", Destination: "api.example.com", Protocol: "http"}
	first, err := service.Resolve(context.Background(), req)
	if err != nil {
		t.Fatalf("first resolve: %v", err)
	}
	second, err := service.Resolve(context.Background(), req)
	if err != nil {
		t.Fatalf("second resolve: %v", err)
	}
	if provider.calls != 1 {
		t.Fatalf("provider calls = %d, want 1", provider.calls)
	}
	if first.Headers["Authorization"] != "Bearer binding" || second.Headers["Authorization"] != "Bearer binding" {
		t.Fatalf("unexpected headers: first=%v second=%v", first.Headers, second.Headers)
	}
}

func TestResolveInvalidatesCacheWhenBindingsRevisionChanges(t *testing.T) {
	provider := &countingProvider{}
	updatedAt := time.Unix(10, 0).UTC()
	store := &fakeBindingStore{
		recordFn: func() *egressauth.BindingRecord {
			return testBindingRecord(updatedAt)
		},
		sourceVersionFn: func(int64, int64) *egressauth.CredentialSourceVersion {
			return testStaticSourceVersion("binding")
		},
	}

	service := NewService(&config.EgressBrokerConfig{
		ClusterID:         "cluster-a",
		DefaultResolveTTL: metav1.Duration{Duration: time.Minute},
	}, store, zap.NewNop())
	service.RegisterProvider("static_headers", provider)

	req := &egressauth.ResolveRequest{SandboxID: "sbx-1", AuthRef: "example-api"}
	if _, err := service.Resolve(context.Background(), req); err != nil {
		t.Fatalf("first resolve: %v", err)
	}
	updatedAt = updatedAt.Add(time.Second)
	if _, err := service.Resolve(context.Background(), req); err != nil {
		t.Fatalf("second resolve: %v", err)
	}
	if provider.calls != 2 {
		t.Fatalf("provider calls = %d, want 2", provider.calls)
	}
}

func TestResolveFallsBackToStaticAuth(t *testing.T) {
	service := NewService(&config.EgressBrokerConfig{
		StaticAuth: []config.StaticEgressAuthConfig{{
			AuthRef: "example-api",
			Headers: map[string]string{"Authorization": "Bearer static"},
			TTL:     metav1.Duration{Duration: time.Minute},
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

func TestResolveRefreshesAfterCacheTTLExpires(t *testing.T) {
	provider := &ttlCountingProvider{ttl: 15 * time.Millisecond}
	store := &fakeBindingStore{
		recordFn: func() *egressauth.BindingRecord {
			return testBindingRecord(time.Unix(10, 0).UTC())
		},
		sourceVersionFn: func(int64, int64) *egressauth.CredentialSourceVersion {
			return testStaticSourceVersion("binding")
		},
	}

	service := NewService(&config.EgressBrokerConfig{
		ClusterID:         "cluster-a",
		DefaultResolveTTL: metav1.Duration{Duration: time.Minute},
	}, store, zap.NewNop())
	service.RegisterProvider("static_headers", provider)

	req := &egressauth.ResolveRequest{SandboxID: "sbx-1", AuthRef: "example-api", Destination: "api.example.com", Protocol: "http"}
	_, err := service.Resolve(context.Background(), req)
	if err != nil {
		t.Fatalf("first resolve: %v", err)
	}
	_, err = service.Resolve(context.Background(), req)
	if err != nil {
		t.Fatalf("second resolve: %v", err)
	}
	if provider.calls != 1 {
		t.Fatalf("provider calls before expiry = %d, want 1", provider.calls)
	}

	time.Sleep(25 * time.Millisecond)

	_, err = service.Resolve(context.Background(), req)
	if err != nil {
		t.Fatalf("third resolve after expiry: %v", err)
	}
	if provider.calls != 2 {
		t.Fatalf("provider calls after expiry = %d, want 2", provider.calls)
	}
}

func TestResolveReturnsNotFoundWhenAuthRefMissing(t *testing.T) {
	service := NewService(&config.EgressBrokerConfig{}, nil, zap.NewNop())
	_, err := service.Resolve(context.Background(), &egressauth.ResolveRequest{AuthRef: "missing"})
	if !errors.Is(err, ErrAuthRefNotFound) {
		t.Fatalf("err = %v, want ErrAuthRefNotFound", err)
	}
}
