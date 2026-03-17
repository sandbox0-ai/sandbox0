package http

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/pkg/egressauth"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"go.uber.org/zap"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type fakeBindingStore struct {
	record *egressauth.BindingRecord
}

func (f *fakeBindingStore) GetBindings(_ context.Context, _, _ string) (*egressauth.BindingRecord, error) {
	return f.record, nil
}

func (f *fakeBindingStore) UpsertBindings(context.Context, *egressauth.BindingRecord) error {
	return nil
}

func (f *fakeBindingStore) DeleteBindings(context.Context, string, string) error {
	return nil
}

func TestHandleResolveReturnsStaticAuthDirective(t *testing.T) {
	server := NewServer(&config.EgressBrokerConfig{
		StaticAuth: []config.StaticEgressAuthConfig{
			{
				AuthRef: "example-api",
				Headers: map[string]string{"Authorization": "Bearer test-token"},
				TTL:     metav1.Duration{Duration: time.Minute},
			},
		},
	}, zap.NewNop(), nil)

	body, err := json.Marshal(&egressauth.ResolveRequest{
		SandboxID:   "sbx_123",
		TeamID:      "team_123",
		AuthRef:     "example-api",
		Destination: "api.example.com",
		Protocol:    "http",
	})
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, "/api/v1/resolve", bytes.NewReader(body))
	rec := httptest.NewRecorder()

	server.handleResolve(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
	resp, apiErr, err := spec.DecodeResponse[egressauth.ResolveResponse](rec.Body)
	if err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if apiErr != nil {
		t.Fatalf("unexpected api error: %+v", apiErr)
	}
	if resp == nil || resp.AuthRef != "example-api" {
		t.Fatalf("unexpected response: %+v", resp)
	}
	if got := resp.Headers["Authorization"]; got != "Bearer test-token" {
		t.Fatalf("authorization header = %q", got)
	}
	if resp.ExpiresAt == nil {
		t.Fatal("expected expiresAt")
	}
}

func TestHandleResolveReturnsNotFoundForUnknownAuthRef(t *testing.T) {
	server := NewServer(&config.EgressBrokerConfig{}, zap.NewNop(), nil)
	body, err := json.Marshal(&egressauth.ResolveRequest{AuthRef: "missing"})
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, "/api/v1/resolve", bytes.NewReader(body))
	rec := httptest.NewRecorder()

	server.handleResolve(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusNotFound)
	}
}

func TestHandleResolvePrefersBindingStore(t *testing.T) {
	server := NewServer(&config.EgressBrokerConfig{
		ClusterID:          "cluster-a",
		DefaultResolveTTL:  metav1.Duration{Duration: time.Minute},
		StaticAuth: []config.StaticEgressAuthConfig{{
			AuthRef: "example-api",
			Headers: map[string]string{"Authorization": "Bearer static"},
			TTL:     metav1.Duration{Duration: time.Minute},
		}},
	}, zap.NewNop(), &fakeBindingStore{
		record: &egressauth.BindingRecord{
			ClusterID: "cluster-a",
			SandboxID: "sbx_123",
			Bindings: []egressauth.CredentialBinding{{
				Ref:      "example-api",
				Provider: "static",
				Headers:  map[string]string{"Authorization": "Bearer store"},
			}},
		},
	})

	body, err := json.Marshal(&egressauth.ResolveRequest{
		SandboxID: "sbx_123",
		AuthRef:   "example-api",
	})
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, "/api/v1/resolve", bytes.NewReader(body))
	rec := httptest.NewRecorder()

	server.handleResolve(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
	resp, apiErr, err := spec.DecodeResponse[egressauth.ResolveResponse](rec.Body)
	if err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if apiErr != nil {
		t.Fatalf("unexpected api error: %+v", apiErr)
	}
	if got := resp.Headers["Authorization"]; got != "Bearer store" {
		t.Fatalf("authorization header = %q, want store value", got)
	}
}
