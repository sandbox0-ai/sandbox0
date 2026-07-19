package http

import (
	nethttp "net/http"
	"net/http/httptest"
	"testing"

	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	coreteamquota "github.com/sandbox0-ai/sandbox0/pkg/teamquota"
	"github.com/sandbox0-ai/sandbox0/pkg/teamquota/guard"
)

func TestForwardedQuotaAdmissionProofRequiresExactRegionalRequest(t *testing.T) {
	request := httptest.NewRequest(
		nethttp.MethodPost,
		"/api/v1/sandboxes?source=edge",
		nil,
	)
	proof, err := internalauth.NewQuotaAdmissionProof(
		internalauth.QuotaAdmissionClassEdgeAdmitted,
		request,
		"team-a",
		"operation-a",
		"request-a",
		internalauth.ServiceRegionalGateway,
		[]coreteamquota.Key{coreteamquota.KeyAPIRequests},
		guard.Version{EnforcementEpoch: 1, RedisGeneration: 1},
	)
	if err != nil {
		t.Fatalf("NewQuotaAdmissionProof() error = %v", err)
	}
	newClaims := func() *internalauth.Claims {
		copyProof := *proof
		copyProof.Keys = append(
			[]coreteamquota.Key(nil),
			proof.Keys...,
		)
		return &internalauth.Claims{
			Caller: internalauth.ServiceRegionalGateway,
			TeamID: "team-a",
			Audit: &internalauth.AuditContext{
				OperationID: "operation-a",
				RequestID:   "request-a",
				Origin:      internalauth.ServiceRegionalGateway,
			},
			QuotaAdmissionProof: &copyProof,
		}
	}
	tests := []struct {
		name    string
		request *nethttp.Request
		mutate  func(*internalauth.Claims)
		want    bool
	}{
		{
			name:    "exact proof",
			request: request,
			want:    true,
		},
		{
			name: "query changed",
			request: httptest.NewRequest(
				nethttp.MethodPost,
				"/api/v1/sandboxes?source=scheduler",
				nil,
			),
		},
		{
			name:    "caller is not regional",
			request: request,
			mutate: func(claims *internalauth.Claims) {
				claims.Caller = internalauth.ServiceManager
			},
		},
		{
			name: "path changed",
			request: httptest.NewRequest(
				nethttp.MethodPost,
				"/api/v1/templates",
				nil,
			),
		},
		{
			name:    "request ID changed",
			request: request,
			mutate: func(claims *internalauth.Claims) {
				claims.Audit.RequestID = "request-b"
			},
		},
		{
			name:    "system proof",
			request: request,
			mutate: func(claims *internalauth.Claims) {
				claims.IsSystem = true
				claims.QuotaAdmissionProof.Class =
					internalauth.QuotaAdmissionClassSystem
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			claims := newClaims()
			if test.mutate != nil {
				test.mutate(claims)
			}
			got := forwardedQuotaAdmissionProof(claims, test.request) != nil
			if got != test.want {
				t.Fatalf("forwarded proof = %t, want %t", got, test.want)
			}
		})
	}
}
