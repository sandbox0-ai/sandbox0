package utils

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/sandbox0-ai/sandbox0/pkg/apispec"
	gatewayspec "github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
)

func TestOverrideTeamQuotaRestoresOriginalPolicySource(t *testing.T) {
	for _, test := range []struct {
		name              string
		source            apispec.TeamQuotaPolicySource
		wantRestoreMethod string
		wantWrites        int
	}{
		{
			name:              "inherited default",
			source:            apispec.TeamQuotaPolicySourceDefault,
			wantRestoreMethod: http.MethodDelete,
			wantWrites:        1,
		},
		{
			name:              "explicit override",
			source:            apispec.TeamQuotaPolicySourceOverride,
			wantRestoreMethod: http.MethodPut,
			wantWrites:        2,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			originalLimit := int64(100)
			var methods []string
			var writes []apispec.TeamQuotaPolicyWriteRequest
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
				if request.Header.Get("Authorization") != "Bearer token" {
					t.Fatalf("Authorization = %q", request.Header.Get("Authorization"))
				}
				if request.Header.Get(internalauth.TeamIDHeader) != "team-a" {
					t.Fatalf("%s = %q", internalauth.TeamIDHeader, request.Header.Get(internalauth.TeamIDHeader))
				}
				methods = append(methods, request.Method)
				switch request.Method {
				case http.MethodGet:
					if request.URL.Path != "/api/v1/teams/team-a/quotas" {
						t.Fatalf("GET path = %q", request.URL.Path)
					}
					_ = gatewayspec.WriteSuccess(w, http.StatusOK, apispec.TeamQuotaList{
						TeamId: "team-a",
						Quotas: []apispec.TeamQuotaStatus{{
							TeamId: "team-a",
							Key:    apispec.SandboxRuntimeCount,
							Kind:   apispec.TeamQuotaKindCapacity,
							Source: test.source,
							Policy: apispec.TeamQuotaPolicy{
								TeamId: "team-a",
								Key:    apispec.SandboxRuntimeCount,
								Kind:   apispec.TeamQuotaKindCapacity,
								Limit:  &originalLimit,
							},
						}},
					})
				case http.MethodPut:
					if request.URL.Path != "/api/v1/teams/team-a/quotas/sandbox_runtime_count" {
						t.Fatalf("PUT path = %q", request.URL.Path)
					}
					var write apispec.TeamQuotaPolicyWriteRequest
					if err := json.NewDecoder(request.Body).Decode(&write); err != nil {
						t.Fatalf("decode write: %v", err)
					}
					writes = append(writes, write)
					capacity, err := write.AsTeamQuotaCapacityPolicyWriteRequest()
					if err != nil {
						t.Fatalf("decode capacity write: %v", err)
					}
					_ = gatewayspec.WriteSuccess(w, http.StatusOK, apispec.TeamQuotaPolicy{
						TeamId: "team-a",
						Key:    apispec.SandboxRuntimeCount,
						Kind:   apispec.TeamQuotaKindCapacity,
						Limit:  &capacity.Limit,
					})
				case http.MethodDelete:
					if request.URL.Path != "/api/v1/teams/team-a/quotas/sandbox_runtime_count" {
						t.Fatalf("DELETE path = %q", request.URL.Path)
					}
					_ = gatewayspec.WriteSuccess(w, http.StatusOK, map[string]string{"message": "deleted"})
				default:
					t.Fatalf("unexpected method %s", request.Method)
				}
			}))
			defer server.Close()

			session := &Session{
				baseURL: server.URL,
				token:   "token",
				teamID:  "team-a",
				client:  server.Client(),
			}
			restore, status, err := session.OverrideTeamQuota(
				context.Background(),
				apispec.SandboxRuntimeCount,
				CapacityTeamQuotaPolicy(0),
			)
			if err != nil {
				t.Fatalf("OverrideTeamQuota() error = %v", err)
			}
			if status != http.StatusOK {
				t.Fatalf("status = %d, want 200", status)
			}
			if err := restore(context.Background()); err != nil {
				t.Fatalf("restore() error = %v", err)
			}
			if len(methods) != 3 || methods[2] != test.wantRestoreMethod {
				t.Fatalf("request methods = %v, want restore %s", methods, test.wantRestoreMethod)
			}
			if len(writes) != test.wantWrites {
				t.Fatalf("writes = %d, want %d", len(writes), test.wantWrites)
			}
			temporary, err := writes[0].AsTeamQuotaCapacityPolicyWriteRequest()
			if err != nil || temporary.Limit != 0 {
				t.Fatalf("temporary write = %#v, err = %v", writes[0], err)
			}
			if test.source == apispec.TeamQuotaPolicySourceOverride {
				restored, err := writes[1].AsTeamQuotaCapacityPolicyWriteRequest()
				if err != nil || restored.Limit != originalLimit {
					t.Fatalf("restore write = %#v, err = %v", writes[1], err)
				}
			}
		})
	}
}
