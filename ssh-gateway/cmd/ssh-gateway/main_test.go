package main

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/pkg/teamquota"
)

func TestNewMetricsServerExposesDefaultCollectors(t *testing.T) {
	server := newMetricsServer()
	wantAddr := fmt.Sprintf(":%d", config.DefaultSSHGatewayMetricsPort)
	if server.Addr != wantAddr {
		t.Fatalf("metrics server address = %q, want %q", server.Addr, wantAddr)
	}

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	server.Handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("GET /metrics status = %d, want %d", recorder.Code, http.StatusOK)
	}
	body := recorder.Body.String()
	for _, metric := range []string{"go_goroutines", "process_cpu_seconds_total"} {
		if !strings.Contains(body, metric) {
			t.Fatalf("GET /metrics response missing %q", metric)
		}
	}
}

func TestInitTeamQuotaRequiresRegionSharedRuntime(t *testing.T) {
	resolver := staticTeamQuotaResolver{}
	tests := []struct {
		name string
		cfg  *config.SSHGatewayConfig
		want string
	}{
		{
			name: "missing region",
			cfg:  &config.SSHGatewayConfig{},
			want: "region ID is required",
		},
		{
			name: "missing Redis",
			cfg: &config.SSHGatewayConfig{
				RegionID: "region-1",
			},
			want: "region-shared Redis URL is required",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			active, network, err := initTeamQuota(
				context.Background(),
				tt.cfg,
				resolver,
			)
			if active != nil || network != nil {
				t.Fatalf(
					"initTeamQuota() = active %#v network %#v, want nil",
					active,
					network,
				)
			}
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf(
					"initTeamQuota() error = %v, want %q",
					err,
					tt.want,
				)
			}
		})
	}
}

type staticTeamQuotaResolver struct{}

func (staticTeamQuotaResolver) EffectivePolicy(
	context.Context,
	string,
	teamquota.Key,
) (*teamquota.Policy, error) {
	return nil, nil
}

func (staticTeamQuotaResolver) TeamAdmissionDisabled(
	context.Context,
	string,
) (bool, error) {
	return false, nil
}
