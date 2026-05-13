package service

import "testing"

func TestPublicGatewayConfigToSandboxAppServicesGroupsRoutesByPort(t *testing.T) {
	cfg := &PublicGatewayConfig{
		Enabled: true,
		Routes: []PublicGatewayRoute{
			{ID: "api", Port: 8080, PathPrefix: "/api", Resume: true},
			{ID: "admin", Port: 8080, PathPrefix: "/admin", Resume: false},
			{ID: "metrics", Port: 9090, PathPrefix: "/metrics", Resume: false},
		},
	}

	services, err := PublicGatewayConfigToSandboxAppServices(cfg)
	if err != nil {
		t.Fatalf("convert public gateway: %v", err)
	}
	if len(services) != 2 {
		t.Fatalf("services len = %d, want 2", len(services))
	}
	if services[0].ID != "p8080" || services[0].Port != 8080 || len(services[0].Ingress.Routes) != 2 {
		t.Fatalf("unexpected first service: %#v", services[0])
	}
	if services[1].ID != "metrics" || services[1].Port != 9090 || len(services[1].Ingress.Routes) != 1 {
		t.Fatalf("unexpected second service: %#v", services[1])
	}
}

func TestSandboxAppServicePublishBlockersRequirePublicRestartableRuntime(t *testing.T) {
	service := SandboxAppService{
		ID:   "api",
		Port: 8080,
		Ingress: SandboxAppServiceIngress{
			Public: true,
			Routes: []SandboxAppServiceRoute{{ID: "api", PathPrefix: "/", Resume: true}},
		},
	}

	blockers := SandboxAppServicePublishBlockers(service)
	if len(blockers) != 1 || blockers[0] != "missing_runtime" {
		t.Fatalf("blockers = %#v, want missing_runtime", blockers)
	}

	service.Runtime = &SandboxAppServiceRuntime{
		Type:    SandboxAppServiceRuntimeCMD,
		Command: []string{"/bin/sh", "-lc", "python -m app.server"},
	}
	if blockers := SandboxAppServicePublishBlockers(service); len(blockers) != 0 {
		t.Fatalf("blockers = %#v, want none", blockers)
	}
}
