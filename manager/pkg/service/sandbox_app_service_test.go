package service

import "testing"

func TestNormalizeSandboxAppServicesCanonicalizesRoutes(t *testing.T) {
	rewrite := "v1"
	services, err := NormalizeSandboxAppServices([]SandboxAppService{{
		ID:   "API",
		Port: 8080,
		Ingress: SandboxAppServiceIngress{
			Public: true,
			Routes: []SandboxAppServiceRoute{{
				ID:            "Users",
				PathPrefix:    "api",
				Methods:       []string{"get", "GET"},
				RewritePrefix: &rewrite,
			}},
		},
	}})
	if err != nil {
		t.Fatalf("NormalizeSandboxAppServices: %v", err)
	}
	if len(services) != 1 {
		t.Fatalf("services len = %d, want 1", len(services))
	}
	if services[0].ID != "api" || services[0].Ingress.Routes[0].ID != "users" {
		t.Fatalf("unexpected service ids: %#v", services[0])
	}
	route := services[0].Ingress.Routes[0]
	if route.PathPrefix != "/api" || *route.RewritePrefix != "/v1" {
		t.Fatalf("unexpected prefixes: %#v", route)
	}
	if len(route.Methods) != 1 || route.Methods[0] != "GET" {
		t.Fatalf("methods = %#v, want GET", route.Methods)
	}
}

func TestSandboxAppServiceViewsReturnsEmptySlice(t *testing.T) {
	views := SandboxAppServiceViews(nil)
	if views == nil {
		t.Fatal("SandboxAppServiceViews(nil) = nil, want empty slice")
	}
	if len(views) != 0 {
		t.Fatalf("SandboxAppServiceViews(nil) length = %d, want 0", len(views))
	}
}

func TestSandboxAppServiceViewsForExposureAddsPublicURL(t *testing.T) {
	views := SandboxAppServiceViewsForExposure("rs-default-api-abcde", "us.sandbox0.app", []SandboxAppService{
		{
			ID:   "api",
			Port: 8080,
			Ingress: SandboxAppServiceIngress{
				Public: true,
				Routes: []SandboxAppServiceRoute{{ID: "api", PathPrefix: "/", Resume: true}},
			},
		},
		{
			ID:   "worker",
			Port: 9000,
			Ingress: SandboxAppServiceIngress{
				Public: false,
			},
		},
	})
	if len(views) != 2 {
		t.Fatalf("views length = %d, want 2", len(views))
	}
	if got, want := views[0].PublicURL, "https://rs-default-api-abcde--p8080.us.sandbox0.app"; got != want {
		t.Fatalf("PublicURL = %q, want %q", got, want)
	}
	if views[1].PublicURL != "" {
		t.Fatalf("private service PublicURL = %q, want empty", views[1].PublicURL)
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

	service.Runtime = &SandboxAppServiceRuntime{Type: SandboxAppServiceRuntimeWarmProcess}
	blockers = SandboxAppServicePublishBlockers(service)
	if len(blockers) != 1 || blockers[0] != "missing_warm_process_name" {
		t.Fatalf("blockers = %#v, want missing_warm_process_name", blockers)
	}

	service.Runtime.WarmProcessName = "python"
	if blockers := SandboxAppServicePublishBlockers(service); len(blockers) != 0 {
		t.Fatalf("blockers = %#v, want none", blockers)
	}
}
