package service

import (
	"encoding/json"
	"strings"
	"testing"
)

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

	service.Runtime = &SandboxAppServiceRuntime{Type: SandboxAppServiceRuntimeManual}
	blockers = SandboxAppServicePublishBlockers(service)
	if len(blockers) != 1 || blockers[0] != "manual_runtime" {
		t.Fatalf("blockers = %#v, want manual_runtime", blockers)
	}
}

func TestNormalizeSandboxAppServicesRejectsResumeRouteWithoutRestartableRuntime(t *testing.T) {
	tests := []struct {
		name    string
		runtime *SandboxAppServiceRuntime
	}{
		{
			name: "missing runtime",
		},
		{
			name:    "manual runtime",
			runtime: &SandboxAppServiceRuntime{Type: SandboxAppServiceRuntimeManual},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NormalizeSandboxAppServices([]SandboxAppService{{
				ID:      "api",
				Port:    8080,
				Runtime: tt.runtime,
				Ingress: SandboxAppServiceIngress{
					Public: true,
					Routes: []SandboxAppServiceRoute{{
						ID:         "api",
						PathPrefix: "/",
						Resume:     true,
					}},
				},
			}})
			if err == nil {
				t.Fatal("NormalizeSandboxAppServices succeeded, want resume runtime error")
			}
			if !strings.Contains(err.Error(), "resume requires runtime.type cmd, function, or nextjs") {
				t.Fatalf("error = %q, want resume runtime message", err.Error())
			}
		})
	}
}

func TestNormalizeSandboxAppServicesSupportsNextJSRuntime(t *testing.T) {
	services, err := NormalizeSandboxAppServices([]SandboxAppService{{
		ID:   "web",
		Port: 3000,
		Runtime: &SandboxAppServiceRuntime{
			Type:    SandboxAppServiceRuntimeNextJS,
			Command: []string{"npm", "run", "dev"},
			CWD:     "/workspace/app",
			Function: &SandboxFunction{
				Source: SandboxFunctionSource{Code: "def handler(request):\n    return None\n"},
			},
		},
		Ingress: SandboxAppServiceIngress{
			Public: true,
			Routes: []SandboxAppServiceRoute{{
				ID:         "web",
				PathPrefix: "/",
				Resume:     true,
			}},
		},
	}})
	if err != nil {
		t.Fatalf("NormalizeSandboxAppServices: %v", err)
	}
	service := services[0]
	if service.Runtime.Type != SandboxAppServiceRuntimeNextJS {
		t.Fatalf("runtime type = %q, want nextjs", service.Runtime.Type)
	}
	if service.Runtime.CWD != "/workspace/app" {
		t.Fatalf("nextjs runtime cwd = %q, want /workspace/app", service.Runtime.CWD)
	}
	if len(service.Runtime.Command) != 0 {
		t.Fatalf("nextjs runtime kept command = %#v", service.Runtime.Command)
	}
	if service.Runtime.Function != nil {
		t.Fatalf("nextjs runtime kept function = %#v", service.Runtime.Function)
	}
	if !SandboxAppServiceHasRestartableRuntime(service) {
		t.Fatal("nextjs runtime should be restartable")
	}
	if blockers := SandboxAppServicePublishBlockers(service); len(blockers) != 0 {
		t.Fatalf("blockers = %#v, want none", blockers)
	}
}

func TestNormalizeSandboxAppServicesAllowsManualRouteWithoutResume(t *testing.T) {
	services, err := NormalizeSandboxAppServices([]SandboxAppService{{
		ID:   "api",
		Port: 8080,
		Ingress: SandboxAppServiceIngress{
			Public: true,
			Routes: []SandboxAppServiceRoute{{
				ID:         "api",
				PathPrefix: "/",
			}},
		},
	}})
	if err != nil {
		t.Fatalf("NormalizeSandboxAppServices: %v", err)
	}
	if services[0].Runtime != nil {
		t.Fatalf("runtime = %#v, want nil manual service", services[0].Runtime)
	}
}

func TestNormalizeSandboxAppServicesAllowsResumeRouteWithCMDRuntime(t *testing.T) {
	_, err := NormalizeSandboxAppServices([]SandboxAppService{{
		ID:   "api",
		Port: 8080,
		Runtime: &SandboxAppServiceRuntime{
			Type:    SandboxAppServiceRuntimeCMD,
			Command: []string{"python3", "-m", "http.server", "8080"},
		},
		Ingress: SandboxAppServiceIngress{
			Public: true,
			Routes: []SandboxAppServiceRoute{{
				ID:         "api",
				PathPrefix: "/",
				Resume:     true,
			}},
		},
	}})
	if err != nil {
		t.Fatalf("NormalizeSandboxAppServices: %v", err)
	}
}

func TestNormalizeSandboxAppServicesSupportsFunctionRuntime(t *testing.T) {
	services, err := NormalizeSandboxAppServices([]SandboxAppService{{
		ID: "webhook",
		Runtime: &SandboxAppServiceRuntime{
			Type: SandboxAppServiceRuntimeFunction,
			Function: &SandboxFunction{
				Source: SandboxFunctionSource{
					Code: "def handler(request):\n    return {'status': 204}\n",
				},
			},
			Command: []string{"/bin/ignored"},
			CWD:     "/ignored",
		},
		Ingress: SandboxAppServiceIngress{Public: true},
	}})
	if err != nil {
		t.Fatalf("NormalizeSandboxAppServices: %v", err)
	}
	service := services[0]
	if service.Runtime.Function == nil {
		t.Fatal("Runtime.Function is nil")
	}
	if service.Runtime.Function.Runtime != "python" {
		t.Fatalf("function runtime = %q, want python", service.Runtime.Function.Runtime)
	}
	if service.Runtime.Function.Handler != "handler" {
		t.Fatalf("function handler = %q, want handler", service.Runtime.Function.Handler)
	}
	if service.Runtime.Function.Source.Type != "inline" {
		t.Fatalf("function source type = %q, want inline", service.Runtime.Function.Source.Type)
	}
	if service.Port != 49983 {
		t.Fatalf("function service port = %d, want 49983", service.Port)
	}
	if len(service.Runtime.Command) != 0 || service.Runtime.CWD != "" {
		t.Fatalf("function runtime kept process fields: %#v", service.Runtime)
	}
	if len(service.Ingress.Routes) != 1 || service.Ingress.Routes[0].ID != "webhook" {
		t.Fatalf("default route = %#v, want webhook route", service.Ingress.Routes)
	}
	if blockers := SandboxAppServicePublishBlockers(service); len(blockers) != 0 {
		t.Fatalf("blockers = %#v, want none", blockers)
	}
}

func TestNormalizeSandboxAppServicesRejectsInvalidFunctionSource(t *testing.T) {
	_, err := NormalizeSandboxAppServices([]SandboxAppService{{
		ID:   "webhook",
		Port: 49983,
		Runtime: &SandboxAppServiceRuntime{
			Type: SandboxAppServiceRuntimeFunction,
			Function: &SandboxFunction{
				Source: SandboxFunctionSource{
					Type: "git",
					Code: "def handler(request):\n    return None\n",
				},
			},
		},
		Ingress: SandboxAppServiceIngress{Public: true},
	}})
	if err == nil {
		t.Fatal("NormalizeSandboxAppServices succeeded, want error")
	}
}

func TestNormalizeSandboxAppServicesRejectsWrongFunctionPort(t *testing.T) {
	_, err := NormalizeSandboxAppServices([]SandboxAppService{{
		ID:   "webhook",
		Port: 8080,
		Runtime: &SandboxAppServiceRuntime{
			Type: SandboxAppServiceRuntimeFunction,
			Function: &SandboxFunction{
				Source: SandboxFunctionSource{
					Code: "def handler(request):\n    return None\n",
				},
			},
		},
		Ingress: SandboxAppServiceIngress{Public: true},
	}})
	if err == nil {
		t.Fatal("NormalizeSandboxAppServices succeeded, want wrong function port error")
	}
}

func TestNormalizeSandboxAppServicesRejectsMissingNonFunctionPort(t *testing.T) {
	_, err := NormalizeSandboxAppServices([]SandboxAppService{{
		ID: "api",
		Runtime: &SandboxAppServiceRuntime{
			Type:    SandboxAppServiceRuntimeCMD,
			Command: []string{"python3", "-m", "http.server", "8080"},
		},
		Ingress: SandboxAppServiceIngress{Public: true},
	}})
	if err == nil {
		t.Fatal("NormalizeSandboxAppServices succeeded, want missing port error")
	}
}

func TestNormalizeSandboxAppServicesRejectsJSONMissingIngress(t *testing.T) {
	var req struct {
		Services []SandboxAppService `json:"services"`
	}
	if err := json.Unmarshal([]byte(`{"services":[{"id":"api","port":8080}]}`), &req); err != nil {
		t.Fatalf("unmarshal services: %v", err)
	}

	_, err := NormalizeSandboxAppServices(req.Services)
	if err == nil {
		t.Fatal("NormalizeSandboxAppServices succeeded, want missing ingress error")
	}
}

func TestNormalizeSandboxAppServicesRejectsJSONRuntimeMissingType(t *testing.T) {
	var req struct {
		Services []SandboxAppService `json:"services"`
	}
	if err := json.Unmarshal([]byte(`{"services":[{"id":"api","port":8080,"runtime":{"command":["python3","-m","http.server","8080"]},"ingress":{"public":false}}]}`), &req); err != nil {
		t.Fatalf("unmarshal services: %v", err)
	}

	_, err := NormalizeSandboxAppServices(req.Services)
	if err == nil {
		t.Fatal("NormalizeSandboxAppServices succeeded, want missing runtime type error")
	}
}

func TestNormalizeSandboxAppServicesRejectsInvalidHTTPHeaderAndOriginFields(t *testing.T) {
	hash := strings.Repeat("a", 64)

	tests := []struct {
		name  string
		route SandboxAppServiceRoute
	}{
		{
			name: "auth header name newline",
			route: SandboxAppServiceRoute{
				ID:         "api",
				PathPrefix: "/",
				Methods:    []string{"GET"},
				Auth: &SandboxAppServiceRouteAuth{
					Mode:              SandboxAppServiceRouteAuthModeHeader,
					HeaderName:        "X-Test\nInjected",
					HeaderValueSHA256: hash,
				},
			},
		},
		{
			name: "cors allowed header newline",
			route: SandboxAppServiceRoute{
				ID:         "api",
				PathPrefix: "/",
				Methods:    []string{"GET"},
				CORS: &SandboxAppServiceRouteCORS{
					AllowedOrigins: []string{"https://example.com"},
					AllowedHeaders: []string{"X-Good\nBad"},
				},
			},
		},
		{
			name: "cors expose header newline",
			route: SandboxAppServiceRoute{
				ID:         "api",
				PathPrefix: "/",
				Methods:    []string{"GET"},
				CORS: &SandboxAppServiceRouteCORS{
					AllowedOrigins: []string{"https://example.com"},
					ExposeHeaders:  []string{"X-Good\nBad"},
				},
			},
		},
		{
			name: "cors origin newline",
			route: SandboxAppServiceRoute{
				ID:         "api",
				PathPrefix: "/",
				Methods:    []string{"GET"},
				CORS: &SandboxAppServiceRouteCORS{
					AllowedOrigins: []string{"https://good.example\nAccess-Control-Allow-Origin: *"},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NormalizeSandboxAppServices([]SandboxAppService{{
				ID:   "api",
				Port: 8080,
				Ingress: SandboxAppServiceIngress{
					Public: true,
					Routes: []SandboxAppServiceRoute{tt.route},
				},
			}})
			if err == nil {
				t.Fatal("NormalizeSandboxAppServices succeeded, want invalid header/origin error")
			}
		})
	}
}
