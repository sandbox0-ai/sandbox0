package managerapi

import (
	"encoding/json"
	"strings"
)

const (
	SandboxAppServiceRouteAuthModeNone   = "none"
	SandboxAppServiceRouteAuthModeBearer = "bearer"
	SandboxAppServiceRouteAuthModeHeader = "header"
)

// SandboxAppServiceRouteAuth controls inbound authentication for one public route.
type SandboxAppServiceRouteAuth struct {
	Mode              string `json:"mode"`
	BearerTokenSHA256 string `json:"bearer_token_sha256,omitempty"`
	HeaderName        string `json:"header_name,omitempty"`
	HeaderValueSHA256 string `json:"header_value_sha256,omitempty"`
}

// SandboxAppServiceRouteCORS controls CORS responses for browser-facing public routes.
type SandboxAppServiceRouteCORS struct {
	AllowedOrigins   []string `json:"allowed_origins,omitempty"`
	AllowedMethods   []string `json:"allowed_methods,omitempty"`
	AllowedHeaders   []string `json:"allowed_headers,omitempty"`
	ExposeHeaders    []string `json:"expose_headers,omitempty"`
	AllowCredentials bool     `json:"allow_credentials,omitempty"`
	MaxAgeSeconds    int      `json:"max_age_seconds,omitempty"`
}

// SandboxAppServiceRouteRateLimit controls per-route request limiting at cluster-gateway.
type SandboxAppServiceRouteRateLimit struct {
	RPS   int `json:"rps"`
	Burst int `json:"burst"`
}

const (
	SandboxAppServiceRuntimeCMD      = "cmd"
	SandboxAppServiceRuntimeManual   = "manual"
	SandboxAppServiceRuntimeFunction = "function"
)

// SandboxAppService describes an application service running inside a sandbox.
type SandboxAppService struct {
	ID          string                    `json:"id"`
	DisplayName string                    `json:"display_name,omitempty"`
	Port        int                       `json:"port"`
	Runtime     *SandboxAppServiceRuntime `json:"runtime,omitempty"`
	Ingress     SandboxAppServiceIngress  `json:"ingress"`
	HealthCheck *SandboxAppServiceHealth  `json:"health_check,omitempty"`
	jsonDecoded bool
	ingressSet  bool
}

// SandboxAppServiceRuntime captures the restartable command for a sandbox service.
type SandboxAppServiceRuntime struct {
	Type        string            `json:"type"`
	Command     []string          `json:"command,omitempty"`
	CWD         string            `json:"cwd,omitempty"`
	EnvVars     map[string]string `json:"env_vars,omitempty"`
	Function    *SandboxFunction  `json:"function,omitempty"`
	jsonDecoded bool
	typeSet     bool
}

// SandboxFunction configures code that cluster-gateway sends to procd for execution.
type SandboxFunction struct {
	Runtime        string                `json:"runtime"`
	Handler        string                `json:"handler,omitempty"`
	MaxConcurrency int                   `json:"max_concurrency,omitempty"`
	Source         SandboxFunctionSource `json:"source"`
}

// SandboxFunctionSource carries user function code in sandbox service config.
type SandboxFunctionSource struct {
	Type string `json:"type"`
	Code string `json:"code,omitempty"`
}

// SandboxAppServiceIngress captures how traffic enters a sandbox service.
type SandboxAppServiceIngress struct {
	Public bool                     `json:"public"`
	Routes []SandboxAppServiceRoute `json:"routes,omitempty"`
}

// SandboxAppServiceRoute is a public route scoped to one sandbox service port.
type SandboxAppServiceRoute struct {
	ID             string                           `json:"id"`
	PathPrefix     string                           `json:"path_prefix,omitempty"`
	Methods        []string                         `json:"methods,omitempty"`
	RewritePrefix  *string                          `json:"rewrite_prefix,omitempty"`
	Auth           *SandboxAppServiceRouteAuth      `json:"auth,omitempty"`
	CORS           *SandboxAppServiceRouteCORS      `json:"cors,omitempty"`
	RateLimit      *SandboxAppServiceRouteRateLimit `json:"rate_limit,omitempty"`
	TimeoutSeconds int                              `json:"timeout_seconds,omitempty"`
	Resume         bool                             `json:"resume"`
}

// SandboxAppServiceHealth describes the readiness endpoint for a service.
type SandboxAppServiceHealth struct {
	Path string `json:"path,omitempty"`
}

func (s *SandboxAppService) UnmarshalJSON(data []byte) error {
	type alias SandboxAppService
	aux := struct {
		Ingress *SandboxAppServiceIngress `json:"ingress"`
		*alias
	}{
		alias: (*alias)(s),
	}
	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}
	s.jsonDecoded = true
	if aux.Ingress != nil {
		s.Ingress = *aux.Ingress
		s.ingressSet = true
	}
	return nil
}

func (r *SandboxAppServiceRuntime) UnmarshalJSON(data []byte) error {
	type alias SandboxAppServiceRuntime
	aux := struct {
		Type *string `json:"type"`
		*alias
	}{
		alias: (*alias)(r),
	}
	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}
	r.jsonDecoded = true
	r.typeSet = aux.Type != nil
	if aux.Type != nil {
		r.Type = *aux.Type
	}
	return nil
}

// MissingIngressAfterJSONDecode reports whether JSON omitted the required
// ingress property. Go callers that construct the DTO directly retain the
// zero-value ingress behavior until manager-side validation runs.
func (s SandboxAppService) MissingIngressAfterJSONDecode() bool {
	return s.jsonDecoded && !s.ingressSet
}

// MissingTypeAfterJSONDecode reports whether JSON omitted or left empty the
// required runtime type. It preserves the distinction between omitted JSON and
// a directly constructed runtime that defaults to manual.
func (r SandboxAppServiceRuntime) MissingTypeAfterJSONDecode() bool {
	return r.jsonDecoded && (!r.typeSet || strings.TrimSpace(r.Type) == "")
}

// SandboxAppServiceHasRestartableRuntime reports whether cluster-gateway can
// recreate the service process after a sandbox runtime is resumed.
func SandboxAppServiceHasRestartableRuntime(service SandboxAppService) bool {
	if service.Runtime == nil {
		return false
	}
	switch service.Runtime.Type {
	case SandboxAppServiceRuntimeCMD:
		return len(service.Runtime.Command) > 0
	case SandboxAppServiceRuntimeFunction:
		return service.Runtime.Function != nil
	default:
		return false
	}
}
