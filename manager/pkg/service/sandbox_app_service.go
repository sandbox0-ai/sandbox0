package service

import (
	"encoding/hex"
	"fmt"
	"net/http"
	"regexp"
	"strings"

	"github.com/sandbox0-ai/sandbox0/pkg/naming"
	"github.com/sandbox0-ai/sandbox0/pkg/sandboxfunction"
)

const (
	SandboxAppServiceRouteAuthModeNone   = "none"
	SandboxAppServiceRouteAuthModeBearer = "bearer"
	SandboxAppServiceRouteAuthModeHeader = "header"
)

const (
	maxSandboxServiceRoutes        = 32
	maxSandboxServiceMethods       = 16
	maxSandboxServiceAllowedValues = 32
)

var sandboxServiceRouteIDPattern = regexp.MustCompile(`^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?$`)
var httpMethodPattern = regexp.MustCompile(`^[A-Z][A-Z0-9_-]*$`)
var sandboxFunctionHandlerPattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)*$`)
var sandboxFunctionFilenamePattern = regexp.MustCompile(`^[A-Za-z0-9_.-]+$`)

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
	SandboxAppServiceRuntimeWarmProcess = "warm_process"
	SandboxAppServiceRuntimeCMD         = "cmd"
	SandboxAppServiceRuntimeManual      = "manual"
	SandboxAppServiceRuntimeFunction    = "function"
)

// SandboxAppService describes an application service running inside a sandbox.
type SandboxAppService struct {
	ID          string                    `json:"id"`
	DisplayName string                    `json:"display_name,omitempty"`
	Port        int                       `json:"port"`
	Runtime     *SandboxAppServiceRuntime `json:"runtime,omitempty"`
	Ingress     SandboxAppServiceIngress  `json:"ingress"`
	HealthCheck *SandboxAppServiceHealth  `json:"health_check,omitempty"`
}

// SandboxAppServiceRuntime captures the restartable command for a sandbox service.
type SandboxAppServiceRuntime struct {
	Type            string            `json:"type"`
	Command         []string          `json:"command,omitempty"`
	CWD             string            `json:"cwd,omitempty"`
	EnvVars         map[string]string `json:"env_vars,omitempty"`
	WarmProcessName string            `json:"warm_process_name,omitempty"`
	Function        *SandboxFunction  `json:"function,omitempty"`
}

// SandboxFunction configures code that cluster-gateway sends to procd for execution.
type SandboxFunction struct {
	Runtime string                `json:"runtime"`
	Handler string                `json:"handler,omitempty"`
	Source  SandboxFunctionSource `json:"source"`
}

// SandboxFunctionSource carries user function code in sandbox service config.
type SandboxFunctionSource struct {
	Type     string `json:"type"`
	Filename string `json:"filename,omitempty"`
	Code     string `json:"code,omitempty"`
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

// SandboxAppServiceView adds derived publishability state to a sandbox service.
type SandboxAppServiceView struct {
	SandboxAppService
	Publishable     bool     `json:"publishable"`
	PublishBlockers []string `json:"publish_blockers,omitempty"`
	PublicURL       string   `json:"public_url,omitempty"`
}

func SandboxAppServicesHaveResumeRoute(services []SandboxAppService) bool {
	for _, svc := range services {
		for _, route := range svc.Ingress.Routes {
			if route.Resume {
				return true
			}
		}
	}
	return false
}

// NormalizeSandboxAppServices validates and canonicalizes sandbox services.
func NormalizeSandboxAppServices(services []SandboxAppService) ([]SandboxAppService, error) {
	if len(services) == 0 {
		return nil, nil
	}
	out := make([]SandboxAppService, 0, len(services))
	seen := make(map[string]struct{}, len(services))
	for i := range services {
		service, err := normalizeSandboxAppService(services[i])
		if err != nil {
			return nil, fmt.Errorf("services[%d]: %w", i, err)
		}
		if _, ok := seen[service.ID]; ok {
			return nil, fmt.Errorf("services[%d]: duplicate id %q", i, service.ID)
		}
		seen[service.ID] = struct{}{}
		out = append(out, service)
	}
	return out, nil
}

func normalizeSandboxAppService(service SandboxAppService) (SandboxAppService, error) {
	service.ID = strings.ToLower(strings.TrimSpace(service.ID))
	if !sandboxServiceRouteIDPattern.MatchString(service.ID) {
		return service, fmt.Errorf("id must be a DNS label")
	}
	service.DisplayName = strings.TrimSpace(service.DisplayName)
	if service.Port <= 0 || service.Port > 65535 {
		return service, fmt.Errorf("port must be between 1 and 65535")
	}
	if service.Runtime != nil {
		runtime := *service.Runtime
		runtime.Type = strings.ToLower(strings.TrimSpace(runtime.Type))
		switch runtime.Type {
		case "", SandboxAppServiceRuntimeManual, SandboxAppServiceRuntimeCMD, SandboxAppServiceRuntimeWarmProcess, SandboxAppServiceRuntimeFunction:
		default:
			return service, fmt.Errorf("runtime.type must be one of: warm_process, cmd, manual, function")
		}
		runtime.CWD = strings.TrimSpace(runtime.CWD)
		runtime.WarmProcessName = strings.TrimSpace(runtime.WarmProcessName)
		if runtime.Type == "" {
			runtime.Type = SandboxAppServiceRuntimeManual
		}
		if runtime.Type == SandboxAppServiceRuntimeCMD && len(runtime.Command) == 0 {
			return service, fmt.Errorf("runtime.command is required for cmd services")
		}
		if runtime.Type == SandboxAppServiceRuntimeFunction {
			function, err := normalizeSandboxFunction(runtime.Function)
			if err != nil {
				return service, fmt.Errorf("runtime.function: %w", err)
			}
			runtime.Command = nil
			runtime.CWD = ""
			runtime.WarmProcessName = ""
			runtime.Function = function
		} else {
			runtime.Function = nil
		}
		service.Runtime = &runtime
	}
	if service.Ingress.Public && len(service.Ingress.Routes) == 0 {
		service.Ingress.Routes = []SandboxAppServiceRoute{{
			ID:         service.ID,
			PathPrefix: "/",
		}}
	}
	if len(service.Ingress.Routes) > maxSandboxServiceRoutes {
		return service, fmt.Errorf("ingress.routes exceeds limit %d", maxSandboxServiceRoutes)
	}
	seenRoutes := make(map[string]struct{}, len(service.Ingress.Routes))
	for i := range service.Ingress.Routes {
		route, err := normalizeSandboxAppServiceRoute(service.Ingress.Routes[i])
		if err != nil {
			return service, fmt.Errorf("ingress.routes[%d]: %w", i, err)
		}
		if _, ok := seenRoutes[route.ID]; ok {
			return service, fmt.Errorf("ingress.routes[%d]: duplicate id %q", i, route.ID)
		}
		seenRoutes[route.ID] = struct{}{}
		service.Ingress.Routes[i] = route
	}
	if service.HealthCheck != nil {
		health := *service.HealthCheck
		health.Path = normalizeGatewayPathPrefix(health.Path)
		service.HealthCheck = &health
	}
	return service, nil
}

func SandboxAppServiceViews(services []SandboxAppService) []SandboxAppServiceView {
	return SandboxAppServiceViewsForExposure("", "", services)
}

// SandboxAppServiceViewsForExposure adds derived fields that depend on the
// deployment exposure domain.
func SandboxAppServiceViewsForExposure(sandboxID, exposureDomain string, services []SandboxAppService) []SandboxAppServiceView {
	if len(services) == 0 {
		return []SandboxAppServiceView{}
	}
	views := make([]SandboxAppServiceView, 0, len(services))
	for _, service := range services {
		blockers := SandboxAppServicePublishBlockers(service)
		views = append(views, SandboxAppServiceView{
			SandboxAppService: service,
			Publishable:       len(blockers) == 0,
			PublishBlockers:   blockers,
			PublicURL:         SandboxAppServicePublicURL(sandboxID, exposureDomain, service),
		})
	}
	return views
}

// SandboxAppServicePublicURL returns the public entrypoint for a service when
// the service is public and the deployment has an exposure domain.
func SandboxAppServicePublicURL(sandboxID, exposureDomain string, service SandboxAppService) string {
	if !service.Ingress.Public {
		return ""
	}
	sandboxID = strings.TrimSpace(sandboxID)
	exposureDomain = strings.Trim(strings.TrimSpace(exposureDomain), ".")
	if sandboxID == "" || exposureDomain == "" {
		return ""
	}
	label, err := naming.BuildExposureHostLabel(sandboxID, service.Port)
	if err != nil {
		return ""
	}
	return "https://" + label + "." + exposureDomain
}

// SandboxAppServicePublishBlockers returns reasons why a service cannot be
// published as a function revision.
func SandboxAppServicePublishBlockers(service SandboxAppService) []string {
	var blockers []string
	if !service.Ingress.Public || len(service.Ingress.Routes) == 0 {
		blockers = append(blockers, "not_public")
	}
	if service.Runtime == nil {
		blockers = append(blockers, "missing_runtime")
	} else if service.Runtime.Type == SandboxAppServiceRuntimeManual {
		blockers = append(blockers, "manual_runtime")
	} else if service.Runtime.Type == SandboxAppServiceRuntimeCMD && len(service.Runtime.Command) == 0 {
		blockers = append(blockers, "missing_command")
	} else if service.Runtime.Type == SandboxAppServiceRuntimeWarmProcess && strings.TrimSpace(service.Runtime.WarmProcessName) == "" {
		blockers = append(blockers, "missing_warm_process_name")
	} else if service.Runtime.Type == SandboxAppServiceRuntimeFunction && service.Runtime.Function == nil {
		blockers = append(blockers, "missing_function")
	}
	return blockers
}

func normalizeSandboxFunction(function *SandboxFunction) (*SandboxFunction, error) {
	if function == nil {
		return nil, fmt.Errorf("is required for function services")
	}
	out := *function
	out.Runtime = strings.ToLower(strings.TrimSpace(out.Runtime))
	if out.Runtime == "" {
		out.Runtime = sandboxfunction.RuntimePython
	}
	if out.Runtime != sandboxfunction.RuntimePython {
		return nil, fmt.Errorf("runtime must be %q", sandboxfunction.RuntimePython)
	}
	out.Handler = strings.TrimSpace(out.Handler)
	if out.Handler == "" {
		out.Handler = sandboxfunction.DefaultHandler
	}
	if !sandboxFunctionHandlerPattern.MatchString(out.Handler) {
		return nil, fmt.Errorf("handler must match %s", sandboxFunctionHandlerPattern.String())
	}

	source := out.Source
	source.Type = strings.ToLower(strings.TrimSpace(source.Type))
	if source.Type == "" {
		source.Type = sandboxfunction.SourceTypeInline
	}
	if source.Type != sandboxfunction.SourceTypeInline {
		return nil, fmt.Errorf("source.type must be %q", sandboxfunction.SourceTypeInline)
	}
	source.Filename = strings.TrimSpace(source.Filename)
	if source.Filename == "" {
		source.Filename = sandboxfunction.DefaultFilename
	}
	if strings.Contains(source.Filename, "/") || strings.Contains(source.Filename, "\\") || strings.HasPrefix(source.Filename, ".") {
		return nil, fmt.Errorf("source.filename must be a relative file name")
	}
	if !sandboxFunctionFilenamePattern.MatchString(source.Filename) {
		return nil, fmt.Errorf("source.filename contains unsupported characters")
	}
	if strings.TrimSpace(source.Code) == "" {
		return nil, fmt.Errorf("source.code is required")
	}
	if len([]byte(source.Code)) > sandboxfunction.MaxInlineSourceBytes {
		return nil, fmt.Errorf("source.code exceeds limit %d bytes", sandboxfunction.MaxInlineSourceBytes)
	}
	out.Source = source
	return &out, nil
}

func normalizeSandboxAppServiceRoute(route SandboxAppServiceRoute) (SandboxAppServiceRoute, error) {
	route.ID = strings.ToLower(strings.TrimSpace(route.ID))
	if !sandboxServiceRouteIDPattern.MatchString(route.ID) {
		return route, fmt.Errorf("id must be a DNS label")
	}
	route.PathPrefix = normalizeGatewayPathPrefix(route.PathPrefix)
	if route.RewritePrefix != nil {
		rewrite := normalizeGatewayRewritePrefix(*route.RewritePrefix)
		route.RewritePrefix = &rewrite
	}
	methods, err := normalizeGatewayMethods(route.Methods)
	if err != nil {
		return route, err
	}
	route.Methods = methods
	if route.Auth != nil {
		auth, err := normalizeSandboxAppServiceRouteAuth(*route.Auth)
		if err != nil {
			return route, err
		}
		route.Auth = &auth
	}
	if route.CORS != nil {
		cors, err := normalizeSandboxAppServiceRouteCORS(*route.CORS)
		if err != nil {
			return route, err
		}
		route.CORS = &cors
	}
	if route.RateLimit != nil {
		if route.RateLimit.RPS <= 0 {
			return route, fmt.Errorf("rate_limit.rps must be greater than 0")
		}
		if route.RateLimit.Burst <= 0 {
			return route, fmt.Errorf("rate_limit.burst must be greater than 0")
		}
	}
	if route.TimeoutSeconds < 0 {
		return route, fmt.Errorf("timeout_seconds must be greater than or equal to 0")
	}
	return route, nil
}

func normalizeGatewayPathPrefix(prefix string) string {
	prefix = strings.TrimSpace(prefix)
	if prefix == "" {
		return "/"
	}
	if !strings.HasPrefix(prefix, "/") {
		prefix = "/" + prefix
	}
	return prefix
}

func normalizeGatewayRewritePrefix(prefix string) string {
	prefix = strings.TrimSpace(prefix)
	if prefix == "" {
		return "/"
	}
	if !strings.HasPrefix(prefix, "/") {
		prefix = "/" + prefix
	}
	return prefix
}

func normalizeGatewayMethods(methods []string) ([]string, error) {
	if len(methods) > maxSandboxServiceMethods {
		return nil, fmt.Errorf("methods exceeds limit %d", maxSandboxServiceMethods)
	}
	if len(methods) == 0 {
		return nil, nil
	}
	out := make([]string, 0, len(methods))
	seen := make(map[string]struct{}, len(methods))
	for _, method := range methods {
		method = strings.ToUpper(strings.TrimSpace(method))
		if method == "" {
			return nil, fmt.Errorf("methods cannot contain empty values")
		}
		if method != "*" && !httpMethodPattern.MatchString(method) {
			return nil, fmt.Errorf("invalid method %q", method)
		}
		if _, ok := seen[method]; ok {
			continue
		}
		seen[method] = struct{}{}
		out = append(out, method)
	}
	return out, nil
}

func normalizeSandboxAppServiceRouteAuth(auth SandboxAppServiceRouteAuth) (SandboxAppServiceRouteAuth, error) {
	auth.Mode = strings.ToLower(strings.TrimSpace(auth.Mode))
	if auth.Mode == "" {
		auth.Mode = SandboxAppServiceRouteAuthModeNone
	}
	switch auth.Mode {
	case SandboxAppServiceRouteAuthModeNone:
		auth.BearerTokenSHA256 = ""
		auth.HeaderName = ""
		auth.HeaderValueSHA256 = ""
	case SandboxAppServiceRouteAuthModeBearer:
		if strings.TrimSpace(auth.BearerTokenSHA256) == "" {
			return auth, fmt.Errorf("auth.bearer_token_sha256 is required for bearer auth")
		}
		auth.BearerTokenSHA256 = strings.ToLower(strings.TrimSpace(auth.BearerTokenSHA256))
		if !validSHA256Hex(auth.BearerTokenSHA256) {
			return auth, fmt.Errorf("auth.bearer_token_sha256 must be a hex encoded SHA-256 digest")
		}
		auth.HeaderName = ""
		auth.HeaderValueSHA256 = ""
	case SandboxAppServiceRouteAuthModeHeader:
		auth.HeaderName = http.CanonicalHeaderKey(strings.TrimSpace(auth.HeaderName))
		auth.HeaderValueSHA256 = strings.ToLower(strings.TrimSpace(auth.HeaderValueSHA256))
		if auth.HeaderName == "" || auth.HeaderValueSHA256 == "" {
			return auth, fmt.Errorf("auth.header_name and auth.header_value_sha256 are required for header auth")
		}
		if !validSHA256Hex(auth.HeaderValueSHA256) {
			return auth, fmt.Errorf("auth.header_value_sha256 must be a hex encoded SHA-256 digest")
		}
	default:
		return auth, fmt.Errorf("unsupported auth.mode %q", auth.Mode)
	}
	return auth, nil
}

func validSHA256Hex(value string) bool {
	if len(value) != 64 {
		return false
	}
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == 32
}

func normalizeSandboxAppServiceRouteCORS(cors SandboxAppServiceRouteCORS) (SandboxAppServiceRouteCORS, error) {
	var err error
	cors.AllowedOrigins, err = normalizeNonEmptyValues("cors.allowed_origins", cors.AllowedOrigins)
	if err != nil {
		return cors, err
	}
	cors.AllowedMethods, err = normalizeGatewayMethods(cors.AllowedMethods)
	if err != nil {
		return cors, fmt.Errorf("cors.allowed_methods: %w", err)
	}
	cors.AllowedHeaders, err = normalizeNonEmptyValues("cors.allowed_headers", cors.AllowedHeaders)
	if err != nil {
		return cors, err
	}
	cors.ExposeHeaders, err = normalizeNonEmptyValues("cors.expose_headers", cors.ExposeHeaders)
	if err != nil {
		return cors, err
	}
	if cors.MaxAgeSeconds < 0 {
		return cors, fmt.Errorf("cors.max_age_seconds must be greater than or equal to 0")
	}
	return cors, nil
}

func normalizeNonEmptyValues(field string, values []string) ([]string, error) {
	if len(values) > maxSandboxServiceAllowedValues {
		return nil, fmt.Errorf("%s exceeds limit %d", field, maxSandboxServiceAllowedValues)
	}
	out := make([]string, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			return nil, fmt.Errorf("%s cannot contain empty values", field)
		}
		key := strings.ToLower(value)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, value)
	}
	return out, nil
}
