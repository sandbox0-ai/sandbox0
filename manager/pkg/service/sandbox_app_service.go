package service

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"strings"

	"github.com/sandbox0-ai/sandbox0/pkg/naming"
	"github.com/sandbox0-ai/sandbox0/pkg/sandboxfunction"
	"golang.org/x/net/http/httpguts"
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
	SandboxAppServiceRuntimeNextJS   = "nextjs"
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
	Runtime string                `json:"runtime"`
	Handler string                `json:"handler,omitempty"`
	Source  SandboxFunctionSource `json:"source"`
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
	case SandboxAppServiceRuntimeNextJS:
		return true
	default:
		return false
	}
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
	if service.jsonDecoded && !service.ingressSet {
		return service, fmt.Errorf("ingress is required")
	}
	service.DisplayName = strings.TrimSpace(service.DisplayName)
	runtimeType := SandboxAppServiceRuntimeManual
	if service.Runtime != nil {
		runtime := *service.Runtime
		if runtime.jsonDecoded && !runtime.typeSet {
			return service, fmt.Errorf("runtime.type is required")
		}
		runtime.Type = strings.ToLower(strings.TrimSpace(runtime.Type))
		if runtime.jsonDecoded && runtime.Type == "" {
			return service, fmt.Errorf("runtime.type is required")
		}
		switch runtime.Type {
		case "", SandboxAppServiceRuntimeManual, SandboxAppServiceRuntimeCMD, SandboxAppServiceRuntimeFunction, SandboxAppServiceRuntimeNextJS:
		default:
			return service, fmt.Errorf("runtime.type must be one of: cmd, manual, function, nextjs")
		}
		runtime.CWD = strings.TrimSpace(runtime.CWD)
		if runtime.Type == "" {
			runtime.Type = SandboxAppServiceRuntimeManual
		}
		runtimeType = runtime.Type
		switch runtime.Type {
		case SandboxAppServiceRuntimeCMD:
			if len(runtime.Command) == 0 {
				return service, fmt.Errorf("runtime.command is required for cmd services")
			}
			runtime.Function = nil
		case SandboxAppServiceRuntimeFunction:
			function, err := normalizeSandboxFunction(runtime.Function)
			if err != nil {
				return service, fmt.Errorf("runtime.function: %w", err)
			}
			runtime.Command = nil
			runtime.CWD = ""
			runtime.Function = function
		case SandboxAppServiceRuntimeNextJS:
			runtime.Command = nil
			runtime.Function = nil
		default:
			runtime.Function = nil
		}
		service.Runtime = &runtime
	}
	if runtimeType == SandboxAppServiceRuntimeFunction {
		if service.Port == 0 {
			service.Port = sandboxfunction.DefaultServicePort
		}
		if service.Port != sandboxfunction.DefaultServicePort {
			return service, fmt.Errorf("port must be omitted or %d for function services", sandboxfunction.DefaultServicePort)
		}
	} else {
		if service.Port <= 0 || service.Port > 65535 {
			return service, fmt.Errorf("port must be between 1 and 65535")
		}
		if service.Port == sandboxfunction.DefaultServicePort {
			return service, fmt.Errorf("port %d is reserved for function services", sandboxfunction.DefaultServicePort)
		}
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
		if service.Ingress.Public && route.Resume && !SandboxAppServiceHasRestartableRuntime(service) {
			return service, fmt.Errorf("ingress.routes[%d]: resume requires runtime.type cmd, function, or nextjs", i)
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
		headerName := strings.TrimSpace(auth.HeaderName)
		if !httpguts.ValidHeaderFieldName(headerName) {
			return auth, fmt.Errorf("auth.header_name must be a valid HTTP header name")
		}
		auth.HeaderName = http.CanonicalHeaderKey(headerName)
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
	cors.AllowedOrigins, err = normalizeCORSOrigins("cors.allowed_origins", cors.AllowedOrigins)
	if err != nil {
		return cors, err
	}
	cors.AllowedMethods, err = normalizeGatewayMethods(cors.AllowedMethods)
	if err != nil {
		return cors, fmt.Errorf("cors.allowed_methods: %w", err)
	}
	cors.AllowedHeaders, err = normalizeHTTPHeaderNames("cors.allowed_headers", cors.AllowedHeaders)
	if err != nil {
		return cors, err
	}
	cors.ExposeHeaders, err = normalizeHTTPHeaderNames("cors.expose_headers", cors.ExposeHeaders)
	if err != nil {
		return cors, err
	}
	if cors.MaxAgeSeconds < 0 {
		return cors, fmt.Errorf("cors.max_age_seconds must be greater than or equal to 0")
	}
	return cors, nil
}

func normalizeHTTPHeaderNames(field string, values []string) ([]string, error) {
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
		if !httpguts.ValidHeaderFieldName(value) {
			return nil, fmt.Errorf("%s contains invalid HTTP header name %q", field, value)
		}
		value = http.CanonicalHeaderKey(value)
		key := strings.ToLower(value)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, value)
	}
	return out, nil
}

func normalizeCORSOrigins(field string, values []string) ([]string, error) {
	if len(values) > maxSandboxServiceAllowedValues {
		return nil, fmt.Errorf("%s exceeds limit %d", field, maxSandboxServiceAllowedValues)
	}
	out := make([]string, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		origin, err := normalizeCORSOrigin(value)
		if err != nil {
			return nil, fmt.Errorf("%s contains invalid origin %q", field, value)
		}
		key := strings.ToLower(origin)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, origin)
	}
	return out, nil
}

func normalizeCORSOrigin(value string) (string, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return "", fmt.Errorf("origin is empty")
	}
	if containsHTTPControlChar(value) {
		return "", fmt.Errorf("origin contains a control character")
	}
	if value == "*" {
		return value, nil
	}
	parsed, err := url.Parse(value)
	if err != nil {
		return "", err
	}
	scheme := strings.ToLower(parsed.Scheme)
	if scheme != "http" && scheme != "https" {
		return "", fmt.Errorf("origin scheme must be http or https")
	}
	if parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return "", fmt.Errorf("origin must be scheme://host[:port]")
	}
	if parsed.Path != "" && parsed.Path != "/" {
		return "", fmt.Errorf("origin must not include a path")
	}
	return scheme + "://" + parsed.Host, nil
}

func containsHTTPControlChar(value string) bool {
	for _, r := range value {
		if r < 0x20 || r == 0x7f {
			return true
		}
	}
	return false
}
