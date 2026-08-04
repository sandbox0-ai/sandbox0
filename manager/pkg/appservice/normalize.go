// Package appservice owns manager-side sandbox application-service validation.
package appservice

import (
	"encoding/hex"
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"strings"

	"github.com/sandbox0-ai/sandbox0/pkg/managerapi"
	"github.com/sandbox0-ai/sandbox0/pkg/sandboxfunction"
	"golang.org/x/net/http/httpguts"
)

const (
	maxSandboxServiceRoutes        = 32
	maxSandboxServiceMethods       = 16
	maxSandboxServiceAllowedValues = 32
	maxSandboxFunctionConcurrency  = 1024
)

var sandboxServiceRouteIDPattern = regexp.MustCompile(`^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?$`)
var httpMethodPattern = regexp.MustCompile(`^[A-Z][A-Z0-9_-]*$`)
var sandboxFunctionHandlerPattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)*$`)

// NormalizeSandboxAppServices validates and canonicalizes sandbox services.
func NormalizeSandboxAppServices(services []managerapi.SandboxAppService) ([]managerapi.SandboxAppService, error) {
	if len(services) == 0 {
		return nil, nil
	}
	out := make([]managerapi.SandboxAppService, 0, len(services))
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

func normalizeSandboxAppService(service managerapi.SandboxAppService) (managerapi.SandboxAppService, error) {
	service.ID = strings.ToLower(strings.TrimSpace(service.ID))
	if !sandboxServiceRouteIDPattern.MatchString(service.ID) {
		return service, fmt.Errorf("id must be a DNS label")
	}
	if service.MissingIngressAfterJSONDecode() {
		return service, fmt.Errorf("ingress is required")
	}
	service.DisplayName = strings.TrimSpace(service.DisplayName)
	runtimeType := managerapi.SandboxAppServiceRuntimeManual
	if service.Runtime != nil {
		runtime := *service.Runtime
		if runtime.MissingTypeAfterJSONDecode() {
			return service, fmt.Errorf("runtime.type is required")
		}
		runtime.Type = strings.ToLower(strings.TrimSpace(runtime.Type))
		switch runtime.Type {
		case "", managerapi.SandboxAppServiceRuntimeManual, managerapi.SandboxAppServiceRuntimeCMD, managerapi.SandboxAppServiceRuntimeFunction:
		default:
			return service, fmt.Errorf("runtime.type must be one of: cmd, manual, function")
		}
		runtime.CWD = strings.TrimSpace(runtime.CWD)
		if runtime.Type == "" {
			runtime.Type = managerapi.SandboxAppServiceRuntimeManual
		}
		runtimeType = runtime.Type
		if runtime.Type == managerapi.SandboxAppServiceRuntimeCMD && len(runtime.Command) == 0 {
			return service, fmt.Errorf("runtime.command is required for cmd services")
		}
		if runtime.Type == managerapi.SandboxAppServiceRuntimeFunction {
			function, err := normalizeSandboxFunction(runtime.Function)
			if err != nil {
				return service, fmt.Errorf("runtime.function: %w", err)
			}
			runtime.Command = nil
			runtime.CWD = ""
			runtime.Function = function
		} else {
			runtime.Function = nil
		}
		service.Runtime = &runtime
	}
	if runtimeType == managerapi.SandboxAppServiceRuntimeFunction {
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
		service.Ingress.Routes = []managerapi.SandboxAppServiceRoute{{
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
		if service.Ingress.Public && route.Resume && !managerapi.SandboxAppServiceHasRestartableRuntime(service) {
			return service, fmt.Errorf("ingress.routes[%d]: resume requires runtime.type cmd or function", i)
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

func normalizeSandboxFunction(function *managerapi.SandboxFunction) (*managerapi.SandboxFunction, error) {
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
	if out.MaxConcurrency < 0 {
		return nil, fmt.Errorf("max_concurrency must be greater than or equal to 0")
	}
	if out.MaxConcurrency > maxSandboxFunctionConcurrency {
		return nil, fmt.Errorf("max_concurrency must be less than or equal to %d", maxSandboxFunctionConcurrency)
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

func normalizeSandboxAppServiceRoute(route managerapi.SandboxAppServiceRoute) (managerapi.SandboxAppServiceRoute, error) {
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

func normalizeSandboxAppServiceRouteAuth(auth managerapi.SandboxAppServiceRouteAuth) (managerapi.SandboxAppServiceRouteAuth, error) {
	auth.Mode = strings.ToLower(strings.TrimSpace(auth.Mode))
	if auth.Mode == "" {
		auth.Mode = managerapi.SandboxAppServiceRouteAuthModeNone
	}
	switch auth.Mode {
	case managerapi.SandboxAppServiceRouteAuthModeNone:
		auth.BearerTokenSHA256 = ""
		auth.HeaderName = ""
		auth.HeaderValueSHA256 = ""
	case managerapi.SandboxAppServiceRouteAuthModeBearer:
		if strings.TrimSpace(auth.BearerTokenSHA256) == "" {
			return auth, fmt.Errorf("auth.bearer_token_sha256 is required for bearer auth")
		}
		auth.BearerTokenSHA256 = strings.ToLower(strings.TrimSpace(auth.BearerTokenSHA256))
		if !validSHA256Hex(auth.BearerTokenSHA256) {
			return auth, fmt.Errorf("auth.bearer_token_sha256 must be a hex encoded SHA-256 digest")
		}
		auth.HeaderName = ""
		auth.HeaderValueSHA256 = ""
	case managerapi.SandboxAppServiceRouteAuthModeHeader:
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

func normalizeSandboxAppServiceRouteCORS(cors managerapi.SandboxAppServiceRouteCORS) (managerapi.SandboxAppServiceRouteCORS, error) {
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
