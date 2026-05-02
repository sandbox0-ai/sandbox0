package service

import (
	"encoding/hex"
	"fmt"
	"net/http"
	"regexp"
	"strings"
)

const (
	PublicGatewayAuthModeNone   = "none"
	PublicGatewayAuthModeBearer = "bearer"
	PublicGatewayAuthModeHeader = "header"
)

const (
	maxPublicGatewayRoutes        = 32
	maxPublicGatewayMethods       = 16
	maxPublicGatewayAllowedValues = 32
)

var publicGatewayRouteIDPattern = regexp.MustCompile(`^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?$`)
var httpMethodPattern = regexp.MustCompile(`^[A-Z][A-Z0-9_-]*$`)

// PublicGatewayConfig controls request-level policy for sandbox public traffic.
type PublicGatewayConfig struct {
	Enabled bool                 `json:"enabled"`
	Routes  []PublicGatewayRoute `json:"routes,omitempty"`
}

// PublicGatewayRoute defines one public route on an exposed sandbox port.
type PublicGatewayRoute struct {
	ID             string                  `json:"id"`
	Port           int                     `json:"port"`
	PathPrefix     string                  `json:"path_prefix,omitempty"`
	Methods        []string                `json:"methods,omitempty"`
	RewritePrefix  *string                 `json:"rewrite_prefix,omitempty"`
	Auth           *PublicGatewayAuth      `json:"auth,omitempty"`
	CORS           *PublicGatewayCORS      `json:"cors,omitempty"`
	RateLimit      *PublicGatewayRateLimit `json:"rate_limit,omitempty"`
	TimeoutSeconds int                     `json:"timeout_seconds,omitempty"`
	Resume         bool                    `json:"resume"`
}

// PublicGatewayAuth controls inbound authentication for one public route.
type PublicGatewayAuth struct {
	Mode              string `json:"mode"`
	BearerTokenSHA256 string `json:"bearer_token_sha256,omitempty"`
	HeaderName        string `json:"header_name,omitempty"`
	HeaderValueSHA256 string `json:"header_value_sha256,omitempty"`
}

// PublicGatewayCORS controls CORS responses for browser-facing public routes.
type PublicGatewayCORS struct {
	AllowedOrigins   []string `json:"allowed_origins,omitempty"`
	AllowedMethods   []string `json:"allowed_methods,omitempty"`
	AllowedHeaders   []string `json:"allowed_headers,omitempty"`
	ExposeHeaders    []string `json:"expose_headers,omitempty"`
	AllowCredentials bool     `json:"allow_credentials,omitempty"`
	MaxAgeSeconds    int      `json:"max_age_seconds,omitempty"`
}

// PublicGatewayRateLimit controls per-route request limiting at cluster-gateway.
type PublicGatewayRateLimit struct {
	RPS   int `json:"rps"`
	Burst int `json:"burst"`
}

func normalizePublicGatewayConfig(cfg *PublicGatewayConfig) (*PublicGatewayConfig, error) {
	if cfg == nil {
		return nil, nil
	}
	out := *cfg
	if len(out.Routes) > maxPublicGatewayRoutes {
		return nil, fmt.Errorf("public_gateway.routes exceeds limit %d", maxPublicGatewayRoutes)
	}
	seen := make(map[string]struct{}, len(out.Routes))
	for i := range out.Routes {
		route, err := normalizePublicGatewayRoute(out.Routes[i])
		if err != nil {
			return nil, fmt.Errorf("public_gateway.routes[%d]: %w", i, err)
		}
		if _, ok := seen[route.ID]; ok {
			return nil, fmt.Errorf("public_gateway.routes[%d]: duplicate id %q", i, route.ID)
		}
		seen[route.ID] = struct{}{}
		out.Routes[i] = route
	}
	return &out, nil
}

func PublicGatewayHasResumeRoute(cfg *PublicGatewayConfig) bool {
	if cfg == nil {
		return false
	}
	for _, route := range cfg.Routes {
		if route.Resume {
			return true
		}
	}
	return false
}

func normalizePublicGatewayRoute(route PublicGatewayRoute) (PublicGatewayRoute, error) {
	route.ID = strings.ToLower(strings.TrimSpace(route.ID))
	if !publicGatewayRouteIDPattern.MatchString(route.ID) {
		return route, fmt.Errorf("id must be a DNS label")
	}
	if route.Port <= 0 || route.Port > 65535 {
		return route, fmt.Errorf("port must be between 1 and 65535")
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
		auth, err := normalizePublicGatewayAuth(*route.Auth)
		if err != nil {
			return route, err
		}
		route.Auth = &auth
	}
	if route.CORS != nil {
		cors, err := normalizePublicGatewayCORS(*route.CORS)
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
	if len(methods) > maxPublicGatewayMethods {
		return nil, fmt.Errorf("methods exceeds limit %d", maxPublicGatewayMethods)
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

func normalizePublicGatewayAuth(auth PublicGatewayAuth) (PublicGatewayAuth, error) {
	auth.Mode = strings.ToLower(strings.TrimSpace(auth.Mode))
	if auth.Mode == "" {
		auth.Mode = PublicGatewayAuthModeNone
	}
	switch auth.Mode {
	case PublicGatewayAuthModeNone:
		auth.BearerTokenSHA256 = ""
		auth.HeaderName = ""
		auth.HeaderValueSHA256 = ""
	case PublicGatewayAuthModeBearer:
		if strings.TrimSpace(auth.BearerTokenSHA256) == "" {
			return auth, fmt.Errorf("auth.bearer_token_sha256 is required for bearer auth")
		}
		auth.BearerTokenSHA256 = strings.ToLower(strings.TrimSpace(auth.BearerTokenSHA256))
		if !validSHA256Hex(auth.BearerTokenSHA256) {
			return auth, fmt.Errorf("auth.bearer_token_sha256 must be a hex encoded SHA-256 digest")
		}
		auth.HeaderName = ""
		auth.HeaderValueSHA256 = ""
	case PublicGatewayAuthModeHeader:
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

func normalizePublicGatewayCORS(cors PublicGatewayCORS) (PublicGatewayCORS, error) {
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
	if len(values) > maxPublicGatewayAllowedValues {
		return nil, fmt.Errorf("%s exceeds limit %d", field, maxPublicGatewayAllowedValues)
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
