package runtime

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"text/template"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/egressauth"
)

type staticHeadersProvider struct{}

type staticTLSClientCertificateProvider struct{}

type staticUsernamePasswordProvider struct{}

func (p *staticHeadersProvider) Resolve(
	_ context.Context,
	req *egressauth.ResolveRequest,
	binding *egressauth.CredentialBinding,
	source *egressauth.CredentialSourceVersion,
	defaultTTL time.Duration,
) (*ResolveResult, error) {
	if binding == nil || source == nil || req == nil {
		return nil, nil
	}
	if source.Spec.StaticHeaders == nil {
		return nil, fmt.Errorf("static_headers source spec is required")
	}
	if binding.Projection.Type != egressauth.CredentialProjectionTypeHTTPHeaders || binding.Projection.HTTPHeaders == nil {
		return nil, fmt.Errorf("http_headers projection is required")
	}

	headers, err := projectHTTPHeaders(binding.Projection.HTTPHeaders, source.Spec.StaticHeaders.Values)
	if err != nil {
		return nil, err
	}

	ttl := defaultTTL
	if ttlOverride, ok := parseBindingTTL(binding.CachePolicy, defaultTTL); ok {
		ttl = ttlOverride
	}
	expiresAt := time.Now().UTC().Add(ttl)
	return &ResolveResult{
		Response: egressauth.NewHTTPHeadersResolveResponse(req.AuthRef, headers, &expiresAt),
		TTL:      ttl,
	}, nil
}

func (p *staticTLSClientCertificateProvider) Resolve(
	_ context.Context,
	req *egressauth.ResolveRequest,
	binding *egressauth.CredentialBinding,
	source *egressauth.CredentialSourceVersion,
	defaultTTL time.Duration,
) (*ResolveResult, error) {
	if binding == nil || source == nil || req == nil {
		return nil, nil
	}
	if source.Spec.StaticTLSClientCertificate == nil {
		return nil, fmt.Errorf("static_tls_client_certificate source spec is required")
	}
	if binding.Projection.Type != egressauth.CredentialProjectionTypeTLSClientCertificate || binding.Projection.TLSClientCertificate == nil {
		return nil, fmt.Errorf("tls_client_certificate projection is required")
	}

	spec := source.Spec.StaticTLSClientCertificate
	ttl := defaultTTL
	if ttlOverride, ok := parseBindingTTL(binding.CachePolicy, defaultTTL); ok {
		ttl = ttlOverride
	}
	expiresAt := time.Now().UTC().Add(ttl)
	return &ResolveResult{
		Response: egressauth.NewTLSClientCertificateResolveResponse(req.AuthRef, &egressauth.TLSClientCertificateDirective{
			CertificatePEM: spec.CertificatePEM,
			PrivateKeyPEM:  spec.PrivateKeyPEM,
			CAPEM:          spec.CAPEM,
		}, &expiresAt),
		TTL: ttl,
	}, nil
}

func (p *staticUsernamePasswordProvider) Resolve(
	_ context.Context,
	req *egressauth.ResolveRequest,
	binding *egressauth.CredentialBinding,
	source *egressauth.CredentialSourceVersion,
	defaultTTL time.Duration,
) (*ResolveResult, error) {
	if binding == nil || source == nil || req == nil {
		return nil, nil
	}
	if source.Spec.StaticUsernamePassword == nil {
		return nil, fmt.Errorf("static_username_password source spec is required")
	}
	if binding.Projection.Type != egressauth.CredentialProjectionTypeUsernamePassword || binding.Projection.UsernamePassword == nil {
		return nil, fmt.Errorf("username_password projection is required")
	}

	spec := source.Spec.StaticUsernamePassword
	ttl := defaultTTL
	if ttlOverride, ok := parseBindingTTL(binding.CachePolicy, defaultTTL); ok {
		ttl = ttlOverride
	}
	expiresAt := time.Now().UTC().Add(ttl)
	return &ResolveResult{
		Response: egressauth.NewUsernamePasswordResolveResponse(req.AuthRef, &egressauth.UsernamePasswordDirective{
			Username: spec.Username,
			Password: spec.Password,
		}, &expiresAt),
		TTL: ttl,
	}, nil
}

func parseBindingTTL(cachePolicy *egressauth.CachePolicySpec, defaultTTL time.Duration) (time.Duration, bool) {
	if cachePolicy == nil {
		return defaultTTL, false
	}

	raw := strings.TrimSpace(cachePolicy.TTL)
	if raw == "" {
		return defaultTTL, false
	}
	ttl, err := time.ParseDuration(raw)
	if err != nil || ttl <= 0 {
		return defaultTTL, false
	}
	return ttl, true
}

func projectHTTPHeaders(projection *egressauth.HTTPHeadersProjection, values map[string]string) (map[string]string, error) {
	if projection == nil {
		return nil, fmt.Errorf("http headers projection is required")
	}
	if len(projection.Headers) == 0 {
		return nil, nil
	}

	out := make(map[string]string, len(projection.Headers))
	for _, header := range projection.Headers {
		name := strings.TrimSpace(header.Name)
		if name == "" {
			return nil, fmt.Errorf("projected header name is required")
		}
		rendered, err := renderValueTemplate(header.ValueTemplate, values)
		if err != nil {
			return nil, fmt.Errorf("render projected header %q: %w", name, err)
		}
		out[name] = rendered
	}
	return out, nil
}

func renderValueTemplate(valueTemplate string, values map[string]string) (string, error) {
	tpl, err := template.New("header").Option("missingkey=error").Parse(valueTemplate)
	if err != nil {
		return "", err
	}

	var rendered bytes.Buffer
	if err := tpl.Execute(&rendered, values); err != nil {
		return "", err
	}
	return rendered.String(), nil
}
