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

type staticSSHPrivateKeyProvider struct{}

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

	var response *egressauth.ResolveResponse
	switch binding.Projection.Type {
	case egressauth.CredentialProjectionTypeHTTPHeaders:
		headers, err := projectHTTPHeaders(binding.Projection.HTTPHeaders, source.Spec.StaticHeaders.Values)
		if err != nil {
			return nil, err
		}

		ttl := defaultTTL
		if ttlOverride, ok := parseBindingTTL(binding.CachePolicy, defaultTTL); ok {
			ttl = ttlOverride
		}
		expiresAt := time.Now().UTC().Add(ttl)
		response = egressauth.NewHTTPHeadersResolveResponse(req.AuthRef, headers, &expiresAt)
		return &ResolveResult{
			Response: response,
			TTL:      ttl,
		}, nil
	case egressauth.CredentialProjectionTypePlaceholderSubstitution:
		directive, err := projectPlaceholderSubstitution(binding.Projection.PlaceholderSubstitution, source.Spec.StaticHeaders.Values)
		if err != nil {
			return nil, err
		}

		ttl := defaultTTL
		if ttlOverride, ok := parseBindingTTL(binding.CachePolicy, defaultTTL); ok {
			ttl = ttlOverride
		}
		expiresAt := time.Now().UTC().Add(ttl)
		response = egressauth.NewPlaceholderSubstitutionResolveResponse(req.AuthRef, directive, &expiresAt)
		return &ResolveResult{
			Response: response,
			TTL:      ttl,
		}, nil
	default:
		return nil, fmt.Errorf("http_headers or placeholder_substitution projection is required")
	}
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

func projectPlaceholderSubstitution(projection *egressauth.PlaceholderSubstitutionProjection, values map[string]string) (*egressauth.PlaceholderSubstitutionDirective, error) {
	if projection == nil {
		return nil, fmt.Errorf("placeholder substitution projection is required")
	}
	if len(projection.Replacements) == 0 {
		return nil, nil
	}

	replacements := make([]egressauth.PlaceholderSubstitutionReplacement, 0, len(projection.Replacements))
	for _, replacement := range projection.Replacements {
		if strings.TrimSpace(replacement.Placeholder) == "" {
			return nil, fmt.Errorf("placeholder is required")
		}
		locations, err := normalizePlaceholderSubstitutionLocations(replacement.Locations)
		if err != nil {
			return nil, err
		}
		rendered, err := renderValueTemplate(replacement.ValueTemplate, values)
		if err != nil {
			return nil, fmt.Errorf("render placeholder replacement: %w", err)
		}
		replacements = append(replacements, egressauth.PlaceholderSubstitutionReplacement{
			Placeholder: replacement.Placeholder,
			Value:       rendered,
			Locations:   locations,
		})
	}

	return &egressauth.PlaceholderSubstitutionDirective{Replacements: replacements}, nil
}

func normalizePlaceholderSubstitutionLocations(in []egressauth.PlaceholderSubstitutionLocation) ([]egressauth.PlaceholderSubstitutionLocation, error) {
	if len(in) == 0 {
		return nil, fmt.Errorf("placeholder substitution locations are required")
	}

	seen := map[egressauth.PlaceholderSubstitutionLocation]struct{}{}
	out := make([]egressauth.PlaceholderSubstitutionLocation, 0, len(in))
	for _, location := range in {
		switch location {
		case egressauth.PlaceholderSubstitutionLocationHeader,
			egressauth.PlaceholderSubstitutionLocationQuery,
			egressauth.PlaceholderSubstitutionLocationBody:
		default:
			return nil, fmt.Errorf("unsupported placeholder substitution location %q", location)
		}
		if _, ok := seen[location]; ok {
			continue
		}
		seen[location] = struct{}{}
		out = append(out, location)
	}
	return out, nil
}

func renderValueTemplate(valueTemplate string, values map[string]string) (string, error) {
	tpl, err := template.New("credential-value").Option("missingkey=error").Parse(valueTemplate)
	if err != nil {
		return "", err
	}

	var rendered bytes.Buffer
	if err := tpl.Execute(&rendered, values); err != nil {
		return "", err
	}
	return rendered.String(), nil
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

func (p *staticSSHPrivateKeyProvider) Resolve(
	_ context.Context,
	req *egressauth.ResolveRequest,
	binding *egressauth.CredentialBinding,
	source *egressauth.CredentialSourceVersion,
	defaultTTL time.Duration,
) (*ResolveResult, error) {
	if binding == nil || source == nil || req == nil {
		return nil, nil
	}
	if source.Spec.StaticSSHPrivateKey == nil {
		return nil, fmt.Errorf("static_ssh_private_key source spec is required")
	}
	if binding.Projection.Type != egressauth.CredentialProjectionTypeSSHProxy || binding.Projection.SSHProxy == nil {
		return nil, fmt.Errorf("ssh_proxy projection is required")
	}

	spec := source.Spec.StaticSSHPrivateKey
	projection := binding.Projection.SSHProxy
	ttl := defaultTTL
	if ttlOverride, ok := parseBindingTTL(binding.CachePolicy, defaultTTL); ok {
		ttl = ttlOverride
	}
	expiresAt := time.Now().UTC().Add(ttl)
	return &ResolveResult{
		Response: egressauth.NewSSHProxyResolveResponse(req.AuthRef, &egressauth.SSHProxyDirective{
			SandboxPublicKeys: append([]string(nil), projection.SandboxPublicKeys...),
			UpstreamUsername:  projection.UpstreamUsername,
			PrivateKeyPEM:     spec.PrivateKeyPEM,
			Passphrase:        spec.Passphrase,
			KnownHosts:        append([]string(nil), projection.KnownHosts...),
		}, &expiresAt),
		TTL: ttl,
	}, nil
}
