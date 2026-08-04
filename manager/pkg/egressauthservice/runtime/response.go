package runtime

import (
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/egressauth"
)

func newHTTPHeadersResolveResponse(authRef string, headers map[string]string, expiresAt *time.Time) *egressauth.ResolveResponse {
	var directives []egressauth.ResolveDirective
	if len(headers) > 0 {
		directives = []egressauth.ResolveDirective{{
			Kind: egressauth.ResolveDirectiveKindHTTPHeaders,
			HTTPHeaders: &egressauth.HTTPHeadersDirective{
				Headers: headers,
			},
		}}
	}
	return newResolveResponse(authRef, directives, expiresAt)
}

func newPlaceholderSubstitutionResolveResponse(authRef string, directive *egressauth.PlaceholderSubstitutionDirective, expiresAt *time.Time) *egressauth.ResolveResponse {
	var directives []egressauth.ResolveDirective
	if directive != nil {
		directives = []egressauth.ResolveDirective{{
			Kind:                    egressauth.ResolveDirectiveKindPlaceholderSubstitution,
			PlaceholderSubstitution: directive,
		}}
	}
	return newResolveResponse(authRef, directives, expiresAt)
}

func newTLSClientCertificateResolveResponse(authRef string, directive *egressauth.TLSClientCertificateDirective, expiresAt *time.Time) *egressauth.ResolveResponse {
	var directives []egressauth.ResolveDirective
	if directive != nil {
		directives = []egressauth.ResolveDirective{{
			Kind:                 egressauth.ResolveDirectiveKindTLSClientCertificate,
			TLSClientCertificate: directive,
		}}
	}
	return newResolveResponse(authRef, directives, expiresAt)
}

func newUsernamePasswordResolveResponse(authRef string, directive *egressauth.UsernamePasswordDirective, expiresAt *time.Time) *egressauth.ResolveResponse {
	var directives []egressauth.ResolveDirective
	if directive != nil {
		directives = []egressauth.ResolveDirective{{
			Kind:             egressauth.ResolveDirectiveKindUsernamePassword,
			UsernamePassword: directive,
		}}
	}
	return newResolveResponse(authRef, directives, expiresAt)
}

func newSSHProxyResolveResponse(authRef string, directive *egressauth.SSHProxyDirective, expiresAt *time.Time) *egressauth.ResolveResponse {
	var directives []egressauth.ResolveDirective
	if directive != nil {
		directives = []egressauth.ResolveDirective{{
			Kind:     egressauth.ResolveDirectiveKindSSHProxy,
			SSHProxy: directive,
		}}
	}
	return newResolveResponse(authRef, directives, expiresAt)
}

func newResolveResponse(authRef string, directives []egressauth.ResolveDirective, expiresAt *time.Time) *egressauth.ResolveResponse {
	return egressauth.CloneResolveResponse(&egressauth.ResolveResponse{
		AuthRef:    authRef,
		Directives: directives,
		ExpiresAt:  expiresAt,
	})
}
