package proxy

import (
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/egressauth"
)

func testHTTPHeadersResolveResponse(authRef string, headers map[string]string, expiresAt *time.Time) *egressauth.ResolveResponse {
	return testResolveResponse(authRef, []egressauth.ResolveDirective{{
		Kind: egressauth.ResolveDirectiveKindHTTPHeaders,
		HTTPHeaders: &egressauth.HTTPHeadersDirective{
			Headers: headers,
		},
	}}, expiresAt)
}

func testPlaceholderSubstitutionResolveResponse(authRef string, directive *egressauth.PlaceholderSubstitutionDirective, expiresAt *time.Time) *egressauth.ResolveResponse {
	return testResolveResponse(authRef, []egressauth.ResolveDirective{{
		Kind:                    egressauth.ResolveDirectiveKindPlaceholderSubstitution,
		PlaceholderSubstitution: directive,
	}}, expiresAt)
}

func testTLSClientCertificateResolveResponse(authRef string, directive *egressauth.TLSClientCertificateDirective, expiresAt *time.Time) *egressauth.ResolveResponse {
	return testResolveResponse(authRef, []egressauth.ResolveDirective{{
		Kind:                 egressauth.ResolveDirectiveKindTLSClientCertificate,
		TLSClientCertificate: directive,
	}}, expiresAt)
}

func testUsernamePasswordResolveResponse(authRef string, directive *egressauth.UsernamePasswordDirective, expiresAt *time.Time) *egressauth.ResolveResponse {
	return testResolveResponse(authRef, []egressauth.ResolveDirective{{
		Kind:             egressauth.ResolveDirectiveKindUsernamePassword,
		UsernamePassword: directive,
	}}, expiresAt)
}

func testSSHProxyResolveResponse(authRef string, directive *egressauth.SSHProxyDirective, expiresAt *time.Time) *egressauth.ResolveResponse {
	return testResolveResponse(authRef, []egressauth.ResolveDirective{{
		Kind:     egressauth.ResolveDirectiveKindSSHProxy,
		SSHProxy: directive,
	}}, expiresAt)
}

func testResolveResponse(authRef string, directives []egressauth.ResolveDirective, expiresAt *time.Time) *egressauth.ResolveResponse {
	return egressauth.CloneResolveResponse(&egressauth.ResolveResponse{
		AuthRef:    authRef,
		Directives: directives,
		ExpiresAt:  expiresAt,
	})
}
