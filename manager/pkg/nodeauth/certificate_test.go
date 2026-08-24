package nodeauth

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"net/http"
	"net/http/httptest"
	"testing"
)

func testCertificateIdentity() CertificateIdentity {
	return CertificateIdentity{
		CommonName: "nomad-node-a", ClusterID: "cluster-a", NodeID: "node-a",
		NodeUID: "node-uid-a", AgentUID: "agent-a",
	}
}

func TestCertificateVerifierReturnsAuthenticatedRoute(t *testing.T) {
	verifier, err := NewCertificateVerifier([]CertificateIdentity{testCertificateIdentity()})
	if err != nil {
		t.Fatal(err)
	}
	identity, err := verifier.Verify(context.Background(), "Bearer nomad-node-a")
	if err != nil {
		t.Fatal(err)
	}
	if identity.ClusterID != "cluster-a" || identity.NodeID != "node-a" ||
		identity.NodeUID != "node-uid-a" || identity.AgentUID != "agent-a" {
		t.Fatalf("identity = %+v", identity)
	}
}

func TestCertificateMiddlewareMapsVerifiedCertificate(t *testing.T) {
	var gotAuthorization string
	inner := http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		gotAuthorization = request.Header.Get("Authorization")
		writer.WriteHeader(http.StatusOK)
	})
	handler, err := NewCertificateMiddleware([]CertificateIdentity{testCertificateIdentity()}, inner)
	if err != nil {
		t.Fatalf("NewCertificateMiddleware() error = %v", err)
	}
	certificate := &x509.Certificate{Subject: pkix.Name{CommonName: "nomad-node-a"}}
	request := httptest.NewRequest(http.MethodPut, "/test", nil)
	request.TLS = &tls.ConnectionState{
		PeerCertificates: []*x509.Certificate{certificate},
		VerifiedChains:   [][]*x509.Certificate{{certificate}},
	}
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", response.Code)
	}
	if gotAuthorization != "Bearer nomad-node-a" {
		t.Fatalf("authorization = %q", gotAuthorization)
	}
}

func TestCertificateMiddlewareRejectsUnverifiedOrUnknownCertificate(t *testing.T) {
	handler, err := NewCertificateMiddleware([]CertificateIdentity{testCertificateIdentity()}, http.NotFoundHandler())
	if err != nil {
		t.Fatal(err)
	}
	for _, commonName := range []string{"nomad-node-a", "unknown"} {
		request := httptest.NewRequest(http.MethodPut, "/test", nil)
		certificate := &x509.Certificate{Subject: pkix.Name{CommonName: commonName}}
		request.TLS = &tls.ConnectionState{PeerCertificates: []*x509.Certificate{certificate}}
		if commonName == "unknown" {
			request.TLS.VerifiedChains = [][]*x509.Certificate{{certificate}}
		}
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != http.StatusUnauthorized {
			t.Fatalf("common name %q status = %d, want 401", commonName, response.Code)
		}
	}
}

func TestCertificateIdentityCatalogRejectsAmbiguousEntries(t *testing.T) {
	identity := testCertificateIdentity()
	if _, err := NewCertificateVerifier([]CertificateIdentity{identity, identity}); err == nil {
		t.Fatal("duplicate certificate identity was accepted")
	}
	identity.NodeID = ""
	if _, err := NewCertificateVerifier([]CertificateIdentity{identity}); err == nil {
		t.Fatal("partial route identity was accepted")
	}
}
