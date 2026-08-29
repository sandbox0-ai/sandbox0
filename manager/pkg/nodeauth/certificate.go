package nodeauth

import (
	"context"
	"crypto/x509"
	"fmt"
	"net/http"
	"strings"
)

// CertificateIdentity maps one verified client certificate common name to
// the durable node identity used by regional authorities.
type CertificateIdentity struct {
	CommonName string
	ClusterID  string
	NodeID     string
	NodeUID    string
	AgentUID   string
}

type certificateVerifier struct {
	identities map[string]CertificateIdentity
	lookup     CertificateIdentityLookup
}

// CertificateIdentityLookup resolves dynamically enrolled worker identities
// from the regional authority. Revoked workers must not be returned.
type CertificateIdentityLookup func(context.Context, string) (CertificateIdentity, error)

// NewCertificateVerifier returns a verifier backed by an immutable client
// certificate identity catalog.
func NewCertificateVerifier(identities []CertificateIdentity) (Verifier, error) {
	return NewCertificateVerifierWithLookup(identities, nil)
}

// NewCertificateVerifierWithLookup combines migration-time static identities
// with the dynamic worker catalog. Static identities remain exact and cannot
// be shadowed by a dynamic record.
func NewCertificateVerifierWithLookup(identities []CertificateIdentity, lookup CertificateIdentityLookup) (Verifier, error) {
	byName, err := certificateIdentityMap(identities)
	if err != nil {
		if lookup == nil || len(identities) != 0 {
			return nil, err
		}
		byName = map[string]CertificateIdentity{}
	}
	return certificateVerifier{identities: byName, lookup: lookup}, nil
}

func (v certificateVerifier) Verify(ctx context.Context, bearer string) (Identity, error) {
	fields := strings.Fields(bearer)
	if len(fields) == 2 && strings.EqualFold(fields[0], "Bearer") {
		bearer = fields[1]
	}
	name := strings.TrimSpace(bearer)
	identity, ok := v.identities[name]
	if !ok && name != "" && v.lookup != nil {
		var err error
		identity, err = v.lookup(ctx, name)
		if err == nil {
			ok = true
		}
	}
	if !ok || name == "" || identity.CommonName != name {
		return Identity{}, fmt.Errorf("unknown node authority client %q", name)
	}
	return Identity{
		ClusterID: identity.ClusterID, NodeID: identity.NodeID,
		NodeUID: identity.NodeUID, AgentUID: identity.AgentUID,
	}, nil
}

// NewCertificateMiddleware projects a verified client certificate identity
// into the bearer verifier contract shared by the authority handlers. The TLS
// listener must already require and verify client certificates; this wrapper
// checks that invariant again and fails closed if it is miswired.
func NewCertificateMiddleware(identities []CertificateIdentity, next http.Handler) (http.Handler, error) {
	verifier, err := NewCertificateVerifier(identities)
	if err != nil {
		return nil, err
	}
	return NewVerifiedCertificateMiddleware(verifier, next)
}

// NewVerifiedCertificateMiddleware accepts only a TLS-verified certificate
// whose common name resolves through the same verifier used by handlers.
func NewVerifiedCertificateMiddleware(verifier Verifier, next http.Handler) (http.Handler, error) {
	if verifier == nil {
		return nil, fmt.Errorf("node authority certificate verifier is required")
	}
	if next == nil {
		return nil, fmt.Errorf("node authority handler is required")
	}
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.TLS == nil || len(request.TLS.PeerCertificates) == 0 || len(request.TLS.VerifiedChains) == 0 {
			http.Error(writer, "node authority requires verified mTLS", http.StatusUnauthorized)
			return
		}
		name := certificateCommonName(request.TLS.PeerCertificates[0])
		if _, err := verifier.Verify(request.Context(), name); err != nil {
			http.Error(writer, "unknown node authority client certificate", http.StatusUnauthorized)
			return
		}
		request.Header.Set("Authorization", "Bearer "+name)
		next.ServeHTTP(writer, request)
	}), nil
}

func certificateIdentityMap(identities []CertificateIdentity) (map[string]CertificateIdentity, error) {
	if len(identities) == 0 {
		return nil, fmt.Errorf("at least one node authority client identity is required")
	}
	byName := make(map[string]CertificateIdentity, len(identities))
	for index, identity := range identities {
		identity.CommonName = strings.TrimSpace(identity.CommonName)
		identity.ClusterID = strings.TrimSpace(identity.ClusterID)
		identity.NodeID = strings.TrimSpace(identity.NodeID)
		identity.NodeUID = strings.TrimSpace(identity.NodeUID)
		identity.AgentUID = strings.TrimSpace(identity.AgentUID)
		if identity.CommonName == "" || identity.NodeUID == "" || identity.AgentUID == "" {
			return nil, fmt.Errorf("node authority client identity %d requires common name, node UID, and agent UID", index)
		}
		if (identity.ClusterID == "") != (identity.NodeID == "") {
			return nil, fmt.Errorf("node authority client identity %d must configure cluster and node IDs together", index)
		}
		if _, exists := byName[identity.CommonName]; exists {
			return nil, fmt.Errorf("duplicate node authority client common name %q", identity.CommonName)
		}
		byName[identity.CommonName] = identity
	}
	return byName, nil
}

func certificateCommonName(certificate *x509.Certificate) string {
	if certificate == nil {
		return ""
	}
	return strings.TrimSpace(certificate.Subject.CommonName)
}
