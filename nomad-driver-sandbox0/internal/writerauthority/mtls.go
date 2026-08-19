// Copyright 2026 Sandbox0 Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package writerauthority

import (
	"context"
	"crypto/x509"
	"fmt"
	"net/http"
	"strings"

	managerauthority "github.com/sandbox0-ai/sandbox0/manager/pkg/rootfswriterauthority"
)

// CertIdentity maps a verified client certificate CN to a Nomad node identity.
type CertIdentity struct {
	CommonName string
	NodeUID    string
	PodUID     string
}

type certVerifier struct {
	identities map[string]CertIdentity
}

// NewCertVerifier returns a CallerVerifier backed by the mTLS certificate map.
func NewCertVerifier(identities []CertIdentity) managerauthority.CallerVerifier {
	return certVerifier{identities: certIdentityMap(identities)}
}

func (v certVerifier) Verify(_ context.Context, bearer string) (managerauthority.CallerIdentity, error) {
	name := strings.TrimSpace(strings.TrimPrefix(bearer, "Bearer "))
	identity, ok := v.identities[name]
	if !ok || name == "" {
		return managerauthority.CallerIdentity{}, fmt.Errorf("unknown writer authority client %q", name)
	}
	return managerauthority.CallerIdentity{NodeUID: identity.NodeUID, PodUID: identity.PodUID}, nil
}

// NewCertMiddleware converts the verified client certificate CN into the bearer
// credential expected by the existing manager handler.
func NewCertMiddleware(identities []CertIdentity, next http.Handler) (http.Handler, error) {
	byName := certIdentityMap(identities)
	if len(byName) == 0 {
		return nil, fmt.Errorf("at least one writer authority client identity is required")
	}
	if next == nil {
		return nil, fmt.Errorf("writer authority handler is required")
	}
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.TLS == nil || len(request.TLS.PeerCertificates) == 0 {
			http.Error(writer, "writer authority requires mTLS", http.StatusUnauthorized)
			return
		}
		certificate := request.TLS.PeerCertificates[0]
		identity := certCommonName(certificate)
		if _, ok := byName[identity]; !ok {
			http.Error(writer, "unknown writer authority client certificate", http.StatusUnauthorized)
			return
		}
		request.Header.Set("Authorization", "Bearer "+identity)
		next.ServeHTTP(writer, request)
	}), nil
}

func certIdentityMap(identities []CertIdentity) map[string]CertIdentity {
	byName := make(map[string]CertIdentity, len(identities))
	for _, identity := range identities {
		name := strings.TrimSpace(identity.CommonName)
		if name != "" {
			byName[name] = identity
		}
	}
	return byName
}

func certCommonName(certificate *x509.Certificate) string {
	return strings.TrimSpace(certificate.Subject.CommonName)
}
