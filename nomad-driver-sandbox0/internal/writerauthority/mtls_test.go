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
	"crypto/x509/pkix"
	"net/http"
	"net/http/httptest"
	"testing"

	"crypto/tls"
	"crypto/x509"
)

func TestCertMiddlewareMapsAllowedCertificate(t *testing.T) {
	var gotAuthorization string
	inner := http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		gotAuthorization = request.Header.Get("Authorization")
		writer.WriteHeader(http.StatusOK)
	})
	handler, err := NewCertMiddleware([]CertIdentity{{
		CommonName: "nomad-node-a", NodeUID: "node-a", PodUID: "pod-a",
	}}, inner)
	if err != nil {
		t.Fatalf("NewCertMiddleware() error = %v", err)
	}
	request := httptest.NewRequest(http.MethodPut, "/test", nil)
	request.TLS = &tls.ConnectionState{PeerCertificates: []*x509.Certificate{{
		Subject: pkix.Name{CommonName: "nomad-node-a"},
	}}}
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", response.Code)
	}
	if gotAuthorization != "Bearer nomad-node-a" {
		t.Fatalf("authorization = %q", gotAuthorization)
	}
}

func TestCertMiddlewareRejectsUnknownCertificate(t *testing.T) {
	handler, err := NewCertMiddleware([]CertIdentity{{CommonName: "allowed"}}, http.NotFoundHandler())
	if err != nil {
		t.Fatalf("NewCertMiddleware() error = %v", err)
	}
	request := httptest.NewRequest(http.MethodPut, "/test", nil)
	request.TLS = &tls.ConnectionState{PeerCertificates: []*x509.Certificate{{
		Subject: pkix.Name{CommonName: "unknown"},
	}}}
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", response.Code)
	}
}
