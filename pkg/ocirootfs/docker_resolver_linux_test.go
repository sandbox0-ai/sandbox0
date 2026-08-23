//go:build linux

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

package ocirootfs

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/opencontainers/go-digest"
)

func TestDockerResolverAuthenticatesAndImportsPinnedIndex(t *testing.T) {
	layer := testLayer(t, testTarEntry{name: "etc/source", body: "authenticated-registry\n"})
	fixture := newOCIImportFixture(t, []testLayerBlob{layer})
	registry := &authenticatedOCIRegistry{
		username: "rootfs-importer", password: "test-password",
		blobs: fixture.resolver.blobs,
		mediaTypes: map[digest.Digest]string{
			fixture.sourceDescriptor.Digest:   fixture.sourceDescriptor.MediaType,
			fixture.manifestDescriptor.Digest: fixture.manifestDescriptor.MediaType,
			fixture.configDescriptor.Digest:   fixture.configDescriptor.MediaType,
			layer.descriptor.Digest:           layer.descriptor.MediaType,
		},
	}
	server := httptest.NewServer(registry)
	t.Cleanup(server.Close)
	host := strings.TrimPrefix(server.URL, "http://")
	credentialsPayload, err := json.Marshal(dockerCredentialsFile{Auths: map[string]dockerAuthEntry{
		host: {Username: registry.username, Password: registry.password},
	}})
	if err != nil {
		t.Fatal(err)
	}
	credentialsPath := writeCredentialsFile(t, string(credentialsPayload), 0o600)
	resolver, err := newDockerResolverOwnedBy(DockerResolverConfig{
		CredentialsFile: credentialsPath,
		PlainHTTPHosts:  []string{host},
	}, uint32(os.Geteuid()))
	if err != nil {
		t.Fatal(err)
	}
	request := fixture.request()
	request.Reference = fmt.Sprintf("%s/sandbox/base@%s", host, fixture.sourceDescriptor.Digest)
	importer := &Importer{resolver: resolver, limits: testLimits(), allowNonRoot: true}
	result, err := importer.Import(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := os.RemoveAll(result.RootPath); err != nil {
			t.Error(err)
		}
	})
	if result.SourceDigest != fixture.sourceDescriptor.Digest ||
		result.ManifestDigest != fixture.manifestDescriptor.Digest ||
		result.ConfigDigest != fixture.configDescriptor.Digest {
		t.Fatalf("unexpected imported OCI identity: %#v", result)
	}
	source, err := os.ReadFile(filepath.Join(result.RootPath, "etc", "source"))
	if err != nil {
		t.Fatal(err)
	}
	if string(source) != "authenticated-registry\n" {
		t.Fatalf("unexpected imported source %q", source)
	}
	procd, err := os.ReadFile(filepath.Join(result.RootPath, "procd"))
	if err != nil {
		t.Fatal(err)
	}
	if string(procd) != testProcdPayload || result.ProcdDigest != fixture.procdDigest {
		t.Fatal("production procd identity was not preserved")
	}
	if registry.unauthorized.Load() == 0 || registry.authorized.Load() == 0 {
		t.Fatalf("registry auth challenge was not exercised: unauthorized=%d authorized=%d",
			registry.unauthorized.Load(), registry.authorized.Load())
	}
}

func TestDockerResolverFencesCrossHostPlaintextRedirects(t *testing.T) {
	layer := testLayer(t, testTarEntry{name: "redirected", body: "verified-redirect\n"})
	fixture := newOCIImportFixture(t, []testLayerBlob{layer})
	var mirrorRequests atomic.Int64
	var mirrorSensitiveHeaders atomic.Int64
	mirror := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		mirrorRequests.Add(1)
		if request.Header.Get("Authorization") != "" || request.Header.Get("Proxy-Authorization") != "" ||
			request.Header.Get("Cookie") != "" {
			mirrorSensitiveHeaders.Add(1)
		}
		response.Header().Set("Content-Type", layer.descriptor.MediaType)
		response.Header().Set("Content-Length", strconv.Itoa(len(layer.compressed)))
		response.Header().Set("Docker-Content-Digest", layer.descriptor.Digest.String())
		_, _ = response.Write(layer.compressed)
	}))
	t.Cleanup(mirror.Close)
	mirrorHost := strings.TrimPrefix(mirror.URL, "http://")
	registry := &authenticatedOCIRegistry{
		username: "rootfs-importer", password: "test-password",
		blobs: fixture.resolver.blobs,
		mediaTypes: map[digest.Digest]string{
			fixture.sourceDescriptor.Digest:   fixture.sourceDescriptor.MediaType,
			fixture.manifestDescriptor.Digest: fixture.manifestDescriptor.MediaType,
			fixture.configDescriptor.Digest:   fixture.configDescriptor.MediaType,
			layer.descriptor.Digest:           layer.descriptor.MediaType,
		},
		redirects: map[digest.Digest]string{
			layer.descriptor.Digest: mirror.URL + "/layer",
		},
	}
	server := httptest.NewServer(registry)
	t.Cleanup(server.Close)
	host := strings.TrimPrefix(server.URL, "http://")
	credentialsPayload, err := json.Marshal(dockerCredentialsFile{Auths: map[string]dockerAuthEntry{
		host: {Username: registry.username, Password: registry.password},
	}})
	if err != nil {
		t.Fatal(err)
	}
	credentialsPath := writeCredentialsFile(t, string(credentialsPayload), 0o600)
	request := fixture.request()
	request.Reference = fmt.Sprintf("%s/sandbox/base@%s", host, fixture.sourceDescriptor.Digest)
	importFrom := func(plainHTTPHosts []string) (Result, error) {
		resolver, resolverErr := newDockerResolverOwnedBy(DockerResolverConfig{
			CredentialsFile: credentialsPath, PlainHTTPHosts: plainHTTPHosts,
		}, uint32(os.Geteuid()))
		if resolverErr != nil {
			return Result{}, resolverErr
		}
		return (&Importer{resolver: resolver, limits: testLimits(), allowNonRoot: true}).Import(t.Context(), request)
	}

	if _, err := importFrom([]string{host}); err == nil {
		t.Fatal("expected redirect to a plaintext host outside the allowlist to fail")
	}
	if mirrorRequests.Load() != 0 {
		t.Fatal("disallowed redirect reached the mirror")
	}
	if entries := mustReadDir(t, fixture.workRoot); len(entries) != 0 {
		t.Fatalf("failed redirect left staging entries: %#v", entries)
	}
	result, err := importFrom([]string{host, mirrorHost})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := os.RemoveAll(result.RootPath); err != nil {
			t.Error(err)
		}
	})
	if mirrorRequests.Load() == 0 || mirrorSensitiveHeaders.Load() != 0 {
		t.Fatalf("unexpected mirror requests=%d sensitive_headers=%d",
			mirrorRequests.Load(), mirrorSensitiveHeaders.Load())
	}
	payload, err := os.ReadFile(filepath.Join(result.RootPath, "redirected"))
	if err != nil {
		t.Fatal(err)
	}
	if string(payload) != "verified-redirect\n" {
		t.Fatalf("unexpected redirected layer payload %q", payload)
	}
}

type authenticatedOCIRegistry struct {
	username   string
	password   string
	blobs      map[digest.Digest][]byte
	mediaTypes map[digest.Digest]string
	redirects  map[digest.Digest]string

	unauthorized atomic.Int64
	authorized   atomic.Int64
}

func (r *authenticatedOCIRegistry) ServeHTTP(response http.ResponseWriter, request *http.Request) {
	username, password, ok := request.BasicAuth()
	if !ok || username != r.username || password != r.password {
		r.unauthorized.Add(1)
		response.Header().Set("WWW-Authenticate", `Basic realm="sandbox0-test-registry"`)
		http.Error(response, "authentication required", http.StatusUnauthorized)
		return
	}
	r.authorized.Add(1)
	response.Header().Set("Docker-Distribution-Api-Version", "registry/2.0")
	if request.URL.Path == "/v2/" {
		response.WriteHeader(http.StatusOK)
		return
	}
	const repositoryPrefix = "/v2/sandbox/base/"
	remainder, found := strings.CutPrefix(request.URL.Path, repositoryPrefix)
	if !found {
		http.NotFound(response, request)
		return
	}
	var encodedDigest string
	if value, ok := strings.CutPrefix(remainder, "manifests/"); ok {
		encodedDigest = value
	} else if value, ok := strings.CutPrefix(remainder, "blobs/"); ok {
		encodedDigest = value
	} else {
		http.NotFound(response, request)
		return
	}
	parsed, err := digest.Parse(encodedDigest)
	if err != nil {
		http.Error(response, "invalid digest", http.StatusBadRequest)
		return
	}
	payload, found := r.blobs[parsed]
	if !found {
		http.NotFound(response, request)
		return
	}
	if request.Method != http.MethodGet && request.Method != http.MethodHead {
		response.Header().Set("Allow", "GET, HEAD")
		http.Error(response, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if target, redirect := r.redirects[parsed]; redirect && request.Method == http.MethodGet {
		response.Header().Set("Location", target)
		response.WriteHeader(http.StatusTemporaryRedirect)
		return
	}
	response.Header().Set("Content-Type", r.mediaTypes[parsed])
	response.Header().Set("Content-Length", strconv.Itoa(len(payload)))
	response.Header().Set("Docker-Content-Digest", parsed.String())
	response.WriteHeader(http.StatusOK)
	if request.Method == http.MethodGet {
		_, _ = response.Write(payload)
	}
}
