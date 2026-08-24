package ocirootfs

import (
	"encoding/base64"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writeCredentialsFile(t *testing.T, payload string, mode os.FileMode) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "pull-credentials.json")
	if err := os.WriteFile(path, []byte(payload), mode); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestLoadDockerCredentialsUsesExactCanonicalHosts(t *testing.T) {
	auth := base64.StdEncoding.EncodeToString([]byte("robot:secret"))
	path := writeCredentialsFile(t, `{"auths":{"registry.example.com":{"username":"user","password":"password"},"https://registry.example.com:5443/v2/":{"auth":"`+auth+`"}}}`, 0o600)
	credentials, err := loadDockerCredentialsOwnedBy(path, uint32(os.Geteuid()))
	if err != nil {
		t.Fatal(err)
	}
	if credentials["registry.example.com"] != (dockerCredential{username: "user", password: "password"}) ||
		credentials["registry.example.com:5443"] != (dockerCredential{username: "robot", password: "secret"}) {
		t.Fatalf("unexpected credentials: %#v", credentials)
	}
	if _, found := credentials["other.example.com"]; found {
		t.Fatal("credential escaped its exact registry host")
	}
	if _, err := NewDockerResolver(DockerResolverConfig{
		PlainHTTPHosts: []string{"registry.example.com:5443"},
	}); err != nil {
		t.Fatal(err)
	}
}

func TestLoadDockerCredentialsRejectsUnsafeFilesAndDocuments(t *testing.T) {
	valid := `{"auths":{"registry.example.com":{"username":"user","password":"password"}}}`
	worldReadable := writeCredentialsFile(t, valid, 0o644)
	symlink := filepath.Join(t.TempDir(), "credentials-link")
	if err := os.Symlink(worldReadable, symlink); err != nil {
		t.Fatal(err)
	}
	for name, path := range map[string]string{
		"world readable": worldReadable,
		"symlink":        symlink,
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := loadDockerCredentialsOwnedBy(path, uint32(os.Geteuid())); err == nil {
				t.Fatal("expected unsafe file error")
			}
		})
	}
	for name, payload := range map[string]string{
		"unknown top level":  `{"auths":{"registry.example.com":{"username":"user","password":"password"}},"credsStore":"helper"}`,
		"unknown auth field": `{"auths":{"registry.example.com":{"identitytoken":"token"}}}`,
		"repository path":    `{"auths":{"registry.example.com/team":{"username":"user","password":"password"}}}`,
		"duplicate host":     `{"auths":{"registry.example.com":{"username":"a","password":"b"},"https://registry.example.com/v2/":{"username":"c","password":"d"}}}`,
		"mixed forms":        `{"auths":{"registry.example.com":{"username":"a","password":"b","auth":"YTpi"}}}`,
		"empty password":     `{"auths":{"registry.example.com":{"username":"a","password":""}}}`,
	} {
		t.Run(name, func(t *testing.T) {
			path := writeCredentialsFile(t, payload, 0o600)
			if _, err := loadDockerCredentialsOwnedBy(path, uint32(os.Geteuid())); err == nil {
				t.Fatal("expected invalid document error")
			}
		})
	}
}

func TestLoadDockerCredentialsRejectsOversizeBeforeDecode(t *testing.T) {
	path := writeCredentialsFile(t, strings.Repeat("x", maxDockerCredentialsBytes+1), 0o600)
	if _, err := loadDockerCredentialsOwnedBy(path, uint32(os.Geteuid())); err == nil {
		t.Fatal("expected size bound error")
	}
}

func TestNormalizeRegistryHostsRejectsAliasesAndDuplicates(t *testing.T) {
	for _, hosts := range [][]string{
		{"https://registry.example.com"},
		{"Registry.example.com"},
		{"registry.example.com/team"},
		{"registry.example.com", "registry.example.com"},
	} {
		if _, err := normalizeRegistryHosts(hosts); err == nil {
			t.Fatalf("expected invalid hosts: %#v", hosts)
		}
	}
}

func TestDockerRedirectPolicyRejectsUnsafeTargets(t *testing.T) {
	origin, err := http.NewRequest(http.MethodGet, "https://registry.example/v2/team/image/blobs/value", nil)
	if err != nil {
		t.Fatal(err)
	}
	tenHops := make([]*http.Request, maxDockerRedirects)
	for index := range tenHops {
		tenHops[index] = origin
	}
	for name, test := range map[string]struct {
		target string
		via    []*http.Request
	}{
		"no origin":          {target: "https://mirror.example/blob"},
		"too many hops":      {target: "https://mirror.example/blob", via: tenHops},
		"URL credentials":    {target: "https://user:password@mirror.example/blob", via: []*http.Request{origin}},
		"unsupported scheme": {target: "ftp://mirror.example/blob", via: []*http.Request{origin}},
		"unlisted plaintext": {target: "http://mirror.example/blob", via: []*http.Request{origin}},
		"noncanonical host":  {target: "https://Mirror.example/blob", via: []*http.Request{origin}},
	} {
		t.Run(name, func(t *testing.T) {
			redirect, err := http.NewRequest(http.MethodGet, test.target, nil)
			if err != nil {
				t.Fatal(err)
			}
			if err := dockerRedirectPolicy(map[string]struct{}{})(redirect, test.via); err == nil {
				t.Fatal("expected redirect policy error")
			}
		})
	}
}

func TestDockerRedirectPolicyStripsSensitiveHeadersAcrossHosts(t *testing.T) {
	origin, err := http.NewRequest(http.MethodGet, "https://registry.example/v2/team/image/blobs/value", nil)
	if err != nil {
		t.Fatal(err)
	}
	redirect, err := http.NewRequest(http.MethodGet, "https://mirror.example/blob", nil)
	if err != nil {
		t.Fatal(err)
	}
	redirect.Header.Set("Authorization", "Basic secret")
	redirect.Header.Set("Proxy-Authorization", "Basic proxy-secret")
	redirect.Header.Set("Cookie", "registry-session=secret")
	if err := dockerRedirectPolicy(nil)(redirect, []*http.Request{origin}); err != nil {
		t.Fatal(err)
	}
	for _, header := range []string{"Authorization", "Proxy-Authorization", "Cookie"} {
		if redirect.Header.Get(header) != "" {
			t.Fatalf("redirect retained %s", header)
		}
	}
	sameHost, err := http.NewRequest(http.MethodGet, "https://registry.example/another", nil)
	if err != nil {
		t.Fatal(err)
	}
	sameHost.Header.Set("Authorization", "Basic same-host")
	if err := dockerRedirectPolicy(nil)(sameHost, []*http.Request{origin}); err != nil {
		t.Fatal(err)
	}
	if sameHost.Header.Get("Authorization") == "" {
		t.Fatal("same-host redirect unexpectedly removed authorization")
	}
}
