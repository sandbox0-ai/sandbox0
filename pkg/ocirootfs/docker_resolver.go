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
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"syscall"

	"github.com/containerd/containerd/v2/core/remotes"
	"github.com/containerd/containerd/v2/core/remotes/docker"
	"golang.org/x/sys/unix"
)

const (
	maxDockerCredentialsBytes = 1 << 20
	maxDockerAuthEntries      = 256
	maxDockerCredentialBytes  = 16 << 10
)

// DockerResolverConfig defines a pull-only resolver. Credentials are loaded
// from one dedicated root-owned file and remain in process memory only.
type DockerResolverConfig struct {
	CredentialsFile string
	PlainHTTPHosts  []string
}

// NewDockerResolver creates a registry resolver with exact-host credentials
// and an explicit allowlist for plaintext development registries.
func NewDockerResolver(config DockerResolverConfig) (remotes.Resolver, error) {
	return newDockerResolverOwnedBy(config, 0)
}

func newDockerResolverOwnedBy(config DockerResolverConfig, expectedUID uint32) (remotes.Resolver, error) {
	credentials, err := loadDockerCredentialsOwnedBy(config.CredentialsFile, expectedUID)
	if err != nil {
		return nil, err
	}
	plainHTTPHosts, err := normalizeRegistryHosts(config.PlainHTTPHosts)
	if err != nil {
		return nil, err
	}
	credentialCallback := func(host string) (string, string, error) {
		normalized, err := normalizeRegistryHost(host)
		if err != nil {
			return "", "", nil
		}
		credential, found := credentials[normalized]
		if !found {
			return "", "", nil
		}
		return credential.username, credential.password, nil
	}
	authorizer := docker.NewDockerAuthorizer(docker.WithAuthCreds(credentialCallback))
	hosts := docker.ConfigureDefaultRegistries(
		docker.WithAuthorizer(authorizer),
		docker.WithPlainHTTP(func(host string) (bool, error) {
			normalized, err := normalizeRegistryHost(host)
			if err != nil {
				return false, err
			}
			_, allowed := plainHTTPHosts[normalized]
			return allowed, nil
		}),
	)
	return docker.NewResolver(docker.ResolverOptions{Hosts: hosts}), nil
}

type dockerCredential struct {
	username string
	password string
}

type dockerCredentialsFile struct {
	Auths map[string]dockerAuthEntry `json:"auths"`
}

type dockerAuthEntry struct {
	Username string `json:"username"`
	Password string `json:"password"`
	Auth     string `json:"auth"`
}

func loadDockerCredentialsOwnedBy(path string, expectedUID uint32) (map[string]dockerCredential, error) {
	if path == "" {
		return map[string]dockerCredential{}, nil
	}
	clean := filepath.Clean(strings.TrimSpace(path))
	if path != clean || !filepath.IsAbs(clean) || clean == string(filepath.Separator) {
		return nil, fmt.Errorf("OCI pull credentials path must be a canonical non-root absolute path")
	}
	fd, err := unix.Open(clean, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return nil, fmt.Errorf("open OCI pull credentials: %w", err)
	}
	file := os.NewFile(uintptr(fd), clean)
	if file == nil {
		_ = unix.Close(fd)
		return nil, fmt.Errorf("wrap OCI pull credentials file descriptor")
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat OCI pull credentials: %w", err)
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 || info.Size() <= 0 ||
		info.Size() > maxDockerCredentialsBytes || stat.Nlink != 1 || stat.Uid != expectedUID {
		return nil, fmt.Errorf("OCI pull credentials must be an expected-owner mode-0600 single-link file within 1..%d bytes", maxDockerCredentialsBytes)
	}
	payload, err := io.ReadAll(io.LimitReader(file, maxDockerCredentialsBytes+1))
	if err != nil {
		return nil, fmt.Errorf("read OCI pull credentials: %w", err)
	}
	if int64(len(payload)) != info.Size() {
		return nil, fmt.Errorf("OCI pull credentials changed while being read")
	}
	var document dockerCredentialsFile
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&document); err != nil {
		return nil, fmt.Errorf("decode OCI pull credentials: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return nil, fmt.Errorf("OCI pull credentials must contain exactly one JSON value")
	}
	if len(document.Auths) == 0 || len(document.Auths) > maxDockerAuthEntries {
		return nil, fmt.Errorf("OCI pull credentials must contain 1..%d auth entries", maxDockerAuthEntries)
	}
	result := make(map[string]dockerCredential, len(document.Auths))
	keys := make([]string, 0, len(document.Auths))
	for key := range document.Auths {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		host, err := normalizeDockerAuthKey(key)
		if err != nil {
			return nil, err
		}
		entry := document.Auths[key]
		credential, err := entry.credential()
		if err != nil {
			return nil, fmt.Errorf("decode OCI pull credential for %s: %w", host, err)
		}
		if _, found := result[host]; found {
			return nil, fmt.Errorf("OCI pull credentials contain duplicate registry host %s", host)
		}
		result[host] = credential
	}
	return result, nil
}

func (entry dockerAuthEntry) credential() (dockerCredential, error) {
	if len(entry.Username) > maxDockerCredentialBytes || len(entry.Password) > maxDockerCredentialBytes ||
		len(entry.Auth) > 2*maxDockerCredentialBytes {
		return dockerCredential{}, fmt.Errorf("credential exceeds configured bounds")
	}
	if entry.Auth != "" && (entry.Username != "" || entry.Password != "") {
		return dockerCredential{}, fmt.Errorf("credential must use either auth or username/password")
	}
	username, password := entry.Username, entry.Password
	if entry.Auth != "" {
		decoded, err := base64.StdEncoding.DecodeString(entry.Auth)
		if err != nil || len(decoded) > 2*maxDockerCredentialBytes {
			return dockerCredential{}, fmt.Errorf("auth field is not bounded canonical base64")
		}
		var found bool
		username, password, found = strings.Cut(string(decoded), ":")
		if !found || base64.StdEncoding.EncodeToString(decoded) != entry.Auth {
			return dockerCredential{}, fmt.Errorf("auth field must encode username:password")
		}
	}
	if username == "" || password == "" || strings.ContainsRune(username, '\x00') || strings.ContainsRune(password, '\x00') {
		return dockerCredential{}, fmt.Errorf("username and password are required")
	}
	return dockerCredential{username: username, password: password}, nil
}

func normalizeRegistryHosts(values []string) (map[string]struct{}, error) {
	if len(values) > maxDockerAuthEntries {
		return nil, fmt.Errorf("plain HTTP registry allowlist exceeds %d hosts", maxDockerAuthEntries)
	}
	result := make(map[string]struct{}, len(values))
	for _, value := range values {
		host, err := normalizeRegistryHost(value)
		if err != nil {
			return nil, fmt.Errorf("plain HTTP registry host: %w", err)
		}
		if _, found := result[host]; found {
			return nil, fmt.Errorf("plain HTTP registry host %s is duplicated", host)
		}
		result[host] = struct{}{}
	}
	return result, nil
}

func normalizeDockerAuthKey(value string) (string, error) {
	if value == "" || value != strings.TrimSpace(value) || len(value) > 512 {
		return "", fmt.Errorf("OCI pull credential registry key is not canonical")
	}
	parsed := value
	if !strings.Contains(value, "://") {
		parsed = "https://" + value
	}
	registryURL, err := url.Parse(parsed)
	if err != nil || registryURL.User != nil || registryURL.Host == "" || registryURL.RawQuery != "" || registryURL.Fragment != "" {
		return "", fmt.Errorf("OCI pull credential registry key %q is invalid", value)
	}
	path := strings.TrimSuffix(registryURL.EscapedPath(), "/")
	if path != "" && path != "/v1" && path != "/v2" {
		return "", fmt.Errorf("OCI pull credential registry key %q contains a repository path", value)
	}
	return normalizeRegistryHost(registryURL.Host)
}

func normalizeRegistryHost(value string) (string, error) {
	if value == "" || value != strings.TrimSpace(value) || value != strings.ToLower(value) ||
		len(value) > 255 || strings.Contains(value, "://") || strings.ContainsAny(value, "/@?#") {
		return "", fmt.Errorf("registry host %q is not canonical", value)
	}
	parsed, err := url.Parse("https://" + value)
	if err != nil || parsed.Host != value || parsed.Hostname() == "" || parsed.User != nil || parsed.Path != "" {
		return "", fmt.Errorf("registry host %q is invalid", value)
	}
	return value, nil
}
