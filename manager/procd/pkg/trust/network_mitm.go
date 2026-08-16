package trust

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

const (
	NetworkMITMCAFileEnv   = "SANDBOX0_NETWORK_MITM_CA_FILE"
	NetworkMITMCABundleEnv = "SANDBOX0_NETWORK_CA_BUNDLE_FILE"
	defaultBundlePath      = "/tmp/sandbox0/network-ca-bundle.crt"
	defaultBundleFileMod   = 0644
)

var tlsBundleEnvVars = []string{
	"NODE_EXTRA_CA_CERTS",
	"SSL_CERT_FILE",
	"REQUESTS_CA_BUNDLE",
	"CURL_CA_BUNDLE",
	"GIT_SSL_CAINFO",
	"AWS_CA_BUNDLE",
}

var systemCABundleCandidates = []string{
	"/etc/ssl/certs/ca-certificates.crt",
	"/etc/pki/tls/certs/ca-bundle.crt",
	"/etc/ssl/ca-bundle.pem",
	"/etc/pki/ca-trust/extracted/pem/tls-ca-bundle.pem",
	"/etc/ssl/cert.pem",
}

// ConfigureNetworkMITMCATrust exposes the network-runtime MITM CA through common TLS environment variables.
func ConfigureNetworkMITMCATrust() (string, error) {
	mitmCAPath := strings.TrimSpace(os.Getenv(NetworkMITMCAFileEnv))
	if mitmCAPath == "" {
		return "", nil
	}
	mitmCA, err := os.ReadFile(mitmCAPath)
	if err != nil {
		return "", fmt.Errorf("read network-runtime MITM CA %s: %w", mitmCAPath, err)
	}
	bundlePath := strings.TrimSpace(os.Getenv(NetworkMITMCABundleEnv))
	if bundlePath == "" {
		bundlePath = defaultBundlePath
	}
	bundle := buildBundle(mitmCA)
	if err := os.MkdirAll(filepath.Dir(bundlePath), 0755); err != nil {
		return "", fmt.Errorf("create network-runtime CA bundle directory: %w", err)
	}
	if err := os.WriteFile(bundlePath, bundle, defaultBundleFileMod); err != nil {
		return "", fmt.Errorf("write network-runtime CA bundle %s: %w", bundlePath, err)
	}
	_ = os.Setenv(NetworkMITMCABundleEnv, bundlePath)
	for _, name := range tlsBundleEnvVars {
		_ = os.Setenv(name, bundlePath)
	}
	return bundlePath, nil
}

func buildBundle(mitmCA []byte) []byte {
	var bundle bytes.Buffer
	if systemCA, ok := readFirstSystemCABundle(); ok {
		bundle.Write(bytes.TrimSpace(systemCA))
		bundle.WriteByte('\n')
	}
	bundle.Write(bytes.TrimSpace(mitmCA))
	bundle.WriteByte('\n')
	return bundle.Bytes()
}

func readFirstSystemCABundle() ([]byte, bool) {
	for _, path := range systemCABundleCandidates {
		data, err := os.ReadFile(path)
		if err == nil && len(bytes.TrimSpace(data)) > 0 {
			return data, true
		}
	}
	return nil, false
}
