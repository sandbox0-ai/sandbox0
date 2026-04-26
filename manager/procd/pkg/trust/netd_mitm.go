package trust

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

const (
	NetdMITMCAFileEnv    = "SANDBOX0_NETD_MITM_CA_FILE"
	NetdMITMCABundleEnv  = "SANDBOX0_NETD_CA_BUNDLE_FILE"
	defaultBundlePath    = "/tmp/sandbox0/netd-ca-bundle.crt"
	defaultBundleFileMod = 0644
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

// ConfigureNetdMITMCATrust exposes the netd MITM CA through common TLS env vars.
func ConfigureNetdMITMCATrust() (string, error) {
	mitmCAPath := strings.TrimSpace(os.Getenv(NetdMITMCAFileEnv))
	if mitmCAPath == "" {
		return "", nil
	}
	mitmCA, err := os.ReadFile(mitmCAPath)
	if err != nil {
		return "", fmt.Errorf("read netd MITM CA %s: %w", mitmCAPath, err)
	}
	bundlePath := strings.TrimSpace(os.Getenv(NetdMITMCABundleEnv))
	if bundlePath == "" {
		bundlePath = defaultBundlePath
	}
	bundle := buildBundle(mitmCA)
	if err := os.MkdirAll(filepath.Dir(bundlePath), 0755); err != nil {
		return "", fmt.Errorf("create netd CA bundle directory: %w", err)
	}
	if err := os.WriteFile(bundlePath, bundle, defaultBundleFileMod); err != nil {
		return "", fmt.Errorf("write netd CA bundle %s: %w", bundlePath, err)
	}
	_ = os.Setenv(NetdMITMCABundleEnv, bundlePath)
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
