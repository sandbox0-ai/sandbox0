package trust

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestConfigureNetdMITMCATrustWritesBundleAndExportsTLSVars(t *testing.T) {
	dir := t.TempDir()
	mitmPath := filepath.Join(dir, "mitm-ca.crt")
	bundlePath := filepath.Join(dir, "bundle.crt")
	if err := os.WriteFile(mitmPath, []byte("-----BEGIN CERTIFICATE-----\nTEST\n-----END CERTIFICATE-----\n"), 0644); err != nil {
		t.Fatalf("write mitm ca: %v", err)
	}
	t.Setenv(NetdMITMCAFileEnv, mitmPath)
	t.Setenv(NetdMITMCABundleEnv, bundlePath)
	for _, name := range tlsBundleEnvVars {
		t.Setenv(name, "")
	}

	got, err := ConfigureNetdMITMCATrust()
	if err != nil {
		t.Fatalf("ConfigureNetdMITMCATrust: %v", err)
	}
	if got != bundlePath {
		t.Fatalf("bundle path = %q, want %q", got, bundlePath)
	}
	data, err := os.ReadFile(bundlePath)
	if err != nil {
		t.Fatalf("read bundle: %v", err)
	}
	if !strings.Contains(string(data), "TEST") {
		t.Fatalf("bundle missing MITM CA: %q", string(data))
	}
	for _, name := range tlsBundleEnvVars {
		if value := os.Getenv(name); value != bundlePath {
			t.Fatalf("%s = %q, want %q", name, value, bundlePath)
		}
	}
}

func TestConfigureNetdMITMCATrustNoopsWithoutCAEnv(t *testing.T) {
	t.Setenv(NetdMITMCAFileEnv, "")

	got, err := ConfigureNetdMITMCATrust()
	if err != nil {
		t.Fatalf("ConfigureNetdMITMCATrust: %v", err)
	}
	if got != "" {
		t.Fatalf("bundle path = %q, want empty", got)
	}
}
