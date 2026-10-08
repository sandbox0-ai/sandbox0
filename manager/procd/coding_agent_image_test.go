package procd

import (
	"os"
	"strings"
	"testing"
)

func TestCodingAgentImageIncludesSharedHeadedBrowser(t *testing.T) {
	dockerfile, err := os.ReadFile("Dockerfile.coding-agent")
	if err != nil {
		t.Fatalf("read coding-agent Dockerfile: %v", err)
	}

	contents := string(dockerfile)
	for _, expected := range []string{
		"PLAYWRIGHT_SKIP_BROWSER_DOWNLOAD=1",
		"kimi playwright-cli; do ln -sf",
		"playwright-core/lib/tools/skills/playwright-cli/SKILL.md",
		"playwright-cli --version",
	} {
		if !strings.Contains(contents, expected) {
			t.Fatalf("coding-agent Dockerfile does not contain %q", expected)
		}
	}

	for _, expected := range []string{
		"bash install-chrome-stable.sh",
		"openbox", "tigervnc-standalone-server", "SANDBOX0_BROWSER_USER",
		"/usr/local/bin/sandbox0-browser", "NOVNC_SHA256", "sha256sum -c -",
		"/opt/sandbox0-browser/novnc/core/rfb.js", "sandbox0-browser --help",
	} {
		if !strings.Contains(contents, expected) {
			t.Fatalf("coding-agent Dockerfile does not contain %q", expected)
		}
	}
	if strings.Contains(contents, "playwright install chromium") {
		t.Fatal("coding-agent image must use Chrome Stable instead of Playwright's browser")
	}
	installer, err := os.ReadFile("coding-agents/install-chrome-stable.sh")
	if err != nil {
		t.Fatal(err)
	}
	for _, expected := range []string{
		"amd64|arm64", "google-chrome-stable_current_${arch}.deb",
		"google-chrome-stable --version", "4755 root:root",
	} {
		if !strings.Contains(string(installer), expected) {
			t.Fatalf("Chrome Stable installer does not contain %q", expected)
		}
	}
}

func TestCodingAgentImageIncludesPinnedTtydDiagnosticBinary(t *testing.T) {
	dockerfile, err := os.ReadFile("Dockerfile.coding-agent")
	if err != nil {
		t.Fatalf("read coding-agent Dockerfile: %v", err)
	}

	contents := string(dockerfile)
	for _, expected := range []string{
		"ARG TARGETARCH",
		"ARG TTYD_VERSION=1.7.7",
		"ttyd_asset=aarch64",
		"ttyd.${ttyd_asset}",
		"sha256sum -c -",
		"ttyd --version",
		"procd sessions remain the durable terminal authority",
	} {
		if !strings.Contains(contents, expected) {
			t.Fatalf("coding-agent Dockerfile does not contain %q", expected)
		}
	}
}

func TestCodingAgentImageIncludesKimiAndZCode(t *testing.T) {
	dockerfile, err := os.ReadFile("Dockerfile.coding-agent")
	if err != nil {
		t.Fatalf("read coding-agent Dockerfile: %v", err)
	}
	for _, expected := range []string{
		"node:24.14.0-bookworm",
		"setup_24.x",
		"ZCODE_COMMIT=328c1a0c0ffaa5a4f65e8fa199af5e4c20706e5f",
		"pnpm --filter @zcode/cli... build",
		"KIMI_COMMIT=21406fb4c805cc8c715e6d1f16ad3fb5f25f4fe3",
		"node build-kimi-sdk.mjs",
		"COPY --from=kimi-sdk-build /src/kimi/sdk-host/dist /opt/kimi-sdk",
		"Kimi CLI/SDK version mismatch",
		"kimi --version",
		"zcode --version",
	} {
		if !strings.Contains(string(dockerfile), expected) {
			t.Fatalf("coding-agent Dockerfile does not contain %q", expected)
		}
	}
}
