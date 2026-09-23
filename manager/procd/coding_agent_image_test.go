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
		"playwright install chromium --with-deps --no-shell",
		"openbox", "tigervnc-standalone-server", "SANDPI_BROWSER_USER",
		"chrome_sandbox", "chmod 4755",
	} {
		if !strings.Contains(contents, expected) {
			t.Fatalf("coding-agent Dockerfile does not contain %q", expected)
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
		"kimi --version",
		"zcode --version",
	} {
		if !strings.Contains(string(dockerfile), expected) {
			t.Fatalf("coding-agent Dockerfile does not contain %q", expected)
		}
	}
}
