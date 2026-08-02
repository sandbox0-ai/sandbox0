package procd

import (
	"os"
	"strings"
	"testing"
)

func TestCodingAgentImageKeepsPlaywrightWithoutBundledBrowser(t *testing.T) {
	dockerfile, err := os.ReadFile("Dockerfile.coding-agent")
	if err != nil {
		t.Fatalf("read coding-agent Dockerfile: %v", err)
	}

	contents := string(dockerfile)
	for _, expected := range []string{
		"PLAYWRIGHT_SKIP_BROWSER_DOWNLOAD=1",
		"playwright-cli; do ln -sf",
		"playwright-cli --version",
	} {
		if !strings.Contains(contents, expected) {
			t.Fatalf("coding-agent Dockerfile does not contain %q", expected)
		}
	}

	for _, forbidden := range []string{
		"playwright install chromium",
		"openbox",
		"tigervnc-standalone-server",
		"x11vnc",
		"xvfb",
		"SANDPI_BROWSER_USER",
		"chrome_sandbox",
	} {
		if strings.Contains(contents, forbidden) {
			t.Fatalf("coding-agent Dockerfile unexpectedly contains %q", forbidden)
		}
	}
}
