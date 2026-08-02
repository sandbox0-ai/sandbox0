package procd

import (
	"os"
	"strings"
	"testing"
)

func TestCodingAgentImageIncludesRemoteResizeVNCServer(t *testing.T) {
	dockerfile, err := os.ReadFile("Dockerfile.coding-agent")
	if err != nil {
		t.Fatalf("read coding-agent Dockerfile: %v", err)
	}

	contents := string(dockerfile)
	for _, expected := range []string{
		"tigervnc-standalone-server",
		"command -v Xtigervnc >/dev/null",
	} {
		if !strings.Contains(contents, expected) {
			t.Fatalf("coding-agent Dockerfile does not contain %q", expected)
		}
	}
}
