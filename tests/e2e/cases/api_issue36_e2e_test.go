package cases

import (
	"os/exec"
	"strings"
	"testing"
)

func TestIssue36GeneratedPythonScriptsAreSyntacticallyValid(t *testing.T) {
	if _, err := exec.LookPath("python3"); err != nil {
		t.Skip("python3 not installed")
	}

	scripts := []struct {
		name   string
		script string
	}{
		{
			name:   "helper",
			script: extractIssue36HelperPython(t, issue36HelperPodCommand()),
		},
		{
			name:   "fragmented_http",
			script: extractInlinePython(t, issue36FragmentedHTTPRequestCommand("127.0.0.1")),
		},
		{
			name:   "opaque_tcp",
			script: extractInlinePython(t, issue36OpaqueTCPCommand("127.0.0.1")),
		},
		{
			name:   "udp_session",
			script: extractInlinePython(t, issue36UDPSessionCommand("127.0.0.1", "token")),
		},
	}

	for _, tc := range scripts {
		t.Run(tc.name, func(t *testing.T) {
			cmd := exec.Command("python3", "-c", "import sys; compile(sys.stdin.read(), '<stdin>', 'exec')")
			cmd.Stdin = strings.NewReader(tc.script)
			output, err := cmd.CombinedOutput()
			if err != nil {
				t.Fatalf("python syntax check failed: %v\n%s", err, string(output))
			}
		})
	}
}

func extractInlinePython(t *testing.T, command string) string {
	t.Helper()
	const prefix = "python3 - <<'PY'\n"
	const suffix = "\nPY"
	return extractBetween(t, command, prefix, suffix)
}

func extractIssue36HelperPython(t *testing.T, command string) string {
	t.Helper()
	const prefix = "cat <<'PY' >/tmp/issue36-helper.py\n"
	const suffix = "\nPY\nexec python3 /tmp/issue36-helper.py"
	return extractBetween(t, command, prefix, suffix)
}

func extractBetween(t *testing.T, value, prefix, suffix string) string {
	t.Helper()
	start := strings.Index(value, prefix)
	if start < 0 {
		t.Fatalf("prefix %q not found in command", prefix)
	}
	start += len(prefix)
	end := strings.Index(value[start:], suffix)
	if end < 0 {
		t.Fatalf("suffix %q not found in command", suffix)
	}
	return value[start : start+end]
}
