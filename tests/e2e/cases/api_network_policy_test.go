package cases

import (
	"os"
	"os/exec"
	"strings"
	"testing"
)

func TestAuditableEgressPythonScriptsCompile(t *testing.T) {
	testCases := []struct {
		name   string
		script string
	}{
		{
			name:   "helper services",
			script: extractAuditableEgressHelperPython(t, auditableEgressHelperServicesCommand()),
		},
		{
			name:   "fragmented http command",
			script: extractInlinePython(t, auditableEgressFragmentedHTTPRequestCommand("127.0.0.1")),
		},
		{
			name:   "opaque tcp command",
			script: extractInlinePython(t, auditableEgressOpaqueTCPCommand("127.0.0.1")),
		},
		{
			name:   "udp session command",
			script: extractInlinePython(t, auditableEgressUDPSessionCommand("127.0.0.1", "token")),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			compilePythonScript(t, tc.script)
		})
	}
}

func extractInlinePython(t *testing.T, command string) string {
	t.Helper()
	const prefix = "cat <<'PY' | python3\n"
	const suffix = "\nPY"
	if !strings.HasPrefix(command, prefix) || !strings.HasSuffix(command, suffix) {
		t.Fatalf("unexpected inline python command:\n%s", command)
	}
	return strings.TrimSuffix(strings.TrimPrefix(command, prefix), suffix)
}

func extractAuditableEgressHelperPython(t *testing.T, command string) string {
	t.Helper()
	const prefix = "set -eu\ncat <<'PY' >/tmp/auditable-egress-helper.py\n"
	const suffix = "\nPY\nnohup python3 /tmp/auditable-egress-helper.py >/tmp/auditable-egress-helper.log 2>&1 &\n"
	if !strings.HasPrefix(command, prefix) || !strings.HasSuffix(command, suffix) {
		t.Fatalf("unexpected helper command:\n%s", command)
	}
	return strings.TrimSuffix(strings.TrimPrefix(command, prefix), suffix)
}

func compilePythonScript(t *testing.T, script string) {
	t.Helper()

	file, err := os.CreateTemp("", "auditable-egress-python-*.py")
	if err != nil {
		t.Fatalf("create temp file: %v", err)
	}
	path := file.Name()
	defer os.Remove(path)

	if _, err := file.WriteString(script); err != nil {
		_ = file.Close()
		t.Fatalf("write temp file: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("close temp file: %v", err)
	}

	cmd := exec.Command("python3", "-c", "import pathlib; compile(pathlib.Path(__import__('sys').argv[1]).read_text(), __import__('sys').argv[1], 'exec')", path)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("compile python script: %v\n%s", err, string(output))
	}
}
