//go:build linux

package redirect

import (
	"context"
	_ "embed"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

//go:embed testdata/routed_udp.py
var routedUDPTest string

// Run in an isolated privileged Linux test container. The subprocess creates
// fresh network and mount namespaces before any link or firewall mutation.
func TestRoutedUDPRedirectKernelIntegration(t *testing.T) {
	if os.Getenv("SANDBOX0_NETWORK_REDIRECT_INTEGRATION") != "1" {
		t.Skip("requires an isolated privileged Linux network test container")
	}
	directory := t.TempDir()
	script, config := filepath.Join(directory, "routed_udp.py"), filepath.Join(directory, "rules.json")
	payload, err := json.Marshal(map[string]string{
		"rules": buildIPTablesRestoreInput(Config{ProxyHTTPPort: 18080, ProxyHTTPSPort: 18443}, nil),
		"ipset": buildIPSetRestoreInput([]string{"192.0.2.2"}),
	})
	if err != nil {
		t.Fatal(err)
	}
	for path, data := range map[string][]byte{script: []byte(routedUDPTest), config: payload} {
		if err := os.WriteFile(path, data, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, "unshare", "--net", "--mount", "--propagation", "private", "--fork",
		"sh", "-ec", `mkdir -p /run/netns; mount -t tmpfs -o size=1m tmpfs /run/netns; exec python3 "$1" "$2"`,
		"routed-udp-test", script, config)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("routed UDP kernel acceptance: %v\n%s", err, output)
	} else {
		t.Logf("%s", output)
	}
}
