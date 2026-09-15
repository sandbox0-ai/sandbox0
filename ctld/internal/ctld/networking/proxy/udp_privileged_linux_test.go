//go:build linux

package proxy

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"go.uber.org/zap/zaptest"
	"golang.org/x/sys/unix"
	"net"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/ctld/internal/ctld/networking/model"
	"github.com/sandbox0-ai/sandbox0/ctld/internal/ctld/networking/policy"
	"github.com/sandbox0-ai/sandbox0/ctld/internal/ctld/networking/redirect"
	"github.com/sandbox0-ai/sandbox0/pkg/config"
	v1alpha1 "github.com/sandbox0-ai/sandbox0/pkg/sandboxspec"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// Exercise actual generated redirect rules and transparent request/reply sockets.
// Ordinary loopback UDP tests cannot reproduce TPROXY's established-socket lookup.
func TestTransparentUDPKernelIntegration(t *testing.T) {
	if os.Getenv("SANDBOX0_NETWORK_REDIRECT_INTEGRATION") != "1" {
		t.Skip("requires an isolated privileged Linux network test container")
	}
	if os.Getenv("SANDBOX0_UDP_PROXY_TEST_CHILD") != "1" {
		binary, err := os.Executable()
		require.NoError(t, err)
		ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
		defer cancel()
		command := exec.CommandContext(ctx, "unshare", "--net", "--mount", "--propagation", "private", "--fork",
			"sh", "-ec", `mkdir -p /run/netns; mount -t tmpfs -o size=1m tmpfs /run/netns; exec "$1" -test.run='^TestTransparentUDPKernelIntegration$' -test.v`, "udp-proxy-test", binary)
		command.Env = append(os.Environ(), "SANDBOX0_UDP_PROXY_TEST_CHILD=1")
		output, err := command.CombinedOutput()
		require.NoError(t, err, "%s", output)
		t.Logf("%s", output)
		return
	}
	run := func(arguments ...string) {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		output, err := exec.CommandContext(ctx, arguments[0], arguments[1:]...).CombinedOutput()
		require.NoError(t, err, "%v: %s", arguments, output)
	}
	run("ip", "netns", "add", "guest")
	t.Cleanup(func() { run("ip", "netns", "delete", "guest") })
	run("ip", "link", "add", "host-veth", "type", "veth", "peer", "name", "guest-veth")
	run("ip", "link", "set", "guest-veth", "netns", "guest")
	run("ip", "addr", "add", "192.0.2.1/24", "dev", "host-veth")
	run("ip", "link", "set", "host-veth", "up")
	run("ip", "link", "set", "lo", "up")
	run("ip", "-n", "guest", "addr", "add", "192.0.2.2/24", "dev", "guest-veth")
	run("ip", "-n", "guest", "link", "set", "guest-veth", "up")
	run("ip", "-n", "guest", "link", "set", "lo", "up")
	run("ip", "-n", "guest", "route", "add", "default", "via", "192.0.2.1")
	redirector := redirect.NewManager(redirect.Config{ProxyHTTPPort: 18080, ProxyHTTPSPort: 18443}, zap.NewNop())
	require.NoError(t, redirector.Sync(t.Context(), []string{"192.0.2.2"}, nil))
	store := policy.NewStore(nil)
	registry, err := newAdapterRegistry([]proxyAdapter{&udpAdapter{}}, []proxyAdapter{&udpPassThroughAdapter{}})
	require.NoError(t, err)
	server := &Server{store: store, logger: zaptest.NewLogger(t), adapters: registry,
		udpClassifiers: defaultUDPClassifiers(), reassembler: newQuicReassembler(),
		cfg: &config.NetworkRuntimeConfig{ProxyUpstreamTimeout: config.Duration{Duration: 5 * time.Second}}}
	for _, port := range []int{18080, 18443} {
		conn, err := listenUDPTransparent(fmt.Sprintf("0.0.0.0:%d", port))
		require.NoError(t, err)
		t.Cleanup(func() { _ = conn.Close() })
		go server.handleUDP(t.Context(), conn)
	}
	t.Cleanup(server.closeUDPSessions)
	revision := 0
	setPolicy := func(mode v1alpha1.NetworkPolicyMode) {
		revision++
		raw, err := json.Marshal(v1alpha1.NetworkPolicySpec{Mode: mode, SandboxID: "owned", TeamID: "team"})
		require.NoError(t, err)
		store.ReconcileSandboxes([]*model.SandboxInfo{{Scope: "runtime-slot", Name: "slot", SourceIP: "192.0.2.2",
			OwnerKind: "runtime-slot", IncarnationID: "boot", Revision: fmt.Sprint(revision),
			NetworkPolicyHash: fmt.Sprint(revision), NetworkPolicy: string(raw)}})
		server.ReconcileActiveFlows()
	}
	// Include the actual-host destination case: an unconnected reply listener
	// bound to the original destination can accidentally steal local upstream I/O.
	for _, target := range []string{"192.0.2.1", "198.51.100.1"} {
		if target == "198.51.100.1" {
			run("ip", "addr", "add", target+"/32", "dev", "lo")
		}
		for _, port := range []int{23460, 443, 853} {
			t.Run(fmt.Sprintf("%s:%d", target, port), func(t *testing.T) {
				setPolicy(v1alpha1.NetworkModeAllowAll)
				lc := net.ListenConfig{Control: func(_, _ string, c syscall.RawConn) error {
					var sockErr error
					err := c.Control(func(fd uintptr) { sockErr = unix.SetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_REUSEADDR, 1) })
					if err != nil {
						return err
					}
					return sockErr
				}}
				pc, err := lc.ListenPacket(t.Context(), "udp4", net.JoinHostPort(target, fmt.Sprint(port)))
				require.NoError(t, err)
				echo := pc.(*net.UDPConn)
				defer echo.Close()
				peers := make(chan string, 16)
				go func() {
					buffer := make([]byte, 1024)
					for {
						n, peer, err := echo.ReadFromUDP(buffer)
						if err != nil {
							return
						}
						peers <- peer.IP.String()
						_, _ = echo.WriteToUDP(buffer[:n], peer)
					}
				}()
				ctx, cancel := context.WithTimeout(t.Context(), 12*time.Second)
				defer cancel()
				client := exec.CommandContext(ctx, "ip", "netns", "exec", "guest", "python3", "-u", "-c", `
import socket, sys
s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
s.settimeout(1)
s.connect((sys.argv[1], int(sys.argv[2])))
for line in sys.stdin:
    token = line.strip().encode()
    s.send(token)
    try:
        payload, source = s.recvfrom(1024)
        assert source == (sys.argv[1], int(sys.argv[2]))
        assert payload == token
        print('echo', flush=True)
    except socket.timeout:
        print('blocked', flush=True)
`, target, fmt.Sprint(port))
				input, err := client.StdinPipe()
				require.NoError(t, err)
				output, err := client.StdoutPipe()
				require.NoError(t, err)
				var stderr strings.Builder
				client.Stderr = &stderr
				require.NoError(t, client.Start())
				defer func() { _ = input.Close(); require.NoError(t, client.Wait(), "%s", stderr.String()) }()
				scanner := bufio.NewScanner(output)
				exchange := func(label, want string) {
					t.Helper()
					_, err := fmt.Fprintln(input, label)
					require.NoError(t, err)
					require.True(t, scanner.Scan(), "%s", stderr.String())
					require.Equal(t, want, scanner.Text(), label)
				}
				exchange("first", "echo")
				exchange("second", "echo")
				exchange("third", "echo")
				setPolicy(v1alpha1.NetworkModeBlockAll)
				exchange("revoked", "blocked")
				setPolicy(v1alpha1.NetworkModeAllowAll)
				exchange("restored-first", "echo")
				exchange("restored-second", "echo")
				for range 5 {
					select {
					case peer := <-peers:
						require.NotEqual(t, "192.0.2.2", peer, "guest bypassed the proxy")
					case <-ctx.Done():
						t.Fatal("missing upstream datagram")
					}
				}
				select {
				case <-peers:
					t.Fatal("denied datagram reached upstream")
				default:
				}
				store.ReconcileSandboxes(nil)
				server.ReconcileActiveFlows()
			})
		}
	}
}
