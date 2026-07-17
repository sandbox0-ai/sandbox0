package execution

import (
	"encoding/hex"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/sandbox0-ai/sandbox0/manager/procd/pkg/session"
	"github.com/sandbox0-ai/sandbox0/pkg/sandboxobservability"
)

func TestResolveSocketAttributesOwnerToTrustedSupervisorRoot(t *testing.T) {
	procRoot := t.TempDir()
	writeProcNet(t, procRoot, "tcp", []procNetEntry{{
		localIP: "127.0.0.1", localPort: 50000,
		remoteIP: "8.8.8.8", remotePort: 443, inode: "4242",
	}})
	writeProcess(t, procRoot, 100, 1, 1000, nil)
	writeProcess(t, procRoot, 200, 100, 2000, map[string]string{"CODEX_THREAD_ID": "thread-correct"})
	writeProcess(t, procRoot, 300, 200, 3000, map[string]string{"CODEX_THREAD_ID": "thread-overridden"})
	linkSocket(t, procRoot, 300, "4242")

	resolver := NewResolver(procRoot, func() []session.ExecutionScopeRoot {
		return []session.ExecutionScopeRoot{{
			PID:                   100,
			ProcessStartTimeTicks: 1000,
			Spec: session.ExecutionScopeSpec{
				Namespace:             "codex",
				Kind:                  "native_session",
				IDEnvironmentVariable: "CODEX_THREAD_ID",
			},
		}}
	})
	scope, ok, err := resolver.ResolveSocket(SocketQuery{
		Transport: "tcp",
		LocalIP:   net.ParseIP("127.0.0.1"), LocalPort: 50000,
		RemoteIP: net.ParseIP("8.8.8.8"), RemotePort: 443,
	})
	if err != nil {
		t.Fatalf("ResolveSocket() error = %v", err)
	}
	if !ok {
		t.Fatal("ResolveSocket() did not attribute socket")
	}
	if scope.Namespace != "codex" || scope.Kind != "native_session" || scope.ID != "thread-correct" {
		t.Fatalf("ResolveSocket() scope = %+v", scope)
	}
	if scope.Attribution != sandboxobservability.ExecutionScopeAttributionProcessEnvironment {
		t.Fatalf("ResolveSocket() attribution = %q", scope.Attribution)
	}
}

func TestResolveSocketLeavesRootTrafficUnattributed(t *testing.T) {
	procRoot := t.TempDir()
	writeProcNet(t, procRoot, "tcp", []procNetEntry{{
		localIP: "127.0.0.1", localPort: 50001,
		remoteIP: "8.8.8.8", remotePort: 443, inode: "5252",
	}})
	writeProcess(t, procRoot, 100, 1, 1000, nil)
	linkSocket(t, procRoot, 100, "5252")

	resolver := NewResolver(procRoot, func() []session.ExecutionScopeRoot {
		return []session.ExecutionScopeRoot{{
			PID:                   100,
			ProcessStartTimeTicks: 1000,
			Spec: session.ExecutionScopeSpec{
				Namespace:             "codex",
				Kind:                  "native_session",
				IDEnvironmentVariable: "CODEX_THREAD_ID",
			},
		}}
	})
	if scope, ok, err := resolver.ResolveSocket(SocketQuery{
		Transport: "tcp",
		LocalIP:   net.ParseIP("127.0.0.1"), LocalPort: 50001,
		RemoteIP: net.ParseIP("8.8.8.8"), RemotePort: 443,
	}); err != nil {
		t.Fatalf("ResolveSocket() error = %v", err)
	} else if ok {
		t.Fatalf("ResolveSocket() attributed shared root traffic: %+v", scope)
	}
}

func TestResolveSocketLeavesSharedRootAndScopedChildUnattributed(t *testing.T) {
	procRoot := t.TempDir()
	writeProcNet(t, procRoot, "tcp", []procNetEntry{{
		localIP: "127.0.0.1", localPort: 50002,
		remoteIP: "8.8.8.8", remotePort: 443, inode: "5353",
	}})
	writeProcess(t, procRoot, 100, 1, 1000, nil)
	writeProcess(t, procRoot, 200, 100, 2000, map[string]string{"CODEX_THREAD_ID": "thread-child"})
	linkSocket(t, procRoot, 100, "5353")
	linkSocket(t, procRoot, 200, "5353")

	resolver := NewResolver(procRoot, func() []session.ExecutionScopeRoot {
		return []session.ExecutionScopeRoot{{
			PID:                   100,
			ProcessStartTimeTicks: 1000,
			Spec: session.ExecutionScopeSpec{
				Namespace:             "codex",
				Kind:                  "native_session",
				IDEnvironmentVariable: "CODEX_THREAD_ID",
			},
		}}
	})
	if scope, ok, err := resolver.ResolveSocket(SocketQuery{
		Transport: "tcp",
		LocalIP:   net.ParseIP("127.0.0.1"), LocalPort: 50002,
		RemoteIP: net.ParseIP("8.8.8.8"), RemotePort: 443,
	}); err != nil {
		t.Fatalf("ResolveSocket() error = %v", err)
	} else if ok {
		t.Fatalf("ResolveSocket() attributed ambiguously shared socket: %+v", scope)
	}
}

func TestResolveSocketUsesCompleteNetworkTuple(t *testing.T) {
	procRoot := t.TempDir()
	writeProcNet(t, procRoot, "tcp", []procNetEntry{
		{
			localIP: "10.0.0.2", localPort: 50000,
			remoteIP: "8.8.8.8", remotePort: 443, inode: "4242",
		},
		{
			localIP: "10.0.0.2", localPort: 50000,
			remoteIP: "1.1.1.1", remotePort: 443, inode: "5252",
		},
	})
	writeProcess(t, procRoot, 100, 1, 1000, nil)
	writeProcess(t, procRoot, 200, 100, 2000, map[string]string{"SCOPE_ID": "wanted"})
	writeProcess(t, procRoot, 101, 1, 1100, nil)
	writeProcess(t, procRoot, 201, 101, 2100, map[string]string{"SCOPE_ID": "other"})
	linkSocket(t, procRoot, 200, "4242")
	linkSocket(t, procRoot, 201, "5252")

	resolver := NewResolver(procRoot, func() []session.ExecutionScopeRoot {
		return []session.ExecutionScopeRoot{
			executionScopeRoot(100, 1000, "SCOPE_ID"),
			executionScopeRoot(101, 1100, "SCOPE_ID"),
		}
	})
	scope, ok, err := resolver.ResolveSocket(SocketQuery{
		Transport: "tcp",
		LocalIP:   net.ParseIP("10.0.0.2"), LocalPort: 50000,
		RemoteIP: net.ParseIP("8.8.8.8"), RemotePort: 443,
	})
	if err != nil {
		t.Fatalf("ResolveSocket() error = %v", err)
	}
	if !ok || scope.ID != "wanted" {
		t.Fatalf("ResolveSocket() scope = %+v, ok = %v", scope, ok)
	}
}

func TestResolveSocketAttributesUnconnectedUDPWildcardPeer(t *testing.T) {
	procRoot := t.TempDir()
	writeProcNet(t, procRoot, "udp", []procNetEntry{{
		localIP: "0.0.0.0", localPort: 53000,
		remoteIP: "0.0.0.0", remotePort: 0, inode: "6262",
	}})
	writeProcess(t, procRoot, 100, 1, 1000, nil)
	writeProcess(t, procRoot, 200, 100, 2000, map[string]string{"SCOPE_ID": "udp-session"})
	linkSocket(t, procRoot, 200, "6262")

	resolver := NewResolver(procRoot, func() []session.ExecutionScopeRoot {
		return []session.ExecutionScopeRoot{executionScopeRoot(100, 1000, "SCOPE_ID")}
	})
	scope, ok, err := resolver.ResolveSocket(SocketQuery{
		Transport: "udp",
		LocalIP:   net.ParseIP("10.0.0.2"), LocalPort: 53000,
		RemoteIP: net.ParseIP("8.8.8.8"), RemotePort: 53,
	})
	if err != nil {
		t.Fatalf("ResolveSocket() error = %v", err)
	}
	if !ok || scope.ID != "udp-session" {
		t.Fatalf("ResolveSocket() scope = %+v, ok = %v", scope, ok)
	}
}

func TestResolveSocketRejectsReusedSupervisorPID(t *testing.T) {
	procRoot := t.TempDir()
	writeProcNet(t, procRoot, "tcp", []procNetEntry{{
		localIP: "10.0.0.2", localPort: 50000,
		remoteIP: "8.8.8.8", remotePort: 443, inode: "7272",
	}})
	writeProcess(t, procRoot, 100, 1, 9999, nil)
	writeProcess(t, procRoot, 200, 100, 2000, map[string]string{"SCOPE_ID": "wrong"})
	linkSocket(t, procRoot, 200, "7272")

	resolver := NewResolver(procRoot, func() []session.ExecutionScopeRoot {
		return []session.ExecutionScopeRoot{executionScopeRoot(100, 1000, "SCOPE_ID")}
	})
	if scope, ok, err := resolver.ResolveSocket(SocketQuery{
		Transport: "tcp",
		LocalIP:   net.ParseIP("10.0.0.2"), LocalPort: 50000,
		RemoteIP: net.ParseIP("8.8.8.8"), RemotePort: 443,
	}); err != nil {
		t.Fatalf("ResolveSocket() error = %v", err)
	} else if ok {
		t.Fatalf("ResolveSocket() attributed reused root PID: %+v", scope)
	}
}

func TestResolveProcessRejectsIdentityChangeDuringRevalidation(t *testing.T) {
	procRoot := t.TempDir()
	writeProcess(t, procRoot, 100, 1, 1000, nil)
	writeProcess(t, procRoot, 200, 100, 2000, map[string]string{"SCOPE_ID": "session"})

	resolver := NewResolver(procRoot, nil)
	ownerStatPath := filepath.Join(procRoot, "200", "stat")
	statReads := 0
	resolver.readFile = func(path string) ([]byte, error) {
		if path == ownerStatPath {
			statReads++
			if statReads >= 3 {
				return processStatData(200, 100, 3000), nil
			}
		}
		return os.ReadFile(path)
	}
	scope, ok := resolver.resolveProcess(
		processIdentity{pid: 200, ppid: 100, startTimeTicks: 2000},
		[]session.ExecutionScopeRoot{executionScopeRoot(100, 1000, "SCOPE_ID")},
	)
	if ok {
		t.Fatalf("resolveProcess() attributed changed process identity: %+v", scope)
	}
}

func TestParseProcIPv6Endpoint(t *testing.T) {
	endpoint, ok := parseProcEndpoint("00000000000000000000000001000000:01BB", true)
	if !ok || !endpoint.IP.Equal(net.ParseIP("::1")) || endpoint.Port != 443 {
		t.Fatalf("parseProcEndpoint() = %+v, %v", endpoint, ok)
	}
}

type procNetEntry struct {
	localIP    string
	localPort  int
	remoteIP   string
	remotePort int
	inode      string
}

func writeProcNet(t *testing.T, root, transport string, entries []procNetEntry) {
	t.Helper()
	if err := os.MkdirAll(filepath.Join(root, "net"), 0755); err != nil {
		t.Fatal(err)
	}
	var content strings.Builder
	content.WriteString("  sl  local_address rem_address   st tx_queue rx_queue tr tm->when retrnsmt   uid  timeout inode\n")
	for index, entry := range entries {
		content.WriteString(fmt.Sprintf(
			"   %d: %s:%04X %s:%04X 01 00000000:00000000 00:00000000 00000000 1000 0 %s\n",
			index,
			procIPHex(t, entry.localIP),
			entry.localPort,
			procIPHex(t, entry.remoteIP),
			entry.remotePort,
			entry.inode,
		))
	}
	if err := os.WriteFile(filepath.Join(root, "net", transport), []byte(content.String()), 0644); err != nil {
		t.Fatal(err)
	}
}

func procIPHex(t *testing.T, value string) string {
	t.Helper()
	ip := net.ParseIP(value).To4()
	if ip == nil {
		t.Fatalf("test helper only supports IPv4, got %q", value)
	}
	raw := append([]byte(nil), ip...)
	reverseBytes(raw)
	return strings.ToUpper(hex.EncodeToString(raw))
}

func writeProcess(t *testing.T, root string, pid, ppid int, startTimeTicks uint64, environ map[string]string) {
	t.Helper()
	dir := filepath.Join(root, fmt.Sprintf("%d", pid))
	if err := os.MkdirAll(filepath.Join(dir, "fd"), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "stat"), processStatData(pid, ppid, startTimeTicks), 0644); err != nil {
		t.Fatal(err)
	}
	var data []byte
	for key, value := range environ {
		data = append(data, []byte(key+"="+value+"\x00")...)
	}
	if err := os.WriteFile(filepath.Join(dir, "environ"), data, 0644); err != nil {
		t.Fatal(err)
	}
}

func processStatData(pid, ppid int, startTimeTicks uint64) []byte {
	fields := make([]string, 20)
	for index := range fields {
		fields[index] = "0"
	}
	fields[0] = "S"
	fields[1] = fmt.Sprintf("%d", ppid)
	fields[19] = fmt.Sprintf("%d", startTimeTicks)
	return []byte(fmt.Sprintf("%d (process %d) %s\n", pid, pid, strings.Join(fields, " ")))
}

func executionScopeRoot(pid int, startTimeTicks uint64, variable string) session.ExecutionScopeRoot {
	return session.ExecutionScopeRoot{
		PID:                   pid,
		ProcessStartTimeTicks: startTimeTicks,
		Spec: session.ExecutionScopeSpec{
			Namespace:             "test",
			Kind:                  "native_session",
			IDEnvironmentVariable: variable,
		},
	}
}

func linkSocket(t *testing.T, root string, pid int, inode string) {
	t.Helper()
	path := filepath.Join(root, fmt.Sprintf("%d", pid), "fd", "3")
	if err := os.Symlink("socket:["+inode+"]", path); err != nil {
		t.Fatal(err)
	}
}
