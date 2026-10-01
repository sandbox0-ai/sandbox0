//go:build linux

package proxy

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/ctld/internal/ctld/networking/conntrack"
	"github.com/sandbox0-ai/sandbox0/ctld/internal/ctld/networking/model"
	"github.com/sandbox0-ai/sandbox0/ctld/internal/ctld/networking/policy"
	"github.com/sandbox0-ai/sandbox0/ctld/internal/ctld/networking/redirect"
	"github.com/sandbox0-ai/sandbox0/pkg/config"
	v1alpha1 "github.com/sandbox0-ai/sandbox0/pkg/sandboxspec"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"
	"golang.org/x/net/http2"
	"golang.org/x/sys/unix"
)

type liveUsageRecorder struct{ bytes atomic.Int64 }

func (r *liveUsageRecorder) RecordEgress(_ *policy.CompiledPolicy, n int64)  { r.bytes.Add(n) }
func (r *liveUsageRecorder) RecordIngress(_ *policy.CompiledPolicy, n int64) { r.bytes.Add(n) }

func TestLiveProxyKernelHandoffKeepsTCPAndHTTP2AndEnforcesRevocation(t *testing.T) {
	if os.Getenv("SANDBOX0_NETWORK_REDIRECT_INTEGRATION") != "1" {
		t.Skip("requires isolated privileged Linux")
	}
	binary, err := os.Executable()
	require.NoError(t, err)
	if os.Getenv("S0_LIVE_PROXY_CHILD") != "1" {
		ctx, cancel := context.WithTimeout(t.Context(), 45*time.Second)
		defer cancel()
		command := exec.CommandContext(ctx, "unshare", "--net", "--mount", "--propagation", "private", "--fork", "sh", "-ec", `mkdir -p /run/netns; mount -t tmpfs -o size=1m tmpfs /run/netns; exec "$1" -test.run='^TestLiveProxyKernelHandoffKeepsTCPAndHTTP2AndEnforcesRevocation$' -test.v`, "live-proxy-test", binary)
		command.Env = append(os.Environ(), "S0_LIVE_PROXY_CHILD=1")
		output, err := command.CombinedOutput()
		require.NoError(t, err, "%s", output)
		t.Logf("%s", output)
		return
	}
	run := func(arguments ...string) {
		t.Helper()
		output, err := exec.Command(arguments[0], arguments[1:]...).CombinedOutput()
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
	echo, err := net.Listen("tcp4", "192.0.2.1:23500")
	require.NoError(t, err)
	defer echo.Close()
	go func() {
		for {
			conn, err := echo.Accept()
			if err != nil {
				return
			}
			go func() { defer conn.Close(); _, _ = io.Copy(conn, conn) }()
		}
	}()
	cert, key, err := newSelfSignedCertificateAuthority("live-proxy", time.Hour)
	require.NoError(t, err)
	authority, err := newCertificateAuthority(cert, key, time.Hour)
	require.NoError(t, err)
	leaf, err := authority.CertificateForHost("api.example.com")
	require.NoError(t, err)
	upstream, err := tls.Listen("tcp4", "192.0.2.1:23443", &tls.Config{Certificates: []tls.Certificate{*leaf}, NextProtos: []string{"h2", "http/1.1"}})
	require.NoError(t, err)
	defer upstream.Close()
	var connections atomic.Int64
	type connectionContextKey struct{}
	keyID := connectionContextKey{}
	httpServer := &http.Server{ConnContext: func(ctx context.Context, _ net.Conn) context.Context {
		return context.WithValue(ctx, keyID, connections.Add(1))
	}, Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = fmt.Fprintf(w, "%v/%s", r.Context().Value(keyID), r.Proto)
	})}
	require.NoError(t, http2.ConfigureServer(httpServer, &http2.Server{}))
	defer httpServer.Close()
	go httpServer.Serve(upstream)
	info := &model.SandboxInfo{Scope: "runtime-slot", Name: "slot", SourceIP: "192.0.2.2", OwnerKind: "runtime-slot", IncarnationID: "same-guest", Revision: "1", NetworkPolicyHash: "1"}
	raw, err := json.Marshal(v1alpha1.NetworkPolicySpec{Mode: v1alpha1.NetworkModeAllowAll, SandboxID: "owned", TeamID: "team"})
	require.NoError(t, err)
	info.NetworkPolicy = string(raw)
	cfg := &config.NetworkRuntimeConfig{ProxyListenAddr: "0.0.0.0", ProxyHTTPPort: 18080, ProxyHTTPSPort: 18443, ProxyHeaderLimit: 65536, ProxyUpstreamTimeout: config.Duration{Duration: 5 * time.Second}}
	sourceStore := policy.NewStore(nil)
	sourceStore.ReconcileSandboxes([]*model.SandboxInfo{info})
	sourceUsage, candidateUsage := &liveUsageRecorder{}, &liveUsageRecorder{}
	source, err := NewServer(cfg, sourceStore, conntrack.NewTracker(), sourceUsage, zaptest.NewLogger(t))
	require.NoError(t, err)
	defer source.Shutdown(t.Context())
	source.Start(t.Context())
	fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM|unix.SOCK_CLOEXEC, 0)
	require.NoError(t, err)
	parent, child := os.NewFile(uintptr(fds[0]), "parent"), os.NewFile(uintptr(fds[1]), "child")
	defer parent.Close()
	client := exec.CommandContext(t.Context(), "ip", "netns", "exec", "guest", binary, "-test.run=^TestLiveProxyClientHelper$")
	client.Env = append(os.Environ(), "S0_LIVE_PROXY_CLIENT=1")
	client.ExtraFiles = []*os.File{child}
	client.Stderr = os.Stderr
	require.NoError(t, client.Start())
	require.NoError(t, child.Close())
	t.Cleanup(func() { _ = client.Process.Kill(); _ = client.Wait() })
	encoder, decoder := json.NewEncoder(parent), json.NewDecoder(parent)
	request := func(command string) string {
		t.Helper()
		require.NoError(t, encoder.Encode(command))
		var result struct {
			Value string
			Error string
		}
		require.NoError(t, decoder.Decode(&result))
		require.Empty(t, result.Error)
		return result.Value
	}
	require.Equal(t, "live echo\n", request("tcp"))
	oldHTTP2 := request("h2-old")
	require.Contains(t, oldHTTP2, "/HTTP/2.0")
	require.Eventually(t, func() bool { return source.activeConnections.Load() == 2 }, time.Second, 10*time.Millisecond)
	files, err := source.ExportListeners()
	require.NoError(t, err)
	defer func() {
		for _, file := range files {
			_ = file.Close()
		}
	}()
	candidateStore := policy.NewStore(nil)
	candidateStore.ReconcileSandboxes([]*model.SandboxInfo{info})
	candidate, err := NewServerWithListeners(cfg, candidateStore, conntrack.NewTracker(), candidateUsage, zaptest.NewLogger(t), files)
	require.NoError(t, err)
	defer candidate.Shutdown(t.Context())
	require.NoError(t, source.Drain())
	candidate.Start(t.Context())
	for range 10 {
		require.Equal(t, "live echo\n", request("tcp"))
		require.Equal(t, oldHTTP2, request("h2-old"), "HTTP/2 must stay on its original TLS and upstream connections")
	}
	newHTTP2 := request("h2-new")
	require.NotEqual(t, oldHTTP2, newHTTP2)
	require.Contains(t, newHTTP2, "/HTTP/2.0")
	require.Eventually(t, func() bool { return candidate.activeConnections.Load() == 1 }, time.Second, 10*time.Millisecond)
	deadline, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, source.WaitDrained(deadline), context.DeadlineExceeded, "drain must not force-close retained connections")
	sourceStore.ReconcileSandboxes(nil)
	candidateStore.ReconcileSandboxes(nil)
	source.ReconcileActiveFlows()
	candidate.ReconcileActiveFlows()
	deadline2, cancel2 := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel2()
	require.NoError(t, source.WaitDrained(deadline2), "policy revocation must still retire draining flows")
	require.NoError(t, candidate.WaitDrained(deadline2))
	require.Eventually(t, func() bool { return sourceUsage.bytes.Load() > 0 && candidateUsage.bytes.Load() > 0 }, time.Second, 10*time.Millisecond)
	t.Log("same TCP and HTTP/2 connections survived listener transfer; new TLS admission and both usage recorders served; revocation retired retained flows")
}

func TestLiveProxyClientHelper(t *testing.T) {
	if os.Getenv("S0_LIVE_PROXY_CLIENT") != "1" {
		t.Skip("subprocess helper")
	}
	control := os.NewFile(3, "control")
	defer control.Close()
	encoder, decoder := json.NewEncoder(control), json.NewDecoder(control)
	var tcp net.Conn
	defer func() {
		if tcp != nil {
			_ = tcp.Close()
		}
	}()
	clients := make(map[string]*http.Client)
	for {
		var command string
		if err := decoder.Decode(&command); err != nil {
			return
		}
		var value string
		var err error
		if command == "tcp" {
			if tcp == nil {
				tcp, err = net.DialTimeout("tcp4", "192.0.2.1:23500", time.Second)
			}
			if err == nil {
				_ = tcp.SetDeadline(time.Now().Add(5 * time.Second))
				_, err = tcp.Write([]byte(strings.Repeat("live echo\n", 32)))
			}
			if err == nil {
				data := make([]byte, 320)
				_, err = io.ReadFull(tcp, data)
				value = string(data[:10])
			}
		} else {
			client := clients[command]
			if client == nil {
				transport := &http.Transport{ForceAttemptHTTP2: true, TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
					return (&net.Dialer{}).DialContext(ctx, "tcp4", "192.0.2.1:23443")
				}}
				defer transport.CloseIdleConnections()
				client = &http.Client{Transport: transport, Timeout: 5 * time.Second}
				clients[command] = client
			}
			var response *http.Response
			response, err = client.Get("https://api.example.com/live")
			if err == nil {
				data, readErr := io.ReadAll(response.Body)
				_ = response.Body.Close()
				value, err = string(data), readErr
			}
		}
		message := struct {
			Value string
			Error string
		}{Value: value}
		if err != nil {
			message.Error = err.Error()
		}
		if err := encoder.Encode(message); err != nil {
			return
		}
	}
}
