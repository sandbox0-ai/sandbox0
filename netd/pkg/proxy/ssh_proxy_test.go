package proxy

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/netd/pkg/policy"
	"github.com/sandbox0-ai/sandbox0/pkg/egressauth"
	"golang.org/x/crypto/ssh"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestProxySSHSessionReoriginatesWithPlatformCredential(t *testing.T) {
	fakeSigner, fakeAuthorizedKey, _ := mustTestSSHSigner(t)
	upstreamSigner, _, upstreamPrivateKeyPEM := mustTestSSHSigner(t)
	upstreamHostSigner, _, _ := mustTestSSHSigner(t)

	upstreamAddr, closeUpstream := startTestSSHUpstream(t, upstreamHostSigner, upstreamSigner.PublicKey())
	defer closeUpstream()
	upstreamHost, upstreamPort := splitTestHostPort(t, upstreamAddr)

	netdLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen netd: %v", err)
	}
	defer netdLn.Close()

	server := &Server{cfg: &config.NetdConfig{ProxyUpstreamTimeout: metav1.Duration{Duration: 5 * time.Second}}}
	done := make(chan error, 1)
	go func() {
		conn, err := netdLn.Accept()
		if err != nil {
			done <- err
			return
		}
		defer conn.Close()
		done <- server.runAdapter(&sshAdapter{}, &adapterRequest{
			Server:   server,
			Conn:     conn,
			DestIP:   net.ParseIP(upstreamHost),
			DestPort: upstreamPort,
			EgressAuth: &egressAuthContext{
				Rule: &policy.CompiledEgressAuthRule{AuthRef: "git-ssh"},
				Resolved: egressauth.NewSSHProxyResolveResponse("git-ssh", &egressauth.SSHProxyDirective{
					SandboxPublicKeys: []string{fakeAuthorizedKey},
					UpstreamUsername:  "git",
					PrivateKeyPEM:     upstreamPrivateKeyPEM,
					KnownHosts: []string{
						testKnownHostLine(upstreamHost, upstreamPort, upstreamHostSigner.PublicKey()),
					},
				}, nil),
			},
		})
	}()

	client, err := ssh.Dial("tcp", netdLn.Addr().String(), &ssh.ClientConfig{
		User:            "sandbox",
		Auth:            []ssh.AuthMethod{ssh.PublicKeys(fakeSigner)},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		Timeout:         5 * time.Second,
	})
	if err != nil {
		t.Fatalf("dial netd ssh proxy: %v", err)
	}
	defer client.Close()

	session, err := client.NewSession()
	if err != nil {
		t.Fatalf("new session: %v", err)
	}
	output, err := session.CombinedOutput("git-upload-pack 'repo.git'")
	if err != nil {
		t.Fatalf("run upstream command: %v", err)
	}
	if got := string(output); got != "exec: git-upload-pack 'repo.git'\n" {
		t.Fatalf("output = %q", got)
	}

	client.Close()
	select {
	case err := <-done:
		if err != nil && !strings.Contains(err.Error(), "closed") && err != io.EOF {
			t.Fatalf("proxy returned error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("proxy did not exit")
	}
}

func TestBridgeSSHChannelDelaysExitStatusUntilUpstreamStreamsDrain(t *testing.T) {
	downstream := newOrderedFakeSSHChannel()
	upstream := newOrderedFakeSSHChannel()
	downstream.closeInput()
	upstream.closeStderr()

	downstreamRequests := make(chan *ssh.Request)
	close(downstreamRequests)
	upstreamRequests := make(chan *ssh.Request, 2)

	done := make(chan struct{})
	go func() {
		bridgeSSHChannel(nil, downstream, downstreamRequests, upstream, upstreamRequests)
		close(done)
	}()

	upstreamRequests <- &ssh.Request{Type: "keepalive@openssh.com"}
	if !downstream.waitForEvent("request:keepalive@openssh.com", time.Second) {
		t.Fatalf("upstream request forwarding did not start; events=%v", downstream.eventsSnapshot())
	}

	upstreamRequests <- &ssh.Request{
		Type:    "exit-status",
		Payload: ssh.Marshal(struct{ Status uint32 }{Status: 0}),
	}
	if downstream.waitForEvent("request:exit-status", 100*time.Millisecond) {
		t.Fatalf("exit-status forwarded before upstream stdout drained; events=%v", downstream.eventsSnapshot())
	}

	if _, err := upstream.stdoutWriter.Write([]byte("ok")); err != nil {
		t.Fatalf("write upstream stdout: %v", err)
	}
	if err := upstream.stdoutWriter.Close(); err != nil {
		t.Fatalf("close upstream stdout: %v", err)
	}
	close(upstreamRequests)

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatalf("bridge did not exit; downstream events=%v upstream events=%v", downstream.eventsSnapshot(), upstream.eventsSnapshot())
	}

	events := downstream.eventsSnapshot()
	outputIndex := eventIndex(events, "stdout:ok")
	exitStatusIndex := eventIndex(events, "request:exit-status")
	if outputIndex < 0 || exitStatusIndex < 0 {
		t.Fatalf("missing stdout or exit-status event; events=%v", events)
	}
	if exitStatusIndex < outputIndex {
		t.Fatalf("exit-status forwarded before stdout; events=%v", events)
	}
}

func TestKnownHostsCallbackRejectsUntrustedKey(t *testing.T) {
	hostSigner, _, _ := mustTestSSHSigner(t)
	otherSigner, _, _ := mustTestSSHSigner(t)
	callback, err := newKnownHostsCallback("127.0.0.1", 2222, []string{
		testKnownHostLine("127.0.0.1", 2222, hostSigner.PublicKey()),
	})
	if err != nil {
		t.Fatalf("known hosts callback: %v", err)
	}
	if err := callback("", nil, otherSigner.PublicKey()); err == nil {
		t.Fatal("expected untrusted key rejection")
	}
}

func testKnownHostLine(host string, port int, key ssh.PublicKey) string {
	pattern := host
	if port > 0 && port != 22 {
		pattern = fmt.Sprintf("[%s]:%d", host, port)
	}
	return pattern + " " + strings.TrimSpace(string(ssh.MarshalAuthorizedKey(key)))
}

type orderedFakeSSHChannel struct {
	stdoutReader *io.PipeReader
	stdoutWriter *io.PipeWriter
	stderrReader *io.PipeReader
	stderrWriter *io.PipeWriter
	stderr       *orderedFakeSSHStream

	mu        sync.Mutex
	events    []string
	eventCh   chan string
	closeOnce sync.Once
}

func newOrderedFakeSSHChannel() *orderedFakeSSHChannel {
	stdoutReader, stdoutWriter := io.Pipe()
	stderrReader, stderrWriter := io.Pipe()
	ch := &orderedFakeSSHChannel{
		stdoutReader: stdoutReader,
		stdoutWriter: stdoutWriter,
		stderrReader: stderrReader,
		stderrWriter: stderrWriter,
		eventCh:      make(chan string, 32),
	}
	ch.stderr = &orderedFakeSSHStream{channel: ch}
	return ch
}

func (c *orderedFakeSSHChannel) Read(p []byte) (int, error) {
	return c.stdoutReader.Read(p)
}

func (c *orderedFakeSSHChannel) Write(p []byte) (int, error) {
	c.record("stdout:" + string(p))
	return len(p), nil
}

func (c *orderedFakeSSHChannel) Close() error {
	c.closeOnce.Do(func() {
		c.record("close")
		_ = c.stdoutReader.Close()
		_ = c.stdoutWriter.Close()
		_ = c.stderrReader.Close()
		_ = c.stderrWriter.Close()
	})
	return nil
}

func (c *orderedFakeSSHChannel) CloseWrite() error {
	c.record("close-write")
	return nil
}

func (c *orderedFakeSSHChannel) SendRequest(name string, _ bool, _ []byte) (bool, error) {
	c.record("request:" + name)
	return true, nil
}

func (c *orderedFakeSSHChannel) Stderr() io.ReadWriter {
	return c.stderr
}

func (c *orderedFakeSSHChannel) closeInput() {
	_ = c.stdoutWriter.Close()
	c.closeStderr()
}

func (c *orderedFakeSSHChannel) closeStderr() {
	_ = c.stderrWriter.Close()
}

func (c *orderedFakeSSHChannel) record(event string) {
	c.mu.Lock()
	c.events = append(c.events, event)
	c.mu.Unlock()
	select {
	case c.eventCh <- event:
	default:
	}
}

func (c *orderedFakeSSHChannel) waitForEvent(event string, timeout time.Duration) bool {
	deadline := time.After(timeout)
	for {
		if c.hasEvent(event) {
			return true
		}
		select {
		case <-c.eventCh:
		case <-deadline:
			return c.hasEvent(event)
		}
	}
}

func (c *orderedFakeSSHChannel) hasEvent(event string) bool {
	return eventIndex(c.eventsSnapshot(), event) >= 0
}

func (c *orderedFakeSSHChannel) eventsSnapshot() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.events...)
}

type orderedFakeSSHStream struct {
	channel *orderedFakeSSHChannel
}

func (s *orderedFakeSSHStream) Read(p []byte) (int, error) {
	return s.channel.stderrReader.Read(p)
}

func (s *orderedFakeSSHStream) Write(p []byte) (int, error) {
	s.channel.record("stderr:" + string(p))
	return len(p), nil
}

func eventIndex(events []string, event string) int {
	for i, current := range events {
		if current == event {
			return i
		}
	}
	return -1
}

func startTestSSHUpstream(t *testing.T, hostSigner ssh.Signer, authorizedKey ssh.PublicKey) (string, func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen upstream ssh: %v", err)
	}
	cfg := &ssh.ServerConfig{
		PublicKeyCallback: func(_ ssh.ConnMetadata, key ssh.PublicKey) (*ssh.Permissions, error) {
			if sshPublicKeysEqual(key, authorizedKey) {
				return nil, nil
			}
			return nil, fmt.Errorf("unauthorized upstream key")
		},
	}
	cfg.AddHostKey(hostSigner)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go serveTestSSHConn(t, conn, cfg)
		}
	}()
	return ln.Addr().String(), func() {
		_ = ln.Close()
		<-done
	}
}

func serveTestSSHConn(t *testing.T, conn net.Conn, cfg *ssh.ServerConfig) {
	t.Helper()
	sshConn, channels, requests, err := ssh.NewServerConn(conn, cfg)
	if err != nil {
		_ = conn.Close()
		return
	}
	defer sshConn.Close()
	go ssh.DiscardRequests(requests)
	for newChannel := range channels {
		if newChannel.ChannelType() != "session" {
			_ = newChannel.Reject(ssh.UnknownChannelType, "session required")
			continue
		}
		channel, requests, err := newChannel.Accept()
		if err != nil {
			continue
		}
		go func() {
			defer channel.Close()
			for req := range requests {
				switch req.Type {
				case "exec":
					var payload struct{ Command string }
					if err := ssh.Unmarshal(req.Payload, &payload); err != nil {
						_ = req.Reply(false, nil)
						continue
					}
					_ = req.Reply(true, nil)
					_, _ = channel.Write([]byte("exec: " + payload.Command + "\n"))
					_, _ = channel.SendRequest("exit-status", false, ssh.Marshal(struct{ Status uint32 }{Status: 0}))
					return
				default:
					if req.WantReply {
						_ = req.Reply(false, nil)
					}
				}
			}
		}()
	}
}

func mustTestSSHSigner(t *testing.T) (ssh.Signer, string, string) {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate rsa key: %v", err)
	}
	block := &pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(key),
	}
	privateKeyPEM := string(pem.EncodeToMemory(block))
	signer, err := ssh.ParsePrivateKey([]byte(privateKeyPEM))
	if err != nil {
		t.Fatalf("parse test private key: %v", err)
	}
	authorizedKey := strings.TrimSpace(string(ssh.MarshalAuthorizedKey(signer.PublicKey())))
	return signer, authorizedKey, privateKeyPEM
}

func splitTestHostPort(t *testing.T, addr string) (string, int) {
	t.Helper()
	host, portValue, err := net.SplitHostPort(addr)
	if err != nil {
		t.Fatalf("split host port: %v", err)
	}
	var port int
	if _, err := fmt.Sscanf(portValue, "%d", &port); err != nil {
		t.Fatalf("parse port: %v", err)
	}
	return host, port
}
