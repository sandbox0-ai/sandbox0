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
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
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
		done <- server.proxySSHSession(&adapterRequest{
			Conn:     conn,
			DestIP:   net.ParseIP(upstreamHost),
			DestPort: upstreamPort,
			EgressAuth: &egressAuthContext{
				ResolvedSSHProxy: &resolvedSSHProxy{
					SandboxPublicKeys: []string{fakeAuthorizedKey},
					UpstreamUsername:  "git",
					PrivateKeyPEM:     upstreamPrivateKeyPEM,
					KnownHosts: []string{
						knownHostLine(upstreamHost, upstreamPort, upstreamHostSigner.PublicKey()),
					},
				},
				Resolved: egressauth.NewSSHProxyResolveResponse("git-ssh", &egressauth.SSHProxyDirective{}, nil),
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

func TestKnownHostsCallbackRejectsUntrustedKey(t *testing.T) {
	hostSigner, _, _ := mustTestSSHSigner(t)
	otherSigner, _, _ := mustTestSSHSigner(t)
	callback, err := newKnownHostsCallback("127.0.0.1", 2222, []string{
		knownHostLine("127.0.0.1", 2222, hostSigner.PublicKey()),
	})
	if err != nil {
		t.Fatalf("known hosts callback: %v", err)
	}
	if err := callback("", nil, otherSigner.PublicKey()); err == nil {
		t.Fatal("expected untrusted key rejection")
	}
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
