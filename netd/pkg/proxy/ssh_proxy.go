package proxy

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/subtle"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"

	"golang.org/x/crypto/ssh"
)

var (
	defaultSSHHostSignerOnce sync.Once
	defaultSSHHostSigner     ssh.Signer
	defaultSSHHostSignerErr  error
)

func (s *Server) proxySSHSession(req *adapterRequest) error {
	if s == nil {
		return fmt.Errorf("server is nil")
	}
	if req == nil || req.Conn == nil || req.DestIP == nil || req.DestPort <= 0 {
		return fmt.Errorf("ssh proxy request is incomplete")
	}
	material := req.EgressAuth.ResolvedSSHProxy
	if material == nil {
		return fmt.Errorf("ssh egress auth material is missing")
	}

	prefixBytes, err := readPrefixBytes(req.Prefix)
	if err != nil {
		return fmt.Errorf("read ssh client prefix: %w", err)
	}
	downstream := newPrefixedConn(req.Conn, prefixBytes)

	authorizedKeys, err := parseSSHPublicKeys(material.SandboxPublicKeys)
	if err != nil {
		return err
	}
	upstreamSigner, err := parseSSHPrivateKey(material.PrivateKeyPEM, material.Passphrase)
	if err != nil {
		return fmt.Errorf("parse upstream ssh private key: %w", err)
	}
	hostSigner, err := defaultTransparentSSHHostSigner()
	if err != nil {
		return fmt.Errorf("load ssh proxy host signer: %w", err)
	}

	serverConfig := &ssh.ServerConfig{
		ServerVersion: "SSH-2.0-sandbox0-netd",
		PublicKeyCallback: func(conn ssh.ConnMetadata, key ssh.PublicKey) (*ssh.Permissions, error) {
			for _, authorized := range authorizedKeys {
				if sshPublicKeysEqual(key, authorized) {
					return nil, nil
				}
			}
			return nil, fmt.Errorf("sandbox ssh public key is not authorized")
		},
	}
	serverConfig.AddHostKey(hostSigner)

	downstreamConn, downstreamChannels, downstreamRequests, err := ssh.NewServerConn(downstream, serverConfig)
	if err != nil {
		return fmt.Errorf("handshake downstream ssh: %w", err)
	}
	defer downstreamConn.Close()
	go ssh.DiscardRequests(downstreamRequests)

	upstreamConn, err := s.dialUpstreamSSH(req, material, upstreamSigner)
	if err != nil {
		return err
	}
	defer upstreamConn.Close()

	return proxySSHChannels(downstreamChannels, upstreamConn)
}

func (s *Server) dialUpstreamSSH(req *adapterRequest, material *resolvedSSHProxy, signer ssh.Signer) (*ssh.Client, error) {
	if s == nil || req == nil || req.DestIP == nil || req.DestPort <= 0 {
		return nil, fmt.Errorf("upstream ssh request is incomplete")
	}
	if material == nil || signer == nil {
		return nil, fmt.Errorf("upstream ssh auth material is incomplete")
	}
	username := strings.TrimSpace(material.UpstreamUsername)
	if username == "" {
		return nil, fmt.Errorf("upstream ssh username is required")
	}
	hostForVerify := sshHostForVerify(req)
	hostKeyCallback, err := newKnownHostsCallback(hostForVerify, req.DestPort, material.KnownHosts)
	if err != nil {
		return nil, err
	}

	cfg := &ssh.ClientConfig{
		User:            username,
		Auth:            []ssh.AuthMethod{ssh.PublicKeys(signer)},
		HostKeyCallback: hostKeyCallback,
		ClientVersion:   "SSH-2.0-sandbox0-netd",
	}
	if s.cfg != nil {
		cfg.Timeout = s.cfg.ProxyUpstreamTimeout.Duration
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = defaultTCPClassificationWindow
	}

	addr := net.JoinHostPort(req.DestIP.String(), fmt.Sprintf("%d", req.DestPort))
	client, err := ssh.Dial("tcp", addr, cfg)
	if err != nil {
		return nil, fmt.Errorf("dial upstream ssh: %w", err)
	}
	return client, nil
}

func proxySSHChannels(downstreamChannels <-chan ssh.NewChannel, upstream *ssh.Client) error {
	if upstream == nil {
		return fmt.Errorf("upstream ssh client is nil")
	}
	for newChannel := range downstreamChannels {
		if newChannel.ChannelType() != "session" {
			_ = newChannel.Reject(ssh.Prohibited, "ssh channel type is not allowed")
			continue
		}

		upstreamChannel, upstreamRequests, err := upstream.OpenChannel(newChannel.ChannelType(), newChannel.ExtraData())
		if err != nil {
			_ = newChannel.Reject(ssh.ConnectionFailed, err.Error())
			continue
		}
		downstreamChannel, downstreamRequests, err := newChannel.Accept()
		if err != nil {
			_ = upstreamChannel.Close()
			continue
		}
		go bridgeSSHChannel(downstreamChannel, downstreamRequests, upstreamChannel, upstreamRequests)
	}
	return nil
}

func bridgeSSHChannel(downstream ssh.Channel, downstreamRequests <-chan *ssh.Request, upstream ssh.Channel, upstreamRequests <-chan *ssh.Request) {
	var closeOnce sync.Once
	closeBoth := func() {
		closeOnce.Do(func() {
			_ = downstream.Close()
			_ = upstream.Close()
		})
	}
	var wg sync.WaitGroup
	var upstreamDone sync.WaitGroup
	wg.Add(6)
	upstreamDone.Add(3)
	go func() {
		defer upstreamDone.Done()
		copySSHStream(&wg, upstream, downstream, closeBoth, false)
	}()
	go copySSHStream(&wg, downstream, upstream, closeBoth, false)
	go func() {
		defer upstreamDone.Done()
		copySSHStream(&wg, upstream.Stderr(), downstream.Stderr(), closeBoth, false)
	}()
	go copySSHStream(&wg, downstream.Stderr(), upstream.Stderr(), closeBoth, false)
	go forwardSSHRequests(&wg, downstreamRequests, upstream, closeBoth, true, false)
	go func() {
		defer upstreamDone.Done()
		forwardSSHRequests(&wg, upstreamRequests, downstream, closeBoth, false, false)
	}()
	go func() {
		upstreamDone.Wait()
		_ = downstream.Close()
	}()
	wg.Wait()
	closeBoth()
}

func copySSHStream(wg *sync.WaitGroup, dst io.Writer, src io.Reader, closeBoth func(), closeOnEOF bool) {
	defer wg.Done()
	_, _ = io.Copy(dst, src)
	if closer, ok := dst.(interface{ CloseWrite() error }); ok {
		_ = closer.CloseWrite()
	}
	if closeOnEOF {
		closeBoth()
	}
}

func forwardSSHRequests(wg *sync.WaitGroup, requests <-chan *ssh.Request, dst ssh.Channel, closeBoth func(), downstreamToUpstream bool, closeOnEnd bool) {
	defer wg.Done()
	for req := range requests {
		if downstreamToUpstream && !allowDownstreamSSHRequest(req.Type) {
			if req.WantReply {
				_ = req.Reply(false, nil)
			}
			continue
		}
		ok, err := dst.SendRequest(req.Type, req.WantReply, req.Payload)
		if req.WantReply {
			if err != nil {
				_ = req.Reply(false, nil)
			} else {
				_ = req.Reply(ok, nil)
			}
		}
	}
	if closeOnEnd {
		closeBoth()
	}
}

func allowDownstreamSSHRequest(requestType string) bool {
	switch requestType {
	case "auth-agent-req@openssh.com", "x11-req":
		return false
	default:
		return true
	}
}

func parseSSHPrivateKey(privateKeyPEM, passphrase string) (ssh.Signer, error) {
	privateKeyPEM = strings.TrimSpace(privateKeyPEM)
	if privateKeyPEM == "" {
		return nil, fmt.Errorf("private key pem is required")
	}
	if strings.TrimSpace(passphrase) != "" {
		return ssh.ParsePrivateKeyWithPassphrase([]byte(privateKeyPEM), []byte(passphrase))
	}
	return ssh.ParsePrivateKey([]byte(privateKeyPEM))
}

func parseSSHPublicKeys(values []string) ([]ssh.PublicKey, error) {
	if len(values) == 0 {
		return nil, fmt.Errorf("sandbox ssh public keys are required")
	}
	out := make([]ssh.PublicKey, 0, len(values))
	for _, value := range values {
		key, _, _, _, err := ssh.ParseAuthorizedKey([]byte(strings.TrimSpace(value)))
		if err != nil {
			return nil, fmt.Errorf("parse sandbox ssh public key: %w", err)
		}
		out = append(out, key)
	}
	return out, nil
}

func defaultTransparentSSHHostSigner() (ssh.Signer, error) {
	defaultSSHHostSignerOnce.Do(func() {
		_, privateKey, err := ed25519.GenerateKey(rand.Reader)
		if err != nil {
			defaultSSHHostSignerErr = err
			return
		}
		defaultSSHHostSigner, defaultSSHHostSignerErr = ssh.NewSignerFromKey(privateKey)
	})
	return defaultSSHHostSigner, defaultSSHHostSignerErr
}

func sshHostForVerify(req *adapterRequest) string {
	if req == nil {
		return ""
	}
	if host := strings.TrimSpace(req.Host); host != "" {
		return host
	}
	if req.DestIP != nil {
		return req.DestIP.String()
	}
	return ""
}

func newKnownHostsCallback(host string, port int, knownHosts []string) (ssh.HostKeyCallback, error) {
	if strings.TrimSpace(host) == "" {
		return nil, fmt.Errorf("upstream ssh host is required")
	}
	if len(knownHosts) == 0 {
		return nil, fmt.Errorf("upstream ssh known_hosts entries are required")
	}
	entries, err := parseKnownHostEntries(knownHosts)
	if err != nil {
		return nil, err
	}
	return func(_ string, _ net.Addr, key ssh.PublicKey) error {
		for _, entry := range entries {
			if entry.matches(host, port) && sshPublicKeysEqual(entry.key, key) {
				return nil
			}
		}
		return fmt.Errorf("upstream ssh host key is not trusted for %s", host)
	}, nil
}

func sshPublicKeysEqual(a, b ssh.PublicKey) bool {
	if a == nil || b == nil {
		return false
	}
	return bytes.Equal(a.Marshal(), b.Marshal())
}

type knownHostEntry struct {
	patterns []string
	key      ssh.PublicKey
}

func parseKnownHostEntries(lines []string) ([]knownHostEntry, error) {
	out := make([]knownHostEntry, 0, len(lines))
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 3 {
			return nil, fmt.Errorf("known_hosts entry is invalid")
		}
		key, _, _, _, err := ssh.ParseAuthorizedKey([]byte(strings.Join(fields[1:], " ")))
		if err != nil {
			return nil, fmt.Errorf("parse known_hosts key: %w", err)
		}
		out = append(out, knownHostEntry{
			patterns: strings.Split(fields[0], ","),
			key:      key,
		})
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("known_hosts entries are required")
	}
	return out, nil
}

func (e knownHostEntry) matches(host string, port int) bool {
	candidates := []string{host}
	if port > 0 {
		candidates = append(candidates, net.JoinHostPort(host, fmt.Sprintf("%d", port)))
		candidates = append(candidates, fmt.Sprintf("[%s]:%d", host, port))
	}
	for _, pattern := range e.patterns {
		pattern = strings.TrimSpace(pattern)
		for _, candidate := range candidates {
			if subtle.ConstantTimeCompare([]byte(pattern), []byte(candidate)) == 1 {
				return true
			}
		}
	}
	return false
}

func knownHostLine(host string, port int, key ssh.PublicKey) string {
	var buf bytes.Buffer
	if port > 0 && port != 22 {
		buf.WriteString(fmt.Sprintf("[%s]:%d", host, port))
	} else {
		buf.WriteString(host)
	}
	buf.WriteByte(' ')
	buf.WriteString(strings.TrimSpace(string(ssh.MarshalAuthorizedKey(key))))
	return buf.String()
}
