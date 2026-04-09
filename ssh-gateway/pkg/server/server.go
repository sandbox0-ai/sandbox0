package server

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"go.uber.org/zap"
	"golang.org/x/crypto/ssh"
)

type permissionTargetKey struct{}

type ptyRequest struct {
	Term   string
	Cols   uint32
	Rows   uint32
	Width  uint32
	Height uint32
	Modes  string
}

type windowChangeRequest struct {
	Cols   uint32
	Rows   uint32
	Width  uint32
	Height uint32
}

type signalRequest struct {
	Signal string
}

type execRequest struct {
	Command string
}

// Server serves SSH connections and bridges sessions to procd contexts.
type Server struct {
	cfg                     *config.SSHGatewayConfig
	logger                  *zap.Logger
	authorizer              SessionAuthorizer
	dataPlaneAuthGen        *internalauth.Generator
	procdStoragePermissions []string
	hostSigner              ssh.Signer
	httpClient              *http.Client
	sshConfig               *ssh.ServerConfig
}

// NewServer creates a new ssh-gateway server.
func NewServer(cfg *config.SSHGatewayConfig, authorizer SessionAuthorizer, dataPlaneAuthGen *internalauth.Generator, logger *zap.Logger) (*Server, error) {
	if cfg == nil {
		cfg = &config.SSHGatewayConfig{}
	}
	if logger == nil {
		logger = zap.NewNop()
	}
	if cfg.SSHHostKeyPath == "" {
		cfg.SSHHostKeyPath = "/secrets/ssh_host_ed25519_key"
	}
	hostKey, err := os.ReadFile(cfg.SSHHostKeyPath)
	if err != nil {
		return nil, fmt.Errorf("read ssh host key: %w", err)
	}
	hostSigner, err := parseHostSigner(hostKey)
	if err != nil {
		return nil, fmt.Errorf("parse ssh host key: %w", err)
	}

	server := &Server{
		cfg:              cfg,
		logger:           logger,
		authorizer:       authorizer,
		dataPlaneAuthGen: dataPlaneAuthGen,
		hostSigner:       hostSigner,
		httpClient: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
	perms := cfg.ProcdStoragePermissions
	if len(perms) == 0 {
		perms = []string{"sandboxvolume:read", "sandboxvolume:write"}
	}
	server.procdStoragePermissions = perms
	server.sshConfig = &ssh.ServerConfig{
		ServerVersion: "SSH-2.0-sandbox0",
		PublicKeyCallback: func(conn ssh.ConnMetadata, key ssh.PublicKey) (*ssh.Permissions, error) {
			target, err := authorizer.Authenticate(context.Background(), conn.User(), key)
			if err != nil {
				logger.Warn("SSH public key authentication failed",
					zap.String("username", conn.User()),
					zap.String("public_key_fingerprint", ssh.FingerprintSHA256(key)),
					zap.Error(err),
				)
				return nil, err
			}
			return &ssh.Permissions{ExtraData: map[any]any{permissionTargetKey{}: target}}, nil
		},
	}
	server.sshConfig.AddHostKey(hostSigner)
	return server, nil
}

func parseHostSigner(hostKey []byte) (ssh.Signer, error) {
	hostSigner, err := ssh.ParsePrivateKey(hostKey)
	if err == nil {
		return hostSigner, nil
	}

	privateKey, loadErr := internalauth.LoadEd25519PrivateKey(hostKey)
	if loadErr != nil {
		return nil, err
	}

	hostSigner, signerErr := ssh.NewSignerFromKey(privateKey)
	if signerErr != nil {
		return nil, signerErr
	}
	return hostSigner, nil
}

// Serve accepts SSH connections on the supplied listener until the context is canceled.
func (s *Server) Serve(ctx context.Context, listener net.Listener) error {
	if listener == nil {
		return errors.New("listener is required")
	}
	var closeOnce sync.Once
	go func() {
		<-ctx.Done()
		closeOnce.Do(func() {
			_ = listener.Close()
		})
	}()

	for {
		conn, err := listener.Accept()
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return nil
			}
			return fmt.Errorf("accept ssh connection: %w", err)
		}
		go s.handleConn(ctx, conn)
	}
}

// Start listens on the configured port and begins serving SSH connections.
func (s *Server) Start(ctx context.Context) error {
	port := s.cfg.SSHPort
	if port == 0 {
		port = 2222
	}
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return fmt.Errorf("listen on ssh port %d: %w", port, err)
	}
	s.logger.Info("Starting ssh-gateway", zap.Int("port", port))
	return s.Serve(ctx, listener)
}

func (s *Server) handleConn(ctx context.Context, conn net.Conn) {
	defer conn.Close()
	sshConn, chans, reqs, err := ssh.NewServerConn(conn, s.sshConfig)
	if err != nil {
		level := zap.WarnLevel
		if errors.Is(err, io.EOF) || strings.Contains(strings.ToLower(err.Error()), "connection reset by peer") {
			level = zap.DebugLevel
		}
		if ce := s.logger.Check(level, "SSH handshake failed"); ce != nil {
			ce.Write(
				zap.String("remote_addr", conn.RemoteAddr().String()),
				zap.Error(err),
			)
		}
		return
	}
	defer sshConn.Close()

	go ssh.DiscardRequests(reqs)

	for {
		select {
		case <-ctx.Done():
			return
		case newChannel, ok := <-chans:
			if !ok {
				return
			}
			if newChannel.ChannelType() != "session" {
				_ = newChannel.Reject(ssh.UnknownChannelType, "only session channels are supported")
				continue
			}
			channel, requests, err := newChannel.Accept()
			if err != nil {
				return
			}
			go s.handleSession(ctx, sshConn, channel, requests)
		}
	}
}

func (s *Server) handleSession(ctx context.Context, conn *ssh.ServerConn, channel ssh.Channel, requests <-chan *ssh.Request) {
	target, ok := conn.Permissions.ExtraData[permissionTargetKey{}].(*SessionTarget)
	if !ok || target == nil {
		defer channel.Close()
		_, _ = channel.Stderr().Write([]byte("sandbox target missing\n"))
		return
	}
	newSSHSession(s, ctx, channel, requests, target).run()
}

func normalizeSSHSignal(value string) string {
	trimmed := strings.TrimSpace(value)
	trimmed = strings.TrimPrefix(strings.ToUpper(trimmed), "SIG")
	return trimmed
}
