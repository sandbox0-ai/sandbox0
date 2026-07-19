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
	obsmetrics "github.com/sandbox0-ai/sandbox0/pkg/observability/metrics"
	"github.com/sandbox0-ai/sandbox0/pkg/teamquota"
	"github.com/sandbox0-ai/sandbox0/pkg/teamquota/activeconnections"
	"go.uber.org/zap"
	"golang.org/x/crypto/ssh"
)

const (
	defaultPlatformMaxConcurrentHandshakes            = 128
	defaultPlatformHandshakeTimeout                   = 10 * time.Second
	defaultPlatformMaxConcurrentChannelsPerConnection = 16
	defaultTeamQuotaReleaseTimeout                    = 2 * time.Second
	maxPlatformConcurrentHandshakes                   = 65_536
	maxPlatformConcurrentChannelsPerConnection        = 256
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
	cfg               *config.SSHGatewayConfig
	logger            *zap.Logger
	authorizer        SessionAuthorizer
	dataPlaneAuthGen  *internalauth.Generator
	hostSigner        ssh.Signer
	httpClient        *http.Client
	sshConfig         *ssh.ServerConfig
	metrics           *obsmetrics.SSHGatewayMetrics
	activeConnections activeconnections.Quota
	networkBytes      networkByteQuota
	handshakeSlots    chan struct{}
}

type networkByteQuota interface {
	Reader(
		context.Context,
		string,
		teamquota.Key,
		io.Reader,
	) io.Reader
	Writer(
		context.Context,
		string,
		teamquota.Key,
		io.Writer,
	) io.Writer
	Close() error
}

// Option configures optional ssh-gateway server behavior.
type Option func(*Server)

// WithMetrics enables ssh-gateway service metrics on the server.
func WithMetrics(metrics *obsmetrics.SSHGatewayMetrics) Option {
	return func(server *Server) {
		server.metrics = metrics
	}
}

// WithActiveConnectionQuota installs mandatory region-shared connection
// leases.
func WithActiveConnectionQuota(quota activeconnections.Quota) Option {
	return func(server *Server) {
		server.activeConnections = quota
	}
}

// WithNetworkByteQuota installs mandatory region-shared ingress and egress
// byte-rate enforcement.
func WithNetworkByteQuota(quota networkByteQuota) Option {
	return func(server *Server) {
		server.networkBytes = quota
	}
}

// NewServer creates a new ssh-gateway server.
func NewServer(cfg *config.SSHGatewayConfig, authorizer SessionAuthorizer, dataPlaneAuthGen *internalauth.Generator, logger *zap.Logger, opts ...Option) (*Server, error) {
	if cfg == nil {
		cfg = &config.SSHGatewayConfig{}
	}
	if logger == nil {
		logger = zap.NewNop()
	}
	if cfg.SSHHostKeyPath == "" {
		cfg.SSHHostKeyPath = "/secrets/ssh_host_ed25519_key"
	}
	if cfg.PlatformMaxConcurrentHandshakes < 0 {
		return nil, fmt.Errorf(
			"platform max concurrent SSH handshakes must be non-negative",
		)
	}
	if cfg.PlatformMaxConcurrentHandshakes >
		maxPlatformConcurrentHandshakes {
		return nil, fmt.Errorf(
			"platform max concurrent SSH handshakes must not exceed %d",
			maxPlatformConcurrentHandshakes,
		)
	}
	if cfg.PlatformMaxConcurrentHandshakes == 0 {
		cfg.PlatformMaxConcurrentHandshakes =
			defaultPlatformMaxConcurrentHandshakes
	}
	if cfg.PlatformHandshakeTimeout.Duration < 0 {
		return nil, fmt.Errorf("platform SSH handshake timeout must be non-negative")
	}
	if cfg.PlatformHandshakeTimeout.Duration == 0 {
		cfg.PlatformHandshakeTimeout.Duration =
			defaultPlatformHandshakeTimeout
	}
	if cfg.PlatformMaxConcurrentChannelsPerConnection < 0 {
		return nil, fmt.Errorf(
			"platform max concurrent SSH channels per connection must be non-negative",
		)
	}
	if cfg.PlatformMaxConcurrentChannelsPerConnection >
		maxPlatformConcurrentChannelsPerConnection {
		return nil, fmt.Errorf(
			"platform max concurrent SSH channels per connection must not exceed %d",
			maxPlatformConcurrentChannelsPerConnection,
		)
	}
	if cfg.PlatformMaxConcurrentChannelsPerConnection == 0 {
		cfg.PlatformMaxConcurrentChannelsPerConnection =
			defaultPlatformMaxConcurrentChannelsPerConnection
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
		handshakeSlots: make(
			chan struct{},
			cfg.PlatformMaxConcurrentHandshakes,
		),
	}
	for _, opt := range opts {
		if opt != nil {
			opt(server)
		}
	}
	if server.activeConnections == nil {
		return nil, fmt.Errorf(
			"region-shared active connection Team Quota is required",
		)
	}
	if server.networkBytes == nil {
		return nil, fmt.Errorf(
			"region-shared network byte Team Quota is required",
		)
	}
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

// SetHTTPClient replaces the HTTP client used for procd requests.
func (s *Server) SetHTTPClient(httpClient *http.Client) {
	if s == nil || httpClient == nil {
		return
	}
	s.httpClient = httpClient
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
		select {
		case s.handshakeSlots <- struct{}{}:
		default:
			s.logger.Warn(
				"SSH handshake rejected by platform overload guard",
				zap.String("remote_addr", addrString(conn.RemoteAddr())),
				zap.Int(
					"max_concurrent_handshakes",
					cap(s.handshakeSlots),
				),
			)
			_ = conn.Close()
			continue
		}
		if s.metrics != nil {
			s.metrics.ConnectionsActive.Inc()
		}
		go func() {
			if s.metrics != nil {
				defer s.metrics.ConnectionsActive.Dec()
			}
			s.handleConn(ctx, conn, func() {
				<-s.handshakeSlots
			})
		}()
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

func (s *Server) handleConn(
	ctx context.Context,
	conn net.Conn,
	releaseHandshakeSlot func(),
) {
	defer conn.Close()
	var releaseHandshakeOnce sync.Once
	releaseHandshake := func() {
		releaseHandshakeOnce.Do(func() {
			if releaseHandshakeSlot != nil {
				releaseHandshakeSlot()
			}
		})
	}
	defer releaseHandshake()
	if err := conn.SetDeadline(
		time.Now().Add(s.cfg.PlatformHandshakeTimeout.Duration),
	); err != nil {
		s.logger.Warn(
			"Failed to set SSH handshake deadline",
			zap.String("remote_addr", addrString(conn.RemoteAddr())),
			zap.Error(err),
		)
		return
	}
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
	releaseHandshake()
	if err := conn.SetDeadline(time.Time{}); err != nil {
		s.logger.Warn(
			"Failed to clear SSH handshake deadline",
			zap.String("remote_addr", addrString(conn.RemoteAddr())),
			zap.Error(err),
		)
		_ = sshConn.Close()
		return
	}
	defer sshConn.Close()

	target, ok := sessionTarget(sshConn)
	if !ok {
		s.logger.Error("Authenticated SSH connection is missing sandbox target")
		return
	}
	connectionCtx, cancelConnection := context.WithCancel(ctx)
	defer cancelConnection()
	connectionLease, err := s.activeConnections.Acquire(
		connectionCtx,
		target.TeamID,
	)
	if err != nil {
		s.logActiveConnectionAdmission(
			"SSH connection rejected by Team Quota",
			target,
			err,
		)
		return
	}
	defer s.releaseActiveConnectionLease(
		connectionLease,
		"SSH connection",
		target,
	)
	go s.closeSSHConnectionOnLeaseLoss(
		connectionCtx,
		connectionLease,
		sshConn,
		target,
	)

	go ssh.DiscardRequests(reqs)

	channelSlots := make(
		chan struct{},
		s.cfg.PlatformMaxConcurrentChannelsPerConnection,
	)
	for {
		select {
		case <-connectionCtx.Done():
			return
		case newChannel, ok := <-chans:
			if !ok {
				return
			}
			if newChannel.ChannelType() != "session" {
				_ = newChannel.Reject(ssh.UnknownChannelType, "only session channels are supported")
				continue
			}
			select {
			case channelSlots <- struct{}{}:
			default:
				s.logger.Warn(
					"SSH channel rejected by per-connection platform overload guard",
					zap.String("team_id", target.TeamID),
					zap.String("sandbox_id", target.SandboxID),
					zap.Int(
						"max_concurrent_channels_per_connection",
						cap(channelSlots),
					),
				)
				_ = newChannel.Reject(
					ssh.ResourceShortage,
					"too many concurrent channels in this SSH connection",
				)
				continue
			}
			channelLease, err := s.activeConnections.Acquire(
				connectionCtx,
				target.TeamID,
			)
			if err != nil {
				<-channelSlots
				s.logActiveConnectionAdmission(
					"SSH channel rejected by Team Quota",
					target,
					err,
				)
				_ = newChannel.Reject(
					ssh.ResourceShortage,
					"team active connection quota unavailable",
				)
				continue
			}
			channel, requests, err := newChannel.Accept()
			if err != nil {
				<-channelSlots
				s.releaseActiveConnectionLease(
					channelLease,
					"SSH channel",
					target,
				)
				return
			}
			channelCtx, cancelChannel := context.WithCancel(connectionCtx)
			quotaChannel := newQuotaSSHChannel(
				channelCtx,
				channel,
				target.TeamID,
				s.networkBytes,
			)
			go func() {
				defer func() { <-channelSlots }()
				defer cancelChannel()
				defer s.releaseActiveConnectionLease(
					channelLease,
					"SSH channel",
					target,
				)
				go s.closeSSHChannelOnLeaseLoss(
					channelCtx,
					channelLease,
					quotaChannel,
					target,
				)
				s.handleSession(
					channelCtx,
					quotaChannel,
					requests,
					target,
				)
			}()
		}
	}
}

func (s *Server) handleSession(
	ctx context.Context,
	channel ssh.Channel,
	requests <-chan *ssh.Request,
	target *SessionTarget,
) {
	newSSHSession(s, ctx, channel, requests, target).run()
}

func sessionTarget(conn *ssh.ServerConn) (*SessionTarget, bool) {
	if conn == nil || conn.Permissions == nil {
		return nil, false
	}
	target, ok := conn.Permissions.ExtraData[permissionTargetKey{}].(*SessionTarget)
	return target, ok && target != nil && strings.TrimSpace(target.TeamID) != ""
}

func (s *Server) releaseActiveConnectionLease(
	lease activeconnections.Lease,
	resource string,
	target *SessionTarget,
) {
	if lease == nil {
		return
	}
	ctx, cancel := context.WithTimeout(
		context.Background(),
		defaultTeamQuotaReleaseTimeout,
	)
	defer cancel()
	if err := lease.Release(ctx); err != nil {
		fields := []zap.Field{zap.Error(err)}
		if target != nil {
			fields = append(
				fields,
				zap.String("team_id", target.TeamID),
				zap.String("sandbox_id", target.SandboxID),
			)
		}
		s.logger.Error(resource+" Team Quota lease release failed", fields...)
	}
}

func (s *Server) closeSSHConnectionOnLeaseLoss(
	ctx context.Context,
	lease activeconnections.Lease,
	conn *ssh.ServerConn,
	target *SessionTarget,
) {
	select {
	case <-ctx.Done():
		return
	case <-lease.Done():
	}
	if err := lease.Err(); err != nil {
		s.logActiveConnectionAdmission(
			"SSH connection Team Quota lease lost",
			target,
			err,
		)
		_ = conn.Close()
	}
}

func (s *Server) closeSSHChannelOnLeaseLoss(
	ctx context.Context,
	lease activeconnections.Lease,
	channel ssh.Channel,
	target *SessionTarget,
) {
	select {
	case <-ctx.Done():
		return
	case <-lease.Done():
	}
	if err := lease.Err(); err != nil {
		s.logActiveConnectionAdmission(
			"SSH channel Team Quota lease lost",
			target,
			err,
		)
		_ = channel.Close()
	}
}

func (s *Server) logActiveConnectionAdmission(
	message string,
	target *SessionTarget,
	err error,
) {
	fields := []zap.Field{zap.Error(err)}
	if target != nil {
		fields = append(
			fields,
			zap.String("team_id", target.TeamID),
			zap.String("sandbox_id", target.SandboxID),
		)
	}
	if teamquota.IsConcurrencyExceeded(err) {
		s.logger.Info(message, fields...)
		return
	}
	s.logger.Error(message, fields...)
}

func addrString(addr net.Addr) string {
	if addr == nil {
		return ""
	}
	return addr.String()
}

func normalizeSSHSignal(value string) string {
	trimmed := strings.TrimSpace(value)
	trimmed = strings.TrimPrefix(strings.ToUpper(trimmed), "SIG")
	return trimmed
}
