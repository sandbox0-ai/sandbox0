package proxy

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/netd/pkg/conntrack"
	"github.com/sandbox0-ai/sandbox0/netd/pkg/policy"
	"go.uber.org/zap"
)

type Server struct {
	cfg               *config.NetdConfig
	store             *policy.Store
	tracker           *conntrack.Tracker
	usageRecorder     UsageRecorder
	logger            *zap.Logger
	hostVerifier      hostVerifier
	httpListener      net.Listener
	httpsListener     net.Listener
	udpHTTPConn       *net.UDPConn
	udpHTTPSConn      *net.UDPConn
	reassembler       *quicReassembler
	tcpClassifiers    []tcpClassifier
	udpClassifiers    []udpClassifier
	adapters          *adapterRegistry
	authResolver      egressAuthResolver
	authCache         egressAuthCache
	tlsAuthority      tlsInterceptAuthority
	upstreamTLSConfig *tls.Config
	auditor           *auditLogger
	auditSeq          uint64
	udpSessionMu      sync.Mutex
	udpSessions       map[udpSessionKey]*udpSession
	exitCh            chan error
	exitOnce          sync.Once
}

type UsageRecorder interface {
	RecordEgress(compiled *policy.CompiledPolicy, bytes int64)
	RecordIngress(compiled *policy.CompiledPolicy, bytes int64)
}

func NewServer(cfg *config.NetdConfig, store *policy.Store, tracker *conntrack.Tracker, usageRecorder UsageRecorder, logger *zap.Logger, opts ...ServerOption) (*Server, error) {
	if cfg == nil {
		return nil, fmt.Errorf("netd config is nil")
	}
	if store == nil {
		return nil, fmt.Errorf("policy store is nil")
	}
	if tracker == nil {
		return nil, fmt.Errorf("conntrack tracker is nil")
	}
	if logger == nil {
		logger = zap.NewNop()
	}
	auditor, err := newAuditLogger(cfg)
	if err != nil {
		return nil, err
	}
	httpLn, err := listenTCPTransparent(net.JoinHostPort(cfg.ProxyListenAddr, fmt.Sprintf("%d", cfg.ProxyHTTPPort)))
	if err != nil {
		if auditor != nil {
			_ = auditor.Close()
		}
		return nil, err
	}
	httpsLn, err := listenTCPTransparent(net.JoinHostPort(cfg.ProxyListenAddr, fmt.Sprintf("%d", cfg.ProxyHTTPSPort)))
	if err != nil {
		_ = httpLn.Close()
		if auditor != nil {
			_ = auditor.Close()
		}
		return nil, err
	}
	udpConn, err := listenUDPTransparent(net.JoinHostPort(cfg.ProxyListenAddr, fmt.Sprintf("%d", cfg.ProxyHTTPSPort)))
	if err != nil {
		_ = httpLn.Close()
		_ = httpsLn.Close()
		if auditor != nil {
			_ = auditor.Close()
		}
		return nil, err
	}
	udpHTTPConn := udpConn
	udpHTTPSConn := udpConn
	if cfg.ProxyHTTPPort != cfg.ProxyHTTPSPort {
		udpHTTPConn, err = listenUDPTransparent(net.JoinHostPort(cfg.ProxyListenAddr, fmt.Sprintf("%d", cfg.ProxyHTTPPort)))
		if err != nil {
			_ = httpLn.Close()
			_ = httpsLn.Close()
			_ = udpConn.Close()
			if auditor != nil {
				_ = auditor.Close()
			}
			return nil, err
		}
	}
	adapters, err := newAdapterRegistry(
		[]proxyAdapter{
			&httpAdapter{},
			&amqpAdapter{},
			&dnsAdapter{},
			&mqttAdapter{},
			&postgresAdapter{},
			&mongodbAdapter{},
			&redisAdapter{},
			&socks5Adapter{},
			&sshAdapter{},
			&tlsAdapter{},
			&udpAdapter{},
		},
		[]proxyAdapter{
			&tcpPassThroughAdapter{},
			&udpPassThroughAdapter{},
		},
	)
	if err != nil {
		_ = httpLn.Close()
		_ = httpsLn.Close()
		_ = udpConn.Close()
		if udpHTTPConn != nil && udpHTTPConn != udpConn {
			_ = udpHTTPConn.Close()
		}
		if auditor != nil {
			_ = auditor.Close()
		}
		return nil, err
	}
	server := &Server{
		cfg:            cfg,
		store:          store,
		tracker:        tracker,
		usageRecorder:  usageRecorder,
		logger:         logger,
		hostVerifier:   newDNSHostVerifier(),
		httpListener:   httpLn,
		httpsListener:  httpsLn,
		udpHTTPConn:    udpHTTPConn,
		udpHTTPSConn:   udpHTTPSConn,
		reassembler:    newQuicReassembler(),
		tcpClassifiers: defaultTCPClassifiers(),
		udpClassifiers: defaultUDPClassifiers(),
		adapters:       adapters,
		authResolver:   noopEgressAuthResolver{},
		authCache:      newMemoryEgressAuthCache(),
		auditor:        auditor,
		exitCh:         make(chan error, 1),
	}
	if cfg.EgressAuthResolverURL != "" {
		server.authResolver = NewHTTPEgressAuthResolver(cfg.EgressAuthResolverURL, cfg.EgressAuthResolverTimeout.Duration, nil)
	}
	if cfg.MITMCACertPath != "" && cfg.MITMCAKeyPath != "" {
		authority, authorityErr := newCertificateAuthorityFromFiles(cfg.MITMCACertPath, cfg.MITMCAKeyPath, cfg.MITMLeafTTL.Duration)
		if authorityErr != nil {
			_ = httpLn.Close()
			_ = httpsLn.Close()
			_ = udpConn.Close()
			if udpHTTPConn != nil && udpHTTPConn != udpConn {
				_ = udpHTTPConn.Close()
			}
			if auditor != nil {
				_ = auditor.Close()
			}
			return nil, authorityErr
		}
		server.tlsAuthority = authority
	}
	for _, opt := range opts {
		if opt != nil {
			opt(server)
		}
	}
	return server, nil
}

func (s *Server) Start(ctx context.Context) {
	if s.exitCh == nil {
		s.exitCh = make(chan error, 1)
	}
	go s.runLoop(ctx, "http accept loop", func() {
		s.acceptLoop(ctx, s.httpListener, s.handleTCPConn)
	})
	go s.runLoop(ctx, "https accept loop", func() {
		s.acceptLoop(ctx, s.httpsListener, s.handleTCPConn)
	})
	if s.udpHTTPConn != nil {
		go s.runLoop(ctx, "udp http handler", func() {
			s.handleUDP(ctx, s.udpHTTPConn)
		})
	}
	if s.udpHTTPSConn != nil && s.udpHTTPSConn != s.udpHTTPConn {
		go s.runLoop(ctx, "udp https handler", func() {
			s.handleUDP(ctx, s.udpHTTPSConn)
		})
	}
}

func (s *Server) Shutdown(ctx context.Context) error {
	var err error
	if s.httpListener != nil {
		if closeErr := s.httpListener.Close(); closeErr != nil {
			err = closeErr
		}
	}
	if s.httpsListener != nil {
		if closeErr := s.httpsListener.Close(); closeErr != nil && err == nil {
			err = closeErr
		}
	}
	if s.udpHTTPConn != nil {
		if closeErr := s.udpHTTPConn.Close(); closeErr != nil && err == nil {
			err = closeErr
		}
	}
	if s.udpHTTPSConn != nil && s.udpHTTPSConn != s.udpHTTPConn {
		if closeErr := s.udpHTTPSConn.Close(); closeErr != nil && err == nil {
			err = closeErr
		}
	}
	if s.auditor != nil {
		if closeErr := s.auditor.Close(); closeErr != nil && err == nil {
			err = closeErr
		}
	}
	s.closeUDPSessions()
	return err
}

func (s *Server) Done() <-chan error {
	return s.exitCh
}

func (s *Server) runLoop(ctx context.Context, name string, fn func()) {
	defer func() {
		if r := recover(); r != nil {
			s.signalExit(fmt.Errorf("%s panic: %v", name, r))
			return
		}
		if ctx.Err() != nil {
			s.signalExit(ctx.Err())
			return
		}
		s.signalExit(fmt.Errorf("%s exited", name))
	}()
	fn()
}

func (s *Server) signalExit(err error) {
	s.exitOnce.Do(func() {
		if err == nil {
			err = errors.New("proxy server stopped")
		}
		_ = s.Shutdown(context.Background())
		if s.exitCh == nil {
			s.exitCh = make(chan error, 1)
		}
		s.exitCh <- err
		close(s.exitCh)
	})
}

func (s *Server) acceptLoop(ctx context.Context, ln net.Listener, handler func(net.Conn)) {
	for {
		conn, err := ln.Accept()
		if err != nil {
			select {
			case <-ctx.Done():
				return
			default:
			}
			if errors.Is(err, net.ErrClosed) {
				s.logger.Warn("Proxy connection closed", zap.Error(err))
				return
			}
			continue
		}
		go handler(conn)
	}
}

func (s *Server) handleTCPConn(conn net.Conn) {
	defer conn.Close()
	origIP, origPort, err := originalDst(conn)
	if err != nil {
		s.logger.Warn("Failed to get original dst", zap.Error(err))
		return
	}

	srcIP := remoteIP(conn.RemoteAddr())
	p := s.store.GetByIP(srcIP)
	ctx := &tcpClassifyContext{
		Compiled:    p,
		SrcIP:       srcIP,
		OrigIP:      origIP,
		OrigPort:    origPort,
		Conn:        conn,
		HeaderLimit: int(s.cfg.ProxyHeaderLimit),
	}
	result, err := classifyTCP(s.tcpClassifiers, ctx)
	if err != nil {
		s.logger.Warn("Failed to classify TCP traffic", zap.Error(err))
		return
	}
	req := &adapterRequest{
		Server:   s,
		Compiled: p,
		SrcIP:    srcIP,
		DestIP:   origIP,
		DestPort: origPort,
		Conn:     conn,
		Host:     result.Host,
	}
	if result.Apply != nil {
		result.Apply(req)
	}
	result.Classification = verifyClassifiedHost(s.hostVerifier, p, result.Classification)
	decision := decideTraffic(p, result.Classification)
	fields := []zap.Field{}
	if result.Error != nil {
		fields = append(fields, zap.Error(result.Error))
	}
	s.handleTCPDecision(req, decision, result.Host, fields...)
}

func (s *Server) handleUDP(ctx context.Context, conn *net.UDPConn) {
	if conn == nil {
		return
	}
	buffer := make([]byte, 64*1024)
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		n, addr, destIP, destPort, err := readUDPDatagram(conn, buffer)
		if err != nil {
			if ctx.Err() != nil {
				s.logger.Error("UDP read failed", zap.Error(err))
				return
			}
			if errors.Is(err, net.ErrClosed) {
				s.logger.Warn("UDP connection closed", zap.Error(err))
				return
			}
			s.logger.Error("UDP read failed", zap.Error(err))
			continue
		}
		payload := make([]byte, n)
		copy(payload, buffer[:n])
		go s.handleUDPDatagram(conn, addr, payload, destIP, destPort)
	}
}

func (s *Server) handleUDPDatagram(conn *net.UDPConn, src *net.UDPAddr, payload []byte, destIP net.IP, destPort int) {
	if src == nil {
		return
	}
	srcIP := src.IP.String()
	p := s.store.GetByIP(srcIP)
	ctx := &udpClassifyContext{
		Compiled:    p,
		SrcIP:       srcIP,
		SrcAddr:     src,
		DestIP:      destIP,
		DestPort:    destPort,
		Payload:     payload,
		Reassembler: s.reassembler,
	}
	result, err := classifyUDP(s.udpClassifiers, ctx)
	if err != nil {
		s.logger.Warn("Failed to classify UDP traffic", zap.Error(err))
		return
	}
	req := &adapterRequest{
		Server:     s,
		Compiled:   p,
		SrcIP:      srcIP,
		DestIP:     destIP,
		DestPort:   destPort,
		Host:       result.Host,
		UDPConn:    conn,
		UDPSource:  src,
		UDPPayload: payload,
	}
	if result.Apply != nil {
		result.Apply(req)
	}
	result.Classification = verifyClassifiedHost(s.hostVerifier, p, result.Classification)
	decision := decideTraffic(p, result.Classification)
	s.handleUDPDecision(req, decision, result.Host)
}

func (s *Server) pipeWithReader(client net.Conn, upstream net.Conn, reader io.Reader, compiled *policy.CompiledPolicy, audit *flowAudit) error {
	upstreamCounter := &countingWriter{writer: upstream}
	clientCounter := &countingWriter{writer: client}
	errCh := make(chan error, 2)
	go func() {
		n, err := io.Copy(upstreamCounter, reader)
		s.recordEgressBytes(compiled, n, audit)
		closeConnWrite(upstream)
		errCh <- err
	}()
	go func() {
		n, err := io.Copy(clientCounter, upstream)
		s.recordIngressBytes(compiled, n, audit)
		closeConnWrite(client)
		errCh <- err
	}()
	errs := make([]error, 0, 2)
	for i := 0; i < 2; i++ {
		if err := normalizeRelayError(<-errCh); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func (s *Server) handleTCPDecision(req *adapterRequest, decision trafficDecision, host string, fields ...zap.Field) {
	if req == nil {
		return
	}
	baseFields := []zap.Field{
		zap.String("src_ip", req.SrcIP),
		zap.String("dst_ip", ipString(req.DestIP)),
		zap.Int("dst_port", req.DestPort),
		zap.String("protocol", decision.Protocol),
		zap.String("reason", decision.Reason),
		zap.String("action", string(decision.Action)),
	}
	if req.Audit == nil {
		req.Audit = s.newFlowAudit(decision.Transport)
	}
	baseFields = append(baseFields, zap.String("flow_id", req.Audit.ID))
	if host != "" {
		baseFields = append(baseFields, zap.String("host", host))
	}
	if decision.MatchedAuthRule != nil {
		baseFields = append(baseFields,
			zap.String("auth_ref", decision.MatchedAuthRule.AuthRef),
			zap.String("auth_rule", decision.MatchedAuthRule.Name),
		)
	}
	baseFields = append(baseFields, fields...)
	switch decision.Action {
	case decisionActionDeny:
		s.logger.Info("TCP decision denied", baseFields...)
		s.recordAuditEvent(req, decision, nil, 0, nil)
		return
	case decisionActionPassThrough:
		adapter, resolveErr := s.resolveAdapter(decision)
		if resolveErr != nil {
			s.logger.Warn("TCP fallback adapter missing", append(baseFields, zap.Error(resolveErr))...)
			s.recordAuditEvent(req, decision, nil, 0, resolveErr)
			return
		}
		baseFields = append(baseFields, zap.String("adapter", adapter.Name()), zap.String("adapter_capability", string(adapter.Capability())))
		s.logger.Info("TCP decision pass-through", baseFields...)
		start := time.Now()
		err := s.runAdapter(adapter, req)
		s.recordAuditEvent(req, decision, adapter, time.Since(start), err)
		if err != nil {
			s.logger.Warn("TCP fallback adapter failed", append(baseFields, zap.Error(err))...)
		}
		return
	case decisionActionUseAdapter:
		s.attachEgressAuth(req, decision)
		if req.EgressAuth != nil {
			baseFields = append(baseFields,
				zap.Bool("auth_cache_hit", req.EgressAuth.CacheHit),
				zap.Bool("auth_resolved", req.EgressAuth.Resolved != nil),
				zap.String("auth_failure_policy", req.EgressAuth.FailurePolicy),
			)
			if req.EgressAuth.BypassReason != "" {
				baseFields = append(baseFields, zap.String("auth_bypass_reason", req.EgressAuth.BypassReason))
			}
			if req.EgressAuth.ResolveError != nil {
				baseFields = append(baseFields, zap.Error(req.EgressAuth.ResolveError))
			}
		}
		adapter, resolveErr := s.resolveAdapter(decision)
		if resolveErr != nil {
			s.logger.Warn("TCP adapter missing", append(baseFields, zap.Error(resolveErr))...)
			s.recordAuditEvent(req, decision, nil, 0, resolveErr)
			return
		}
		baseFields = append(baseFields, zap.String("adapter", adapter.Name()), zap.String("adapter_capability", string(adapter.Capability())))
		s.logger.Debug("TCP decision use adapter", baseFields...)
		start := time.Now()
		err := s.runAdapter(adapter, req)
		s.recordAuditEvent(req, decision, adapter, time.Since(start), err)
		if err != nil {
			s.logger.Warn("TCP adapter failed", append(baseFields, zap.Error(err))...)
		}
		return
	default:
		s.logger.Warn("TCP decision unknown action", baseFields...)
		s.recordAuditEvent(req, decision, nil, 0, fmt.Errorf("unknown tcp decision action"))
		return
	}
}

func (s *Server) relayTCPConn(client net.Conn, prefix io.Reader, destIP net.IP, destPort int, compiled *policy.CompiledPolicy, audit *flowAudit) error {
	if destIP == nil || destPort <= 0 {
		return fmt.Errorf("missing destination")
	}
	upstream, err := net.DialTimeout("tcp", net.JoinHostPort(destIP.String(), fmt.Sprintf("%d", destPort)), s.cfg.ProxyUpstreamTimeout.Duration)
	if err != nil {
		return err
	}
	upstream = &countingConn{Conn: upstream}
	defer upstream.Close()

	reader := io.Reader(client)
	if prefix != nil {
		reader = io.MultiReader(prefix, client)
	}
	return s.pipeWithReader(client, upstream, reader, compiled, audit)
}

func (s *Server) proxyHTTPRequest(req *adapterRequest) error {
	if s == nil {
		return fmt.Errorf("server is nil")
	}
	if req == nil || req.Conn == nil {
		return fmt.Errorf("http proxy request is nil")
	}
	prefixBytes, err := io.ReadAll(req.Prefix)
	if err != nil {
		return fmt.Errorf("read buffered http request: %w", err)
	}
	headerEnd := findHTTPHeaderTerminator(prefixBytes)
	if headerEnd < 0 {
		return fmt.Errorf("http request headers are incomplete")
	}
	httpReq, err := parseBufferedHTTPRequest(prefixBytes[:headerEnd])
	if err != nil {
		return fmt.Errorf("parse buffered http request: %w", err)
	}
	bodyReader := io.MultiReader(bytes.NewReader(prefixBytes[headerEnd:]), req.Conn)
	switch {
	case httpReq.ContentLength > 0:
		httpReq.Body = io.NopCloser(io.LimitReader(bodyReader, httpReq.ContentLength))
	case httpReq.ContentLength == 0 && len(httpReq.TransferEncoding) == 0:
		httpReq.Body = http.NoBody
	default:
		httpReq.Body = io.NopCloser(bodyReader)
	}
	httpReq.RequestURI = ""
	httpReq.Close = true
	httpReq.Header.Set("Connection", "close")
	if httpReq.Host == "" && req.Host != "" {
		httpReq.Host = req.Host
	}
	if req.EgressAuth != nil && len(req.EgressAuth.ResolvedHeaders) > 0 {
		injectHTTPHeaders(httpReq, req.EgressAuth.ResolvedHeaders)
	}

	upstream, err := net.DialTimeout("tcp", net.JoinHostPort(req.DestIP.String(), fmt.Sprintf("%d", req.DestPort)), s.cfg.ProxyUpstreamTimeout.Duration)
	if err != nil {
		return err
	}
	upstream = &countingConn{Conn: upstream}
	defer upstream.Close()

	if err := httpReq.Write(upstream); err != nil {
		s.recordEgressBytes(req.Compiled, upstream.(*countingConn).WrittenBytes(), req.Audit)
		return fmt.Errorf("write upstream http request: %w", err)
	}
	s.recordEgressBytes(req.Compiled, upstream.(*countingConn).WrittenBytes(), req.Audit)

	clientCounter := &countingWriter{writer: req.Conn}
	n, err := io.Copy(clientCounter, upstream)
	s.recordIngressBytes(req.Compiled, n, req.Audit)
	return normalizeRelayError(err)
}

func (s *Server) handleUDPDecision(req *adapterRequest, decision trafficDecision, host string) {
	if req == nil || req.UDPSource == nil {
		return
	}
	if decision.Action != decisionActionDeny && req.DestIP != nil && req.DestPort > 0 {
		session, err := s.ensureUDPSession(req)
		if err != nil {
			req.Audit = s.newFlowAudit(decision.Transport)
			fields := []zap.Field{
				zap.String("src_ip", req.UDPSource.IP.String()),
				zap.String("dst_ip", ipString(req.DestIP)),
				zap.Int("dst_port", req.DestPort),
				zap.String("protocol", decision.Protocol),
				zap.String("reason", decision.Reason),
				zap.String("action", string(decision.Action)),
			}
			s.logger.Warn("UDP session setup failed", append(fields, zap.Error(err))...)
			s.recordAuditEvent(req, decision, nil, 0, err)
			return
		}
		req.UDPSession = session
		req.Audit = session.Audit()
	}
	fields := []zap.Field{
		zap.String("src_ip", req.UDPSource.IP.String()),
		zap.String("dst_ip", ipString(req.DestIP)),
		zap.Int("dst_port", req.DestPort),
		zap.String("protocol", decision.Protocol),
		zap.String("reason", decision.Reason),
		zap.String("action", string(decision.Action)),
	}
	if req.Audit == nil {
		req.Audit = s.newFlowAudit(decision.Transport)
	}
	fields = append(fields, zap.String("flow_id", req.Audit.ID))
	if host != "" {
		fields = append(fields, zap.String("host", host))
	}
	if decision.MatchedAuthRule != nil {
		fields = append(fields,
			zap.String("auth_ref", decision.MatchedAuthRule.AuthRef),
			zap.String("auth_rule", decision.MatchedAuthRule.Name),
		)
	}
	switch decision.Action {
	case decisionActionDeny:
		s.logger.Info("UDP decision denied", fields...)
		s.recordAuditEvent(req, decision, nil, 0, nil)
		return
	case decisionActionPassThrough:
		adapter, resolveErr := s.resolveAdapter(decision)
		if resolveErr != nil {
			s.logger.Warn("UDP fallback adapter missing", append(fields, zap.Error(resolveErr))...)
			if req.UDPSession != nil {
				req.UDPSession.closeWithError(resolveErr)
			}
			s.recordAuditEvent(req, decision, nil, 0, resolveErr)
			return
		}
		if req.UDPSession != nil {
			req.UDPSession.bindAudit(req, decision, adapter)
		}
		fields = append(fields, zap.String("adapter", adapter.Name()), zap.String("adapter_capability", string(adapter.Capability())))
		if req.DestIP == nil || req.DestPort <= 0 {
			err := fmt.Errorf("destination unavailable for udp pass-through")
			s.logger.Warn("UDP decision pass-through unavailable", append(fields, zap.Error(err))...)
			if req.UDPSession != nil {
				req.UDPSession.closeWithError(err)
				return
			}
			s.recordAuditEvent(req, decision, adapter, 0, err)
			return
		}
		s.logger.Info("UDP decision pass-through", fields...)
		start := time.Now()
		err := s.runAdapter(adapter, req)
		if err != nil {
			s.logger.Warn("UDP fallback adapter failed", append(fields, zap.Error(err))...)
			if req.UDPSession != nil {
				req.UDPSession.closeWithError(err)
				return
			}
			s.recordAuditEvent(req, decision, adapter, time.Since(start), err)
			return
		}
		return
	case decisionActionUseAdapter:
		s.attachEgressAuth(req, decision)
		if req.EgressAuth != nil {
			fields = append(fields,
				zap.Bool("auth_cache_hit", req.EgressAuth.CacheHit),
				zap.Bool("auth_resolved", req.EgressAuth.Resolved != nil),
				zap.String("auth_failure_policy", req.EgressAuth.FailurePolicy),
			)
			if req.EgressAuth.BypassReason != "" {
				fields = append(fields, zap.String("auth_bypass_reason", req.EgressAuth.BypassReason))
			}
			if req.EgressAuth.ResolveError != nil {
				fields = append(fields, zap.Error(req.EgressAuth.ResolveError))
			}
		}
		adapter, resolveErr := s.resolveAdapter(decision)
		if resolveErr != nil {
			s.logger.Warn("UDP adapter missing", append(fields, zap.Error(resolveErr))...)
			if req.UDPSession != nil {
				req.UDPSession.closeWithError(resolveErr)
			}
			s.recordAuditEvent(req, decision, nil, 0, resolveErr)
			return
		}
		if req.UDPSession != nil {
			req.UDPSession.bindAudit(req, decision, adapter)
		}
		fields = append(fields, zap.String("adapter", adapter.Name()), zap.String("adapter_capability", string(adapter.Capability())))
		s.logger.Debug("UDP decision use adapter", fields...)
		start := time.Now()
		err := s.runAdapter(adapter, req)
		if err != nil {
			s.logger.Warn("UDP adapter failed", append(fields, zap.Error(err))...)
			if req.UDPSession != nil {
				req.UDPSession.closeWithError(err)
				return
			}
			s.recordAuditEvent(req, decision, adapter, time.Since(start), err)
			return
		}
		return
	default:
		s.logger.Warn("UDP decision unknown action", fields...)
		s.recordAuditEvent(req, decision, nil, 0, fmt.Errorf("unknown udp decision action"))
		return
	}
}

func (s *Server) recordAuditEvent(req *adapterRequest, decision trafficDecision, adapter proxyAdapter, duration time.Duration, err error) {
	if s == nil || s.auditor == nil {
		return
	}
	if auditErr := s.auditor.Record(req, decision, adapter, duration, err); auditErr != nil {
		s.logger.Warn("Failed to record audit event", zap.Error(auditErr))
	}
}

func (s *Server) resolveAdapter(decision trafficDecision) (proxyAdapter, error) {
	if s == nil {
		return nil, fmt.Errorf("server is nil")
	}
	if s.adapters == nil {
		return nil, fmt.Errorf("adapter registry is not configured")
	}
	return s.adapters.Resolve(decision)
}

func (s *Server) runAdapter(adapter proxyAdapter, req *adapterRequest) error {
	if s == nil {
		return fmt.Errorf("server is nil")
	}
	if adapter == nil {
		return fmt.Errorf("adapter is nil")
	}
	switch adapter.Capability() {
	case adapterCapabilityPassThrough:
		return s.runPassThrough(adapter, req)
	case adapterCapabilityInspect, adapterCapabilityTerminate:
		return adapter.Handle(req)
	default:
		return fmt.Errorf("unsupported adapter capability %q", adapter.Capability())
	}
}

func (s *Server) runPassThrough(adapter proxyAdapter, req *adapterRequest) error {
	if s == nil {
		return fmt.Errorf("server is nil")
	}
	if adapter == nil {
		return fmt.Errorf("adapter is nil")
	}
	if req == nil {
		return fmt.Errorf("adapter request is nil")
	}
	switch adapter.Transport() {
	case "tcp":
		if req.Conn == nil {
			return fmt.Errorf("tcp pass-through requires connection")
		}
		s.recordFlow(req.SrcIP, req.DestIP, req.DestPort, "tcp", remotePort(req.Conn.RemoteAddr()))
		return s.relayTCPConn(req.Conn, req.Prefix, req.DestIP, req.DestPort, req.Compiled, req.Audit)
	case "udp":
		if req.UDPConn == nil || req.UDPSource == nil {
			return fmt.Errorf("udp pass-through requires source datagram")
		}
		s.recordFlow(req.SrcIP, req.DestIP, req.DestPort, "udp", req.UDPSource.Port)
		return s.forwardUDPDatagram(req.UDPConn, req.UDPSource, req.UDPPayload, req.DestIP, req.DestPort, req.Compiled, req.Audit)
	default:
		return fmt.Errorf("unsupported pass-through transport %q", adapter.Transport())
	}
}

func (s *Server) forwardUDPDatagram(
	conn *net.UDPConn,
	src *net.UDPAddr,
	payload []byte,
	destIP net.IP,
	destPort int,
	compiled *policy.CompiledPolicy,
	audit *flowAudit,
) error {
	if conn == nil || src == nil || destIP == nil || destPort <= 0 {
		return fmt.Errorf("missing destination")
	}
	req := &adapterRequest{
		Server:     s,
		Compiled:   compiled,
		Audit:      audit,
		SrcIP:      src.IP.String(),
		DestIP:     destIP,
		DestPort:   destPort,
		UDPConn:    conn,
		UDPSource:  src,
		UDPPayload: payload,
	}
	session, err := s.ensureUDPSession(req)
	if err != nil {
		return err
	}
	return session.Forward(payload)
}

func normalizeHost(host string) string {
	host = strings.ToLower(strings.TrimSpace(host))
	if strings.Contains(host, ":") {
		host, _, _ = net.SplitHostPort(host)
	}
	return host
}

func remoteIP(addr net.Addr) string {
	if addr == nil {
		return ""
	}
	host, _, err := net.SplitHostPort(addr.String())
	if err != nil {
		return addr.String()
	}
	return host
}

func remotePort(addr net.Addr) int {
	if addr == nil {
		return 0
	}
	_, port, err := net.SplitHostPort(addr.String())
	if err != nil {
		return 0
	}
	value, err := strconv.Atoi(port)
	if err != nil {
		return 0
	}
	return value
}

func ipString(ip net.IP) string {
	if ip == nil {
		return ""
	}
	return ip.String()
}

func closeConnWrite(conn net.Conn) {
	if conn == nil {
		return
	}
	type closeWriter interface {
		CloseWrite() error
	}
	if writer, ok := conn.(closeWriter); ok {
		_ = writer.CloseWrite()
	}
}

func normalizeRelayError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) {
		return nil
	}
	if errors.Is(err, syscall.ECONNRESET) || errors.Is(err, syscall.EPIPE) {
		return nil
	}
	return err
}

func (s *Server) recordFlow(srcIP string, dstIP net.IP, dstPort int, proto string, srcPort int) {
	if s.tracker == nil {
		return
	}
	srcAddr, err := netip.ParseAddr(srcIP)
	if err != nil || !srcAddr.IsValid() {
		return
	}
	dstAddr, ok := netip.AddrFromSlice(dstIP)
	if !ok || !dstAddr.IsValid() {
		return
	}
	var protoNum uint8
	switch strings.ToLower(proto) {
	case "udp":
		protoNum = 17
	case "tcp":
		protoNum = 6
	default:
		return
	}
	if srcPort <= 0 || dstPort <= 0 || srcPort > 65535 || dstPort > 65535 {
		return
	}
	s.tracker.Record(conntrack.FlowKey{
		Proto:   protoNum,
		SrcIP:   srcAddr,
		DstIP:   dstAddr,
		SrcPort: uint16(srcPort),
		DstPort: uint16(dstPort),
	})
}

func (s *Server) recordEgressBytes(compiled *policy.CompiledPolicy, bytes int64, audit *flowAudit) {
	if bytes <= 0 {
		return
	}
	if audit != nil {
		audit.RecordEgress(bytes)
	}
	if s.usageRecorder == nil {
		return
	}
	s.usageRecorder.RecordEgress(compiled, bytes)
}

func (s *Server) recordIngressBytes(compiled *policy.CompiledPolicy, bytes int64, audit *flowAudit) {
	if bytes <= 0 {
		return
	}
	if audit != nil {
		audit.RecordIngress(bytes)
	}
	if s.usageRecorder == nil {
		return
	}
	s.usageRecorder.RecordIngress(compiled, bytes)
}

func (s *Server) newFlowAudit(transport string) *flowAudit {
	if s == nil {
		return newFlowAudit("flow-0", time.Now())
	}
	sequence := atomic.AddUint64(&s.auditSeq, 1)
	prefix := transport
	if prefix == "" {
		prefix = "flow"
	}
	return newFlowAudit(fmt.Sprintf("%s-%d", prefix, sequence), time.Now())
}

func (s *Server) closeUDPSessions() {
	if s == nil {
		return
	}
	s.udpSessionMu.Lock()
	sessions := make([]*udpSession, 0, len(s.udpSessions))
	for _, session := range s.udpSessions {
		sessions = append(sessions, session)
	}
	s.udpSessionMu.Unlock()
	for _, session := range sessions {
		session.close()
	}
}

type countingWriter struct {
	writer io.Writer
}

func (c *countingWriter) Write(p []byte) (int, error) {
	return c.writer.Write(p)
}

type countingConn struct {
	net.Conn
	read    int64
	written int64
}

func (c *countingConn) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	atomic.AddInt64(&c.read, int64(n))
	return n, err
}

func (c *countingConn) Write(p []byte) (int, error) {
	n, err := c.Conn.Write(p)
	atomic.AddInt64(&c.written, int64(n))
	return n, err
}

func (c *countingConn) ReadBytes() int64 {
	if c == nil {
		return 0
	}
	return atomic.LoadInt64(&c.read)
}

func (c *countingConn) WrittenBytes() int64 {
	if c == nil {
		return 0
	}
	return atomic.LoadInt64(&c.written)
}
