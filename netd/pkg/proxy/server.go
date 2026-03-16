package proxy

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/netip"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/netd/pkg/conntrack"
	"github.com/sandbox0-ai/sandbox0/netd/pkg/policy"
	"go.uber.org/zap"
)

type Server struct {
	cfg              *config.NetdConfig
	store            *policy.Store
	tracker          *conntrack.Tracker
	usageRecorder    UsageRecorder
	logger           *zap.Logger
	httpListener     net.Listener
	httpsListener    net.Listener
	udpConn          *net.UDPConn
	reassembler      *quicReassembler
	httpClassifiers  []tcpClassifier
	httpsClassifiers []tcpClassifier
	udpClassifiers   []udpClassifier
	tcpAdapters      map[string]proxyAdapter
	tcpFallback      proxyAdapter
	udpAdapters      map[string]proxyAdapter
	udpFallback      proxyAdapter
	auditor          *auditLogger
	auditSeq         uint64
	exitCh           chan error
	exitOnce         sync.Once
}

type UsageRecorder interface {
	RecordEgress(compiled *policy.CompiledPolicy, bytes int64)
	RecordIngress(compiled *policy.CompiledPolicy, bytes int64)
}

func NewServer(cfg *config.NetdConfig, store *policy.Store, tracker *conntrack.Tracker, usageRecorder UsageRecorder, logger *zap.Logger) (*Server, error) {
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
	return &Server{
		cfg:              cfg,
		store:            store,
		tracker:          tracker,
		usageRecorder:    usageRecorder,
		logger:           logger,
		httpListener:     httpLn,
		httpsListener:    httpsLn,
		udpConn:          udpConn,
		reassembler:      newQuicReassembler(),
		httpClassifiers:  defaultHTTPClassifiers(),
		httpsClassifiers: defaultHTTPSClassifiers(),
		udpClassifiers:   defaultUDPClassifiers(),
		tcpAdapters: map[string]proxyAdapter{
			"http":     &httpAdapter{},
			"postgres": &postgresAdapter{},
			"ssh":      &sshAdapter{},
			"tls":      &tlsAdapter{},
		},
		udpAdapters: map[string]proxyAdapter{
			"udp": &udpAdapter{},
		},
		tcpFallback: &tcpPassThroughAdapter{},
		udpFallback: &udpPassThroughAdapter{},
		auditor:     auditor,
		exitCh:      make(chan error, 1),
	}, nil
}

func (s *Server) Start(ctx context.Context) {
	if s.exitCh == nil {
		s.exitCh = make(chan error, 1)
	}
	go s.runLoop(ctx, "http accept loop", func() {
		s.acceptLoop(ctx, s.httpListener, s.handleHTTPConn)
	})
	go s.runLoop(ctx, "https accept loop", func() {
		s.acceptLoop(ctx, s.httpsListener, s.handleHTTPSConn)
	})
	go s.runLoop(ctx, "udp handler", func() {
		s.handleUDP(ctx)
	})
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
	if s.udpConn != nil {
		if closeErr := s.udpConn.Close(); closeErr != nil && err == nil {
			err = closeErr
		}
	}
	if s.auditor != nil {
		if closeErr := s.auditor.Close(); closeErr != nil && err == nil {
			err = closeErr
		}
	}
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

func (s *Server) handleHTTPConn(conn net.Conn) {
	defer conn.Close()
	origIP, origPort, err := originalDst(conn)
	if err != nil {
		s.logger.Warn("Failed to get original dst", zap.Error(err))
		return
	}

	srcIP := remoteIP(conn.RemoteAddr())
	p := s.store.GetByIP(srcIP)
	ctx := &tcpClassifyContext{
		Compiled:     p,
		SrcIP:        srcIP,
		OrigIP:       origIP,
		OrigPort:     origPort,
		Conn:         conn,
		ObservedConn: &recordingConn{Conn: conn},
	}
	result, err := classifyTCP(s.httpClassifiers, ctx)
	if err != nil {
		s.logger.Warn("Failed to classify HTTP traffic", zap.Error(err))
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
	decision := decideTraffic(p, result.Classification)
	fields := []zap.Field{}
	if result.Error != nil {
		fields = append(fields, zap.Error(result.Error))
	}
	s.handleTCPDecision(req, decision, result.Host, fields...)
}

func (s *Server) handleHTTPSConn(conn net.Conn) {
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
	result, err := classifyTCP(s.httpsClassifiers, ctx)
	if err != nil {
		s.logger.Warn("Failed to classify HTTPS traffic", zap.Error(err))
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
	decision := decideTraffic(p, result.Classification)
	fields := []zap.Field{}
	if result.Error != nil {
		fields = append(fields, zap.Error(result.Error))
	}
	s.handleTCPDecision(req, decision, result.Host, fields...)
}

func (s *Server) handleUDP(ctx context.Context) {
	if s.udpConn == nil {
		return
	}
	buffer := make([]byte, 64*1024)
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		n, addr, destIP, destPort, err := readUDPDatagram(s.udpConn, buffer)
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
		go s.handleUDPDatagram(addr, payload, destIP, destPort)
	}
}

func (s *Server) handleUDPDatagram(src *net.UDPAddr, payload []byte, destIP net.IP, destPort int) {
	if src == nil {
		return
	}
	if destPort == 0 {
		destPort = 443
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
		UDPSource:  src,
		UDPPayload: payload,
	}
	if result.Apply != nil {
		result.Apply(req)
	}
	decision := decideTraffic(p, result.Classification)
	s.handleUDPDecision(req, decision, result.Host)
}

func (s *Server) pipe(client net.Conn, upstream net.Conn, reader *bufio.Reader, compiled *policy.CompiledPolicy, audit *flowAudit) {
	s.pipeWithReader(client, upstream, reader, compiled, audit)
}

func (s *Server) pipeWithReader(client net.Conn, upstream net.Conn, reader io.Reader, compiled *policy.CompiledPolicy, audit *flowAudit) {
	upstreamCounter := &countingWriter{writer: upstream}
	clientCounter := &countingWriter{writer: client}
	errCh := make(chan error, 2)
	go func() {
		n, err := io.Copy(upstreamCounter, reader)
		s.recordEgressBytes(compiled, n, audit)
		errCh <- err
	}()
	go func() {
		n, err := io.Copy(clientCounter, upstream)
		s.recordIngressBytes(compiled, n, audit)
		errCh <- err
	}()
	<-errCh
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
	baseFields = append(baseFields, fields...)
	switch decision.Action {
	case decisionActionDeny:
		s.logger.Info("TCP decision denied", baseFields...)
		s.recordAuditEvent(req, decision, nil, 0, nil)
		return
	case decisionActionPassThrough:
		adapter := s.tcpFallback
		if adapter != nil {
			baseFields = append(baseFields, zap.String("adapter", adapter.Name()), zap.String("adapter_capability", string(adapter.Capability())))
		}
		s.logger.Info("TCP decision pass-through", baseFields...)
		start := time.Now()
		err := s.runTCPFallbackAdapter(req)
		s.recordAuditEvent(req, decision, adapter, time.Since(start), err)
		if err != nil {
			s.logger.Warn("TCP fallback adapter failed", append(baseFields, zap.Error(err))...)
		}
		return
	case decisionActionUseAdapter:
		adapter, ok := s.tcpAdapters[decision.Protocol]
		if ok && adapter != nil {
			baseFields = append(baseFields, zap.String("adapter", adapter.Name()), zap.String("adapter_capability", string(adapter.Capability())))
		}
		s.logger.Debug("TCP decision use adapter", baseFields...)
		start := time.Now()
		err := s.runTCPAdapter(decision.Protocol, req)
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
	s.pipeWithReader(client, upstream, reader, compiled, audit)
	return nil
}

func (s *Server) handleUDPDecision(req *adapterRequest, decision trafficDecision, host string) {
	if req == nil || req.UDPSource == nil {
		return
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
	switch decision.Action {
	case decisionActionDeny:
		s.logger.Info("UDP decision denied", fields...)
		s.recordAuditEvent(req, decision, nil, 0, nil)
		return
	case decisionActionPassThrough:
		adapter := s.udpFallback
		if adapter != nil {
			fields = append(fields, zap.String("adapter", adapter.Name()), zap.String("adapter_capability", string(adapter.Capability())))
		}
		if req.DestIP == nil || req.DestPort <= 0 {
			err := fmt.Errorf("destination unavailable for udp pass-through")
			s.logger.Warn("UDP decision pass-through unavailable", append(fields, zap.Error(err))...)
			s.recordAuditEvent(req, decision, adapter, 0, err)
			return
		}
		s.logger.Info("UDP decision pass-through", fields...)
		start := time.Now()
		err := s.runUDPFallbackAdapter(req)
		s.recordAuditEvent(req, decision, adapter, time.Since(start), err)
		if err != nil {
			s.logger.Warn("UDP fallback adapter failed", append(fields, zap.Error(err))...)
		}
		return
	case decisionActionUseAdapter:
		adapter, ok := s.udpAdapters[decision.Protocol]
		if ok && adapter != nil {
			fields = append(fields, zap.String("adapter", adapter.Name()), zap.String("adapter_capability", string(adapter.Capability())))
		}
		s.logger.Debug("UDP decision use adapter", fields...)
		start := time.Now()
		err := s.runUDPAdapter(decision.Protocol, req)
		s.recordAuditEvent(req, decision, adapter, time.Since(start), err)
		if err != nil {
			s.logger.Warn("UDP adapter failed", append(fields, zap.Error(err))...)
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

func (s *Server) runTCPAdapter(protocol string, req *adapterRequest) error {
	if s == nil {
		return fmt.Errorf("server is nil")
	}
	adapter := s.tcpAdapters[protocol]
	if adapter == nil {
		return fmt.Errorf("tcp adapter not found for protocol %q", protocol)
	}
	return adapter.Handle(req)
}

func (s *Server) runUDPAdapter(protocol string, req *adapterRequest) error {
	if s == nil {
		return fmt.Errorf("server is nil")
	}
	adapter := s.udpAdapters[protocol]
	if adapter == nil {
		return fmt.Errorf("udp adapter not found for protocol %q", protocol)
	}
	return adapter.Handle(req)
}

func (s *Server) runTCPFallbackAdapter(req *adapterRequest) error {
	if s == nil || s.tcpFallback == nil {
		return fmt.Errorf("tcp fallback adapter is not configured")
	}
	return s.tcpFallback.Handle(req)
}

func (s *Server) runUDPFallbackAdapter(req *adapterRequest) error {
	if s == nil || s.udpFallback == nil {
		return fmt.Errorf("udp fallback adapter is not configured")
	}
	return s.udpFallback.Handle(req)
}

func (s *Server) forwardUDPDatagram(
	src *net.UDPAddr,
	payload []byte,
	destIP net.IP,
	destPort int,
	compiled *policy.CompiledPolicy,
	audit *flowAudit,
) error {
	if src == nil || destIP == nil || destPort <= 0 {
		return fmt.Errorf("missing destination")
	}
	upstreamAddr := &net.UDPAddr{IP: destIP, Port: destPort}
	upstreamConn, err := net.DialUDP("udp", nil, upstreamAddr)
	if err != nil {
		return err
	}
	defer upstreamConn.Close()

	_ = upstreamConn.SetDeadline(time.Now().Add(s.cfg.ProxyUpstreamTimeout.Duration))
	n, err := upstreamConn.Write(payload)
	if err != nil {
		return err
	}
	s.recordEgressBytes(compiled, int64(n), audit)

	replyBuf := make([]byte, 64*1024)
	n, _, err = upstreamConn.ReadFromUDP(replyBuf)
	if err != nil {
		return err
	}
	s.recordIngressBytes(compiled, int64(n), audit)
	_, err = s.udpConn.WriteToUDP(replyBuf[:n], src)
	return err
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

type countingWriter struct {
	writer io.Writer
}

func (c *countingWriter) Write(p []byte) (int, error) {
	return c.writer.Write(p)
}

type countingConn struct {
	net.Conn
	written int64
}

func (c *countingConn) Write(p []byte) (int, error) {
	n, err := c.Conn.Write(p)
	c.written += int64(n)
	return n, err
}

type recordingConn struct {
	net.Conn
	recorded bytes.Buffer
}

func (c *recordingConn) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	if n > 0 {
		_, _ = c.recorded.Write(p[:n])
	}
	return n, err
}

func (c *recordingConn) RecordedReader() io.Reader {
	return bytes.NewReader(c.recorded.Bytes())
}

func (c *recordingConn) RecordedBytes() []byte {
	return append([]byte(nil), c.recorded.Bytes()...)
}
