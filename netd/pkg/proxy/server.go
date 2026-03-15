package proxy

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/netd/pkg/conntrack"
	"github.com/sandbox0-ai/sandbox0/netd/pkg/policy"
	"go.uber.org/zap"
)

type Server struct {
	cfg           *config.NetdConfig
	store         *policy.Store
	tracker       *conntrack.Tracker
	usageRecorder UsageRecorder
	logger        *zap.Logger
	httpListener  net.Listener
	httpsListener net.Listener
	udpConn       *net.UDPConn
	reassembler   *quicReassembler
	exitCh        chan error
	exitOnce      sync.Once
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
	httpLn, err := listenTCPTransparent(net.JoinHostPort(cfg.ProxyListenAddr, fmt.Sprintf("%d", cfg.ProxyHTTPPort)))
	if err != nil {
		return nil, err
	}
	httpsLn, err := listenTCPTransparent(net.JoinHostPort(cfg.ProxyListenAddr, fmt.Sprintf("%d", cfg.ProxyHTTPSPort)))
	if err != nil {
		_ = httpLn.Close()
		return nil, err
	}
	udpConn, err := listenUDPTransparent(net.JoinHostPort(cfg.ProxyListenAddr, fmt.Sprintf("%d", cfg.ProxyHTTPSPort)))
	if err != nil {
		_ = httpLn.Close()
		_ = httpsLn.Close()
		return nil, err
	}
	return &Server{
		cfg:           cfg,
		store:         store,
		tracker:       tracker,
		usageRecorder: usageRecorder,
		logger:        logger,
		httpListener:  httpLn,
		httpsListener: httpsLn,
		udpConn:       udpConn,
		reassembler:   newQuicReassembler(),
		exitCh:        make(chan error, 1),
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
	if !policy.AllowEgressL4(p, origIP, origPort, "tcp") {
		s.logger.Info("HTTP L4 denied",
			zap.String("src_ip", srcIP),
			zap.String("dst_ip", origIP.String()),
			zap.Int("dst_port", origPort),
			zap.Bool("policy_exists", p != nil),
		)
		return
	}

	observedConn := &recordingConn{Conn: conn}
	reader := bufio.NewReader(observedConn)
	req, err := http.ReadRequest(reader)
	if err != nil {
		s.handleUnknownTCP(conn, observedConn.RecordedReader(), p, srcIP, origIP, origPort, "http", "parse_failed", zap.Error(err))
		return
	}
	host := normalizeHost(req.Host)
	if host == "" {
		s.handleUnknownTCP(conn, observedConn.RecordedReader(), p, srcIP, origIP, origPort, "http", "missing_host")
		return
	}
	if !policy.AllowEgressDomain(p, host) {
		s.logger.Info("HTTP L7 denied",
			zap.String("src_ip", srcIP),
			zap.String("host", host),
			zap.String("dst_ip", origIP.String()),
			zap.Int("dst_port", origPort),
		)
		return
	}
	s.recordFlow(srcIP, origIP, origPort, "tcp", remotePort(conn.RemoteAddr()))

	upstream, err := net.DialTimeout("tcp", net.JoinHostPort(origIP.String(), fmt.Sprintf("%d", origPort)), s.cfg.ProxyUpstreamTimeout.Duration)
	if err != nil {
		s.logger.Warn("HTTP upstream dial failed", zap.Error(err))
		return
	}
	upstream = &countingConn{Conn: upstream}
	defer upstream.Close()

	if err := req.Write(upstream); err != nil {
		s.logger.Warn("HTTP upstream write failed", zap.Error(err))
		return
	}
	if counter, ok := upstream.(*countingConn); ok {
		s.recordEgressBytes(p, counter.written)
		counter.written = 0
	}

	s.pipe(conn, upstream, reader, p)
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
	if !policy.AllowEgressL4(p, origIP, origPort, "tcp") {
		s.logger.Info("TLS L4 denied",
			zap.String("src_ip", srcIP),
			zap.String("dst_ip", origIP.String()),
			zap.Int("dst_port", origPort),
			zap.Bool("policy_exists", p != nil),
		)
		return
	}

	clientHello, err := readClientHello(conn, int(s.cfg.ProxyHeaderLimit))
	if err != nil {
		s.logger.Warn("TLS parse failed", zap.Error(err))
		return
	}
	host := normalizeHost(clientHello.ServerName)
	if host == "" {
		s.handleUnknownTCP(conn, bytes.NewReader(clientHello.Raw), p, srcIP, origIP, origPort, "tls", "missing_sni")
		return
	}
	if !policy.AllowEgressDomain(p, host) {
		s.logger.Info("TLS L7 denied",
			zap.String("src_ip", srcIP),
			zap.String("host", host),
			zap.String("dst_ip", origIP.String()),
			zap.Int("dst_port", origPort),
		)
		return
	}
	s.recordFlow(srcIP, origIP, origPort, "tcp", remotePort(conn.RemoteAddr()))

	if err := s.relayTCPConn(conn, bytes.NewReader(clientHello.Raw), origIP, origPort, p); err != nil {
		s.logger.Warn("TLS upstream relay failed", zap.Error(err))
	}
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
	if destIP == nil {
		s.handleUnknownUDP(src, payload, p, destIP, destPort, "missing_original_dst")
		return
	}
	if !policy.AllowEgressL4(p, destIP, destPort, "udp") {
		s.logger.Info("UDP L4 denied",
			zap.String("src_ip", srcIP),
			zap.String("dst_ip", destIP.String()),
			zap.Int("dst_port", destPort),
			zap.Bool("policy_exists", p != nil),
		)
		return
	}
	if policy.HasDomainRules(p) {
		sni := s.reassembler.ParseSNI(payload, srcIP, destIP.String())
		if sni == "" {
			s.handleUnknownUDP(src, payload, p, destIP, destPort, "missing_sni")
			return
		}
		if !policy.AllowEgressDomain(p, sni) {
			s.logger.Info("UDP L7 denied",
				zap.String("src_ip", srcIP),
				zap.String("host", sni),
				zap.String("dst_ip", destIP.String()),
				zap.Int("dst_port", destPort),
			)
			return
		}
	}
	s.recordFlow(srcIP, destIP, destPort, "udp", src.Port)
	if err := s.forwardUDPDatagram(src, payload, destIP, destPort, p); err != nil {
		s.logger.Warn("UDP upstream relay failed", zap.Error(err))
	}
}

func (s *Server) pipe(client net.Conn, upstream net.Conn, reader *bufio.Reader, compiled *policy.CompiledPolicy) {
	s.pipeWithReader(client, upstream, reader, compiled)
}

func (s *Server) pipeWithReader(client net.Conn, upstream net.Conn, reader io.Reader, compiled *policy.CompiledPolicy) {
	upstreamCounter := &countingWriter{writer: upstream}
	clientCounter := &countingWriter{writer: client}
	errCh := make(chan error, 2)
	go func() {
		n, err := io.Copy(upstreamCounter, reader)
		s.recordEgressBytes(compiled, n)
		errCh <- err
	}()
	go func() {
		n, err := io.Copy(clientCounter, upstream)
		s.recordIngressBytes(compiled, n)
		errCh <- err
	}()
	<-errCh
}

func (s *Server) handleUnknownTCP(
	conn net.Conn,
	prefix io.Reader,
	compiled *policy.CompiledPolicy,
	srcIP string,
	destIP net.IP,
	destPort int,
	protocol string,
	reason string,
	fields ...zap.Field,
) {
	action := policy.UnknownFallbackAction(compiled)
	baseFields := []zap.Field{
		zap.String("src_ip", srcIP),
		zap.String("dst_ip", ipString(destIP)),
		zap.Int("dst_port", destPort),
		zap.String("protocol", protocol),
		zap.String("reason", reason),
		zap.String("action", string(action)),
	}
	baseFields = append(baseFields, fields...)
	if action != policy.UnknownTrafficPassThrough {
		s.logger.Info("Unknown TCP fallback denied", baseFields...)
		return
	}

	s.logger.Info("Unknown TCP fallback pass-through", baseFields...)
	s.recordFlow(srcIP, destIP, destPort, "tcp", remotePort(conn.RemoteAddr()))
	if err := s.relayTCPConn(conn, prefix, destIP, destPort, compiled); err != nil {
		s.logger.Warn("Unknown TCP fallback relay failed", append(baseFields, zap.Error(err))...)
	}
}

func (s *Server) relayTCPConn(client net.Conn, prefix io.Reader, destIP net.IP, destPort int, compiled *policy.CompiledPolicy) error {
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
	s.pipeWithReader(client, upstream, reader, compiled)
	return nil
}

func (s *Server) handleUnknownUDP(
	src *net.UDPAddr,
	payload []byte,
	compiled *policy.CompiledPolicy,
	destIP net.IP,
	destPort int,
	reason string,
) {
	if src == nil {
		return
	}
	action := policy.UnknownFallbackAction(compiled)
	fields := []zap.Field{
		zap.String("src_ip", src.IP.String()),
		zap.String("dst_ip", ipString(destIP)),
		zap.Int("dst_port", destPort),
		zap.String("reason", reason),
		zap.String("action", string(action)),
	}
	if action != policy.UnknownTrafficPassThrough {
		s.logger.Info("Unknown UDP fallback denied", fields...)
		return
	}
	if destIP == nil || destPort <= 0 {
		s.logger.Warn("Unknown UDP fallback unavailable", fields...)
		return
	}

	s.logger.Info("Unknown UDP fallback pass-through", fields...)
	s.recordFlow(src.IP.String(), destIP, destPort, "udp", src.Port)
	if err := s.forwardUDPDatagram(src, payload, destIP, destPort, compiled); err != nil {
		s.logger.Warn("Unknown UDP fallback relay failed", append(fields, zap.Error(err))...)
	}
}

func (s *Server) forwardUDPDatagram(
	src *net.UDPAddr,
	payload []byte,
	destIP net.IP,
	destPort int,
	compiled *policy.CompiledPolicy,
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
	s.recordEgressBytes(compiled, int64(n))

	replyBuf := make([]byte, 64*1024)
	n, _, err = upstreamConn.ReadFromUDP(replyBuf)
	if err != nil {
		return err
	}
	s.recordIngressBytes(compiled, int64(n))
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

func (s *Server) recordEgressBytes(compiled *policy.CompiledPolicy, bytes int64) {
	if s.usageRecorder == nil || bytes <= 0 {
		return
	}
	s.usageRecorder.RecordEgress(compiled, bytes)
}

func (s *Server) recordIngressBytes(compiled *policy.CompiledPolicy, bytes int64) {
	if s.usageRecorder == nil || bytes <= 0 {
		return
	}
	s.usageRecorder.RecordIngress(compiled, bytes)
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

type clientHelloInfo struct {
	ServerName string
	Raw        []byte
}

func readClientHello(conn net.Conn, limit int) (*clientHelloInfo, error) {
	if limit <= 0 {
		limit = 64 * 1024
	}
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	defer conn.SetReadDeadline(time.Time{})

	buf := make([]byte, limit)
	n, err := conn.Read(buf)
	if err != nil {
		return nil, err
	}
	buf = buf[:n]

	ch := &clientHelloInfo{Raw: buf}
	serverName := parseSNIFromTLS(buf)
	ch.ServerName = serverName
	return ch, nil
}

func parseSNIFromTLS(data []byte) string {
	if len(data) < 5 {
		return ""
	}
	if data[0] != 0x16 {
		return ""
	}
	recordLen := int(data[3])<<8 | int(data[4])
	if recordLen+5 > len(data) {
		return ""
	}
	offset := 5
	if data[offset] != 0x01 {
		return ""
	}
	offset += 4
	if offset+2 > len(data) {
		return ""
	}
	offset += 2 + 32
	if offset >= len(data) {
		return ""
	}
	sessionLen := int(data[offset])
	offset += 1 + sessionLen
	if offset+2 > len(data) {
		return ""
	}
	cipherLen := int(data[offset])<<8 | int(data[offset+1])
	offset += 2 + cipherLen
	if offset >= len(data) {
		return ""
	}
	compLen := int(data[offset])
	offset += 1 + compLen
	if offset+2 > len(data) {
		return ""
	}
	extLen := int(data[offset])<<8 | int(data[offset+1])
	offset += 2
	end := offset + extLen
	if end > len(data) {
		return ""
	}
	for offset+4 <= end {
		extType := int(data[offset])<<8 | int(data[offset+1])
		extSize := int(data[offset+2])<<8 | int(data[offset+3])
		offset += 4
		if offset+extSize > end {
			return ""
		}
		if extType == 0x00 {
			if offset+2 > end {
				return ""
			}
			listLen := int(data[offset])<<8 | int(data[offset+1])
			offset += 2
			listEnd := offset + listLen
			for offset+3 <= listEnd {
				nameType := data[offset]
				nameLen := int(data[offset+1])<<8 | int(data[offset+2])
				offset += 3
				if nameType == 0 && offset+nameLen <= listEnd {
					return string(data[offset : offset+nameLen])
				}
				offset += nameLen
			}
			return ""
		}
		offset += extSize
	}
	return ""
}
