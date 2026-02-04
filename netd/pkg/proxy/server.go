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
	"time"

	"github.com/sandbox0-ai/infra/infra-operator/api/config"
	"github.com/sandbox0-ai/infra/netd/pkg/conntrack"
	"github.com/sandbox0-ai/infra/netd/pkg/policy"
	"go.uber.org/zap"
)

type Server struct {
	cfg           *config.NetdConfig
	store         *policy.Store
	tracker       *conntrack.Tracker
	logger        *zap.Logger
	httpListener  net.Listener
	httpsListener net.Listener
	udpConn       *net.UDPConn
	reassembler   *quicReassembler
}

func NewServer(cfg *config.NetdConfig, store *policy.Store, tracker *conntrack.Tracker, logger *zap.Logger) (*Server, error) {
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
		logger:        logger,
		httpListener:  httpLn,
		httpsListener: httpsLn,
		udpConn:       udpConn,
		reassembler:   newQuicReassembler(),
	}, nil
}

func (s *Server) Start(ctx context.Context) {
	go s.acceptLoop(ctx, s.httpListener, s.handleHTTPConn)
	go s.acceptLoop(ctx, s.httpsListener, s.handleHTTPSConn)
	go s.handleUDP(ctx)
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
				return
			}
			s.logger.Error("Proxy accept failed", zap.Error(err))
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

	reader := bufio.NewReader(conn)
	req, err := http.ReadRequest(reader)
	if err != nil {
		s.logger.Debug("HTTP parse failed", zap.Error(err))
		return
	}
	host := normalizeHost(req.Host)
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
		s.logger.Debug("HTTP upstream dial failed", zap.Error(err))
		return
	}
	defer upstream.Close()

	if err := req.Write(upstream); err != nil {
		s.logger.Debug("HTTP upstream write failed", zap.Error(err))
		return
	}

	s.pipe(conn, upstream, reader)
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
		s.logger.Debug("TLS parse failed", zap.Error(err))
		return
	}
	host := normalizeHost(clientHello.ServerName)
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

	upstream, err := net.DialTimeout("tcp", net.JoinHostPort(origIP.String(), fmt.Sprintf("%d", origPort)), s.cfg.ProxyUpstreamTimeout.Duration)
	if err != nil {
		s.logger.Debug("TLS upstream dial failed", zap.Error(err))
		return
	}
	defer upstream.Close()

	reader := io.MultiReader(bytes.NewReader(clientHello.Raw), conn)
	s.pipeWithReader(conn, upstream, reader)
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
				return
			}
			if errors.Is(err, net.ErrClosed) {
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
		if s.cfg.FailClosed {
			s.logger.Info("UDP L4 denied: missing original dst",
				zap.String("src_ip", srcIP),
				zap.Int("dst_port", destPort),
			)
			return
		}
	} else if !policy.AllowEgressL4(p, destIP, destPort, "udp") {
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
			s.logger.Info("UDP L7 denied: missing SNI",
				zap.String("src_ip", srcIP),
				zap.String("dst_ip", destIP.String()),
				zap.Int("dst_port", destPort),
			)
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

	if destIP == nil {
		return
	}
	upstreamAddr := &net.UDPAddr{IP: destIP, Port: destPort}
	upstreamConn, err := net.DialUDP("udp", nil, upstreamAddr)
	if err != nil {
		s.logger.Debug("UDP upstream dial failed", zap.Error(err))
		return
	}
	defer upstreamConn.Close()

	_ = upstreamConn.SetDeadline(time.Now().Add(s.cfg.ProxyUpstreamTimeout.Duration))
	if _, err := upstreamConn.Write(payload); err != nil {
		s.logger.Debug("UDP upstream write failed", zap.Error(err))
		return
	}

	replyBuf := make([]byte, 64*1024)
	n, _, err := upstreamConn.ReadFromUDP(replyBuf)
	if err != nil {
		s.logger.Debug("UDP upstream read failed", zap.Error(err))
		return
	}
	_, _ = s.udpConn.WriteToUDP(replyBuf[:n], src)
}

func (s *Server) pipe(client net.Conn, upstream net.Conn, reader *bufio.Reader) {
	s.pipeWithReader(client, upstream, reader)
}

func (s *Server) pipeWithReader(client net.Conn, upstream net.Conn, reader io.Reader) {
	errCh := make(chan error, 2)
	go func() {
		_, err := io.Copy(upstream, reader)
		errCh <- err
	}()
	go func() {
		_, err := io.Copy(client, upstream)
		errCh <- err
	}()
	<-errCh
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
