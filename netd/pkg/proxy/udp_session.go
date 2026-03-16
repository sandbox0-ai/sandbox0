package proxy

import (
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sandbox0-ai/sandbox0/netd/pkg/policy"
	"go.uber.org/zap"
)

type udpSessionKey struct {
	SrcIP    string
	SrcPort  int
	DestIP   string
	DestPort int
}

type udpSession struct {
	server *Server
	key    udpSessionKey

	mu          sync.Mutex
	compiled    *policy.CompiledPolicy
	audit       *flowAudit
	clientConn  *net.UDPConn
	clientAddr  *net.UDPAddr
	destIP      net.IP
	destPort    int
	upstream    *net.UDPConn
	closed      bool
	auditReq    *adapterRequest
	decision    trafficDecision
	adapter     proxyAdapter
	terminalErr error

	lastSeenUnixNano int64
	closeOnce        sync.Once
}

func newUDPSessionKey(req *adapterRequest) (udpSessionKey, error) {
	if req == nil || req.UDPSource == nil || req.DestIP == nil || req.DestPort <= 0 {
		return udpSessionKey{}, fmt.Errorf("udp session requires source and destination")
	}
	return udpSessionKey{
		SrcIP:    req.UDPSource.IP.String(),
		SrcPort:  req.UDPSource.Port,
		DestIP:   req.DestIP.String(),
		DestPort: req.DestPort,
	}, nil
}

func newUDPSession(server *Server, key udpSessionKey, req *adapterRequest) *udpSession {
	audit := req.Audit
	if audit == nil && server != nil {
		audit = server.newFlowAudit("udp")
	}
	session := &udpSession{
		server:     server,
		key:        key,
		compiled:   req.Compiled,
		audit:      audit,
		clientConn: req.UDPConn,
		clientAddr: cloneUDPAddr(req.UDPSource),
		destIP:     cloneIP(req.DestIP),
		destPort:   req.DestPort,
	}
	session.touch()
	return session
}

func (s *Server) ensureUDPSession(req *adapterRequest) (*udpSession, error) {
	if s == nil {
		return nil, fmt.Errorf("server is nil")
	}
	key, err := newUDPSessionKey(req)
	if err != nil {
		return nil, err
	}
	s.udpSessionMu.Lock()
	if s.udpSessions == nil {
		s.udpSessions = make(map[udpSessionKey]*udpSession)
	}
	session := s.udpSessions[key]
	if session == nil || session.isClosed() {
		session = newUDPSession(s, key, req)
		s.udpSessions[key] = session
	}
	s.udpSessionMu.Unlock()
	session.update(req)
	return session, nil
}

func (s *Server) removeUDPSession(session *udpSession) {
	if s == nil || session == nil {
		return
	}
	s.udpSessionMu.Lock()
	defer s.udpSessionMu.Unlock()
	if current := s.udpSessions[session.key]; current == session {
		delete(s.udpSessions, session.key)
	}
}

func (s *Server) udpSessionIdleTimeout() time.Duration {
	if s == nil || s.cfg == nil || s.cfg.ProxyUpstreamTimeout.Duration <= 0 {
		return 30 * time.Second
	}
	return s.cfg.ProxyUpstreamTimeout.Duration
}

func (session *udpSession) update(req *adapterRequest) {
	if session == nil || req == nil {
		return
	}
	session.mu.Lock()
	defer session.mu.Unlock()
	if req.Compiled != nil {
		session.compiled = req.Compiled
	}
	if req.UDPConn != nil {
		session.clientConn = req.UDPConn
	}
	if req.UDPSource != nil {
		session.clientAddr = cloneUDPAddr(req.UDPSource)
	}
	if req.Audit != nil && session.audit == nil {
		session.audit = req.Audit
	}
	if req.DestIP != nil {
		session.destIP = cloneIP(req.DestIP)
	}
	if req.DestPort > 0 {
		session.destPort = req.DestPort
	}
	session.touch()
}

func (session *udpSession) Audit() *flowAudit {
	if session == nil {
		return nil
	}
	session.mu.Lock()
	defer session.mu.Unlock()
	return session.audit
}

func (session *udpSession) Forward(payload []byte) error {
	if session == nil {
		return fmt.Errorf("udp session is nil")
	}
	if len(payload) == 0 {
		return nil
	}
	upstream, compiled, audit, err := session.ensureUpstream()
	if err != nil {
		return err
	}
	session.touch()
	_ = upstream.SetWriteDeadline(time.Now().Add(session.server.udpSessionIdleTimeout()))
	n, err := upstream.Write(payload)
	if n > 0 && session.server != nil {
		session.server.recordEgressBytes(compiled, int64(n), audit)
	}
	if err != nil {
		session.closeWithError(err)
	}
	return err
}

func (session *udpSession) bindAudit(req *adapterRequest, decision trafficDecision, adapter proxyAdapter) {
	if session == nil || req == nil {
		return
	}
	session.mu.Lock()
	defer session.mu.Unlock()
	session.decision = decision
	session.adapter = adapter
	session.auditReq = &adapterRequest{
		Compiled: req.Compiled,
		Audit:    session.audit,
		SrcIP:    req.SrcIP,
		DestIP:   cloneIP(req.DestIP),
		DestPort: req.DestPort,
		Host:     req.Host,
	}
}

func (session *udpSession) ensureUpstream() (*net.UDPConn, *policy.CompiledPolicy, *flowAudit, error) {
	if session == nil || session.server == nil {
		return nil, nil, nil, fmt.Errorf("udp session is not configured")
	}
	session.mu.Lock()
	defer session.mu.Unlock()
	if session.closed {
		return nil, nil, nil, fmt.Errorf("udp session is closed")
	}
	if session.upstream != nil {
		return session.upstream, session.compiled, session.audit, nil
	}
	if session.destIP == nil || session.destPort <= 0 {
		return nil, nil, nil, fmt.Errorf("udp session destination is missing")
	}
	upstream, err := net.DialUDP("udp", nil, &net.UDPAddr{IP: cloneIP(session.destIP), Port: session.destPort})
	if err != nil {
		return nil, nil, nil, err
	}
	session.upstream = upstream
	go session.readLoop(upstream)
	return session.upstream, session.compiled, session.audit, nil
}

func (session *udpSession) readLoop(conn *net.UDPConn) {
	if session == nil || session.server == nil || conn == nil {
		return
	}
	buf := make([]byte, 64*1024)
	idleTimeout := session.server.udpSessionIdleTimeout()
	for {
		_ = conn.SetReadDeadline(time.Now().Add(idleTimeout))
		n, err := conn.Read(buf)
		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				if session.idleFor() >= idleTimeout {
					session.close()
					return
				}
				continue
			}
			if !errors.Is(err, net.ErrClosed) {
				session.server.logger.Warn("UDP upstream session read failed", zap.Error(err))
			}
			session.closeWithError(err)
			return
		}
		payload := append([]byte(nil), buf[:n]...)
		session.touch()
		clientConn, clientAddr, compiled, audit := session.snapshot()
		if clientConn == nil || clientAddr == nil {
			continue
		}
		if session.server != nil {
			session.server.recordIngressBytes(compiled, int64(n), audit)
		}
		if _, writeErr := clientConn.WriteToUDP(payload, clientAddr); writeErr != nil {
			if !errors.Is(writeErr, net.ErrClosed) {
				session.server.logger.Warn("UDP session reply write failed", zap.Error(writeErr))
			}
			session.closeWithError(writeErr)
			return
		}
	}
}

func (session *udpSession) snapshot() (*net.UDPConn, *net.UDPAddr, *policy.CompiledPolicy, *flowAudit) {
	session.mu.Lock()
	defer session.mu.Unlock()
	return session.clientConn, cloneUDPAddr(session.clientAddr), session.compiled, session.audit
}

func (session *udpSession) touch() {
	if session == nil {
		return
	}
	atomic.StoreInt64(&session.lastSeenUnixNano, time.Now().UnixNano())
}

func (session *udpSession) idleFor() time.Duration {
	if session == nil {
		return 0
	}
	lastSeen := atomic.LoadInt64(&session.lastSeenUnixNano)
	if lastSeen == 0 {
		return 0
	}
	return time.Since(time.Unix(0, lastSeen))
}

func (session *udpSession) isClosed() bool {
	if session == nil {
		return true
	}
	session.mu.Lock()
	defer session.mu.Unlock()
	return session.closed
}

func (session *udpSession) close() {
	session.closeWithError(nil)
}

func (session *udpSession) closeWithError(err error) {
	if session == nil {
		return
	}
	session.closeOnce.Do(func() {
		session.mu.Lock()
		session.closed = true
		upstream := session.upstream
		if err != nil && session.terminalErr == nil {
			session.terminalErr = err
		}
		auditReq := session.auditReq
		decision := session.decision
		adapter := session.adapter
		audit := session.audit
		terminalErr := session.terminalErr
		session.upstream = nil
		session.mu.Unlock()
		if upstream != nil {
			_ = upstream.Close()
		}
		if session.server != nil {
			session.server.removeUDPSession(session)
			if auditReq != nil {
				duration := time.Duration(0)
				if audit != nil {
					duration = time.Since(audit.StartedAt)
				}
				session.server.recordAuditEvent(auditReq, decision, adapter, duration, terminalErr)
			}
		}
	})
}

func cloneIP(ip net.IP) net.IP {
	if ip == nil {
		return nil
	}
	return append(net.IP(nil), ip...)
}

func cloneUDPAddr(addr *net.UDPAddr) *net.UDPAddr {
	if addr == nil {
		return nil
	}
	return &net.UDPAddr{
		IP:   cloneIP(addr.IP),
		Port: addr.Port,
		Zone: addr.Zone,
	}
}
