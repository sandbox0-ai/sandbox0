package proxy

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sandbox0-ai/sandbox0/netd/pkg/policy"
	"github.com/sandbox0-ai/sandbox0/pkg/teamquota/activeconnections"
	"go.uber.org/zap"
)

type udpSessionKey struct {
	TeamID    string
	SandboxID string
	SrcIP     string
	SrcPort   int
	DestIP    string
	DestPort  int
}

type udpSession struct {
	server *Server
	key    udpSessionKey
	ctx    context.Context
	cancel context.CancelFunc

	mu              sync.Mutex
	compiled        *policy.CompiledPolicy
	audit           *flowAudit
	clientConn      *net.UDPConn
	clientAddr      *net.UDPAddr
	destIP          net.IP
	destPort        int
	upstream        *net.UDPConn
	upstreamInit    bool
	upstreamInitErr error
	downstream      udpReplyConn
	closed          bool
	auditReq        *adapterRequest
	decision        trafficDecision
	adapter         proxyAdapter
	terminalErr     error
	connectionLease activeconnections.Lease

	lastSeenUnixNano int64
	closeOnce        sync.Once
}

type udpReplyConn interface {
	Write([]byte) (int, error)
	Close() error
}

type udpReplyDialer func(local *net.UDPAddr, remote *net.UDPAddr) (udpReplyConn, error)

func newUDPSessionKey(req *adapterRequest) (udpSessionKey, error) {
	if req == nil || req.UDPSource == nil || req.DestIP == nil || req.DestPort <= 0 {
		return udpSessionKey{}, fmt.Errorf("udp session requires source and destination")
	}
	return udpSessionKey{
		TeamID:    compiledTeamID(req.Compiled),
		SandboxID: compiledSandboxID(req.Compiled),
		SrcIP:     req.UDPSource.IP.String(),
		SrcPort:   req.UDPSource.Port,
		DestIP:    req.DestIP.String(),
		DestPort:  req.DestPort,
	}, nil
}

func newUDPSession(server *Server, key udpSessionKey, req *adapterRequest) *udpSession {
	audit := req.Audit
	if audit == nil && server != nil {
		audit = server.newFlowAudit("udp")
	}
	parentCtx := req.Context
	if parentCtx == nil {
		parentCtx = context.TODO()
	}
	sessionCtx, cancel := context.WithCancel(parentCtx)
	session := &udpSession{
		server:     server,
		key:        key,
		ctx:        sessionCtx,
		cancel:     cancel,
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

// ForgetSandboxUDPSessions closes every session originating from one sandbox
// IP. Policy removal and replacement call this before the IP can be reused so
// an upstream socket, reply route, or Team Quota lease cannot cross sandbox
// identity boundaries.
func (s *Server) ForgetSandboxUDPSessions(podIP string) {
	if s == nil {
		return
	}
	podIP = net.ParseIP(podIP).String()
	if podIP == "<nil>" {
		return
	}
	s.udpSessionMu.Lock()
	sessions := make([]*udpSession, 0)
	for key, session := range s.udpSessions {
		if key.SrcIP == podIP {
			sessions = append(sessions, session)
		}
	}
	s.udpSessionMu.Unlock()
	for _, session := range sessions {
		session.close()
	}
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
		session.closeWithError(err)
		return err
	}
	session.touch()
	_ = upstream.SetWriteDeadline(time.Now().Add(session.server.udpSessionIdleTimeout()))
	if session.server != nil {
		if err := session.server.waitBandwidth(session.ctx, compiled, bandwidthEgress, len(payload)); err != nil {
			session.closeWithError(err)
			return err
		}
	}
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
	if session.upstreamInit {
		if session.upstreamInitErr != nil {
			return nil, nil, nil, session.upstreamInitErr
		}
		return nil, nil, nil, fmt.Errorf(
			"udp session upstream initialization did not complete",
		)
	}
	session.upstreamInit = true
	if session.destIP == nil || session.destPort <= 0 {
		session.upstreamInitErr = fmt.Errorf(
			"udp session destination is missing",
		)
		return nil, nil, nil, session.upstreamInitErr
	}
	lease, err := session.server.acquireActiveConnectionLease(
		session.ctx,
		session.compiled,
	)
	if err != nil {
		session.server.logActiveConnectionQuota(
			"UDP session rejected by Team Quota",
			"udp",
			session.compiled,
			err,
		)
		session.upstreamInitErr = err
		return nil, nil, nil, session.upstreamInitErr
	}
	session.connectionLease = lease
	go session.closeOnActiveConnectionLeaseLoss(lease)
	if leaseLost(lease) {
		session.upstreamInitErr = lease.Err()
		return nil, nil, nil, session.upstreamInitErr
	}
	upstream, err := net.DialUDP("udp", nil, &net.UDPAddr{IP: cloneIP(session.destIP), Port: session.destPort})
	if err != nil {
		session.upstreamInitErr = err
		return nil, nil, nil, session.upstreamInitErr
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
		if session.destinationPort() == 53 {
			session.server.observeDNSResponse(session.key.SrcIP, payload)
		}
		compiled, audit := session.auditSnapshot()
		if session.server != nil {
			if err := session.server.waitBandwidth(session.ctx, compiled, bandwidthIngress, len(payload)); err != nil {
				session.closeWithError(err)
				return
			}
		}
		written, writeErr := session.replyToClient(payload)
		if writeErr != nil {
			if !errors.Is(writeErr, net.ErrClosed) {
				session.server.logger.Warn("UDP session reply write failed", zap.Error(writeErr))
			}
			session.closeWithError(writeErr)
			return
		}
		if session.server != nil && written > 0 {
			session.server.recordIngressBytes(compiled, int64(written), audit)
		}
	}
}

func (session *udpSession) replyToClient(payload []byte) (int, error) {
	if session == nil {
		return 0, fmt.Errorf("udp session is nil")
	}
	if len(payload) == 0 {
		return 0, nil
	}
	downstream, err := session.ensureDownstream()
	if err != nil {
		return 0, err
	}
	return downstream.Write(payload)
}

func (session *udpSession) ensureDownstream() (udpReplyConn, error) {
	if session == nil || session.server == nil {
		return nil, fmt.Errorf("udp session is not configured")
	}
	session.mu.Lock()
	defer session.mu.Unlock()
	if session.closed {
		return nil, fmt.Errorf("udp session is closed")
	}
	if session.downstream != nil {
		return session.downstream, nil
	}
	if session.destIP == nil || session.destPort <= 0 || session.clientAddr == nil {
		return nil, fmt.Errorf("udp session reply route is missing")
	}
	dialer := session.server.udpReplyDialer
	if dialer == nil {
		dialer = dialUDPTransparent
	}
	downstream, err := dialer(
		&net.UDPAddr{IP: cloneIP(session.destIP), Port: session.destPort},
		cloneUDPAddr(session.clientAddr),
	)
	if err != nil {
		return nil, err
	}
	session.downstream = downstream
	return downstream, nil
}

func (session *udpSession) auditSnapshot() (*policy.CompiledPolicy, *flowAudit) {
	session.mu.Lock()
	defer session.mu.Unlock()
	return session.compiled, session.audit
}

func (session *udpSession) destinationPort() int {
	if session == nil {
		return 0
	}
	session.mu.Lock()
	defer session.mu.Unlock()
	return session.destPort
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
		if session.cancel != nil {
			session.cancel()
		}
		session.mu.Lock()
		session.closed = true
		upstream := session.upstream
		downstream := session.downstream
		if err != nil && session.terminalErr == nil {
			session.terminalErr = err
		}
		auditReq := session.auditReq
		decision := session.decision
		adapter := session.adapter
		audit := session.audit
		terminalErr := session.terminalErr
		connectionLease := session.connectionLease
		compiled := session.compiled
		session.upstream = nil
		session.downstream = nil
		session.connectionLease = nil
		session.mu.Unlock()
		if upstream != nil {
			_ = upstream.Close()
		}
		if downstream != nil {
			_ = downstream.Close()
		}
		if session.server != nil {
			session.server.releaseActiveConnectionLease(
				connectionLease,
				"udp",
				compiled,
			)
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

func (session *udpSession) closeOnActiveConnectionLeaseLoss(
	lease activeconnections.Lease,
) {
	if session == nil || lease == nil {
		return
	}
	select {
	case <-session.ctx.Done():
		return
	case <-lease.Done():
	}
	if err := lease.Err(); err != nil {
		if session.server != nil {
			compiled, _ := session.auditSnapshot()
			session.server.logActiveConnectionQuota(
				"UDP session Team Quota lease lost",
				"udp",
				compiled,
				err,
			)
		}
		session.closeWithError(err)
	}
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
