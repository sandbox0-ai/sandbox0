package proxy

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"

	"github.com/sandbox0-ai/sandbox0/ctld/internal/ctld/networking/policy"
)

const maxActiveTCPFlows = 65536

var errFlowRetired = errors.New("network policy incarnation is no longer active")

// tcpFlow owns both sides of one accepted connection. Kernel conntrack deletion
// alone cannot unblock a userspace relay, so policy retirement closes these
// descriptors and cancels pending dials before acknowledging the network revision.
type tcpFlow struct {
	sourceIP string
	compiled *policy.CompiledPolicy
	ctx      context.Context
	cancel   context.CancelFunc
	mu       sync.Mutex
	closed   bool
	conns    []io.Closer
}

func (s *Server) beginTCPFlow(sourceIP string, compiled *policy.CompiledPolicy, conn net.Conn) (*tcpFlow, error) {
	s.flowMu.Lock()
	defer s.flowMu.Unlock()
	if s.closing.Load() || len(s.tcpFlows) >= maxActiveTCPFlows || !s.store.IsCurrentBinding(sourceIP, compiled) {
		return nil, errFlowRetired
	}
	ctx, cancel := context.WithCancel(context.Background())
	flow := &tcpFlow{sourceIP: sourceIP, compiled: compiled, ctx: ctx, cancel: cancel, conns: []io.Closer{conn}}
	if s.tcpFlows == nil {
		s.tcpFlows = make(map[*tcpFlow]struct{})
	}
	s.tcpFlows[flow] = struct{}{}
	return flow, nil
}

func (s *Server) endTCPFlow(flow *tcpFlow) {
	flow.close()
	s.flowMu.Lock()
	delete(s.tcpFlows, flow)
	s.flowMu.Unlock()
}

func (flow *tcpFlow) retain(conn io.Closer) error {
	if flow == nil || conn == nil {
		return nil
	}
	flow.mu.Lock()
	defer flow.mu.Unlock()
	if flow.closed {
		_ = conn.Close()
		return errFlowRetired
	}
	// Each protocol opens at most a probe and an upstream. A bounded list also
	// prevents future adapters from retaining an unbounded number of descriptors.
	if len(flow.conns) >= 8 {
		_ = conn.Close()
		return errors.New("proxy flow descriptor limit exceeded")
	}
	flow.conns = append(flow.conns, conn)
	return nil
}

func (flow *tcpFlow) close() {
	if flow == nil {
		return
	}
	flow.mu.Lock()
	defer flow.mu.Unlock()
	if flow.closed {
		return
	}
	flow.closed = true
	flow.cancel()
	for _, conn := range flow.conns {
		_ = conn.Close()
	}
	flow.conns = nil
}

func (req *adapterRequest) flowContext() context.Context {
	if req != nil && req.Flow != nil {
		return req.Flow.ctx
	}
	return context.Background()
}

// ReconcileActiveFlows fences stale authorizations, including connections still
// classifying traffic. New connections are compared with the same current store
// under flowMu, so an old snapshot cannot register after this sweep completes.
func (s *Server) ReconcileActiveFlows() {
	if s == nil {
		return
	}
	s.flowMu.Lock()
	for flow := range s.tcpFlows {
		if !s.store.IsCurrentBinding(flow.sourceIP, flow.compiled) {
			flow.close()
			delete(s.tcpFlows, flow)
		}
	}
	s.flowMu.Unlock()
	s.reconcileUDPSessions()
}

func (s *Server) closeTCPFlows() {
	s.closing.Store(true)
	s.flowMu.Lock()
	defer s.flowMu.Unlock()
	for flow := range s.tcpFlows {
		flow.close()
		delete(s.tcpFlows, flow)
	}
}
