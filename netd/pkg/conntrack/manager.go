//go:build linux

package conntrack

import (
	"context"
	"fmt"
	"net/netip"
	"sync"

	ct "github.com/ti-mo/conntrack"
	"go.uber.org/zap"
)

type Manager struct {
	logger  *zap.Logger
	mu      sync.Mutex
	conn    *ct.Conn
	enabled bool
}

type FlowKey struct {
	Proto   uint8
	SrcIP   netip.Addr
	DstIP   netip.Addr
	SrcPort uint16
	DstPort uint16
}

func NewManager(logger *zap.Logger) (*Manager, error) {
	if logger == nil {
		logger = zap.NewNop()
	}
	conn, err := ct.Dial(nil)
	if err != nil {
		return &Manager{
			logger:  logger,
			conn:    nil,
			enabled: false,
		}, fmt.Errorf("conntrack dial: %w", err)
	}
	return &Manager{
		logger:  logger,
		conn:    conn,
		enabled: true,
	}, nil
}

func (m *Manager) Close() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.conn != nil {
		_ = m.conn.Close()
		m.conn = nil
	}
}

func (m *Manager) CleanupFlows(_ context.Context, flows []FlowKey) {
	if len(flows) == 0 {
		return
	}
	m.mu.Lock()
	conn := m.conn
	enabled := m.enabled
	m.mu.Unlock()
	if conn == nil || !enabled {
		m.logger.Warn("Conntrack connection is nil")
		return
	}
	for _, flow := range flows {
		if !flow.SrcIP.IsValid() || !flow.DstIP.IsValid() {
			continue
		}
		f := ct.NewFlow(flow.Proto, 0, flow.SrcIP, flow.DstIP, flow.SrcPort, flow.DstPort, 120, 0)
		if err := conn.Delete(f); err != nil {
			m.logger.Info("Conntrack delete failed",
				zap.String("src_ip", flow.SrcIP.String()),
				zap.String("dst_ip", flow.DstIP.String()),
				zap.Uint16("src_port", flow.SrcPort),
				zap.Uint16("dst_port", flow.DstPort),
				zap.Error(err),
			)
		}
	}
}
