//go:build !linux

package conntrack

import (
	"context"
	"fmt"
	"net/netip"

	"go.uber.org/zap"
)

type Manager struct{}

type FlowKey struct {
	Proto   uint8
	SrcIP   netip.Addr
	DstIP   netip.Addr
	SrcPort uint16
	DstPort uint16
}

func NewManager(_ *zap.Logger) (*Manager, error) {
	return nil, fmt.Errorf("conntrack manager is only supported on linux")
}

func (m *Manager) Close() {}

func (m *Manager) CleanupFlows(_ context.Context, _ []FlowKey) {}
