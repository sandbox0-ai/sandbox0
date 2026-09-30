//go:build !linux

package conntrack

import (
	"context"
	"fmt"

	"go.uber.org/zap"
)

type Manager struct{}

func NewManager(_ *zap.Logger) (*Manager, error) {
	return nil, fmt.Errorf("conntrack manager is only supported on linux")
}

func (m *Manager) Close() {}

func (m *Manager) CleanupFlows(_ context.Context, _ []FlowKey) {}
