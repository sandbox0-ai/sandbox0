//go:build !linux

package redirect

import (
	"context"
	"fmt"

	"go.uber.org/zap"
)

type noopManager struct{}

func NewManager(cfg Config, logger *zap.Logger) Manager {
	_ = cfg
	_ = logger
	return &noopManager{}
}

func (m *noopManager) Sync(ctx context.Context, sandboxIPs []string, bypassCIDRs []string) error {
	_ = ctx
	_ = sandboxIPs
	_ = bypassCIDRs
	return fmt.Errorf("iptables manager is only supported on linux")
}

func (m *noopManager) ForceSync(ctx context.Context, sandboxIPs []string, bypassCIDRs []string) error {
	return m.Sync(ctx, sandboxIPs, bypassCIDRs)
}

func (m *noopManager) Cleanup(ctx context.Context) error {
	_ = ctx
	return nil
}
