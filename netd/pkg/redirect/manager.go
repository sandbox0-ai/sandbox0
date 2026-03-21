//go:build linux

package redirect

import (
	"context"
	"fmt"
	"net"
	"os/exec"
	"strings"

	"github.com/coreos/go-iptables/iptables"
	"github.com/vishvananda/netlink"
	"go.uber.org/zap"
	"golang.org/x/sys/unix"
)

const (
	ruleTableID = 100
	mangleTable = "mangle"
	natTable    = "nat"
)

type iptablesManager struct {
	cfg    Config
	logger *zap.Logger
	ipt    *iptables.IPTables
}

func NewManager(cfg Config, logger *zap.Logger) Manager {
	if logger == nil {
		logger = zap.NewNop()
	}
	ipt, err := iptables.NewWithProtocol(iptables.ProtocolIPv4)
	if err != nil {
		logger.Error("Failed to initialize iptables", zap.Error(err))
	}
	return &iptablesManager{
		cfg:    cfg,
		logger: logger,
		ipt:    ipt,
	}
}

func (m *iptablesManager) Sync(ctx context.Context, sandboxIPs []string, bypassCIDRs []string) error {
	if m.cfg.ProxyHTTPPort == 0 || m.cfg.ProxyHTTPSPort == 0 {
		return fmt.Errorf("proxy ports are not configured")
	}
	if m.ipt == nil {
		return fmt.Errorf("iptables is not initialized")
	}
	m.logger.Info(
		"Iptables sync start",
		zap.Int("sandbox_ips", len(sandboxIPs)),
		zap.Strings("sandbox_ips", sandboxIPs),
		zap.Int("bypass_cidrs", len(bypassCIDRs)),
		zap.Strings("bypass_cidrs", bypassCIDRs),
	)
	if err := m.ensurePolicyRouting(ctx); err != nil {
		return err
	}
	if err := m.ensureCustomChain(ctx); err != nil {
		return err
	}
	if err := m.ensureJump(ctx); err != nil {
		return err
	}
	if err := m.ensureNATBypassChain(ctx); err != nil {
		return err
	}
	if err := m.ensureNATBypassJump(ctx); err != nil {
		return err
	}
	if err := m.syncNATBypassChain(ctx); err != nil {
		return err
	}
	if err := m.syncIPSet(ctx, normalizeIPs(sandboxIPs)); err != nil {
		return err
	}

	restoreInput := buildIPTablesRestoreInput(m.cfg, bypassCIDRs)

	// 3. Apply atomically
	cmd := exec.CommandContext(ctx, "iptables-restore", "--noflush")
	cmd.Stdin = strings.NewReader(restoreInput)
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("iptables-restore failed: %s: %w", string(out), err)
	}

	m.logger.Info("Iptables sync complete")
	return nil
}

func (m *iptablesManager) syncIPSet(ctx context.Context, ips []string) error {
	cmd := exec.CommandContext(ctx, "ipset", "restore")
	cmd.Stdin = strings.NewReader(buildIPSetRestoreInput(ips))
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("ipset restore failed: %s: %w", string(out), err)
	}
	m.logger.Info("IPSet sync complete", zap.Int("ips", len(ips)), zap.Strings("ips_list", ips))
	return nil
}

func (m *iptablesManager) Cleanup(ctx context.Context) error {
	if err := m.flushCustomChain(ctx); err != nil {
		return err
	}
	_ = m.ipt.DeleteIfExists(mangleTable, "PREROUTING", "-j", chainName)
	_ = m.ipt.DeleteChain(mangleTable, chainName)
	_ = m.ipt.DeleteIfExists(natTable, "PREROUTING", "-j", natChainName)
	_ = m.ipt.ClearChain(natTable, natChainName)
	_ = m.ipt.DeleteChain(natTable, natChainName)

	// Cleanup ipset
	cmd := exec.CommandContext(ctx, "ipset", "destroy", ipsetName)
	return cmd.Run()
}

func (m *iptablesManager) ensureCustomChain(ctx context.Context) error {
	_ = ctx
	exists, err := m.ipt.ChainExists(mangleTable, chainName)
	if err != nil {
		return err
	}
	if exists {
		return nil
	}
	return m.ipt.NewChain(mangleTable, chainName)
}

func (m *iptablesManager) ensureJump(ctx context.Context) error {
	_ = ctx
	exists, err := m.ipt.Exists(mangleTable, "PREROUTING", "-j", chainName)
	if err != nil {
		return err
	}
	if exists {
		return nil
	}
	return m.ipt.Append(mangleTable, "PREROUTING", "-j", chainName)
}

func (m *iptablesManager) ensureNATBypassChain(ctx context.Context) error {
	_ = ctx
	exists, err := m.ipt.ChainExists(natTable, natChainName)
	if err != nil {
		return err
	}
	if exists {
		return nil
	}
	return m.ipt.NewChain(natTable, natChainName)
}

func (m *iptablesManager) ensureNATBypassJump(ctx context.Context) error {
	_ = ctx
	_ = m.ipt.DeleteIfExists(natTable, "PREROUTING", "-j", natChainName)
	return m.ipt.Insert(natTable, "PREROUTING", 1, "-j", natChainName)
}

func (m *iptablesManager) syncNATBypassChain(ctx context.Context) error {
	_ = ctx
	if err := m.ipt.ClearChain(natTable, natChainName); err != nil {
		return err
	}
	return m.ipt.Append(natTable, natChainName, natBypassRuleSpec()...)
}

func (m *iptablesManager) flushCustomChain(ctx context.Context) error {
	_ = ctx
	return m.ipt.ClearChain(mangleTable, chainName)
}

func (m *iptablesManager) ensurePolicyRouting(ctx context.Context) error {
	_ = ctx
	rules, err := netlink.RuleList(netlink.FAMILY_V4)
	if err != nil {
		return err
	}
	if !ruleExists(rules, uint32(1), uint32(1), ruleTableID) {
		rule := netlink.NewRule()
		rule.Mark = 1
		mask := uint32(1)
		rule.Mask = &mask
		rule.Table = ruleTableID
		if err := netlink.RuleAdd(rule); err != nil && !isExist(err) {
			return err
		}
	}

	filter := &netlink.Route{Table: ruleTableID}
	routes, err := netlink.RouteListFiltered(netlink.FAMILY_V4, filter, netlink.RT_FILTER_TABLE)
	if err != nil {
		return err
	}
	if !localRouteExists(routes) {
		_, dst, err := net.ParseCIDR("0.0.0.0/0")
		if err != nil {
			return err
		}
		lo, err := netlink.LinkByName("lo")
		if err != nil {
			return err
		}
		route := netlink.Route{
			LinkIndex: lo.Attrs().Index,
			Dst:       dst,
			Table:     ruleTableID,
			Scope:     netlink.SCOPE_HOST,
			Type:      unix.RTN_LOCAL,
		}
		if err := netlink.RouteAdd(&route); err != nil && !isExist(err) {
			return err
		}
	}
	return nil
}

func ruleExists(rules []netlink.Rule, mark, mask uint32, table int) bool {
	for _, rule := range rules {
		if rule.Table != table || rule.Mark != mark {
			continue
		}
		if rule.Mask == nil {
			continue
		}
		if *rule.Mask == mask {
			return true
		}
	}
	return false
}

func localRouteExists(routes []netlink.Route) bool {
	for _, route := range routes {
		if route.Dst == nil {
			continue
		}
		if route.Dst.String() == "0.0.0.0/0" && route.Type == unix.RTN_LOCAL {
			return true
		}
	}
	return false
}

func isExist(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(strings.ToLower(err.Error()), "exists")
}
