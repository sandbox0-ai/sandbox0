//go:build linux

package redirect

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"os/exec"
	"strconv"
	"strings"

	"github.com/coreos/go-iptables/iptables"
	"github.com/vishvananda/netlink"
	"go.uber.org/zap"
	"golang.org/x/sys/unix"
)

const (
	chainName       = "NETD_PREROUTING"
	ipsetName       = "netd-sandbox-ips"
	tproxyMark      = "0x1/0x1"
	ruleTableID     = 100
	defaultLoopback = "127.0.0.0/8"
	mangleTable     = "mangle"
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
	if err := m.ensureChain(ctx); err != nil {
		return err
	}
	if err := m.ensureJump(ctx); err != nil {
		return err
	}
	if err := m.flushChain(ctx); err != nil {
		return err
	}

	bypass := append([]string{defaultLoopback}, bypassCIDRs...)
	for _, cidr := range normalizeCIDRs(bypass) {
		if err := m.appendRule(ctx, "-d", cidr, "-j", "RETURN"); err != nil {
			return err
		}
	}

	if err := m.syncIPSet(ctx, normalizeIPs(sandboxIPs)); err != nil {
		return err
	}

	// TCP 443
	if err := m.appendRule(ctx, "-m", "set", "--match-set", ipsetName, "src", "-p", "tcp", "--dport", "443", "-m", "conntrack", "--ctstate", "NEW", "-j", "TPROXY",
		"--on-port", strconv.Itoa(m.cfg.ProxyHTTPSPort), "--tproxy-mark", tproxyMark); err != nil {
		return err
	}
	if err := m.appendRule(ctx, "-m", "set", "--match-set", ipsetName, "src", "-p", "tcp", "--dport", "443", "-m", "socket", "--transparent", "-j", "TPROXY",
		"--on-port", strconv.Itoa(m.cfg.ProxyHTTPSPort), "--tproxy-mark", tproxyMark); err != nil {
		return err
	}

	// TCP 853
	if err := m.appendRule(ctx, "-m", "set", "--match-set", ipsetName, "src", "-p", "tcp", "--dport", "853", "-m", "conntrack", "--ctstate", "NEW", "-j", "TPROXY",
		"--on-port", strconv.Itoa(m.cfg.ProxyHTTPSPort), "--tproxy-mark", tproxyMark); err != nil {
		return err
	}
	if err := m.appendRule(ctx, "-m", "set", "--match-set", ipsetName, "src", "-p", "tcp", "--dport", "853", "-m", "socket", "--transparent", "-j", "TPROXY",
		"--on-port", strconv.Itoa(m.cfg.ProxyHTTPSPort), "--tproxy-mark", tproxyMark); err != nil {
		return err
	}

	// UDP 443
	if err := m.appendRule(ctx, "-m", "set", "--match-set", ipsetName, "src", "-p", "udp", "--dport", "443", "-m", "conntrack", "--ctstate", "NEW", "-j", "TPROXY",
		"--on-port", strconv.Itoa(m.cfg.ProxyHTTPSPort), "--tproxy-mark", tproxyMark); err != nil {
		return err
	}
	if err := m.appendRule(ctx, "-m", "set", "--match-set", ipsetName, "src", "-p", "udp", "--dport", "443", "-m", "socket", "--transparent", "-j", "TPROXY",
		"--on-port", strconv.Itoa(m.cfg.ProxyHTTPSPort), "--tproxy-mark", tproxyMark); err != nil {
		return err
	}

	// UDP 853
	if err := m.appendRule(ctx, "-m", "set", "--match-set", ipsetName, "src", "-p", "udp", "--dport", "853", "-m", "conntrack", "--ctstate", "NEW", "-j", "TPROXY",
		"--on-port", strconv.Itoa(m.cfg.ProxyHTTPSPort), "--tproxy-mark", tproxyMark); err != nil {
		return err
	}
	if err := m.appendRule(ctx, "-m", "set", "--match-set", ipsetName, "src", "-p", "udp", "--dport", "853", "-m", "socket", "--transparent", "-j", "TPROXY",
		"--on-port", strconv.Itoa(m.cfg.ProxyHTTPSPort), "--tproxy-mark", tproxyMark); err != nil {
		return err
	}

	// TCP All
	if err := m.appendRule(ctx, "-m", "set", "--match-set", ipsetName, "src", "-p", "tcp", "-m", "conntrack", "--ctstate", "NEW", "-j", "TPROXY",
		"--on-port", strconv.Itoa(m.cfg.ProxyHTTPPort), "--tproxy-mark", tproxyMark); err != nil {
		return err
	}
	if err := m.appendRule(ctx, "-m", "set", "--match-set", ipsetName, "src", "-p", "tcp", "-m", "socket", "--transparent", "-j", "TPROXY",
		"--on-port", strconv.Itoa(m.cfg.ProxyHTTPPort), "--tproxy-mark", tproxyMark); err != nil {
		return err
	}

	// UDP All
	if err := m.appendRule(ctx, "-m", "set", "--match-set", ipsetName, "src", "-p", "udp", "-m", "conntrack", "--ctstate", "NEW", "-j", "TPROXY",
		"--on-port", strconv.Itoa(m.cfg.ProxyHTTPPort), "--tproxy-mark", tproxyMark); err != nil {
		return err
	}
	if err := m.appendRule(ctx, "-m", "set", "--match-set", ipsetName, "src", "-p", "udp", "-m", "socket", "--transparent", "-j", "TPROXY",
		"--on-port", strconv.Itoa(m.cfg.ProxyHTTPPort), "--tproxy-mark", tproxyMark); err != nil {
		return err
	}

	m.logger.Info("Iptables sync complete")
	return nil
}

func (m *iptablesManager) syncIPSet(ctx context.Context, ips []string) error {
	var buf bytes.Buffer
	// Create set if not exists
	buf.WriteString(fmt.Sprintf("create %s hash:ip family inet -exist\n", ipsetName))
	// Flush set
	buf.WriteString(fmt.Sprintf("flush %s\n", ipsetName))
	// Add IPs
	for _, ip := range ips {
		buf.WriteString(fmt.Sprintf("add %s %s -exist\n", ipsetName, ip))
	}

	cmd := exec.CommandContext(ctx, "ipset", "restore")
	cmd.Stdin = &buf
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("ipset restore failed: %s: %w", string(out), err)
	}
	return nil
}

func (m *iptablesManager) Cleanup(ctx context.Context) error {
	if err := m.flushChain(ctx); err != nil {
		return err
	}
	_ = m.ipt.DeleteIfExists(mangleTable, "PREROUTING", "-j", chainName)
	_ = m.ipt.DeleteChain(mangleTable, chainName)

	// Cleanup ipset
	cmd := exec.CommandContext(ctx, "ipset", "destroy", ipsetName)
	_ = cmd.Run()

	return nil
}

func (m *iptablesManager) ensureChain(ctx context.Context) error {
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

func (m *iptablesManager) flushChain(ctx context.Context) error {
	_ = ctx
	return m.ipt.ClearChain(mangleTable, chainName)
}

func (m *iptablesManager) appendRule(ctx context.Context, args ...string) error {
	_ = ctx
	return m.ipt.Append(mangleTable, chainName, args...)
}

func normalizeIPs(values []string) []string {
	out := make([]string, 0, len(values))
	seen := map[string]struct{}{}
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out
}

func normalizeCIDRs(values []string) []string {
	out := make([]string, 0, len(values))
	seen := map[string]struct{}{}
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out
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
