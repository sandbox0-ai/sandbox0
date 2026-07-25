//go:build linux

package redirect

import (
	"context"
	"fmt"
	"net"
	"os/exec"
	"strings"
	"sync"

	"github.com/coreos/go-iptables/iptables"
	"github.com/vishvananda/netlink"
	"go.uber.org/zap"
	"golang.org/x/sys/unix"
)

const (
	ruleTableID        = 100
	policyRulePriority = 100
	mangleTable        = "mangle"
	natTable           = "nat"
)

type iptablesManager struct {
	cfg         Config
	logger      *zap.Logger
	ipt         iptablesClient
	syncMu      sync.Mutex
	initialized bool
	sandboxIPs  []string
	bypassCIDRs []string
}

type iptablesClient interface {
	ChainExists(table, chain string) (bool, error)
	ClearChain(table, chain string) error
	DeleteById(table, chain string, id int) error
	DeleteChain(table, chain string) error
	DeleteIfExists(table, chain string, rulespec ...string) error
	Insert(table, chain string, pos int, rulespec ...string) error
	List(table, chain string) ([]string, error)
	NewChain(table, chain string) error
}

func NewManager(cfg Config, logger *zap.Logger) Manager {
	if logger == nil {
		logger = zap.NewNop()
	}
	ipt, err := iptables.NewWithProtocol(iptables.ProtocolIPv4)
	if err != nil {
		logger.Error("Failed to initialize iptables", zap.Error(err))
	}
	var iptClient iptablesClient
	if ipt != nil {
		iptClient = ipt
	}
	return &iptablesManager{
		cfg:    cfg,
		logger: logger,
		ipt:    iptClient,
	}
}

func (m *iptablesManager) Sync(ctx context.Context, sandboxIPs []string, bypassCIDRs []string) error {
	return m.sync(ctx, sandboxIPs, bypassCIDRs, false)
}

func (m *iptablesManager) ForceSync(ctx context.Context, sandboxIPs []string, bypassCIDRs []string) error {
	return m.sync(ctx, sandboxIPs, bypassCIDRs, true)
}

func (m *iptablesManager) sync(
	ctx context.Context,
	sandboxIPs []string,
	bypassCIDRs []string,
	force bool,
) error {
	if m.cfg.ProxyHTTPPort == 0 || m.cfg.ProxyHTTPSPort == 0 {
		return fmt.Errorf("proxy ports are not configured")
	}
	if m.ipt == nil {
		return fmt.Errorf("iptables is not initialized")
	}
	sandboxIPs = normalizeIPs(sandboxIPs)
	bypassCIDRs = normalizeCIDRs(bypassCIDRs)

	m.syncMu.Lock()
	defer m.syncMu.Unlock()

	switch planRedirectSync(m.initialized, m.sandboxIPs, m.bypassCIDRs, sandboxIPs, bypassCIDRs, force) {
	case redirectSyncNoop:
		return nil
	case redirectSyncIPSet:
		if err := m.syncIPSet(ctx, sandboxIPs); err != nil {
			return err
		}
		m.sandboxIPs = append(m.sandboxIPs[:0], sandboxIPs...)
		m.logger.Info("IPSet-only redirect sync complete", zap.Int("sandbox_ips", len(sandboxIPs)))
		return nil
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
	if err := m.ensureNATChain(ctx); err != nil {
		return err
	}
	if err := m.ensureNATJump(ctx); err != nil {
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
	if err := m.ensureJump(ctx); err != nil {
		return err
	}
	if err := m.ensureNATJump(ctx); err != nil {
		return err
	}
	m.initialized = true
	m.sandboxIPs = append(m.sandboxIPs[:0], sandboxIPs...)
	m.bypassCIDRs = append(m.bypassCIDRs[:0], bypassCIDRs...)

	m.logger.Info("Iptables sync complete")
	return nil
}

type redirectSyncPlan int

const (
	redirectSyncFull redirectSyncPlan = iota
	redirectSyncIPSet
	redirectSyncNoop
)

func planRedirectSync(
	initialized bool,
	previousSandboxIPs []string,
	previousBypassCIDRs []string,
	sandboxIPs []string,
	bypassCIDRs []string,
	force bool,
) redirectSyncPlan {
	if force || !initialized || !sameStringSet(previousBypassCIDRs, bypassCIDRs) {
		return redirectSyncFull
	}
	if sameStringSet(previousSandboxIPs, sandboxIPs) {
		return redirectSyncNoop
	}
	return redirectSyncIPSet
}

func sameStringSet(left, right []string) bool {
	left = normalizeUnique(left)
	right = normalizeUnique(right)
	if len(left) != len(right) {
		return false
	}
	values := make(map[string]struct{}, len(left))
	for _, value := range left {
		values[value] = struct{}{}
	}
	for _, value := range right {
		if _, ok := values[value]; !ok {
			return false
		}
	}
	return true
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
	return m.ensureTopJump(ctx, mangleTable, "PREROUTING", chainName)
}

func (m *iptablesManager) ensureNATChain(ctx context.Context) error {
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

func (m *iptablesManager) ensureNATJump(ctx context.Context) error {
	return m.ensureTopJump(ctx, natTable, "PREROUTING", natChainName)
}

func (m *iptablesManager) ensureTopJump(ctx context.Context, table, chain, target string) error {
	_ = ctx
	rules, err := m.ipt.List(table, chain)
	if err != nil {
		return err
	}
	jumpLines := jumpRuleLineNumbers(rules, chain, target)
	if len(jumpLines) == 0 || jumpLines[0] != 1 {
		if err := m.ipt.Insert(table, chain, 1, "-j", target); err != nil {
			return err
		}
		rules, err = m.ipt.List(table, chain)
		if err != nil {
			return err
		}
		jumpLines = jumpRuleLineNumbers(rules, chain, target)
	}
	if len(jumpLines) == 0 || jumpLines[0] != 1 {
		return fmt.Errorf("ensure %s/%s jump to %s at line 1", table, chain, target)
	}
	for i := len(jumpLines) - 1; i >= 1; i-- {
		if err := m.ipt.DeleteById(table, chain, jumpLines[i]); err != nil {
			return err
		}
	}
	return nil
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
	if !ruleExists(rules, uint32(1), uint32(1), ruleTableID, policyRulePriority) {
		rule := netlink.NewRule()
		rule.Mark = 1
		mask := uint32(1)
		rule.Mask = &mask
		rule.Table = ruleTableID
		// The TPROXY mark must win before CNI source-based rules. Terway installs
		// per-pod source rules at priority 2048, while an unspecified priority is
		// assigned near 32765 and routes marked packets away from the local proxy.
		rule.Priority = policyRulePriority
		if err := netlink.RuleAdd(rule); err != nil && !isExist(err) {
			return err
		}
	}
	for i := range rules {
		rule := rules[i]
		if !ruleMatches(rule, uint32(1), uint32(1), ruleTableID) || rule.Priority == policyRulePriority {
			continue
		}
		if err := netlink.RuleDel(&rule); err != nil && !isNotExist(err) {
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

func ruleExists(rules []netlink.Rule, mark, mask uint32, table, priority int) bool {
	for _, rule := range rules {
		if ruleMatches(rule, mark, mask, table) && rule.Priority == priority {
			return true
		}
	}
	return false
}

func ruleMatches(rule netlink.Rule, mark, mask uint32, table int) bool {
	return rule.Table == table && rule.Mark == mark && rule.Mask != nil && *rule.Mask == mask
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

func jumpRuleLineNumbers(rules []string, chain, target string) []int {
	lines := []int{}
	ruleLine := 0
	for _, rule := range rules {
		fields := strings.Fields(rule)
		if len(fields) < 4 || fields[0] != "-A" || fields[1] != chain {
			continue
		}
		ruleLine++
		if !ruleJumpsTo(fields, target) {
			continue
		}
		lines = append(lines, ruleLine)
	}
	return lines
}

func ruleJumpsTo(fields []string, target string) bool {
	for i := 2; i < len(fields)-1; i++ {
		if fields[i] == "-j" && fields[i+1] == target {
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

func isNotExist(err error) bool {
	if err == nil {
		return false
	}
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "no such") || strings.Contains(message, "not found")
}
