// Package dataplane implements the network data plane for netd.
// It uses iptables/nftables for packet filtering and tc for traffic shaping.
package dataplane

import (
	"context"
	"fmt"
	"net"
	"os/exec"
	"strings"
	"sync"

	"github.com/sandbox0-ai/infra/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/infra/netd/pkg/watcher"
	"go.uber.org/zap"
)

// DataPlane manages network rules for sandboxes
type DataPlane struct {
	logger              *zap.Logger
	proxyHTTPPort       int
	proxyHTTPSPort      int
	procdPort           int
	failClosed          bool
	storageProxyCIDR    string
	clusterDNSCIDR      string
	internalGatewayCIDR string

	// Track applied rules per sandbox
	mu           sync.RWMutex
	sandboxRules map[string]*SandboxRules
}

// SandboxRules tracks rules applied for a sandbox
type SandboxRules struct {
	SandboxID    string
	PodIP        string
	VethName     string
	EgressRules  []string
	IngressRules []string
	TCClass      string
	Applied      bool
}

// NewDataPlane creates a new DataPlane
func NewDataPlane(
	logger *zap.Logger,
	proxyHTTPPort int,
	proxyHTTPSPort int,
	procdPort int,
	failClosed bool,
	storageProxyCIDR string,
	clusterDNSCIDR string,
	internalGatewayCIDR string,
) *DataPlane {
	return &DataPlane{
		logger:              logger,
		proxyHTTPPort:       proxyHTTPPort,
		proxyHTTPSPort:      proxyHTTPSPort,
		procdPort:           procdPort,
		failClosed:          failClosed,
		storageProxyCIDR:    storageProxyCIDR,
		clusterDNSCIDR:      clusterDNSCIDR,
		internalGatewayCIDR: internalGatewayCIDR,
		sandboxRules:        make(map[string]*SandboxRules),
	}
}

// Initialize sets up base iptables chains for netd
func (dp *DataPlane) Initialize(ctx context.Context) error {
	dp.logger.Info("Initializing dataplane")

	// Create custom chains for netd
	chains := []struct {
		table string
		chain string
	}{
		{"filter", "NETD-EGRESS"},
		{"filter", "NETD-INGRESS"},
		{"nat", "NETD-PREROUTING"},
		{"nat", "NETD-OUTPUT"},
		{"mangle", "NETD-EGRESS"},
	}

	for _, c := range chains {
		// Create chain if not exists
		if err := dp.runIPTables("-t", c.table, "-N", c.chain); err != nil {
			// Chain might already exist, ignore error
			dp.logger.Debug("Chain creation (may already exist)",
				zap.String("table", c.table),
				zap.String("chain", c.chain),
			)
		}

		// Flush chain
		if err := dp.runIPTables("-t", c.table, "-F", c.chain); err != nil {
			return fmt.Errorf("flush chain %s/%s: %w", c.table, c.chain, err)
		}
	}

	// Insert jumps to custom chains from built-in chains
	// These will be inserted at the beginning
	jumpRules := []struct {
		table     string
		chain     string
		target    string
		condition string
	}{
		{"filter", "FORWARD", "NETD-EGRESS", "-m comment --comment netd-egress"},
		{"filter", "FORWARD", "NETD-INGRESS", "-m comment --comment netd-ingress"},
		{"nat", "PREROUTING", "NETD-PREROUTING", "-m comment --comment netd-prerouting"},
		{"nat", "OUTPUT", "NETD-OUTPUT", "-m comment --comment netd-output"},
	}

	for _, r := range jumpRules {
		// Check if rule exists
		checkArgs := []string{"-t", r.table, "-C", r.chain, "-j", r.target}
		if r.condition != "" {
			checkArgs = append(checkArgs, strings.Fields(r.condition)...)
		}
		if err := dp.runIPTables(checkArgs...); err != nil {
			// Rule doesn't exist, insert it
			insertArgs := []string{"-t", r.table, "-I", r.chain, "1", "-j", r.target}
			if r.condition != "" {
				insertArgs = append(insertArgs, strings.Fields(r.condition)...)
			}
			if err := dp.runIPTables(insertArgs...); err != nil {
				return fmt.Errorf("insert jump rule: %w", err)
			}
		}
	}

	dp.logger.Info("Dataplane initialized")
	return nil
}

// ApplyPodRules applies network rules for a sandbox pod
func (dp *DataPlane) ApplyPodRules(
	ctx context.Context,
	info *watcher.SandboxInfo,
	networkPolicy *v1alpha1.SandboxNetworkPolicy,
	bandwidthPolicy *v1alpha1.SandboxBandwidthPolicy,
) error {
	dp.mu.Lock()
	defer dp.mu.Unlock()

	if info.PodIP == "" {
		return fmt.Errorf("pod IP is empty for sandbox %s", info.SandboxID)
	}

	dp.logger.Info("Applying rules for sandbox",
		zap.String("sandboxID", info.SandboxID),
		zap.String("podIP", info.PodIP),
	)

	rules := &SandboxRules{
		SandboxID: info.SandboxID,
		PodIP:     info.PodIP,
	}

	// Apply egress rules
	if err := dp.applyEgressRules(ctx, info, networkPolicy, rules); err != nil {
		return fmt.Errorf("apply egress rules: %w", err)
	}

	// Apply ingress rules
	if err := dp.applyIngressRules(ctx, info, networkPolicy, rules); err != nil {
		return fmt.Errorf("apply ingress rules: %w", err)
	}

	// Apply bandwidth rules
	if bandwidthPolicy != nil {
		if err := dp.applyBandwidthRules(ctx, info, bandwidthPolicy, rules); err != nil {
			return fmt.Errorf("apply bandwidth rules: %w", err)
		}
	}

	rules.Applied = true
	dp.sandboxRules[info.SandboxID] = rules

	dp.logger.Info("Rules applied for sandbox",
		zap.String("sandboxID", info.SandboxID),
	)

	return nil
}

// applyEgressRules applies egress (outbound) rules for a sandbox
func (dp *DataPlane) applyEgressRules(
	ctx context.Context,
	info *watcher.SandboxInfo,
	policy *v1alpha1.SandboxNetworkPolicy,
	rules *SandboxRules,
) error {
	podIP := info.PodIP
	sandboxID := info.SandboxID
	comment := fmt.Sprintf("sandbox:%s", sandboxID)

	// 1. Allow established connections
	if err := dp.runIPTables(
		"-t", "filter", "-A", "NETD-EGRESS",
		"-s", podIP,
		"-m", "conntrack", "--ctstate", "ESTABLISHED,RELATED",
		"-m", "comment", "--comment", comment,
		"-j", "ACCEPT",
	); err != nil {
		return err
	}

	// 2. Always allow storage-proxy
	if dp.storageProxyCIDR != "" {
		if err := dp.runIPTables(
			"-t", "filter", "-A", "NETD-EGRESS",
			"-s", podIP, "-d", dp.storageProxyCIDR,
			"-m", "comment", "--comment", comment+":storage-proxy",
			"-j", "ACCEPT",
		); err != nil {
			return err
		}
	}

	// 3. Allow DNS to cluster DNS
	if dp.clusterDNSCIDR != "" {
		if err := dp.runIPTables(
			"-t", "filter", "-A", "NETD-EGRESS",
			"-s", podIP, "-d", dp.clusterDNSCIDR,
			"-p", "udp", "--dport", "53",
			"-m", "comment", "--comment", comment+":dns",
			"-j", "ACCEPT",
		); err != nil {
			return err
		}
	}

	// 4. Redirect HTTP/HTTPS to proxy (for domain-based filtering)
	enforceProxyPorts := []int{80, 443}
	if policy != nil && policy.Spec.Egress != nil && len(policy.Spec.Egress.EnforceProxyPorts) > 0 {
		enforceProxyPorts = make([]int, len(policy.Spec.Egress.EnforceProxyPorts))
		for i, p := range policy.Spec.Egress.EnforceProxyPorts {
			enforceProxyPorts[i] = int(p)
		}
	}

	for _, port := range enforceProxyPorts {
		var proxyPort int
		if port == 80 {
			proxyPort = dp.proxyHTTPPort
		} else if port == 443 {
			proxyPort = dp.proxyHTTPSPort
		} else {
			proxyPort = dp.proxyHTTPSPort // Default to HTTPS proxy
		}

		if err := dp.runIPTables(
			"-t", "nat", "-A", "NETD-PREROUTING",
			"-s", podIP,
			"-p", "tcp", "--dport", fmt.Sprintf("%d", port),
			"-m", "comment", "--comment", comment+":proxy-redirect",
			"-j", "REDIRECT", "--to-ports", fmt.Sprintf("%d", proxyPort),
		); err != nil {
			return err
		}
	}

	// 5. Apply allowed CIDRs
	if policy != nil && policy.Spec.Egress != nil {
		for _, cidr := range policy.Spec.Egress.AllowedCIDRs {
			if err := dp.runIPTables(
				"-t", "filter", "-A", "NETD-EGRESS",
				"-s", podIP, "-d", cidr,
				"-m", "comment", "--comment", comment+":allow-cidr",
				"-j", "ACCEPT",
			); err != nil {
				return err
			}
		}
	}

	// 6. Block platform-denied CIDRs (RFC1918, metadata, etc.)
	deniedCIDRs := v1alpha1.PlatformDeniedCIDRs
	if policy != nil && policy.Spec.Egress != nil && len(policy.Spec.Egress.AlwaysDeniedCIDRs) > 0 {
		deniedCIDRs = policy.Spec.Egress.AlwaysDeniedCIDRs
	}

	for _, cidr := range deniedCIDRs {
		if err := dp.runIPTables(
			"-t", "filter", "-A", "NETD-EGRESS",
			"-s", podIP, "-d", cidr,
			"-m", "comment", "--comment", comment+":deny-internal",
			"-j", "DROP",
		); err != nil {
			return err
		}
	}

	// 7. Default action (deny by default for enterprise security)
	defaultAction := "DROP"
	if policy != nil && policy.Spec.Egress != nil && policy.Spec.Egress.DefaultAction == "allow" {
		defaultAction = "ACCEPT"
	}

	if err := dp.runIPTables(
		"-t", "filter", "-A", "NETD-EGRESS",
		"-s", podIP,
		"-m", "comment", "--comment", comment+":default",
		"-j", defaultAction,
	); err != nil {
		return err
	}

	return nil
}

// applyIngressRules applies ingress (inbound) rules for a sandbox
func (dp *DataPlane) applyIngressRules(
	ctx context.Context,
	info *watcher.SandboxInfo,
	policy *v1alpha1.SandboxNetworkPolicy,
	rules *SandboxRules,
) error {
	podIP := info.PodIP
	sandboxID := info.SandboxID
	comment := fmt.Sprintf("sandbox:%s", sandboxID)

	// 1. Allow established connections
	if err := dp.runIPTables(
		"-t", "filter", "-A", "NETD-INGRESS",
		"-d", podIP,
		"-m", "conntrack", "--ctstate", "ESTABLISHED,RELATED",
		"-m", "comment", "--comment", comment,
		"-j", "ACCEPT",
	); err != nil {
		return err
	}

	// 2. Allow internal-gateway to procd port
	if dp.internalGatewayCIDR != "" {
		if err := dp.runIPTables(
			"-t", "filter", "-A", "NETD-INGRESS",
			"-s", dp.internalGatewayCIDR, "-d", podIP,
			"-p", "tcp", "--dport", fmt.Sprintf("%d", dp.procdPort),
			"-m", "comment", "--comment", comment+":internal-gateway",
			"-j", "ACCEPT",
		); err != nil {
			return err
		}
	}

	// 3. Apply allowed source CIDRs from policy
	if policy != nil && policy.Spec.Ingress != nil {
		for _, cidr := range policy.Spec.Ingress.AllowedSourceCIDRs {
			if err := dp.runIPTables(
				"-t", "filter", "-A", "NETD-INGRESS",
				"-s", cidr, "-d", podIP,
				"-m", "comment", "--comment", comment+":allow-source",
				"-j", "ACCEPT",
			); err != nil {
				return err
			}
		}

		// Apply allowed ports
		for _, portSpec := range policy.Spec.Ingress.AllowedPorts {
			protocol := portSpec.Protocol
			if protocol == "" {
				protocol = "tcp"
			}
			if err := dp.runIPTables(
				"-t", "filter", "-A", "NETD-INGRESS",
				"-d", podIP,
				"-p", protocol, "--dport", fmt.Sprintf("%d", portSpec.Port),
				"-m", "comment", "--comment", comment+":allow-port",
				"-j", "ACCEPT",
			); err != nil {
				return err
			}
		}
	}

	// 4. Default deny for ingress
	defaultAction := "DROP"
	if policy != nil && policy.Spec.Ingress != nil && policy.Spec.Ingress.DefaultAction == "allow" {
		defaultAction = "ACCEPT"
	}

	if err := dp.runIPTables(
		"-t", "filter", "-A", "NETD-INGRESS",
		"-d", podIP,
		"-m", "comment", "--comment", comment+":default",
		"-j", defaultAction,
	); err != nil {
		return err
	}

	return nil
}

// applyBandwidthRules applies tc bandwidth shaping rules
func (dp *DataPlane) applyBandwidthRules(
	ctx context.Context,
	info *watcher.SandboxInfo,
	policy *v1alpha1.SandboxBandwidthPolicy,
	rules *SandboxRules,
) error {
	// Find the veth interface for this pod
	// This is a simplified implementation - in production, you'd need to
	// find the actual veth pair for the pod network namespace
	vethName := fmt.Sprintf("veth%s", info.SandboxID[:8])
	rules.VethName = vethName

	// Apply egress rate limit using tc htb
	if policy.Spec.EgressRateLimit != nil {
		rateLimit := policy.Spec.EgressRateLimit
		rateBits := rateLimit.RateBps
		burstBytes := rateLimit.BurstBytes
		if burstBytes == 0 {
			burstBytes = rateBits / 8 // Default burst to 1 second of data
		}

		// Create qdisc if not exists
		dp.runTC("qdisc", "add", "dev", vethName, "root", "handle", "1:", "htb", "default", "10")

		// Create class for this sandbox
		classID := fmt.Sprintf("1:%s", info.SandboxID[:4])
		if err := dp.runTC(
			"class", "add", "dev", vethName,
			"parent", "1:", "classid", classID,
			"htb", "rate", fmt.Sprintf("%dbit", rateBits),
			"burst", fmt.Sprintf("%d", burstBytes),
		); err != nil {
			dp.logger.Warn("Failed to add tc class", zap.Error(err))
		}

		// Add filter to match this pod's traffic
		if err := dp.runTC(
			"filter", "add", "dev", vethName,
			"protocol", "ip", "parent", "1:0",
			"prio", "1", "u32",
			"match", "ip", "src", info.PodIP+"/32",
			"flowid", classID,
		); err != nil {
			dp.logger.Warn("Failed to add tc filter", zap.Error(err))
		}

		rules.TCClass = classID
	}

	return nil
}

// RemovePodRules removes all rules for a sandbox
func (dp *DataPlane) RemovePodRules(ctx context.Context, sandboxID string) error {
	dp.mu.Lock()
	defer dp.mu.Unlock()

	rules, ok := dp.sandboxRules[sandboxID]
	if !ok {
		return nil
	}

	dp.logger.Info("Removing rules for sandbox",
		zap.String("sandboxID", sandboxID),
	)

	comment := fmt.Sprintf("sandbox:%s", sandboxID)

	// Remove iptables rules by comment
	tables := []string{"filter", "nat", "mangle"}
	chains := []string{"NETD-EGRESS", "NETD-INGRESS", "NETD-PREROUTING", "NETD-OUTPUT"}

	for _, table := range tables {
		for _, chain := range chains {
			// List rules and find ones with our comment
			output, err := exec.CommandContext(ctx, "iptables", "-t", table, "-S", chain).Output()
			if err != nil {
				continue // Chain might not exist in this table
			}

			lines := strings.Split(string(output), "\n")
			for _, line := range lines {
				if strings.Contains(line, comment) {
					// Extract rule and delete it
					// Rule format: -A CHAIN ... -> we need -D CHAIN ...
					if strings.HasPrefix(line, "-A ") {
						deleteRule := strings.Replace(line, "-A ", "-D ", 1)
						args := strings.Fields(deleteRule)
						dp.runIPTables(append([]string{"-t", table}, args...)...)
					}
				}
			}
		}
	}

	// Remove tc rules
	if rules.VethName != "" && rules.TCClass != "" {
		dp.runTC("filter", "del", "dev", rules.VethName, "parent", "1:0")
		dp.runTC("class", "del", "dev", rules.VethName, "classid", rules.TCClass)
	}

	delete(dp.sandboxRules, sandboxID)

	dp.logger.Info("Rules removed for sandbox",
		zap.String("sandboxID", sandboxID),
	)

	return nil
}

// Cleanup removes all netd rules
func (dp *DataPlane) Cleanup(ctx context.Context) error {
	dp.logger.Info("Cleaning up dataplane")

	// Flush all netd chains
	chains := []struct {
		table string
		chain string
	}{
		{"filter", "NETD-EGRESS"},
		{"filter", "NETD-INGRESS"},
		{"nat", "NETD-PREROUTING"},
		{"nat", "NETD-OUTPUT"},
		{"mangle", "NETD-EGRESS"},
	}

	for _, c := range chains {
		dp.runIPTables("-t", c.table, "-F", c.chain)
	}

	// Clear sandbox rules map
	dp.mu.Lock()
	dp.sandboxRules = make(map[string]*SandboxRules)
	dp.mu.Unlock()

	dp.logger.Info("Dataplane cleaned up")
	return nil
}

// runIPTables executes an iptables command
func (dp *DataPlane) runIPTables(args ...string) error {
	cmd := exec.Command("iptables", args...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		dp.logger.Debug("iptables command failed",
			zap.Strings("args", args),
			zap.String("output", string(output)),
			zap.Error(err),
		)
		return fmt.Errorf("iptables %v: %w (%s)", args, err, string(output))
	}
	return nil
}

// runTC executes a tc command
func (dp *DataPlane) runTC(args ...string) error {
	cmd := exec.Command("tc", args...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		dp.logger.Debug("tc command failed",
			zap.Strings("args", args),
			zap.String("output", string(output)),
			zap.Error(err),
		)
		return fmt.Errorf("tc %v: %w (%s)", args, err, string(output))
	}
	return nil
}

// IsPrivateIP checks if an IP is in private/reserved ranges
func IsPrivateIP(ip net.IP) bool {
	privateRanges := []string{
		"10.0.0.0/8",
		"127.0.0.0/8",
		"169.254.0.0/16",
		"172.16.0.0/12",
		"192.168.0.0/16",
		"fc00::/7",
		"fe80::/10",
	}

	for _, cidr := range privateRanges {
		_, network, _ := net.ParseCIDR(cidr)
		if network.Contains(ip) {
			return true
		}
	}
	return false
}
