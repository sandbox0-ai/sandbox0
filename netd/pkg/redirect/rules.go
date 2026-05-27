//go:build linux

package redirect

import (
	"bytes"
	"fmt"
	"strings"
)

const (
	chainName         = "NETD_PREROUTING"
	natChainName      = "NETD_NAT_PREROUTING"
	ipsetName         = "netd-sandbox-ips"
	nextIPSetName     = "netd-sandbox-ips-next"
	tproxyMark        = "0x1/0x1"
	defaultLoopback   = "127.0.0.0/8"
	natBypassJumpMark = tproxyMark
	calicoWorkloadIF  = "cali+"
	ciliumWorkloadIF  = "lxc+"
)

func buildIPTablesRestoreInput(cfg Config, bypassCIDRs []string) string {
	var buf bytes.Buffer
	bypass := normalizeCIDRs(append([]string{defaultLoopback}, bypassCIDRs...))

	buf.WriteString("*mangle\n")
	buf.WriteString(fmt.Sprintf("-F %s\n", chainName))

	for _, cidr := range bypass {
		buf.WriteString(fmt.Sprintf("-A %s -d %s -j RETURN\n", chainName, cidr))
	}

	// Calico/Canal and Cilium native mode need TCP TPROXY on their workload
	// veth interfaces. Bridge CNIs use the NAT REDIRECT fallback below.
	for _, inputInterface := range []string{calicoWorkloadIF, ciliumWorkloadIF} {
		appendTPROXYRules(&buf, inputInterface, "tcp", 443, cfg.ProxyHTTPSPort)
		appendTPROXYRules(&buf, inputInterface, "tcp", 853, cfg.ProxyHTTPSPort)
		appendTPROXYRules(&buf, inputInterface, "tcp", 0, cfg.ProxyHTTPPort)
	}
	appendTPROXYRules(&buf, "", "udp", 443, cfg.ProxyHTTPSPort)
	appendTPROXYRules(&buf, "", "udp", 853, cfg.ProxyHTTPSPort)
	appendTPROXYRules(&buf, "", "udp", 0, cfg.ProxyHTTPPort)

	buf.WriteString("COMMIT\n")

	buf.WriteString("*nat\n")
	buf.WriteString(fmt.Sprintf("-F %s\n", natChainName))
	buf.WriteString(fmt.Sprintf("-A %s -m mark --mark %s -j ACCEPT\n", natChainName, natBypassJumpMark))
	for _, cidr := range bypass {
		buf.WriteString(fmt.Sprintf("-A %s -d %s -j RETURN\n", natChainName, cidr))
	}
	appendTCPRedirectRules(&buf, 443, cfg.ProxyHTTPSPort)
	appendTCPRedirectRules(&buf, 853, cfg.ProxyHTTPSPort)
	appendTCPRedirectRules(&buf, 0, cfg.ProxyHTTPPort)
	buf.WriteString("COMMIT\n")

	return buf.String()
}

func appendTPROXYRules(buf *bytes.Buffer, inputInterface, protocol string, destPort int, proxyPort int) {
	if buf == nil || protocol == "" || proxyPort <= 0 {
		return
	}
	base := fmt.Sprintf("-A %s", chainName)
	if inputInterface != "" {
		base += fmt.Sprintf(" -i %s", inputInterface)
	}
	base += fmt.Sprintf(" -m set --match-set %s src -p %s", ipsetName, protocol)
	if destPort > 0 {
		base += fmt.Sprintf(" --dport %d", destPort)
	}
	_, _ = fmt.Fprintf(buf, "%s -m connmark --mark %s -j TPROXY --on-port %d --tproxy-mark %s\n",
		base, tproxyMark, proxyPort, tproxyMark)
	_, _ = fmt.Fprintf(buf, "%s -m conntrack --ctstate NEW -j CONNMARK --set-mark %s\n",
		base, tproxyMark)
	_, _ = fmt.Fprintf(buf, "%s -m conntrack --ctstate NEW -j TPROXY --on-port %d --tproxy-mark %s\n",
		base, proxyPort, tproxyMark)
	_, _ = fmt.Fprintf(buf, "%s -m socket --transparent -j TPROXY --on-port %d --tproxy-mark %s\n",
		base, proxyPort, tproxyMark)
}

func appendTCPRedirectRules(buf *bytes.Buffer, destPort int, proxyPort int) {
	if buf == nil || proxyPort <= 0 {
		return
	}
	base := fmt.Sprintf("-A %s -m set --match-set %s src -p tcp", natChainName, ipsetName)
	if destPort > 0 {
		base += fmt.Sprintf(" --dport %d", destPort)
	}
	_, _ = fmt.Fprintf(buf, "%s -j REDIRECT --to-ports %d\n", base, proxyPort)
}

func buildIPSetRestoreInput(ips []string) string {
	var buf bytes.Buffer
	buf.WriteString(fmt.Sprintf("create %s hash:ip family inet -exist\n", ipsetName))
	buf.WriteString(fmt.Sprintf("create %s hash:ip family inet -exist\n", nextIPSetName))
	buf.WriteString(fmt.Sprintf("flush %s\n", nextIPSetName))
	for _, ip := range ips {
		buf.WriteString(fmt.Sprintf("add %s %s -exist\n", nextIPSetName, ip))
	}
	buf.WriteString(fmt.Sprintf("swap %s %s\n", nextIPSetName, ipsetName))
	buf.WriteString(fmt.Sprintf("destroy %s\n", nextIPSetName))
	return buf.String()
}

func normalizeIPs(values []string) []string {
	return normalizeUnique(values)
}

func normalizeCIDRs(values []string) []string {
	return normalizeUnique(values)
}

func normalizeUnique(values []string) []string {
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
