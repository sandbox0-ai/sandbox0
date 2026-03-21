package redirect

import (
	"bytes"
	"fmt"
	"strings"
)

const (
	chainName          = "NETD_PREROUTING"
	natChainName       = "NETD_NAT_PREROUTING"
	ipsetName          = "netd-sandbox-ips"
	tproxyMark         = "0x1/0x1"
	defaultLoopback    = "127.0.0.0/8"
	natBypassJumpMark  = tproxyMark
)

func buildIPTablesRestoreInput(cfg Config, bypassCIDRs []string) string {
	var buf bytes.Buffer
	buf.WriteString("*mangle\n")
	buf.WriteString(fmt.Sprintf("-F %s\n", chainName))

	bypass := append([]string{defaultLoopback}, bypassCIDRs...)
	for _, cidr := range normalizeCIDRs(bypass) {
		buf.WriteString(fmt.Sprintf("-A %s -d %s -j RETURN\n", chainName, cidr))
	}

	appendTPROXYRules(&buf, "tcp", 443, cfg.ProxyHTTPSPort)
	appendTPROXYRules(&buf, "tcp", 853, cfg.ProxyHTTPSPort)
	appendTPROXYRules(&buf, "udp", 443, cfg.ProxyHTTPSPort)
	appendTPROXYRules(&buf, "udp", 853, cfg.ProxyHTTPSPort)
	appendTPROXYRules(&buf, "tcp", 0, cfg.ProxyHTTPPort)
	appendTPROXYRules(&buf, "udp", 0, cfg.ProxyHTTPPort)

	buf.WriteString("COMMIT\n")
	return buf.String()
}

func appendTPROXYRules(buf *bytes.Buffer, protocol string, destPort int, proxyPort int) {
	if buf == nil || protocol == "" || proxyPort <= 0 {
		return
	}
	base := fmt.Sprintf("-A %s -m set --match-set %s src -p %s", chainName, ipsetName, protocol)
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

func natBypassRuleSpec() []string {
	return []string{"-m", "mark", "--mark", natBypassJumpMark, "-j", "ACCEPT"}
}

func buildIPSetRestoreInput(ips []string) string {
	var buf bytes.Buffer
	buf.WriteString(fmt.Sprintf("create %s hash:ip family inet -exist\n", ipsetName))
	buf.WriteString(fmt.Sprintf("flush %s\n", ipsetName))
	for _, ip := range ips {
		buf.WriteString(fmt.Sprintf("add %s %s -exist\n", ipsetName, ip))
	}
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
