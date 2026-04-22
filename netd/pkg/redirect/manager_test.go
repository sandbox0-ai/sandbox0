//go:build linux

package redirect

import (
	"strings"
	"testing"
)

func TestBuildIPTablesRestoreInputRoutesSpecificAndGenericTraffic(t *testing.T) {
	cfg := Config{
		ProxyHTTPPort:  18080,
		ProxyHTTPSPort: 18443,
	}

	restore := buildIPTablesRestoreInput(cfg, []string{"10.0.0.0/8", "10.0.0.0/8", " 192.168.0.0/16 "})

	mustContain(t, restore, "-A "+chainName+" -d "+defaultLoopback+" -j RETURN")
	mustContain(t, restore, "-A "+chainName+" -d 10.0.0.0/8 -j RETURN")
	mustContain(t, restore, "-A "+chainName+" -d 192.168.0.0/16 -j RETURN")

	mustContain(t, restore, "-p tcp --dport 443 -m conntrack --ctstate NEW -j TPROXY --on-port 18443")
	mustContain(t, restore, "-p tcp --dport 443 -m connmark --mark 0x1/0x1 -j TPROXY --on-port 18443")
	mustContain(t, restore, "-p tcp --dport 443 -m conntrack --ctstate NEW -j CONNMARK --set-mark 0x1/0x1")
	mustContain(t, restore, "-p tcp --dport 853 -m socket --transparent -j TPROXY --on-port 18443")
	mustContain(t, restore, "-p udp --dport 443 -m conntrack --ctstate NEW -j TPROXY --on-port 18443")
	mustContain(t, restore, "-p udp --dport 443 -m connmark --mark 0x1/0x1 -j TPROXY --on-port 18443")
	mustContain(t, restore, "-p udp --dport 443 -m conntrack --ctstate NEW -j CONNMARK --set-mark 0x1/0x1")
	mustContain(t, restore, "-p udp --dport 853 -m socket --transparent -j TPROXY --on-port 18443")

	mustContain(t, restore, "-p tcp -m conntrack --ctstate NEW -j TPROXY --on-port 18080")
	mustContain(t, restore, "-p tcp -m connmark --mark 0x1/0x1 -j TPROXY --on-port 18080")
	mustContain(t, restore, "-p tcp -m conntrack --ctstate NEW -j CONNMARK --set-mark 0x1/0x1")
	mustContain(t, restore, "-p tcp -m socket --transparent -j TPROXY --on-port 18080")
	mustContain(t, restore, "-p udp -m conntrack --ctstate NEW -j TPROXY --on-port 18080")
	mustContain(t, restore, "-p udp -m connmark --mark 0x1/0x1 -j TPROXY --on-port 18080")
	mustContain(t, restore, "-p udp -m conntrack --ctstate NEW -j CONNMARK --set-mark 0x1/0x1")
	mustContain(t, restore, "-p udp -m socket --transparent -j TPROXY --on-port 18080")

	if strings.Count(restore, "-d 10.0.0.0/8 -j RETURN") != 1 {
		t.Fatalf("expected duplicate bypass CIDRs to be normalized once, got:\n%s", restore)
	}
}

func TestBuildIPSetRestoreInput(t *testing.T) {
	restore := buildIPSetRestoreInput([]string{"10.0.0.2", "10.0.0.3"})

	mustContain(t, restore, "create "+ipsetName+" hash:ip family inet -exist")
	mustContain(t, restore, "flush "+ipsetName)
	mustContain(t, restore, "add "+ipsetName+" 10.0.0.2 -exist")
	mustContain(t, restore, "add "+ipsetName+" 10.0.0.3 -exist")
}

func TestNatBypassRuleSpecSkipsDNATForMarkedPackets(t *testing.T) {
	if got, want := natBypassRuleSpec(), []string{"-m", "mark", "--mark", tproxyMark, "-j", "ACCEPT"}; strings.Join(got, " ") != strings.Join(want, " ") {
		t.Fatalf("unexpected nat bypass rule spec: %#v", got)
	}
}

func TestNormalizeInputsDeduplicateAndTrim(t *testing.T) {
	ips := normalizeIPs([]string{" 10.0.0.2 ", "", "10.0.0.2", "10.0.0.3"})
	if len(ips) != 2 || ips[0] != "10.0.0.2" || ips[1] != "10.0.0.3" {
		t.Fatalf("unexpected normalized ips: %#v", ips)
	}

	cidrs := normalizeCIDRs([]string{" 10.0.0.0/8 ", "", "10.0.0.0/8", "192.168.0.0/16"})
	if len(cidrs) != 2 || cidrs[0] != "10.0.0.0/8" || cidrs[1] != "192.168.0.0/16" {
		t.Fatalf("unexpected normalized cidrs: %#v", cidrs)
	}
}

func mustContain(t *testing.T, got string, want string) {
	t.Helper()
	if !strings.Contains(got, want) {
		t.Fatalf("expected output to contain %q, got:\n%s", want, got)
	}
}
