//go:build linux

package redirect

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/vishvananda/netlink"
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

	mustContain(t, restore, "-i cali+ -m set --match-set "+ipsetName+" src -p tcp --dport 443 -m conntrack --ctstate NEW -j TPROXY --on-port 18443")
	mustContain(t, restore, "-i cali+ -m set --match-set "+ipsetName+" src -p tcp --dport 443 -m connmark --mark 0x1/0x1 -j TPROXY --on-port 18443")
	mustContain(t, restore, "-i cali+ -m set --match-set "+ipsetName+" src -p tcp --dport 853 -m socket --transparent -j TPROXY --on-port 18443")
	mustContain(t, restore, "-i cali+ -m set --match-set "+ipsetName+" src -p tcp -m conntrack --ctstate NEW -j TPROXY --on-port 18080")
	mustContain(t, restore, "-i cali+ -m set --match-set "+ipsetName+" src -p tcp -m connmark --mark 0x1/0x1 -j TPROXY --on-port 18080")
	mustContain(t, restore, "-i cali+ -m set --match-set "+ipsetName+" src -p tcp -m socket --transparent -j TPROXY --on-port 18080")
	mustContain(t, restore, "-i lxc+ -m set --match-set "+ipsetName+" src -p tcp --dport 443 -m conntrack --ctstate NEW -j TPROXY --on-port 18443")
	mustContain(t, restore, "-i lxc+ -m set --match-set "+ipsetName+" src -p tcp --dport 853 -m socket --transparent -j TPROXY --on-port 18443")
	mustContain(t, restore, "-i lxc+ -m set --match-set "+ipsetName+" src -p tcp -m conntrack --ctstate NEW -j TPROXY --on-port 18080")
	mustContain(t, restore, "-i lxc+ -m set --match-set "+ipsetName+" src -p tcp -m socket --transparent -j TPROXY --on-port 18080")
	mustContain(t, restore, "-p udp --dport 443 -m conntrack --ctstate NEW -j TPROXY --on-port 18443")
	mustContain(t, restore, "-p udp --dport 443 -m connmark --mark 0x1/0x1 -j TPROXY --on-port 18443")
	mustContain(t, restore, "-p udp --dport 443 -m conntrack --ctstate NEW -j CONNMARK --set-mark 0x1/0x1")
	mustContain(t, restore, "-p udp --dport 853 -m socket --transparent -j TPROXY --on-port 18443")

	mustContain(t, restore, "-p udp -m conntrack --ctstate NEW -j TPROXY --on-port 18080")
	mustContain(t, restore, "-p udp -m connmark --mark 0x1/0x1 -j TPROXY --on-port 18080")
	mustContain(t, restore, "-p udp -m conntrack --ctstate NEW -j CONNMARK --set-mark 0x1/0x1")
	mustContain(t, restore, "-p udp -m socket --transparent -j TPROXY --on-port 18080")

	mustContain(t, restore, "-A "+natChainName+" -m mark --mark 0x1/0x1 -j ACCEPT")
	mustContain(t, restore, "-A "+natChainName+" -d "+defaultLoopback+" -j RETURN")
	mustContain(t, restore, "-A "+natChainName+" -d 10.0.0.0/8 -j RETURN")
	mustContain(t, restore, "-p tcp --dport 443 -j REDIRECT --to-ports 18443")
	mustContain(t, restore, "-p tcp --dport 853 -j REDIRECT --to-ports 18443")
	mustContain(t, restore, "-p tcp -j REDIRECT --to-ports 18080")

	mustNotContain(t, restore, "-A "+chainName+" -m set --match-set "+ipsetName+" src -p tcp")

	if strings.Count(restore, "-d 10.0.0.0/8 -j RETURN") != 2 {
		t.Fatalf("expected duplicate bypass CIDRs to be normalized once per chain, got:\n%s", restore)
	}
}

func TestBuildIPSetRestoreInput(t *testing.T) {
	restore := buildIPSetRestoreInput([]string{"10.0.0.2", "10.0.0.3"})

	mustContain(t, restore, "create "+ipsetName+" hash:ip family inet -exist")
	mustContain(t, restore, "create "+nextIPSetName+" hash:ip family inet -exist")
	mustContain(t, restore, "flush "+nextIPSetName)
	mustContain(t, restore, "add "+nextIPSetName+" 10.0.0.2 -exist")
	mustContain(t, restore, "add "+nextIPSetName+" 10.0.0.3 -exist")
	mustContain(t, restore, "swap "+nextIPSetName+" "+ipsetName)
	mustContain(t, restore, "destroy "+nextIPSetName)
}

func TestEnsureTopJumpInsertsMissingJumpAtFirstRule(t *testing.T) {
	ipt := newFakeIPTables([]string{"-j CNI_PREROUTING"})
	manager := &iptablesManager{ipt: ipt}

	if err := manager.ensureTopJump(context.Background(), mangleTable, "PREROUTING", chainName); err != nil {
		t.Fatal(err)
	}

	wantRules := []string{"-j " + chainName, "-j CNI_PREROUTING"}
	assertStringSlicesEqual(t, ipt.rulesFor(mangleTable, "PREROUTING"), wantRules)
	assertStringSlicesEqual(t, ipt.ops, []string{"insert mangle/PREROUTING 1 -j " + chainName})
}

func TestEnsureTopJumpInsertsBeforeDeletingExistingLowerJump(t *testing.T) {
	ipt := newFakeIPTables([]string{"-j CNI_PREROUTING", "-j " + chainName})
	manager := &iptablesManager{ipt: ipt}

	if err := manager.ensureTopJump(context.Background(), mangleTable, "PREROUTING", chainName); err != nil {
		t.Fatal(err)
	}

	wantRules := []string{"-j " + chainName, "-j CNI_PREROUTING"}
	assertStringSlicesEqual(t, ipt.rulesFor(mangleTable, "PREROUTING"), wantRules)
	assertStringSlicesEqual(t, ipt.ops, []string{
		"insert mangle/PREROUTING 1 -j " + chainName,
		"delete-by-id mangle/PREROUTING 3 -j " + chainName,
	})
}

func TestEnsureTopJumpDeletesDuplicateLowerJumpWithoutReinserting(t *testing.T) {
	ipt := newFakeIPTables([]string{"-j " + chainName, "-j CNI_PREROUTING", "-j " + chainName})
	manager := &iptablesManager{ipt: ipt}

	if err := manager.ensureTopJump(context.Background(), mangleTable, "PREROUTING", chainName); err != nil {
		t.Fatal(err)
	}

	wantRules := []string{"-j " + chainName, "-j CNI_PREROUTING"}
	assertStringSlicesEqual(t, ipt.rulesFor(mangleTable, "PREROUTING"), wantRules)
	assertStringSlicesEqual(t, ipt.ops, []string{"delete-by-id mangle/PREROUTING 3 -j " + chainName})
}

func TestJumpRuleLineNumbersOnlyCountsRulesInTargetChain(t *testing.T) {
	rules := []string{
		"-P PREROUTING ACCEPT",
		"-N " + chainName,
		"-A PREROUTING -m comment --comment cni -j CNI_PREROUTING",
		"-A PREROUTING -m comment --comment netd -j " + chainName,
		"-A OUTPUT -j " + chainName,
		"-A PREROUTING -g " + chainName,
		"-A PREROUTING -j " + chainName,
	}

	got := jumpRuleLineNumbers(rules, "PREROUTING", chainName)
	assertIntsEqual(t, got, []int{2, 4})
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

func TestPlanRedirectSyncUsesIPSetOnlyForSandboxChanges(t *testing.T) {
	previousIPs := []string{"10.0.0.2"}
	previousBypass := []string{"10.0.0.0/8"}

	if got := planRedirectSync(true, previousIPs, previousBypass, []string{"10.0.0.2", "10.0.0.3"}, previousBypass, false); got != redirectSyncIPSet {
		t.Fatalf("plan = %v, want IPSet-only sync", got)
	}
	if got := planRedirectSync(true, previousIPs, previousBypass, previousIPs, previousBypass, false); got != redirectSyncNoop {
		t.Fatalf("plan = %v, want no-op sync", got)
	}
}

func TestPlanRedirectSyncKeepsPeriodicAndBypassReconciliationFull(t *testing.T) {
	ips := []string{"10.0.0.2"}
	bypass := []string{"10.0.0.0/8"}

	tests := []struct {
		name        string
		initialized bool
		nextBypass  []string
		force       bool
	}{
		{name: "initial", initialized: false, nextBypass: bypass},
		{name: "periodic", initialized: true, nextBypass: bypass, force: true},
		{name: "bypass changed", initialized: true, nextBypass: []string{"192.168.0.0/16"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := planRedirectSync(tt.initialized, ips, bypass, ips, tt.nextBypass, tt.force); got != redirectSyncFull {
				t.Fatalf("plan = %v, want full sync", got)
			}
		})
	}
}

func TestPlanRedirectSyncIgnoresInputOrder(t *testing.T) {
	previousIPs := []string{"10.0.0.2", "10.0.0.3"}
	previousBypass := []string{"10.0.0.0/8", "192.168.0.0/16"}

	got := planRedirectSync(
		true,
		previousIPs,
		previousBypass,
		[]string{"10.0.0.3", "10.0.0.2"},
		[]string{"192.168.0.0/16", "10.0.0.0/8"},
		false,
	)
	if got != redirectSyncNoop {
		t.Fatalf("plan = %v, want no-op sync", got)
	}
}

func TestRuleExistsRequiresPriorityBeforeCNISourceRules(t *testing.T) {
	mask := uint32(1)
	rules := []netlink.Rule{
		{Mark: 1, Mask: &mask, Table: ruleTableID, Priority: 32765},
	}

	if ruleExists(rules, 1, 1, ruleTableID, policyRulePriority) {
		t.Fatal("late fallback rule must not satisfy the required TPROXY policy rule")
	}

	rules = append(rules, netlink.Rule{
		Mark:     1,
		Mask:     &mask,
		Table:    ruleTableID,
		Priority: policyRulePriority,
	})
	if !ruleExists(rules, 1, 1, ruleTableID, policyRulePriority) {
		t.Fatal("expected the high-priority TPROXY policy rule to match")
	}
}

func TestRuleMatchesRequiresExactMarkMaskAndTable(t *testing.T) {
	mask := uint32(1)
	rule := netlink.Rule{Mark: 1, Mask: &mask, Table: ruleTableID}

	if !ruleMatches(rule, 1, 1, ruleTableID) {
		t.Fatal("expected exact rule signature to match")
	}
	if ruleMatches(rule, 2, 1, ruleTableID) {
		t.Fatal("unexpected mark match")
	}
	if ruleMatches(rule, 1, 2, ruleTableID) {
		t.Fatal("unexpected mask match")
	}
	if ruleMatches(rule, 1, 1, ruleTableID+1) {
		t.Fatal("unexpected table match")
	}
	rule.Mask = nil
	if ruleMatches(rule, 1, 1, ruleTableID) {
		t.Fatal("rule without a mask must not match")
	}
}

func mustContain(t *testing.T, got string, want string) {
	t.Helper()
	if !strings.Contains(got, want) {
		t.Fatalf("expected output to contain %q, got:\n%s", want, got)
	}
}

func mustNotContain(t *testing.T, got string, want string) {
	t.Helper()
	if strings.Contains(got, want) {
		t.Fatalf("expected output not to contain %q, got:\n%s", want, got)
	}
}

type fakeIPTables struct {
	rules map[string][]string
	ops   []string
}

func newFakeIPTables(preroutingRules []string) *fakeIPTables {
	fake := &fakeIPTables{rules: map[string][]string{}}
	fake.rules[fakeKey(mangleTable, "PREROUTING")] = append([]string(nil), preroutingRules...)
	return fake
}

func (f *fakeIPTables) ChainExists(table, chain string) (bool, error) {
	_, ok := f.rules[fakeKey(table, chain)]
	return ok, nil
}

func (f *fakeIPTables) ClearChain(table, chain string) error {
	f.rules[fakeKey(table, chain)] = nil
	f.ops = append(f.ops, fmt.Sprintf("clear %s/%s", table, chain))
	return nil
}

func (f *fakeIPTables) DeleteById(table, chain string, id int) error {
	key := fakeKey(table, chain)
	rules := f.rules[key]
	if id < 1 || id > len(rules) {
		return fmt.Errorf("rule id %d out of range", id)
	}
	rule := rules[id-1]
	f.rules[key] = append(append([]string{}, rules[:id-1]...), rules[id:]...)
	f.ops = append(f.ops, fmt.Sprintf("delete-by-id %s/%s %d %s", table, chain, id, rule))
	return nil
}

func (f *fakeIPTables) DeleteChain(table, chain string) error {
	delete(f.rules, fakeKey(table, chain))
	f.ops = append(f.ops, fmt.Sprintf("delete-chain %s/%s", table, chain))
	return nil
}

func (f *fakeIPTables) DeleteIfExists(table, chain string, rulespec ...string) error {
	key := fakeKey(table, chain)
	targetRule := strings.Join(rulespec, " ")
	for i, rule := range f.rules[key] {
		if rule != targetRule {
			continue
		}
		f.rules[key] = append(append([]string{}, f.rules[key][:i]...), f.rules[key][i+1:]...)
		f.ops = append(f.ops, fmt.Sprintf("delete-if-exists %s/%s %s", table, chain, targetRule))
		return nil
	}
	return nil
}

func (f *fakeIPTables) Insert(table, chain string, pos int, rulespec ...string) error {
	key := fakeKey(table, chain)
	rules := f.rules[key]
	if pos < 1 {
		return fmt.Errorf("rule position %d out of range", pos)
	}
	if pos > len(rules)+1 {
		pos = len(rules) + 1
	}
	rule := strings.Join(rulespec, " ")
	idx := pos - 1
	f.rules[key] = append(append(append([]string{}, rules[:idx]...), rule), rules[idx:]...)
	f.ops = append(f.ops, fmt.Sprintf("insert %s/%s %d %s", table, chain, pos, rule))
	return nil
}

func (f *fakeIPTables) List(table, chain string) ([]string, error) {
	out := []string{"-P " + chain + " ACCEPT"}
	for _, rule := range f.rules[fakeKey(table, chain)] {
		out = append(out, "-A "+chain+" "+rule)
	}
	return out, nil
}

func (f *fakeIPTables) NewChain(table, chain string) error {
	f.rules[fakeKey(table, chain)] = []string{}
	f.ops = append(f.ops, fmt.Sprintf("new-chain %s/%s", table, chain))
	return nil
}

func (f *fakeIPTables) rulesFor(table, chain string) []string {
	return append([]string(nil), f.rules[fakeKey(table, chain)]...)
}

func fakeKey(table, chain string) string {
	return table + "/" + chain
}

func assertStringSlicesEqual(t *testing.T, got, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("unexpected strings:\ngot:  %#v\nwant: %#v", got, want)
	}
	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("unexpected strings:\ngot:  %#v\nwant: %#v", got, want)
		}
	}
}

func assertIntsEqual(t *testing.T, got, want []int) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("unexpected ints:\ngot:  %#v\nwant: %#v", got, want)
	}
	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("unexpected ints:\ngot:  %#v\nwant: %#v", got, want)
		}
	}
}
