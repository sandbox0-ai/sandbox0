package proxy

import (
	"fmt"
	"net"
	"testing"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/netd/pkg/policy"
)

type stubHostVerifier struct {
	ok  bool
	err error
}

func (v stubHostVerifier) Verify(_ string, _ net.IP) (bool, error) {
	return v.ok, v.err
}

func TestVerifyClassifiedHostKeepsKnownTrafficWhenVerified(t *testing.T) {
	compiled := &policy.CompiledPolicy{
		Mode: v1alpha1.NetworkModeAllowAll,
		Egress: policy.CompiledRuleSet{
			DeniedDomains: []policy.DomainRule{{Pattern: "blocked.example.com", Type: policy.DomainMatchExact}},
		},
	}
	classification := classifyKnownTraffic("tcp", "http", net.ParseIP("8.8.8.8"), 443, "example.com")
	classification = verifyClassifiedHost(stubHostVerifier{ok: true}, compiled, classification)
	if classification.Verification != "" {
		t.Fatalf("expected verified classification, got verification %q", classification.Verification)
	}
}

func TestVerifyClassifiedHostMarksMismatchAsUnknown(t *testing.T) {
	compiled := &policy.CompiledPolicy{
		Mode: v1alpha1.NetworkModeAllowAll,
		Egress: policy.CompiledRuleSet{
			DeniedDomains: []policy.DomainRule{{Pattern: "blocked.example.com", Type: policy.DomainMatchExact}},
		},
	}
	classification := classifyKnownTraffic("tcp", "http", net.ParseIP("8.8.8.8"), 443, "example.com")
	classification = verifyClassifiedHost(stubHostVerifier{}, compiled, classification)
	if classification.Verification != "host_dest_mismatch" {
		t.Fatalf("unexpected verification reason %q", classification.Verification)
	}
}

func TestVerifyClassifiedHostMarksLookupFailureAsUnknown(t *testing.T) {
	compiled := &policy.CompiledPolicy{
		Mode: v1alpha1.NetworkModeAllowAll,
		Egress: policy.CompiledRuleSet{
			DeniedDomains: []policy.DomainRule{{Pattern: "blocked.example.com", Type: policy.DomainMatchExact}},
		},
	}
	classification := classifyKnownTraffic("tcp", "http", net.ParseIP("8.8.8.8"), 443, "example.com")
	classification = verifyClassifiedHost(stubHostVerifier{err: fmt.Errorf("lookup failed")}, compiled, classification)
	if classification.Verification != "host_resolution_failed" {
		t.Fatalf("unexpected verification reason %q", classification.Verification)
	}
}

func TestVerifyClassifiedHostMismatchFailsClosedUnderBlockAll(t *testing.T) {
	compiled := &policy.CompiledPolicy{
		Mode: v1alpha1.NetworkModeBlockAll,
		Egress: policy.CompiledRuleSet{
			AllowedCIDRs:   []*net.IPNet{mustCIDR("8.8.8.0/24")},
			AllowedPorts:   []policy.PortRange{{Protocol: "tcp", Start: 443, End: 443}},
			AllowedDomains: []policy.DomainRule{{Pattern: "example.com", Type: policy.DomainMatchExact}},
		},
	}
	classification := classifyKnownTraffic("tcp", "http", net.ParseIP("8.8.8.8"), 443, "example.com")
	classification = verifyClassifiedHost(stubHostVerifier{}, compiled, classification)
	decision := decideTraffic(compiled, classification)
	if decision.Action != decisionActionDeny {
		t.Fatalf("expected deny, got %s", decision.Action)
	}
	if decision.ClassifierResult != "known" {
		t.Fatalf("expected known classifier result, got %s", decision.ClassifierResult)
	}
	if decision.Reason != "host_dest_mismatch" {
		t.Fatalf("expected host_dest_mismatch reason, got %s", decision.Reason)
	}
}

func TestVerifyClassifiedHostMismatchStillHonorsDeniedDomainUnderAllowAll(t *testing.T) {
	compiled := &policy.CompiledPolicy{
		Mode: v1alpha1.NetworkModeAllowAll,
		Egress: policy.CompiledRuleSet{
			DeniedDomains: []policy.DomainRule{{Pattern: "blocked.example.com", Type: policy.DomainMatchExact}},
		},
	}
	classification := classifyKnownTraffic("tcp", "http", net.ParseIP("8.8.8.8"), 443, "blocked.example.com")
	classification = verifyClassifiedHost(stubHostVerifier{}, compiled, classification)
	decision := decideTraffic(compiled, classification)
	if decision.Action != decisionActionDeny {
		t.Fatalf("expected deny, got %s", decision.Action)
	}
	if decision.Reason != "l7_denied" {
		t.Fatalf("expected l7_denied reason, got %s", decision.Reason)
	}
}

func TestVerifyClassifiedHostMismatchFallsBackUnderAllowAllForNonDeniedDomain(t *testing.T) {
	compiled := &policy.CompiledPolicy{
		Mode: v1alpha1.NetworkModeAllowAll,
		Egress: policy.CompiledRuleSet{
			DeniedDomains: []policy.DomainRule{{Pattern: "blocked.example.com", Type: policy.DomainMatchExact}},
		},
	}
	classification := classifyKnownTraffic("tcp", "http", net.ParseIP("8.8.8.8"), 443, "example.com")
	classification = verifyClassifiedHost(stubHostVerifier{}, compiled, classification)
	decision := decideTraffic(compiled, classification)
	if decision.Action != decisionActionPassThrough {
		t.Fatalf("expected pass-through, got %s", decision.Action)
	}
	if decision.Reason != "host_dest_mismatch" {
		t.Fatalf("expected host_dest_mismatch reason, got %s", decision.Reason)
	}
}

func TestDNSHostVerifierAcceptsLiteralIPHost(t *testing.T) {
	verifier := newDNSHostVerifier()
	ok, err := verifier.Verify("8.8.8.8", net.ParseIP("8.8.8.8"))
	if err != nil {
		t.Fatalf("Verify returned error: %v", err)
	}
	if !ok {
		t.Fatalf("expected literal ip host to verify")
	}
}
