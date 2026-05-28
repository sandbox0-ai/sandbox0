package proxy

import (
	"encoding/binary"
	"io"
	"net"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/netd/pkg/policy"
	"golang.org/x/net/dns/dnsmessage"
)

func TestDNSHostCacheStoresResponsePerSandboxAndTTL(t *testing.T) {
	now := time.Unix(100, 0)
	cache := newDNSHostCache()
	cache.now = func() time.Time { return now }

	cache.ObserveResponse("10.0.0.2", buildDNSAResponse(t, "GitHub.COM.", "140.82.112.4", 60))
	candidates := cache.Lookup("10.0.0.2", net.ParseIP("140.82.112.4"))
	if len(candidates) != 1 || candidates[0].Host != "github.com" {
		t.Fatalf("candidates = %#v, want github.com", candidates)
	}
	if got := cache.Lookup("10.0.0.3", net.ParseIP("140.82.112.4")); len(got) != 0 {
		t.Fatalf("other sandbox candidates = %#v, want none", got)
	}

	now = now.Add(61 * time.Second)
	if got := cache.Lookup("10.0.0.2", net.ParseIP("140.82.112.4")); len(got) != 0 {
		t.Fatalf("expired candidates = %#v, want none", got)
	}
}

func TestDNSHostCacheUsesQuestionNameThroughCNAME(t *testing.T) {
	cache := newDNSHostCache()
	cache.ObserveResponse("10.0.0.2", buildDNSCNAMEResponse(t, "github.com.", "github.map.fastly.net.", "140.82.112.4", 60))

	candidates := cache.Lookup("10.0.0.2", net.ParseIP("140.82.112.4"))
	if len(candidates) != 1 || candidates[0].Host != "github.com" {
		t.Fatalf("candidates = %#v, want original question host", candidates)
	}
}

func TestDNSHostCacheForgetSandbox(t *testing.T) {
	cache := newDNSHostCache()
	cache.ObserveResponse("10.0.0.2", buildDNSAResponse(t, "github.com.", "140.82.112.4", 60))
	cache.ForgetSandbox("10.0.0.2")

	if got := cache.Lookup("10.0.0.2", net.ParseIP("140.82.112.4")); len(got) != 0 {
		t.Fatalf("candidates = %#v, want none", got)
	}
}

func TestDNSTCPResponseObserverCachesCompleteFramesAcrossWrites(t *testing.T) {
	cache := newDNSHostCache()
	server := &Server{dnsCache: cache}
	observer := &dnsTCPResponseObserver{
		writer:    io.Discard,
		server:    server,
		sandboxIP: "10.0.0.2",
	}
	response := buildDNSAResponse(t, "github.com.", "140.82.112.4", 60)
	frame := make([]byte, 2+len(response))
	binary.BigEndian.PutUint16(frame[:2], uint16(len(response)))
	copy(frame[2:], response)

	if n, err := observer.Write(frame[:3]); err != nil || n != 3 {
		t.Fatalf("first write = (%d, %v), want 3 nil", n, err)
	}
	if got := cache.Lookup("10.0.0.2", net.ParseIP("140.82.112.4")); len(got) != 0 {
		t.Fatalf("partial frame candidates = %#v, want none", got)
	}
	if n, err := observer.Write(frame[3:]); err != nil || n != len(frame)-3 {
		t.Fatalf("second write = (%d, %v), want %d nil", n, err, len(frame)-3)
	}
	candidates := cache.Lookup("10.0.0.2", net.ParseIP("140.82.112.4"))
	if len(candidates) != 1 || candidates[0].Host != "github.com" {
		t.Fatalf("candidates = %#v, want github.com", candidates)
	}
}

func TestApplyCachedDNSHostMatchesSSHCredentialRule(t *testing.T) {
	cache := newDNSHostCache()
	cache.ObserveResponse("10.0.0.2", buildDNSAResponse(t, "github.com.", "140.82.112.4", 60))
	server := &Server{dnsCache: cache}
	compiled := &policy.CompiledPolicy{
		Mode: v1alpha1.NetworkModeAllowAll,
		Egress: policy.CompiledRuleSet{
			AuthRules: []policy.CompiledEgressAuthRule{{
				Name:     "git-ssh",
				AuthRef:  "git-cred",
				Protocol: v1alpha1.EgressAuthProtocolSSH,
				Domains:  []policy.DomainRule{{Pattern: "github.com", Type: policy.DomainMatchExact}},
			}},
		},
	}
	req := &adapterRequest{
		Compiled: compiled,
		SrcIP:    "10.0.0.2",
		DestIP:   net.ParseIP("140.82.112.4"),
		DestPort: 22,
	}
	classification := server.applyCachedDNSHost(req, classifyKnownTraffic("tcp", "ssh", req.DestIP, req.DestPort, ""))
	if classification.Host != "github.com" {
		t.Fatalf("host = %q, want github.com", classification.Host)
	}
	if classification.HostSource != classificationHostSourceDNSCache {
		t.Fatalf("host source = %q, want dns-cache", classification.HostSource)
	}
	if req.Host != "github.com" {
		t.Fatalf("request host = %q, want github.com", req.Host)
	}
	decision := decideTraffic(compiled, classification)
	if decision.MatchedAuthRule == nil || decision.MatchedAuthRule.AuthRef != "git-cred" {
		t.Fatalf("matched auth rule = %+v, want git-cred", decision.MatchedAuthRule)
	}
}

func TestApplyCachedDNSHostDoesNotUseOlderMatchingHostWhenLatestDoesNotMatch(t *testing.T) {
	now := time.Unix(100, 0)
	cache := newDNSHostCache()
	cache.now = func() time.Time { return now }
	cache.ObserveResponse("10.0.0.2", buildDNSAResponse(t, "github.com.", "140.82.112.4", 60))
	now = now.Add(time.Second)
	cache.ObserveResponse("10.0.0.2", buildDNSAResponse(t, "example.com.", "140.82.112.4", 60))
	server := &Server{dnsCache: cache}
	compiled := &policy.CompiledPolicy{
		Mode: v1alpha1.NetworkModeAllowAll,
		Egress: policy.CompiledRuleSet{
			AuthRules: []policy.CompiledEgressAuthRule{{
				Name:     "git-ssh",
				AuthRef:  "git-cred",
				Protocol: v1alpha1.EgressAuthProtocolSSH,
				Domains:  []policy.DomainRule{{Pattern: "github.com", Type: policy.DomainMatchExact}},
			}},
		},
	}
	req := &adapterRequest{
		Compiled: compiled,
		SrcIP:    "10.0.0.2",
		DestIP:   net.ParseIP("140.82.112.4"),
		DestPort: 22,
	}

	classification := server.applyCachedDNSHost(req, classifyKnownTraffic("tcp", "ssh", req.DestIP, req.DestPort, ""))
	if classification.Host != "" {
		t.Fatalf("host = %q, want empty because latest DNS host does not match policy", classification.Host)
	}
}

func buildDNSAResponse(t *testing.T, host string, ip string, ttl uint32) []byte {
	t.Helper()
	name := dnsmessage.MustNewName(host)
	parsed := net.ParseIP(ip).To4()
	if parsed == nil {
		t.Fatalf("invalid IPv4 address %q", ip)
	}
	builder := dnsmessage.NewBuilder(nil, dnsmessage.Header{Response: true, RCode: dnsmessage.RCodeSuccess})
	builder.EnableCompression()
	if err := builder.StartQuestions(); err != nil {
		t.Fatalf("start questions: %v", err)
	}
	if err := builder.Question(dnsmessage.Question{Name: name, Type: dnsmessage.TypeA, Class: dnsmessage.ClassINET}); err != nil {
		t.Fatalf("add question: %v", err)
	}
	if err := builder.StartAnswers(); err != nil {
		t.Fatalf("start answers: %v", err)
	}
	if err := builder.AResource(dnsmessage.ResourceHeader{Name: name, Type: dnsmessage.TypeA, Class: dnsmessage.ClassINET, TTL: ttl}, dnsmessage.AResource{A: [4]byte{parsed[0], parsed[1], parsed[2], parsed[3]}}); err != nil {
		t.Fatalf("add A record: %v", err)
	}
	out, err := builder.Finish()
	if err != nil {
		t.Fatalf("finish dns response: %v", err)
	}
	return out
}

func buildDNSCNAMEResponse(t *testing.T, host string, cname string, ip string, ttl uint32) []byte {
	t.Helper()
	name := dnsmessage.MustNewName(host)
	cnameName := dnsmessage.MustNewName(cname)
	parsed := net.ParseIP(ip).To4()
	if parsed == nil {
		t.Fatalf("invalid IPv4 address %q", ip)
	}
	builder := dnsmessage.NewBuilder(nil, dnsmessage.Header{Response: true, RCode: dnsmessage.RCodeSuccess})
	builder.EnableCompression()
	if err := builder.StartQuestions(); err != nil {
		t.Fatalf("start questions: %v", err)
	}
	if err := builder.Question(dnsmessage.Question{Name: name, Type: dnsmessage.TypeA, Class: dnsmessage.ClassINET}); err != nil {
		t.Fatalf("add question: %v", err)
	}
	if err := builder.StartAnswers(); err != nil {
		t.Fatalf("start answers: %v", err)
	}
	if err := builder.CNAMEResource(dnsmessage.ResourceHeader{Name: name, Type: dnsmessage.TypeCNAME, Class: dnsmessage.ClassINET, TTL: ttl}, dnsmessage.CNAMEResource{CNAME: cnameName}); err != nil {
		t.Fatalf("add CNAME record: %v", err)
	}
	if err := builder.AResource(dnsmessage.ResourceHeader{Name: cnameName, Type: dnsmessage.TypeA, Class: dnsmessage.ClassINET, TTL: ttl}, dnsmessage.AResource{A: [4]byte{parsed[0], parsed[1], parsed[2], parsed[3]}}); err != nil {
		t.Fatalf("add A record: %v", err)
	}
	out, err := builder.Finish()
	if err != nil {
		t.Fatalf("finish dns response: %v", err)
	}
	return out
}
