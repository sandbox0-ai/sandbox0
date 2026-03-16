package proxy

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/sandbox0-ai/sandbox0/netd/pkg/policy"
)

const (
	defaultHostVerifyTimeout  = 2 * time.Second
	defaultHostVerifyCacheTTL = 30 * time.Second
)

type hostVerifier interface {
	Verify(host string, destIP net.IP) (bool, error)
}

type hostResolutionCacheEntry struct {
	ips       []net.IP
	expiresAt time.Time
}

type dnsHostVerifier struct {
	resolver *net.Resolver
	timeout  time.Duration
	ttl      time.Duration
	now      func() time.Time

	mu    sync.Mutex
	cache map[string]hostResolutionCacheEntry
}

func newDNSHostVerifier() *dnsHostVerifier {
	return &dnsHostVerifier{
		resolver: net.DefaultResolver,
		timeout:  defaultHostVerifyTimeout,
		ttl:      defaultHostVerifyCacheTTL,
		now:      time.Now,
		cache:    map[string]hostResolutionCacheEntry{},
	}
}

func (v *dnsHostVerifier) Verify(host string, destIP net.IP) (bool, error) {
	host = normalizeHost(host)
	if host == "" || destIP == nil {
		return false, fmt.Errorf("host verifier requires host and destination")
	}
	if literal := net.ParseIP(host); literal != nil {
		return literal.Equal(destIP), nil
	}
	ips, err := v.lookup(host)
	if err != nil {
		return false, err
	}
	for _, candidate := range ips {
		if candidate.Equal(destIP) {
			return true, nil
		}
	}
	return false, nil
}

func (v *dnsHostVerifier) lookup(host string) ([]net.IP, error) {
	if v == nil {
		return nil, fmt.Errorf("host verifier is nil")
	}
	if ips, ok := v.lookupCached(host); ok {
		return ips, nil
	}
	timeout := v.timeout
	if timeout <= 0 {
		timeout = defaultHostVerifyTimeout
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	addrs, err := v.resolver.LookupIPAddr(ctx, host)
	if err != nil {
		return nil, err
	}
	ips := make([]net.IP, 0, len(addrs))
	for _, addr := range addrs {
		if addr.IP == nil {
			continue
		}
		ips = append(ips, append(net.IP(nil), addr.IP...))
	}
	if len(ips) == 0 {
		return nil, fmt.Errorf("host %q resolved without ip addresses", host)
	}
	v.store(host, ips)
	return ips, nil
}

func (v *dnsHostVerifier) lookupCached(host string) ([]net.IP, bool) {
	if v == nil {
		return nil, false
	}
	now := time.Now()
	if v.now != nil {
		now = v.now()
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	entry, ok := v.cache[host]
	if !ok || !entry.expiresAt.After(now) {
		if ok {
			delete(v.cache, host)
		}
		return nil, false
	}
	ips := make([]net.IP, 0, len(entry.ips))
	for _, ip := range entry.ips {
		ips = append(ips, append(net.IP(nil), ip...))
	}
	return ips, true
}

func (v *dnsHostVerifier) store(host string, ips []net.IP) {
	if v == nil {
		return
	}
	now := time.Now()
	if v.now != nil {
		now = v.now()
	}
	ttl := v.ttl
	if ttl <= 0 {
		ttl = defaultHostVerifyCacheTTL
	}
	copied := make([]net.IP, 0, len(ips))
	for _, ip := range ips {
		copied = append(copied, append(net.IP(nil), ip...))
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	v.cache[host] = hostResolutionCacheEntry{
		ips:       copied,
		expiresAt: now.Add(ttl),
	}
}

func verifyClassifiedHost(verifier hostVerifier, compiled *policy.CompiledPolicy, classification trafficClassification) trafficClassification {
	if verifier == nil || compiled == nil || !policy.HasDomainRules(compiled) {
		return classification
	}
	if classification.UnknownReason != "" || classification.Host == "" || classification.DestIP == nil {
		return classification
	}
	ok, err := verifier.Verify(classification.Host, classification.DestIP)
	if err != nil {
		classification.Verification = "host_resolution_failed"
		return classification
	}
	if !ok {
		classification.Verification = "host_dest_mismatch"
	}
	return classification
}
