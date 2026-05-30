package proxy

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/netd/pkg/policy"
	"golang.org/x/net/dns/dnsmessage"
)

const (
	defaultDNSHostCacheMinTTL = 5 * time.Second
	defaultDNSHostCacheMaxTTL = 5 * time.Minute
	maxDNSHostCacheEntries    = 4096

	classificationHostSourceDNSCache = "dns-cache"
)

type dnsHostCache struct {
	mu     sync.Mutex
	now    func() time.Time
	minTTL time.Duration
	maxTTL time.Duration

	bySandbox map[string]map[string]map[string]dnsHostCacheEntry
}

type dnsHostCacheEntry struct {
	host       string
	ip         string
	expiresAt  time.Time
	observedAt time.Time
}

type dnsHostCandidate struct {
	Host       string
	ObservedAt time.Time
}

type dnsHostRecord struct {
	host string
	ip   net.IP
	ttl  uint32
}

type dnsAddressRecord struct {
	ip  net.IP
	ttl uint32
}

type dnsCNAMERecord struct {
	target string
	ttl    uint32
}

func newDNSHostCache() *dnsHostCache {
	return &dnsHostCache{
		now:       time.Now,
		minTTL:    defaultDNSHostCacheMinTTL,
		maxTTL:    defaultDNSHostCacheMaxTTL,
		bySandbox: make(map[string]map[string]map[string]dnsHostCacheEntry),
	}
}

func (c *dnsHostCache) ObserveResponse(sandboxIP string, payload []byte) {
	sandboxIP = strings.TrimSpace(sandboxIP)
	if c == nil || sandboxIP == "" || len(payload) == 0 {
		return
	}
	records, err := parseDNSHostRecords(payload)
	if err != nil || len(records) == 0 {
		return
	}
	now := c.currentTime()
	c.mu.Lock()
	defer c.mu.Unlock()
	sandboxEntries := c.bySandbox[sandboxIP]
	if sandboxEntries == nil {
		sandboxEntries = make(map[string]map[string]dnsHostCacheEntry)
		c.bySandbox[sandboxIP] = sandboxEntries
	}
	for _, record := range records {
		host := normalizeDNSHostname(record.host)
		if host == "" || record.ip == nil {
			continue
		}
		ip := record.ip.String()
		if ip == "" {
			continue
		}
		hostEntries := sandboxEntries[ip]
		if hostEntries == nil {
			hostEntries = make(map[string]dnsHostCacheEntry)
			sandboxEntries[ip] = hostEntries
		}
		hostEntries[host] = dnsHostCacheEntry{
			host:       host,
			ip:         ip,
			expiresAt:  now.Add(c.clampTTL(record.ttl)),
			observedAt: now,
		}
	}
	c.pruneLocked(sandboxIP, now)
}

func (c *dnsHostCache) Lookup(sandboxIP string, destIP net.IP) []dnsHostCandidate {
	sandboxIP = strings.TrimSpace(sandboxIP)
	if c == nil || sandboxIP == "" || destIP == nil {
		return nil
	}
	ip := destIP.String()
	if ip == "" {
		return nil
	}
	now := c.currentTime()
	c.mu.Lock()
	defer c.mu.Unlock()
	sandboxEntries := c.bySandbox[sandboxIP]
	if len(sandboxEntries) == 0 {
		return nil
	}
	hostEntries := sandboxEntries[ip]
	if len(hostEntries) == 0 {
		return nil
	}
	candidates := make([]dnsHostCandidate, 0, len(hostEntries))
	for host, entry := range hostEntries {
		if !entry.expiresAt.After(now) {
			delete(hostEntries, host)
			continue
		}
		candidates = append(candidates, dnsHostCandidate{
			Host:       entry.host,
			ObservedAt: entry.observedAt,
		})
	}
	if len(hostEntries) == 0 {
		delete(sandboxEntries, ip)
	}
	if len(sandboxEntries) == 0 {
		delete(c.bySandbox, sandboxIP)
	}
	sort.Slice(candidates, func(i, j int) bool {
		if !candidates[i].ObservedAt.Equal(candidates[j].ObservedAt) {
			return candidates[i].ObservedAt.After(candidates[j].ObservedAt)
		}
		return candidates[i].Host < candidates[j].Host
	})
	return candidates
}

func (c *dnsHostCache) ForgetSandbox(sandboxIP string) {
	if c == nil {
		return
	}
	sandboxIP = strings.TrimSpace(sandboxIP)
	if sandboxIP == "" {
		return
	}
	c.mu.Lock()
	delete(c.bySandbox, sandboxIP)
	c.mu.Unlock()
}

func (c *dnsHostCache) currentTime() time.Time {
	if c == nil || c.now == nil {
		return time.Now()
	}
	return c.now()
}

func (c *dnsHostCache) clampTTL(ttl uint32) time.Duration {
	if c == nil {
		return defaultDNSHostCacheMinTTL
	}
	value := time.Duration(ttl) * time.Second
	minTTL := c.minTTL
	if minTTL <= 0 {
		minTTL = defaultDNSHostCacheMinTTL
	}
	maxTTL := c.maxTTL
	if maxTTL <= 0 {
		maxTTL = defaultDNSHostCacheMaxTTL
	}
	if value < minTTL {
		return minTTL
	}
	if value > maxTTL {
		return maxTTL
	}
	return value
}

func (c *dnsHostCache) pruneLocked(sandboxIP string, now time.Time) {
	sandboxEntries := c.bySandbox[sandboxIP]
	if len(sandboxEntries) == 0 {
		return
	}
	entries := make([]dnsHostCacheEntry, 0)
	for ip, hostEntries := range sandboxEntries {
		for host, entry := range hostEntries {
			if !entry.expiresAt.After(now) {
				delete(hostEntries, host)
				continue
			}
			entries = append(entries, entry)
		}
		if len(hostEntries) == 0 {
			delete(sandboxEntries, ip)
		}
	}
	if len(sandboxEntries) == 0 {
		delete(c.bySandbox, sandboxIP)
		return
	}
	if len(entries) <= maxDNSHostCacheEntries {
		return
	}
	sort.Slice(entries, func(i, j int) bool {
		if !entries[i].observedAt.Equal(entries[j].observedAt) {
			return entries[i].observedAt.Before(entries[j].observedAt)
		}
		if entries[i].ip != entries[j].ip {
			return entries[i].ip < entries[j].ip
		}
		return entries[i].host < entries[j].host
	})
	for _, entry := range entries[:len(entries)-maxDNSHostCacheEntries] {
		if hostEntries := sandboxEntries[entry.ip]; hostEntries != nil {
			delete(hostEntries, entry.host)
			if len(hostEntries) == 0 {
				delete(sandboxEntries, entry.ip)
			}
		}
	}
	if len(sandboxEntries) == 0 {
		delete(c.bySandbox, sandboxIP)
	}
}

func parseDNSHostRecords(payload []byte) ([]dnsHostRecord, error) {
	var parser dnsmessage.Parser
	header, err := parser.Start(payload)
	if err != nil {
		return nil, err
	}
	if !header.Response || header.RCode != dnsmessage.RCodeSuccess {
		return nil, nil
	}
	questions, err := parser.AllQuestions()
	if err != nil {
		return nil, err
	}
	if len(questions) == 0 {
		return nil, nil
	}
	queryHosts := make([]string, 0, len(questions))
	seenQueries := make(map[string]struct{}, len(questions))
	for _, question := range questions {
		host := normalizeDNSName(question.Name)
		if host == "" {
			continue
		}
		if _, ok := seenQueries[host]; ok {
			continue
		}
		seenQueries[host] = struct{}{}
		queryHosts = append(queryHosts, host)
	}
	if len(queryHosts) == 0 {
		return nil, nil
	}

	addresses := map[string][]dnsAddressRecord{}
	cnames := map[string][]dnsCNAMERecord{}
	for {
		header, err := parser.AnswerHeader()
		if err == dnsmessage.ErrSectionDone {
			break
		}
		if err != nil {
			return nil, err
		}
		owner := normalizeDNSName(header.Name)
		switch header.Type {
		case dnsmessage.TypeA:
			resource, err := parser.AResource()
			if err != nil {
				return nil, err
			}
			if owner != "" {
				addresses[owner] = append(addresses[owner], dnsAddressRecord{
					ip:  net.IPv4(resource.A[0], resource.A[1], resource.A[2], resource.A[3]),
					ttl: header.TTL,
				})
			}
		case dnsmessage.TypeAAAA:
			resource, err := parser.AAAAResource()
			if err != nil {
				return nil, err
			}
			if owner != "" {
				ip := make(net.IP, net.IPv6len)
				copy(ip, resource.AAAA[:])
				addresses[owner] = append(addresses[owner], dnsAddressRecord{
					ip:  ip,
					ttl: header.TTL,
				})
			}
		case dnsmessage.TypeCNAME:
			resource, err := parser.CNAMEResource()
			if err != nil {
				return nil, err
			}
			target := normalizeDNSName(resource.CNAME)
			if owner != "" && target != "" {
				cnames[owner] = append(cnames[owner], dnsCNAMERecord{
					target: target,
					ttl:    header.TTL,
				})
			}
		default:
			if err := parser.SkipAnswer(); err != nil {
				return nil, err
			}
		}
	}

	records := make([]dnsHostRecord, 0)
	for _, queryHost := range queryHosts {
		records = append(records, resolveDNSHostRecords(queryHost, addresses, cnames)...)
	}
	return records, nil
}

func resolveDNSHostRecords(queryHost string, addresses map[string][]dnsAddressRecord, cnames map[string][]dnsCNAMERecord) []dnsHostRecord {
	type pendingName struct {
		name string
		ttl  uint32
	}
	out := make([]dnsHostRecord, 0)
	queue := []pendingName{{name: queryHost, ttl: math.MaxUint32}}
	visited := map[string]struct{}{}
	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]
		if current.name == "" {
			continue
		}
		if _, ok := visited[current.name]; ok {
			continue
		}
		visited[current.name] = struct{}{}
		for _, address := range addresses[current.name] {
			ttl := minDNSRecordTTL(current.ttl, address.ttl)
			out = append(out, dnsHostRecord{
				host: queryHost,
				ip:   address.ip,
				ttl:  ttl,
			})
		}
		for _, cname := range cnames[current.name] {
			queue = append(queue, pendingName{
				name: cname.target,
				ttl:  minDNSRecordTTL(current.ttl, cname.ttl),
			})
		}
	}
	return out
}

func minDNSRecordTTL(a, b uint32) uint32 {
	if a < b {
		return a
	}
	return b
}

func normalizeDNSName(name dnsmessage.Name) string {
	return normalizeDNSHostname(name.String())
}

func normalizeDNSHostname(host string) string {
	host = strings.TrimSuffix(strings.TrimSpace(host), ".")
	return normalizeHost(host)
}

func (s *Server) observeDNSResponse(sandboxIP string, payload []byte) {
	if s == nil || s.dnsCache == nil {
		return
	}
	s.dnsCache.ObserveResponse(sandboxIP, payload)
}

func (s *Server) forgetDNSHostCache(sandboxIP string) {
	if s == nil || s.dnsCache == nil {
		return
	}
	s.dnsCache.ForgetSandbox(sandboxIP)
}

// ForgetSandboxDNS removes DNS host associations for a sandbox pod IP.
func (s *Server) ForgetSandboxDNS(sandboxIP string) {
	s.forgetDNSHostCache(sandboxIP)
}

func (s *Server) applyCachedDNSHost(req *adapterRequest, classification trafficClassification) trafficClassification {
	if s == nil || s.dnsCache == nil || req == nil {
		return classification
	}
	if classification.Transport != "tcp" || classification.Protocol != "ssh" || classification.Host != "" || classification.UnknownReason != "" {
		return classification
	}
	candidates := s.dnsCache.Lookup(req.SrcIP, classification.DestIP)
	if len(candidates) == 0 {
		return classification
	}
	host := chooseCachedSSHHost(req, classification, candidates)
	if host == "" {
		return classification
	}
	classification.Host = host
	classification.HostSource = classificationHostSourceDNSCache
	req.Host = host
	return classification
}

func chooseCachedSSHHost(req *adapterRequest, classification trafficClassification, candidates []dnsHostCandidate) string {
	if len(candidates) == 0 {
		return ""
	}
	latest := strings.TrimSpace(candidates[0].Host)
	if latest == "" {
		return ""
	}
	if len(candidates) == 1 {
		return latest
	}
	if policyAllowsCachedSSHHost(req, classification, latest) {
		return latest
	}
	return ""
}

func policyAllowsCachedSSHHost(req *adapterRequest, classification trafficClassification, host string) bool {
	if req == nil {
		return false
	}
	if policy.MatchEgressAuthRule(req.Compiled, classification.Transport, classification.Protocol, classification.DestPort, host) != nil {
		return true
	}
	if hasDomainScopedSSHAuthRule(req.Compiled) {
		return false
	}
	if !policy.AllowEgressDestination(req.Compiled, classification.DestIP, classification.DestPort, classification.Transport, host, classification.Protocol) {
		return false
	}
	if policy.HasDomainRules(req.Compiled) && !policy.AllowEgressDomain(req.Compiled, host) {
		return false
	}
	return true
}

func hasDomainScopedSSHAuthRule(compiled *policy.CompiledPolicy) bool {
	if compiled == nil {
		return false
	}
	for _, rule := range compiled.Egress.AuthRules {
		if rule.Rollout == v1alpha1.EgressAuthRolloutDisabled || len(rule.Domains) == 0 {
			continue
		}
		if rule.Protocol == "" || rule.Protocol == v1alpha1.EgressAuthProtocolSSH {
			return true
		}
	}
	return false
}

type dnsTCPResponseObserver struct {
	writer    io.Writer
	server    *Server
	sandboxIP string
	buffer    []byte
}

func (w *dnsTCPResponseObserver) Write(p []byte) (int, error) {
	n, err := w.writer.Write(p)
	if n > 0 {
		w.observe(p[:n])
	}
	return n, err
}

func (w *dnsTCPResponseObserver) observe(data []byte) {
	if w == nil || w.server == nil || len(data) == 0 {
		return
	}
	w.buffer = append(w.buffer, data...)
	for {
		if len(w.buffer) < 2 {
			return
		}
		messageLength := int(binary.BigEndian.Uint16(w.buffer[:2]))
		if messageLength <= 0 {
			w.buffer = w.buffer[2:]
			continue
		}
		if messageLength > math.MaxUint16 {
			w.buffer = nil
			return
		}
		if len(w.buffer) < 2+messageLength {
			if len(w.buffer) > 2+math.MaxUint16 {
				w.buffer = nil
			}
			return
		}
		message := append([]byte(nil), w.buffer[2:2+messageLength]...)
		w.server.observeDNSResponse(w.sandboxIP, message)
		w.buffer = w.buffer[2+messageLength:]
	}
}

func (s *Server) pipeDNSOverTCP(client net.Conn, upstream net.Conn, upstreamWriter io.Reader, downstreamWriter io.Reader, compiled *policy.CompiledPolicy, audit *flowAudit, sandboxIP string) error {
	upstreamCounter := &countingWriter{writer: s.bandwidthLimitedWriter(upstream, compiled, bandwidthEgress)}
	clientCounter := &countingWriter{writer: s.bandwidthLimitedWriter(client, compiled, bandwidthIngress)}
	dnsObserver := &dnsTCPResponseObserver{
		writer:    clientCounter,
		server:    s,
		sandboxIP: sandboxIP,
	}
	errCh := make(chan error, 2)
	go func() {
		n, err := io.Copy(upstreamCounter, upstreamWriter)
		s.recordEgressBytes(compiled, n, audit)
		closeConnWrite(upstream)
		errCh <- err
	}()
	go func() {
		n, err := io.Copy(dnsObserver, downstreamWriter)
		s.recordIngressBytes(compiled, n, audit)
		closeConnWrite(client)
		errCh <- err
	}()
	errs := make([]error, 0, 2)
	for i := 0; i < 2; i++ {
		if err := normalizeRelayError(<-errCh); err != nil {
			errs = append(errs, err)
		}
	}
	return errorsJoin(errs...)
}

func errorsJoin(errs ...error) error {
	if len(errs) == 1 {
		return errs[0]
	}
	return errors.Join(errs...)
}

func (s *Server) proxyDNSTCP(req *adapterRequest) error {
	if s == nil || req == nil || req.Conn == nil {
		return fmt.Errorf("dns tcp proxy requires connection")
	}
	if req.DestIP == nil || req.DestPort <= 0 {
		return fmt.Errorf("missing destination")
	}
	s.recordFlow(req.SrcIP, req.DestIP, req.DestPort, "tcp", remotePort(req.Conn.RemoteAddr()))
	upstream := req.UpstreamConn
	var err error
	if upstream == nil {
		upstream, err = s.dialTCPUpstreamForRequest(req)
		if err != nil {
			return err
		}
	}
	upstream = &countingConn{Conn: upstream}
	defer upstream.Close()

	reader := io.Reader(req.Conn)
	if req.Prefix != nil {
		reader = io.MultiReader(req.Prefix, req.Conn)
	}
	upstreamReader := io.Reader(upstream)
	if req.UpstreamPrefix != nil {
		upstreamReader = io.MultiReader(req.UpstreamPrefix, upstream)
	}
	return s.pipeDNSOverTCP(req.Conn, upstream, reader, upstreamReader, req.Compiled, req.Audit, req.SrcIP)
}
