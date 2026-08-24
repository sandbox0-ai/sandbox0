package policy

import (
	"net"
	"sync"
	"time"

	"github.com/sandbox0-ai/sandbox0/ctld/internal/ctld/networking/model"
	v1alpha1 "github.com/sandbox0-ai/sandbox0/pkg/sandboxspec"
	"go.uber.org/zap"
)

type Store struct {
	logger     *zap.Logger
	mu         sync.RWMutex
	byIP       map[string]*policyEntry
	byKey      map[string]*policyEntry
	platformMu sync.RWMutex
	platform   *PlatformPolicy
}

type policyEntry struct {
	compiled   *CompiledPolicy
	policyHash string
	sourceIP   string
	updatedAt  time.Time
}

type SandboxPolicyChange struct {
	Scope       string
	Name        string
	SourceIP    string
	OldSourceIP string
	PolicyHash  string
	PrevHash    string
	Initial     bool
}

type SandboxPolicyReconcileResult struct {
	Upserted   int
	RemovedIPs []string
	Changed    []SandboxPolicyChange
}

func NewStore(logger *zap.Logger) *Store {
	if logger == nil {
		logger = zap.NewNop()
	}
	return &Store{
		logger: logger,
		byIP:   make(map[string]*policyEntry),
		byKey:  make(map[string]*policyEntry),
	}
}

func (s *Store) ReconcileSandboxes(sandboxes []*model.SandboxInfo) SandboxPolicyReconcileResult {
	result := SandboxPolicyReconcileResult{}
	desired := make(map[string]*policyEntry, len(sandboxes))
	desiredInfo := make(map[string]*model.SandboxInfo, len(sandboxes))
	currentKeys := make(map[string]struct{}, len(sandboxes))
	now := time.Now()
	for _, info := range sandboxes {
		if info == nil || info.SourceIP == "" {
			continue
		}
		key := info.Scope + "/" + info.Name
		currentKeys[key] = struct{}{}
		spec, err := v1alpha1.ParseNetworkPolicyFromAnnotation(info.NetworkPolicy)
		if err != nil {
			s.logger.Warn("Failed to parse network policy", zap.Error(err), zap.String("source_ip", info.SourceIP))
			continue
		}
		compiled, err := CompileNetworkPolicy(spec)
		if err != nil {
			s.logger.Warn("Failed to compile network policy", zap.Error(err), zap.String("source_ip", info.SourceIP))
			continue
		}
		applySandboxOwner(compiled, info)
		desired[key] = &policyEntry{
			compiled:   compiled,
			policyHash: info.NetworkPolicyHash,
			sourceIP:   info.SourceIP,
			updatedAt:  now,
		}
		desiredInfo[key] = info
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	for key, existing := range s.byKey {
		if _, ok := currentKeys[key]; ok {
			continue
		}
		delete(s.byKey, key)
		if existing != nil && existing.sourceIP != "" {
			delete(s.byIP, existing.sourceIP)
			result.RemovedIPs = append(result.RemovedIPs, existing.sourceIP)
		}
	}
	for key, entry := range desired {
		info := desiredInfo[key]
		existing := s.byKey[key]
		change := SandboxPolicyChange{
			Scope:      info.Scope,
			Name:       info.Name,
			SourceIP:   entry.sourceIP,
			PolicyHash: entry.policyHash,
			Initial:    existing == nil,
		}
		if existing != nil {
			change.PrevHash = existing.policyHash
			change.OldSourceIP = existing.sourceIP
		}
		if existing == nil || existing.policyHash != entry.policyHash || existing.sourceIP != entry.sourceIP {
			result.Changed = append(result.Changed, change)
		}
		if existing != nil && existing.sourceIP != "" && existing.sourceIP != entry.sourceIP {
			delete(s.byIP, existing.sourceIP)
			result.RemovedIPs = append(result.RemovedIPs, existing.sourceIP)
		}
		s.byKey[key] = entry
		s.byIP[entry.sourceIP] = entry
		result.Upserted++
	}
	if len(result.RemovedIPs) > 0 || len(result.Changed) > 0 {
		s.logger.Info(
			"Sandbox network policies reconciled",
			zap.Int("upserted", result.Upserted),
			zap.Int("changed", len(result.Changed)),
			zap.Int("removed_ips", len(result.RemovedIPs)),
		)
	}
	return result
}

func applySandboxOwner(compiled *CompiledPolicy, info *model.SandboxInfo) {
	if compiled == nil || info == nil {
		return
	}
	compiled.OwnerKind = info.OwnerKind
}

func (s *Store) DeleteByKey(namespace, name string) {
	key := namespace + "/" + name
	s.mu.Lock()
	entry := s.byKey[key]
	delete(s.byKey, key)
	if entry != nil && entry.sourceIP != "" {
		delete(s.byIP, entry.sourceIP)
	}
	sourceIP := ""
	if entry != nil {
		sourceIP = entry.sourceIP
	}
	s.logger.Info(
		"Sandbox network policy deleted by key",
		zap.String("sandbox", key),
		zap.String("source_ip", sourceIP),
	)
	s.mu.Unlock()
}

func (s *Store) GetByIP(sourceIP string) *CompiledPolicy {
	s.mu.RLock()
	defer s.mu.RUnlock()
	entry := s.byIP[sourceIP]
	if entry == nil || entry.compiled == nil {
		return nil
	}
	clone := *entry.compiled
	clone.Egress = cloneRuleSet(entry.compiled.Egress)
	clone.Platform = clonePlatformPolicy(s.getPlatformPolicy(), sourceIP)
	return &clone
}

func (s *Store) SetPlatformPolicy(policy *PlatformPolicy) {
	s.platformMu.Lock()
	s.platform = policy
	s.platformMu.Unlock()
}

func (s *Store) AllowedPlatformCIDRs() []string {
	s.platformMu.RLock()
	defer s.platformMu.RUnlock()
	if s.platform == nil || len(s.platform.AllowedCIDRs) == 0 {
		return nil
	}
	out := make([]string, 0, len(s.platform.AllowedCIDRs))
	for _, cidr := range s.platform.AllowedCIDRs {
		if cidr == nil {
			continue
		}
		out = append(out, cidr.String())
	}
	return out
}

func (s *Store) getPlatformPolicy() *PlatformPolicy {
	s.platformMu.RLock()
	defer s.platformMu.RUnlock()
	return s.platform
}

func cloneRuleSet(in CompiledRuleSet) CompiledRuleSet {
	out := in
	out.AllowedCIDRs = append([]*net.IPNet(nil), in.AllowedCIDRs...)
	out.DeniedCIDRs = append([]*net.IPNet(nil), in.DeniedCIDRs...)
	out.AllowedPorts = append([]PortRange(nil), in.AllowedPorts...)
	out.DeniedPorts = append([]PortRange(nil), in.DeniedPorts...)
	out.AllowedDomains = append([]DomainRule(nil), in.AllowedDomains...)
	out.DeniedDomains = append([]DomainRule(nil), in.DeniedDomains...)
	out.AuthRules = append([]CompiledEgressAuthRule(nil), in.AuthRules...)
	for i := range out.AuthRules {
		out.AuthRules[i].HTTPMatch = cloneCompiledHTTPMatch(in.AuthRules[i].HTTPMatch)
	}
	return out
}

func cloneCompiledHTTPMatch(in *CompiledHTTPMatch) *CompiledHTTPMatch {
	if in == nil {
		return nil
	}
	return &CompiledHTTPMatch{
		Methods:      append([]string(nil), in.Methods...),
		Paths:        append([]string(nil), in.Paths...),
		PathPrefixes: append([]string(nil), in.PathPrefixes...),
		Query:        append([]CompiledHTTPValueMatch(nil), in.Query...),
		Headers:      append([]CompiledHTTPValueMatch(nil), in.Headers...),
	}
}

func clonePlatformPolicy(in *PlatformPolicy, sourceIP string) *PlatformPolicy {
	if in == nil {
		return nil
	}
	out := &PlatformPolicy{
		AllowedCIDRs:   append([]*net.IPNet(nil), in.AllowedCIDRs...),
		DeniedCIDRs:    append([]*net.IPNet(nil), in.DeniedCIDRs...),
		AllowedDomains: append([]DomainRule(nil), in.AllowedDomains...),
		DeniedDomains:  append([]DomainRule(nil), in.DeniedDomains...),
		SourceIP:       sourceIP,
	}
	if len(in.SandboxIPs) > 0 {
		out.SandboxIPs = make(map[string]struct{}, len(in.SandboxIPs))
		for ip := range in.SandboxIPs {
			out.SandboxIPs[ip] = struct{}{}
		}
	}
	return out
}
