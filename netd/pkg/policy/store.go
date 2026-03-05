package policy

import (
	"net"
	"sync"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/netd/pkg/watcher"
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
	podIP      string
	updatedAt  time.Time
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

func (s *Store) UpsertFromSandbox(info *watcher.SandboxInfo) (bool, string) {
	if info == nil || info.PodIP == "" {
		return false, ""
	}
	spec, err := v1alpha1.ParseNetworkPolicyFromAnnotation(info.NetworkPolicy)
	if err != nil {
		s.logger.Warn("Failed to parse network policy", zap.Error(err), zap.String("pod_ip", info.PodIP))
		return false, ""
	}
	compiled, err := CompileNetworkPolicy(spec)
	if err != nil {
		s.logger.Warn("Failed to compile network policy", zap.Error(err), zap.String("pod_ip", info.PodIP))
		return false, ""
	}
	key := info.Namespace + "/" + info.Name
	changed := false
	prevHash := ""
	s.mu.Lock()
	defer s.mu.Unlock()
	if existing := s.byKey[key]; existing != nil {
		prevHash = existing.policyHash
		if existing.policyHash != info.NetworkPolicyHash {
			changed = true
		}
		if existing.podIP != "" && existing.podIP != info.PodIP {
			delete(s.byIP, existing.podIP)
		}
	}
	entry := &policyEntry{
		compiled:   compiled,
		policyHash: info.NetworkPolicyHash,
		podIP:      info.PodIP,
		updatedAt:  time.Now(),
	}
	s.byKey[key] = entry
	s.byIP[info.PodIP] = entry
	s.logger.Info(
		"Sandbox network policy updated",
		zap.String("sandbox", key),
		zap.String("pod_ip", info.PodIP),
		zap.Bool("changed", changed),
		zap.String("policy_hash", info.NetworkPolicyHash),
		zap.String("prev_hash", prevHash),
	)
	return changed, prevHash
}

func (s *Store) DeleteByKey(namespace, name string) {
	key := namespace + "/" + name
	s.mu.Lock()
	entry := s.byKey[key]
	delete(s.byKey, key)
	if entry != nil && entry.podIP != "" {
		delete(s.byIP, entry.podIP)
	}
	podIP := ""
	if entry != nil {
		podIP = entry.podIP
	}
	s.logger.Info(
		"Sandbox network policy deleted by key",
		zap.String("sandbox", key),
		zap.String("pod_ip", podIP),
	)
	s.mu.Unlock()
}

func (s *Store) GetByIP(podIP string) *CompiledPolicy {
	s.mu.RLock()
	defer s.mu.RUnlock()
	entry := s.byIP[podIP]
	if entry == nil || entry.compiled == nil {
		return nil
	}
	clone := *entry.compiled
	clone.Egress = cloneRuleSet(entry.compiled.Egress)
	clone.Platform = s.getPlatformPolicy()
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
	return out
}
