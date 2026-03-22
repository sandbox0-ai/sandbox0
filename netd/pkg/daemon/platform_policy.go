package daemon

import (
	"net"
	"strings"
	"sync"

	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/netd/pkg/policy"
	"github.com/sandbox0-ai/sandbox0/netd/pkg/watcher"
	"go.uber.org/zap"
)

var platformServiceNames = map[string]struct{}{
	"cluster-gateway": {},
	"manager":         {},
	"storage-proxy":   {},
}

var clusterDNSServiceNames = map[string]struct{}{
	"kube-dns": {},
	"coredns":  {},
}

type platformPolicyState struct {
	cfg       *config.NetdConfig
	store     *policy.Store
	logger    *zap.Logger
	mu        sync.RWMutex
	sandboxes map[string]*watcher.SandboxInfo
	services  map[string]*watcher.ServiceInfo
	endpoints map[string]*watcher.EndpointsInfo
}

func newPlatformPolicyState(cfg *config.NetdConfig, store *policy.Store, logger *zap.Logger) *platformPolicyState {
	if logger == nil {
		logger = zap.NewNop()
	}
	state := &platformPolicyState{
		cfg:       cfg,
		store:     store,
		logger:    logger,
		sandboxes: make(map[string]*watcher.SandboxInfo),
		services:  make(map[string]*watcher.ServiceInfo),
		endpoints: make(map[string]*watcher.EndpointsInfo),
	}
	state.rebuild()
	return state
}

func (s *platformPolicyState) OnSandboxUpsert(info *watcher.SandboxInfo) {
	if info == nil {
		return
	}
	key := info.Namespace + "/" + info.Name
	s.mu.Lock()
	s.sandboxes[key] = info
	s.mu.Unlock()
	s.rebuild()
}

func (s *platformPolicyState) OnSandboxDelete(info *watcher.SandboxInfo) {
	if info == nil {
		return
	}
	key := info.Namespace + "/" + info.Name
	s.mu.Lock()
	delete(s.sandboxes, key)
	s.mu.Unlock()
	s.rebuild()
}

func (s *platformPolicyState) OnServiceUpsert(info *watcher.ServiceInfo) {
	if info == nil {
		return
	}
	key := info.Namespace + "/" + info.Name
	s.mu.Lock()
	s.services[key] = info
	s.mu.Unlock()
	s.rebuild()
}

func (s *platformPolicyState) OnServiceDelete(info *watcher.ServiceInfo) {
	if info == nil {
		return
	}
	key := info.Namespace + "/" + info.Name
	s.mu.Lock()
	delete(s.services, key)
	s.mu.Unlock()
	s.rebuild()
}

func (s *platformPolicyState) OnEndpointsUpsert(info *watcher.EndpointsInfo) {
	if info == nil {
		return
	}
	key := info.Namespace + "/" + info.Name
	s.mu.Lock()
	s.endpoints[key] = info
	s.mu.Unlock()
	s.rebuild()
}

func (s *platformPolicyState) OnEndpointsDelete(info *watcher.EndpointsInfo) {
	if info == nil {
		return
	}
	key := info.Namespace + "/" + info.Name
	s.mu.Lock()
	delete(s.endpoints, key)
	s.mu.Unlock()
	s.rebuild()
}

func (s *platformPolicyState) rebuild() {
	if s.store == nil {
		return
	}
	sandboxes, services, endpoints := s.snapshot()
	allowedCIDRs := make([]string, 0, len(services))
	allowedDomains := []string{}
	sandboxPodIPs := make(map[string]struct{}, len(sandboxes))
	matchedServices := make([]string, 0, len(services))
	for _, sandbox := range sandboxes {
		if sandbox == nil {
			continue
		}
		if ip := strings.TrimSpace(sandbox.PodIP); ip != "" {
			sandboxPodIPs[ip] = struct{}{}
		}
	}
	for key, svc := range services {
		if !isPlatformService(svc) {
			continue
		}
		matchedServices = append(matchedServices, key)
		if svc.ClusterIP != "" && strings.ToLower(svc.ClusterIP) != "none" {
			allowedCIDRs = append(allowedCIDRs, svc.ClusterIP)
		}
		allowedDomains = append(allowedDomains, platformServiceDomains(svc)...)
		if ep := endpoints[key]; ep != nil {
			allowedCIDRs = append(allowedCIDRs, ep.Addresses...)
		}
	}

	platformAllowedCIDRs := []string{}
	platformDeniedCIDRs := []string{}
	platformAllowedDomains := []string{}
	platformDeniedDomains := []string{}
	if s.cfg != nil {
		platformAllowedCIDRs = s.cfg.PlatformAllowedCIDRs
		platformDeniedCIDRs = s.cfg.PlatformDeniedCIDRs
		platformAllowedDomains = s.cfg.PlatformAllowedDomains
		platformDeniedDomains = s.cfg.PlatformDeniedDomains
	}
	allowedCIDRs = normalizeCIDRInputs(append(allowedCIDRs, platformAllowedCIDRs...), s.logger)
	allowedDomains = normalizeDomainInputs(append(allowedDomains, platformAllowedDomains...))
	deniedCIDRs := normalizeCIDRInputs(platformDeniedCIDRs, s.logger)

	policyRules, err := policy.BuildPlatformPolicy(
		allowedCIDRs,
		deniedCIDRs,
		allowedDomains,
		platformDeniedDomains,
	)
	if err != nil {
		s.logger.Warn("Failed to build platform policy", zap.Error(err))
		return
	}
	policyRules.SandboxPodIPs = sandboxPodIPs
	s.store.SetPlatformPolicy(policyRules)
	s.logger.Info(
		"Platform policy updated",
		zap.Int("sandboxes_total", len(sandboxes)),
		zap.Int("sandbox_pod_ips", len(sandboxPodIPs)),
		zap.Int("services_total", len(services)),
		zap.Int("services_matched", len(matchedServices)),
		zap.Int("endpoints_total", len(endpoints)),
		zap.Int("allowed_cidrs", len(allowedCIDRs)),
		zap.Int("denied_cidrs", len(deniedCIDRs)),
		zap.Int("allowed_domains", len(allowedDomains)),
		zap.Int("denied_domains", len(platformDeniedDomains)),
		zap.Strings("matched_services", matchedServices),
	)
}

func (s *platformPolicyState) snapshot() (map[string]*watcher.SandboxInfo, map[string]*watcher.ServiceInfo, map[string]*watcher.EndpointsInfo) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	sandboxes := make(map[string]*watcher.SandboxInfo, len(s.sandboxes))
	for key, info := range s.sandboxes {
		sandboxes[key] = info
	}
	services := make(map[string]*watcher.ServiceInfo, len(s.services))
	for key, info := range s.services {
		services[key] = info
	}
	endpoints := make(map[string]*watcher.EndpointsInfo, len(s.endpoints))
	for key, info := range s.endpoints {
		endpoints[key] = info
	}
	return sandboxes, services, endpoints
}

func isPlatformService(info *watcher.ServiceInfo) bool {
	if info == nil || info.Labels == nil {
		return isClusterDNSService(info)
	}
	if isClusterDNSService(info) {
		return true
	}
	if info.Labels["app.kubernetes.io/managed-by"] != "sandbox0infra-operator" {
		return false
	}
	name := info.Labels["app.kubernetes.io/name"]
	for platformName := range platformServiceNames {
		if name == platformName || strings.HasSuffix(name, "-"+platformName) {
			return true
		}
	}
	return false
}

func isClusterDNSService(info *watcher.ServiceInfo) bool {
	if info == nil {
		return false
	}
	if info.Namespace != "kube-system" {
		return false
	}
	_, ok := clusterDNSServiceNames[info.Name]
	return ok
}

func platformServiceDomains(info *watcher.ServiceInfo) []string {
	if info == nil {
		return nil
	}
	name := strings.TrimSpace(info.Name)
	namespace := strings.TrimSpace(info.Namespace)
	if name == "" || namespace == "" {
		return nil
	}
	return []string{
		name,
		name + "." + namespace,
		name + "." + namespace + ".svc",
		name + "." + namespace + ".svc.cluster.local",
	}
}

func normalizeCIDRInputs(values []string, logger *zap.Logger) []string {
	if len(values) == 0 {
		return nil
	}
	seen := map[string]struct{}{}
	out := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		var cidr string
		if !strings.Contains(value, "/") {
			if ip := net.ParseIP(value); ip != nil {
				cidr = ip.String() + "/32"
			} else {
				if logger != nil {
					logger.Warn("Ignoring invalid platform CIDR", zap.String("value", value))
				}
				continue
			}
		} else {
			_, parsedCIDR, err := net.ParseCIDR(value)
			if err != nil || parsedCIDR == nil {
				if logger != nil {
					logger.Warn("Ignoring invalid platform CIDR", zap.String("value", value))
				}
				continue
			}
			cidr = parsedCIDR.String()
		}
		if _, ok := seen[cidr]; ok {
			continue
		}
		seen[cidr] = struct{}{}
		out = append(out, cidr)
	}
	return out
}

func normalizeDomainInputs(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	seen := map[string]struct{}{}
	out := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.ToLower(strings.TrimSpace(value))
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
