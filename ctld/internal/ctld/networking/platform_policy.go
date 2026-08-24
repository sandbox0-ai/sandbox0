package networking

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net"
	"sort"
	"strings"
	"sync"

	"github.com/sandbox0-ai/sandbox0/ctld/internal/ctld/networking/model"
	"github.com/sandbox0-ai/sandbox0/ctld/internal/ctld/networking/policy"
	"github.com/sandbox0-ai/sandbox0/pkg/config"
	"go.uber.org/zap"
)

type platformPolicyState struct {
	cfg      *config.NetworkRuntimeConfig
	store    *policy.Store
	logger   *zap.Logger
	mu       sync.Mutex
	lastHash string
}

func newPlatformPolicyState(cfg *config.NetworkRuntimeConfig, store *policy.Store, logger *zap.Logger) *platformPolicyState {
	if logger == nil {
		logger = zap.NewNop()
	}
	state := &platformPolicyState{cfg: cfg, store: store, logger: logger}
	state.Reconcile(nil)
	return state
}

// Reconcile rebuilds the platform boundary from runtime-slot identities and
// explicit regional service allow/deny configuration. Service discovery is a
// control-plane responsibility; ctld never watches an orchestrator catalog.
func (s *platformPolicyState) Reconcile(sandboxes []*model.SandboxInfo) {
	if s == nil || s.store == nil {
		return
	}
	sandboxIPs := make(map[string]struct{}, len(sandboxes))
	for _, sandbox := range sandboxes {
		if sandbox == nil {
			continue
		}
		if ip := strings.TrimSpace(sandbox.SourceIP); ip != "" {
			sandboxIPs[ip] = struct{}{}
		}
	}

	var allowedCIDRs, deniedCIDRs, allowedDomains, deniedDomains []string
	if s.cfg != nil {
		allowedCIDRs = normalizeCIDRInputs(s.cfg.PlatformAllowedCIDRs, s.logger)
		deniedCIDRs = normalizeCIDRInputs(s.cfg.PlatformDeniedCIDRs, s.logger)
		allowedDomains = normalizeDomainInputs(s.cfg.PlatformAllowedDomains)
		deniedDomains = normalizeDomainInputs(s.cfg.PlatformDeniedDomains)
	}
	sort.Strings(allowedCIDRs)
	sort.Strings(deniedCIDRs)
	sort.Strings(allowedDomains)
	sort.Strings(deniedDomains)
	sandboxIPList := make([]string, 0, len(sandboxIPs))
	for ip := range sandboxIPs {
		sandboxIPList = append(sandboxIPList, ip)
	}
	sort.Strings(sandboxIPList)
	nextHash := hashPlatformPolicyInputs(sandboxIPList, allowedCIDRs, deniedCIDRs, allowedDomains, deniedDomains)
	s.mu.Lock()
	if s.lastHash == nextHash {
		s.mu.Unlock()
		return
	}
	s.lastHash = nextHash
	s.mu.Unlock()

	rules, err := policy.BuildPlatformPolicy(allowedCIDRs, deniedCIDRs, allowedDomains, deniedDomains)
	if err != nil {
		s.logger.Warn("Failed to build platform policy", zap.Error(err))
		return
	}
	rules.SandboxIPs = sandboxIPs
	s.store.SetPlatformPolicy(rules)
	s.logger.Info("Platform policy updated",
		zap.Int("sandboxes_total", len(sandboxes)),
		zap.Int("sandbox_ips", len(sandboxIPs)),
		zap.Int("allowed_cidrs", len(allowedCIDRs)),
		zap.Int("denied_cidrs", len(deniedCIDRs)),
		zap.Int("allowed_domains", len(allowedDomains)),
		zap.Int("denied_domains", len(deniedDomains)),
	)
}

func hashPlatformPolicyInputs(parts ...[]string) string {
	hash := sha256.New()
	for _, values := range parts {
		for _, value := range values {
			_, _ = hash.Write([]byte(value))
			_, _ = hash.Write([]byte{0})
		}
		_, _ = hash.Write([]byte{1})
	}
	return hex.EncodeToString(hash.Sum(nil))
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
				bits := 128
				if ip.To4() != nil {
					bits = 32
				}
				cidr = ip.String() + "/" + fmt.Sprint(bits)
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
