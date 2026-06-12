package proxy

import (
	"net/http"

	"github.com/sandbox0-ai/sandbox0/netd/pkg/policy"
)

func (s *Server) prepareEgressAuthForHTTPRequest(req *adapterRequest, httpReq *http.Request, classifiedProtocol string) {
	if req == nil || req.EgressAuth == nil || req.EgressAuth.ShouldBypass() {
		return
	}
	ctx := req.EgressAuth
	candidates := ctx.CandidateRules
	if len(candidates) == 0 && ctx.Rule != nil {
		candidates = []*policy.CompiledEgressAuthRule{ctx.Rule}
	}
	if len(candidates) == 0 {
		return
	}
	if !ctx.RequestMatch && ctx.Rule != nil && ctx.Rule.HTTPMatch == nil && !ctx.ResolveOnHTTPRequest {
		return
	}
	rule := matchEgressAuthRuleForHTTPRequest(candidates, httpReq)
	if rule == nil {
		req.EgressAuth = nil
		return
	}
	selectEgressAuthRuleForHTTPRequest(ctx, rule, s)
	decision := trafficDecision{
		Transport:       "tcp",
		Protocol:        classifiedProtocol,
		MatchedAuthRule: rule,
		NeedsEgressAuth: true,
	}
	s.resolveEgressAuth(req, decision)
}

func matchEgressAuthRuleForHTTPRequest(candidates []*policy.CompiledEgressAuthRule, httpReq *http.Request) *policy.CompiledEgressAuthRule {
	for _, rule := range candidates {
		if rule == nil {
			continue
		}
		if rule.HTTPMatch != nil && !policy.MatchHTTPRequest(rule.HTTPMatch, httpReq) {
			continue
		}
		return rule
	}
	return nil
}

func selectEgressAuthRuleForHTTPRequest(ctx *egressAuthContext, rule *policy.CompiledEgressAuthRule, server *Server) {
	if ctx == nil || rule == nil || ctx.Rule == rule {
		if ctx != nil {
			ctx.RequestMatch = true
			ctx.ResolveOnHTTPRequest = false
		}
		return
	}
	ctx.Rule = rule
	ctx.RequestMatch = true
	ctx.ResolveOnHTTPRequest = false
	ctx.Resolved = nil
	ctx.ResolvedHeaders = nil
	ctx.ResolvedTLSClientCertificate = nil
	ctx.ResolvedUsernamePassword = nil
	ctx.ResolvedSSHProxy = nil
	ctx.CacheHit = false
	ctx.ResolveAttempt = false
	ctx.ResolveError = nil
	ctx.BypassReason = ""
	ctx.EnforcementReason = ""
	if server != nil {
		ctx.FailurePolicy = egressAuthFailurePolicy(server.cfg, rule)
	}
}

func egressAuthNeedsHTTPMatch(req *adapterRequest) bool {
	return req != nil &&
		req.EgressAuth != nil &&
		(req.EgressAuth.RequestMatch ||
			(req.EgressAuth.Rule != nil && req.EgressAuth.Rule.HTTPMatch != nil))
}

func egressAuthResolvesOnHTTPRequest(req *adapterRequest) bool {
	return req != nil && req.EgressAuth != nil && req.EgressAuth.ResolveOnHTTPRequest
}
