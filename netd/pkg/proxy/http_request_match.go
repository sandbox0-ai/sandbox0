package proxy

import (
	"net/http"

	"github.com/sandbox0-ai/sandbox0/netd/pkg/policy"
)

func (s *Server) prepareEgressAuthForHTTPRequest(req *adapterRequest, httpReq *http.Request, classifiedProtocol string) {
	if req == nil || req.EgressAuth == nil || req.EgressAuth.Rule == nil {
		return
	}
	if req.EgressAuth.Rule.HTTPMatch == nil {
		return
	}
	if !policy.MatchHTTPRequest(req.EgressAuth.Rule.HTTPMatch, httpReq) {
		req.EgressAuth = nil
		return
	}
	decision := trafficDecision{
		Transport:       "tcp",
		Protocol:        classifiedProtocol,
		MatchedAuthRule: req.EgressAuth.Rule,
		NeedsEgressAuth: true,
	}
	s.resolveEgressAuth(req, decision)
}

func egressAuthNeedsHTTPMatch(req *adapterRequest) bool {
	return req != nil &&
		req.EgressAuth != nil &&
		req.EgressAuth.Rule != nil &&
		req.EgressAuth.Rule.HTTPMatch != nil
}
