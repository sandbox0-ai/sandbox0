package http

import (
	nethttp "net/http"

	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
)

// forwardedQuotaAdmissionProof accepts only a regional edge proof that exactly
// matches the scheduler request. The generator clones it when re-signing, so
// the scheduler cannot add keys or alter request identity.
func forwardedQuotaAdmissionProof(
	claims *internalauth.Claims,
	request *nethttp.Request,
) *internalauth.QuotaAdmissionProof {
	if claims == nil ||
		claims.Caller != internalauth.ServiceRegionalGateway ||
		claims.QuotaAdmissionProof == nil ||
		claims.QuotaAdmissionProof.Class != internalauth.QuotaAdmissionClassEdgeAdmitted ||
		!claims.QuotaAdmissionProof.MatchesRequest(claims, request) {
		return nil
	}
	return claims.QuotaAdmissionProof
}
