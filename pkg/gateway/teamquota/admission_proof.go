package teamquota

import (
	"context"
	"fmt"
	"net/http"
	"sort"

	"github.com/gin-gonic/gin"

	"github.com/sandbox0-ai/sandbox0/pkg/gateway/authn"
	gatewaymiddleware "github.com/sandbox0-ai/sandbox0/pkg/gateway/middleware"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	coreteamquota "github.com/sandbox0-ai/sandbox0/pkg/teamquota"
	"github.com/sandbox0-ai/sandbox0/pkg/teamquota/guard"
)

type admittedKeysContextKey struct{}
type consumedAdmissionProofContextKey struct{}

// AdmissionProofVersion returns the stable region policy version that must be
// signed into a newly forwarded proof.
func (c *Controller) AdmissionProofVersion(
	ctx context.Context,
) (guard.Version, error) {
	if c == nil || c.proofConsumer == nil {
		return guard.Version{}, &coreteamquota.UnavailableError{
			Operation: "read team quota admission proof version",
			Err:       fmt.Errorf("admission proof consumer is not configured"),
		}
	}
	version, err := c.proofConsumer.CurrentVersion(ctx)
	if err != nil {
		return guard.Version{}, &coreteamquota.UnavailableError{
			Operation: "read team quota admission proof version",
			Err:       err,
		}
	}
	return version, nil
}

// RecordAdmittedKeys records keys actually admitted or owned by
// this hop. The immutable context value is safe to read while a proxy request
// is being prepared.
func RecordAdmittedKeys(ginCtx *gin.Context, keys ...coreteamquota.Key) error {
	if ginCtx == nil || ginCtx.Request == nil {
		return fmt.Errorf("team quota admission request is required")
	}
	current := AdmittedKeys(ginCtx.Request.Context())
	seen := make(map[coreteamquota.Key]struct{}, len(current)+len(keys))
	for _, key := range current {
		seen[key] = struct{}{}
	}
	for _, key := range keys {
		if !coreteamquota.KnownKey(key) {
			return fmt.Errorf("unknown team quota key %q", key)
		}
		seen[key] = struct{}{}
	}
	admittedKeys := make([]coreteamquota.Key, 0, len(seen))
	for key := range seen {
		admittedKeys = append(admittedKeys, key)
	}
	sort.Slice(admittedKeys, func(i, j int) bool {
		return admittedKeys[i] < admittedKeys[j]
	})
	ctx := context.WithValue(
		ginCtx.Request.Context(),
		admittedKeysContextKey{},
		admittedKeys,
	)
	ginCtx.Request = ginCtx.Request.WithContext(ctx)
	return nil
}

// AdmittedKeys returns a copy of keys actually admitted by the
// current hop.
func AdmittedKeys(ctx context.Context) []coreteamquota.Key {
	if ctx == nil {
		return nil
	}
	keys, _ := ctx.Value(admittedKeysContextKey{}).([]coreteamquota.Key)
	return append([]coreteamquota.Key(nil), keys...)
}

func shouldSkipForwardedKey(
	ginCtx *gin.Context,
	trustForwardedProof bool,
	key coreteamquota.Key,
) bool {
	if !trustForwardedProof || ginCtx == nil || ginCtx.Request == nil {
		return false
	}
	_, proof, ok := forwardedAdmissionProof(ginCtx)
	if !ok ||
		consumedAdmissionProofID(ginCtx.Request.Context()) != proof.ProofID ||
		!proof.ContainsKey(key) {
		return false
	}
	return true
}

// ConsumeForwardedAdmissionProof runs after authentication and before every
// Team Quota middleware that may trust an upstream proof. Only the first
// region-wide use records the proof ID in this request's private context.
func (c *Controller) ConsumeForwardedAdmissionProof() gin.HandlerFunc {
	return func(ginCtx *gin.Context) {
		if ginCtx == nil || ginCtx.Request == nil {
			return
		}
		claims, proof, ok := forwardedAdmissionProof(ginCtx)
		if !ok {
			ginCtx.Next()
			return
		}
		if c == nil || c.proofConsumer == nil {
			c.abortUnavailable(
				ginCtx,
				"team quota admission proof consumer unavailable",
				nil,
			)
			return
		}
		trusted, err := c.proofConsumer.Consume(
			ginCtx.Request.Context(),
			claims.TeamID,
			proof.ProofID,
			proof.IssuedAtMS,
			proof.ExpiresAtMS,
			proof.PolicyVersion(),
		)
		if err != nil {
			c.abortUnavailable(
				ginCtx,
				"team quota admission proof consumer unavailable",
				err,
			)
			return
		}
		if trusted {
			ctx := context.WithValue(
				ginCtx.Request.Context(),
				consumedAdmissionProofContextKey{},
				proof.ProofID,
			)
			ginCtx.Request = ginCtx.Request.WithContext(ctx)
		}
		ginCtx.Next()
	}
}

func forwardedAdmissionProof(
	ginCtx *gin.Context,
) (*internalauth.Claims, *internalauth.QuotaAdmissionProof, bool) {
	if ginCtx == nil || ginCtx.Request == nil {
		return nil, nil, false
	}
	authCtx := gatewaymiddleware.GetAuthContext(ginCtx)
	if authCtx == nil || authCtx.AuthMethod != authn.AuthMethodInternal {
		return nil, nil, false
	}
	claims := internalauth.ClaimsFromContext(ginCtx.Request.Context())
	if claims == nil ||
		claims.QuotaAdmissionProof == nil ||
		authCtx.TeamID != claims.TeamID ||
		(authCtx.Caller != "" && authCtx.Caller != claims.Caller) {
		return nil, nil, false
	}
	if claims.Audit == nil ||
		authCtx.OperationID != claims.Audit.OperationID ||
		authCtx.RequestID != claims.Audit.RequestID ||
		!claims.QuotaAdmissionProof.MatchesRequest(claims, ginCtx.Request) {
		return nil, nil, false
	}
	switch claims.QuotaAdmissionProof.Class {
	case internalauth.QuotaAdmissionClassEdgeAdmitted:
		if claims.Caller != internalauth.ServiceRegionalGateway &&
			claims.Caller != internalauth.ServiceScheduler {
			return nil, nil, false
		}
	case internalauth.QuotaAdmissionClassSystem:
		if !systemQuotaAdmissionAllowed(claims.Caller, ginCtx.Request) {
			return nil, nil, false
		}
	default:
		return nil, nil, false
	}
	return claims, claims.QuotaAdmissionProof, true
}

func consumedAdmissionProofID(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	proofID, _ := ctx.Value(consumedAdmissionProofContextKey{}).(string)
	return proofID
}

// System control-plane calls currently use /internal/v1 and therefore never
// enter Team Quota middleware. Keep this allowlist explicit and empty until a
// concrete system-owned /api route is reviewed.
var systemQuotaAdmissionAllowlist = map[systemQuotaAdmissionRoute]struct{}{}

type systemQuotaAdmissionRoute struct {
	caller string
	method string
	path   string
}

func systemQuotaAdmissionAllowed(caller string, req *http.Request) bool {
	if req == nil {
		return false
	}
	_, ok := systemQuotaAdmissionAllowlist[systemQuotaAdmissionRoute{
		caller: caller,
		method: req.Method,
		path:   internalauth.CanonicalRequestPath(req),
	}]
	return ok
}
