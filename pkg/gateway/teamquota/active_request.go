package teamquota

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"sync"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	gatewaymiddleware "github.com/sandbox0-ai/sandbox0/pkg/gateway/middleware"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	coreteamquota "github.com/sandbox0-ai/sandbox0/pkg/teamquota"
)

var activeRequestAdmissionDecisions = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Namespace: "sandbox0",
		Subsystem: "gateway",
		Name:      "team_quota_active_request_admission_decisions_total",
		Help:      "Team Quota active_request_count admission decisions.",
	},
	[]string{"outcome"},
)

// AdmitActiveRequests holds one region-shared concurrency lease for the full
// lifetime of each authenticated team request.
func (c *Controller) AdmitActiveRequests(
	trustForwardedProof bool,
) gin.HandlerFunc {
	return func(ginCtx *gin.Context) {
		cleanup, ok := c.AttachActiveRequest(ginCtx, trustForwardedProof)
		if !ok {
			return
		}
		defer cleanup()
		ginCtx.Next()
	}
}

// AttachActiveRequest applies active-request admission around handlers invoked
// directly, including Gin NoRoute fallbacks.
func (c *Controller) AttachActiveRequest(
	ginCtx *gin.Context,
	trustForwardedProof bool,
) (func(), bool) {
	authCtx := gatewaymiddleware.GetAuthContext(ginCtx)
	if shouldSkipForwardedKey(
		ginCtx,
		trustForwardedProof,
		coreteamquota.KeyActiveRequestCount,
	) {
		activeRequestAdmissionDecisions.WithLabelValues("bypassed_forwarded").Inc()
		return func() {}, true
	}
	if isSystemAdminQuotaRepairRequest(ginCtx, authCtx) {
		activeRequestAdmissionDecisions.WithLabelValues("bypassed_admin_repair").Inc()
		return func() {}, true
	}
	if authCtx == nil || strings.TrimSpace(authCtx.TeamID) == "" {
		activeRequestAdmissionDecisions.WithLabelValues("unavailable").Inc()
		c.abortUnavailable(ginCtx, "team quota requires an authenticated team", nil)
		return func() {}, false
	}
	if c == nil || c.concurrencyLimiter == nil {
		activeRequestAdmissionDecisions.WithLabelValues("unavailable").Inc()
		c.abortUnavailable(ginCtx, "team quota concurrency limiter unavailable", nil)
		return func() {}, false
	}

	lease, err := c.concurrencyLimiter.Acquire(
		ginCtx.Request.Context(),
		authCtx.TeamID,
		coreteamquota.KeyActiveRequestCount,
	)
	if err != nil {
		if coreteamquota.IsTeamAdmissionDisabled(err) &&
			isOwnTeamDeletionRequest(ginCtx, authCtx.TeamID) {
			if c.admitOwnTeamDeletionRetry(ginCtx, authCtx.TeamID) {
				activeRequestAdmissionDecisions.WithLabelValues("bypassed_deletion_retry").Inc()
				return func() {}, true
			}
			activeRequestAdmissionDecisions.WithLabelValues("rejected_deletion_retry").Inc()
			return func() {}, false
		}
		if coreteamquota.IsConcurrencyExceeded(err) {
			activeRequestAdmissionDecisions.WithLabelValues("denied").Inc()
			c.abortActiveRequestConcurrencyError(ginCtx, err)
		} else {
			activeRequestAdmissionDecisions.WithLabelValues("unavailable").Inc()
			c.abortUnavailable(ginCtx, "team quota concurrency limiter unavailable", err)
		}
		return func() {}, false
	}

	originalRequest := ginCtx.Request
	leaseCtx, cancelLeaseContext := context.WithCancelCause(originalRequest.Context())
	ginCtx.Request = originalRequest.WithContext(leaseCtx)
	originalWriter := ginCtx.Writer
	ginCtx.Writer = &connectionLeaseResponseWriter{
		ResponseWriter: originalWriter,
		ctx:            leaseCtx,
	}
	watchStop := make(chan struct{})
	watchDone := make(chan struct{})
	go func() {
		defer close(watchDone)
		select {
		case <-lease.Done():
			cancelLeaseContext(lease.Err())
		case <-watchStop:
		}
	}()

	if err := RecordAdmittedKeys(
		ginCtx,
		coreteamquota.KeyActiveRequestCount,
	); err != nil {
		ginCtx.Writer = originalWriter
		close(watchStop)
		<-watchDone
		cancelLeaseContext(nil)
		releaseLease(lease)
		activeRequestAdmissionDecisions.WithLabelValues("unavailable").Inc()
		c.abortUnavailable(ginCtx, "team quota admission proof unavailable", err)
		return func() {}, false
	}
	activeRequestAdmissionDecisions.WithLabelValues("allowed").Inc()

	var once sync.Once
	return func() {
		once.Do(func() {
			ginCtx.Writer = originalWriter
			close(watchStop)
			<-watchDone
			cancelLeaseContext(nil)
			releaseLease(lease)
		})
	}, true
}

func (c *Controller) abortActiveRequestConcurrencyError(ginCtx *gin.Context, err error) {
	var exceeded *coreteamquota.ConcurrencyExceededError
	if !errors.As(err, &exceeded) {
		c.abortUnavailable(ginCtx, "team quota concurrency decision unavailable", err)
		return
	}
	ginCtx.Header("Retry-After", "1")
	spec.JSONError(
		ginCtx,
		http.StatusTooManyRequests,
		spec.CodeQuotaExceeded,
		"team active_request_count quota exceeded",
		gin.H{
			"limit": exceeded.Limit,
			"used":  exceeded.Used,
		},
	)
	ginCtx.Abort()
}
