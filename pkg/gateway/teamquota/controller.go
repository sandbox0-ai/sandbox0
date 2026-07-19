// Package teamquota exposes region-owned Team Quota HTTP APIs and gateway
// request admission.
package teamquota

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.uber.org/zap"

	apiconfig "github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/pkg/apispec"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/authn"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/identity"
	gatewaymiddleware "github.com/sandbox0-ai/sandbox0/pkg/gateway/middleware"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	coreteamquota "github.com/sandbox0-ai/sandbox0/pkg/teamquota"
	teamquotaconcurrency "github.com/sandbox0-ai/sandbox0/pkg/teamquota/concurrency"
	teamquotadistributed "github.com/sandbox0-ai/sandbox0/pkg/teamquota/distributed"
	"github.com/sandbox0-ai/sandbox0/pkg/teamquota/guard"
	teamquotanetwork "github.com/sandbox0-ai/sandbox0/pkg/teamquota/network"
	teamquotarate "github.com/sandbox0-ai/sandbox0/pkg/teamquota/rate"
	"github.com/sandbox0-ai/sandbox0/pkg/tokenbucket"
)

const (
	unavailableRetryAfterSeconds         = 1
	deletedTeamCleanupInterval           = time.Minute
	deletedTeamCleanupBatchSize          = 1000
	maxDeletedTeamCleanupBatchesPerCycle = 16
	minDeletedTeamTombstoneRetention     = time.Hour
	deletedTeamTokenExpirySafetyMargin   = 5 * time.Minute
	unknownAccessTokenTTLRetention       = 30 * 24 * time.Hour
	deletionRetryTokens                  = int64(1)
	deletionRetryBurst                   = int64(3)
)

var apiRequestAdmissionDecisions = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Namespace: "sandbox0",
		Subsystem: "gateway",
		Name:      "team_quota_admission_decisions_total",
		Help:      "Team Quota api_requests admission decisions.",
	},
	[]string{"outcome"},
)

// RateLimiter is the request-rate subset used by the gateway.
type RateLimiter interface {
	Take(ctx context.Context, teamID string, key coreteamquota.Key, cost int64) (tokenbucket.Decision, error)
	Invalidate(teamID string, key coreteamquota.Key)
}

// DistributedAdmissionDisabler writes the Redis marker used by every
// distributed enforcer before identity deletion is allowed to proceed.
type DistributedAdmissionDisabler interface {
	DisableTeamDistributedAdmission(ctx context.Context, teamID string) error
}

// AdmissionProofConsumer atomically consumes a signed forwarding proof once
// across every cluster-gateway replica in a region.
type AdmissionProofConsumer interface {
	CurrentVersion(context.Context) (guard.Version, error)
	Consume(
		context.Context,
		string,
		string,
		int64,
		int64,
		guard.Version,
	) (bool, error)
	Close() error
}

// ConnectionLease is one live distributed-concurrency allocation.
type ConnectionLease interface {
	Done() <-chan struct{}
	Err() error
	Release(context.Context) error
}

// ConcurrencyLimiter admits and reports exact live concurrency.
type ConcurrencyLimiter interface {
	Acquire(context.Context, string, coreteamquota.Key) (ConnectionLease, error)
	Usage(context.Context, string, coreteamquota.Key) (int64, error)
	Invalidate(string, coreteamquota.Key)
	Close() error
}

// NetworkLimiter accounts directional bytes before forwarding them.
type NetworkLimiter interface {
	WaitN(context.Context, string, coreteamquota.Key, int) error
	Close() error
}

type concurrencyLimiterAdapter struct {
	limiter *teamquotaconcurrency.Limiter
}

func (a concurrencyLimiterAdapter) Acquire(
	ctx context.Context,
	teamID string,
	key coreteamquota.Key,
) (ConnectionLease, error) {
	return a.limiter.Acquire(ctx, teamID, key)
}

func (a concurrencyLimiterAdapter) Usage(
	ctx context.Context,
	teamID string,
	key coreteamquota.Key,
) (int64, error) {
	return a.limiter.Usage(ctx, teamID, key)
}

func (a concurrencyLimiterAdapter) Invalidate(teamID string, key coreteamquota.Key) {
	a.limiter.Invalidate(teamID, key)
}

func (a concurrencyLimiterAdapter) Close() error {
	return a.limiter.Close()
}

type redisStore interface {
	coreteamquota.PolicyReader
	coreteamquota.TeamAdmissionStateResolver
}

// TeamLookup verifies that system-admin policy operations target an existing
// team.
type TeamLookup interface {
	GetTeamByID(ctx context.Context, id string) (*identity.Team, error)
}

type deletedTeamTombstoneStore interface {
	ListDeletedTeamTombstones(
		context.Context,
		time.Time,
		*coreteamquota.DeletedTeamTombstone,
		int,
	) ([]coreteamquota.DeletedTeamTombstone, error)
	PruneDeletedTeamTombstone(context.Context, string, time.Time) (bool, error)
}

// DeletedTeamTombstoneRetention returns a safe retention window derived from
// the longest access-token lifetime accepted by this gateway. A verifier with
// no configured issuance TTL uses a conservative fallback.
func DeletedTeamTombstoneRetention(accessTokenTTL time.Duration) time.Duration {
	if accessTokenTTL <= 0 {
		return unknownAccessTokenTTLRetention
	}
	if accessTokenTTL > time.Duration(1<<63-1)-deletedTeamTokenExpirySafetyMargin {
		return time.Duration(1<<63 - 1)
	}
	retention := accessTokenTTL + deletedTeamTokenExpirySafetyMargin
	if retention < minDeletedTeamTombstoneRetention {
		return minDeletedTeamTombstoneRetention
	}
	return retention
}

// Controller owns the shared Team Quota HTTP handlers and API request
// admission middleware.
type Controller struct {
	policyReader       coreteamquota.PolicyReader
	policyManager      coreteamquota.PolicyManager
	teamLookup         TeamLookup
	limiter            RateLimiter
	concurrencyLimiter ConcurrencyLimiter
	networkLimiter     NetworkLimiter
	proofConsumer      AdmissionProofConsumer
	disabler           DistributedAdmissionDisabler
	marker             teamquotadistributed.AtomicAdmissionMarker
	bucket             tokenbucket.Bucket
	logger             *zap.Logger
	regionID           string

	ownsMarker             bool
	ownsBucket             bool
	ownsConcurrencyLimiter bool
	ownsNetworkLimiter     bool
	ownsProofConsumer      bool

	cleanupMu      sync.Mutex
	cleanupStarted bool
	cleanupCursor  *coreteamquota.DeletedTeamTombstone
}

// ControllerOption injects optional distributed enforcers. Injected
// dependencies remain caller-owned.
type ControllerOption func(*Controller)

// WithPolicyManager grants the controller serialized region policy-owner
// authority. Read-only consumers must not set this option.
func WithPolicyManager(manager coreteamquota.PolicyManager) ControllerOption {
	return func(controller *Controller) {
		controller.policyManager = manager
	}
}

// WithConcurrencyLimiter injects exact live-concurrency admission.
func WithConcurrencyLimiter(limiter ConcurrencyLimiter) ControllerOption {
	return func(controller *Controller) {
		controller.concurrencyLimiter = limiter
	}
}

// WithNetworkLimiter injects external-boundary byte admission.
func WithNetworkLimiter(limiter NetworkLimiter) ControllerOption {
	return func(controller *Controller) {
		controller.networkLimiter = limiter
	}
}

// WithAdmissionProofConsumer injects region-shared one-time proof admission.
func WithAdmissionProofConsumer(consumer AdmissionProofConsumer) ControllerOption {
	return func(controller *Controller) {
		controller.proofConsumer = consumer
	}
}

// WithRegionID sets the distributed key namespace for injected test
// primitives.
func WithRegionID(regionID string) ControllerOption {
	return func(controller *Controller) {
		controller.regionID = strings.TrimSpace(regionID)
	}
}

// NewController builds a controller from explicitly supplied dependencies.
// A nil store or limiter is retained so requests fail closed with 503 instead
// of making process startup depend on a transient backend.
func NewController(
	reader coreteamquota.PolicyReader,
	teamLookup TeamLookup,
	limiter RateLimiter,
	bucket tokenbucket.Bucket,
	logger *zap.Logger,
	opts ...ControllerOption,
) *Controller {
	if logger == nil {
		logger = zap.NewNop()
	}
	controller := &Controller{
		policyReader: reader,
		teamLookup:   teamLookup,
		limiter:      limiter,
		bucket:       bucket,
		logger:       logger,
	}
	if disabler, ok := limiter.(DistributedAdmissionDisabler); ok {
		controller.disabler = disabler
	}
	for _, opt := range opts {
		if opt != nil {
			opt(controller)
		}
	}
	return controller
}

// NewDistributedController builds all Redis-backed gateway enforcers.
func NewDistributedController(
	ctx context.Context,
	store redisStore,
	teamLookup TeamLookup,
	regionID string,
	cfg apiconfig.TeamQuotaDistributedEnforcementConfig,
	logger *zap.Logger,
	opts ...ControllerOption,
) (*Controller, error) {
	controller := NewController(store, teamLookup, nil, nil, logger, opts...)
	controller.regionID = strings.TrimSpace(regionID)
	cfg.RedisKeyPrefix = coreteamquota.NormalizeTeamQuotaRedisKeyPrefix(cfg.RedisKeyPrefix)
	marker, err := teamquotadistributed.NewRedisAdmissionMarker(ctx, store, teamquotadistributed.AdmissionMarkerConfig{
		RegionID:  regionID,
		RedisURL:  cfg.RedisURL,
		KeyPrefix: cfg.RedisKeyPrefix,
		Timeout:   cfg.RedisTimeout.Duration,
	})
	if err != nil {
		return nil, fmt.Errorf("create distributed admission marker: %w", err)
	}
	bucket, err := tokenbucket.NewRedisBucket(ctx, tokenbucket.RedisConfig{
		URL:       cfg.RedisURL,
		KeyPrefix: cfg.RedisKeyPrefix,
		Timeout:   cfg.RedisTimeout.Duration,
	})
	if err != nil {
		_ = marker.Close()
		return nil, fmt.Errorf("create distributed token bucket: %w", err)
	}
	limiter, err := teamquotarate.NewLimiter(store, marker, bucket, teamquotarate.Config{
		RegionID:       regionID,
		PolicyCacheTTL: cfg.PolicyCacheTTL.Duration,
	})
	if err != nil {
		_ = bucket.Close()
		_ = marker.Close()
		return nil, fmt.Errorf("create distributed rate limiter: %w", err)
	}
	controller.limiter = limiter
	controller.disabler = limiter
	controller.marker = marker
	controller.bucket = bucket
	controller.ownsMarker = true
	controller.ownsBucket = true

	proofConsumer, err := teamquotadistributed.NewRedisAdmissionProofConsumer(
		ctx,
		marker,
		teamquotadistributed.AdmissionProofConsumerConfig{
			RegionID:  regionID,
			RedisURL:  cfg.RedisURL,
			KeyPrefix: cfg.RedisKeyPrefix,
			Timeout:   cfg.RedisTimeout.Duration,
		},
	)
	if err != nil {
		_ = controller.Close()
		return nil, fmt.Errorf("create distributed admission proof consumer: %w", err)
	}
	controller.proofConsumer = proofConsumer
	controller.ownsProofConsumer = true

	concurrencyLimiter, err := teamquotaconcurrency.NewRedisLimiter(
		ctx,
		store,
		teamquotaconcurrency.Config{
			RegionID:       regionID,
			RedisURL:       cfg.RedisURL,
			RedisKeyPrefix: cfg.RedisKeyPrefix,
			RedisTimeout:   cfg.RedisTimeout.Duration,
			PolicyCacheTTL: cfg.PolicyCacheTTL.Duration,
			LeaseTTL:       cfg.LeaseTTL.Duration,
			RenewInterval:  cfg.RenewInterval.Duration,
		},
	)
	if err != nil {
		_ = controller.Close()
		return nil, fmt.Errorf("create distributed concurrency limiter: %w", err)
	}
	controller.concurrencyLimiter = concurrencyLimiterAdapter{limiter: concurrencyLimiter}
	controller.ownsConcurrencyLimiter = true

	networkLimiter, err := teamquotanetwork.NewRedis(
		ctx,
		store,
		teamquotanetwork.Config{
			RegionID:       regionID,
			RedisURL:       cfg.RedisURL,
			RedisKeyPrefix: cfg.RedisKeyPrefix,
			RedisTimeout:   cfg.RedisTimeout.Duration,
			PolicyCacheTTL: cfg.PolicyCacheTTL.Duration,
		},
	)
	if err != nil {
		_ = controller.Close()
		return nil, fmt.Errorf("create distributed network limiter: %w", err)
	}
	controller.networkLimiter = networkLimiter
	controller.ownsNetworkLimiter = true
	return controller, nil
}

// Close releases only dependencies owned by this controller.
func (c *Controller) Close() error {
	if c == nil {
		return nil
	}
	var errs []error
	if c.ownsMarker && c.marker != nil {
		errs = append(errs, c.marker.Close())
	}
	if c.ownsBucket && c.bucket != nil {
		errs = append(errs, c.bucket.Close())
	}
	if c.ownsConcurrencyLimiter && c.concurrencyLimiter != nil {
		errs = append(errs, c.concurrencyLimiter.Close())
	}
	if c.ownsNetworkLimiter && c.networkLimiter != nil {
		errs = append(errs, c.networkLimiter.Close())
	}
	if c.ownsProofConsumer && c.proofConsumer != nil {
		errs = append(errs, c.proofConsumer.Close())
	}
	return errors.Join(errs...)
}

// StartDeletedTeamTombstoneCleanup bounds durable deletion markers after all
// access tokens issued before team deletion have expired. PostgreSQL remains
// fail-closed until the identity directory confirms that the team is absent.
func (c *Controller) StartDeletedTeamTombstoneCleanup(
	ctx context.Context,
	retention time.Duration,
) error {
	if c == nil {
		return fmt.Errorf("team quota controller is not configured")
	}
	if ctx == nil {
		return fmt.Errorf("team quota tombstone cleanup context is required")
	}
	if retention <= 0 {
		return fmt.Errorf("team quota tombstone retention must be positive")
	}
	store, ok := c.policyReader.(deletedTeamTombstoneStore)
	if !ok || store == nil {
		return fmt.Errorf("team quota store does not support tombstone retention")
	}
	if c.teamLookup == nil {
		return fmt.Errorf("team directory is required for tombstone retention")
	}

	c.cleanupMu.Lock()
	if c.cleanupStarted {
		c.cleanupMu.Unlock()
		return nil
	}
	c.cleanupStarted = true
	c.cleanupMu.Unlock()

	go c.runDeletedTeamTombstoneCleanup(ctx, store, retention)
	return nil
}

func (c *Controller) runDeletedTeamTombstoneCleanup(
	ctx context.Context,
	store deletedTeamTombstoneStore,
	retention time.Duration,
) {
	c.cleanupDeletedTeamTombstones(ctx, store, retention)
	ticker := time.NewTicker(deletedTeamCleanupInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.cleanupDeletedTeamTombstones(ctx, store, retention)
		}
	}
}

func (c *Controller) cleanupDeletedTeamTombstones(
	ctx context.Context,
	store deletedTeamTombstoneStore,
	retention time.Duration,
) {
	if c == nil || store == nil || c.teamLookup == nil || retention <= 0 {
		return
	}
	cutoff := time.Now().UTC().Add(-retention)
	c.cleanupMu.Lock()
	cursor := c.cleanupCursor
	c.cleanupMu.Unlock()
	for range maxDeletedTeamCleanupBatchesPerCycle {
		tombstones, err := store.ListDeletedTeamTombstones(
			ctx,
			cutoff,
			cursor,
			deletedTeamCleanupBatchSize,
		)
		if err != nil {
			c.logger.Warn("Failed to list deleted Team Quota tombstones", zap.Error(err))
			return
		}
		if len(tombstones) == 0 {
			c.setDeletedTeamCleanupCursor(nil)
			return
		}

		for _, tombstone := range tombstones {
			teamID := tombstone.TeamID
			_, lookupErr := c.teamLookup.GetTeamByID(ctx, teamID)
			switch {
			case lookupErr == nil:
				continue
			case !errors.Is(lookupErr, identity.ErrTeamNotFound):
				c.logger.Warn(
					"Failed to verify deleted Team Quota identity",
					zap.String("team_id", teamID),
					zap.Error(lookupErr),
				)
				continue
			}
			if c.marker != nil {
				if err := c.marker.Forget(ctx, teamID); err != nil {
					c.logger.Warn(
						"Failed to forget deleted Team Quota admission marker",
						zap.String("team_id", teamID),
						zap.Error(err),
					)
					continue
				}
			}
			_, pruneErr := store.PruneDeletedTeamTombstone(ctx, teamID, cutoff)
			if pruneErr != nil {
				c.logger.Warn(
					"Failed to prune deleted Team Quota tombstone",
					zap.String("team_id", teamID),
					zap.Error(pruneErr),
				)
				continue
			}
		}
		last := tombstones[len(tombstones)-1]
		cursor = &last
		c.setDeletedTeamCleanupCursor(cursor)
		if len(tombstones) < deletedTeamCleanupBatchSize {
			c.setDeletedTeamCleanupCursor(nil)
			return
		}
	}
}

func (c *Controller) setDeletedTeamCleanupCursor(cursor *coreteamquota.DeletedTeamTombstone) {
	c.cleanupMu.Lock()
	defer c.cleanupMu.Unlock()
	if cursor == nil {
		c.cleanupCursor = nil
		return
	}
	cloned := *cursor
	c.cleanupCursor = &cloned
}

// DisableTeamDistributedAdmission publishes the regional Redis cache marker. It must
// succeed after the durable PostgreSQL tombstone and before identity deletion.
func (c *Controller) DisableTeamDistributedAdmission(ctx context.Context, teamID string) error {
	if c == nil || c.disabler == nil {
		return &coreteamquota.UnavailableError{
			Operation: "disable distributed team admission",
			Err:       fmt.Errorf("team quota distributed disabler is not configured"),
		}
	}
	return c.disabler.DisableTeamDistributedAdmission(ctx, teamID)
}

// RateLimitAPIRequests consumes one api_requests token. Cluster-gateway skips
// it only when a matching signed forwarding proof includes this key.
func (c *Controller) RateLimitAPIRequests(
	trustForwardedProof bool,
) gin.HandlerFunc {
	return func(ginCtx *gin.Context) {
		if !c.AdmitAPIRequest(ginCtx, trustForwardedProof) {
			return
		}
		ginCtx.Next()
	}
}

// AdmitAPIRequest consumes one api_requests token without advancing Gin's
// handler chain. Direct handlers use it to keep surrounding traffic admission
// active while proxying the request.
func (c *Controller) AdmitAPIRequest(
	ginCtx *gin.Context,
	trustForwardedProof bool,
) bool {
	authCtx := gatewaymiddleware.GetAuthContext(ginCtx)
	if deletionRetryWasAdmitted(ginCtx) {
		apiRequestAdmissionDecisions.WithLabelValues("allowed_deletion_retry").Inc()
		return true
	}
	if shouldSkipForwardedKey(
		ginCtx,
		trustForwardedProof,
		coreteamquota.KeyAPIRequests,
	) {
		apiRequestAdmissionDecisions.WithLabelValues("bypassed_forwarded").Inc()
		return true
	}
	if isSystemAdminQuotaRepairRequest(ginCtx, authCtx) {
		apiRequestAdmissionDecisions.WithLabelValues("bypassed_admin_repair").Inc()
		return true
	}
	if authCtx == nil || strings.TrimSpace(authCtx.TeamID) == "" {
		apiRequestAdmissionDecisions.WithLabelValues("unavailable").Inc()
		c.abortUnavailable(ginCtx, "team quota requires an authenticated team", nil)
		return false
	}
	err := c.TakeRate(
		ginCtx.Request.Context(),
		authCtx.TeamID,
		coreteamquota.KeyAPIRequests,
		1,
	)
	if err != nil {
		if coreteamquota.IsTeamAdmissionDisabled(err) &&
			isOwnTeamDeletionRequest(ginCtx, authCtx.TeamID) {
			if c.admitOwnTeamDeletionRetry(ginCtx, authCtx.TeamID) {
				apiRequestAdmissionDecisions.WithLabelValues("allowed_deletion_retry").Inc()
				return true
			}
			return false
		}
		if coreteamquota.IsRateExceeded(err) {
			apiRequestAdmissionDecisions.WithLabelValues("denied").Inc()
			c.abortRateExceeded(ginCtx, err, "team api_requests quota exceeded")
			return false
		}
		apiRequestAdmissionDecisions.WithLabelValues("unavailable").Inc()
		c.abortUnavailable(ginCtx, "team quota rate limiter unavailable", err)
		return false
	}
	if err := RecordAdmittedKeys(ginCtx, coreteamquota.KeyAPIRequests); err != nil {
		apiRequestAdmissionDecisions.WithLabelValues("unavailable").Inc()
		c.abortUnavailable(ginCtx, "team quota admission proof unavailable", err)
		return false
	}
	apiRequestAdmissionDecisions.WithLabelValues("allowed").Inc()
	return true
}

// admitOwnTeamDeletionRetry admits the only operation allowed to finish after
// its Team tombstone exists and marks it so later admission middleware does not
// consume the bounded retry token twice.
func (c *Controller) admitOwnTeamDeletionRetry(
	ginCtx *gin.Context,
	teamID string,
) bool {
	if enableErr := enableDeletionRetryEgress(ginCtx); enableErr != nil {
		apiRequestAdmissionDecisions.WithLabelValues("unavailable").Inc()
		c.abortUnavailable(ginCtx, deletionRetryTrafficUnavailable, enableErr)
		return false
	}
	if deletionRetryRequestHasBody(ginCtx.Request) {
		apiRequestAdmissionDecisions.WithLabelValues("denied_deletion_retry_body").Inc()
		spec.JSONError(
			ginCtx,
			http.StatusBadRequest,
			spec.CodeBadRequest,
			"team deletion retry request body is not allowed",
		)
		ginCtx.Abort()
		return false
	}
	if !c.admitDeletionRetry(ginCtx, teamID) {
		return false
	}
	ginCtx.Set(deletionRetryAdmittedContextKey, true)
	return true
}

func deletionRetryWasAdmitted(ginCtx *gin.Context) bool {
	if ginCtx == nil {
		return false
	}
	admitted, _ := ginCtx.Get(deletionRetryAdmittedContextKey)
	value, _ := admitted.(bool)
	return value
}

func deletionRetryRequestHasBody(request *http.Request) bool {
	return request != nil &&
		(request.ContentLength != 0 || len(request.TransferEncoding) != 0)
}

// TakeRate performs one immediate typed rate admission.
func (c *Controller) TakeRate(
	ctx context.Context,
	teamID string,
	key coreteamquota.Key,
	cost int64,
) error {
	teamID = strings.TrimSpace(teamID)
	if c == nil || c.limiter == nil {
		return &coreteamquota.UnavailableError{
			Operation: "take gateway rate tokens",
			Err:       fmt.Errorf("rate limiter is not configured"),
		}
	}
	if teamID == "" {
		return &coreteamquota.UnavailableError{
			Operation: "take gateway rate tokens",
			Err:       fmt.Errorf("team_id is required"),
		}
	}
	decision, err := c.limiter.Take(ctx, teamID, key, cost)
	if err != nil {
		if coreteamquota.IsUnavailable(err) {
			return err
		}
		return &coreteamquota.UnavailableError{
			Operation: fmt.Sprintf("take %s rate tokens", key),
			Err:       err,
		}
	}
	if !decision.Allowed {
		return &coreteamquota.RateExceededError{
			TeamID:     teamID,
			Key:        key,
			Remaining:  decision.Remaining,
			RetryAfter: decision.RetryAfter,
		}
	}
	return nil
}

func (c *Controller) admitDeletionRetry(ginCtx *gin.Context, teamID string) bool {
	if c == nil || c.bucket == nil {
		c.abortUnavailable(ginCtx, "team deletion retry admission unavailable", nil)
		return false
	}
	decision, err := c.bucket.TakeN(
		ginCtx.Request.Context(),
		deletionRetryBucketKey(c.regionID, teamID),
		tokenbucket.Policy{
			Tokens:   deletionRetryTokens,
			Interval: time.Second,
			Burst:    deletionRetryBurst,
			Revision: 1,
		},
		1,
	)
	if err != nil {
		c.abortUnavailable(ginCtx, "team deletion retry admission unavailable", err)
		return false
	}
	if !decision.Allowed {
		c.abortRateExceeded(
			ginCtx,
			&coreteamquota.RateExceededError{
				TeamID:     teamID,
				Key:        coreteamquota.KeyAPIRequests,
				Remaining:  decision.Remaining,
				RetryAfter: decision.RetryAfter,
			},
			"team deletion retry quota exceeded",
		)
		return false
	}
	return true
}

func (c *Controller) abortRateExceeded(ginCtx *gin.Context, err error, message string) {
	var exceeded *coreteamquota.RateExceededError
	if !errors.As(err, &exceeded) {
		c.abortUnavailable(ginCtx, "team quota decision unavailable", err)
		return
	}
	retryAfter := retryAfterSeconds(exceeded.RetryAfter)
	ginCtx.Header("Retry-After", strconv.Itoa(retryAfter))
	ginCtx.Header("X-RateLimit-Remaining", strconv.FormatInt(exceeded.Remaining, 10))
	spec.JSONError(
		ginCtx,
		http.StatusTooManyRequests,
		spec.CodeQuotaExceeded,
		message,
		gin.H{"retry_after": retryAfter},
	)
	ginCtx.Abort()
}

func isOwnTeamDeletionRequest(ginCtx *gin.Context, teamID string) bool {
	return ginCtx != nil &&
		ginCtx.Request != nil &&
		ginCtx.Request.Method == http.MethodDelete &&
		ginCtx.FullPath() == "/teams/:id" &&
		strings.TrimSpace(ginCtx.Param("id")) == strings.TrimSpace(teamID)
}

func isSystemAdminQuotaRepairRequest(
	ginCtx *gin.Context,
	authCtx *authn.AuthContext,
) bool {
	if ginCtx == nil ||
		ginCtx.Request == nil ||
		authCtx == nil ||
		!authCtx.IsSystemAdmin {
		return false
	}
	switch ginCtx.FullPath() {
	case "/api/v1/teams/:team_id/quotas":
		return ginCtx.Request.Method == http.MethodGet
	case "/api/v1/teams/:team_id/quotas/:key":
		return ginCtx.Request.Method == http.MethodPut ||
			ginCtx.Request.Method == http.MethodDelete
	default:
		return false
	}
}

func deletionRetryBucketKey(regionID, teamID string) string {
	return fmt.Sprintf(
		"team-quota:deletion-retry:v1:%d:%s:%d:%s",
		len(regionID),
		regionID,
		len(teamID),
		teamID,
	)
}

// ListCurrent returns effective policies and capacity usage for the current
// authenticated team.
func (c *Controller) ListCurrent(ginCtx *gin.Context) {
	authCtx := gatewaymiddleware.GetAuthContext(ginCtx)
	if authCtx == nil {
		spec.JSONError(ginCtx, http.StatusUnauthorized, spec.CodeUnauthorized, "not authenticated")
		return
	}
	teamID := strings.TrimSpace(authCtx.TeamID)
	if teamID == "" {
		spec.JSONError(ginCtx, http.StatusBadRequest, spec.CodeBadRequest, "team context is required")
		return
	}
	c.list(ginCtx, teamID, false)
}

// ListTeam returns effective policies and capacity usage for an administrator-
// selected team.
func (c *Controller) ListTeam(ginCtx *gin.Context) {
	if !requireSystemAdmin(ginCtx) {
		return
	}
	c.list(ginCtx, strings.TrimSpace(ginCtx.Param("team_id")), true)
}

func (c *Controller) list(ginCtx *gin.Context, teamID string, verifyTeam bool) {
	if teamID == "" {
		spec.JSONError(ginCtx, http.StatusBadRequest, spec.CodeBadRequest, "team_id is required")
		return
	}
	if verifyTeam && !c.requireExistingTeam(ginCtx, teamID) {
		return
	}
	if c == nil || c.policyReader == nil {
		c.abortUnavailable(ginCtx, "team quota policy reader unavailable", nil)
		return
	}
	statuses, err := c.policyReader.ListStatus(ginCtx.Request.Context(), teamID)
	if err != nil {
		c.abortUnavailable(ginCtx, "failed to list team quotas", err)
		return
	}
	if len(statuses) != len(coreteamquota.Keys()) {
		c.abortUnavailable(ginCtx, "effective team quota policy set is incomplete", nil)
		return
	}
	for index := range statuses {
		status := &statuses[index]
		if status.Kind != coreteamquota.KindConcurrency {
			continue
		}
		if c == nil || c.concurrencyLimiter == nil {
			c.abortUnavailable(ginCtx, "team quota concurrency limiter unavailable", nil)
			return
		}
		used, err := c.concurrencyLimiter.Usage(
			ginCtx.Request.Context(),
			teamID,
			status.Key,
		)
		if err != nil {
			c.abortUnavailable(ginCtx, "failed to read live team quota usage", err)
			return
		}
		status.Committed = 0
		status.Reserved = 0
		status.Used = used
		remaining := status.Policy.Limit - used
		if remaining < 0 {
			remaining = 0
		}
		status.Remaining = &remaining
	}
	response := apispec.TeamQuotaList{
		TeamId: teamID,
		Quotas: make([]apispec.TeamQuotaStatus, 0, len(statuses)),
	}
	for _, status := range statuses {
		response.Quotas = append(response.Quotas, statusToAPI(status))
	}
	spec.JSONSuccess(ginCtx, http.StatusOK, response)
}

// PutTeamPolicy replaces one explicit team override.
func (c *Controller) PutTeamPolicy(ginCtx *gin.Context) {
	if !requireSystemAdmin(ginCtx) {
		return
	}
	teamID := strings.TrimSpace(ginCtx.Param("team_id"))
	key := coreteamquota.Key(strings.TrimSpace(ginCtx.Param("key")))
	if teamID == "" {
		spec.JSONError(ginCtx, http.StatusBadRequest, spec.CodeBadRequest, "team_id is required")
		return
	}
	if !coreteamquota.KnownKey(key) {
		spec.JSONError(ginCtx, http.StatusBadRequest, spec.CodeBadRequest, "unknown team quota key")
		return
	}
	if !c.requireExistingTeam(ginCtx, teamID) {
		return
	}

	var request apispec.TeamQuotaPolicyWriteRequest
	if err := ginCtx.ShouldBindJSON(&request); err != nil {
		spec.JSONError(ginCtx, http.StatusBadRequest, spec.CodeBadRequest, "invalid request body")
		return
	}
	policy, err := policyFromWriteRequest(teamID, key, request)
	if err != nil {
		spec.JSONError(ginCtx, http.StatusBadRequest, spec.CodeBadRequest, err.Error())
		return
	}
	if c == nil || c.policyManager == nil {
		c.abortUnavailable(ginCtx, "team quota policy manager unavailable", nil)
		return
	}
	if err := c.policyManager.PutTeamPolicy(ginCtx.Request.Context(), teamID, policy); err != nil {
		if coreteamquota.IsUnavailable(err) {
			c.abortUnavailable(ginCtx, "failed to update team quota policy", err)
			return
		}
		spec.JSONError(ginCtx, http.StatusBadRequest, spec.CodeBadRequest, err.Error())
		return
	}
	if c.limiter != nil {
		c.limiter.Invalidate(teamID, key)
	}
	if c.concurrencyLimiter != nil {
		c.concurrencyLimiter.Invalidate(teamID, key)
	}
	effective, err := c.policyManager.EffectivePolicy(ginCtx.Request.Context(), teamID, key)
	if err != nil || effective == nil {
		c.abortUnavailable(ginCtx, "failed to read updated team quota policy", err)
		return
	}
	spec.JSONSuccess(ginCtx, http.StatusOK, policyToAPI(*effective))
}

// DeleteTeamPolicy removes one explicit team override.
func (c *Controller) DeleteTeamPolicy(ginCtx *gin.Context) {
	if !requireSystemAdmin(ginCtx) {
		return
	}
	teamID := strings.TrimSpace(ginCtx.Param("team_id"))
	key := coreteamquota.Key(strings.TrimSpace(ginCtx.Param("key")))
	if teamID == "" {
		spec.JSONError(ginCtx, http.StatusBadRequest, spec.CodeBadRequest, "team_id is required")
		return
	}
	if !coreteamquota.KnownKey(key) {
		spec.JSONError(ginCtx, http.StatusBadRequest, spec.CodeBadRequest, "unknown team quota key")
		return
	}
	if !c.requireExistingTeam(ginCtx, teamID) {
		return
	}
	if c == nil || c.policyManager == nil {
		c.abortUnavailable(ginCtx, "team quota policy manager unavailable", nil)
		return
	}
	if err := c.policyManager.DeleteTeamPolicy(ginCtx.Request.Context(), teamID, key); err != nil {
		if coreteamquota.IsUnavailable(err) {
			c.abortUnavailable(ginCtx, "failed to delete team quota policy", err)
			return
		}
		spec.JSONError(ginCtx, http.StatusBadRequest, spec.CodeBadRequest, err.Error())
		return
	}
	if c.limiter != nil {
		c.limiter.Invalidate(teamID, key)
	}
	if c.concurrencyLimiter != nil {
		c.concurrencyLimiter.Invalidate(teamID, key)
	}
	spec.JSONSuccess(ginCtx, http.StatusOK, gin.H{"message": "team quota policy deleted"})
}

func (c *Controller) requireExistingTeam(ginCtx *gin.Context, teamID string) bool {
	if c == nil || c.teamLookup == nil {
		c.abortUnavailable(ginCtx, "team directory unavailable", nil)
		return false
	}
	_, err := c.teamLookup.GetTeamByID(ginCtx.Request.Context(), teamID)
	switch {
	case err == nil:
		return true
	case errors.Is(err, identity.ErrTeamNotFound):
		spec.JSONError(ginCtx, http.StatusNotFound, spec.CodeNotFound, "team not found")
		return false
	default:
		c.abortUnavailable(ginCtx, "failed to resolve team", err)
		return false
	}
}

func (c *Controller) abortUnavailable(ginCtx *gin.Context, message string, err error) {
	ginCtx.Header("Retry-After", strconv.Itoa(unavailableRetryAfterSeconds))
	if c != nil && c.logger != nil && err != nil {
		c.logger.Warn(message, zap.Error(err))
	}
	spec.JSONError(ginCtx, http.StatusServiceUnavailable, spec.CodeUnavailable, message)
	ginCtx.Abort()
}

func policyFromWriteRequest(
	teamID string,
	key coreteamquota.Key,
	request apispec.TeamQuotaPolicyWriteRequest,
) (coreteamquota.Policy, error) {
	payload, err := request.MarshalJSON()
	if err != nil {
		return coreteamquota.Policy{}, fmt.Errorf("encode team quota policy request: %w", err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(payload, &fields); err != nil {
		return coreteamquota.Policy{}, fmt.Errorf("decode team quota policy request: %w", err)
	}
	kind, err := requiredWriteRequestString(fields, "kind")
	if err != nil {
		return coreteamquota.Policy{}, err
	}
	policy := coreteamquota.Policy{
		TeamID: teamID,
		Key:    key,
		Kind:   coreteamquota.Kind(kind),
	}
	switch policy.Kind {
	case coreteamquota.KindCapacity, coreteamquota.KindConcurrency:
		if err := requireExactWriteRequestFields(fields, "kind", "limit"); err != nil {
			return coreteamquota.Policy{}, fmt.Errorf("%s policy: %w", policy.Kind, err)
		}
		limit, err := requiredWriteRequestInt64(fields, "limit")
		if err != nil {
			return coreteamquota.Policy{}, err
		}
		policy.Limit = limit
	case coreteamquota.KindRate:
		if err := requireExactWriteRequestFields(fields, "kind", "tokens", "interval_ms", "burst"); err != nil {
			return coreteamquota.Policy{}, fmt.Errorf("rate policy: %w", err)
		}
		tokens, err := requiredWriteRequestInt64(fields, "tokens")
		if err != nil {
			return coreteamquota.Policy{}, err
		}
		intervalMillis, err := requiredWriteRequestInt64(fields, "interval_ms")
		if err != nil {
			return coreteamquota.Policy{}, err
		}
		burst, err := requiredWriteRequestInt64(fields, "burst")
		if err != nil {
			return coreteamquota.Policy{}, err
		}
		policy.Tokens = tokens
		policy.IntervalMillis = intervalMillis
		policy.Burst = burst
	default:
		return coreteamquota.Policy{}, fmt.Errorf("unknown team quota kind %q", kind)
	}
	if err := policy.Validate(); err != nil {
		return coreteamquota.Policy{}, err
	}
	return policy, nil
}

func requireExactWriteRequestFields(fields map[string]json.RawMessage, required ...string) error {
	if len(fields) != len(required) {
		return fmt.Errorf("requires exactly %s", strings.Join(required, ", "))
	}
	for _, name := range required {
		if _, ok := fields[name]; !ok {
			return fmt.Errorf("requires %s", strings.Join(required, ", "))
		}
	}
	return nil
}

func requiredWriteRequestString(fields map[string]json.RawMessage, name string) (string, error) {
	raw, ok := fields[name]
	if !ok {
		return "", fmt.Errorf("%s is required", name)
	}
	var value *string
	if err := json.Unmarshal(raw, &value); err != nil {
		return "", fmt.Errorf("%s must be a string", name)
	}
	if value == nil {
		return "", fmt.Errorf("%s is required", name)
	}
	return *value, nil
}

func requiredWriteRequestInt64(fields map[string]json.RawMessage, name string) (int64, error) {
	raw, ok := fields[name]
	if !ok {
		return 0, fmt.Errorf("%s is required", name)
	}
	var value *int64
	if err := json.Unmarshal(raw, &value); err != nil {
		return 0, fmt.Errorf("%s must be an integer", name)
	}
	if value == nil {
		return 0, fmt.Errorf("%s is required", name)
	}
	return *value, nil
}

func statusToAPI(status coreteamquota.Status) apispec.TeamQuotaStatus {
	var remaining *int64
	if status.Remaining != nil {
		value := *status.Remaining
		remaining = &value
	}
	return apispec.TeamQuotaStatus{
		TeamId:    status.TeamID,
		Key:       apispec.TeamQuotaKey(status.Key),
		Kind:      apispec.TeamQuotaKind(status.Kind),
		Unit:      apispec.TeamQuotaUnit(status.Unit),
		Source:    apispec.TeamQuotaPolicySource(status.Source),
		Policy:    policyToAPI(status.Policy),
		Committed: status.Committed,
		Reserved:  status.Reserved,
		Used:      status.Used,
		Remaining: remaining,
	}
}

func policyToAPI(policy coreteamquota.Policy) apispec.TeamQuotaPolicy {
	revision := policy.Revision
	response := apispec.TeamQuotaPolicy{
		TeamId:   policy.TeamID,
		Key:      apispec.TeamQuotaKey(policy.Key),
		Kind:     apispec.TeamQuotaKind(policy.Kind),
		Unit:     apispec.TeamQuotaUnit(coreteamquota.UnitForKey(policy.Key)),
		Revision: &revision,
	}
	if policy.Kind == coreteamquota.KindCapacity || policy.Kind == coreteamquota.KindConcurrency {
		limit := policy.Limit
		response.Limit = &limit
		return response
	}
	tokens := policy.Tokens
	interval := policy.IntervalMillis
	burst := policy.Burst
	response.Tokens = &tokens
	response.IntervalMs = &interval
	response.Burst = &burst
	return response
}

func retryAfterSeconds(duration time.Duration) int {
	if duration <= 0 {
		return 1
	}
	seconds := int(duration / time.Second)
	if duration%time.Second != 0 {
		seconds++
	}
	if seconds < 1 {
		return 1
	}
	return seconds
}

func requireSystemAdmin(ginCtx *gin.Context) bool {
	authCtx := gatewaymiddleware.GetAuthContext(ginCtx)
	if authCtx == nil {
		spec.JSONError(ginCtx, http.StatusUnauthorized, spec.CodeUnauthorized, "not authenticated")
		return false
	}
	if !authCtx.IsSystemAdmin {
		spec.JSONError(ginCtx, http.StatusForbidden, spec.CodeForbidden, "system admin access required")
		return false
	}
	return true
}
