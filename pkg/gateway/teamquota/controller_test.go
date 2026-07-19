package teamquota

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/sandbox0-ai/sandbox0/pkg/apispec"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/authn"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/identity"
	coreteamquota "github.com/sandbox0-ai/sandbox0/pkg/teamquota"
	"github.com/sandbox0-ai/sandbox0/pkg/tokenbucket"
)

type fakeRateLimiter struct {
	decision    tokenbucket.Decision
	err         error
	calls       int
	lastTeamID  string
	lastKey     coreteamquota.Key
	invalidated bool
	invalidTeam string
	invalidKey  coreteamquota.Key
}

type fakeConcurrencyLimiter struct {
	used        int64
	err         error
	invalidated bool
	invalidTeam string
	invalidKey  coreteamquota.Key
}

type fakeTokenBucket struct {
	decision tokenbucket.Decision
	err      error
	calls    int
	key      string
	policy   tokenbucket.Policy
	cost     int64
}

func (f *fakeTokenBucket) TakeN(
	_ context.Context,
	key string,
	policy tokenbucket.Policy,
	cost int64,
) (tokenbucket.Decision, error) {
	f.calls++
	f.key = key
	f.policy = policy
	f.cost = cost
	return f.decision, f.err
}

func (*fakeTokenBucket) Close() error { return nil }

func (f *fakeConcurrencyLimiter) Acquire(
	context.Context,
	string,
	coreteamquota.Key,
) (ConnectionLease, error) {
	return nil, f.err
}

func (f *fakeConcurrencyLimiter) Usage(
	context.Context,
	string,
	coreteamquota.Key,
) (int64, error) {
	return f.used, f.err
}

func (f *fakeConcurrencyLimiter) Invalidate(teamID string, key coreteamquota.Key) {
	f.invalidated = true
	f.invalidTeam = teamID
	f.invalidKey = key
}

func (*fakeConcurrencyLimiter) Close() error { return nil }

func (f *fakeRateLimiter) Take(
	_ context.Context,
	teamID string,
	key coreteamquota.Key,
	_ int64,
) (tokenbucket.Decision, error) {
	f.calls++
	f.lastTeamID = teamID
	f.lastKey = key
	return f.decision, f.err
}

func (f *fakeRateLimiter) Invalidate(teamID string, key coreteamquota.Key) {
	f.invalidated = true
	f.invalidTeam = teamID
	f.invalidKey = key
}

type fakePolicyManager struct {
	statuses     []coreteamquota.Status
	listErr      error
	putPolicy    *coreteamquota.Policy
	putErr       error
	deleteKey    coreteamquota.Key
	deleteErr    error
	effective    *coreteamquota.Policy
	effectiveErr error
}

func (f *fakePolicyManager) ListStatus(context.Context, string) ([]coreteamquota.Status, error) {
	return f.statuses, f.listErr
}

func (f *fakePolicyManager) PutTeamPolicy(
	_ context.Context,
	_ string,
	policy coreteamquota.Policy,
) error {
	f.putPolicy = &policy
	return f.putErr
}

func (f *fakePolicyManager) DeleteTeamPolicy(
	_ context.Context,
	_ string,
	key coreteamquota.Key,
) error {
	f.deleteKey = key
	return f.deleteErr
}

func (*fakePolicyManager) ReplaceDefaultPoliciesVersioned(
	context.Context,
	[]coreteamquota.Policy,
	coreteamquota.DefaultPolicyVersion,
) error {
	return nil
}

func (f *fakePolicyManager) EffectivePolicy(
	context.Context,
	string,
	coreteamquota.Key,
) (*coreteamquota.Policy, error) {
	return f.effective, f.effectiveErr
}

type readOnlyPolicyReader struct {
	reader coreteamquota.PolicyReader
}

func (r readOnlyPolicyReader) ListStatus(
	ctx context.Context,
	teamID string,
) ([]coreteamquota.Status, error) {
	return r.reader.ListStatus(ctx, teamID)
}

func (r readOnlyPolicyReader) EffectivePolicy(
	ctx context.Context,
	teamID string,
	key coreteamquota.Key,
) (*coreteamquota.Policy, error) {
	return r.reader.EffectivePolicy(ctx, teamID, key)
}

type fakeTeamLookup struct {
	err error
}

func (f fakeTeamLookup) GetTeamByID(context.Context, string) (*identity.Team, error) {
	if f.err != nil {
		return nil, f.err
	}
	return &identity.Team{}, nil
}

type fakeRetentionStore struct {
	*fakePolicyManager
	teamIDs []string
	pruned  []string
}

func (f *fakeRetentionStore) ListDeletedTeamTombstones(
	_ context.Context,
	_ time.Time,
	after *coreteamquota.DeletedTeamTombstone,
	limit int,
) ([]coreteamquota.DeletedTeamTombstone, error) {
	tombstones := make([]coreteamquota.DeletedTeamTombstone, 0, min(limit, len(f.teamIDs)))
	for i, teamID := range f.teamIDs {
		tombstone := coreteamquota.DeletedTeamTombstone{
			TeamID:    teamID,
			DeletedAt: time.Unix(int64(i+1), 0).UTC(),
		}
		if after != nil && !tombstone.DeletedAt.After(after.DeletedAt) {
			continue
		}
		tombstones = append(tombstones, tombstone)
		if len(tombstones) == limit {
			break
		}
	}
	return tombstones, nil
}

func (f *fakeRetentionStore) PruneDeletedTeamTombstone(
	_ context.Context,
	teamID string,
	_ time.Time,
) (bool, error) {
	f.pruned = append(f.pruned, teamID)
	return true, nil
}

type retentionTeamLookup struct {
	results map[string]error
}

type fakeAtomicAdmissionMarker struct {
	forgotten []string
	forgetErr error
}

func (*fakeAtomicAdmissionMarker) Disabled(context.Context, string) (bool, error) {
	return false, nil
}

func (*fakeAtomicAdmissionMarker) Disable(context.Context, string) error { return nil }

func (*fakeAtomicAdmissionMarker) RedisKey(string) (string, error) {
	return "test:admission", nil
}

func (*fakeAtomicAdmissionMarker) Recover(context.Context, string) error { return nil }

func (m *fakeAtomicAdmissionMarker) Forget(_ context.Context, teamID string) error {
	if m.forgetErr != nil {
		return m.forgetErr
	}
	m.forgotten = append(m.forgotten, teamID)
	return nil
}

func (*fakeAtomicAdmissionMarker) Close() error { return nil }

func (f retentionTeamLookup) GetTeamByID(
	_ context.Context,
	teamID string,
) (*identity.Team, error) {
	if err := f.results[teamID]; err != nil {
		return nil, err
	}
	return &identity.Team{ID: teamID}, nil
}

func TestDeletedTeamTombstoneCleanupRequiresMissingIdentity(t *testing.T) {
	store := &fakeRetentionStore{
		fakePolicyManager: &fakePolicyManager{},
		teamIDs:           []string{"missing", "present", "unavailable"},
	}
	controller := NewController(
		store,
		retentionTeamLookup{results: map[string]error{
			"missing":     identity.ErrTeamNotFound,
			"unavailable": errors.New("identity database unavailable"),
		}},
		nil,
		nil,
		nil,
	)
	marker := &fakeAtomicAdmissionMarker{}
	controller.marker = marker

	controller.cleanupDeletedTeamTombstones(
		context.Background(),
		store,
		time.Hour,
	)
	if len(store.pruned) != 1 || store.pruned[0] != "missing" {
		t.Fatalf("pruned tombstones = %v, want only missing identity", store.pruned)
	}
	if len(marker.forgotten) != 1 || marker.forgotten[0] != "missing" {
		t.Fatalf("forgotten Redis markers = %v, want only missing identity", marker.forgotten)
	}
}

func TestDeletedTeamTombstoneCleanupKeepsPostgresWhenMarkerForgetFails(t *testing.T) {
	store := &fakeRetentionStore{
		fakePolicyManager: &fakePolicyManager{},
		teamIDs:           []string{"missing"},
	}
	controller := NewController(
		store,
		retentionTeamLookup{results: map[string]error{
			"missing": identity.ErrTeamNotFound,
		}},
		nil,
		nil,
		nil,
	)
	controller.marker = &fakeAtomicAdmissionMarker{
		forgetErr: errors.New("redis unavailable"),
	}
	controller.cleanupDeletedTeamTombstones(context.Background(), store, time.Hour)
	if len(store.pruned) != 0 {
		t.Fatalf("pruned tombstones = %v, want none after Redis failure", store.pruned)
	}
}

func TestDeletedTeamTombstoneCleanupPagesPastRetainedIdentities(t *testing.T) {
	teamIDs := make([]string, deletedTeamCleanupBatchSize+1)
	for i := range teamIDs {
		teamIDs[i] = fmt.Sprintf("team-%04d", i)
	}
	last := teamIDs[len(teamIDs)-1]
	store := &fakeRetentionStore{
		fakePolicyManager: &fakePolicyManager{},
		teamIDs:           teamIDs,
	}
	controller := NewController(
		store,
		retentionTeamLookup{results: map[string]error{last: identity.ErrTeamNotFound}},
		nil,
		nil,
		nil,
	)

	controller.cleanupDeletedTeamTombstones(context.Background(), store, time.Hour)

	if len(store.pruned) != 1 || store.pruned[0] != last {
		t.Fatalf("pruned tombstones = %v, want only %s", store.pruned, last)
	}
}

func TestDeletedTeamTombstoneRetentionCoversAccessTokenLifetime(t *testing.T) {
	tests := []struct {
		name      string
		accessTTL time.Duration
		want      time.Duration
	}{
		{
			name:      "short token uses minimum",
			accessTTL: 15 * time.Minute,
			want:      time.Hour,
		},
		{
			name:      "long token adds safety margin",
			accessTTL: 48 * time.Hour,
			want:      48*time.Hour + 5*time.Minute,
		},
		{
			name: "unknown verifier lifetime is conservative",
			want: 30 * 24 * time.Hour,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := DeletedTeamTombstoneRetention(test.accessTTL); got != test.want {
				t.Fatalf("DeletedTeamTombstoneRetention() = %s, want %s", got, test.want)
			}
		})
	}
}

func TestRateLimitAPIRequestsFailsClosedAndChargesUnprovenInternal(t *testing.T) {
	gin.SetMode(gin.TestMode)
	tests := []struct {
		name                string
		auth                *authn.AuthContext
		trustForwardedProof bool
		limiter             *fakeRateLimiter
		wantStatus          int
		wantCalls           int
		wantRetry           string
	}{
		{
			name:       "allowed team",
			auth:       &authn.AuthContext{TeamID: "team-a", AuthMethod: authn.AuthMethodAPIKey},
			limiter:    &fakeRateLimiter{decision: tokenbucket.Decision{Allowed: true, Remaining: 8}},
			wantStatus: http.StatusNoContent,
			wantCalls:  1,
		},
		{
			name:       "denied",
			auth:       &authn.AuthContext{TeamID: "team-a", AuthMethod: authn.AuthMethodAPIKey},
			limiter:    &fakeRateLimiter{decision: tokenbucket.Decision{RetryAfter: 1100 * time.Millisecond}},
			wantStatus: http.StatusTooManyRequests,
			wantCalls:  1,
			wantRetry:  "2",
		},
		{
			name:       "backend unavailable",
			auth:       &authn.AuthContext{TeamID: "team-a", AuthMethod: authn.AuthMethodAPIKey},
			limiter:    &fakeRateLimiter{err: errors.New("redis unavailable")},
			wantStatus: http.StatusServiceUnavailable,
			wantCalls:  1,
			wantRetry:  "1",
		},
		{
			name:       "limiter missing",
			auth:       &authn.AuthContext{TeamID: "team-a", AuthMethod: authn.AuthMethodAPIKey},
			wantStatus: http.StatusServiceUnavailable,
			wantRetry:  "1",
		},
		{
			name:       "team missing",
			auth:       &authn.AuthContext{AuthMethod: authn.AuthMethodAPIKey},
			limiter:    &fakeRateLimiter{decision: tokenbucket.Decision{Allowed: true}},
			wantStatus: http.StatusServiceUnavailable,
			wantRetry:  "1",
		},
		{
			name:                "unproven internal request",
			auth:                &authn.AuthContext{TeamID: "team-a", AuthMethod: authn.AuthMethodInternal},
			trustForwardedProof: true,
			limiter:             &fakeRateLimiter{decision: tokenbucket.Decision{Allowed: true}},
			wantStatus:          http.StatusNoContent,
			wantCalls:           1,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var limiter RateLimiter
			if test.limiter != nil {
				limiter = test.limiter
			}
			controller := NewController(nil, nil, limiter, nil, nil)
			router := gin.New()
			router.Use(func(c *gin.Context) {
				if test.auth != nil {
					c.Set("auth_context", test.auth)
					c.Request = c.Request.WithContext(authn.WithAuthContext(c.Request.Context(), test.auth))
				}
				c.Next()
			})
			router.Use(controller.RateLimitAPIRequests(test.trustForwardedProof))
			router.GET("/test", func(c *gin.Context) { c.Status(http.StatusNoContent) })

			recorder := httptest.NewRecorder()
			router.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/test", nil))
			if recorder.Code != test.wantStatus {
				t.Fatalf("status = %d, want %d; body=%s", recorder.Code, test.wantStatus, recorder.Body.String())
			}
			if test.limiter != nil && test.limiter.calls != test.wantCalls {
				t.Fatalf("limiter calls = %d, want %d", test.limiter.calls, test.wantCalls)
			}
			if got := recorder.Header().Get("Retry-After"); got != test.wantRetry {
				t.Fatalf("Retry-After = %q, want %q", got, test.wantRetry)
			}
		})
	}
}

func TestRateLimitAPIRequestsDeletionRetryIsNarrowAndBounded(t *testing.T) {
	gin.SetMode(gin.TestMode)
	disabled := &coreteamquota.UnavailableError{
		Operation: "take api request",
		Err:       &coreteamquota.TeamAdmissionDisabledError{TeamID: "team-a"},
	}
	tests := []struct {
		name          string
		auth          *authn.AuthContext
		targetTeamID  string
		limiter       *fakeRateLimiter
		bucket        *fakeTokenBucket
		wantStatus    int
		wantHandler   bool
		wantRateCalls int
		wantRetries   int
	}{
		{
			name:          "active own team uses normal quota",
			auth:          &authn.AuthContext{TeamID: "team-a", AuthMethod: authn.AuthMethodAPIKey},
			targetTeamID:  "team-a",
			limiter:       &fakeRateLimiter{decision: tokenbucket.Decision{Allowed: true}},
			bucket:        &fakeTokenBucket{},
			wantStatus:    http.StatusNoContent,
			wantHandler:   true,
			wantRateCalls: 1,
		},
		{
			name:          "disabled own team uses bounded retry",
			auth:          &authn.AuthContext{TeamID: "team-a", AuthMethod: authn.AuthMethodAPIKey},
			targetTeamID:  "team-a",
			limiter:       &fakeRateLimiter{err: disabled},
			bucket:        &fakeTokenBucket{decision: tokenbucket.Decision{Allowed: true, Remaining: 2}},
			wantStatus:    http.StatusNoContent,
			wantHandler:   true,
			wantRateCalls: 1,
			wantRetries:   1,
		},
		{
			name:          "disabled other team cannot retry",
			auth:          &authn.AuthContext{TeamID: "team-a", AuthMethod: authn.AuthMethodAPIKey},
			targetTeamID:  "team-b",
			limiter:       &fakeRateLimiter{err: disabled},
			bucket:        &fakeTokenBucket{decision: tokenbucket.Decision{Allowed: true}},
			wantStatus:    http.StatusServiceUnavailable,
			wantRateCalls: 1,
		},
		{
			name:          "ordinary backend failure cannot retry",
			auth:          &authn.AuthContext{TeamID: "team-a", AuthMethod: authn.AuthMethodAPIKey},
			targetTeamID:  "team-a",
			limiter:       &fakeRateLimiter{err: errors.New("redis unavailable")},
			bucket:        &fakeTokenBucket{decision: tokenbucket.Decision{Allowed: true}},
			wantStatus:    http.StatusServiceUnavailable,
			wantRateCalls: 1,
		},
		{
			name:          "ordinary rate denial cannot retry",
			auth:          &authn.AuthContext{TeamID: "team-a", AuthMethod: authn.AuthMethodAPIKey},
			targetTeamID:  "team-a",
			limiter:       &fakeRateLimiter{decision: tokenbucket.Decision{RetryAfter: time.Second}},
			bucket:        &fakeTokenBucket{decision: tokenbucket.Decision{Allowed: true}},
			wantStatus:    http.StatusTooManyRequests,
			wantRateCalls: 1,
		},
		{
			name:          "disabled retry bucket denies",
			auth:          &authn.AuthContext{TeamID: "team-a", AuthMethod: authn.AuthMethodAPIKey},
			targetTeamID:  "team-a",
			limiter:       &fakeRateLimiter{err: disabled},
			bucket:        &fakeTokenBucket{decision: tokenbucket.Decision{RetryAfter: 500 * time.Millisecond}},
			wantStatus:    http.StatusTooManyRequests,
			wantRateCalls: 1,
			wantRetries:   1,
		},
		{
			name:          "disabled retry backend fails closed",
			auth:          &authn.AuthContext{TeamID: "team-a", AuthMethod: authn.AuthMethodAPIKey},
			targetTeamID:  "team-a",
			limiter:       &fakeRateLimiter{err: disabled},
			bucket:        &fakeTokenBucket{err: errors.New("redis unavailable")},
			wantStatus:    http.StatusServiceUnavailable,
			wantRateCalls: 1,
			wantRetries:   1,
		},
		{
			name:         "missing authenticated team cannot retry",
			auth:         &authn.AuthContext{AuthMethod: authn.AuthMethodAPIKey},
			targetTeamID: "team-a",
			limiter:      &fakeRateLimiter{err: disabled},
			bucket:       &fakeTokenBucket{decision: tokenbucket.Decision{Allowed: true}},
			wantStatus:   http.StatusServiceUnavailable,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			controller := NewController(
				nil,
				nil,
				test.limiter,
				test.bucket,
				nil,
				WithRegionID("region-a"),
				WithNetworkLimiter(&trafficNetworkLimiter{}),
			)
			router := gin.New()
			router.Use(func(c *gin.Context) {
				if test.auth != nil {
					c.Set("auth_context", test.auth)
				}
				c.Next()
			})
			router.Use(controller.AdmitEdgeTraffic(false))
			router.Use(controller.RateLimitAPIRequests(false))
			handlerCalled := false
			router.DELETE("/teams/:id", func(c *gin.Context) {
				handlerCalled = true
				c.Status(http.StatusNoContent)
			})

			recorder := httptest.NewRecorder()
			router.ServeHTTP(
				recorder,
				httptest.NewRequest(http.MethodDelete, "/teams/"+test.targetTeamID, nil),
			)
			if recorder.Code != test.wantStatus {
				t.Fatalf("status = %d, want %d; body=%s", recorder.Code, test.wantStatus, recorder.Body.String())
			}
			if handlerCalled != test.wantHandler {
				t.Fatalf("handler called = %t, want %t", handlerCalled, test.wantHandler)
			}
			if test.limiter.calls != test.wantRateCalls {
				t.Fatalf("normal limiter calls = %d, want %d", test.limiter.calls, test.wantRateCalls)
			}
			if test.bucket.calls != test.wantRetries {
				t.Fatalf("retry bucket calls = %d, want %d", test.bucket.calls, test.wantRetries)
			}
			if test.wantRetries == 1 {
				if test.bucket.key != deletionRetryBucketKey("region-a", "team-a") {
					t.Fatalf("retry bucket key = %q", test.bucket.key)
				}
				if test.bucket.policy.Tokens != 1 ||
					test.bucket.policy.Interval != time.Second ||
					test.bucket.policy.Burst != 3 ||
					test.bucket.cost != 1 {
					t.Fatalf("retry bucket policy/cost = %#v/%d", test.bucket.policy, test.bucket.cost)
				}
			}
		})
	}
}

func TestRateLimitAPIRequestsDeletionRetryRequiresEdgeTrafficLane(t *testing.T) {
	disabled := &coreteamquota.UnavailableError{
		Operation: "take api request",
		Err:       &coreteamquota.TeamAdmissionDisabledError{TeamID: "team-a"},
	}
	bucket := &fakeTokenBucket{
		decision: tokenbucket.Decision{Allowed: true},
	}
	controller := NewController(
		nil,
		nil,
		&fakeRateLimiter{err: disabled},
		bucket,
		nil,
		WithRegionID("region-a"),
	)
	router := gin.New()
	router.Use(func(c *gin.Context) {
		c.Set("auth_context", &authn.AuthContext{
			TeamID:     "team-a",
			AuthMethod: authn.AuthMethodAPIKey,
		})
		c.Next()
	})
	router.Use(controller.RateLimitAPIRequests(false))
	handlerCalled := false
	router.DELETE("/teams/:id", func(c *gin.Context) {
		handlerCalled = true
		c.Status(http.StatusNoContent)
	})

	recorder := httptest.NewRecorder()
	router.ServeHTTP(
		recorder,
		httptest.NewRequest(http.MethodDelete, "/teams/team-a", nil),
	)

	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503; body=%s", recorder.Code, recorder.Body.String())
	}
	if handlerCalled {
		t.Fatal("deletion retry handler ran without bounded edge traffic lane")
	}
	if bucket.calls != 0 {
		t.Fatalf("retry bucket calls = %d, want 0", bucket.calls)
	}
}

func TestPutTeamPolicyValidatesShapeAndInvalidatesDistributedCaches(t *testing.T) {
	gin.SetMode(gin.TestMode)
	effective := coreteamquota.Policy{
		TeamID:         "team-a",
		Key:            coreteamquota.KeyAPIRequests,
		Kind:           coreteamquota.KindRate,
		Revision:       19,
		Tokens:         10,
		IntervalMillis: 1000,
		Burst:          20,
	}
	manager := &fakePolicyManager{effective: &effective}
	limiter := &fakeRateLimiter{}
	concurrencyLimiter := &fakeConcurrencyLimiter{}
	controller := NewController(
		manager,
		fakeTeamLookup{},
		limiter,
		nil,
		nil,
		WithConcurrencyLimiter(concurrencyLimiter),
		WithPolicyManager(manager),
	)
	router := gin.New()
	router.Use(func(c *gin.Context) {
		c.Set("auth_context", &authn.AuthContext{IsSystemAdmin: true})
		c.Next()
	})
	router.PUT("/teams/:team_id/quotas/:key", controller.PutTeamPolicy)

	body := bytes.NewBufferString(`{"kind":"rate","tokens":10,"interval_ms":1000,"burst":20}`)
	recorder := httptest.NewRecorder()
	router.ServeHTTP(
		recorder,
		httptest.NewRequest(http.MethodPut, "/teams/team-a/quotas/api_requests", body),
	)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", recorder.Code, recorder.Body.String())
	}
	if manager.putPolicy == nil || manager.putPolicy.Tokens != 10 || manager.putPolicy.Burst != 20 {
		t.Fatalf("stored policy = %#v", manager.putPolicy)
	}
	if !limiter.invalidated ||
		limiter.invalidTeam != "team-a" ||
		limiter.invalidKey != coreteamquota.KeyAPIRequests {
		t.Fatalf("cache invalidation = %#v", limiter)
	}
	if !concurrencyLimiter.invalidated ||
		concurrencyLimiter.invalidTeam != "team-a" ||
		concurrencyLimiter.invalidKey != coreteamquota.KeyAPIRequests {
		t.Fatalf("concurrency cache invalidation = %#v", concurrencyLimiter)
	}
	if !strings.Contains(recorder.Body.String(), `"revision":19`) {
		t.Fatalf("response missing effective revision: %s", recorder.Body.String())
	}
	if !strings.Contains(recorder.Body.String(), `"unit":"requests"`) {
		t.Fatalf("response missing canonical unit: %s", recorder.Body.String())
	}

	for _, invalidBody := range []string{
		`{"kind":"capacity"}`,
		`{"kind":"capacity","limit":null}`,
		`{"kind":"capacity","limit":0,"tokens":1}`,
		`{"kind":"capacity","limit":1.5}`,
		`{"kind":"rate","tokens":1,"interval_ms":1000,"burst":1}`,
	} {
		invalid := httptest.NewRecorder()
		router.ServeHTTP(
			invalid,
			httptest.NewRequest(
				http.MethodPut,
				"/teams/team-a/quotas/sandbox_runtime_count",
				bytes.NewBufferString(invalidBody),
			),
		)
		if invalid.Code != http.StatusBadRequest {
			t.Fatalf(
				"body %s returned status = %d, want 400; response=%s",
				invalidBody,
				invalid.Code,
				invalid.Body.String(),
			)
		}
	}
}

func TestPutTeamPolicyDoesNotInferWriterAuthorityFromPolicyReader(t *testing.T) {
	gin.SetMode(gin.TestMode)
	manager := &fakePolicyManager{}
	controller := NewController(
		readOnlyPolicyReader{reader: manager},
		fakeTeamLookup{},
		nil,
		nil,
		nil,
	)
	router := gin.New()
	router.Use(func(c *gin.Context) {
		c.Set("auth_context", &authn.AuthContext{IsSystemAdmin: true})
		c.Next()
	})
	router.PUT("/teams/:team_id/quotas/:key", controller.PutTeamPolicy)

	response := httptest.NewRecorder()
	router.ServeHTTP(
		response,
		httptest.NewRequest(
			http.MethodPut,
			"/teams/team-a/quotas/sandbox_runtime_count",
			bytes.NewBufferString(`{"kind":"capacity","limit":10}`),
		),
	)

	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503; body=%s", response.Code, response.Body.String())
	}
	if manager.putPolicy != nil {
		t.Fatalf("policy reader unexpectedly gained writer authority: %#v", manager.putPolicy)
	}
}

func TestTeamQuotaAPIKeyEnumMatchesCoreRegistry(t *testing.T) {
	apiKeys := []apispec.TeamQuotaKey{
		apispec.SandboxIdentityCount,
		apispec.SandboxRuntimeCount,
		apispec.SandboxCpuMillicores,
		apispec.SandboxMemoryBytes,
		apispec.SandboxEphemeralStorageBytes,
		apispec.VolumeStorageBytes,
		apispec.SnapshotStorageBytes,
		apispec.RootfsStorageBytes,
		apispec.TemplateImageStorageBytes,
		apispec.StorageObjectCount,
		apispec.ControlPlaneObjectCount,
		apispec.ActiveConnectionCount,
		apispec.ActiveRequestCount,
		apispec.ApiRequests,
		apispec.SandboxServiceRequests,
		apispec.SandboxStarts,
		apispec.NetworkOperations,
		apispec.NetworkIngressBytes,
		apispec.NetworkEgressBytes,
		apispec.StorageOperations,
		apispec.ObservabilityIngestBytes,
	}
	if len(apiKeys) != len(coreteamquota.Keys()) {
		t.Fatalf("API keys = %d, core keys = %d", len(apiKeys), len(coreteamquota.Keys()))
	}
	seen := make(map[coreteamquota.Key]struct{}, len(apiKeys))
	for _, key := range apiKeys {
		seen[coreteamquota.Key(key)] = struct{}{}
	}
	for _, key := range coreteamquota.Keys() {
		if _, ok := seen[key]; !ok {
			t.Fatalf("API enum missing core Team Quota key %q", key)
		}
	}
}

func TestListCurrentReturnsAllPoliciesAndNullableRateRemaining(t *testing.T) {
	gin.SetMode(gin.TestMode)
	store := &fakePolicyManager{statuses: completeStatuses("team-a")}
	controller := NewController(
		store,
		nil,
		&fakeRateLimiter{},
		nil,
		nil,
		WithConcurrencyLimiter(&fakeConcurrencyLimiter{}),
	)
	router := gin.New()
	router.Use(func(c *gin.Context) {
		authCtx := &authn.AuthContext{
			TeamID:      "team-a",
			AuthMethod:  authn.AuthMethodAPIKey,
			Permissions: []string{authn.PermQuotaRead},
		}
		c.Set("auth_context", authCtx)
		c.Next()
	})
	router.GET("/quotas", controller.ListCurrent)

	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/quotas", nil))
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", recorder.Code, recorder.Body.String())
	}
	if !strings.Contains(recorder.Body.String(), `"team_id":"team-a"`) {
		t.Fatalf("response missing team: %s", recorder.Body.String())
	}
	if !strings.Contains(recorder.Body.String(), `"remaining":null`) {
		t.Fatalf("rate remaining must be explicit null: %s", recorder.Body.String())
	}
	if !strings.Contains(recorder.Body.String(), `"source":"default"`) {
		t.Fatalf("response missing policy source: %s", recorder.Body.String())
	}
}

func TestStatusToAPIPreservesPolicySource(t *testing.T) {
	for _, source := range []coreteamquota.PolicySource{
		coreteamquota.PolicySourceDefault,
		coreteamquota.PolicySourceOverride,
	} {
		status := coreteamquota.Status{
			TeamID: "team-a",
			Key:    coreteamquota.KeySandboxRuntimeCount,
			Kind:   coreteamquota.KindCapacity,
			Unit:   "count",
			Source: source,
			Policy: coreteamquota.Policy{
				TeamID:   "team-a",
				Key:      coreteamquota.KeySandboxRuntimeCount,
				Kind:     coreteamquota.KindCapacity,
				Revision: 1,
				Limit:    10,
			},
		}
		got := statusToAPI(status)
		if got.Source != apispec.TeamQuotaPolicySource(source) {
			t.Fatalf("statusToAPI() source = %q, want %q", got.Source, source)
		}
	}
}

func completeStatuses(teamID string) []coreteamquota.Status {
	statuses := make([]coreteamquota.Status, 0, len(coreteamquota.Keys()))
	for _, key := range coreteamquota.Keys() {
		kind, _ := coreteamquota.KindForKey(key)
		policy := coreteamquota.Policy{
			TeamID:   teamID,
			Key:      key,
			Kind:     kind,
			Revision: 1,
		}
		status := coreteamquota.Status{
			TeamID: teamID,
			Key:    key,
			Kind:   kind,
			Unit:   coreteamquota.UnitForKey(key),
			Source: coreteamquota.PolicySourceDefault,
			Policy: policy,
		}
		if kind == coreteamquota.KindCapacity {
			policy.Limit = 100
			status.Policy = policy
			remaining := int64(100)
			status.Remaining = &remaining
		} else {
			policy.Tokens = 10
			policy.IntervalMillis = 1000
			policy.Burst = 20
			status.Policy = policy
		}
		statuses = append(statuses, status)
	}
	return statuses
}
