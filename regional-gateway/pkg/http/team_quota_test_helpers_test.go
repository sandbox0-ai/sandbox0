package http

import (
	"context"
	"io"
	"sync"
	"sync/atomic"

	gatewayteamquota "github.com/sandbox0-ai/sandbox0/pkg/gateway/teamquota"
	coreteamquota "github.com/sandbox0-ai/sandbox0/pkg/teamquota"
	"github.com/sandbox0-ai/sandbox0/pkg/teamquota/guard"
	"github.com/sandbox0-ai/sandbox0/pkg/tokenbucket"
	"go.uber.org/zap"
)

type allowingTeamQuotaRateLimiter struct{}

type allowingAdmissionProofConsumer struct{}

func (allowingAdmissionProofConsumer) CurrentVersion(
	context.Context,
) (guard.Version, error) {
	return guard.Version{EnforcementEpoch: 1, RedisGeneration: 1}, nil
}

func (allowingAdmissionProofConsumer) Consume(
	context.Context,
	string,
	string,
	int64,
	int64,
	guard.Version,
) (bool, error) {
	return true, nil
}

func (allowingAdmissionProofConsumer) Close() error { return nil }

func (allowingTeamQuotaRateLimiter) Take(
	context.Context,
	string,
	coreteamquota.Key,
	int64,
) (tokenbucket.Decision, error) {
	return tokenbucket.Decision{Allowed: true, Remaining: 100}, nil
}

func (allowingTeamQuotaRateLimiter) Invalidate(string, coreteamquota.Key) {}

func newAllowingTeamQuotaController(logger *zap.Logger) *gatewayteamquota.Controller {
	return gatewayteamquota.NewController(
		nil,
		nil,
		allowingTeamQuotaRateLimiter{},
		nil,
		logger,
		gatewayteamquota.WithConcurrencyLimiter(allowingTeamQuotaConcurrencyLimiter{}),
		gatewayteamquota.WithNetworkLimiter(allowingTeamQuotaNetworkLimiter{}),
		gatewayteamquota.WithAdmissionProofConsumer(allowingAdmissionProofConsumer{}),
	)
}

type allowingTeamQuotaConcurrencyLimiter struct{}

func (allowingTeamQuotaConcurrencyLimiter) Acquire(
	context.Context,
	string,
	coreteamquota.Key,
) (gatewayteamquota.ConnectionLease, error) {
	return newAllowingTeamQuotaLease(), nil
}

func (allowingTeamQuotaConcurrencyLimiter) Usage(
	context.Context,
	string,
	coreteamquota.Key,
) (int64, error) {
	return 0, nil
}

func (allowingTeamQuotaConcurrencyLimiter) Invalidate(string, coreteamquota.Key) {}
func (allowingTeamQuotaConcurrencyLimiter) Close() error                         { return nil }

type allowingTeamQuotaLease struct {
	done chan struct{}
	once sync.Once
}

func newAllowingTeamQuotaLease() *allowingTeamQuotaLease {
	return &allowingTeamQuotaLease{done: make(chan struct{})}
}

func (l *allowingTeamQuotaLease) Done() <-chan struct{} { return l.done }
func (*allowingTeamQuotaLease) Err() error              { return nil }
func (l *allowingTeamQuotaLease) Release(context.Context) error {
	l.once.Do(func() { close(l.done) })
	return nil
}

type allowingTeamQuotaNetworkLimiter struct{}

func (allowingTeamQuotaNetworkLimiter) WaitN(
	context.Context,
	string,
	coreteamquota.Key,
	int,
) error {
	return nil
}

func (allowingTeamQuotaNetworkLimiter) Reader(
	_ context.Context,
	_ string,
	_ coreteamquota.Key,
	reader io.Reader,
) io.Reader {
	return reader
}

func (allowingTeamQuotaNetworkLimiter) Close() error { return nil }

type countingTeamQuotaRateLimiter struct {
	calls    atomic.Int64
	decision tokenbucket.Decision
}

func (l *countingTeamQuotaRateLimiter) Take(
	context.Context,
	string,
	coreteamquota.Key,
	int64,
) (tokenbucket.Decision, error) {
	l.calls.Add(1)
	return l.decision, nil
}

func (*countingTeamQuotaRateLimiter) Invalidate(string, coreteamquota.Key) {}

func (l *countingTeamQuotaRateLimiter) Calls() int64 {
	return l.calls.Load()
}

func newDenyingTeamQuotaController(
	logger *zap.Logger,
) (
	*gatewayteamquota.Controller,
	*countingTeamQuotaRateLimiter,
	*recordingTeamQuotaNetworkLimiter,
) {
	limiter := &countingTeamQuotaRateLimiter{}
	networkLimiter := &recordingTeamQuotaNetworkLimiter{}
	return gatewayteamquota.NewController(
		nil,
		nil,
		limiter,
		nil,
		logger,
		gatewayteamquota.WithConcurrencyLimiter(allowingTeamQuotaConcurrencyLimiter{}),
		gatewayteamquota.WithNetworkLimiter(networkLimiter),
		gatewayteamquota.WithAdmissionProofConsumer(allowingAdmissionProofConsumer{}),
	), limiter, networkLimiter
}

type recordingTeamQuotaConcurrencyLimiter struct {
	acquireCalls atomic.Int64
	releaseCalls atomic.Int64
	err          error
}

func (l *recordingTeamQuotaConcurrencyLimiter) Acquire(
	context.Context,
	string,
	coreteamquota.Key,
) (gatewayteamquota.ConnectionLease, error) {
	l.acquireCalls.Add(1)
	if l.err != nil {
		return nil, l.err
	}
	return &recordingTeamQuotaLease{
		done:  make(chan struct{}),
		owner: l,
	}, nil
}

func (*recordingTeamQuotaConcurrencyLimiter) Usage(
	context.Context,
	string,
	coreteamquota.Key,
) (int64, error) {
	return 0, nil
}

func (*recordingTeamQuotaConcurrencyLimiter) Invalidate(string, coreteamquota.Key) {}
func (*recordingTeamQuotaConcurrencyLimiter) Close() error                         { return nil }

func (l *recordingTeamQuotaConcurrencyLimiter) AcquireCalls() int64 {
	return l.acquireCalls.Load()
}

func (l *recordingTeamQuotaConcurrencyLimiter) ReleaseCalls() int64 {
	return l.releaseCalls.Load()
}

type recordingTeamQuotaLease struct {
	done  chan struct{}
	owner *recordingTeamQuotaConcurrencyLimiter
	once  sync.Once
}

func (l *recordingTeamQuotaLease) Done() <-chan struct{} { return l.done }
func (*recordingTeamQuotaLease) Err() error              { return nil }
func (l *recordingTeamQuotaLease) Release(context.Context) error {
	l.once.Do(func() {
		l.owner.releaseCalls.Add(1)
		close(l.done)
	})
	return nil
}

type recordingTeamQuotaNetworkLimiter struct {
	mu     sync.Mutex
	totals map[coreteamquota.Key]int64
}

func (l *recordingTeamQuotaNetworkLimiter) WaitN(
	_ context.Context,
	_ string,
	key coreteamquota.Key,
	bytes int,
) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.totals == nil {
		l.totals = make(map[coreteamquota.Key]int64)
	}
	l.totals[key] += int64(bytes)
	return nil
}

func (l *recordingTeamQuotaNetworkLimiter) Reader(
	ctx context.Context,
	teamID string,
	key coreteamquota.Key,
	reader io.Reader,
) io.Reader {
	return &recordingTeamQuotaReader{
		ctx:     ctx,
		teamID:  teamID,
		key:     key,
		reader:  reader,
		limiter: l,
	}
}

func (*recordingTeamQuotaNetworkLimiter) Close() error { return nil }

func (l *recordingTeamQuotaNetworkLimiter) Total(key coreteamquota.Key) int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.totals[key]
}

type recordingTeamQuotaReader struct {
	ctx     context.Context
	teamID  string
	key     coreteamquota.Key
	reader  io.Reader
	limiter *recordingTeamQuotaNetworkLimiter
}

func (r *recordingTeamQuotaReader) Read(payload []byte) (int, error) {
	n, err := r.reader.Read(payload)
	if n > 0 {
		if quotaErr := r.limiter.WaitN(r.ctx, r.teamID, r.key, n); quotaErr != nil {
			return 0, quotaErr
		}
	}
	return n, err
}

func newRecordingTeamQuotaController(
	logger *zap.Logger,
	concurrencyErr error,
) (
	*gatewayteamquota.Controller,
	*countingTeamQuotaRateLimiter,
	*recordingTeamQuotaConcurrencyLimiter,
	*recordingTeamQuotaNetworkLimiter,
) {
	rateLimiter := &countingTeamQuotaRateLimiter{
		decision: tokenbucket.Decision{Allowed: true, Remaining: 100},
	}
	concurrencyLimiter := &recordingTeamQuotaConcurrencyLimiter{err: concurrencyErr}
	networkLimiter := &recordingTeamQuotaNetworkLimiter{}
	return gatewayteamquota.NewController(
			nil,
			nil,
			rateLimiter,
			nil,
			logger,
			gatewayteamquota.WithConcurrencyLimiter(concurrencyLimiter),
			gatewayteamquota.WithNetworkLimiter(networkLimiter),
			gatewayteamquota.WithAdmissionProofConsumer(allowingAdmissionProofConsumer{}),
		),
		rateLimiter,
		concurrencyLimiter,
		networkLimiter
}
