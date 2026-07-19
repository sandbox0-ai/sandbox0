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

func (allowingTeamQuotaRateLimiter) Take(
	context.Context,
	string,
	coreteamquota.Key,
	int64,
) (tokenbucket.Decision, error) {
	return tokenbucket.Decision{Allowed: true, Remaining: 100}, nil
}

func (allowingTeamQuotaRateLimiter) Invalidate(string, coreteamquota.Key) {}

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

type countingTeamQuotaNetworkLimiter struct {
	bytes                   atomic.Int64
	ingressBytes            atomic.Int64
	egressBytes             atomic.Int64
	activeConnections       *atomic.Int64
	callsWithoutActiveLease atomic.Int64
}

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

func (l *countingTeamQuotaNetworkLimiter) WaitN(
	_ context.Context,
	_ string,
	key coreteamquota.Key,
	bytes int,
) error {
	if l.activeConnections != nil && l.activeConnections.Load() != 1 {
		l.callsWithoutActiveLease.Add(1)
	}
	l.bytes.Add(int64(bytes))
	switch key {
	case coreteamquota.KeyNetworkIngressBytes:
		l.ingressBytes.Add(int64(bytes))
	case coreteamquota.KeyNetworkEgressBytes:
		l.egressBytes.Add(int64(bytes))
	}
	return nil
}

func (*countingTeamQuotaNetworkLimiter) Reader(
	_ context.Context,
	_ string,
	_ coreteamquota.Key,
	reader io.Reader,
) io.Reader {
	return reader
}

func (*countingTeamQuotaNetworkLimiter) Close() error { return nil }

func (l *countingTeamQuotaNetworkLimiter) Bytes() int64 {
	return l.bytes.Load()
}

func (l *countingTeamQuotaNetworkLimiter) BytesFor(key coreteamquota.Key) int64 {
	switch key {
	case coreteamquota.KeyNetworkIngressBytes:
		return l.ingressBytes.Load()
	case coreteamquota.KeyNetworkEgressBytes:
		return l.egressBytes.Load()
	default:
		return 0
	}
}

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

type countingTeamQuotaRateLimiter struct {
	calls      atomic.Int64
	lastTeamID atomic.Value
	lastKey    atomic.Value
}

func (l *countingTeamQuotaRateLimiter) Take(
	_ context.Context,
	teamID string,
	key coreteamquota.Key,
	_ int64,
) (tokenbucket.Decision, error) {
	l.calls.Add(1)
	l.lastTeamID.Store(teamID)
	l.lastKey.Store(key)
	return tokenbucket.Decision{Allowed: true, Remaining: 100}, nil
}

func (*countingTeamQuotaRateLimiter) Invalidate(string, coreteamquota.Key) {}

type countingTeamQuotaConcurrencyLimiter struct {
	acquireCalls atomic.Int64
	releaseCalls atomic.Int64
	active       atomic.Int64
	lastTeamID   atomic.Value
	lastKey      atomic.Value
}

func (l *countingTeamQuotaConcurrencyLimiter) Acquire(
	_ context.Context,
	teamID string,
	key coreteamquota.Key,
) (gatewayteamquota.ConnectionLease, error) {
	l.acquireCalls.Add(1)
	l.active.Add(1)
	l.lastTeamID.Store(teamID)
	l.lastKey.Store(key)
	return &countingTeamQuotaLease{
		limiter: l,
		done:    make(chan struct{}),
	}, nil
}

func (*countingTeamQuotaConcurrencyLimiter) Usage(
	context.Context,
	string,
	coreteamquota.Key,
) (int64, error) {
	return 0, nil
}

func (*countingTeamQuotaConcurrencyLimiter) Invalidate(string, coreteamquota.Key) {}
func (*countingTeamQuotaConcurrencyLimiter) Close() error                         { return nil }

type countingTeamQuotaLease struct {
	limiter *countingTeamQuotaConcurrencyLimiter
	done    chan struct{}
	once    sync.Once
}

func (l *countingTeamQuotaLease) Done() <-chan struct{} { return l.done }
func (*countingTeamQuotaLease) Err() error              { return nil }
func (l *countingTeamQuotaLease) Release(context.Context) error {
	l.once.Do(func() {
		l.limiter.active.Add(-1)
		l.limiter.releaseCalls.Add(1)
		close(l.done)
	})
	return nil
}

func newCountingTeamQuotaController(
	logger *zap.Logger,
) (*gatewayteamquota.Controller, *countingTeamQuotaRateLimiter) {
	limiter := &countingTeamQuotaRateLimiter{}
	return gatewayteamquota.NewController(
		nil,
		nil,
		limiter,
		nil,
		logger,
		gatewayteamquota.WithConcurrencyLimiter(allowingTeamQuotaConcurrencyLimiter{}),
		gatewayteamquota.WithNetworkLimiter(allowingTeamQuotaNetworkLimiter{}),
		gatewayteamquota.WithAdmissionProofConsumer(allowingAdmissionProofConsumer{}),
	), limiter
}

func newCountingPublicExposureTeamQuotaController(
	logger *zap.Logger,
) (
	*gatewayteamquota.Controller,
	*countingTeamQuotaRateLimiter,
	*countingTeamQuotaConcurrencyLimiter,
	*countingTeamQuotaNetworkLimiter,
) {
	rateLimiter := &countingTeamQuotaRateLimiter{}
	concurrencyLimiter := &countingTeamQuotaConcurrencyLimiter{}
	networkLimiter := &countingTeamQuotaNetworkLimiter{
		activeConnections: &concurrencyLimiter.active,
	}
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

type zeroTeamQuotaRateLimiter struct{}

func (zeroTeamQuotaRateLimiter) Take(
	context.Context,
	string,
	coreteamquota.Key,
	int64,
) (tokenbucket.Decision, error) {
	return tokenbucket.Decision{Allowed: false}, nil
}

func (zeroTeamQuotaRateLimiter) Invalidate(string, coreteamquota.Key) {}

func newZeroRateTeamQuotaController(logger *zap.Logger) *gatewayteamquota.Controller {
	return gatewayteamquota.NewController(
		nil,
		nil,
		zeroTeamQuotaRateLimiter{},
		nil,
		logger,
		gatewayteamquota.WithConcurrencyLimiter(allowingTeamQuotaConcurrencyLimiter{}),
		gatewayteamquota.WithNetworkLimiter(allowingTeamQuotaNetworkLimiter{}),
		gatewayteamquota.WithAdmissionProofConsumer(allowingAdmissionProofConsumer{}),
	)
}
