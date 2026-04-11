package proxy

import (
	"context"
	"errors"
	"net"
	"net/http"
	"time"
)

type upstreamTimeoutDisabledKey struct{}

// WithUpstreamTimeoutDisabled marks a request context so gateway upstream calls
// should not apply the default proxy timeout.
func WithUpstreamTimeoutDisabled(ctx context.Context) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, upstreamTimeoutDisabledKey{}, true)
}

// WithUpstreamTimeoutDisabledRequest applies the no-timeout marker to a request.
func WithUpstreamTimeoutDisabledRequest(req *http.Request) *http.Request {
	if req == nil {
		return nil
	}
	return req.WithContext(WithUpstreamTimeoutDisabled(req.Context()))
}

// UpstreamTimeoutDisabled reports whether upstream timeout enforcement is disabled.
func UpstreamTimeoutDisabled(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	disabled, _ := ctx.Value(upstreamTimeoutDisabledKey{}).(bool)
	return disabled
}

// EffectiveUpstreamTimeout returns the timeout to apply for an upstream request.
func EffectiveUpstreamTimeout(ctx context.Context, defaultTimeout time.Duration) time.Duration {
	if defaultTimeout <= 0 || UpstreamTimeoutDisabled(ctx) {
		return 0
	}
	return defaultTimeout
}

// ApplyRequestTimeout derives a request context with the effective upstream
// timeout. The returned cancel func is always safe to call.
func ApplyRequestTimeout(req *http.Request, defaultTimeout time.Duration) (*http.Request, context.CancelFunc) {
	if req == nil {
		return nil, func() {}
	}
	timeout := EffectiveUpstreamTimeout(req.Context(), defaultTimeout)
	if timeout <= 0 {
		return req, func() {}
	}
	ctx, cancel := context.WithTimeout(req.Context(), timeout)
	return req.WithContext(ctx), cancel
}

// IsTimeoutError reports whether err was caused by an upstream timeout.
func IsTimeoutError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var netErr net.Error
	return errors.As(err, &netErr) && netErr.Timeout()
}
