package volumefuse

import (
	"context"

	"github.com/hanwen/go-fuse/v2/fuse"
)

// RequestIdentity is stable when an initialized kernel FUSE connection
// resends an in-flight request after userspace recovery.
type RequestIdentity struct {
	Scope  string
	Unique uint64
	Resend bool
}

type requestIdentityContextKey struct{}
type requestCompletionContextKey struct{}

// RequestCompletionToken is acknowledged only after go-fuse has written the
// corresponding reply to the kernel. Implementations should capture the
// backend instance that durably committed the request.
type RequestCompletionToken interface {
	RequestAcknowledged()
}

// RequestCompletionTokenFunc adapts a function to RequestCompletionToken.
type RequestCompletionTokenFunc func()

// RequestAcknowledged implements RequestCompletionToken.
func (f RequestCompletionTokenFunc) RequestAcknowledged() {
	if f != nil {
		f()
	}
}

type requestCompletionSlot struct {
	register func(RequestIdentity, RequestCompletionToken)
}

func contextForHeader(scope string, header *fuse.InHeader, register func(RequestIdentity, RequestCompletionToken)) context.Context {
	if header == nil || header.Unique == 0 {
		return context.Background()
	}
	identity := RequestIdentity{
		Scope:  scope,
		Unique: header.OriginalUnique(),
		Resend: header.IsResend(),
	}
	return ContextWithRequestIdentity(context.Background(), identity, func(token RequestCompletionToken) {
		if register != nil {
			register(identity, token)
		}
	})
}

// ContextWithRequestIdentity attaches a stable FUSE request identity and an
// optional completion-token receiver. It is useful for adapters and crash
// recovery tests that invoke a backend without a live RawFileSystem server.
func ContextWithRequestIdentity(ctx context.Context, identity RequestIdentity, onCompletion func(RequestCompletionToken)) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	if identity.Scope == "" || identity.Unique == 0 {
		return ctx
	}
	ctx = context.WithValue(ctx, requestIdentityContextKey{}, identity)
	return context.WithValue(ctx, requestCompletionContextKey{}, &requestCompletionSlot{
		register: func(_ RequestIdentity, token RequestCompletionToken) {
			if onCompletion != nil {
				onCompletion(token)
			}
		},
	})
}

// RequestIdentityFromContext returns the kernel request identity attached by
// FileSystem before it enters a backend session.
func RequestIdentityFromContext(ctx context.Context) (RequestIdentity, bool) {
	if ctx == nil {
		return RequestIdentity{}, false
	}
	identity, ok := ctx.Value(requestIdentityContextKey{}).(RequestIdentity)
	return identity, ok && identity.Scope != "" && identity.Unique != 0
}

// AttachRequestCompletionToken binds a committed request to the backend that
// must receive its kernel-reply acknowledgement. It returns false when ctx is
// not a FUSE request context or token is nil.
func AttachRequestCompletionToken(ctx context.Context, token RequestCompletionToken) bool {
	if ctx == nil || token == nil {
		return false
	}
	identity, ok := RequestIdentityFromContext(ctx)
	if !ok {
		return false
	}
	slot, ok := ctx.Value(requestCompletionContextKey{}).(*requestCompletionSlot)
	if !ok || slot == nil || slot.register == nil {
		return false
	}
	slot.register(identity, token)
	return true
}
