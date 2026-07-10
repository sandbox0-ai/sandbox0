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
	identity     RequestIdentity
	target       *FileSystem
	onCompletion func(RequestCompletionToken)
}

func (s *requestCompletionSlot) attach(token RequestCompletionToken) bool {
	if s == nil || token == nil {
		return false
	}
	if s.target != nil {
		return s.target.registerRequestCompletion(s.identity, token)
	}
	if s.onCompletion != nil {
		s.onCompletion(token)
		return true
	}
	return false
}

type requestContext struct {
	context.Context
	identity   RequestIdentity
	completion requestCompletionSlot
}

func (c *requestContext) Value(key any) any {
	if c == nil {
		return nil
	}
	switch key.(type) {
	case requestIdentityContextKey:
		return c.identity
	case requestCompletionContextKey:
		return &c.completion
	default:
		return c.Context.Value(key)
	}
}

func contextForHeader(scope string, header *fuse.InHeader, target *FileSystem) context.Context {
	if header == nil || header.Unique == 0 {
		return context.Background()
	}
	identity := RequestIdentity{
		Scope:  scope,
		Unique: header.OriginalUnique(),
		Resend: header.IsResend(),
	}
	return &requestContext{
		Context:  context.Background(),
		identity: identity,
		completion: requestCompletionSlot{
			identity: identity,
			target:   target,
		},
	}
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
	return &requestContext{
		Context:  ctx,
		identity: identity,
		completion: requestCompletionSlot{
			identity:     identity,
			onCompletion: onCompletion,
		},
	}
}

// RequestIdentityFromContext returns the kernel request identity attached by
// FileSystem before it enters a backend session.
func RequestIdentityFromContext(ctx context.Context) (RequestIdentity, bool) {
	if ctx == nil {
		return RequestIdentity{}, false
	}
	if requestCtx, ok := ctx.(*requestContext); ok {
		identity := requestCtx.identity
		return identity, identity.Scope != "" && identity.Unique != 0
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
	var slot *requestCompletionSlot
	if requestCtx, ok := ctx.(*requestContext); ok {
		if requestCtx.identity.Scope == "" || requestCtx.identity.Unique == 0 {
			return false
		}
		slot = &requestCtx.completion
	} else {
		if _, ok := RequestIdentityFromContext(ctx); !ok {
			return false
		}
		slot, _ = ctx.Value(requestCompletionContextKey{}).(*requestCompletionSlot)
	}
	if slot == nil || (slot.target == nil && slot.onCompletion == nil) {
		return false
	}
	return slot.attach(token)
}
