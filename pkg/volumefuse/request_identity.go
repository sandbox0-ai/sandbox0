package volumefuse

import (
	"context"

	"github.com/hanwen/go-fuse/v2/fuse"
)

// RequestIdentity is stable when an initialized kernel FUSE connection
// resends an in-flight request after userspace recovery.
type RequestIdentity struct {
	Unique uint64
	Resend bool
}

type requestIdentityContextKey struct{}

func contextForHeader(header *fuse.InHeader) context.Context {
	ctx := context.Background()
	if header == nil || header.Unique == 0 {
		return ctx
	}
	return context.WithValue(ctx, requestIdentityContextKey{}, RequestIdentity{
		Unique: header.OriginalUnique(),
		Resend: header.IsResend(),
	})
}

// RequestIdentityFromContext returns the kernel request identity attached by
// FileSystem before it enters a backend session.
func RequestIdentityFromContext(ctx context.Context) (RequestIdentity, bool) {
	if ctx == nil {
		return RequestIdentity{}, false
	}
	identity, ok := ctx.Value(requestIdentityContextKey{}).(RequestIdentity)
	return identity, ok && identity.Unique != 0
}
