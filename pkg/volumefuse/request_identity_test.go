package volumefuse

import (
	"context"
	"testing"

	"github.com/hanwen/go-fuse/v2/fuse"
)

func TestContextForHeaderPreservesResendIdentity(t *testing.T) {
	header := &fuse.InHeader{Unique: uint64(1)<<63 | 42}
	identity, ok := RequestIdentityFromContext(contextForHeader("shard-1", header, nil))
	if !ok || identity.Scope != "shard-1" || identity.Unique != 42 || !identity.Resend {
		t.Fatalf("identity = %+v, ok=%v", identity, ok)
	}
}

func TestRequestCompletionTokenTargetsCapturedBackend(t *testing.T) {
	var captured RequestCompletionToken
	ctx := ContextWithRequestIdentity(context.Background(), RequestIdentity{
		Scope:  "shard-1",
		Unique: 42,
	}, func(token RequestCompletionToken) {
		captured = token
	})

	acknowledged := false
	if !AttachRequestCompletionToken(ctx, RequestCompletionTokenFunc(func() {
		acknowledged = true
	})) {
		t.Fatal("AttachRequestCompletionToken() = false")
	}
	if captured == nil {
		t.Fatal("completion token was not captured")
	}
	captured.RequestAcknowledged()
	if !acknowledged {
		t.Fatal("captured completion token was not acknowledged")
	}
}

func TestFileSystemAcknowledgesOriginalCompletionOnce(t *testing.T) {
	fs := New("shard-1", 0, nil)
	header := &fuse.InHeader{Unique: 42}
	ctx := fs.requestContext(header)
	acknowledgements := 0
	if !AttachRequestCompletionToken(ctx, RequestCompletionTokenFunc(func() {
		acknowledgements++
	})) {
		t.Fatal("AttachRequestCompletionToken() = false")
	}

	fs.RequestAcknowledged(header)
	fs.RequestAcknowledged(header)
	if acknowledgements != 1 {
		t.Fatalf("acknowledgements = %d, want 1", acknowledgements)
	}
}
