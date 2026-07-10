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
	fs := NewWithRequestScope("shard-1", "recovery-tag-generation-7", 0, nil)
	header := &fuse.InHeader{Unique: 42}
	ctx := fs.requestContext(header)
	identity, ok := RequestIdentityFromContext(ctx)
	if !ok || identity.Scope != "recovery-tag-generation-7" {
		t.Fatalf("identity = %+v, ok=%v", identity, ok)
	}
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

func BenchmarkContextForHeader(b *testing.B) {
	header := &fuse.InHeader{Unique: 42}
	fs := New("connection-generation", 0, nil)
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		ctx := fs.requestContext(header)
		if _, ok := RequestIdentityFromContext(ctx); !ok {
			b.Fatal("request identity missing")
		}
	}
}
