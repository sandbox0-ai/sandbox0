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

func TestFileSystemAcknowledgesEveryCapturedCompletion(t *testing.T) {
	fs := New("connection-generation", 0, nil)
	header := &fuse.InHeader{Unique: 43}
	ctx := fs.requestContext(header)
	acknowledgements := make([]int, 4)
	for index := range acknowledgements {
		index := index
		if !AttachRequestCompletionToken(ctx, RequestCompletionTokenFunc(func() {
			acknowledgements[index]++
		})) {
			t.Fatalf("AttachRequestCompletionToken(%d) = false", index)
		}
	}

	fs.RequestAcknowledged(header)
	fs.RequestAcknowledged(header)
	for index, count := range acknowledgements {
		if count != 1 {
			t.Fatalf("acknowledgements[%d] = %d, want 1", index, count)
		}
	}
}

func TestFileSystemCompletionTableFailsClosedAtCapacity(t *testing.T) {
	fs := New("connection-generation", 0, nil)
	token := RequestCompletionTokenFunc(func() {})
	for unique := uint64(1); unique <= requestCompletionSlotCount; unique++ {
		if !AttachRequestCompletionToken(fs.requestContext(&fuse.InHeader{Unique: unique}), token) {
			t.Fatalf("AttachRequestCompletionToken(%d) = false", unique)
		}
	}
	if AttachRequestCompletionToken(fs.requestContext(&fuse.InHeader{Unique: requestCompletionSlotCount + 1}), token) {
		t.Fatal("AttachRequestCompletionToken(over capacity) = true")
	}
	fs.RequestAcknowledged(&fuse.InHeader{Unique: 1})
	if !AttachRequestCompletionToken(fs.requestContext(&fuse.InHeader{Unique: requestCompletionSlotCount + 1}), token) {
		t.Fatal("AttachRequestCompletionToken(after release) = false")
	}
}

func TestFileSystemCompletionTableResolvesModuloCollisions(t *testing.T) {
	fs := New("connection-generation", 0, nil)
	first := &fuse.InHeader{Unique: 42}
	second := &fuse.InHeader{Unique: 42 + requestCompletionSlotCount}
	firstAcknowledged := false
	secondAcknowledged := false
	if !AttachRequestCompletionToken(fs.requestContext(first), RequestCompletionTokenFunc(func() {
		firstAcknowledged = true
	})) || !AttachRequestCompletionToken(fs.requestContext(second), RequestCompletionTokenFunc(func() {
		secondAcknowledged = true
	})) {
		t.Fatal("failed to attach colliding completion tokens")
	}

	fs.RequestAcknowledged(second)
	if firstAcknowledged || !secondAcknowledged {
		t.Fatalf("after second ack first=%v second=%v", firstAcknowledged, secondAcknowledged)
	}
	fs.RequestAcknowledged(first)
	if !firstAcknowledged {
		t.Fatal("colliding first completion was not acknowledged")
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

func BenchmarkRequestAcknowledgedEmpty(b *testing.B) {
	fs := New("connection-generation", 0, nil)
	header := &fuse.InHeader{Unique: 42}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		fs.RequestAcknowledged(header)
	}
}
