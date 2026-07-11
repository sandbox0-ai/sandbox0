package volumefuse

import (
	"context"
	"testing"
	"time"

	"github.com/hanwen/go-fuse/v2/fuse"
)

func TestContextForHeaderPreservesResendIdentity(t *testing.T) {
	header := &fuse.InHeader{Unique: uint64(1)<<63 | 42}
	identity, ok := RequestIdentityFromContext(contextForHeader("shard-1", header, nil))
	if !ok || identity.Scope != "shard-1" || identity.Unique != 42 || !identity.Resend {
		t.Fatalf("identity = %+v, ok=%v", identity, ok)
	}
}

func TestFileSystemWaitsForEveryRecoveredReply(t *testing.T) {
	fs := NewWithRequestScope("volume", "scope", time.Second, nil)
	if err := fs.BeginRecoveryDrain(2); err != nil {
		t.Fatal(err)
	}
	first := &fuse.InHeader{Unique: 41 | fuse.UNIQUE_RESEND}
	second := &fuse.InHeader{Unique: 42 | fuse.UNIQUE_RESEND}
	_ = fs.requestContext(first)
	_ = fs.requestContext(second)

	done := make(chan error, 1)
	go func() { done <- fs.WaitRecoveryDrain(context.Background()) }()
	fs.RequestAcknowledged(first)
	select {
	case err := <-done:
		t.Fatalf("WaitRecoveryDrain() returned after one reply: %v", err)
	case <-time.After(10 * time.Millisecond):
	}
	fs.RequestAcknowledged(second)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("WaitRecoveryDrain() did not observe both replies")
	}
}

func TestFileSystemRecoveryDrainFailsClosed(t *testing.T) {
	fs := NewWithRequestScope("volume", "scope", time.Second, nil)
	if err := fs.WaitRecoveryDrain(context.Background()); err == nil {
		t.Fatal("WaitRecoveryDrain() error = nil before initialization")
	}
	if err := fs.BeginRecoveryDrain(0); err != nil {
		t.Fatal(err)
	}
	if err := fs.BeginRecoveryDrain(0); err == nil {
		t.Fatal("BeginRecoveryDrain() error = nil after initialization")
	}
	_ = fs.requestContext(&fuse.InHeader{Unique: 41 | fuse.UNIQUE_RESEND})
	if err := fs.WaitRecoveryDrain(context.Background()); err == nil {
		t.Fatal("WaitRecoveryDrain() error = nil after unexpected resend")
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

func TestFileSystemReusesUniqueAfterCompletionTableDrains(t *testing.T) {
	fs := New("connection-generation", 0, nil)
	header := &fuse.InHeader{Unique: 42}
	acknowledgements := 0
	for request := 0; request < 2; request++ {
		ctx := fs.requestContext(header)
		if !AttachRequestCompletionToken(ctx, RequestCompletionTokenFunc(func() {
			acknowledgements++
		})) {
			t.Fatalf("AttachRequestCompletionToken(%d) = false", request)
		}
		fs.RequestAcknowledged(header)
	}
	if acknowledgements != 2 || fs.pendingCount != 0 {
		t.Fatalf("acknowledgements=%d pending=%d, want 2/0", acknowledgements, fs.pendingCount)
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

func TestFileSystemCompletionTableReusesCollisionTombstone(t *testing.T) {
	fs := New("connection-generation", 0, nil)
	first := &fuse.InHeader{Unique: 42}
	second := &fuse.InHeader{Unique: 42 + requestCompletionSlotCount}
	if !AttachRequestCompletionToken(fs.requestContext(first), RequestCompletionTokenFunc(func() {})) ||
		!AttachRequestCompletionToken(fs.requestContext(second), RequestCompletionTokenFunc(func() {})) {
		t.Fatal("failed to attach colliding completion tokens")
	}
	fs.RequestAcknowledged(first)

	acknowledgements := 0
	if !AttachRequestCompletionToken(fs.requestContext(second), RequestCompletionTokenFunc(func() {
		acknowledgements++
	})) {
		t.Fatal("failed to attach completion behind a tombstone")
	}
	fs.RequestAcknowledged(second)
	if acknowledgements != 1 || fs.pendingCount != 0 {
		t.Fatalf("acknowledgements=%d pending=%d, want 1/0", acknowledgements, fs.pendingCount)
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
