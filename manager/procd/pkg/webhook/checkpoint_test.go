package webhook

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestCheckpointDeliveryDrainsAcknowledgementAndKeepsCanceledWaitGated(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			close(started)
			<-release
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	d := NewDispatcher(Options{OutboxDir: t.TempDir(), RequestTimeout: time.Second}, nil)
	t.Cleanup(func() { _ = d.Shutdown(context.Background()) })
	d.SetIdentity("parent", "team")
	d.SetConfig(server.URL, "secret")
	_, err := d.Enqueue(Event{EventID: "evt-inflight", EventType: EventTypeAgentEvent})
	require.NoError(t, err)
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("delivery did not start")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, d.PauseDelivery(ctx), context.DeadlineExceeded)
	_, err = d.Enqueue(Event{EventID: "evt-pending", EventType: EventTypeAgentEvent})
	require.NoError(t, err)
	close(release)
	require.NoError(t, d.PauseDelivery(t.Context()))
	_, err = os.Stat(d.recordPath("evt-inflight"))
	require.True(t, errors.Is(err, os.ErrNotExist), "capture acknowledgement must include the delivery result")
	require.Equal(t, int32(1), calls.Load())
	require.FileExists(t, d.recordPath("evt-pending"))
	d.ResumeDelivery()
	require.Eventually(t, func() bool { return calls.Load() == 2 }, time.Second, time.Millisecond)
}

func TestCheckpointResumeRetainsSignedOutboxAndForkQuarantinesInheritedBytes(t *testing.T) {
	for _, target := range []string{"parent", "child"} {
		t.Run(target, func(t *testing.T) {
			type received struct {
				body      []byte
				signature string
			}
			messages := make(chan received, 3)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				body, _ := io.ReadAll(r.Body)
				messages <- received{body, r.Header.Get("X-Sandbox0-Signature")}
				w.WriteHeader(http.StatusNoContent)
			}))
			defer server.Close()
			d := NewDispatcher(Options{OutboxDir: t.TempDir()}, nil)
			t.Cleanup(func() { _ = d.Shutdown(context.Background()) })
			d.SetIdentity("parent", "team")
			d.SetConfig(server.URL, "secret")
			require.NoError(t, d.PauseDelivery(t.Context()))
			_, err := d.Enqueue(Event{EventID: "evt-inherited", EventType: EventTypeAgentEvent, Payload: map[string]string{"message": "preserve"}})
			require.NoError(t, err)
			original, err := os.ReadFile(d.recordPath("evt-inherited"))
			require.NoError(t, err)
			record, err := readRecord(d.recordPath("evt-inherited"))
			require.NoError(t, err)
			for range 2 {
				require.NoError(t, d.RebindCheckpointIdentity("parent", target, "team"))
			}
			_, err = d.Enqueue(Event{EventID: "evt-new", EventType: EventTypeAgentEvent})
			require.NoError(t, err)
			d.ResumeDelivery()
			var delivered []received
			want := 1
			if target == "parent" {
				want = 2
			}
			for range want {
				select {
				case msg := <-messages:
					delivered = append(delivered, msg)
				case <-time.After(2 * time.Second):
					t.Fatal("delivery was not resumed")
				}
			}
			if target == "parent" {
				require.Equal(t, []byte(record.Body), delivered[0].body)
				require.Equal(t, record.Signature, delivered[0].signature)
			} else {
				inherited := filepath.Join(d.options.OutboxDir, "inherited", "evt-inherited.json")
				require.Eventually(t, func() bool { _, err := os.Stat(inherited); return err == nil }, time.Second, time.Millisecond)
				actual, err := os.ReadFile(inherited)
				require.NoError(t, err)
				require.Equal(t, original, actual)
				_, err = d.Enqueue(Event{EventID: "evt-wrong-owner", SandboxID: "parent", TeamID: "team"})
				require.ErrorIs(t, err, ErrEventOwnerChanged)
			}
			var event Event
			require.NoError(t, json.Unmarshal(delivered[len(delivered)-1].body, &event))
			require.Equal(t, target, event.SandboxID)
			require.Equal(t, signPayload("secret", delivered[len(delivered)-1].body), delivered[len(delivered)-1].signature)
			require.NoError(t, d.PauseDelivery(t.Context()))
			require.Empty(t, messages, "fork must not emit inherited parent events")
		})
	}
}

func TestCheckpointDeliveryRejectsWrongOwnerAndUndrainedRebind(t *testing.T) {
	d := NewDispatcher(Options{OutboxDir: t.TempDir()}, nil)
	t.Cleanup(func() { _ = d.Shutdown(context.Background()) })
	d.SetIdentity("parent", "team")
	require.Error(t, d.RebindCheckpointIdentity("parent", "child", "team"))
	require.NoError(t, d.PauseDelivery(t.Context()))
	require.Error(t, d.RebindCheckpointIdentity("other", "child", "team"))
	require.Error(t, d.RebindCheckpointIdentity("parent", "child", "other-team"))
	require.NoError(t, d.RebindCheckpointIdentity("parent", "child", "team"))
}

func TestCheckpointForkOwnerFenceSurvivesColdRestartBeforeQuarantine(t *testing.T) {
	messages := make(chan Event, 3)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var event Event
		_ = json.NewDecoder(r.Body).Decode(&event)
		messages <- event
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	dir := t.TempDir()
	first := NewDispatcher(Options{OutboxDir: dir}, nil)
	require.NoError(t, first.SetIdentity("parent", "team"))
	first.SetConfig(server.URL, "secret")
	require.NoError(t, first.PauseDelivery(t.Context()))
	_, err := first.Enqueue(Event{EventID: "evt-inherited", EventType: EventTypeAgentEvent})
	require.NoError(t, err)
	require.NoError(t, first.RebindCheckpointIdentity("parent", "child", "team"))
	// Lose this process before its lazy worker has moved inherited records.
	require.NoError(t, first.Shutdown(t.Context()))
	require.FileExists(t, filepath.Join(dir, "evt-inherited.json"))
	second := NewDispatcher(Options{OutboxDir: dir}, nil)
	t.Cleanup(func() { _ = second.Shutdown(context.Background()) })
	second.ResumeDelivery() // Cannot bypass assignment after a cold restart.
	require.NoError(t, second.drainOutbox())
	require.Empty(t, messages)
	second.SetConfig(server.URL, "secret")
	require.NoError(t, second.SetIdentity("grandchild", "team"))
	_, err = second.Enqueue(Event{EventID: "evt-new", EventType: EventTypeAgentEvent})
	require.NoError(t, err)
	select {
	case event := <-messages:
		require.Equal(t, "grandchild", event.SandboxID)
		require.Equal(t, "evt-new", event.EventID)
	case <-time.After(2 * time.Second):
		t.Fatal("new runtime delivery stayed blocked")
	}
	require.NoError(t, second.PauseDelivery(t.Context()))
	require.Empty(t, messages)
	require.FileExists(t, filepath.Join(dir, "inherited", "evt-inherited.json"))
}

func TestCheckpointOwnerPersistenceFailureKeepsDeliveryGated(t *testing.T) {
	d := NewDispatcher(Options{OutboxDir: t.TempDir()}, nil)
	t.Cleanup(func() { _ = d.Shutdown(context.Background()) })
	require.NoError(t, d.SetIdentity("parent", "team"))
	require.NoError(t, d.PauseDelivery(t.Context()))
	path := filepath.Join(d.options.OutboxDir, ".checkpoint-owner")
	require.NoError(t, os.Mkdir(path, 0o700))
	require.Error(t, d.RebindCheckpointIdentity("parent", "child", "team"))
	require.False(t, d.beginDelivery())
	d.mu.RLock()
	owner := d.sandbox
	d.mu.RUnlock()
	require.Equal(t, "parent", owner)
	require.NoError(t, os.Remove(path))
	require.NoError(t, d.RebindCheckpointIdentity("parent", "child", "team"))
}

func TestLegacyIndentedOutboxRetainsOriginalSignedBody(t *testing.T) {
	event := Event{EventID: "evt-legacy", SandboxID: "owner", TeamID: "team", Payload: map[string]any{"text": "spaces stay inside strings", "html": "<body>", "nested": map[string]int{"value": 42}}}
	body, err := json.Marshal(event)
	require.NoError(t, err)
	record := deliveryRecord{Event: event, TargetURL: "http://127.0.0.1:1", Body: body, Signature: signPayload("secret", body)}
	legacy, err := json.MarshalIndent(record, "", "  ")
	require.NoError(t, err)
	path := filepath.Join(t.TempDir(), "legacy.json")
	require.NoError(t, os.WriteFile(path, legacy, 0o600))
	decoded, err := readRecord(path)
	require.NoError(t, err)
	require.Equal(t, body, []byte(decoded.Body))
	require.Equal(t, signPayload("secret", decoded.Body), decoded.Signature)
}
