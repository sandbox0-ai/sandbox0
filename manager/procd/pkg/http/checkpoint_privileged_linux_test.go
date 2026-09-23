//go:build linux

package http

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/procd/pkg/session"
	"github.com/sandbox0-ai/sandbox0/manager/procd/pkg/webhook"
	"github.com/sandbox0-ai/sandbox0/pkg/procdapi"
	"github.com/sandbox0-ai/sandbox0/pkg/runtimecontrol"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"golang.org/x/sys/unix"
)

// TestMemoryCheckpointSessionGuest verifies the real HTTP and supervisor path
// inside a checkpointed sentry. The host destroys the source runtime before
// restoring, and the worker's memory-only token detects an accidental restart.
func TestMemoryCheckpointSessionGuest(t *testing.T) {
	if os.Getenv(migrationGuestEnv) != "1" {
		t.Skip("isolated checkpoint guest only")
	}
	mode := os.Getenv("SANDBOX0_CHECKPOINT_TEST_MODE")
	require.Contains(t, []string{"resume", "fork"}, mode)
	failed, err := strconv.Atoi(os.Getenv("SANDBOX0_CHECKPOINT_FAILED_ATTEMPTS"))
	require.NoError(t, err)
	log, err := os.OpenFile("/evidence/guest.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
	require.NoError(t, err)
	require.NoError(t, unix.Dup2(int(log.Fd()), 1))
	require.NoError(t, unix.Dup2(int(log.Fd()), 2))
	store, err := session.NewFileStore("/tmp/sessions")
	require.NoError(t, err)
	supervisor, err := session.NewSupervisor(store, zap.NewNop())
	require.NoError(t, err)
	defer supervisor.Close()
	events := make(chan webhook.Event, 16)
	webhookServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		mac := hmac.New(sha256.New, []byte("checkpoint-secret"))
		_, _ = mac.Write(body)
		var event webhook.Event
		if err != nil || json.Unmarshal(body, &event) != nil || r.Header.Get("X-Sandbox0-Signature") != hex.EncodeToString(mac.Sum(nil)) {
			t.Errorf("restored webhook changed its signed body")
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		events <- event
		w.WriteHeader(http.StatusNoContent)
	}))
	defer webhookServer.Close()
	dispatcher := webhook.NewDispatcher(webhook.Options{OutboxDir: "/tmp/webhooks", RequestTimeout: time.Second}, zap.NewNop())
	defer dispatcher.Shutdown(context.Background())
	f := newMigrationHTTPFixtureWithWebhook(t, supervisor, dispatcher, &runtimecontrol.WebhookConfig{URL: webhookServer.URL, Secret: "checkpoint-secret"})
	select {
	case ready := <-events:
		require.Equal(t, webhook.EventTypeSandboxReady, ready.EventType)
	case <-time.After(5 * time.Second):
		t.Fatal("initial webhook was not delivered")
	}

	f.s.checkpointController = f.controller
	host := httptest.NewUnstartedServer(f.s.router)
	require.NoError(t, host.Listener.Close())
	host.Listener, err = net.Listen("tcp", ":0")
	require.NoError(t, err)
	host.Start()
	host.URL = "http://127.0.0.1:" + strconv.Itoa(host.Listener.Addr().(*net.TCPAddr).Port)
	defer host.Close()
	client := procdapi.NewProcdClient(procdapi.ProcdClientConfig{})
	value, _, err := supervisor.Create(session.SessionSpec{
		Command: []string{"/payload", "-test.run=^TestMigrationSessionWorker$"},
		Env:     map[string]string{migrationWorkerEnv: "1"}, IO: session.IOSpec{Mode: session.IOModePipes},
	}, "checkpoint-creation")
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		value, err = supervisor.Get(value.ID)
		return err == nil && value.Phase == session.PhaseRunning
	}, 5*time.Second, 10*time.Millisecond)
	input := session.InputRequest{InputID: "before", ExpectedAttemptID: value.Attempt.ID,
		DataBase64: base64.StdEncoding.EncodeToString([]byte("before\n"))}
	_, err = supervisor.WriteInput(value.ID, input)
	require.NoError(t, err)
	line := waitMigrationSessionOutput(t, supervisor, value.ID, 1)
	parts := strings.Split(strings.TrimSpace(line), ":")
	require.Len(t, parts, 3)
	pid, err := strconv.Atoi(parts[0])
	require.NoError(t, err)
	require.Equal(t, value.Attempt.PID, pid)
	writeEvidence := func(name string, generation int64, duplicate bool) {
		data, err := json.Marshal(migrationGuestEvidence{Instance: f.s.instanceID, Attempt: value.Attempt.ID,
			PID: pid, Token: parts[1], Generation: generation, ReceiptSurvived: duplicate, WebhookVerified: strings.HasPrefix(name, "restored")})
		require.NoError(t, err)
		require.NoError(t, os.WriteFile("/evidence/"+name+".new", data, 0600))
		require.NoError(t, os.Rename("/evidence/"+name+".new", "/evidence/"+name+".json"))
	}
	assignment := f.request.Assignment.Target
	assignment.RuntimeGeneration = 1
	epoch := int64(9)
	for iteration := 0; iteration < 2; iteration++ {
		suffix := ""
		if iteration > 0 {
			suffix = "-again"
		}
		capture, err := runtimecontrol.NewCheckpointCaptureAssignment("capture"+suffix, assignment)
		require.NoError(t, err)
		request := procdapi.RuntimeCheckpointRequest{Action: procdapi.MigrationPrepare, InstanceID: f.s.instanceID,
			CaptureEpoch: epoch, LifecycleEpoch: epoch, Capture: capture}
		_, err = client.CheckpointRuntime(t.Context(), host.URL, request, checkpointHTTPToken(t, f, request))
		require.NoError(t, err)
		_, err = dispatcher.Enqueue(webhook.Event{EventID: "evt-inherited" + suffix, EventType: webhook.EventTypeAgentEvent})
		require.NoError(t, err)
		require.Empty(t, events, "prepared guest cannot send pending webhooks")
		writeEvidence("prepared"+suffix, assignment.RuntimeGeneration, iteration > 0)
		for {
			if _, err := os.Stat("/evidence/restore" + suffix); err == nil {
				break
			}
			time.Sleep(20 * time.Millisecond)
		}
		restored := assignment
		restored.RuntimeGeneration++
		kind := runtimecontrol.CheckpointResume
		epoch++
		if mode == "fork" && iteration == 0 {
			kind, restored.SandboxID, restored.RuntimeGeneration, epoch = runtimecontrol.CheckpointFork, "child", 1, 1
		}
		request.Action, request.LifecycleEpoch = procdapi.MigrationRestore, epoch
		request.Restore = &runtimecontrol.CheckpointRestoreAssignment{OperationID: "restore" + suffix,
			Capture: capture, Kind: kind, Target: restored}
		if iteration == 0 && failed > 0 {
			restored.RuntimeGeneration += int64(failed)
			request.Restore.FromGeneration = restored.RuntimeGeneration - 1
			request.Restore.Target = restored
		}
		for range 2 {
			_, err = client.CheckpointRuntime(context.Background(), host.URL, request, checkpointHTTPToken(t, f, request))
			require.NoError(t, err)
		}
		_, err = dispatcher.Enqueue(webhook.Event{EventID: "evt-restored" + suffix, EventType: webhook.EventTypeAgentEvent})
		require.NoError(t, err)
		wantEvents := 2
		if kind == runtimecontrol.CheckpointFork {
			wantEvents = 1
		}
		seen := map[string]bool{}
		for range wantEvents {
			select {
			case event := <-events:
				require.Equal(t, restored.SandboxID, event.SandboxID)
				seen[event.EventID] = true
			case <-time.After(5 * time.Second):
				t.Fatal("restored webhook worker did not resume")
			}
		}
		require.True(t, seen["evt-restored"+suffix])
		if kind == runtimecontrol.CheckpointFork {
			require.Eventually(t, func() bool {
				_, err := os.Stat(filepath.Join("/tmp/webhooks/inherited", "evt-inherited"+suffix+".json"))
				return err == nil
			}, 5*time.Second, 10*time.Millisecond)
		} else {
			require.True(t, seen["evt-inherited"+suffix])
		}
		receipt, err := supervisor.WriteInput(value.ID, input)
		require.NoError(t, err)
		require.True(t, receipt.Duplicate)
		input.InputID = "after" + suffix
		input.DataBase64 = base64.StdEncoding.EncodeToString([]byte(input.InputID + "\n"))
		_, err = supervisor.WriteInput(value.ID, input)
		require.NoError(t, err)
		output := waitMigrationSessionOutput(t, supervisor, value.ID, iteration+2)
		require.Contains(t, output, parts[0]+":"+parts[1]+":"+input.InputID+"\n")
		after, err := supervisor.Get(value.ID)
		require.NoError(t, err)
		require.Equal(t, value.Attempt.ID, after.Attempt.ID)
		require.Equal(t, value.Attempt.PID, after.Attempt.PID)
		require.EqualValues(t, restored.RuntimeGeneration, after.RuntimeGeneration)
		owner, err := store.BindSandbox(restored.SandboxID)
		require.NoError(t, err)
		require.True(t, owner)
		writeEvidence("restored"+suffix, after.RuntimeGeneration, receipt.Duplicate)
		assignment = restored
		epoch++
	}
	select {}
}
