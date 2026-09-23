package http

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"github.com/sandbox0-ai/sandbox0/pkg/procdapi"
	"github.com/sandbox0-ai/sandbox0/pkg/runtimecontrol"
)

func checkpointHTTPFixture(t *testing.T) (migrationHTTPFixture, procdapi.RuntimeCheckpointRequest) {
	t.Helper()
	f := newMigrationHTTPFixture(t, nil)
	f.s.checkpointController = f.controller
	source := f.request.Assignment.Target
	source.RuntimeGeneration = f.request.Assignment.SourceGeneration
	capture, err := runtimecontrol.NewCheckpointCaptureAssignment("capture", source)
	if err != nil {
		t.Fatal(err)
	}
	return f, procdapi.RuntimeCheckpointRequest{Action: procdapi.MigrationPrepare,
		InstanceID: f.s.instanceID, CaptureEpoch: 9, LifecycleEpoch: 9, Capture: capture}
}

func checkpointHTTPToken(t *testing.T, f migrationHTTPFixture, request procdapi.RuntimeCheckpointRequest) string {
	t.Helper()
	permission, err := request.Permission()
	if err != nil {
		t.Fatal(err)
	}
	team, sandbox := request.ActingSandbox()
	return f.scopedToken(t, internalauth.ServiceManager, team, sandbox, []string{permission}, false)
}

func checkpointHTTPSend(t *testing.T, f migrationHTTPFixture, request procdapi.RuntimeCheckpointRequest, token string) *httptest.ResponseRecorder {
	t.Helper()
	data, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	r := httptest.NewRequest(http.MethodPut, procdapi.RuntimeCheckpointPath, bytes.NewReader(data))
	r.Header.Set(internalauth.DefaultTokenHeader, token)
	w := httptest.NewRecorder()
	f.s.router.ServeHTTP(w, r)
	return w
}

func checkpointHTTPFork(f migrationHTTPFixture, capture procdapi.RuntimeCheckpointRequest) procdapi.RuntimeCheckpointRequest {
	restore := capture
	restore.Action, restore.LifecycleEpoch = procdapi.MigrationRestore, 1
	target := f.request.Assignment.Target
	target.SandboxID, target.RuntimeGeneration = "child", 1
	restore.Restore = &runtimecontrol.CheckpointRestoreAssignment{OperationID: "fork",
		Capture: capture.Capture, Kind: runtimecontrol.CheckpointFork, Target: target}
	return restore
}

func TestCheckpointHTTPForkRequiresIndependentTargetAuthority(t *testing.T) {
	f, capture := checkpointHTTPFixture(t)
	restore := checkpointHTTPFork(f, capture)
	if w := checkpointHTTPSend(t, f, restore, checkpointHTTPToken(t, f, restore)); w.Code != http.StatusConflict {
		t.Fatalf("unprepared restore: %d", w.Code)
	}
	captureToken := checkpointHTTPToken(t, f, capture)
	for range 2 {
		if w := checkpointHTTPSend(t, f, capture, captureToken); w.Code != http.StatusOK {
			t.Fatalf("capture prepare: %d %s", w.Code, w.Body.String())
		}
	}
	if w := checkpointHTTPSend(t, f, restore, captureToken); w.Code != http.StatusForbidden {
		t.Fatalf("prepare permission authorized restore: %d", w.Code)
	}
	permission, _ := restore.Permission()
	parentToken := f.scopedToken(t, internalauth.ServiceManager, "team-1", "sandbox-1", []string{permission}, false)
	if w := checkpointHTTPSend(t, f, restore, parentToken); w.Code != http.StatusForbidden {
		t.Fatalf("parent token authorized child execution: %d", w.Code)
	}
	if w := f.send(t, context.Background(), f.request, f.token(t, f.request)); w.Code != http.StatusConflict {
		t.Fatalf("migration bypassed checkpoint ownership: %d", w.Code)
	}
	for range 2 {
		if w := checkpointHTTPSend(t, f, restore, checkpointHTTPToken(t, f, restore)); w.Code != http.StatusOK {
			t.Fatalf("fork restore: %d %s", w.Code, w.Body.String())
		}
	}
	// Parent epoch 9 must not prevent the child's next lifecycle at epoch 2.
	childCapture, err := runtimecontrol.NewCheckpointCaptureAssignment("child-pause", restore.Restore.Target)
	if err != nil {
		t.Fatal(err)
	}
	child := procdapi.RuntimeCheckpointRequest{Action: procdapi.MigrationPrepare,
		InstanceID: f.s.instanceID, CaptureEpoch: 2, LifecycleEpoch: 2, Capture: childCapture}
	if w := checkpointHTTPSend(t, f, child, checkpointHTTPToken(t, f, child)); w.Code != http.StatusOK {
		t.Fatalf("child inherited parent lifecycle counter: %d %s", w.Code, w.Body.String())
	}
	if w := checkpointHTTPSend(t, f, restore, checkpointHTTPToken(t, f, restore)); w.Code != http.StatusConflict {
		t.Fatalf("old restore reopened later capture: %d", w.Code)
	}
}

func TestCheckpointHTTPCancellationAndMigrationExcludeEachOther(t *testing.T) {
	f, capture := checkpointHTTPFixture(t)
	cancel := capture
	cancel.Action = procdapi.MigrationCancel
	for range 2 {
		if w := checkpointHTTPSend(t, f, cancel, checkpointHTTPToken(t, f, cancel)); w.Code != http.StatusOK {
			t.Fatalf("cancel: %d %s", w.Code, w.Body.String())
		}
	}
	if w := checkpointHTTPSend(t, f, capture, checkpointHTTPToken(t, f, capture)); w.Code != http.StatusConflict {
		t.Fatalf("prepare overtook cancellation: %d", w.Code)
	}
	nextMigration := f.request
	nextMigration.LifecycleEpoch++
	if w := f.send(t, context.Background(), nextMigration, f.token(t, nextMigration)); w.Code != http.StatusOK {
		t.Fatalf("new migration: %d", w.Code)
	}
	nextCapture := capture
	nextCapture.CaptureEpoch, nextCapture.LifecycleEpoch = 11, 11
	nextCapture.Capture.OperationID = "next-capture"
	if w := checkpointHTTPSend(t, f, nextCapture, checkpointHTTPToken(t, f, nextCapture)); w.Code != http.StatusConflict {
		t.Fatalf("checkpoint bypassed migration: %d", w.Code)
	}
}

func TestCheckpointHTTPDrainFailureCannotAuthorizeRestore(t *testing.T) {
	f, capture := checkpointHTTPFixture(t)
	release, accepted := f.s.barrier.enter(httptest.NewRequest(http.MethodPut, "/files", nil))
	if !accepted {
		t.Fatal("could not admit source mutation")
	}
	data, _ := json.Marshal(capture)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	r := httptest.NewRequest(http.MethodPut, procdapi.RuntimeCheckpointPath, bytes.NewReader(data)).WithContext(ctx)
	r.Header.Set(internalauth.DefaultTokenHeader, checkpointHTTPToken(t, f, capture))
	w := httptest.NewRecorder()
	f.s.router.ServeHTTP(w, r)
	if w.Code != http.StatusConflict {
		t.Fatalf("canceled drain: %d", w.Code)
	}
	release()
	restore := checkpointHTTPFork(f, capture)
	if w := checkpointHTTPSend(t, f, restore, checkpointHTTPToken(t, f, restore)); w.Code != http.StatusConflict {
		t.Fatalf("restore accepted incomplete drain: %d", w.Code)
	}
	if ready, _ := f.controller.CanServe(); ready {
		t.Fatal("failed drain reopened source admission")
	}
	if w := checkpointHTTPSend(t, f, capture, checkpointHTTPToken(t, f, capture)); w.Code != http.StatusOK {
		t.Fatalf("drain retry: %d", w.Code)
	}
}
