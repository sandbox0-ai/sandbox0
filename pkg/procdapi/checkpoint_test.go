package procdapi

import (
	"testing"

	"github.com/google/uuid"
	"github.com/sandbox0-ai/sandbox0/pkg/runtimecontrol"
)

func TestCheckpointPermissionsBindCaptureAndIndependentRestore(t *testing.T) {
	source := runtimecontrol.Assignment{SandboxID: "source", TeamID: "team", RuntimeGeneration: 5, SecurityClass: "standard"}
	capture, err := runtimecontrol.NewCheckpointCaptureAssignment("capture", source)
	if err != nil {
		t.Fatal(err)
	}
	prepare := RuntimeCheckpointRequest{Action: MigrationPrepare, InstanceID: uuid.NewString(),
		CaptureEpoch: 20, LifecycleEpoch: 20, Capture: capture}
	preparePermission, err := prepare.Permission()
	if err != nil {
		t.Fatal(err)
	}
	target := source
	target.SandboxID, target.RuntimeGeneration = "child", 1
	restore := prepare
	restore.Action, restore.LifecycleEpoch = MigrationRestore, 1
	restore.Restore = &runtimecontrol.CheckpointRestoreAssignment{OperationID: "fork", Capture: capture,
		Kind: runtimecontrol.CheckpointFork, Target: target}
	restorePermission, err := restore.Permission()
	if err != nil || restorePermission == preparePermission {
		t.Fatal("capture permission authorizes a fork restore")
	}
	team, sandbox := restore.ActingSandbox()
	if team != "team" || sandbox != "child" {
		t.Fatal("fork restore uses source token scope")
	}
	changed := restore
	changed.LifecycleEpoch++
	permission, err := changed.Permission()
	if err != nil || permission == restorePermission {
		t.Fatal("restore epoch is not signed")
	}
	changed = restore
	changed.CaptureEpoch++
	permission, err = changed.Permission()
	if err != nil || permission == restorePermission {
		t.Fatal("capture epoch is not signed")
	}
	digest, _ := restore.Digest()
	response := RuntimeCheckpointResponse{InstanceID: restore.InstanceID, RequestDigest: digest,
		RuntimeGeneration: 1, State: "ready"}
	if err := response.ValidateFor(restore); err != nil {
		t.Fatal(err)
	}
	if err := response.ValidateFor(prepare); err == nil {
		t.Fatal("restore receipt substituted for capture acknowledgement")
	}
	response.RuntimeGeneration = source.RuntimeGeneration
	if err := response.ValidateFor(restore); err == nil {
		t.Fatal("source generation substituted for restored child")
	}
	changed = prepare
	changed.Restore = restore.Restore
	if _, err := changed.Permission(); err == nil {
		t.Fatal("prepare selected a restore destination")
	}
	changed = restore
	changed.Restore = nil
	if _, err := changed.Permission(); err == nil {
		t.Fatal("targetless restore accepted")
	}
	changed = restore
	copyRestore := *restore.Restore
	copyRestore.Kind, copyRestore.Target.SandboxID, copyRestore.Target.RuntimeGeneration = runtimecontrol.CheckpointResume, "source", 6
	changed.Restore = &copyRestore
	if _, err := changed.Permission(); err == nil {
		t.Fatal("resume regressed the source lifecycle epoch")
	}
}
