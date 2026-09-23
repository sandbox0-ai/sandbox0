package runtimecontrol

import (
	"math"
	"testing"
)

func TestCheckpointRestoreBindsSourceWithoutPreselectingDestination(t *testing.T) {
	source := Assignment{SandboxID: "source", TeamID: "team", RuntimeGeneration: 7,
		SecurityClass: "standard", EnvVars: map[string]string{"MODE": "original"}}
	capture, err := NewCheckpointCaptureAssignment("capture", source)
	if err != nil {
		t.Fatal(err)
	}
	resume := CheckpointRestoreAssignment{OperationID: "resume", Capture: capture, Kind: CheckpointResume, Target: source}
	resume.Target.RuntimeGeneration++
	fork := resume
	fork.Kind, fork.OperationID, fork.Target.SandboxID, fork.Target.RuntimeGeneration = CheckpointFork, "fork", "child", 1
	for _, request := range []CheckpointRestoreAssignment{resume, fork} {
		digest, err := request.Digest()
		if err != nil || len(digest) != 64 {
			t.Fatalf("valid restore digest = %q, %v", digest, err)
		}
		for _, tc := range []struct {
			name string
			edit func(*CheckpointRestoreAssignment)
		}{
			{"team", func(r *CheckpointRestoreAssignment) { r.Target.TeamID = "other" }},
			{"security", func(r *CheckpointRestoreAssignment) { r.Target.SecurityClass = "privileged" }},
			{"environment", func(r *CheckpointRestoreAssignment) { r.Target.EnvVars = nil }},
			{"webhook", func(r *CheckpointRestoreAssignment) { r.Target.Webhook = &WebhookConfig{URL: "https://example.com"} }},
			{"reset", func(r *CheckpointRestoreAssignment) { r.Target.ResetCopiedSessionState = true }},
			{"generation", func(r *CheckpointRestoreAssignment) { r.Target.RuntimeGeneration++ }},
			{"revision", func(r *CheckpointRestoreAssignment) { r.Capture.Revision = "invalid" }},
			{"source", func(r *CheckpointRestoreAssignment) { r.Capture.SandboxID = "unrelated" }},
			{"missing_operation", func(r *CheckpointRestoreAssignment) { r.OperationID = "" }},
			{"nul_operation", func(r *CheckpointRestoreAssignment) { r.OperationID = "op\x00" }},
			{"kind", func(r *CheckpointRestoreAssignment) { r.Kind = "unknown" }},
		} {
			t.Run(string(request.Kind)+"/"+tc.name, func(t *testing.T) {
				changed := request
				tc.edit(&changed)
				if _, err := changed.Digest(); err == nil {
					t.Fatal("changed restore accepted")
				}
			})
		}
		other := request
		other.OperationID += "-other"
		otherDigest, err := other.Digest()
		if err != nil || otherDigest == digest {
			t.Fatal("restore operation identity is not bound")
		}
	}
	fork.Target.SandboxID = source.SandboxID
	if err := fork.Validate(); err == nil {
		t.Fatal("fork reused source identity")
	}
	resume.Target.SandboxID = "child"
	if err := resume.Validate(); err == nil {
		t.Fatal("resume changed sandbox identity")
	}
}

func TestCheckpointForkDoesNotOverflowSourceGeneration(t *testing.T) {
	source := Assignment{SandboxID: "source", TeamID: "team", RuntimeGeneration: math.MaxInt64, SecurityClass: "standard"}
	capture, err := NewCheckpointCaptureAssignment("capture", source)
	if err != nil {
		t.Fatal(err)
	}
	restore := CheckpointRestoreAssignment{OperationID: "fork", Capture: capture, Kind: CheckpointFork, Target: source}
	restore.Target.SandboxID, restore.Target.RuntimeGeneration = "child", 1
	if err := restore.Validate(); err != nil {
		t.Fatal(err)
	}
	restore.Kind, restore.Target.SandboxID, restore.Target.RuntimeGeneration = CheckpointResume, source.SandboxID, math.MinInt64
	if err := restore.Validate(); err == nil {
		t.Fatal("overflowing resume accepted")
	}
}

func TestCheckpointRestoreRetryBindsConsumedGeneration(t *testing.T) {
	source := Assignment{SandboxID: "source", TeamID: "team", RuntimeGeneration: 7, SecurityClass: "standard"}
	capture, err := NewCheckpointCaptureAssignment("capture", source)
	if err != nil {
		t.Fatal(err)
	}
	for _, kind := range []CheckpointRestoreKind{CheckpointResume, CheckpointFork} {
		t.Run(string(kind), func(t *testing.T) {
			a := CheckpointRestoreAssignment{OperationID: "new-explicit-attempt", Capture: capture, Kind: kind, Target: source}
			a.FromGeneration, a.Target.RuntimeGeneration = 8, 9
			if kind == CheckpointFork {
				a.Target.SandboxID, a.FromGeneration, a.Target.RuntimeGeneration = "child", 1, 2
			}
			if err := a.Validate(); err != nil {
				t.Fatal(err)
			}
			for _, previous := range []int64{0, -1, a.FromGeneration - 1, a.FromGeneration + 1, math.MaxInt64} {
				changed := a
				changed.FromGeneration = previous
				if changed.Validate() == nil {
					t.Fatalf("accepted mismatched predecessor %d", previous)
				}
			}
		})
	}
}
