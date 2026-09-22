package runtimecontrol

import (
	"math"
	"testing"
)

func TestMigrationAssignmentBindsUnchangedSource(t *testing.T) {
	source := Assignment{SandboxID: "sandbox-1", TeamID: "team-1", RuntimeGeneration: 7,
		SecurityClass: "standard", EnvVars: map[string]string{"MODE": "original"}}
	revision, err := source.Revision()
	if err != nil {
		t.Fatal(err)
	}
	request := MigrationAssignment{OperationID: "migration-1", SourceGeneration: 7, SourceRevision: revision, Target: source}
	request.Target.RuntimeGeneration = 8
	digest, err := request.Digest()
	if err != nil || len(digest) != 64 {
		t.Fatalf("valid migration digest = %q, %v", digest, err)
	}
	for _, tc := range []struct {
		name string
		edit func(*MigrationAssignment)
	}{
		{"sandbox", func(m *MigrationAssignment) { m.Target.SandboxID = "other" }},
		{"team", func(m *MigrationAssignment) { m.Target.TeamID = "other" }},
		{"security", func(m *MigrationAssignment) { m.Target.SecurityClass = "privileged" }},
		{"environment", func(m *MigrationAssignment) { m.Target.EnvVars = map[string]string{"MODE": "changed"} }},
		{"webhook", func(m *MigrationAssignment) { m.Target.Webhook = &WebhookConfig{URL: "https://example.com"} }},
		{"session_reset", func(m *MigrationAssignment) { m.Target.ResetCopiedSessionState = true }},
		{"revision", func(m *MigrationAssignment) { m.SourceRevision = "invalid" }},
		{"skipped_generation", func(m *MigrationAssignment) { m.Target.RuntimeGeneration++ }},
		{"same_generation", func(m *MigrationAssignment) { m.Target.RuntimeGeneration-- }},
		{"zero_source", func(m *MigrationAssignment) { m.SourceGeneration = 0 }},
		{"overflow", func(m *MigrationAssignment) {
			m.SourceGeneration = math.MaxInt64
			m.Target.RuntimeGeneration = math.MinInt64
		}},
		{"missing_operation", func(m *MigrationAssignment) { m.OperationID = "" }},
		{"noncanonical_operation", func(m *MigrationAssignment) { m.OperationID += "\n" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			changed := request
			tc.edit(&changed)
			if _, err := changed.Digest(); err == nil {
				t.Fatal("changed migration was accepted")
			}
		})
	}
	request.OperationID = "migration-2"
	other, err := request.Digest()
	if err != nil || other == digest {
		t.Fatalf("operation is not bound into digest: %q, %v", other, err)
	}
}
