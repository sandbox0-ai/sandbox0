package agentskill

import (
	"testing"

	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
)

func TestResolveReturnsErrorWhenEnabledWithoutReleaseVersion(t *testing.T) {
	_, err := Resolve(config.AgentSkillConfig{
		Enabled: true,
	})
	if err == nil {
		t.Fatal("expected resolve to fail without a release version")
	}
}

func TestResolveBuildsMetadataForConfiguredSkill(t *testing.T) {
	metadata, err := Resolve(config.AgentSkillConfig{
		Enabled:        true,
		Name:           "sandbox0",
		ReleaseVersion: "0.1.0",
	})
	if err != nil {
		t.Fatalf("resolve metadata: %v", err)
	}
	if metadata.ReleaseTag != "v0.1.0" {
		t.Fatalf("unexpected release tag %q", metadata.ReleaseTag)
	}
	if metadata.DownloadURL != "https://github.com/sandbox0-ai/sandbox0/releases/download/v0.1.0/sandbox0-agent-skill-0.1.0.tar.gz" {
		t.Fatalf("unexpected download URL %q", metadata.DownloadURL)
	}
}
