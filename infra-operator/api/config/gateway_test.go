package config

import "testing"

func TestGatewayConfigApplyDefaultsKeepsAgentSkillDisabledWithoutReleaseVersion(t *testing.T) {
	cfg := GatewayConfig{}

	cfg.ApplyDefaults()

	if cfg.AgentSkill.Enabled {
		t.Fatal("expected agent skill to stay disabled without a release version")
	}
	if cfg.AgentSkill.Name != "sandbox0" {
		t.Fatalf("unexpected default agent skill name %q", cfg.AgentSkill.Name)
	}
	if cfg.AgentSkill.ArtifactBaseURL != "https://github.com/sandbox0-ai/sandbox0/releases/download" {
		t.Fatalf("unexpected default agent skill base URL %q", cfg.AgentSkill.ArtifactBaseURL)
	}
	if cfg.AgentSkill.ArtifactPrefix != "sandbox0-agent-skill" {
		t.Fatalf("unexpected default agent skill prefix %q", cfg.AgentSkill.ArtifactPrefix)
	}
}

func TestGatewayConfigApplyDefaultsEnablesAgentSkillWhenReleaseVersionIsSet(t *testing.T) {
	cfg := GatewayConfig{
		AgentSkill: AgentSkillConfig{
			ReleaseVersion: "0.1.0",
		},
	}

	cfg.ApplyDefaults()

	if !cfg.AgentSkill.Enabled {
		t.Fatal("expected agent skill to be enabled when release version is configured")
	}
}
