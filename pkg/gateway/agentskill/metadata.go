package agentskill

import (
	"fmt"
	"strings"

	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
)

const (
	DefaultName            = "sandbox0"
	DefaultArtifactPrefix  = "sandbox0-agent-skill"
	DefaultArtifactBaseURL = "https://github.com/sandbox0-ai/sandbox0/releases/download"
)

type Metadata struct {
	Name           string   `json:"name"`
	ReleaseVersion string   `json:"releaseVersion"`
	ReleaseTag     string   `json:"releaseTag"`
	ArtifactPrefix string   `json:"artifactPrefix"`
	SourcePriority []string `json:"sourcePriority"`
	DownloadURL    string   `json:"downloadUrl"`
	ChecksumURL    string   `json:"checksumUrl"`
	ManifestURL    string   `json:"manifestUrl"`
}

func Resolve(cfg config.AgentSkillConfig) (*Metadata, error) {
	if !cfg.Enabled {
		return nil, fmt.Errorf("agent skill disabled")
	}

	name := strings.TrimSpace(cfg.Name)
	if name == "" {
		name = DefaultName
	}
	version := strings.TrimSpace(cfg.ReleaseVersion)
	if version == "" {
		return nil, fmt.Errorf("agent skill release version is not configured")
	}
	prefix := strings.TrimSpace(cfg.ArtifactPrefix)
	if prefix == "" {
		prefix = DefaultArtifactPrefix
	}
	baseURL := strings.TrimRight(strings.TrimSpace(cfg.ArtifactBaseURL), "/")
	if baseURL == "" {
		baseURL = DefaultArtifactBaseURL
	}
	releaseTag := "v" + version
	baseReleaseURL := baseURL + "/" + releaseTag
	archiveName := prefix + "-" + version + ".tar.gz"

	return &Metadata{
		Name:           name,
		ReleaseVersion: version,
		ReleaseTag:     releaseTag,
		ArtifactPrefix: prefix,
		SourcePriority: []string{
			"source-code",
			"pkg/apispec/openapi.yaml",
			"s0-cli-help-and-implementation",
			"bundled-docs",
			"hosted-website-docs",
		},
		DownloadURL: baseReleaseURL + "/" + archiveName,
		ChecksumURL: baseReleaseURL + "/" + archiveName + ".sha256",
		ManifestURL: baseReleaseURL + "/" + prefix + "-" + version + ".manifest.json",
	}, nil
}
