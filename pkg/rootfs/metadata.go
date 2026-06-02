package rootfs

import (
	"strconv"
	"strings"

	"github.com/sandbox0-ai/sandbox0/pkg/ctldapi"
)

const (
	AnnotationSandboxID = "sandbox0.ai/sandbox-id"
	AnnotationTeamID    = "sandbox0.ai/team-id"
	AnnotationMode      = "sandbox0.ai/rootfs-mode"
	AnnotationVolumeID  = "sandbox0.ai/rootfs-volume-id"
	AnnotationCtldPort  = "sandbox0.ai/rootfs-ctld-port"

	ModeS0FSUpperdir = "s0fs-upperdir"

	RuntimeClassName = "sandbox0-rootfs"
)

type Metadata struct {
	SandboxID string
	TeamID    string
	Mode      string
	VolumeID  string
	CtldPort  int
}

func MetadataFromAnnotations(annotations map[string]string) Metadata {
	if len(annotations) == 0 {
		return Metadata{}
	}
	ctldPort := 0
	if raw := strings.TrimSpace(annotations[AnnotationCtldPort]); raw != "" {
		if parsed, err := strconv.Atoi(raw); err == nil && parsed > 0 {
			ctldPort = parsed
		}
	}
	return Metadata{
		SandboxID: strings.TrimSpace(annotations[AnnotationSandboxID]),
		TeamID:    strings.TrimSpace(annotations[AnnotationTeamID]),
		Mode:      strings.TrimSpace(annotations[AnnotationMode]),
		VolumeID:  strings.TrimSpace(annotations[AnnotationVolumeID]),
		CtldPort:  ctldPort,
	}
}

func (m Metadata) UsesS0FSUpperdir() bool {
	if strings.TrimSpace(m.VolumeID) == "" {
		return false
	}
	return strings.TrimSpace(m.Mode) == ModeS0FSUpperdir
}

func (m Metadata) CanPrepareS0FSUpperdir() bool {
	return m.UsesS0FSUpperdir() &&
		strings.TrimSpace(m.SandboxID) != "" &&
		strings.TrimSpace(m.TeamID) != "" &&
		m.CtldPort > 0
}

func (m Metadata) PrepareRootFSRequest() (ctldapi.PrepareRootFSRequest, bool) {
	if !m.CanPrepareS0FSUpperdir() {
		return ctldapi.PrepareRootFSRequest{}, false
	}
	return ctldapi.PrepareRootFSRequest{
		SandboxID:      strings.TrimSpace(m.SandboxID),
		TeamID:         strings.TrimSpace(m.TeamID),
		RootFSVolumeID: strings.TrimSpace(m.VolumeID),
	}, true
}
