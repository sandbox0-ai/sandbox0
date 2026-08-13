// Package s0fsrollout defines the fail-closed admission policy used while
// migrating sandbox root filesystems to S0FS.
package s0fsrollout

import (
	"fmt"
	"strings"

	"github.com/sandbox0-ai/sandbox0/pkg/naming"
)

// AdmissionMode controls how newly admitted S0FS sandboxes obtain carriers.
type AdmissionMode string

const (
	AdmissionModeOff    AdmissionMode = "off"
	AdmissionModeCold   AdmissionMode = "cold"
	AdmissionModeShared AdmissionMode = "shared"
)

// Admission is an immutable, normalized S0FS rollout policy.
type Admission struct {
	mode               AdmissionMode
	teamIDs            map[string]struct{}
	templateIDs        map[string]struct{}
	rejectLegacyClaims bool
	legacyAdmitAll     bool
}

// NewAdmission validates and normalizes one rollout policy. An empty mode
// preserves the original sharedCarrierPool.enabled behavior for existing
// configurations; explicit rollout configurations must use off, cold, or
// shared and an allowlist.
func NewAdmission(mode string, teamIDs, templateIDs []string, rejectLegacyClaims, legacySharedEnabled bool) (Admission, error) {
	mode = strings.TrimSpace(strings.ToLower(mode))
	legacyAdmitAll := false
	if mode == "" {
		if legacySharedEnabled {
			mode = string(AdmissionModeShared)
			legacyAdmitAll = true
		} else {
			mode = string(AdmissionModeOff)
		}
	}
	admissionMode := AdmissionMode(mode)
	switch admissionMode {
	case AdmissionModeOff, AdmissionModeCold, AdmissionModeShared:
	default:
		return Admission{}, fmt.Errorf("unsupported S0FS admission mode %q", mode)
	}

	return Admission{
		mode:               admissionMode,
		teamIDs:            normalizeSet(teamIDs),
		templateIDs:        normalizeSet(templateIDs),
		rejectLegacyClaims: rejectLegacyClaims,
		legacyAdmitAll:     legacyAdmitAll,
	}, nil
}

// Mode returns the normalized carrier allocation mode.
func (a Admission) Mode() AdmissionMode {
	return a.mode
}

// RejectLegacyClaims reports whether unmatched new claims must fail closed.
func (a Admission) RejectLegacyClaims() bool {
	return a.rejectLegacyClaims
}

// Admits reports whether a logical template belongs to the configured cohort.
// Team allowlists apply only to private templates; template allowlists apply
// to both public and private templates.
func (a Admission) Admits(scope, teamID, templateID string) bool {
	if a.mode == AdmissionModeOff {
		return false
	}
	if a.legacyAdmitAll {
		return true
	}
	if _, ok := a.templateIDs[strings.TrimSpace(templateID)]; ok {
		return true
	}
	if strings.TrimSpace(scope) != naming.ScopeTeam {
		return false
	}
	_, ok := a.teamIDs[strings.TrimSpace(teamID)]
	return ok
}

// UsesSharedCarrier reports whether admitted new claims may reserve warm
// carriers. Cold mode and disabled admission always create cold carriers.
func (a Admission) UsesSharedCarrier() bool {
	return a.mode == AdmissionModeShared
}

func normalizeSet(values []string) map[string]struct{} {
	if len(values) == 0 {
		return nil
	}
	result := make(map[string]struct{}, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" {
			result[value] = struct{}{}
		}
	}
	return result
}
