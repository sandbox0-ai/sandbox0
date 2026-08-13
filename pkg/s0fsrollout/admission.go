// Package s0fsrollout defines the fail-closed admission policy used while
// migrating sandbox root filesystems to S0FS.
package s0fsrollout

import (
	"fmt"
	"sort"
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
	cohort             Cohort
	rejectLegacyClaims bool
	legacyAdmitAll     bool
}

// Cohort matches logical templates for staged rollout controls. Team IDs only
// match private templates, while template IDs match both public and private
// templates.
type Cohort struct {
	teamIDs     map[string]struct{}
	templateIDs map[string]struct{}
}

// NewCohort normalizes one explicit team/template selection.
func NewCohort(teamIDs, templateIDs []string) Cohort {
	return Cohort{
		teamIDs:     normalizeSet(teamIDs),
		templateIDs: normalizeSet(templateIDs),
	}
}

// Empty reports whether the cohort has no selectors.
func (c Cohort) Empty() bool {
	return len(c.teamIDs) == 0 && len(c.templateIDs) == 0
}

// TeamIDs returns a copy of the normalized private-team selectors.
func (c Cohort) TeamIDs() []string {
	return setValues(c.teamIDs)
}

// TemplateIDs returns a copy of the normalized logical-template selectors.
func (c Cohort) TemplateIDs() []string {
	return setValues(c.templateIDs)
}

// Matches reports whether one logical template belongs to the cohort.
func (c Cohort) Matches(scope, teamID, templateID string) bool {
	if _, ok := c.templateIDs[strings.TrimSpace(templateID)]; ok {
		return true
	}
	if strings.TrimSpace(scope) != naming.ScopeTeam {
		return false
	}
	_, ok := c.teamIDs[strings.TrimSpace(teamID)]
	return ok
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
		cohort:             NewCohort(teamIDs, templateIDs),
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
	return a.cohort.Matches(scope, teamID, templateID)
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

func setValues(values map[string]struct{}) []string {
	if len(values) == 0 {
		return nil
	}
	result := make([]string, 0, len(values))
	for value := range values {
		result = append(result, value)
	}
	sort.Strings(result)
	return result
}
