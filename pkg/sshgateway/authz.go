package sshgateway

import "strings"

// AuthorizedGrantsHeader carries team/user grants derived from a verified SSH key.
// The header is only sent on the internal ssh-gateway to regional-gateway route.
const AuthorizedGrantsHeader = "X-SSH-Key-Grants"

// AuthorizedGrant identifies one team/user pair authorized by an SSH key.
type AuthorizedGrant struct {
	TeamID string
	UserID string
}

// EncodeAuthorizedGrants formats SSH key grants for an internal HTTP header.
func EncodeAuthorizedGrants(grants []AuthorizedGrant) string {
	parts := make([]string, 0, len(grants))
	seen := make(map[string]struct{}, len(grants))
	for _, grant := range grants {
		teamID := strings.TrimSpace(grant.TeamID)
		userID := strings.TrimSpace(grant.UserID)
		if teamID == "" || userID == "" {
			continue
		}
		encoded := teamID + "=" + userID
		if _, ok := seen[encoded]; ok {
			continue
		}
		seen[encoded] = struct{}{}
		parts = append(parts, encoded)
	}
	return strings.Join(parts, ",")
}

// ParseAuthorizedGrants parses SSH key grants from an internal HTTP header.
func ParseAuthorizedGrants(raw string) []AuthorizedGrant {
	fields := strings.Split(raw, ",")
	grants := make([]AuthorizedGrant, 0, len(fields))
	for _, field := range fields {
		teamID, userID, ok := strings.Cut(strings.TrimSpace(field), "=")
		teamID = strings.TrimSpace(teamID)
		userID = strings.TrimSpace(userID)
		if !ok || teamID == "" || userID == "" {
			continue
		}
		grants = append(grants, AuthorizedGrant{TeamID: teamID, UserID: userID})
	}
	return grants
}

// UserIDForTeam returns the user authorized for teamID by the SSH key grants.
func UserIDForTeam(grants []AuthorizedGrant, teamID string) (string, bool) {
	teamID = strings.TrimSpace(teamID)
	if teamID == "" {
		return "", false
	}
	for _, grant := range grants {
		if strings.TrimSpace(grant.TeamID) == teamID {
			userID := strings.TrimSpace(grant.UserID)
			return userID, userID != ""
		}
	}
	return "", false
}
