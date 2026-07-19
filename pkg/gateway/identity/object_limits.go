package identity

import "fmt"

const maxIdentityPasswordHashBytes = 255

func validateUserForPersistence(user *User) error {
	if user == nil {
		return fmt.Errorf("user is required")
	}
	for _, field := range []struct {
		name     string
		value    string
		maxBytes int
	}{
		{name: "email", value: user.Email, maxBytes: MaxIdentityEmailBytes},
		{name: "name", value: user.Name, maxBytes: MaxIdentityNameBytes},
		{name: "avatar_url", value: user.AvatarURL, maxBytes: MaxIdentityAvatarURLBytes},
		{name: "password_hash", value: user.PasswordHash, maxBytes: maxIdentityPasswordHashBytes},
	} {
		if err := validateIdentityFieldSize(field.name, field.value, field.maxBytes); err != nil {
			return err
		}
	}
	return nil
}

func validateTeamForPersistence(team *Team) error {
	if team == nil {
		return fmt.Errorf("team is required")
	}
	for _, field := range []struct {
		name     string
		value    string
		maxBytes int
	}{
		{name: "team_name", value: team.Name, maxBytes: MaxIdentityTeamNameBytes},
		{name: "team_slug", value: team.Slug, maxBytes: MaxIdentityTeamSlugBytes},
	} {
		if err := validateIdentityFieldSize(field.name, field.value, field.maxBytes); err != nil {
			return err
		}
	}
	if team.HomeRegionID != nil {
		if err := validateIdentityFieldSize(
			"home_region_id",
			*team.HomeRegionID,
			MaxIdentityHomeRegionIDBytes,
		); err != nil {
			return err
		}
	}
	return nil
}

func validateTeamMemberForPersistence(member *TeamMember) error {
	if member == nil {
		return fmt.Errorf("team member is required")
	}
	return validateIdentityFieldSize("team_member_role", member.Role, MaxIdentityTeamRoleBytes)
}
