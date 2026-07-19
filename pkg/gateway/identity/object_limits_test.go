package identity

import (
	"strings"
	"testing"
)

func TestIdentityPersistenceObjectsHaveHardFieldLimits(t *testing.T) {
	ownerID := "owner"
	homeRegionID := strings.Repeat("r", MaxIdentityHomeRegionIDBytes+1)
	tests := []struct {
		name     string
		validate func() error
	}{
		{
			name: "user email",
			validate: func() error {
				return validateUserForPersistence(&User{
					Email: strings.Repeat("e", MaxIdentityEmailBytes+1),
				})
			},
		},
		{
			name: "user name",
			validate: func() error {
				return validateUserForPersistence(&User{
					Name: strings.Repeat("n", MaxIdentityNameBytes+1),
				})
			},
		},
		{
			name: "team slug",
			validate: func() error {
				return validateTeamForPersistence(&Team{
					Slug:    strings.Repeat("s", MaxIdentityTeamSlugBytes+1),
					OwnerID: &ownerID,
				})
			},
		},
		{
			name: "team home region",
			validate: func() error {
				return validateTeamForPersistence(&Team{
					OwnerID:      &ownerID,
					HomeRegionID: &homeRegionID,
				})
			},
		},
		{
			name: "team member role",
			validate: func() error {
				return validateTeamMemberForPersistence(&TeamMember{
					Role: strings.Repeat("r", MaxIdentityTeamRoleBytes+1),
				})
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := test.validate(); !IsIdentityPayloadTooLarge(err) {
				t.Fatalf("error = %v, want identity payload too large", err)
			}
		})
	}
}
