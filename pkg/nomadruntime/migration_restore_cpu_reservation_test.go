package nomadruntime

import (
	"testing"

	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	"github.com/stretchr/testify/require"
)

func TestMigrationRestoreCPUReservationMatchesExactRestore(t *testing.T) {
	_, observation, _, _ := migrationRestoreNodeFixture(t)
	request := observation.Request
	reservation, err := protocol.NewMigrationRestoreCPUReservation(request.Image.Target,
		request.Image.Resources, request.Image.Resources.CPUMillicores+350)
	require.NoError(t, err)
	grant := protocol.MigrationRestoreCPUGrant{Reservation: reservation, RestoreRequestDigest: observation.RequestDigest}
	require.NoError(t, grant.ValidateRestore(request))
	// A different endpoint with its own self-consistent image receipt is still
	// another restore command; an earlier capacity reservation cannot follow it.
	changed := request
	changed.Image.Target.ControlEndpoint = "unix:///private/another-slot.sock"
	digest, err := changed.Image.Digest()
	require.NoError(t, err)
	changed.Prepared.RequestDigest = digest
	require.NoError(t, changed.Validate())
	require.Error(t, grant.ValidateRestore(changed))
	require.Equal(t, request.Image.Resources, reservation.Resources)
}
