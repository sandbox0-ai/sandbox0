package runtimeslot

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMigrationAuthorityDigestsPreserveWireCompatibility(t *testing.T) {
	request := migrationProtocolRequest()
	digest, err := request.Digest()
	require.NoError(t, err)
	// Recorded from the original implementation before consolidating JSON hashing.
	require.Equal(t, "998d6af574b315c1a55c27aff58bf368ef5a0f4dd90da4b6e709e5d9c90ff4a6", digest)
	command, err := NewNodeChannelMigrationCaptureCommand(request)
	require.NoError(t, err)
	require.Equal(t, "783acf7bdb80d48a0139abf43816f3c5a2d3886576d24a48eff510fc548cba8d", command.RequestID)
}

func TestAuthorityDigestsRejectUnvalidatedRequests(t *testing.T) {
	requests := []interface{ Digest() (string, error) }{
		MigrationCaptureRequest{}, MigrationAdoptionRequest{}, MigrationCapturePeerRequest{},
		MigrationCPUPreflightRequest{}, MigrationSourceFinalizeRequest{}, MigrationSourceGCRequest{},
		MigrationImagePrepareRequest{}, MigrationImagePrefetchRequest{}, MigrationRestoreRequest{},
		MigrationStagingRequest{}, MigrationPublicationRequest{}, MigrationSourceFenceRequest{},
		MigrationFailureRequest{}, MigrationFailureCleanupRequest{}, MigrationFailureFinalizeRequest{},
		MigrationCaptureFailureRequest{}, MigrationCaptureFailureFinalizeRequest{},
		CheckpointImageCancelRequest{}, MigrationRestoreCPUGrant{},
	}
	for _, request := range requests {
		digest, err := request.Digest()
		require.Error(t, err, "%T must validate before hashing", request)
		require.Empty(t, digest)
	}
}

func TestDecodeNodeChannelMessageRejectsUnknownAndTrailingInput(t *testing.T) {
	for _, payload := range []string{
		`{"version":1,"unknown":true}`, `{"version":1} {}`, `{"version":1} garbage`,
	} {
		var target struct {
			Version int `json:"version"`
		}
		require.Error(t, DecodeNodeChannelMessage([]byte(payload), &target), payload)
	}
	var target struct {
		Version int `json:"version"`
	}
	require.NoError(t, DecodeNodeChannelMessage([]byte("{\"version\":1} \n\t"), &target))
	require.Equal(t, 1, target.Version)
}
