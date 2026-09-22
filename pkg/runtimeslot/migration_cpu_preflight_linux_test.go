//go:build linux

package runtimeslot

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCPUPreflightNodeClientUsesSecureControlAndRejectsMixedReply(t *testing.T) {
	for _, mixed := range []bool{false, true} {
		request, observation := cpuPreflightProtocolFixture(t)
		client, endpoint := startNodeControlTestServer(t, func(w http.ResponseWriter, r *http.Request) {
			require.Equal(t, http.MethodPut, r.Method)
			require.Equal(t, NodeMigrationCPUPreflightControlPath, r.URL.Path)
			var received MigrationCPUPreflightRequest
			require.NoError(t, json.NewDecoder(r.Body).Decode(&received))
			observation.RequestDigest, _ = received.Digest()
			observation.Launch.Target = received.Target
			response := NodeControlResponse{Phase: "cpu_preflight", MigrationCPUPreflight: &observation}
			if mixed {
				response.MigrationRestore = &MigrationRestoreObservation{}
			}
			require.NoError(t, json.NewEncoder(w).Encode(response))
		})
		request.Target.ControlEndpoint, request.Source.Target.ControlEndpoint = endpoint, endpoint
		result, err := client.PreflightMigrationCPU(t.Context(), request)
		if mixed {
			require.Error(t, err)
			require.Nil(t, result)
		} else {
			require.NoError(t, err)
			require.NoError(t, result.ValidateFor(request))
		}
	}
}
