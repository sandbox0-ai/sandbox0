package nomadclaim

import (
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/service"
	"github.com/stretchr/testify/require"
)

func TestClaimDefaultsPersistAConfigValidForRuntimeReconstruction(t *testing.T) {
	zero, shorter := int32(0), int32(300)
	for _, tc := range []struct {
		name string
		ttl  *int32
		hard int32
		want int32
	}{
		{"short hard TTL caps inherited default", nil, 900, 900},
		{"long hard TTL preserves inherited default", nil, 7200, 3600},
		{"disabled hard TTL preserves inherited default", nil, 0, 3600},
		{"explicit disabled idle TTL", &zero, 900, 0},
		{"explicit shorter idle TTL", &shorter, 900, 300},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fixture := newClaimServiceFixture(t)
			fixture.service.defaultTTL = time.Hour
			request := &service.ClaimRequest{
				TeamID: "team-1", UserID: "user-1", Template: "default", OperationID: "operation-ttl",
				Config: &sandboxstore.SandboxConfig{TTL: tc.ttl, HardTTL: &tc.hard},
			}
			response, err := fixture.service.ClaimSandbox(t.Context(), request)
			require.NoError(t, err)
			record := fixture.store.records[response.SandboxID]
			require.NotNil(t, record.Config.TTL)
			require.Equal(t, tc.want, *record.Config.TTL)
			require.Equal(t, tc.ttl, request.Config.TTL)
			// This is the same validation used when reconstructing a persisted
			// runtime after a manager restart or lost initial claim response.
			require.NoError(t, service.NormalizeSandboxConfigForPersistence(service.CloneSandboxConfig(&record.Config)))
			if tc.want > 0 {
				require.Equal(t, record.ClaimedAt.Add(time.Duration(tc.want)*time.Second), record.ExpiresAt)
			}
		})
	}
}
