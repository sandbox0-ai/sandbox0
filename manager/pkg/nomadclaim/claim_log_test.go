package nomadclaim

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/runtimeslotclaim"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/service"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

func TestClaimLogsCorrelatedPlannerPhasesWithoutChangingReadiness(t *testing.T) {
	fixture := newClaimServiceFixture(t)
	core, logs := observer.New(zap.InfoLevel)
	fixture.service.logger = zap.New(core)
	phases := []runtimeslotclaim.PhaseObservation{
		{Phase: runtimeslotclaim.PhaseIngressToPlanner, Duration: 37*time.Millisecond + 900*time.Nanosecond, Succeeded: true},
		{Phase: runtimeslotclaim.PhaseNodeClaim, Duration: 300 * time.Millisecond, Succeeded: true},
		{Phase: runtimeslotclaim.PhaseProcdProbe, Duration: 83 * time.Millisecond, Succeeded: true},
	}
	fixture.planner.result = &runtimeslotclaim.Result{
		Slot:         &sandboxstore.RuntimeSlot{ID: "slot-1", AllocationID: "allocation-1", AllocationNamespace: "default"},
		Grant:        &sandboxstore.RootFSWriterGrant{ID: "grant-1"},
		ProcdAddress: "http://10.0.0.8:49983", Duration: 420 * time.Millisecond, WithinSLO: true, Phases: phases,
	}
	response, err := fixture.service.ClaimSandbox(context.Background(), &service.ClaimRequest{
		TeamID: "team-1", UserID: "user-1", Template: "default", OperationID: "operation-1",
		Config: &sandboxstore.SandboxConfig{EnvVars: map[string]string{"TOKEN": "must-not-be-logged"}},
	})
	require.NoError(t, err)
	require.Equal(t, 420*time.Millisecond, response.CommandReadyDuration)
	require.True(t, response.CommandReadyWithinSLO)
	require.Equal(t, phases, fixture.planner.result.Phases)
	entries := logs.FilterMessage("Claimed Nomad sandbox").All()
	require.Len(t, entries, 1)
	fields := entries[0].ContextMap()
	require.Equal(t, response.SandboxID, fields["sandboxID"])
	require.Equal(t, []interface{}{
		map[string]interface{}{"phase": "ingress_to_planner", "durationMicros": int64(37000), "succeeded": true},
		map[string]interface{}{"phase": "node_claim", "durationMicros": int64(300000), "succeeded": true},
		map[string]interface{}{"phase": "procd_probe", "durationMicros": int64(83000), "succeeded": true},
	}, fields["claimPhases"])
	require.NotContains(t, fields, "TOKEN")
	require.NotContains(t, fields, "environment")
	encoded, err := json.Marshal(fields)
	require.NoError(t, err)
	require.NotContains(t, string(encoded), "must-not-be-logged")
}

func TestClaimPhaseLogsPreserveEmptyAndUnsuccessfulObservations(t *testing.T) {
	for _, tc := range []struct {
		name   string
		phases claimPhaseLogs
		want   []interface{}
	}{
		{name: "no observations", want: []interface{}{}},
		{name: "zero duration failure", phases: claimPhaseLogs{{Phase: runtimeslotclaim.PhaseProcdProbe}},
			want: []interface{}{map[string]interface{}{"phase": "procd_probe", "durationMicros": int64(0), "succeeded": false}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			encoder := zapcore.NewMapObjectEncoder()
			require.NoError(t, encoder.AddArray("claimPhases", tc.phases))
			require.Equal(t, tc.want, encoder.Fields["claimPhases"])
		})
	}
}

type failingClaimPhaseEncoder struct {
	zapcore.ArrayEncoder
	err error
}

func (encoder failingClaimPhaseEncoder) AppendObject(zapcore.ObjectMarshaler) error {
	return encoder.err
}

func TestClaimPhaseLogsReturnEncoderFailure(t *testing.T) {
	want := errors.New("cannot encode phase")
	phases := claimPhaseLogs{{Phase: runtimeslotclaim.PhaseNodeClaim, Succeeded: true}}
	require.ErrorIs(t, phases.MarshalLogArray(failingClaimPhaseEncoder{err: want}), want)
}
