package rootfswriterauthority

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestConsumeRequestValidatesCanonicalDigest(t *testing.T) {
	request := ConsumeRequest{
		WriterEpoch: 1, BindingVersion: 1,
		BindingDigest: strings.Repeat("ab", 32), WriterToken: "token",
	}
	require.NoError(t, request.Validate())
	request.BindingDigest = strings.ToUpper(request.BindingDigest)
	require.Error(t, request.Validate())
}

func TestLeaseObservationRequiresAuthorityOrderedTimes(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).UTC()
	observation := LeaseObservation{
		ServerTime: now, RenewAfter: now.Add(time.Minute), LeaseExpiresAt: now.Add(2 * time.Minute),
	}
	require.NoError(t, observation.Validate())
	observation.RenewAfter = observation.LeaseExpiresAt
	require.Error(t, observation.Validate())
	observation.RenewAfter = now.Add(-time.Second)
	require.Error(t, observation.Validate())
}

func TestRenewRequestUsesIdentityOnlyWireContract(t *testing.T) {
	request := RenewRequest{
		WriterEpoch: 1, BindingVersion: 1,
		BindingDigest: strings.Repeat("ab", 32),
	}
	require.NoError(t, request.Validate())
	payload, err := json.Marshal(request)
	require.NoError(t, err)
	require.JSONEq(t, `{
		"writer_epoch": 1,
		"binding_version": 1,
		"binding_digest": "`+strings.Repeat("ab", 32)+`"
	}`, string(payload))
	require.NotContains(t, string(payload), "lease")
	require.NotContains(t, string(payload), "token")
	require.Equal(t, "/internal/v1/rootfs-writer-grants/grant-1/renew", RenewPath("grant-1"))
	require.Equal(t, "/internal/v1/rootfs-writer-grants/grant-1/fork-running", RunningForkPath("grant-1"))
}

func TestRenewRequestRejectsInvalidBinding(t *testing.T) {
	request := RenewRequest{
		WriterEpoch: 1, BindingVersion: 1,
		BindingDigest: "short",
	}
	require.ErrorContains(t, request.Validate(), "binding_digest")
}

func TestBatchRenewRequestAndResponseRequireUniqueExactItems(t *testing.T) {
	item := BatchRenewItem{GrantID: "grant-1", RenewRequest: RenewRequest{
		WriterEpoch: 1, BindingVersion: 1, BindingDigest: strings.Repeat("ab", 32),
	}}
	require.NoError(t, (BatchRenewRequest{Items: []BatchRenewItem{item}}).Validate())
	require.Error(t, (BatchRenewRequest{Items: []BatchRenewItem{item, item}}).Validate())

	now := time.Unix(1_700_000_000, 0).UTC()
	observation := LeaseObservation{ServerTime: now, RenewAfter: now.Add(time.Minute), LeaseExpiresAt: now.Add(2 * time.Minute)}
	response := BatchRenewResponse{Results: []BatchRenewResult{{GrantID: "grant-1", Observation: &observation}}}
	require.NoError(t, response.Validate(1))
	response.Results[0] = BatchRenewResult{GrantID: "grant-1", ErrorCode: RenewErrorUnavailable, Error: "unavailable"}
	require.NoError(t, response.Validate(1))
	response.Results[0].ErrorCode = "unknown"
	require.Error(t, response.Validate(1))
}

func TestTerminalRequestContainsOnlyImmutableBinding(t *testing.T) {
	request := TerminalRequest{
		WriterEpoch: 7, BindingVersion: 1,
		BindingDigest: strings.Repeat("ab", 32),
	}
	require.NoError(t, request.Validate())
	payload, err := json.Marshal(request)
	require.NoError(t, err)
	require.JSONEq(t, `{
		"writer_epoch": 7,
		"binding_version": 1,
		"binding_digest": "`+strings.Repeat("ab", 32)+`"
	}`, string(payload))
	require.NotContains(t, string(payload), "state")
	require.NotContains(t, string(payload), "token")
	require.Equal(t,
		"/internal/v1/rootfs-writer-grants/grant%20id+percent%25/terminal",
		TerminalPath("grant id+percent%"),
	)
}
