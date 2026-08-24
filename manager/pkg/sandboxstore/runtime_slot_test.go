package sandboxstore

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/opencontainers/go-digest"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	"github.com/stretchr/testify/require"
)

func TestNormalizeRuntimeSlotRegistrationRequiresCanonicalIdentity(t *testing.T) {
	request := runtimeSlotTestRegistration("slot-1", "allocation-1")
	normalized, err := normalizeRegisterRuntimeSlotRequest(request)
	require.NoError(t, err)
	require.Equal(t, DefaultRuntimeSlotHeartbeatTTL, normalized.HeartbeatTTL)
	require.Equal(t, "unix:///run/sandbox0/slots/slot-1.sock", normalized.ControlEndpoint)

	changed := *request
	changed.CompatibilityDigest = "SHA256:not-canonical"
	_, err = normalizeRegisterRuntimeSlotRequest(&changed)
	require.ErrorContains(t, err, "canonical sha256")

	changed = *request
	changed.ControlEndpoint = "file:///tmp/control"
	_, err = normalizeRegisterRuntimeSlotRequest(&changed)
	require.ErrorContains(t, err, "scheme")

	changed = *request
	changed.ControlEndpoint = "unix://remote-host/run/control.sock"
	_, err = normalizeRegisterRuntimeSlotRequest(&changed)
	require.ErrorContains(t, err, "absolute path")
}

func TestNormalizeRuntimeSlotProofsClonesAndBoundsInputs(t *testing.T) {
	proof := bytes.Repeat([]byte{0x41}, 32)
	request := &ReportRuntimeSlotReadyRequest{
		SlotID: "slot", AllocationID: "allocation", NodeUID: "node", NodeBootID: "boot",
		RuntimeReadyDigest: proof, NetworkReadyDigest: proof, StorageReadyDigest: proof,
		HeartbeatTTL: time.Minute,
	}
	normalized, err := normalizeReportRuntimeSlotReadyRequest(request)
	require.NoError(t, err)
	proof[0] = 0
	require.Equal(t, byte(0x41), normalized.RuntimeReadyDigest[0])

	request.RuntimeReadyDigest = []byte("short")
	_, err = normalizeReportRuntimeSlotReadyRequest(request)
	require.ErrorContains(t, err, "32-byte")

	heartbeat := &HeartbeatRuntimeSlotRequest{
		SlotID: "slot", AllocationID: "allocation", NodeUID: "node", NodeBootID: "boot",
		TTL: 6 * time.Minute,
	}
	_, err = normalizeHeartbeatRuntimeSlotRequest(heartbeat)
	require.ErrorContains(t, err, "between 1s")
}

func TestNormalizeAcquireRuntimeSlotRequestUsesMillisecondTTLPrecision(t *testing.T) {
	request := &AcquireRuntimeSlotRequest{
		OperationID: "operation", ClaimID: "claim", SandboxID: "sandbox",
		FilesystemID: "filesystem", SourceGenerationID: "generation",
		CompatibilityDigest:       runtimeSlotTestRegistration("unused", "unused").CompatibilityDigest,
		RuntimeAssignmentRevision: strings.Repeat("ab", 32),
		NetworkPolicyDigest:       "sha256:" + strings.Repeat("cd", 32),
		ClaimTTL:                  1500*time.Millisecond + 900*time.Microsecond,
		Resources:                 runtimeSlotTestResources(),
	}
	normalized, err := normalizeAcquireRuntimeSlotRequest(request)
	require.NoError(t, err)
	require.Equal(t, 1500*time.Millisecond, normalized.ClaimTTL)
}

func TestNormalizeAcquireRuntimeSlotRequestRejectsNoncanonicalOperationBindings(t *testing.T) {
	valid := &AcquireRuntimeSlotRequest{
		OperationID: "operation", ClaimID: "claim", SandboxID: "sandbox",
		FilesystemID: "filesystem", SourceGenerationID: "generation",
		CompatibilityDigest:       runtimeSlotTestRegistration("unused", "unused").CompatibilityDigest,
		RuntimeAssignmentRevision: strings.Repeat("ab", 32),
		NetworkPolicyDigest:       "sha256:" + strings.Repeat("cd", 32),
		ClaimTTL:                  time.Second,
		Resources:                 runtimeSlotTestResources(),
	}
	tests := []struct {
		name   string
		mutate func(*AcquireRuntimeSlotRequest)
		error  string
	}{
		{
			name: "short runtime revision",
			mutate: func(request *AcquireRuntimeSlotRequest) {
				request.RuntimeAssignmentRevision = "ab"
			},
			error: "canonical 32-byte hexadecimal digest",
		},
		{
			name: "uppercase runtime revision",
			mutate: func(request *AcquireRuntimeSlotRequest) {
				request.RuntimeAssignmentRevision = strings.Repeat("AB", 32)
			},
			error: "canonical 32-byte hexadecimal digest",
		},
		{
			name: "network digest without algorithm",
			mutate: func(request *AcquireRuntimeSlotRequest) {
				request.NetworkPolicyDigest = strings.Repeat("cd", 32)
			},
			error: "canonical sha256 digest",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := *valid
			test.mutate(&request)
			_, err := normalizeAcquireRuntimeSlotRequest(&request)
			require.ErrorContains(t, err, test.error)
		})
	}
}

func runtimeSlotTestResources() protocol.RuntimeResourceRequest {
	return protocol.RuntimeResourceRequest{
		Version: protocol.RuntimeResourceRequestVersion, CPUMillicores: 1_000,
		MemoryBytes: 1 << 30, PIDsLimit: protocol.DefaultRuntimePIDsLimit,
	}
}

func registerRuntimeSlotWithTestCapacity(
	t *testing.T,
	ctx context.Context,
	store *PGSandboxStore,
	request *RegisterRuntimeSlotRequest,
) (*RuntimeSlot, error) {
	t.Helper()
	if _, err := store.RegisterRuntimeNodeCapacity(ctx, &RegisterRuntimeNodeCapacityRequest{
		ClusterID: request.ClusterID, NodeID: request.NodeID, NodeUID: request.NodeUID,
		NodeBootID: request.NodeBootID, CPUMillicores: 8_000, MemoryBytes: 16 << 30,
		CPUSetCPUs: "0-7", CPUSetMems: "0", TTL: time.Minute,
	}); err != nil {
		return nil, err
	}
	return store.RegisterRuntimeSlot(ctx, request)
}

func TestNormalizeFenceRuntimeSlotForReconcileRequest(t *testing.T) {
	normalized, err := normalizeFenceRuntimeSlotForReconcileRequest(&FenceRuntimeSlotForReconcileRequest{
		SlotID: " slot-1 ", ExpectedRevision: 3,
	})
	require.NoError(t, err)
	require.Equal(t, "slot-1", normalized.SlotID)

	_, err = normalizeFenceRuntimeSlotForReconcileRequest(&FenceRuntimeSlotForReconcileRequest{SlotID: "slot-1"})
	require.ErrorContains(t, err, "expected_revision")
}

func runtimeSlotTestRegistration(slotID, allocationID string) *RegisterRuntimeSlotRequest {
	return &RegisterRuntimeSlotRequest{
		SlotID: slotID, ClusterID: "cluster-a", AllocationID: allocationID,
		AllocationNamespace: "default", NodeID: "nomad-node-a", NodeUID: "node-a", NodeBootID: "boot-a",
		NetNSIdentity:       "netns-" + allocationID,
		ControlEndpoint:     "unix:///run/sandbox0/slots/" + slotID + ".sock",
		CompatibilityDigest: digest.FromString("amd64/runsc/directfs/2cpu/1g").String(),
	}
}
