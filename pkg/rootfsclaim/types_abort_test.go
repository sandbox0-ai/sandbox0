package rootfsclaim

import (
	"encoding/hex"
	"testing"

	"github.com/sandbox0-ai/sandbox0/pkg/rootfshandoff"
	"github.com/stretchr/testify/require"
)

func TestPrelaunchAbortProofDigestIsCanonical(t *testing.T) {
	proof := PrelaunchAbortProof{
		Version: 1, Parent: "sha256:parent", ClaimID: "claim", WriterGrantID: "grant",
		WriterEpoch: 7, BindingVersion: 1,
		BindingDigest: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		NodeUID:       "node", BootID: "boot", RuntimeGeneration: "runtime",
		PodUID: "pod", PodSandboxID: "sandbox", SlotNonce: "slot", RootFSID: "rootfs",
		SnapshotterState: rootfshandoff.StateTombstoned,
	}
	digest, err := proof.Digest()
	require.NoError(t, err)
	response := PrelaunchAbortResponse{Proof: proof, ProofDigest: hex.EncodeToString(digest[:])}
	require.NoError(t, response.Validate())

	response.Proof.RootFSID = "other"
	require.ErrorContains(t, response.Validate(), "does not match")
}

func TestPrelaunchAbortProofRejectsNonTerminalSnapshotterState(t *testing.T) {
	proof := PrelaunchAbortProof{Version: 1, SnapshotterState: rootfshandoff.StateReady}
	require.Error(t, proof.Validate())
}
