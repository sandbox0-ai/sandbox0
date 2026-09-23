package runtimeslot

import "testing"

func TestCheckpointImageCancellationCapabilityPreservesCanonicalOrdering(t *testing.T) {
	hello := testNodeChannelHello()
	hello.Capabilities = []NodeChannelCommandKind{
		NodeChannelCommandClaim, NodeChannelCommandCommandReady,
		NodeChannelCommandMigrationStagingReserve, NodeChannelCommandMigrationStagingRelease,
		NodeChannelCommandMigrationFailureStop, NodeChannelCommandMigrationFailureCleanup,
		NodeChannelCommandCheckpointImageCancel, NodeChannelCommandMigrationFailureFinalize,
		NodeChannelCommandCleanup,
	}
	if err := hello.Validate(); err != nil {
		t.Fatal(err)
	}
	hello.Capabilities[5], hello.Capabilities[6] = hello.Capabilities[6], hello.Capabilities[5]
	if err := hello.Validate(); err == nil {
		t.Fatal("out-of-order cancellation capability accepted")
	}
}
