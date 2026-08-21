package sandboxstore

import (
	"bytes"
	"testing"
)

func TestNomadForkDigestPreservesLegacyEncodingWhileValidationBindsResources(t *testing.T) {
	left := rootFSTestSandboxRecord("fork-target", "team-1")
	left.RuntimeBackend = SandboxRuntimeBackendNomad
	left.DesiredState = SandboxDesiredStatePaused
	left.OwnerKind = "team"
	left.ResourceMillicpu = 1000
	left.ResourceMemoryMiB = 1024
	right := *left
	right.ResourceMillicpu = 2000
	right.ResourceMemoryMiB = 2048

	leftDigest, err := NomadSandboxForkTargetRecordDigest(left)
	if err != nil {
		t.Fatal(err)
	}
	rightDigest, err := NomadSandboxForkTargetRecordDigest(&right)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(leftDigest, rightDigest) {
		t.Fatal("numeric metering fields changed the rolling-compatible fork digest")
	}
	if nomadForkTargetMatches(left, &right) {
		t.Fatal("fork target validation ignored numeric metering resources")
	}
}
