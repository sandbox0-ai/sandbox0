package sandboxstore

import (
	"bytes"
	"testing"
)

func TestNomadForkDigestPreservesLegacyEncodingWhileValidationBindsResources(t *testing.T) {
	left := rootFSTestSandboxRecord("fork-target", "team-1")
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

func TestNomadForkAllowsOnlyStandardToPrivilegedClassUpgrade(t *testing.T) {
	source := rootFSTestSandboxRecord("source", "team-1")
	source.TemplateSpec.MainContainer.SecurityClass = "standard"
	target := *source
	target.TemplateSpec = *source.TemplateSpec.DeepCopy()
	target.TemplateSpec.MainContainer.SecurityClass = "privileged"
	if !nomadForkTemplateSpecDerivedFromSource(source, &target) {
		t.Fatal("new fork did not accept the privileged class")
	}
	target.TemplateSpec.MainContainer.Image = "different"
	if nomadForkTemplateSpecDerivedFromSource(source, &target) {
		t.Fatal("fork accepted an unrelated template change")
	}
}
