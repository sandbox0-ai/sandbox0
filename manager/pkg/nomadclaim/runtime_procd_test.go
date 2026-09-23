package nomadclaim

import (
	"github.com/opencontainers/go-digest"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/service"
	"github.com/sandbox0-ai/sandbox0/pkg/procdartifact"
	v1alpha1 "github.com/sandbox0-ai/sandbox0/pkg/sandboxspec"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestRuntimeProcdSelectionIsIndependentOfEmbeddedArtifact(t *testing.T) {
	f := newClaimServiceFixture(t)
	selected := &procdartifact.Artifact{Digest: digest.FromString("new platform daemon").String(), Protocol: f.config.RootFSProcdProtocol}
	f.config.RuntimeProcd = selected
	_, err := New(f.config)
	require.ErrorContains(t, err, "does not support node-mounted")
	for index := range f.config.RuntimeClasses.classes {
		f.config.RuntimeClasses.classes[index].Compatibility.DriverVersion = "0.3.0"
	}
	s, err := New(f.config)
	require.NoError(t, err)
	original := *selected
	selected.Digest = digest.FromString("changed after construction").String()
	req := &service.ClaimRequest{SandboxID: "sandbox-1", RuntimeGeneration: 67}
	a, err := s.runtimeAssignment(v1alpha1.SandboxTemplateSpec{}, req)
	require.NoError(t, err)
	require.Equal(t, original, *a.Procd)
	require.NotEqual(t, f.store.artifact.ProcdDigest, a.Procd.Digest)
	before, err := a.Revision()
	require.NoError(t, err)
	a.Procd.Digest = selected.Digest
	after, err := a.Revision()
	require.NoError(t, err)
	require.NotEqual(t, before, after)
	again, err := s.runtimeAssignment(v1alpha1.SandboxTemplateSpec{}, req)
	require.NoError(t, err)
	require.Equal(t, original, *again.Procd)
	f.config.RuntimeProcd = nil
	f.config.RootFSProcdDigest = procdartifact.PlaceholderDigest()
	_, err = New(f.config)
	require.ErrorContains(t, err, "placeholder RootFS")
}

func TestColdResumeUsesSelectedProcdWithoutChangingCarrierOrRootFS(t *testing.T) {
	f := newClaimServiceFixture(t)
	sandboxID := preparePausedNomadResume(t, f)
	for index := range f.config.RuntimeClasses.classes {
		f.config.RuntimeClasses.classes[index].Compatibility.DriverVersion = "0.3.0"
	}
	selected := &procdartifact.Artifact{Digest: digest.FromString("upgraded daemon").String(), Protocol: f.config.RootFSProcdProtocol}
	f.config.RuntimeProcd = selected
	var err error
	f.service, err = New(f.config)
	require.NoError(t, err)
	oldArtifact := *f.store.artifact
	response, err := f.service.ResumeSandboxAndWait(t.Context(), sandboxID)
	require.NoError(t, err)
	require.True(t, response.Resumed)
	require.Len(t, f.planner.requests, 1)
	request := f.planner.requests[0]
	require.Equal(t, *selected, *request.Runtime.Procd)
	require.EqualValues(t, 2, request.Runtime.RuntimeGeneration)
	require.Equal(t, f.runtimeClass.CompatibilityDigest, request.CompatibilityDigest)
	require.Equal(t, oldArtifact, *f.store.artifact)
}
