package template

import (
	"testing"

	api "github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshead"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTemplateImageRevisionIdentityIsImmutablePerSpec(t *testing.T) {
	tpl := &Template{TemplateID: "python", Scope: "team", TeamID: "team-1", Spec: api.SandboxTemplateSpec{MainContainer: api.ContainerSpec{Image: "python:3.13"}}}
	first, err := NewTemplateImageRevision(tpl)
	require.NoError(t, err)
	replayed, err := NewTemplateImageRevision(tpl)
	require.NoError(t, err)
	assert.Equal(t, first.RevisionID, replayed.RevisionID)

	tpl.Spec.EnvVars = map[string]string{"MODE": "test"}
	changed, err := NewTemplateImageRevision(tpl)
	require.NoError(t, err)
	assert.NotEqual(t, first.RevisionID, changed.RevisionID)
	assert.Equal(t, "team-1", changed.ImageFSStorageScope())
}

func TestPublicTemplateImageRevisionUsesPlatformReadOnlyScope(t *testing.T) {
	revision := &TemplateImageRevision{Scope: "public"}
	assert.Equal(t, rootfshead.PublicImageFSTeamID, revision.ImageFSStorageScope())
}

func TestTemplateWithImageRevisionIsClaimableOnlyAfterReadyHead(t *testing.T) {
	tpl := &Template{Status: &api.SandboxTemplateStatus{ImageRevision: &api.TemplateImageRevisionStatus{
		State: api.TemplateImageRevisionStateImporting,
	}}}
	assert.False(t, tpl.ReadyForClaim())
	tpl.Status.ImageRevision.State = api.TemplateImageRevisionStateReady
	assert.False(t, tpl.ReadyForClaim())
	tpl.Status.ImageRevision.ImageFSHeadID = "head-1"
	assert.True(t, tpl.ReadyForClaim())
}
