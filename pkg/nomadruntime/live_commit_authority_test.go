package nomadruntime

import (
	"context"
	"testing"

	managerauthority "github.com/sandbox0-ai/sandbox0/manager/pkg/rootfswriterauthority"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshandoff"
	rootfssession "github.com/sandbox0-ai/sandbox0/pkg/rootfssession"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/rootfswriterauthority"
	"github.com/stretchr/testify/require"
)

type recordingLiveClient struct {
	stage   rootfshandoff.StageRequest
	publish managerauthority.PublishLiveCommitRequest
	install managerauthority.InstallLiveCommitRequest
}

func (c *recordingLiveClient) PublishLiveCommit(_ context.Context, stage rootfshandoff.StageRequest, r managerauthority.PublishLiveCommitRequest) (protocol.LiveCommitProgress, error) {
	c.stage, c.publish = stage, r
	return protocol.LiveCommitProgress{CommittedGenerationID: r.Generation.ID, CommittedSequence: r.CutSequence}, nil
}
func (c *recordingLiveClient) InstallLiveCommit(_ context.Context, stage rootfshandoff.StageRequest, r managerauthority.InstallLiveCommitRequest) (protocol.LiveCommitProgress, error) {
	c.stage, c.install = stage, r
	return protocol.LiveCommitProgress{InstalledGenerationID: r.GenerationID, InstalledSequence: r.CutSequence}, nil
}
func (c *recordingLiveClient) ObserveLiveCommit(_ context.Context, stage rootfshandoff.StageRequest) (protocol.LiveCommitObservation, error) {
	c.stage = stage
	return protocol.LiveCommitObservation{WriterState: "retiring"}, nil
}

func TestLiveCommitAuthorityKeepsLaunchBindingAndUsesCommittedParent(t *testing.T) {
	client := &recordingLiveClient{}
	adapter := liveCommitAuthorityAdapter{client: client}
	stage := rootfshandoff.StageRequest{InitialGeneration: "launch", Identity: rootfshandoff.Identity{WriterGrantID: "grant", WriterEpoch: 3}}
	publication := rootfssession.LiveCommitPublication{OperationID: "live/grant/5", CutSequence: 5, ExpectedCommittedGenerationID: "previous-remote", BaseGenerationID: "previous-installed",
		Generation: rootfshandoff.GenerationDescriptor{Version: rootfshandoff.GenerationDescriptorVersion, GenerationID: "next-remote", FilesystemID: "fs", SourceOCIDigest: "oci", BaseArtifactDigest: "artifact", BaseBlockRoot: "base-root", CurrentBlockHead: "next-root", WriterEpoch: 3, FormatGeneration: 2, DurabilityState: "s3_materialized", LocatorVersion: 1, Descriptor: []byte("descriptor")}}
	progress, err := adapter.PublishLiveRootFS(t.Context(), stage, publication)
	require.NoError(t, err)
	require.Equal(t, publication.Generation.GenerationID, progress.CommittedGenerationID)
	require.Equal(t, stage, client.stage)
	require.Equal(t, "previous-remote", client.publish.Generation.ParentGenerationID)
	require.Equal(t, "previous-installed", client.publish.BaseGenerationID)
	require.Equal(t, publication.Generation.Descriptor, client.publish.Generation.Descriptor)
	require.Equal(t, publication.Generation.BaseArtifactDigest, client.publish.Generation.BaseArtifactDigest)
	require.Equal(t, publication.Generation.SourceOCIDigest, client.publish.Generation.SourceOCIDigest)
	require.Equal(t, int64(3), client.publish.Generation.WriterEpoch)
	_, err = adapter.InstallLiveRootFS(t.Context(), stage, rootfssession.LiveCommitInstallation{OperationID: publication.OperationID, GenerationID: publication.Generation.GenerationID, CutSequence: 5})
	require.NoError(t, err)
	require.Equal(t, publication.OperationID, client.install.OperationID)
	require.Equal(t, "next-remote", client.install.GenerationID)
	observation, err := adapter.ObserveLiveCommit(t.Context(), stage)
	require.NoError(t, err)
	require.Equal(t, "retiring", observation.WriterState)
	require.Equal(t, "launch", client.stage.InitialGeneration)
}
