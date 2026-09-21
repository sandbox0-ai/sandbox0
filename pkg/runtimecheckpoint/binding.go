package runtimecheckpoint

import (
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/opencontainers/go-digest"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshandoff"
	"github.com/sandbox0-ai/sandbox0/pkg/runtimecontrol"
)

// Bind derives image identity from existing runtime contracts. checkpoint
// must be the generation captured while execution was stopped, not the head
// from initial claim or a later independently captured filesystem snapshot.
// Checking that temporal condition belongs to the source custody journal.
func Bind(
	operationID string,
	source rootfshandoff.StageRequest,
	assignment runtimecontrol.Assignment,
	compatibilityDigest, cpuFeaturesDigest string,
	checkpoint rootfshandoff.GenerationDescriptor,
) (Binding, error) {
	if err := source.ValidateDurableBinding(); err != nil {
		return Binding{}, fmt.Errorf("checkpoint source binding: %w", err)
	}
	if source.Identity.WriterGrantToken != "" {
		return Binding{}, fmt.Errorf("checkpoint source must not contain a bearer token")
	}
	if err := checkpoint.Validate(); err != nil {
		return Binding{}, fmt.Errorf("checkpoint RootFS generation: %w", err)
	}
	revision, err := assignment.Revision()
	if err != nil {
		return Binding{}, err
	}
	if checkpoint.GenerationID == source.InitialGeneration ||
		checkpoint.FilesystemID != source.Identity.RootFSID ||
		checkpoint.WriterEpoch != source.Identity.WriterEpoch ||
		checkpoint.SourceOCIDigest != source.Identity.SourceOCIDigest ||
		source.Identity.RuntimeGeneration != strconv.FormatInt(assignment.RuntimeGeneration, 10) {
		return Binding{}, fmt.Errorf("checkpoint does not belong to the exact source writer and runtime")
	}
	sourceDigest, err := source.BindingDigest()
	if err != nil {
		return Binding{}, err
	}
	rootfsPayload, err := json.Marshal(checkpoint)
	if err != nil {
		return Binding{}, err
	}
	result := Binding{
		OperationID: operationID, SandboxID: assignment.SandboxID, TeamID: assignment.TeamID,
		SourceBindingDigest:        digest.NewDigestFromBytes(digest.SHA256, sourceDigest[:]).String(),
		RuntimeCompatibilityDigest: compatibilityDigest,
		AssignmentRevision:         digest.NewDigestFromEncoded(digest.SHA256, revision).String(),
		CPUFeaturesDigest:          cpuFeaturesDigest, RootFSGenerationID: checkpoint.GenerationID,
		RootFSDescriptorDigest: digest.FromBytes(rootfsPayload).String(),
	}
	if err := result.Validate(); err != nil {
		return Binding{}, err
	}
	return result, nil
}
