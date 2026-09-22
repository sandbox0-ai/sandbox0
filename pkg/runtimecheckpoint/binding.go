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
	scope, err := BindCapture(operationID, source, assignment, compatibilityDigest, cpuFeaturesDigest)
	if err != nil {
		return Binding{}, err
	}
	if err := checkpoint.Validate(); err != nil {
		return Binding{}, fmt.Errorf("checkpoint RootFS generation: %w", err)
	}
	if checkpoint.GenerationID == source.InitialGeneration ||
		checkpoint.FilesystemID != source.Identity.RootFSID ||
		checkpoint.WriterEpoch != source.Identity.WriterEpoch ||
		checkpoint.SourceOCIDigest != source.Identity.SourceOCIDigest {
		return Binding{}, fmt.Errorf("checkpoint does not belong to the exact source writer and runtime")
	}
	rootfsPayload, err := json.Marshal(checkpoint)
	if err != nil {
		return Binding{}, err
	}
	result := scope.source
	result.RootFSGenerationID = checkpoint.GenerationID
	result.RootFSDescriptorDigest = digest.FromBytes(rootfsPayload).String()
	return result, result.Validate()
}

// BindCapture derives disposable upload identity from the existing exact source
// contracts. It does not bind a filesystem cut or authorize publication, fencing,
// or execution. The caller must own the regional capture and staging grants.
func BindCapture(operationID string, source rootfshandoff.StageRequest, assignment runtimecontrol.Assignment,
	compatibilityDigest, cpuFeaturesDigest string) (CaptureScope, error) {
	if err := source.ValidateDurableBinding(); err != nil {
		return CaptureScope{}, fmt.Errorf("checkpoint source binding: %w", err)
	}
	if source.Identity.WriterGrantToken != "" {
		return CaptureScope{}, fmt.Errorf("checkpoint source must not contain a bearer token")
	}
	revision, err := assignment.Revision()
	if err != nil {
		return CaptureScope{}, err
	}
	if source.Identity.RuntimeGeneration != strconv.FormatInt(assignment.RuntimeGeneration, 10) {
		return CaptureScope{}, fmt.Errorf("checkpoint does not belong to the exact source runtime")
	}
	sourceDigest, err := source.BindingDigest()
	if err != nil {
		return CaptureScope{}, err
	}
	result := Binding{
		OperationID: operationID, SandboxID: assignment.SandboxID, TeamID: assignment.TeamID,
		SourceBindingDigest:        digest.NewDigestFromBytes(digest.SHA256, sourceDigest[:]).String(),
		RuntimeCompatibilityDigest: compatibilityDigest,
		AssignmentRevision:         digest.NewDigestFromEncoded(digest.SHA256, revision).String(),
		CPUFeaturesDigest:          cpuFeaturesDigest,
	}
	if err := result.validateSource(); err != nil {
		return CaptureScope{}, err
	}
	return CaptureScope{source: result}, nil
}
