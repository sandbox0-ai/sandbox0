package rootfsrebase

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/opencontainers/go-digest"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
)

const (
	WorkerProtocolVersion      = 1
	MaxWorkerChangedBlocks     = 4 << 20
	MaxWorkerRollbackRetention = 7 * 24 * time.Hour
)

// WorkerRequest is the immutable regional authority supplied to one
// privileged offline rebase execution. It contains no object-store credential.
type WorkerRequest struct {
	Version                    int    `json:"version"`
	OperationID                string `json:"operation_id"`
	SandboxID                  string `json:"sandbox_id"`
	TeamID                     string `json:"team_id"`
	FilesystemID               string `json:"filesystem_id"`
	SourceGenerationID         string `json:"source_generation_id"`
	SourceOCIDigest            string `json:"source_oci_digest"`
	SourceBaseArtifactDigest   string `json:"source_base_artifact_digest"`
	SourceBaseBlockRoot        string `json:"source_base_block_root"`
	SourceCurrentBlockHead     string `json:"source_current_block_head"`
	SourceFormatGeneration     int    `json:"source_format_generation"`
	SourceLocatorVersion       int64  `json:"source_locator_version"`
	SourceBaseDescriptor       []byte `json:"source_base_descriptor"`
	SourceGenerationDescriptor []byte `json:"source_generation_descriptor"`
	TargetGenerationID         string `json:"target_generation_id"`
	TargetSourceOCIDigest      string `json:"target_source_oci_digest"`
	TargetBaseArtifactDigest   string `json:"target_base_artifact_digest"`
	TargetBaseBlockRoot        string `json:"target_base_block_root"`
	TargetFormatGeneration     int    `json:"target_format_generation"`
	TargetWriterEpoch          int64  `json:"target_writer_epoch"`
	TargetBaseDescriptor       []byte `json:"target_base_descriptor"`
	RollbackExpiresAt          string `json:"rollback_expires_at"`
	MaxChangedBlocks           int    `json:"max_changed_blocks"`
}

// Validate rejects mutable, ambiguous, or incompatible generation bindings
// before the worker creates a branch, mount, or immutable object.
func (r WorkerRequest) Validate() error {
	if r.Version != WorkerProtocolVersion {
		return fmt.Errorf("unsupported rebase worker protocol version %d", r.Version)
	}
	for name, value := range map[string]string{
		"operation_id": r.OperationID, "sandbox_id": r.SandboxID, "team_id": r.TeamID,
		"filesystem_id": r.FilesystemID, "source_generation_id": r.SourceGenerationID,
		"target_generation_id": r.TargetGenerationID,
	} {
		if value == "" || strings.TrimSpace(value) != value || len(value) > 512 {
			return fmt.Errorf("%s is required, canonical, and at most 512 bytes", name)
		}
	}
	for name, value := range map[string]string{
		"source_oci_digest":           r.SourceOCIDigest,
		"source_base_artifact_digest": r.SourceBaseArtifactDigest,
		"source_base_block_root":      r.SourceBaseBlockRoot,
		"source_current_block_head":   r.SourceCurrentBlockHead,
		"target_source_oci_digest":    r.TargetSourceOCIDigest,
		"target_base_artifact_digest": r.TargetBaseArtifactDigest,
		"target_base_block_root":      r.TargetBaseBlockRoot,
	} {
		parsed, err := digest.Parse(value)
		if err != nil || parsed.Algorithm() != digest.SHA256 || parsed.String() != value {
			return fmt.Errorf("%s must be a canonical sha256 digest", name)
		}
	}
	if r.SourceBaseArtifactDigest == r.TargetBaseArtifactDigest {
		return fmt.Errorf("source and target Base artifacts must differ")
	}
	if r.SourceFormatGeneration <= 0 || r.SourceFormatGeneration != r.TargetFormatGeneration ||
		r.SourceLocatorVersion <= 0 || r.SourceLocatorVersion == math.MaxInt64 || r.TargetWriterEpoch <= 0 {
		return fmt.Errorf("format generation, locator version, or target writer epoch is invalid")
	}
	if r.MaxChangedBlocks <= 0 || r.MaxChangedBlocks > MaxWorkerChangedBlocks {
		return fmt.Errorf("max_changed_blocks must be between 1 and %d", MaxWorkerChangedBlocks)
	}
	deadline, err := time.Parse(time.RFC3339Nano, r.RollbackExpiresAt)
	if err != nil || deadline.IsZero() || deadline.UTC().Format(time.RFC3339Nano) != r.RollbackExpiresAt {
		return fmt.Errorf("rollback_expires_at must be canonical UTC RFC3339Nano")
	}
	sourceBase, err := rootfsblock.DecodeDescriptor(r.SourceBaseDescriptor)
	if err != nil {
		return fmt.Errorf("source Base descriptor: %w", err)
	}
	source, err := rootfsblock.DecodeDescriptor(r.SourceGenerationDescriptor)
	if err != nil {
		return fmt.Errorf("source generation descriptor: %w", err)
	}
	targetBase, err := rootfsblock.DecodeDescriptor(r.TargetBaseDescriptor)
	if err != nil {
		return fmt.Errorf("target Base descriptor: %w", err)
	}
	if sourceBase.MappingRoot.RootDigest != r.SourceBaseBlockRoot ||
		source.MappingRoot.RootDigest != r.SourceCurrentBlockHead ||
		targetBase.MappingRoot.RootDigest != r.TargetBaseBlockRoot {
		return fmt.Errorf("generation descriptor root does not match its authority identity")
	}
	if sourceBase.LogicalSizeBytes != source.LogicalSizeBytes ||
		sourceBase.LogicalSizeBytes != targetBase.LogicalSizeBytes ||
		sourceBase.BlockSizeBytes != source.BlockSizeBytes ||
		sourceBase.BlockSizeBytes != targetBase.BlockSizeBytes {
		return fmt.Errorf("source and target generation block geometry differs")
	}
	return nil
}

// Digest returns the stable request identity persisted by the node journal.
func (r WorkerRequest) Digest() (string, error) {
	if err := r.Validate(); err != nil {
		return "", err
	}
	payload, err := json.Marshal(r)
	if err != nil {
		return "", err
	}
	return digest.FromBytes(payload).String(), nil
}

// WorkerResult is an S3-materialized target generation plus the file-aware
// apply health proof. PostgreSQL remains authoritative for installing it.
type WorkerResult struct {
	Version            int         `json:"version"`
	RequestDigest      string      `json:"request_digest"`
	GenerationID       string      `json:"generation_id"`
	FilesystemID       string      `json:"filesystem_id"`
	ParentGenerationID string      `json:"parent_generation_id"`
	SourceOCIDigest    string      `json:"source_oci_digest"`
	BaseArtifactDigest string      `json:"base_artifact_digest"`
	BaseBlockRoot      string      `json:"base_block_root"`
	CurrentBlockHead   string      `json:"current_block_head"`
	WriterEpoch        int64       `json:"writer_epoch"`
	FormatGeneration   int         `json:"format_generation"`
	DurabilityState    string      `json:"durability_state"`
	LocatorVersion     int64       `json:"locator_version"`
	Descriptor         []byte      `json:"descriptor"`
	HealthCheckDigest  []byte      `json:"health_check_digest"`
	DirtyBlocks        int         `json:"dirty_blocks"`
	PublishedObjects   int         `json:"published_objects"`
	PublishedBytes     int64       `json:"published_bytes"`
	Apply              ApplyResult `json:"apply"`
	ProofDigest        string      `json:"proof_digest"`
}

// SealProof binds the immutable output and apply proof to the exact request.
func (r *WorkerResult) SealProof() error {
	if r == nil {
		return fmt.Errorf("rebase worker result is required")
	}
	r.ProofDigest = ""
	payload, err := json.Marshal(r)
	if err != nil {
		return err
	}
	r.ProofDigest = digest.FromBytes(payload).String()
	return nil
}

// ValidateFor verifies a cached node result before it can be sent to the
// regional publication transaction.
func (r WorkerResult) ValidateFor(request WorkerRequest) error {
	if err := request.Validate(); err != nil {
		return err
	}
	requestDigest, err := request.Digest()
	if err != nil {
		return err
	}
	if r.Version != WorkerProtocolVersion || r.RequestDigest != requestDigest ||
		r.GenerationID != request.TargetGenerationID || r.FilesystemID != request.FilesystemID ||
		r.ParentGenerationID != request.SourceGenerationID ||
		r.SourceOCIDigest != request.TargetSourceOCIDigest ||
		r.BaseArtifactDigest != request.TargetBaseArtifactDigest ||
		r.BaseBlockRoot != request.TargetBaseBlockRoot || r.WriterEpoch != request.TargetWriterEpoch ||
		r.FormatGeneration != request.TargetFormatGeneration ||
		r.LocatorVersion != request.SourceLocatorVersion+1 ||
		r.DurabilityState != rootfsblock.DurabilityS3 || r.DirtyBlocks < 0 ||
		r.DirtyBlocks > request.MaxChangedBlocks || r.PublishedObjects < 0 || r.PublishedBytes < 0 {
		return fmt.Errorf("rebase worker result identity does not match its request")
	}
	descriptor, err := rootfsblock.DecodeDescriptor(r.Descriptor)
	if err != nil {
		return fmt.Errorf("target generation descriptor: %w", err)
	}
	targetBase, err := rootfsblock.DecodeDescriptor(request.TargetBaseDescriptor)
	if err != nil || descriptor.CompositeTail != nil ||
		descriptor.MappingRoot.RootDigest != r.CurrentBlockHead ||
		descriptor.LogicalSizeBytes != targetBase.LogicalSizeBytes ||
		descriptor.BlockSizeBytes != targetBase.BlockSizeBytes {
		return fmt.Errorf("target generation descriptor is not a materialized target branch")
	}
	if err := r.Apply.Validate(); err != nil {
		return fmt.Errorf("rebase apply result: %w", err)
	}
	health, err := r.Apply.HealthProofBytes()
	if err != nil || !bytes.Equal(health, r.HealthCheckDigest) {
		return fmt.Errorf("rebase health-check digest does not match apply proof")
	}
	parsedProof, err := digest.Parse(r.ProofDigest)
	if err != nil || parsedProof.Algorithm() != digest.SHA256 || parsedProof.String() != r.ProofDigest {
		return fmt.Errorf("rebase worker proof digest is invalid")
	}
	clone := r
	clone.ProofDigest = ""
	payload, err := json.Marshal(clone)
	if err != nil || digest.FromBytes(payload).String() != r.ProofDigest {
		return fmt.Errorf("rebase worker proof digest does not match its result")
	}
	return nil
}
