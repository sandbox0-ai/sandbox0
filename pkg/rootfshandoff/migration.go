package rootfshandoff

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"strings"

	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
)

// MigrationRootFSCutRequest names the filesystem half of one completed memory
// capture. The source node must verify stopped execution and exact image
// custody before passing it to the session manager. It grants no new writer.
type MigrationRootFSCutRequest struct {
	OperationID          string `json:"operation_id"`
	CaptureRequestDigest string `json:"capture_request_digest"`
	SourceBindingDigest  string `json:"source_binding_digest"`
	GenerationID         string `json:"generation_id"`
}

func (r MigrationRootFSCutRequest) Validate() error {
	for _, value := range []string{r.OperationID, r.GenerationID} {
		if value == "" || len(value) > 256 || strings.TrimSpace(value) != value || strings.ContainsAny(value, "\x00\r\n") {
			return fmt.Errorf("migration RootFS operation and generation must be canonical identifiers")
		}
	}
	for _, value := range []string{r.CaptureRequestDigest, r.SourceBindingDigest} {
		decoded, err := hex.DecodeString(value)
		if err != nil || len(decoded) != sha256.Size || hex.EncodeToString(decoded) != value {
			return fmt.Errorf("migration RootFS binding must be a canonical SHA256 digest")
		}
	}
	return nil
}

// MigrationRootFSCut is immutable source-cut evidence. A local result is not
// a published regional head, physical cleanup proof or destination authority.
type MigrationRootFSCut struct {
	Request    MigrationRootFSCutRequest `json:"request"`
	Generation GenerationDescriptor      `json:"generation"`
	Sequence   uint64                    `json:"sequence"`
	Digest     string                    `json:"digest"`
}

func (r MigrationRootFSCut) digest() (string, error) {
	r.Digest = ""
	payload, err := json.Marshal(r)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:]), nil
}

func NewMigrationRootFSCut(request MigrationRootFSCutRequest, generation GenerationDescriptor, sequence uint64) (MigrationRootFSCut, error) {
	result := MigrationRootFSCut{Request: request, Generation: generation, Sequence: sequence}
	var err error
	result.Digest, err = result.digest()
	if err == nil {
		err = result.Validate()
	}
	return result, err
}

func (r MigrationRootFSCut) Validate() error {
	if err := r.Request.Validate(); err != nil {
		return err
	}
	if err := r.Generation.Validate(); err != nil {
		return err
	}
	if r.Sequence > math.MaxInt64 || r.Generation.GenerationID != r.Request.GenerationID || r.Generation.DurabilityState != rootfsblock.DurabilityS3 {
		return fmt.Errorf("migration RootFS cut changed its generation or sequence")
	}
	want, err := r.digest()
	if err != nil || r.Digest != want {
		return fmt.Errorf("migration RootFS cut digest changed")
	}
	return nil
}

// ValidateFor joins the cut to the original tokenless writer binding, including
// the format and base artifact. Consumers must use the full descriptor digest,
// not just the block mapping root, when binding this cut to an execution image.
func (r MigrationRootFSCut) ValidateFor(stage StageRequest, request MigrationRootFSCutRequest) error {
	if err := r.Validate(); err != nil {
		return err
	}
	if err := stage.ValidateDurableBinding(); err != nil {
		return err
	}
	binding, err := stage.BindingDigest()
	if err != nil {
		return err
	}
	if stage.Identity.WriterGrantToken != "" || stage.Generation == nil || stage.Generation.LocatorVersion == math.MaxInt64 ||
		r.Request != request || request.SourceBindingDigest != hex.EncodeToString(binding[:]) ||
		r.Generation.GenerationID == stage.InitialGeneration || r.Generation.FilesystemID != stage.Identity.RootFSID ||
		r.Generation.WriterEpoch != stage.Identity.WriterEpoch || r.Generation.SourceOCIDigest != stage.Generation.SourceOCIDigest ||
		r.Generation.BaseArtifactDigest != stage.Generation.BaseArtifactDigest || r.Generation.BaseBlockRoot != stage.Generation.BaseBlockRoot ||
		r.Generation.FormatGeneration != stage.Generation.FormatGeneration || r.Generation.LocatorVersion != stage.Generation.LocatorVersion+1 {
		return fmt.Errorf("migration RootFS cut does not match the exact source writer")
	}
	return nil
}
