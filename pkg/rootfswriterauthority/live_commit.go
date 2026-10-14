package rootfswriterauthority

import (
	"fmt"
	"math"
	"net/url"
	"strconv"
	"strings"

	"github.com/opencontainers/go-digest"
)

type LiveCommitProgress struct {
	CommittedGenerationID string `json:"committed_generation_id"`
	CommittedSequence     uint64 `json:"committed_sequence"`
	InstalledGenerationID string `json:"installed_generation_id"`
	InstalledSequence     uint64 `json:"installed_sequence"`
	OperationID           string `json:"operation_id"`
	RequestDigest         []byte `json:"-"`
}

// Observation distinguishes an uncommitted pending node operation from a lost
// reply after a regional commit, including when retirement has already fenced
// new publication. It does not substitute for a physical terminal proof.
type LiveCommitObservation struct {
	WriterState               string             `json:"writer_state"`
	Progress                  LiveCommitProgress `json:"progress"`
	CommittedDescriptorDigest string             `json:"committed_descriptor_digest,omitempty"`
}

func (o LiveCommitObservation) Validate(grantID, initialGenerationID string) error {
	switch o.WriterState {
	case "consumed", "retiring", "retired", "canceled":
	default:
		return fmt.Errorf("invalid live writer state")
	}
	p := o.Progress
	if p.CommittedGenerationID == "" || p.InstalledGenerationID == "" || p.CommittedSequence > math.MaxInt64 || p.InstalledSequence > p.CommittedSequence {
		return fmt.Errorf("invalid live commit progress")
	}
	if p.CommittedSequence == 0 {
		if p.OperationID != "" || p.CommittedGenerationID != initialGenerationID || p.InstalledGenerationID != initialGenerationID {
			return fmt.Errorf("invalid launch progress")
		}
	} else {
		sequence, err := LiveCommitSequence(grantID, p.OperationID)
		if err != nil || sequence != p.CommittedSequence {
			return fmt.Errorf("live progress does not match its grant and sequence")
		}
		if o.WriterState == "consumed" || o.WriterState == "retiring" {
			d, err := digest.Parse(o.CommittedDescriptorDigest)
			if err != nil || d.Algorithm() != digest.SHA256 || d.Validate() != nil {
				return fmt.Errorf("live observation lacks the immutable committed descriptor digest")
			}
		}
	}
	if p.InstalledSequence == 0 && p.InstalledGenerationID != initialGenerationID ||
		(p.InstalledSequence == p.CommittedSequence) != (p.InstalledGenerationID == p.CommittedGenerationID) {
		return fmt.Errorf("invalid installed live base")
	}
	return nil
}

// LiveCommitOperationID is stable across lost replies and daemon restarts. The
// grant's immutable binding supplies the epoch/node scope; sequences never reset
// when the local WAL rotates.
func LiveCommitOperationID(grantID string, sequence uint64) string {
	return "live/" + url.PathEscape(grantID) + "/" + strconv.FormatUint(sequence, 10)
}

func LiveCommitSequence(grantID, operationID string) (uint64, error) {
	prefix := "live/" + url.PathEscape(grantID) + "/"
	if strings.TrimSpace(grantID) == "" || !strings.HasPrefix(operationID, prefix) {
		return 0, fmt.Errorf("live operation does not match its grant")
	}
	sequence, err := strconv.ParseUint(strings.TrimPrefix(operationID, prefix), 10, 64)
	if err != nil || sequence == 0 || sequence > math.MaxInt64 || operationID != LiveCommitOperationID(grantID, sequence) {
		return 0, fmt.Errorf("live cut sequence is not canonical")
	}
	return sequence, nil
}
