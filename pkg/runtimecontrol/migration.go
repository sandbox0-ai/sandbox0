package runtimecontrol

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"strings"
)

// MigrationAssignment is an internal, exact generation handover. It must be
// delivered only under manager's migration execution authorization. It is not
// part of the public sandbox or template API and cannot change workload config.
type MigrationAssignment struct {
	OperationID      string     `json:"operation_id"`
	SourceGeneration int64      `json:"source_generation"`
	SourceRevision   string     `json:"source_revision"`
	Target           Assignment `json:"target"`
}

func (m MigrationAssignment) Validate() error {
	if m.OperationID == "" || len(m.OperationID) > 256 ||
		strings.TrimSpace(m.OperationID) != m.OperationID || strings.ContainsAny(m.OperationID, "\x00\r\n") {
		return fmt.Errorf("migration operation identity is invalid")
	}
	if m.SourceGeneration <= 0 || m.SourceGeneration == math.MaxInt64 ||
		m.Target.RuntimeGeneration != m.SourceGeneration+1 {
		return fmt.Errorf("migration must advance the exact source generation once")
	}
	if err := m.Target.Validate(); err != nil {
		return err
	}
	source := m.Target
	source.RuntimeGeneration = m.SourceGeneration
	revision, err := source.Revision()
	if err != nil {
		return err
	}
	if revision != m.SourceRevision {
		return fmt.Errorf("migration cannot change the source assignment except its generation")
	}
	return nil
}

func (m MigrationAssignment) Digest() (string, error) {
	if err := m.Validate(); err != nil {
		return "", err
	}
	payload, err := json.Marshal(m)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:]), nil
}
