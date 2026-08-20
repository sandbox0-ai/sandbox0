// Copyright 2026 Sandbox0 Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package driver

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/containerd/errdefs"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	bolt "go.etcd.io/bbolt"
)

const (
	runtimeSlotJournalVersion   = 1
	runtimeSlotProofRetention   = 24 * time.Hour
	runtimeSlotJournalMaxIDSize = 512
)

var runtimeSlotJournalBucket = []byte("runtime-slots-v1")

// runtimeSlotJournalRegistration is the durable node-local identity created
// before a warm slot is exposed as ready to the regional authority.
type runtimeSlotJournalRegistration struct {
	Version          int    `json:"version"`
	SlotID           string `json:"slot_id"`
	ClusterID        string `json:"cluster_id"`
	AllocationID     string `json:"allocation_id"`
	NodeID           string `json:"node_id"`
	NodeBootID       string `json:"node_boot_id"`
	NetNSPath        string `json:"netns_path"`
	NetNSIdentity    string `json:"netns_identity"`
	NetworkChain     string `json:"network_chain"`
	RunscContainerID string `json:"runsc_container_id"`
	StableMount      string `json:"stable_mount"`
	StableMountID    string `json:"stable_mount_id"`
	MountNamespaceID string `json:"mount_namespace_id"`
}

func (r runtimeSlotJournalRegistration) Validate() error {
	if r.Version != runtimeSlotJournalVersion {
		return fmt.Errorf("unsupported runtime slot journal version %d", r.Version)
	}
	for name, value := range map[string]string{
		"slot_id": r.SlotID, "cluster_id": r.ClusterID, "allocation_id": r.AllocationID,
		"node_id": r.NodeID, "node_boot_id": r.NodeBootID, "netns_identity": r.NetNSIdentity,
		"network_chain": r.NetworkChain, "runsc_container_id": r.RunscContainerID,
		"stable_mount_id": r.StableMountID, "mount_namespace_id": r.MountNamespaceID,
	} {
		if value == "" || strings.TrimSpace(value) != value || len(value) > runtimeSlotJournalMaxIDSize {
			return fmt.Errorf("%s is required, canonical, and at most %d bytes", name, runtimeSlotJournalMaxIDSize)
		}
	}
	for name, value := range map[string]string{"netns_path": r.NetNSPath, "stable_mount": r.StableMount} {
		if len(value) > 4096 || !filepath.IsAbs(value) || filepath.Clean(value) == string(filepath.Separator) || filepath.Clean(value) != value {
			return fmt.Errorf("%s must be a canonical non-root absolute path", name)
		}
	}
	if r.RunscContainerID != protocol.NomadRunscContainerID(r.SlotID) ||
		r.NetworkChain != networkChainName(r.RunscContainerID) {
		return fmt.Errorf("runtime slot runsc and network identities are not deterministic")
	}
	return nil
}

type runtimeSlotJournalRecord struct {
	Version      int                                 `json:"version"`
	Registration runtimeSlotJournalRegistration      `json:"registration"`
	Cleanup      *protocol.NodeCleanupControlRequest `json:"cleanup,omitempty"`
	Proof        *protocol.NodeCleanupControlProof   `json:"proof,omitempty"`
	CreatedAt    string                              `json:"created_at"`
	UpdatedAt    string                              `json:"updated_at"`
	CompletedAt  string                              `json:"completed_at,omitempty"`
}

type runtimeSlotJournal struct {
	db        *bolt.DB
	retention time.Duration
}

func newRuntimeSlotJournal(path string, retention time.Duration) (*runtimeSlotJournal, error) {
	path = filepath.Clean(strings.TrimSpace(path))
	if !filepath.IsAbs(path) || path == string(filepath.Separator) {
		return nil, fmt.Errorf("runtime slot journal path must be a non-root absolute path")
	}
	if retention <= 0 {
		return nil, fmt.Errorf("runtime slot proof retention must be positive")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return nil, fmt.Errorf("create runtime slot journal directory: %w", err)
	}
	if info, err := os.Lstat(path); err == nil {
		if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
			return nil, fmt.Errorf("runtime slot journal must be a regular file")
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("inspect runtime slot journal: %w", err)
	}
	db, err := bolt.Open(path, 0o600, &bolt.Options{Timeout: 2 * time.Second})
	if err != nil {
		return nil, fmt.Errorf("open runtime slot journal: %w", err)
	}
	if err := os.Chmod(path, 0o600); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("secure runtime slot journal: %w", err)
	}
	if err := db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(runtimeSlotJournalBucket)
		return err
	}); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("initialize runtime slot journal: %w", err)
	}
	return &runtimeSlotJournal{db: db, retention: retention}, nil
}

func (j *runtimeSlotJournal) Close() error {
	if j == nil || j.db == nil {
		return nil
	}
	return j.db.Close()
}

func (j *runtimeSlotJournal) Ping() error {
	if j == nil || j.db == nil {
		return fmt.Errorf("runtime slot journal is unavailable: %w", errdefs.ErrUnavailable)
	}
	return j.db.View(func(tx *bolt.Tx) error {
		if tx.Bucket(runtimeSlotJournalBucket) == nil {
			return fmt.Errorf("runtime slot journal bucket is absent: %w", errdefs.ErrUnavailable)
		}
		return nil
	})
}

func (j *runtimeSlotJournal) Register(registration runtimeSlotJournalRegistration) error {
	if j == nil || j.db == nil {
		return fmt.Errorf("runtime slot journal is unavailable: %w", errdefs.ErrUnavailable)
	}
	if err := registration.Validate(); err != nil {
		return fmt.Errorf("validate runtime slot journal registration: %w: %w", err, errdefs.ErrInvalidArgument)
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	return j.db.Update(func(tx *bolt.Tx) error {
		bucket, err := runtimeSlotJournalBucketFrom(tx)
		if err != nil {
			return err
		}
		stored, err := decodeRuntimeSlotJournalRecord(bucket.Get([]byte(registration.SlotID)))
		if err != nil && !errdefs.IsNotFound(err) {
			return err
		}
		if err == nil {
			if stored.Registration != registration {
				return fmt.Errorf("runtime slot journal registration changed: %w", errdefs.ErrAlreadyExists)
			}
			return nil
		}
		record := runtimeSlotJournalRecord{
			Version: runtimeSlotJournalVersion, Registration: registration,
			CreatedAt: now, UpdatedAt: now,
		}
		return putRuntimeSlotJournalRecord(bucket, record)
	})
}

func (j *runtimeSlotJournal) Get(slotID string) (runtimeSlotJournalRecord, error) {
	if j == nil || j.db == nil {
		return runtimeSlotJournalRecord{}, fmt.Errorf("runtime slot journal is unavailable: %w", errdefs.ErrUnavailable)
	}
	var record runtimeSlotJournalRecord
	err := j.db.View(func(tx *bolt.Tx) error {
		bucket, err := runtimeSlotJournalBucketFrom(tx)
		if err != nil {
			return err
		}
		record, err = decodeRuntimeSlotJournalRecord(bucket.Get([]byte(slotID)))
		return err
	})
	return record, err
}

func (j *runtimeSlotJournal) BeginCleanup(request protocol.NodeCleanupControlRequest) (runtimeSlotJournalRecord, error) {
	if j == nil || j.db == nil {
		return runtimeSlotJournalRecord{}, fmt.Errorf("runtime slot journal is unavailable: %w", errdefs.ErrUnavailable)
	}
	if err := request.Validate(); err != nil {
		return runtimeSlotJournalRecord{}, fmt.Errorf("validate runtime slot journal cleanup: %w: %w", err, errdefs.ErrInvalidArgument)
	}
	var result runtimeSlotJournalRecord
	err := j.db.Update(func(tx *bolt.Tx) error {
		bucket, err := runtimeSlotJournalBucketFrom(tx)
		if err != nil {
			return err
		}
		current, err := decodeRuntimeSlotJournalRecord(bucket.Get([]byte(request.SlotID)))
		if err != nil {
			return err
		}
		if err := current.matchesCleanup(request); err != nil {
			return err
		}
		if current.Cleanup != nil {
			if *current.Cleanup != request {
				return fmt.Errorf("runtime slot journal is bound to another cleanup: %w", errdefs.ErrAlreadyExists)
			}
			result = current
			return nil
		}
		clone := request
		current.Cleanup = &clone
		current.UpdatedAt = time.Now().UTC().Format(time.RFC3339Nano)
		if err := putRuntimeSlotJournalRecord(bucket, current); err != nil {
			return err
		}
		result = current
		return nil
	})
	return result, err
}

func (j *runtimeSlotJournal) CompleteCleanup(
	request protocol.NodeCleanupControlRequest,
	proof protocol.NodeCleanupControlProof,
) error {
	if j == nil || j.db == nil {
		return fmt.Errorf("runtime slot journal is unavailable: %w", errdefs.ErrUnavailable)
	}
	if err := proof.Validate(); err != nil {
		return fmt.Errorf("validate runtime slot journal proof: %w: %w", err, errdefs.ErrInvalidArgument)
	}
	if proof.Request() != request {
		return fmt.Errorf("runtime slot journal proof does not match its cleanup request: %w", errdefs.ErrInvalidArgument)
	}
	return j.db.Update(func(tx *bolt.Tx) error {
		bucket, err := runtimeSlotJournalBucketFrom(tx)
		if err != nil {
			return err
		}
		current, err := decodeRuntimeSlotJournalRecord(bucket.Get([]byte(request.SlotID)))
		if err != nil {
			return err
		}
		if current.Cleanup == nil || *current.Cleanup != request {
			return fmt.Errorf("runtime slot cleanup was not durably started: %w", errdefs.ErrFailedPrecondition)
		}
		if current.Proof != nil {
			if *current.Proof != proof {
				return fmt.Errorf("runtime slot cleanup proof changed: %w", errdefs.ErrAlreadyExists)
			}
			return nil
		}
		now := time.Now().UTC().Format(time.RFC3339Nano)
		clone := proof
		current.Proof = &clone
		current.CompletedAt = now
		current.UpdatedAt = now
		return putRuntimeSlotJournalRecord(bucket, current)
	})
}

func (j *runtimeSlotJournal) Prune(now time.Time) (int, error) {
	if j == nil || j.db == nil {
		return 0, fmt.Errorf("runtime slot journal is unavailable: %w", errdefs.ErrUnavailable)
	}
	cutoff := now.UTC().Add(-j.retention)
	deleted := 0
	err := j.db.Update(func(tx *bolt.Tx) error {
		bucket, err := runtimeSlotJournalBucketFrom(tx)
		if err != nil {
			return err
		}
		var keys [][]byte
		if err := bucket.ForEach(func(key, payload []byte) error {
			if payload == nil {
				return nil
			}
			record, err := decodeRuntimeSlotJournalRecord(payload)
			if err != nil {
				return fmt.Errorf("decode runtime slot journal %q: %w", key, err)
			}
			if record.Proof == nil || record.CompletedAt == "" {
				return nil
			}
			completedAt, err := time.Parse(time.RFC3339Nano, record.CompletedAt)
			if err != nil {
				return fmt.Errorf("parse runtime slot completion %q: %w", key, err)
			}
			if !completedAt.After(cutoff) {
				keys = append(keys, append([]byte(nil), key...))
			}
			return nil
		}); err != nil {
			return err
		}
		for _, key := range keys {
			if err := bucket.Delete(key); err != nil {
				return err
			}
			deleted++
		}
		return nil
	})
	return deleted, err
}

func (r runtimeSlotJournalRecord) matchesCleanup(request protocol.NodeCleanupControlRequest) error {
	registration := r.Registration
	if registration.SlotID != request.SlotID || registration.ClusterID != request.ClusterID ||
		registration.AllocationID != request.AllocationID || registration.NodeID != request.NodeID ||
		registration.NodeBootID != request.NodeBootID || registration.NetNSIdentity != request.NetNSIdentity ||
		registration.RunscContainerID != request.RunscContainerID {
		return fmt.Errorf("runtime slot journal does not match cleanup incarnation: %w", errdefs.ErrFailedPrecondition)
	}
	return nil
}

func decodeRuntimeSlotJournalRecord(payload []byte) (runtimeSlotJournalRecord, error) {
	if payload == nil {
		return runtimeSlotJournalRecord{}, fmt.Errorf("runtime slot journal record is absent: %w", errdefs.ErrNotFound)
	}
	var record runtimeSlotJournalRecord
	if err := json.Unmarshal(payload, &record); err != nil {
		return runtimeSlotJournalRecord{}, fmt.Errorf("decode runtime slot journal record: %w", err)
	}
	if record.Version != runtimeSlotJournalVersion || record.Registration.Validate() != nil || record.CreatedAt == "" || record.UpdatedAt == "" {
		return runtimeSlotJournalRecord{}, fmt.Errorf("runtime slot journal record is invalid: %w", errdefs.ErrFailedPrecondition)
	}
	if _, err := time.Parse(time.RFC3339Nano, record.CreatedAt); err != nil {
		return runtimeSlotJournalRecord{}, fmt.Errorf("runtime slot journal creation time is invalid: %w", errdefs.ErrFailedPrecondition)
	}
	if _, err := time.Parse(time.RFC3339Nano, record.UpdatedAt); err != nil {
		return runtimeSlotJournalRecord{}, fmt.Errorf("runtime slot journal update time is invalid: %w", errdefs.ErrFailedPrecondition)
	}
	if record.Cleanup != nil {
		if err := record.Cleanup.Validate(); err != nil {
			return runtimeSlotJournalRecord{}, fmt.Errorf("runtime slot journal cleanup is invalid: %w", errdefs.ErrFailedPrecondition)
		}
		if err := record.matchesCleanup(*record.Cleanup); err != nil {
			return runtimeSlotJournalRecord{}, err
		}
	}
	if record.Proof != nil {
		if record.Cleanup == nil || record.Proof.Validate() != nil || record.Proof.Request() != *record.Cleanup || record.CompletedAt == "" {
			return runtimeSlotJournalRecord{}, fmt.Errorf("runtime slot journal proof is invalid: %w", errdefs.ErrFailedPrecondition)
		}
	} else if record.CompletedAt != "" {
		return runtimeSlotJournalRecord{}, fmt.Errorf("runtime slot journal completion lacks its proof: %w", errdefs.ErrFailedPrecondition)
	}
	if record.CompletedAt != "" {
		if _, err := time.Parse(time.RFC3339Nano, record.CompletedAt); err != nil {
			return runtimeSlotJournalRecord{}, fmt.Errorf("runtime slot journal completion time is invalid: %w", errdefs.ErrFailedPrecondition)
		}
	}
	return record, nil
}

func putRuntimeSlotJournalRecord(bucket *bolt.Bucket, record runtimeSlotJournalRecord) error {
	if bucket == nil {
		return fmt.Errorf("runtime slot journal bucket is absent: %w", errdefs.ErrUnavailable)
	}
	payload, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("encode runtime slot journal record: %w", err)
	}
	if err := bucket.Put([]byte(record.Registration.SlotID), payload); err != nil {
		return fmt.Errorf("persist runtime slot journal record: %w", err)
	}
	return nil
}

func runtimeSlotJournalBucketFrom(tx *bolt.Tx) (*bolt.Bucket, error) {
	if tx == nil {
		return nil, fmt.Errorf("runtime slot journal transaction is unavailable: %w", errdefs.ErrUnavailable)
	}
	bucket := tx.Bucket(runtimeSlotJournalBucket)
	if bucket == nil {
		return nil, fmt.Errorf("runtime slot journal bucket is absent: %w", errdefs.ErrUnavailable)
	}
	return bucket, nil
}
