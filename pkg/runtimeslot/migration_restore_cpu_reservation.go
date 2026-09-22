package runtimeslot

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
)

const MigrationRestoreCPUReservationVersion = 1

// MigrationRestoreCPUReservation describes additional CPU admitted separately
// from the immutable sandbox lease. Constructing or validating this descriptor
// does not admit capacity or authorize a cgroup write. The regional transaction
// must own it, and both node and driver must bind it to the exact restore.
//
// There is deliberately no local expiry: time passing cannot prove that a
// crashed driver stopped using the additional CPU. Release requires a verified
// quota reset or the existing physical cleanup proof for the base lease.
// This candidate contract is not connected to the runtime execution path.
type MigrationRestoreCPUReservation struct {
	Version            int                  `json:"version"`
	ReservationID      string               `json:"reservation_id"`
	Target             NodeChannelTarget    `json:"target"`
	Resources          RuntimeResourceLease `json:"resources"`
	TotalCPUMillicores int64                `json:"total_cpu_millicores"`
}

// NewMigrationRestoreCPUReservation owns a value snapshot of the original
// resources. Memory, PIDs, cpusets, period and CPU weight are never changed by
// this CPU-only supplement.
func NewMigrationRestoreCPUReservation(target NodeChannelTarget, resources RuntimeResourceLease, totalCPU int64) (MigrationRestoreCPUReservation, error) {
	r := MigrationRestoreCPUReservation{Version: MigrationRestoreCPUReservationVersion,
		Target: target, Resources: resources, TotalCPUMillicores: totalCPU}
	r.ReservationID = r.reservationID()
	return r, r.Validate()
}

func (r MigrationRestoreCPUReservation) reservationID() string {
	// The digest includes the base lease and destination identity, so neither
	// another boot nor a changed CPU amount can reuse an admitted reservation.
	r.ReservationID = ""
	payload, _ := json.Marshal(r)
	hash := sha256.Sum256(append([]byte("sandbox0-migration-restore-cpu-v1\x00"), payload...))
	return "restore-cpu-" + hex.EncodeToString(hash[:])
}

func (r MigrationRestoreCPUReservation) Validate() error {
	if r.Version != MigrationRestoreCPUReservationVersion {
		return fmt.Errorf("invalid migration restore CPU reservation version")
	}
	if err := r.Target.validate(true); err != nil {
		return err
	}
	if err := r.Resources.Validate(); err != nil {
		return err
	}
	t, l := r.Target, r.Resources
	if t.SlotID != l.SlotID || t.ClusterID != l.ClusterID || t.NodeID != l.NodeID ||
		t.NodeUID != l.NodeUID || t.NodeBootID != l.NodeBootID {
		return fmt.Errorf("restore CPU reservation changed its resource lease target")
	}
	if r.TotalCPUMillicores > MaxRuntimeCPUMillicores ||
		r.TotalCPUMillicores <= l.CPUMillicores ||
		r.TotalCPUMillicores-l.CPUMillicores < MinRuntimeCPUMillicores {
		return fmt.Errorf("restore CPU supplement must add bounded enforceable capacity")
	}
	if r.ReservationID != r.reservationID() {
		return fmt.Errorf("restore CPU reservation identity changed")
	}
	return nil
}

// MigrationRestoreCPUGrant binds previously reserved capacity to an exact
// restore. Keeping this separate allows admission before source capture, when
// the final restore digest does not exist yet. A region must persist the grant
// once and reject rebinding; these value checks alone do not authorize use.
type MigrationRestoreCPUGrant struct {
	Reservation          MigrationRestoreCPUReservation `json:"reservation"`
	RestoreRequestDigest string                         `json:"restore_request_digest"`
}

func (g MigrationRestoreCPUGrant) Validate() error {
	if err := g.Reservation.Validate(); err != nil {
		return err
	}
	_, err := DecodeProof("restore_request_digest", g.RestoreRequestDigest)
	return err
}

// ValidateRestore cannot substitute for the restore command's writer or
// execution fencing, or for regional admission of the additional capacity.
func (g MigrationRestoreCPUGrant) ValidateRestore(request MigrationRestoreRequest) error {
	if err := g.Validate(); err != nil {
		return err
	}
	digest, err := request.Digest()
	if err != nil {
		return err
	}
	r := g.Reservation
	if digest != g.RestoreRequestDigest || request.Image.Target != r.Target || request.Image.Resources != r.Resources {
		return fmt.Errorf("restore CPU reservation belongs to another restore")
	}
	return nil
}

func (g MigrationRestoreCPUGrant) Digest() (string, error) {
	if err := g.Validate(); err != nil {
		return "", err
	}
	payload, err := json.Marshal(g)
	if err != nil {
		return "", err
	}
	hash := sha256.Sum256(payload)
	return hex.EncodeToString(hash[:]), nil
}

// MigrationRestoreCPUResetProof is physical node evidence, never a timer or an
// acknowledgement that reset was requested. The expected cgroup identity must
// come from durable node custody of the applied reservation, not this receipt.
type MigrationRestoreCPUResetProof struct {
	GrantDigest     string `json:"grant_digest"`
	CgroupID        uint64 `json:"cgroup_id"`
	CPUPeriodMicros uint64 `json:"cpu_period_micros"`
	CPUQuotaMicros  int64  `json:"cpu_quota_micros"`
}

func (p MigrationRestoreCPUResetProof) ValidateFor(g MigrationRestoreCPUGrant, expectedCgroupID uint64) error {
	want, err := g.Digest()
	if err != nil {
		return err
	}
	r := g.Reservation
	if p.GrantDigest != want || expectedCgroupID == 0 || p.CgroupID != expectedCgroupID ||
		p.CPUPeriodMicros != r.Resources.CPUPeriodMicros || p.CPUQuotaMicros != r.Resources.CPUQuotaMicros {
		return fmt.Errorf("restore CPU reset lacks the exact original quota and cgroup proof")
	}
	return nil
}
