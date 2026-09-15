package runtimeslot

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
)

const RegistrationAbortOperationPrefix = "registration-abort-"

// RegistrationAbortRequest asks the region to fence an unregistered physical
// incarnation. It conveys no authority to release a writer or resource lease.
// Only PostgreSQL exclusion of registration can authorize grantless cleanup.
type RegistrationAbortRequest struct {
	SlotID        string                   `json:"slot_id"`
	ClusterID     string                   `json:"cluster_id"`
	AllocationID  string                   `json:"allocation_id"`
	NodeID        string                   `json:"node_id"`
	NodeUID       string                   `json:"node_uid"`
	NodeBootID    string                   `json:"node_boot_id"`
	NetNSIdentity string                   `json:"netns_identity"`
	Proof         *NodeCleanupControlProof `json:"proof,omitempty"`
}

func (r RegistrationAbortRequest) Validate() error {
	cleanup, err := r.CleanupRequest()
	if err != nil {
		return err
	}
	if r.Proof != nil {
		if err := r.Proof.Validate(); err != nil {
			return err
		}
		if r.Proof.Request() != cleanup {
			return fmt.Errorf("registration abort proof belongs to another cleanup")
		}
	}
	return nil
}

// CleanupRequest derives one immutable operation; callers must obtain the
// matching regional fence before sending it to physical cleanup.
func (r RegistrationAbortRequest) CleanupRequest() (NodeCleanupControlRequest, error) {
	for name, value := range map[string]string{
		"slot_id": r.SlotID, "cluster_id": r.ClusterID, "allocation_id": r.AllocationID,
		"node_id": r.NodeID, "node_uid": r.NodeUID, "node_boot_id": r.NodeBootID,
		"netns_identity": r.NetNSIdentity,
	} {
		if err := validateRequiredID(name, value); err != nil {
			return NodeCleanupControlRequest{}, err
		}
	}
	r.Proof = nil
	payload, err := json.Marshal(r)
	if err != nil {
		return NodeCleanupControlRequest{}, err
	}
	digest := sha256.Sum256(payload)
	request := NodeCleanupControlRequest{
		OperationID: RegistrationAbortOperationPrefix + hex.EncodeToString(digest[:]),
		SlotID:      r.SlotID, ClusterID: r.ClusterID, AllocationID: r.AllocationID,
		NodeID: r.NodeID, NodeUID: r.NodeUID, NodeBootID: r.NodeBootID,
		NetNSIdentity: r.NetNSIdentity, RunscContainerID: NomadRunscContainerID(r.SlotID),
	}
	return request, request.Validate()
}

// RegistrationAbortResponse distinguishes an existing regional slot from a
// durable registration fence. Neither result is itself physical absence proof.
type RegistrationAbortResponse struct {
	Registered bool                       `json:"registered"`
	Cleanup    *NodeCleanupControlRequest `json:"cleanup,omitempty"`
	Completed  bool                       `json:"completed"`
}

func (r RegistrationAbortResponse) ValidateFor(request RegistrationAbortRequest) error {
	if err := request.Validate(); err != nil {
		return err
	}
	if r.Registered {
		if r.Cleanup != nil || r.Completed || request.Proof != nil {
			return fmt.Errorf("registered slot cannot acknowledge a registration abort")
		}
		return nil
	}
	expected, err := request.CleanupRequest()
	if err != nil {
		return err
	}
	if r.Cleanup == nil || *r.Cleanup != expected || (request.Proof != nil && !r.Completed) {
		return fmt.Errorf("registration abort acknowledgement does not match the exact request")
	}
	return nil
}

func RegistrationAbortPath(slotID string) string { return SlotPath(slotID) + "/registration-abort" }
