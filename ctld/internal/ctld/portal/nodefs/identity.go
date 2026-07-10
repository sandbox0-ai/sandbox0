package nodefs

import (
	"errors"
	"fmt"
)

const (
	// SlotBits leaves enough room for 1,048,575 portal slots in each FUSE
	// connection generation. Slot zero is reserved for the shard namespace.
	SlotBits = 20
	// LocalIDBits leaves enough room for 17,592,186,044,415 backend inode or
	// handle IDs in each portal.
	LocalIDBits = 64 - SlotBits
	// BindingGenerationBits is reserved inside each portal-local ID so inode
	// and handle identities from an old backend binding cannot reach a new
	// session after ctld recovers or switches the binding.
	BindingGenerationBits = 8
	// BackendLocalIDBits leaves 36 bits for a backend inode or handle.
	BackendLocalIDBits = LocalIDBits - BindingGenerationBits

	MaxSlot              = uint32(1<<SlotBits - 1)
	MaxLocalID           = uint64(1<<LocalIDBits - 1)
	MaxBackendLocalID    = uint64(1<<BackendLocalIDBits - 1)
	MaxBindingGeneration = uint64(1<<BindingGenerationBits - 1)

	// ShardRootNodeID is the FUSE root of a nodefs shard. It belongs to the
	// synthetic shard namespace rather than to a portal slot.
	ShardRootNodeID = uint64(1)
)

var (
	ErrInvalidSlot              = errors.New("invalid nodefs portal slot")
	ErrInvalidLocalID           = errors.New("invalid nodefs local id")
	ErrInvalidBackendLocalID    = errors.New("invalid nodefs backend local id")
	ErrInvalidBindingGeneration = errors.New("invalid nodefs binding generation")
	ErrSyntheticNode            = errors.New("nodefs synthetic node")
	ErrCrossPortal              = errors.New("nodefs request crosses portal slots")
	ErrNoRouteID                = errors.New("nodefs request has no routable id")
	ErrSlotNotFound             = errors.New("nodefs portal slot not found")
	ErrSlotRegistered           = errors.New("nodefs portal slot already registered")
	ErrSlotRetired              = errors.New("nodefs portal slot is retired")
	ErrSlotDraining             = errors.New("nodefs portal slot is draining")
)

// Slot identifies a portal within one FUSE connection generation. Slots must
// not be reused until the entire connection generation has been replaced,
// because the kernel may retain old NodeIDs after a portal is unpublished.
type Slot uint32

// NewSlot validates a journal or allocator value before it enters the router.
func NewSlot(value uint64) (Slot, error) {
	if value == 0 || value > uint64(MaxSlot) {
		return 0, fmt.Errorf("%w: %d is outside [1,%d]", ErrInvalidSlot, value, MaxSlot)
	}
	return Slot(value), nil
}

// EncodeNodeID combines a portal slot and backend inode into a FUSE NodeID.
func EncodeNodeID(slot Slot, localID uint64) (uint64, error) {
	return encodeID(slot, localID)
}

// DecodeNodeID splits a portal FUSE NodeID into its slot and backend inode.
// The shard root is reported as ErrSyntheticNode so callers can dispatch it to
// the shard namespace instead of a portal backend.
func DecodeNodeID(nodeID uint64) (Slot, uint64, error) {
	if nodeID == ShardRootNodeID {
		return 0, 0, ErrSyntheticNode
	}
	return decodeID(nodeID)
}

// EncodeHandleID combines a portal slot and backend file or directory handle.
// Handle zero represents no handle in FUSE and therefore cannot be encoded.
func EncodeHandleID(slot Slot, localID uint64) (uint64, error) {
	return encodeID(slot, localID)
}

// DecodeHandleID splits a non-zero FUSE handle into its portal slot and
// backend handle.
func DecodeHandleID(handleID uint64) (Slot, uint64, error) {
	return decodeID(handleID)
}

// EncodeBindingNodeID combines a slot, durable backend generation, and
// backend inode. Portal roots intentionally do not use this representation;
// SessionMux keeps their outer NodeID stable across binding switches.
func EncodeBindingNodeID(slot Slot, generation, backendLocalID uint64) (uint64, error) {
	localID, err := encodeBindingLocalID(generation, backendLocalID)
	if err != nil {
		return 0, err
	}
	return EncodeNodeID(slot, localID)
}

// DecodeBindingNodeID splits a non-root portal NodeID into its durable
// binding identity and backend inode.
func DecodeBindingNodeID(nodeID uint64) (Slot, uint64, uint64, error) {
	slot, localID, err := DecodeNodeID(nodeID)
	if err != nil {
		return 0, 0, 0, err
	}
	generation, backendLocalID, err := decodeBindingLocalID(localID)
	return slot, generation, backendLocalID, err
}

// EncodeBindingHandleID combines a slot, durable backend generation, and
// backend file or directory handle.
func EncodeBindingHandleID(slot Slot, generation, backendLocalID uint64) (uint64, error) {
	localID, err := encodeBindingLocalID(generation, backendLocalID)
	if err != nil {
		return 0, err
	}
	return EncodeHandleID(slot, localID)
}

// DecodeBindingHandleID splits a portal handle into its durable binding
// identity and backend handle.
func DecodeBindingHandleID(handleID uint64) (Slot, uint64, uint64, error) {
	slot, localID, err := DecodeHandleID(handleID)
	if err != nil {
		return 0, 0, 0, err
	}
	generation, backendLocalID, err := decodeBindingLocalID(localID)
	return slot, generation, backendLocalID, err
}

func encodeID(slot Slot, localID uint64) (uint64, error) {
	if slot == 0 || uint64(slot) > uint64(MaxSlot) {
		return 0, fmt.Errorf("%w: %d is outside [1,%d]", ErrInvalidSlot, slot, MaxSlot)
	}
	if localID == 0 || localID > MaxLocalID {
		return 0, fmt.Errorf("%w: %d is outside [1,%d]", ErrInvalidLocalID, localID, MaxLocalID)
	}
	return uint64(slot)<<LocalIDBits | localID, nil
}

func decodeID(id uint64) (Slot, uint64, error) {
	slotValue := id >> LocalIDBits
	if slotValue == 0 || slotValue > uint64(MaxSlot) {
		return 0, 0, fmt.Errorf("%w: encoded id %d has slot %d", ErrInvalidSlot, id, slotValue)
	}
	localID := id & MaxLocalID
	if localID == 0 {
		return 0, 0, fmt.Errorf("%w: encoded id %d has local id zero", ErrInvalidLocalID, id)
	}
	return Slot(slotValue), localID, nil
}

func encodeBindingLocalID(generation, backendLocalID uint64) (uint64, error) {
	if generation == 0 || generation > MaxBindingGeneration {
		return 0, fmt.Errorf("%w: %d is outside [1,%d]", ErrInvalidBindingGeneration, generation, MaxBindingGeneration)
	}
	if backendLocalID == 0 || backendLocalID > MaxBackendLocalID {
		return 0, fmt.Errorf("%w: %d is outside [1,%d]", ErrInvalidBackendLocalID, backendLocalID, MaxBackendLocalID)
	}
	return generation<<BackendLocalIDBits | backendLocalID, nil
}

func decodeBindingLocalID(localID uint64) (uint64, uint64, error) {
	generation := localID >> BackendLocalIDBits
	if generation == 0 || generation > MaxBindingGeneration {
		return 0, 0, fmt.Errorf("%w: encoded local id %d has generation %d", ErrInvalidBindingGeneration, localID, generation)
	}
	backendLocalID := localID & MaxBackendLocalID
	if backendLocalID == 0 {
		return 0, 0, fmt.Errorf("%w: encoded local id %d has backend id zero", ErrInvalidBackendLocalID, localID)
	}
	return generation, backendLocalID, nil
}
