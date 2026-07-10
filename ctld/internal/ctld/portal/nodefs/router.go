package nodefs

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
)

type routeEntry[T any] struct {
	target T

	inFlight atomic.Uint64
	draining atomic.Bool
	drained  chan struct{}
	drainEnd sync.Once
}

// routeLeaseHooks lets a route target extend the router lease over target-local
// state. SessionMux uses it to keep a backend binding quiescent while a file
// operation is in flight.
type routeLeaseHooks interface {
	acquireRouteLease() bool
	releaseRouteLease()
}

// Router maps encoded FUSE IDs to portal-local IDs and an opaque backend
// target. The success paths use only a shared map lock and atomic refcount; they
// do not allocate. Portal removal waits for all acquired requests to finish.
//
// Router does not allocate slots. Once drained, a slot is retired for the
// lifetime of this Router, matching the rule that slots cannot be reused within
// a FUSE connection generation.
type Router[T any] struct {
	mu      sync.RWMutex
	entries map[Slot]*routeEntry[T]
	retired []uint64
}

// NewRouter constructs an empty portal router.
func NewRouter[T any]() *Router[T] {
	return &Router[T]{
		entries: make(map[Slot]*routeEntry[T]),
	}
}

// Register makes target available for new requests on slot.
func (r *Router[T]) Register(slot Slot, target T) error {
	if _, err := NewSlot(uint64(slot)); err != nil {
		return err
	}
	if r == nil {
		return fmt.Errorf("register slot %d: router is nil", slot)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.entries == nil {
		r.entries = make(map[Slot]*routeEntry[T])
	}
	if r.isRetiredLocked(slot) {
		return fmt.Errorf("%w: %d", ErrSlotRetired, slot)
	}
	if _, exists := r.entries[slot]; exists {
		return fmt.Errorf("%w: %d", ErrSlotRegistered, slot)
	}
	r.entries[slot] = &routeEntry[T]{target: target, drained: make(chan struct{})}
	return nil
}

// Retire imports a durable tombstone before the router starts serving. It is
// rejected while the slot is registered because active targets must be drained
// instead.
func (r *Router[T]) Retire(slot Slot) error {
	if _, err := NewSlot(uint64(slot)); err != nil {
		return err
	}
	if r == nil {
		return fmt.Errorf("retire slot %d: router is nil", slot)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.entries[slot]; exists {
		return fmt.Errorf("%w: %d", ErrSlotRegistered, slot)
	}
	r.setRetiredLocked(slot)
	return nil
}

// Lease pins one portal target while a routed request is in flight. A
// successful acquisition must be released exactly once. Keeping the token a
// small value lets request handlers defer Release without a heap allocation.
type Lease[T any] struct {
	Slot   Slot
	Target T

	entry *routeEntry[T]
}

// Release allows a draining portal to finish once the request is complete.
func (l Lease[T]) Release() {
	if l.entry == nil {
		return
	}
	if hooks, ok := any(l.entry.target).(routeLeaseHooks); ok {
		hooks.releaseRouteLease()
	}
	remaining := l.entry.inFlight.Add(^uint64(0))
	if remaining == 0 && l.entry.draining.Load() {
		l.entry.drainEnd.Do(func() { close(l.entry.drained) })
	}
}

// AcquireNode routes one FUSE NodeID and returns its portal-local inode.
func (r *Router[T]) AcquireNode(nodeID uint64) (Lease[T], uint64, error) {
	var zero Lease[T]
	slot, localID, err := DecodeNodeID(nodeID)
	if err != nil {
		return zero, 0, err
	}
	lease, err := r.acquireSlot(slot)
	if err != nil {
		return zero, 0, err
	}
	return lease, localID, err
}

// AcquireHandle routes one non-zero FUSE file or directory handle.
func (r *Router[T]) AcquireHandle(handleID uint64) (Lease[T], uint64, error) {
	var zero Lease[T]
	slot, localID, err := DecodeHandleID(handleID)
	if err != nil {
		return zero, 0, err
	}
	lease, err := r.acquireSlot(slot)
	if err != nil {
		return zero, 0, err
	}
	return lease, localID, err
}

// AcquireNodeHandle routes an inode and an optional handle. A zero handle is
// preserved as zero. A non-zero handle must belong to the inode's portal.
func (r *Router[T]) AcquireNodeHandle(nodeID, handleID uint64) (Lease[T], uint64, uint64, error) {
	var zero Lease[T]
	slot, localNode, err := DecodeNodeID(nodeID)
	if err != nil {
		return zero, 0, 0, err
	}
	localHandle := uint64(0)
	if handleID != 0 {
		handleSlot, decoded, err := DecodeHandleID(handleID)
		if err != nil {
			return zero, 0, 0, err
		}
		if handleSlot != slot {
			return zero, 0, 0, crossPortalError(slot, handleSlot)
		}
		localHandle = decoded
	}
	lease, err := r.acquireSlot(slot)
	if err != nil {
		return zero, 0, 0, err
	}
	return lease, localNode, localHandle, err
}

// AcquireNodes routes the two inode operands used by rename and link. Both
// nodes must belong to the same portal.
func (r *Router[T]) AcquireNodes(firstNodeID, secondNodeID uint64) (Lease[T], uint64, uint64, error) {
	var zero Lease[T]
	firstSlot, firstLocal, err := DecodeNodeID(firstNodeID)
	if err != nil {
		return zero, 0, 0, err
	}
	secondSlot, secondLocal, err := DecodeNodeID(secondNodeID)
	if err != nil {
		return zero, 0, 0, err
	}
	if secondSlot != firstSlot {
		return zero, 0, 0, crossPortalError(firstSlot, secondSlot)
	}
	lease, err := r.acquireSlot(firstSlot)
	if err != nil {
		return zero, 0, 0, err
	}
	return lease, firstLocal, secondLocal, err
}

// AcquireCopy routes both inode and handle pairs used by copy_file_range. Zero
// optional handles are allowed; every non-zero ID must belong to one portal.
func (r *Router[T]) AcquireCopy(
	inputNodeID, inputHandleID, outputNodeID, outputHandleID uint64,
) (Lease[T], uint64, uint64, uint64, uint64, error) {
	var zero Lease[T]
	inputSlot, inputNode, err := DecodeNodeID(inputNodeID)
	if err != nil {
		return zero, 0, 0, 0, 0, err
	}
	outputSlot, outputNode, err := DecodeNodeID(outputNodeID)
	if err != nil {
		return zero, 0, 0, 0, 0, err
	}
	if outputSlot != inputSlot {
		return zero, 0, 0, 0, 0, crossPortalError(inputSlot, outputSlot)
	}
	inputHandle, err := decodeOptionalHandleForSlot(inputSlot, inputHandleID)
	if err != nil {
		return zero, 0, 0, 0, 0, err
	}
	outputHandle, err := decodeOptionalHandleForSlot(inputSlot, outputHandleID)
	if err != nil {
		return zero, 0, 0, 0, 0, err
	}
	lease, err := r.acquireSlot(inputSlot)
	if err != nil {
		return zero, 0, 0, 0, 0, err
	}
	return lease, inputNode, inputHandle, outputNode, outputHandle, err
}

// Drain stops admitting new requests, waits for existing leases, removes the
// slot, and returns its target. If ctx is canceled, the entry remains in the
// draining state so a later Drain call can safely continue the removal.
func (r *Router[T]) Drain(ctx context.Context, slot Slot) (T, error) {
	var zero T
	if r == nil {
		return zero, fmt.Errorf("drain slot %d: router is nil", slot)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if _, err := NewSlot(uint64(slot)); err != nil {
		return zero, err
	}

	r.mu.Lock()
	entry := r.entries[slot]
	if entry == nil {
		r.mu.Unlock()
		return zero, fmt.Errorf("%w: %d", ErrSlotNotFound, slot)
	}
	entry.draining.Store(true)
	if entry.inFlight.Load() == 0 {
		r.retireLocked(slot, entry)
		target := entry.target
		r.mu.Unlock()
		return target, nil
	}
	drained := entry.drained
	r.mu.Unlock()

	select {
	case <-drained:
	case <-ctx.Done():
		return zero, ctx.Err()
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if current := r.entries[slot]; current != nil && current != entry {
		return zero, fmt.Errorf("drain slot %d: route changed while draining", slot)
	}
	if r.entries[slot] == entry {
		r.retireLocked(slot, entry)
	}
	return entry.target, nil
}

func (r *Router[T]) acquireSlot(slot Slot) (Lease[T], error) {
	var zero Lease[T]
	if r == nil {
		return zero, fmt.Errorf("acquire slot %d: router is nil", slot)
	}
	r.mu.RLock()
	entry := r.entries[slot]
	if entry == nil {
		r.mu.RUnlock()
		return zero, fmt.Errorf("%w: %d", ErrSlotNotFound, slot)
	}
	if entry.draining.Load() {
		r.mu.RUnlock()
		return zero, fmt.Errorf("%w: %d", ErrSlotDraining, slot)
	}
	entry.inFlight.Add(1)
	target := entry.target
	if hooks, ok := any(target).(routeLeaseHooks); ok {
		if !hooks.acquireRouteLease() {
			entry.inFlight.Add(^uint64(0))
			r.mu.RUnlock()
			return zero, fmt.Errorf("%w: %d", ErrSlotSwitching, slot)
		}
	}
	r.mu.RUnlock()
	return Lease[T]{Slot: slot, Target: target, entry: entry}, nil
}

func (r *Router[T]) retireLocked(slot Slot, expected *routeEntry[T]) {
	if r.entries[slot] == expected {
		delete(r.entries, slot)
	}
	r.setRetiredLocked(slot)
}

func (r *Router[T]) isRetiredLocked(slot Slot) bool {
	word, bit := retirementBit(slot)
	return word < len(r.retired) && r.retired[word]&(uint64(1)<<bit) != 0
}

func (r *Router[T]) setRetiredLocked(slot Slot) {
	word, bit := retirementBit(slot)
	if len(r.retired) == 0 {
		r.retired = make([]uint64, (uint64(MaxSlot)+64)/64)
	}
	r.retired[word] |= uint64(1) << bit
}

func retirementBit(slot Slot) (int, uint) {
	return int(uint32(slot) / 64), uint(uint32(slot) % 64)
}

func decodeOptionalHandleForSlot(slot Slot, handleID uint64) (uint64, error) {
	if handleID == 0 {
		return 0, nil
	}
	handleSlot, localID, err := DecodeHandleID(handleID)
	if err != nil {
		return 0, err
	}
	if handleSlot != slot {
		return 0, crossPortalError(slot, handleSlot)
	}
	return localID, nil
}

func crossPortalError(first, second Slot) error {
	return fmt.Errorf("%w: slots %d and %d", ErrCrossPortal, first, second)
}
