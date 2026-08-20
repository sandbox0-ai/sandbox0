package runtimeslotnomad

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/containerd/errdefs"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/runtimeslotreconciler"
)

type fakeAPI struct {
	allocation  *Allocation
	serverErr   error
	client      bool
	clientErr   error
	stopErr     error
	gcErr       error
	stopCalls   []string
	gcCalls     int
	clientCalls int
	serverCalls int
}

func (a *fakeAPI) ServerAllocation(
	_ context.Context,
	_ runtimeslotreconciler.AllocationTarget,
) (*Allocation, error) {
	a.serverCalls++
	if a.allocation == nil {
		return nil, a.serverErr
	}
	clone := *a.allocation
	return &clone, a.serverErr
}

func (a *fakeAPI) ClientAllocationPresent(
	_ context.Context,
	_ runtimeslotreconciler.AllocationTarget,
) (bool, error) {
	a.clientCalls++
	return a.client, a.clientErr
}

func (a *fakeAPI) StopAllocation(
	_ context.Context,
	_ runtimeslotreconciler.AllocationTarget,
	operationID string,
) error {
	a.stopCalls = append(a.stopCalls, operationID)
	return a.stopErr
}

func (a *fakeAPI) GarbageCollectAllocation(
	_ context.Context,
	_ runtimeslotreconciler.AllocationTarget,
) error {
	a.gcCalls++
	return a.gcErr
}

func TestControllerObservesServerAndDirectClientOwnership(t *testing.T) {
	target := testTarget()
	api := &fakeAPI{allocation: testAllocation(), client: false}
	controller, err := New(api)
	if err != nil {
		t.Fatal(err)
	}

	observation, err := controller.Observe(t.Context(), target)
	if err != nil {
		t.Fatal(err)
	}
	if !observation.PhysicalPresent || observation.Target != target || len(observation.ProofDigest) != 32 {
		t.Fatalf("observation = %+v", observation)
	}

	api.allocation.DesiredStatus = "stop"
	stopped, err := controller.Observe(t.Context(), target)
	if err != nil {
		t.Fatal(err)
	}
	if stopped.PhysicalPresent {
		t.Fatalf("stopped observation = %+v", stopped)
	}
	api.allocation = nil
	absent, err := controller.Observe(t.Context(), target)
	if err != nil {
		t.Fatal(err)
	}
	if absent.PhysicalPresent || !reflect.DeepEqual(stopped.ProofDigest, absent.ProofDigest) {
		t.Fatalf("absence proof changed after server GC: %x != %x", stopped.ProofDigest, absent.ProofDigest)
	}

	api.client = true
	clientOwned, err := controller.Observe(t.Context(), target)
	if err != nil {
		t.Fatal(err)
	}
	if !clientOwned.PhysicalPresent {
		t.Fatal("direct Nomad client ownership was ignored")
	}
}

func TestControllerPurgesServerThenExactClient(t *testing.T) {
	target := testTarget()
	api := &fakeAPI{allocation: testAllocation(), client: true}
	controller, err := New(api)
	if err != nil {
		t.Fatal(err)
	}
	request := runtimeslotreconciler.AllocationPurgeRequest{OperationID: "purge-operation", Target: target}
	if err := controller.Purge(t.Context(), request); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(api.stopCalls, []string{request.OperationID}) || api.gcCalls != 1 {
		t.Fatalf("stop = %v, gc = %d", api.stopCalls, api.gcCalls)
	}

	api.allocation.DesiredStatus = "stop"
	if err := controller.Purge(t.Context(), request); err != nil {
		t.Fatal(err)
	}
	if len(api.stopCalls) != 1 || api.gcCalls != 2 {
		t.Fatalf("retry stop = %v, gc = %d", api.stopCalls, api.gcCalls)
	}

	api.allocation = nil
	if err := controller.Purge(t.Context(), request); err != nil {
		t.Fatal(err)
	}
	if len(api.stopCalls) != 1 || api.gcCalls != 3 {
		t.Fatalf("server-GC retry stop = %v, gc = %d", api.stopCalls, api.gcCalls)
	}
}

func TestControllerRejectsMismatchedServerIdentityBeforeNodeAccess(t *testing.T) {
	api := &fakeAPI{allocation: testAllocation()}
	api.allocation.NodeID = "another-node"
	controller, err := New(api)
	if err != nil {
		t.Fatal(err)
	}
	_, err = controller.Observe(t.Context(), testTarget())
	if !errors.Is(err, errdefs.ErrFailedPrecondition) {
		t.Fatalf("Observe() error = %v", err)
	}
	if api.clientCalls != 0 {
		t.Fatalf("direct client calls = %d", api.clientCalls)
	}

	err = controller.Purge(t.Context(), runtimeslotreconciler.AllocationPurgeRequest{
		OperationID: "purge-operation", Target: testTarget(),
	})
	if !errors.Is(err, errdefs.ErrFailedPrecondition) || api.gcCalls != 0 || len(api.stopCalls) != 0 {
		t.Fatalf("Purge() error = %v, stop = %v, gc = %d", err, api.stopCalls, api.gcCalls)
	}
}

func TestControllerPropagatesClientGCNotReady(t *testing.T) {
	api := &fakeAPI{allocation: testAllocation(), gcErr: runtimeslotreconciler.ErrAllocationStillPresent}
	controller, err := New(api)
	if err != nil {
		t.Fatal(err)
	}
	err = controller.Purge(t.Context(), runtimeslotreconciler.AllocationPurgeRequest{
		OperationID: "purge-operation", Target: testTarget(),
	})
	if !errors.Is(err, runtimeslotreconciler.ErrAllocationStillPresent) {
		t.Fatalf("Purge() error = %v", err)
	}
}

func TestControllerRejectsNonCanonicalTargetBeforeNomadAccess(t *testing.T) {
	api := &fakeAPI{}
	controller, err := New(api)
	if err != nil {
		t.Fatal(err)
	}
	target := testTarget()
	target.AllocationID = "nested/allocation"
	_, err = controller.Observe(t.Context(), target)
	if !errors.Is(err, errdefs.ErrInvalidArgument) {
		t.Fatalf("Observe() error = %v", err)
	}
	if api.serverCalls != 0 || api.clientCalls != 0 {
		t.Fatalf("server calls = %d, client calls = %d", api.serverCalls, api.clientCalls)
	}
}

func testTarget() runtimeslotreconciler.AllocationTarget {
	return runtimeslotreconciler.AllocationTarget{
		ClusterID: "cluster-1", AllocationID: "allocation-1",
		AllocationNamespace: "default", NodeID: "node-1",
	}
}

func testAllocation() *Allocation {
	return &Allocation{
		ID: "allocation-1", Namespace: "default", NodeID: "node-1", DesiredStatus: "run",
	}
}
