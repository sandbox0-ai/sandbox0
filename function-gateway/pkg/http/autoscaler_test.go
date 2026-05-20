package http

import (
	"testing"

	"github.com/sandbox0-ai/sandbox0/pkg/gateway/functions"
)

func TestFunctionAutoscalerReserveReadyHonorsTargetConcurrency(t *testing.T) {
	autoscaler := newFunctionAutoscaler(&Server{})
	instances := []*functions.RuntimeInstance{
		{
			ID:        "inst-a",
			SandboxID: "sandbox-a",
			State:     functions.RuntimeInstanceStateReady,
		},
		{
			ID:        "inst-b",
			SandboxID: "sandbox-b",
			State:     functions.RuntimeInstanceStateReady,
		},
	}
	cfg := functions.Autoscaling{MaxActive: 2, TargetConcurrency: 1, ScaleDownAfterSeconds: 300}

	first, firstRelease := autoscaler.reserveReady(instances, cfg, false)
	if first == nil {
		t.Fatal("first reservation is nil")
	}
	defer firstRelease()
	second, secondRelease := autoscaler.reserveReady(instances, cfg, false)
	if second == nil {
		t.Fatal("second reservation is nil")
	}
	defer secondRelease()
	if first.SandboxID == second.SandboxID {
		t.Fatalf("second reservation used %q, want the other ready runtime", second.SandboxID)
	}
	third, thirdRelease := autoscaler.reserveReady(instances, cfg, false)
	if third != nil {
		thirdRelease()
		t.Fatalf("third reservation = %+v, want nil when all local runtimes hit target", third)
	}
}

func TestFunctionAutoscalerReserveReadyCanOverflowSoftTarget(t *testing.T) {
	autoscaler := newFunctionAutoscaler(&Server{})
	instances := []*functions.RuntimeInstance{{
		ID:        "inst-a",
		SandboxID: "sandbox-a",
		State:     functions.RuntimeInstanceStateReady,
	}}
	cfg := functions.Autoscaling{MaxActive: 1, TargetConcurrency: 1, ScaleDownAfterSeconds: 300}

	first, firstRelease := autoscaler.reserveReady(instances, cfg, false)
	if first == nil {
		t.Fatal("first reservation is nil")
	}
	defer firstRelease()
	second, secondRelease := autoscaler.reserveReady(instances, cfg, true)
	if second == nil {
		t.Fatal("second reservation is nil with allowOverTarget")
	}
	defer secondRelease()
	if second.SandboxID != "sandbox-a" {
		t.Fatalf("second sandbox = %q, want sandbox-a", second.SandboxID)
	}
}

func TestFunctionAutoscalerTracksLocalInflight(t *testing.T) {
	autoscaler := newFunctionAutoscaler(&Server{})
	release := autoscaler.reserveSandbox("sandbox-a")
	if !autoscaler.hasLocalInflight("sandbox-a") {
		t.Fatal("hasLocalInflight() = false, want true")
	}
	release()
	if autoscaler.hasLocalInflight("sandbox-a") {
		t.Fatal("hasLocalInflight() = true after release, want false")
	}
}

func TestActiveRuntimeInstanceCountIncludesAllocatedCapacity(t *testing.T) {
	instances := []*functions.RuntimeInstance{
		{State: functions.RuntimeInstanceStateStarting},
		{State: functions.RuntimeInstanceStateReady},
		{State: functions.RuntimeInstanceStateDraining},
		{State: functions.RuntimeInstanceStateFailed},
	}
	if got := activeRuntimeInstanceCount(instances); got != 3 {
		t.Fatalf("activeRuntimeInstanceCount() = %d, want 3", got)
	}
}
