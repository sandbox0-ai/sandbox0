package quota

import "testing"

func TestCheckAllowsUnlimitedWhenLimitIsNil(t *testing.T) {
	decision := Check("team-1", DimensionActiveSandboxes, 100, 1, nil)
	if !decision.Allowed {
		t.Fatalf("Allowed = false, want true")
	}
}

func TestCheckRejectsWhenRequestedWouldExceedLimit(t *testing.T) {
	decision := Check("team-1", DimensionActiveSandboxes, 2, 1, &Limit{
		TeamID:     "team-1",
		Dimension:  DimensionActiveSandboxes,
		LimitValue: 2,
	})
	if decision.Allowed {
		t.Fatalf("Allowed = true, want false")
	}
	if decision.Err() == nil {
		t.Fatal("Err() = nil, want quota error")
	}
}
