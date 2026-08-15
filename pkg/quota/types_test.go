package quota

import (
	"errors"
	"testing"
)

func TestDimensionsContainsFiveManagedKeys(t *testing.T) {
	got := Dimensions()
	if len(got) != 5 {
		t.Fatalf("Dimensions() len = %d, want 5: %v", len(got), got)
	}
	for _, dimension := range got {
		if !KnownDimension(dimension) {
			t.Fatalf("Dimensions() contains unknown key %q", dimension)
		}
	}
	if KnownDimension(Dimension("egress")) || KnownDimension(Dimension("ingress")) {
		t.Fatal("legacy cumulative network dimensions remain supported")
	}
	if KnownDimension(Dimension("cpu_millicpu")) || KnownDimension(Dimension("memory_mib")) {
		t.Fatal("compute resource dimensions remain supported")
	}
	if KindForDimension(DimensionSandboxClaims) != KindRate || UnitForDimension(DimensionSandboxClaims) != "claims" {
		t.Fatal("sandbox_claims is not a claims rate quota")
	}
}

func TestValidatePolicyValuesRejectsNonzeroBurstForZeroRate(t *testing.T) {
	err := ValidatePolicyValues(DimensionAPIRequests, 0, 1000, 1)
	if err == nil {
		t.Fatal("ValidatePolicyValues() error = nil, want zero-rate burst rejection")
	}
}

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

func TestIsExceeded(t *testing.T) {
	err := (&Decision{Allowed: false}).Err()
	if !IsExceeded(err) {
		t.Fatal("IsExceeded = false, want true")
	}
	if IsExceeded(errors.New("other")) {
		t.Fatal("IsExceeded = true for non-quota error")
	}
}

func TestNewStatus(t *testing.T) {
	limitValue := int64(5)
	status := NewStatus("team-1", DimensionActiveSandboxes, &Policy{
		TeamID:     "team-1",
		Dimension:  DimensionActiveSandboxes,
		Kind:       KindCapacity,
		LimitValue: limitValue,
		Source:     SourceTeamOverride,
	}, 2)
	if status.Unlimited {
		t.Fatal("Unlimited = true, want false")
	}
	if status.LimitValue == nil || *status.LimitValue != limitValue {
		t.Fatalf("LimitValue = %v, want %d", status.LimitValue, limitValue)
	}
	if status.Current == nil || *status.Current != 2 || status.Remaining == nil || *status.Remaining != 3 {
		t.Fatalf("Remaining = %v, want 3", status.Remaining)
	}
	if status.Unit != "count" {
		t.Fatalf("Unit = %q, want count", status.Unit)
	}

	rate := NewStatus("team-1", DimensionAPIRequests, &Policy{
		TeamID:     "team-1",
		Dimension:  DimensionAPIRequests,
		Kind:       KindRate,
		LimitValue: 100,
		IntervalMS: 1000,
		BurstValue: 200,
		Source:     SourceRegionDefault,
	}, 10)
	if rate.Current != nil || rate.Remaining != nil {
		t.Fatalf("rate status = %+v, want no cumulative current or remaining", rate)
	}
	if rate.IntervalMS == nil || *rate.IntervalMS != 1000 || rate.BurstValue == nil || *rate.BurstValue != 200 {
		t.Fatalf("rate status = %+v, want interval and burst", rate)
	}

	unlimited := NewStatus("team-1", DimensionNetworkEgress, nil, 10)
	if !unlimited.Unlimited || unlimited.LimitValue != nil || unlimited.Remaining != nil || unlimited.Current != nil {
		t.Fatalf("unlimited status = %+v, want nil rate values", unlimited)
	}
	if unlimited.Unit != "bytes" {
		t.Fatalf("Unit = %q, want bytes", unlimited.Unit)
	}
}
