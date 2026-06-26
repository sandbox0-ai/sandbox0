package quota

import (
	"errors"
	"testing"
)

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

func TestBytesToGBRoundUp(t *testing.T) {
	tests := []struct {
		name  string
		value int64
		want  int64
	}{
		{name: "zero", value: 0, want: 0},
		{name: "one byte", value: 1, want: 1},
		{name: "one gb", value: BytesPerGB, want: 1},
		{name: "one gb plus one byte", value: BytesPerGB + 1, want: 2},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := BytesToGBRoundUp(tt.value); got != tt.want {
				t.Fatalf("BytesToGBRoundUp(%d) = %d, want %d", tt.value, got, tt.want)
			}
		})
	}
}

func TestNewStatus(t *testing.T) {
	limitValue := int64(5)
	status := NewStatus("team-1", DimensionActiveSandboxes, &Limit{
		TeamID:     "team-1",
		Dimension:  DimensionActiveSandboxes,
		LimitValue: limitValue,
	}, 2)
	if status.Unlimited {
		t.Fatal("Unlimited = true, want false")
	}
	if status.LimitValue == nil || *status.LimitValue != limitValue {
		t.Fatalf("LimitValue = %v, want %d", status.LimitValue, limitValue)
	}
	if status.Remaining == nil || *status.Remaining != 3 {
		t.Fatalf("Remaining = %v, want 3", status.Remaining)
	}
	if status.Unit != "count" {
		t.Fatalf("Unit = %q, want count", status.Unit)
	}

	unlimited := NewStatus("team-1", DimensionEgress, nil, 10)
	if !unlimited.Unlimited || unlimited.LimitValue != nil || unlimited.Remaining != nil {
		t.Fatalf("unlimited status = %+v, want nil limit and remaining", unlimited)
	}
	if unlimited.Unit != "bytes" {
		t.Fatalf("Unit = %q, want bytes", unlimited.Unit)
	}
}
