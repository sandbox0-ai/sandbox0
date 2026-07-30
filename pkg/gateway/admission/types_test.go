package admission

import (
	"errors"
	"strings"
	"testing"
)

func TestUpdateValidate(t *testing.T) {
	valid := Update{
		Version: 7,
		State:   StateRestricted,
		Source:  "  control-plane  ",
		Reason:  "  policy  ",
	}
	normalized, err := valid.Validate()
	if err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
	if normalized.Source != "control-plane" || normalized.Reason != "policy" {
		t.Fatalf("Validate() = %#v", normalized)
	}

	tests := []struct {
		name   string
		update Update
	}{
		{name: "negative version", update: Update{Version: -1, State: StateAllowed, Source: "source"}},
		{name: "invalid state", update: Update{Version: 1, State: "blocked", Source: "source"}},
		{name: "missing source", update: Update{Version: 1, State: StateAllowed}},
		{name: "source too long", update: Update{Version: 1, State: StateAllowed, Source: strings.Repeat("s", maxSourceLength+1)}},
		{name: "reason too long", update: Update{Version: 1, State: StateAllowed, Source: "source", Reason: strings.Repeat("r", maxReasonLength+1)}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := tt.update.Validate()
			if !errors.Is(err, ErrInvalidUpdate) {
				t.Fatalf("Validate() error = %v, want ErrInvalidUpdate", err)
			}
		})
	}
}

func TestRecordMatches(t *testing.T) {
	record := Record{
		Version: 2,
		State:   StateAllowed,
		Source:  "source",
		Reason:  "reason",
	}
	update := Update{
		Version: 2,
		State:   StateAllowed,
		Source:  "source",
		Reason:  "reason",
	}
	if !record.Matches(update) {
		t.Fatal("Matches() = false, want true")
	}
	update.Reason = "different"
	if record.Matches(update) {
		t.Fatal("Matches() = true, want false")
	}
}
