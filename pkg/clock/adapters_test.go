package clock

import "testing"

func TestZapFields(t *testing.T) {
	fields := zapFields([]any{"count", 3, "state", "ready"})
	if len(fields) != 2 || fields[0].Key != "count" || fields[1].Key != "state" {
		t.Fatalf("zapFields() = %#v", fields)
	}
}

func TestZapFieldsPreservesMalformedArguments(t *testing.T) {
	fields := zapFields([]any{"count", 3, "dangling"})
	if len(fields) != 1 || fields[0].Key != "args" {
		t.Fatalf("zapFields() = %#v", fields)
	}
}
