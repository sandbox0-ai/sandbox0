package apikey

import (
	"strings"
	"testing"

	"github.com/sandbox0-ai/sandbox0/pkg/resourceguard"
)

func TestValidateCreateInputNameBoundaryAndOneByteOver(t *testing.T) {
	if err := ValidateCreateInput(strings.Repeat("n", int(MaxNameBytes)), []string{"viewer"}); err != nil {
		t.Fatalf("ValidateCreateInput(boundary) error = %v", err)
	}
	err := ValidateCreateInput(strings.Repeat("n", int(MaxNameBytes)+1), []string{"viewer"})
	if !resourceguard.IsTooLarge(err) {
		t.Fatalf("ValidateCreateInput(one byte over) error = %v, want TooLargeError", err)
	}
}

func TestValidateCreateInputUsesUTF8ByteLength(t *testing.T) {
	if err := ValidateCreateInput(strings.Repeat("界", int(MaxNameBytes/3)), nil); err != nil {
		t.Fatalf("ValidateCreateInput(UTF-8 boundary) error = %v", err)
	}
	err := ValidateCreateInput(strings.Repeat("界", int(MaxNameBytes/3)+1), nil)
	if !resourceguard.IsTooLarge(err) {
		t.Fatalf("ValidateCreateInput(UTF-8 over) error = %v, want TooLargeError", err)
	}
}
