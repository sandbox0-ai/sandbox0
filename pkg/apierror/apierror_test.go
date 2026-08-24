package apierror

import (
	"errors"
	"fmt"
	"testing"
)

func TestCategoriesSurviveWrapping(t *testing.T) {
	cause := errors.New("changed")
	conflict := fmt.Errorf("save: %w", NewConflict("sandbox", "sandbox-a", cause))
	if !IsConflict(conflict) || IsNotFound(conflict) || !errors.Is(conflict, cause) {
		t.Fatalf("unexpected wrapped conflict: %v", conflict)
	}
	if !IsNotFound(NewNotFound("sandbox", "sandbox-a")) {
		t.Fatal("not-found category was not preserved")
	}
	if !IsForbidden(NewForbidden("sandbox", "sandbox-a", cause)) {
		t.Fatal("forbidden category was not preserved")
	}
}
