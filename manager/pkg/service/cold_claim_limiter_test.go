package service

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestColdClaimLimiterTimesOutWhenTemplateSlotIsBusy(t *testing.T) {
	limiter := newColdClaimLimiter(1, 20*time.Millisecond)
	release, err := limiter.acquire(context.Background(), "tpl-default/default")
	if err != nil {
		t.Fatalf("first acquire: %v", err)
	}
	defer release()

	_, err = limiter.acquire(context.Background(), "tpl-default/default")
	if !errors.Is(err, ErrColdClaimCapacityUnavailable) {
		t.Fatalf("second acquire error = %v, want ErrColdClaimCapacityUnavailable", err)
	}
}

func TestColdClaimLimiterScopesSlotsByTemplate(t *testing.T) {
	limiter := newColdClaimLimiter(1, 20*time.Millisecond)
	release, err := limiter.acquire(context.Background(), "tpl-default/default")
	if err != nil {
		t.Fatalf("first acquire: %v", err)
	}
	defer release()

	otherRelease, err := limiter.acquire(context.Background(), "tpl-node/node")
	if err != nil {
		t.Fatalf("acquire different template: %v", err)
	}
	otherRelease()
}

func TestColdClaimLimiterReleasesSlot(t *testing.T) {
	limiter := newColdClaimLimiter(1, 20*time.Millisecond)
	release, err := limiter.acquire(context.Background(), "tpl-default/default")
	if err != nil {
		t.Fatalf("first acquire: %v", err)
	}
	release()

	nextRelease, err := limiter.acquire(context.Background(), "tpl-default/default")
	if err != nil {
		t.Fatalf("second acquire after release: %v", err)
	}
	nextRelease()
}
