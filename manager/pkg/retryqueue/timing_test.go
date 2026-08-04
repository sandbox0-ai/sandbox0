package retryqueue

import (
	"testing"
	"time"
)

func TestExponentialBackoff(t *testing.T) {
	for _, test := range []struct {
		attempt int
		want    time.Duration
	}{
		{attempt: 0, want: time.Second},
		{attempt: 1, want: time.Second},
		{attempt: 2, want: 2 * time.Second},
		{attempt: 3, want: 4 * time.Second},
		{attempt: 100, want: 5 * time.Second},
	} {
		if got := ExponentialBackoff(test.attempt, time.Second, 5*time.Second); got != test.want {
			t.Fatalf("ExponentialBackoff(%d) = %s, want %s", test.attempt, got, test.want)
		}
	}
}

func TestDurationSeconds(t *testing.T) {
	for _, test := range []struct {
		duration time.Duration
		want     int
	}{
		{duration: 0, want: 1},
		{duration: 400 * time.Millisecond, want: 1},
		{duration: 1600 * time.Millisecond, want: 2},
	} {
		if got := DurationSeconds(test.duration); got != test.want {
			t.Fatalf("DurationSeconds(%s) = %d, want %d", test.duration, got, test.want)
		}
	}
}
