package main

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestParseActiveLockInitialDelay(t *testing.T) {
	tests := []struct {
		name    string
		raw     string
		want    time.Duration
		wantErr bool
	}{
		{name: "empty"},
		{name: "duration", raw: " 30s ", want: 30 * time.Second},
		{name: "invalid", raw: "later", wantErr: true},
		{name: "negative", raw: "-1s", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseActiveLockInitialDelay(tt.raw)
			if (err != nil) != tt.wantErr {
				t.Fatalf("parseActiveLockInitialDelay(%q) error = %v, wantErr %v", tt.raw, err, tt.wantErr)
			}
			if got != tt.want {
				t.Fatalf("parseActiveLockInitialDelay(%q) = %s, want %s", tt.raw, got, tt.want)
			}
		})
	}
}

type testActiveLock struct {
	onClose func()
}

func (g *testActiveLock) Close() error {
	if g.onClose != nil {
		g.onClose()
	}
	return nil
}

type testNetdRunner struct {
	run func(context.Context) error
}

func (r testNetdRunner) Run(ctx context.Context) error { return r.run(ctx) }

func TestRunGuardedNetdRepeatedlyYieldsAndReacquiresInProcess(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var mu sync.Mutex
	events := make([]string, 0, 5)
	acquires := 0
	runs := 0
	err := runGuardedNetd(
		ctx,
		"/tmp/test-netd.lock",
		0,
		10*time.Millisecond,
		time.Millisecond,
		func(context.Context, string) (activeLock, error) {
			mu.Lock()
			defer mu.Unlock()
			acquires++
			cycle := acquires
			events = append(events, "acquire")
			return &testActiveLock{onClose: func() {
				mu.Lock()
				defer mu.Unlock()
				events = append(events, "close")
				if cycle == 2 {
					cancel()
				}
			}}, nil
		},
		func() netdRunner {
			runs++
			return testNetdRunner{run: func(runCtx context.Context) error {
				<-runCtx.Done()
				return runCtx.Err()
			}}
		},
		nil,
	)
	if err == nil || err != context.Canceled {
		t.Fatalf("runGuardedNetd() error = %v, want context canceled", err)
	}
	if acquires != 2 || runs != 2 {
		t.Fatalf("acquires=%d runs=%d, want two in-process cycles", acquires, runs)
	}
	wantEvents := []string{"acquire", "close", "acquire", "close"}
	for i := range wantEvents {
		if i >= len(events) || events[i] != wantEvents[i] {
			t.Fatalf("events = %#v, want %#v", events, wantEvents)
		}
	}
}
