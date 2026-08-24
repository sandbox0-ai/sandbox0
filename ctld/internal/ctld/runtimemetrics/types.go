package runtimemetrics

import (
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/sandboxobservability"
)

// SampleSink accepts projected runtime samples without blocking collection.
type SampleSink interface {
	TryEnqueue(sandboxobservability.RuntimeSample) bool
}

// CollectResult summarizes one bounded collection attempt.
type CollectResult struct {
	StatsReceived int
	Matched       int
	Enqueued      int
	Dropped       int
	Failed        int
}

const (
	defaultMaxConcurrency   = 4
	defaultRequestTimeout   = 2 * time.Second
	defaultCollectionBudget = 10 * time.Second
)
