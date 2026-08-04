package framework

import (
	"context"
	"time"
)

// TestContext carries shared dependencies across suites.
type TestContext struct {
	Context    context.Context
	Cluster    *Cluster
	Namespace  string
	StartedAt  time.Time
	Timeout    time.Duration
	RetryDelay time.Duration
}

// NewTestContext creates a base context for E2E suites.
func NewTestContext(cluster *Cluster) *TestContext {
	return &TestContext{
		Context:    context.Background(),
		Cluster:    cluster,
		Namespace:  "sandbox0-e2e",
		StartedAt:  time.Now(),
		Timeout:    5 * time.Minute,
		RetryDelay: 2 * time.Second,
	}
}
