package main

import (
	"time"

	coreobs "github.com/sandbox0-ai/sandbox0/pkg/observability/core"
	"go.uber.org/zap"
)

func newProcdLogger(level string) (*zap.Logger, error) {
	logger, err := coreobs.NewLogger(coreobs.LoggerConfig{ServiceName: "procd", Level: level})
	if err != nil {
		return nil, err
	}
	// Formatting time.Local can lazily read /etc/localtime from the demand-
	// paged RootFS on the first log. Keep only log timestamps in UTC: changing
	// TZ or time.Local would also change user processes and application code.
	return logger.WithOptions(zap.WithClock(procdLogClock{})), nil
}

type procdLogClock struct{}

func (procdLogClock) Now() time.Time { return time.Now().UTC() }

func (procdLogClock) NewTicker(interval time.Duration) *time.Ticker {
	return time.NewTicker(interval)
}
