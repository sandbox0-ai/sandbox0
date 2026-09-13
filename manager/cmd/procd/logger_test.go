package main

import (
	"testing"
	"time"

	"go.uber.org/zap/zapcore"
)

func TestProcdLoggerPreservesConfiguredLevel(t *testing.T) {
	for _, test := range []struct {
		level string
		want  zapcore.Level
	}{
		{level: "debug", want: zapcore.DebugLevel},
		{level: "info", want: zapcore.InfoLevel},
		{level: "warn", want: zapcore.WarnLevel},
		{level: "error", want: zapcore.ErrorLevel},
		{level: "invalid", want: zapcore.InfoLevel},
	} {
		t.Run(test.level, func(t *testing.T) {
			logger, err := newProcdLogger(test.level)
			if err != nil {
				t.Fatal(err)
			}
			if !logger.Core().Enabled(test.want) || logger.Core().Enabled(test.want-1) {
				t.Fatalf("logger did not preserve level %s", test.want)
			}
		})
	}
}

func TestProcdLogClockUsesUTCWithoutChangingProcessLocation(t *testing.T) {
	location := time.Local
	clock := procdLogClock{}
	before := time.Now()
	now := clock.Now()
	after := time.Now()
	if now.Location() != time.UTC || now.Before(before) || now.After(after) {
		t.Fatalf("UTC log clock returned an unexpected instant or location")
	}
	if time.Local != location {
		t.Fatal("log clock changed the process location")
	}
	ticker := clock.NewTicker(time.Millisecond)
	defer ticker.Stop()
	select {
	case <-ticker.C:
	case <-time.After(5 * time.Second):
		t.Fatal("log clock ticker did not fire")
	}
}
