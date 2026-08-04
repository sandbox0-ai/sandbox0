package service

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseSandboxProcessLogLineAcceptsEmptyDataWithTimestamp(t *testing.T) {
	line := []byte(`2026-04-17T12:34:56.789123456Z {"message":"sandbox process output","process_id":"ctx-1","process_type":"cmd","source":"stdout","data":""}`)

	event, occurredAt, ok := ParseSandboxProcessLogLine(line)

	require.True(t, ok)
	assert.Equal(t, "ctx-1", event.ProcessID)
	assert.Equal(t, "stdout", event.Source)
	assert.Equal(t, "", event.Data)
	assert.Equal(t, time.Date(2026, 4, 17, 12, 34, 56, 789123456, time.UTC), occurredAt)
}
