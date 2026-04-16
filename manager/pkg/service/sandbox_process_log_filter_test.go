package service

import (
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSandboxProcessLogReadCloserFiltersProcdLogs(t *testing.T) {
	processLine := `{"message":"sandbox process output","process_id":"ctx-1","process_type":"cmd","source":"stdout","data":"hello"}`
	timestampedProcessLine := `2026-04-17T12:34:56.789123456Z {"message":"sandbox process output","process_id":"ctx-1","process_type":"cmd","source":"stderr","data":"failed"}`
	input := strings.Join([]string{
		`{"level":"info","ts":"2026-04-17T12:34:56Z","msg":"procd started"}`,
		processLine,
		`plain procd log line`,
		timestampedProcessLine,
		`{"message":"sandbox process output","process_id":"ctx-1","process_type":"cmd","source":"prompt","data":""}`,
		`{"message":"user output","source":"stdout","data":"ignored"}`,
	}, "\n")

	filtered := newSandboxProcessLogReadCloser(io.NopCloser(strings.NewReader(input)))
	data, err := io.ReadAll(filtered)
	require.NoError(t, err)
	require.NoError(t, filtered.Close())

	assert.Equal(t, processLine+"\n"+timestampedProcessLine+"\n", string(data))
}

func TestIsSandboxProcessLogLineRejectsMissingData(t *testing.T) {
	line := []byte(`{"message":"sandbox process output","process_id":"ctx-1","process_type":"cmd","source":"stdout"}`)

	assert.False(t, isSandboxProcessLogLine(line))
}
