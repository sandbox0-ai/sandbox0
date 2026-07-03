package service

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/procd/pkg/process"
)

const maxSandboxProcessLogScanBytes = int(MaxSandboxLogLimitBytes) + process.DefaultContainerLogMaxLineBytes

type sandboxProcessLogReadCloser struct {
	source  io.ReadCloser
	scanner *bufio.Scanner
	buffer  []byte
}

type SandboxProcessLogEvent struct {
	Message     string `json:"message"`
	ProcessID   string `json:"process_id"`
	ProcessType string `json:"process_type"`
	PID         int    `json:"pid,omitempty"`
	Alias       string `json:"alias,omitempty"`
	Source      string `json:"source"`
	Data        string `json:"data"`
	Truncated   bool   `json:"truncated,omitempty"`
}

func newSandboxProcessLogReadCloser(source io.ReadCloser) io.ReadCloser {
	scanner := bufio.NewScanner(source)
	scanner.Buffer(make([]byte, 0, process.DefaultContainerLogMaxLineBytes), maxSandboxProcessLogScanBytes)
	return &sandboxProcessLogReadCloser{
		source:  source,
		scanner: scanner,
	}
}

func (r *sandboxProcessLogReadCloser) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}

	for len(r.buffer) == 0 {
		if !r.scanner.Scan() {
			if err := r.scanner.Err(); err != nil {
				return 0, fmt.Errorf("filter sandbox process logs: %w", err)
			}
			return 0, io.EOF
		}

		line := r.scanner.Bytes()
		if !isSandboxProcessLogLine(line) {
			continue
		}
		r.buffer = append(r.buffer[:0], line...)
		r.buffer = append(r.buffer, '\n')
	}

	n := copy(p, r.buffer)
	r.buffer = r.buffer[n:]
	return n, nil
}

func (r *sandboxProcessLogReadCloser) Close() error {
	return r.source.Close()
}

func isSandboxProcessLogLine(line []byte) bool {
	_, _, ok := ParseSandboxProcessLogLine(line)
	return ok
}

func ParseSandboxProcessLogLine(line []byte) (SandboxProcessLogEvent, time.Time, bool) {
	timestamp, payload := splitKubernetesLogTimestamp(line)
	payload = bytes.TrimSpace(payload)
	if len(payload) == 0 {
		return SandboxProcessLogEvent{}, time.Time{}, false
	}

	var decoded struct {
		SandboxProcessLogEvent
		Data *string `json:"data"`
	}
	if err := json.Unmarshal(payload, &decoded); err != nil {
		return SandboxProcessLogEvent{}, time.Time{}, false
	}
	if decoded.Data == nil {
		return SandboxProcessLogEvent{}, time.Time{}, false
	}
	event := decoded.SandboxProcessLogEvent
	event.Data = *decoded.Data
	if event.Message != process.ContainerLogProcessOutputMessage {
		return SandboxProcessLogEvent{}, time.Time{}, false
	}
	switch event.Source {
	case string(process.OutputSourceStdout), string(process.OutputSourceStderr), string(process.OutputSourcePTY):
		return event, timestamp, true
	default:
		return SandboxProcessLogEvent{}, time.Time{}, false
	}
}

func splitKubernetesLogTimestamp(line []byte) (time.Time, []byte) {
	trimmed := bytes.TrimLeft(line, " \t")
	if bytes.HasPrefix(trimmed, []byte("{")) {
		return time.Time{}, trimmed
	}

	idx := bytes.IndexByte(trimmed, ' ')
	if idx <= 0 {
		return time.Time{}, trimmed
	}
	timestamp, err := time.Parse(time.RFC3339Nano, string(trimmed[:idx]))
	if err != nil {
		return time.Time{}, trimmed
	}
	return timestamp.UTC(), trimmed[idx+1:]
}
