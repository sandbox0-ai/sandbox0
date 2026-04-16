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
	payload := bytes.TrimSpace(stripKubernetesLogTimestamp(line))
	if len(payload) == 0 {
		return false
	}

	var event sandboxProcessLogEvent
	if err := json.Unmarshal(payload, &event); err != nil {
		return false
	}
	if event.Message != process.ContainerLogProcessOutputMessage || event.Data == nil {
		return false
	}
	switch event.Source {
	case string(process.OutputSourceStdout), string(process.OutputSourceStderr), string(process.OutputSourcePTY):
		return true
	default:
		return false
	}
}

func stripKubernetesLogTimestamp(line []byte) []byte {
	trimmed := bytes.TrimLeft(line, " \t")
	if bytes.HasPrefix(trimmed, []byte("{")) {
		return trimmed
	}

	idx := bytes.IndexByte(trimmed, ' ')
	if idx <= 0 {
		return trimmed
	}
	if _, err := time.Parse(time.RFC3339Nano, string(trimmed[:idx])); err != nil {
		return trimmed
	}
	return trimmed[idx+1:]
}

type sandboxProcessLogEvent struct {
	Message string  `json:"message"`
	Source  string  `json:"source"`
	Data    *string `json:"data"`
}
