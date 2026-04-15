package process

import (
	"bytes"
	"encoding/json"
	"io"
	"os"
	"strings"
	"sync"
)

const DefaultContainerLogMaxLineBytes = 4096

var (
	defaultOutputForwarderMu sync.RWMutex
	defaultOutputForwarder   OutputForwarder
)

// SetDefaultOutputForwarder configures the output forwarder inherited by new processes.
func SetDefaultOutputForwarder(forwarder OutputForwarder) {
	defaultOutputForwarderMu.Lock()
	defer defaultOutputForwarderMu.Unlock()
	defaultOutputForwarder = forwarder
}

func defaultOutputForwarderSnapshot() OutputForwarder {
	defaultOutputForwarderMu.RLock()
	defer defaultOutputForwarderMu.RUnlock()
	return defaultOutputForwarder
}

// ContainerLogForwarderOptions configures process output mirroring to container logs.
type ContainerLogForwarderOptions struct {
	Stdout       io.Writer
	Stderr       io.Writer
	MaxLineBytes int
}

// ContainerLogForwarder writes bounded process output events as JSON lines.
type ContainerLogForwarder struct {
	mu           sync.Mutex
	stdout       io.Writer
	stderr       io.Writer
	maxLineBytes int
	streams      map[containerLogStreamKey]*containerLogLineState
}

// NewContainerLogForwarder creates a process output forwarder for pod logs.
func NewContainerLogForwarder(opts ContainerLogForwarderOptions) *ContainerLogForwarder {
	stdout := opts.Stdout
	if stdout == nil {
		stdout = os.Stdout
	}
	stderr := opts.Stderr
	if stderr == nil {
		stderr = os.Stderr
	}
	maxLineBytes := opts.MaxLineBytes
	if maxLineBytes <= 0 {
		maxLineBytes = DefaultContainerLogMaxLineBytes
	}
	return &ContainerLogForwarder{
		stdout:       stdout,
		stderr:       stderr,
		maxLineBytes: maxLineBytes,
		streams:      make(map[containerLogStreamKey]*containerLogLineState),
	}
}

// ForwardOutput mirrors a process output chunk to container stdout/stderr.
func (f *ContainerLogForwarder) ForwardOutput(desc ProcessDescriptor, output ProcessOutput) {
	if f == nil || len(output.Data) == 0 || output.Source == OutputSourcePrompt {
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.forwardLocked(desc, output.Source, output.Data)
}

// FlushProcessOutput writes any buffered partial lines for one process.
func (f *ContainerLogForwarder) FlushProcessOutput(desc ProcessDescriptor) {
	if f == nil {
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	for key, state := range f.streams {
		if key.processID != desc.ProcessID {
			continue
		}
		if len(state.data) > 0 {
			f.writeLineLocked(desc, key.source, state.data, false)
		}
		delete(f.streams, key)
	}
}

func (f *ContainerLogForwarder) forwardLocked(desc ProcessDescriptor, source OutputSource, data []byte) {
	key := containerLogStreamKey{processID: desc.ProcessID, source: source}
	state := f.streams[key]
	if state == nil {
		state = &containerLogLineState{}
		f.streams[key] = state
	}

	for len(data) > 0 {
		if state.discarding {
			if idx := bytes.IndexByte(data, '\n'); idx >= 0 {
				state.discarding = false
				data = data[idx+1:]
				continue
			}
			return
		}

		if idx := bytes.IndexByte(data, '\n'); idx >= 0 {
			linePart := trimTrailingCarriageReturn(data[:idx])
			if len(state.data)+len(linePart) > f.maxLineBytes {
				available := f.maxLineBytes - len(state.data)
				if available > 0 {
					state.data = append(state.data, linePart[:available]...)
				}
				f.writeLineLocked(desc, source, state.data, true)
			} else {
				state.data = append(state.data, linePart...)
				f.writeLineLocked(desc, source, state.data, false)
			}
			state.data = state.data[:0]
			data = data[idx+1:]
			continue
		}

		if len(state.data)+len(data) > f.maxLineBytes {
			available := f.maxLineBytes - len(state.data)
			if available > 0 {
				state.data = append(state.data, data[:available]...)
			}
			f.writeLineLocked(desc, source, state.data, true)
			state.data = state.data[:0]
			state.discarding = true
			return
		}

		state.data = append(state.data, data...)
		return
	}
}

func (f *ContainerLogForwarder) writeLineLocked(desc ProcessDescriptor, source OutputSource, data []byte, truncated bool) {
	line := containerLogLine{
		Message:     "sandbox process output",
		ProcessID:   desc.ProcessID,
		ProcessType: desc.ProcessType,
		PID:         desc.PID,
		Alias:       desc.Alias,
		Source:      source,
		Data:        strings.ToValidUTF8(string(data), "?"),
		Truncated:   truncated,
	}
	payload, err := json.Marshal(line)
	if err != nil {
		return
	}
	payload = append(payload, '\n')
	_, _ = f.writerForSource(source).Write(payload)
}

func (f *ContainerLogForwarder) writerForSource(source OutputSource) io.Writer {
	if source == OutputSourceStderr {
		return f.stderr
	}
	return f.stdout
}

func trimTrailingCarriageReturn(data []byte) []byte {
	if len(data) > 0 && data[len(data)-1] == '\r' {
		return data[:len(data)-1]
	}
	return data
}

type containerLogStreamKey struct {
	processID string
	source    OutputSource
}

type containerLogLineState struct {
	data       []byte
	discarding bool
}

type containerLogLine struct {
	Message     string       `json:"message"`
	ProcessID   string       `json:"process_id"`
	ProcessType ProcessType  `json:"process_type"`
	PID         int          `json:"pid,omitempty"`
	Alias       string       `json:"alias,omitempty"`
	Source      OutputSource `json:"source"`
	Data        string       `json:"data"`
	Truncated   bool         `json:"truncated,omitempty"`
}
