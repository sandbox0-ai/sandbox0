package process

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"
)

func TestContainerLogForwarderForwardsProcessOutput(t *testing.T) {
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	forwarder := NewContainerLogForwarder(ContainerLogForwarderOptions{
		Stdout:       &stdout,
		Stderr:       &stderr,
		MaxLineBytes: 64,
	})
	base := NewBaseProcess("ctx-test", ProcessTypeCMD, ProcessConfig{Type: ProcessTypeCMD, Alias: "worker"})
	base.SetOutputForwarder(forwarder)
	base.SetPID(123)
	outputCh := base.ReadOutput()

	base.PublishOutput(ProcessOutput{Source: OutputSourceStdout, Data: []byte("hello")})
	base.PublishOutput(ProcessOutput{Source: OutputSourceStderr, Data: []byte("failed\n")})
	base.PublishOutput(ProcessOutput{Source: OutputSourcePrompt})
	base.CloseOutput()

	select {
	case output := <-outputCh:
		if string(output.Data) != "hello" {
			t.Fatalf("process output data = %q, want hello", string(output.Data))
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for process output")
	}

	stdoutEvent := decodeContainerLogLine(t, stdout.Bytes())
	if stdoutEvent.Message != "sandbox process output" || stdoutEvent.ProcessID != "ctx-test" || stdoutEvent.ProcessType != ProcessTypeCMD || stdoutEvent.PID != 123 || stdoutEvent.Alias != "worker" || stdoutEvent.Source != OutputSourceStdout || stdoutEvent.Data != "hello" {
		t.Fatalf("unexpected stdout event: %#v", stdoutEvent)
	}
	stderrEvent := decodeContainerLogLine(t, stderr.Bytes())
	if stderrEvent.Source != OutputSourceStderr || stderrEvent.Data != "failed" {
		t.Fatalf("unexpected stderr event: %#v", stderrEvent)
	}
}

func TestContainerLogForwarderTruncatesLongLines(t *testing.T) {
	var stdout bytes.Buffer
	forwarder := NewContainerLogForwarder(ContainerLogForwarderOptions{Stdout: &stdout, MaxLineBytes: 5})
	base := NewBaseProcess("ctx-test", ProcessTypeCMD, ProcessConfig{Type: ProcessTypeCMD})
	base.SetOutputForwarder(forwarder)

	base.PublishOutput(ProcessOutput{Source: OutputSourceStdout, Data: []byte("123456789\nnext\n")})

	events := decodeContainerLogLines(t, stdout.Bytes())
	if len(events) != 2 {
		t.Fatalf("event count = %d, want 2: %s", len(events), stdout.String())
	}
	if events[0].Data != "12345" || !events[0].Truncated {
		t.Fatalf("first event = %#v, want truncated 12345", events[0])
	}
	if events[1].Data != "next" || events[1].Truncated {
		t.Fatalf("second event = %#v, want full next", events[1])
	}
}

func TestDefaultOutputForwarderIsInheritedByNewProcesses(t *testing.T) {
	var stdout bytes.Buffer
	forwarder := NewContainerLogForwarder(ContainerLogForwarderOptions{Stdout: &stdout})
	SetDefaultOutputForwarder(forwarder)
	t.Cleanup(func() { SetDefaultOutputForwarder(nil) })

	base := NewBaseProcess("ctx-default", ProcessTypeCMD, ProcessConfig{Type: ProcessTypeCMD, Alias: "helper"})
	base.PublishOutput(ProcessOutput{Source: OutputSourceStdout, Data: []byte("ready\n")})

	event := decodeContainerLogLine(t, stdout.Bytes())
	if event.ProcessID != "ctx-default" || event.Alias != "helper" || event.Source != OutputSourceStdout || event.Data != "ready" {
		t.Fatalf("unexpected inherited forwarder event: %#v", event)
	}
}

func decodeContainerLogLine(t *testing.T, data []byte) containerLogLine {
	t.Helper()
	events := decodeContainerLogLines(t, data)
	if len(events) != 1 {
		t.Fatalf("event count = %d, want 1: %s", len(events), string(data))
	}
	return events[0]
}

func decodeContainerLogLines(t *testing.T, data []byte) []containerLogLine {
	t.Helper()
	lines := bytes.Split(bytes.TrimSpace(data), []byte("\n"))
	events := make([]containerLogLine, 0, len(lines))
	for _, line := range lines {
		if len(line) == 0 {
			continue
		}
		var event containerLogLine
		if err := json.Unmarshal(line, &event); err != nil {
			t.Fatalf("unmarshal event %q: %v", string(line), err)
		}
		events = append(events, event)
	}
	return events
}
