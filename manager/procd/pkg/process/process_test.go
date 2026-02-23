// Package process provides process management for Procd.
package process

import (
	"io"
	"os"
	"sync"
	"testing"
	"time"
)

// TestBaseProcess_StateTransitions tests process state transitions.
func TestBaseProcess_StateTransitions(t *testing.T) {
	config := ProcessConfig{
		Type:  ProcessTypeREPL,
		Alias: "python",
		CWD:   "/tmp",
	}

	bp := NewBaseProcess("test-id", ProcessTypeREPL, config)

	// Initial state
	if bp.State() != ProcessStateCreated {
		t.Errorf("Initial state = %s, want %s", bp.State(), ProcessStateCreated)
	}

	// Set to starting
	bp.SetState(ProcessStateStarting)
	if bp.State() != ProcessStateStarting {
		t.Errorf("State = %s, want %s", bp.State(), ProcessStateStarting)
	}

	// Set to running
	bp.SetState(ProcessStateRunning)
	if bp.State() != ProcessStateRunning {
		t.Errorf("State = %s, want %s", bp.State(), ProcessStateRunning)
	}

	if !bp.IsRunning() {
		t.Error("IsRunning() = false, want true")
	}
	if bp.IsPaused() {
		t.Error("IsPaused() = true, want false")
	}

	// Set to paused
	bp.SetState(ProcessStatePaused)
	if bp.State() != ProcessStatePaused {
		t.Errorf("State = %s, want %s", bp.State(), ProcessStatePaused)
	}

	if bp.IsRunning() {
		t.Error("IsRunning() = true, want false")
	}
	if !bp.IsPaused() {
		t.Error("IsPaused() = false, want true")
	}

	// Set back to running
	bp.SetState(ProcessStateRunning)
	if bp.IsPaused() {
		t.Error("IsPaused() = true, want false")
	}

	// Set to stopped
	bp.SetState(ProcessStateStopped)
	if bp.State() != ProcessStateStopped {
		t.Errorf("State = %s, want %s", bp.State(), ProcessStateStopped)
	}
}

// TestBaseProcess_PIDAndExitCode tests PID and exit code management.
func TestBaseProcess_PIDAndExitCode(t *testing.T) {
	config := ProcessConfig{
		Type:  ProcessTypeREPL,
		Alias: "python",
	}

	bp := NewBaseProcess("test-id", ProcessTypeREPL, config)

	// Initial PID should be 0
	if bp.PID() != 0 {
		t.Errorf("Initial PID = %d, want 0", bp.PID())
	}

	// Set PID
	bp.SetPID(12345)
	if bp.PID() != 12345 {
		t.Errorf("PID = %d, want 12345", bp.PID())
	}

	// Exit code
	bp.SetExitCode(0)
	code, err := bp.ExitCode()
	if err != nil {
		t.Errorf("ExitCode() error = %v", err)
	}
	if code != 0 {
		t.Errorf("ExitCode() = %d, want 0", code)
	}

	bp.SetExitCode(1)
	code, err = bp.ExitCode()
	if err != nil {
		t.Errorf("ExitCode() error = %v", err)
	}
	if code != 1 {
		t.Errorf("ExitCode() = %d, want 1", code)
	}
}

// TestBaseProcess_IDAndType tests ID and type getters.
func TestBaseProcess_IDAndType(t *testing.T) {
	config := ProcessConfig{
		Type:  ProcessTypeREPL,
		Alias: "python",
	}

	bp := NewBaseProcess("test-id-123", ProcessTypeREPL, config)

	if bp.ID() != "test-id-123" {
		t.Errorf("ID() = %s, want test-id-123", bp.ID())
	}

	if bp.Type() != ProcessTypeREPL {
		t.Errorf("Type() = %s, want %s", bp.Type(), ProcessTypeREPL)
	}
}

// TestBaseProcess_PTYManagement tests PTY management.
func TestBaseProcess_PTYManagement(t *testing.T) {
	config := ProcessConfig{
		Type:  ProcessTypeREPL,
		Alias: "python",
	}

	bp := NewBaseProcess("test-id", ProcessTypeREPL, config)

	// Initially no PTY
	if bp.GetPTY() != nil {
		t.Error("GetPTY() = non-nil, want nil")
	}

	// Set PTY (use a pipe as a mock PTY for testing)
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	defer w.Close()

	bp.SetPTY(w)

	if bp.GetPTY() == nil {
		t.Error("GetPTY() = nil, want non-nil")
	}

	// WriteInput should work
	err = bp.WriteInput([]byte("test"))
	if err != nil {
		t.Errorf("WriteInput() error = %v", err)
	}
}

// TestBaseProcess_ResourceUsageWithNoPID tests ResourceUsage with no PID set.
func TestBaseProcess_ResourceUsageWithNoPID(t *testing.T) {
	config := ProcessConfig{
		Type:  ProcessTypeREPL,
		Alias: "python",
	}

	bp := NewBaseProcess("test-id", ProcessTypeREPL, config)

	// Don't set PID - should return zero values
	usage := bp.ResourceUsage()

	if usage.CPUPercent != -1 {
		t.Errorf("CPUPercent = %f, want -1", usage.CPUPercent)
	}
	if usage.MemoryRSS != 0 {
		t.Errorf("MemoryRSS = %d, want 0", usage.MemoryRSS)
	}
	if usage.MemoryVMS != 0 {
		t.Errorf("MemoryVMS = %d, want 0", usage.MemoryVMS)
	}
	if usage.ThreadCount != 0 {
		t.Errorf("ThreadCount = %d, want 0", usage.ThreadCount)
	}
	if usage.OpenFiles != 0 {
		t.Errorf("OpenFiles = %d, want 0", usage.OpenFiles)
	}
}

// TestBaseProcess_PublishAndReadOutput tests output publishing and reading.
func TestBaseProcess_PublishAndReadOutput(t *testing.T) {
	config := ProcessConfig{
		Type:  ProcessTypeREPL,
		Alias: "python",
	}

	bp := NewBaseProcess("test-id", ProcessTypeREPL, config)

	output := ProcessOutput{
		Source: OutputSourceStdout,
		Data:   []byte("test output"),
	}

	// Fork before publishing
	ch := bp.ReadOutput()

	// Publish output
	bp.PublishOutput(output)

	// Should receive the output
	select {
	case received := <-ch:
		if string(received.Data) != string(output.Data) {
			t.Errorf("Received data = %s, want %s", string(received.Data), string(output.Data))
		}
	case <-time.After(time.Second):
		t.Error("timeout waiting for output")
	}

	// Close output
	bp.CloseOutput()

	// Channel should be closed
	select {
	case _, ok := <-ch:
		if ok {
			t.Error("expected channel to be closed")
		}
	case <-time.After(time.Second):
		t.Error("timeout waiting for channel close")
	}
}

// TestBaseProcess_ConcurrentStateAccess tests concurrent state access.
func TestBaseProcess_ConcurrentStateAccess(t *testing.T) {
	config := ProcessConfig{
		Type:  ProcessTypeREPL,
		Alias: "python",
	}

	bp := NewBaseProcess("test-id", ProcessTypeREPL, config)
	bp.SetPID(12345)

	const numGoroutines = 100
	const numOps = 100

	var wg sync.WaitGroup
	done := make(chan struct{})

	// Concurrent state readers
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < numOps; j++ {
				select {
				case <-done:
					return
				default:
					_ = bp.State()
					_ = bp.IsRunning()
					_ = bp.IsPaused()
					_ = bp.PID()
				}
			}
		}()
	}

	// Concurrent state writers
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			for j := 0; j < numOps/10; j++ {
				select {
				case <-done:
					return
				default:
					if n%2 == 0 {
						bp.SetState(ProcessStateRunning)
					} else {
						bp.SetState(ProcessStatePaused)
					}
				}
			}
		}(i)
	}

	// Let it run for a bit
	time.Sleep(100 * time.Millisecond)
	close(done)
	wg.Wait()

	// Verify final state is accessible
	_ = bp.State()
	_ = bp.IsRunning()
}

// TestProcessConfig_Defaults tests ProcessConfig default values.
func TestProcessConfig_Defaults(t *testing.T) {
	config := ProcessConfig{
		Type: ProcessTypeREPL,
	}

	if config.Type != ProcessTypeREPL {
		t.Errorf("Type = %s, want %s", config.Type, ProcessTypeREPL)
	}

	// Empty slices and maps should be nil or empty
	if config.Command != nil && len(config.Command) > 0 {
		t.Error("Command should be empty or nil")
	}

	if config.EnvVars == nil {
		// Nil is OK, but let's make sure we can use it
		config.EnvVars = make(map[string]string)
	}

	config.EnvVars["TEST"] = "value"
	if config.EnvVars["TEST"] != "value" {
		t.Error("EnvVars map not working")
	}
}

// TestOutputSource_Values tests OutputSource constants.
func TestOutputSource_Values(t *testing.T) {
	tests := []struct {
		value    OutputSource
		expected string
	}{
		{OutputSourceStdout, "stdout"},
		{OutputSourceStderr, "stderr"},
		{OutputSourcePTY, "pty"},
	}

	for _, tt := range tests {
		if string(tt.value) != tt.expected {
			t.Errorf("OutputSource = %s, want %s", tt.value, tt.expected)
		}
	}
}

// TestProcessState_Values tests ProcessState constants.
func TestProcessState_Values(t *testing.T) {
	states := []ProcessState{
		ProcessStateCreated,
		ProcessStateStarting,
		ProcessStateRunning,
		ProcessStatePaused,
		ProcessStateStopped,
		ProcessStateKilled,
		ProcessStateCrashed,
	}

	for _, state := range states {
		if state == "" {
			t.Errorf("ProcessState constant should not be empty")
		}
	}

	// Verify they're all different
	seen := make(map[ProcessState]bool)
	for _, state := range states {
		if seen[state] {
			t.Errorf("Duplicate ProcessState: %s", state)
		}
		seen[state] = true
	}
}

// TestBaseProcess_WriteInputNoPTY tests WriteInput when no PTY is set.
func TestBaseProcess_WriteInputNoPTY(t *testing.T) {
	config := ProcessConfig{
		Type:  ProcessTypeREPL,
		Alias: "python",
	}

	bp := NewBaseProcess("test-id", ProcessTypeREPL, config)

	// Don't set PTY
	err := bp.WriteInput([]byte("test"))
	if err != ErrProcessNotRunning {
		t.Errorf("WriteInput() error = %v, want %v", err, ErrProcessNotRunning)
	}
}

// TestBaseProcess_WriteInputBufferFull tests input buffer backpressure.
func TestBaseProcess_WriteInputBufferFull(t *testing.T) {
	config := ProcessConfig{
		Type:       ProcessTypeREPL,
		Alias:      "python",
		BufferSize: 1,
	}

	bp := NewBaseProcess("test-id", ProcessTypeREPL, config)

	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	defer w.Close()

	bp.SetPTY(w)

	if err := bp.WriteInput([]byte("first")); err != nil {
		t.Fatalf("WriteInput(first) error = %v", err)
	}
	if err := bp.WriteInput([]byte("second")); err != ErrInputBufferFull {
		t.Errorf("WriteInput(second) error = %v, want %v", err, ErrInputBufferFull)
	}

	bp.stopInputWriter()
}

// TestBaseProcess_InputReadyFlushesQueue tests queued input flush on prompt.
func TestBaseProcess_InputReadyFlushesQueue(t *testing.T) {
	config := ProcessConfig{
		Type:       ProcessTypeREPL,
		Alias:      "python",
		BufferSize: 2,
	}

	bp := NewBaseProcess("test-id", ProcessTypeREPL, config)

	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	defer w.Close()

	bp.SetPTY(w)

	if err := bp.WriteInput([]byte("one")); err != nil {
		t.Fatalf("WriteInput(one) error = %v", err)
	}
	if err := bp.WriteInput([]byte("two")); err != nil {
		t.Fatalf("WriteInput(two) error = %v", err)
	}

	bp.signalInputReady()

	expected := "onetwo"
	done := make(chan string, 1)
	go func() {
		buf := make([]byte, len(expected))
		n, _ := io.ReadFull(r, buf)
		done <- string(buf[:n])
	}()

	select {
	case got := <-done:
		if got != expected {
			t.Errorf("input flush = %q, want %q", got, expected)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for input flush")
	}

	bp.stopInputWriter()
}

// TestBaseProcess_WriteInputFinished tests rejecting input on finished process.
func TestBaseProcess_WriteInputFinished(t *testing.T) {
	config := ProcessConfig{
		Type:  ProcessTypeREPL,
		Alias: "python",
	}

	bp := NewBaseProcess("test-id", ProcessTypeREPL, config)

	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	defer w.Close()

	bp.SetPTY(w)
	bp.SetState(ProcessStateStopped)

	if err := bp.WriteInput([]byte("test")); err != ErrProcessFinished {
		t.Errorf("WriteInput() error = %v, want %v", err, ErrProcessFinished)
	}

	bp.stopInputWriter()
}

// BenchmarkBaseProcess_StateRead benchmarks concurrent state reading.
func BenchmarkBaseProcess_StateRead(b *testing.B) {
	config := ProcessConfig{
		Type:  ProcessTypeREPL,
		Alias: "python",
	}

	bp := NewBaseProcess("test-id", ProcessTypeREPL, config)
	bp.SetPID(12345)
	bp.SetState(ProcessStateRunning)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = bp.State()
		_ = bp.IsRunning()
		_ = bp.PID()
	}
}

// BenchmarkBaseProcess_ResourceUsage benchmarks resource usage calculation.
func BenchmarkBaseProcess_ResourceUsage(b *testing.B) {
	config := ProcessConfig{
		Type:  ProcessTypeREPL,
		Alias: "python",
	}

	bp := NewBaseProcess("test-id", ProcessTypeREPL, config)
	bp.SetPID(os.Getpid())

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = bp.ResourceUsage()
	}
}
