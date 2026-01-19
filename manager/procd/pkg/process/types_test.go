// Package process provides process management for Procd.
package process

import (
	"os"
	"sync"
	"testing"
	"time"
)

// TestMultiplexedChannel_BasicFork tests basic fork functionality.
func TestMultiplexedChannel_BasicFork(t *testing.T) {
	mc := NewMultiplexedChannel[ProcessOutput](10)
	defer mc.Close()

	ch1, cancel1 := mc.Fork()
	defer cancel1()

	ch2, cancel2 := mc.Fork()
	defer cancel2()

	if mc.SubscriberCount() != 2 {
		t.Errorf("SubscriberCount() = %d, want 2", mc.SubscriberCount())
	}

	// Publish an event
	event := ProcessOutput{
		Source: OutputSourceStdout,
		Data:   []byte("test data"),
	}
	mc.Publish(event)

	// Both subscribers should receive the event
	select {
	case <-ch1:
		// OK
	case <-time.After(time.Second):
		t.Error("ch1 did not receive event")
	}

	select {
	case <-ch2:
		// OK
	case <-time.After(time.Second):
		t.Error("ch2 did not receive event")
	}
}

// TestMultiplexedChannel_Unsubscribe tests unsubscribe functionality.
func TestMultiplexedChannel_Unsubscribe(t *testing.T) {
	mc := NewMultiplexedChannel[ProcessOutput](10)
	defer mc.Close()

	ch, cancel := mc.Fork()

	// Initial subscriber count
	if mc.SubscriberCount() != 1 {
		t.Errorf("SubscriberCount() = %d, want 1", mc.SubscriberCount())
	}

	// Unsubscribe
	cancel()

	// Wait for dispatch goroutine to process
	time.Sleep(100 * time.Millisecond)

	// Subscriber count should be 0
	if mc.SubscriberCount() != 0 {
		t.Errorf("SubscriberCount() = %d, want 0", mc.SubscriberCount())
	}

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

// TestMultiplexedChannel_MultipleSubscribers tests many subscribers.
func TestMultiplexedChannel_MultipleSubscribers(t *testing.T) {
	mc := NewMultiplexedChannel[ProcessOutput](10)
	defer mc.Close()

	const numSubscribers = 100
	var subs []<-chan ProcessOutput
	var cancels []func()

	for i := 0; i < numSubscribers; i++ {
		ch, cancel := mc.Fork()
		subs = append(subs, ch)
		cancels = append(cancels, cancel)
	}

	if mc.SubscriberCount() != numSubscribers {
		t.Errorf("SubscriberCount() = %d, want %d", mc.SubscriberCount(), numSubscribers)
	}

	// Publish multiple events
	const numEvents = 10
	for i := 0; i < numEvents; i++ {
		mc.Publish(ProcessOutput{
			Source: OutputSourceStdout,
			Data:   []byte("test"),
		})
	}

	// Verify all subscribers received all events
	var wg sync.WaitGroup
	for _, ch := range subs {
		wg.Add(1)
		go func(c <-chan ProcessOutput) {
			defer wg.Done()
			count := 0
			timeout := time.After(5 * time.Second)
			for {
				select {
				case <-c:
					count++
					if count >= numEvents {
						return
					}
				case <-timeout:
					t.Errorf("subscriber only received %d/%d events", count, numEvents)
					return
				}
			}
		}(ch)
	}

	wg.Wait()

	// Clean up
	for _, cancel := range cancels {
		cancel()
	}
}

// TestMultiplexedChannel_ConcurrentForkUnsubscribe tests concurrent fork/unsubscribe.
func TestMultiplexedChannel_ConcurrentForkUnsubscribe(t *testing.T) {
	mc := NewMultiplexedChannel[ProcessOutput](10)
	defer mc.Close()

	const numGoroutines = 50
	var wg sync.WaitGroup

	// Concurrently fork and unsubscribe
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ch, cancel := mc.Fork()
			time.Sleep(time.Duration(i) * time.Microsecond)
			cancel()
			// Drain channel to allow goroutine to exit
			for range ch {
			}
		}()
	}

	wg.Wait()
}

// TestMultiplexedChannel_BufferFull tests behavior when buffer is full.
func TestMultiplexedChannel_BufferFull(t *testing.T) {
	mc := NewMultiplexedChannel[ProcessOutput](2) // Small buffer
	defer mc.Close()

	_, cancel := mc.Fork()
	defer cancel()

	// Fill the channel buffer
	for i := 0; i < 2; i++ {
		mc.Publish(ProcessOutput{Source: OutputSourceStdout})
	}

	// Publish more events - should be dropped, not block
	// This test verifies the dispatch doesn't block when subscriber buffer is full
	done := make(chan bool)
	go func() {
		for i := 0; i < 10; i++ {
			mc.Publish(ProcessOutput{Source: OutputSourceStdout})
		}
		close(done)
	}()

	select {
	case <-done:
		// OK - didn't block
	case <-time.After(time.Second):
		t.Error("Publish blocked when subscriber buffer was full")
	}
}

// TestMultiplexedChannel_ForkAfterClose tests fork after source is closed.
func TestMultiplexedChannel_ForkAfterClose(t *testing.T) {
	mc := NewMultiplexedChannel[ProcessOutput](10)

	// Close immediately
	mc.Close()

	// Fork after close should return a closed channel
	ch, cancel := mc.Fork()
	defer cancel()

	select {
	case _, ok := <-ch:
		if ok {
			t.Error("expected closed channel")
		}
	case <-time.After(time.Second):
		t.Error("timeout waiting for closed channel")
	}
}

// TestMultiplexedChannel_CloseAllSubscribers tests that all subscribers are closed when source closes.
func TestMultiplexedChannel_CloseAllSubscribers(t *testing.T) {
	mc := NewMultiplexedChannel[ProcessOutput](10)

	const numSubscribers = 10
	var chs []<-chan ProcessOutput
	var cancels []func()

	for i := 0; i < numSubscribers; i++ {
		ch, cancel := mc.Fork()
		chs = append(chs, ch)
		cancels = append(cancels, cancel)
	}

	// Publish an event before closing
	mc.Publish(ProcessOutput{Source: OutputSourceStdout})

	// Close the source
	mc.Close()

	// All subscribers should receive the event and then be closed
	for i, ch := range chs {
		select {
		case _, ok := <-ch:
			if !ok {
				t.Errorf("subscriber %d: expected to receive event before close", i)
			}
		case <-time.After(time.Second):
			t.Errorf("subscriber %d: timeout waiting for event", i)
		}

		// Channel should now be closed
		select {
		case _, ok := <-ch:
			if ok {
				t.Errorf("subscriber %d: expected channel to be closed", i)
			}
		case <-time.After(time.Second):
			t.Errorf("subscriber %d: timeout waiting for close", i)
		}
	}

	// Clean up cancels
	for _, cancel := range cancels {
		cancel()
	}
}

// TestMultiplexedChannel_UnsubscribeNonExistent tests unsubscribing a channel that was never subscribed.
func TestMultiplexedChannel_UnsubscribeNonExistent(t *testing.T) {
	mc := NewMultiplexedChannel[ProcessOutput](10)
	defer mc.Close()

	// Create a channel that was never subscribed
	ch := make(chan ProcessOutput, 10)

	// Unsubscribing should not panic
	mc.Unsubscribe(ch)

	if mc.SubscriberCount() != 0 {
		t.Errorf("SubscriberCount() = %d, want 0", mc.SubscriberCount())
	}
}

// TestBaseProcess_StateTransitions tests process state transitions.
func TestBaseProcess_StateTransitions(t *testing.T) {
	config := ProcessConfig{
		Type:     ProcessTypeREPL,
		Language: "python",
		CWD:      "/tmp",
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
		Type:     ProcessTypeREPL,
		Language: "python",
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
		Type:     ProcessTypeREPL,
		Language: "python",
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
		Type:     ProcessTypeREPL,
		Language: "python",
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
		Type:     ProcessTypeREPL,
		Language: "python",
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
		Type:     ProcessTypeREPL,
		Language: "python",
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
		Type:     ProcessTypeREPL,
		Language: "python",
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
		Type:     ProcessTypeREPL,
		Language: "python",
	}

	bp := NewBaseProcess("test-id", ProcessTypeREPL, config)

	// Don't set PTY
	err := bp.WriteInput([]byte("test"))
	if err != ErrProcessNotRunning {
		t.Errorf("WriteInput() error = %v, want %v", err, ErrProcessNotRunning)
	}
}

// BenchmarkMultiplexedChannel_Publish benchmarks publish performance.
func BenchmarkMultiplexedChannel_Publish(b *testing.B) {
	mc := NewMultiplexedChannel[ProcessOutput](1000)
	defer mc.Close()

	// Add some subscribers
	for i := 0; i < 10; i++ {
		ch, cancel := mc.Fork()
		defer cancel()
		go func(c <-chan ProcessOutput) {
			for range c {
			}
		}(ch)
	}

	event := ProcessOutput{
		Source: OutputSourceStdout,
		Data:   make([]byte, 100),
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mc.Publish(event)
	}
}

// BenchmarkMultiplexedChannel_Fork benchmarks fork performance.
func BenchmarkMultiplexedChannel_Fork(b *testing.B) {
	mc := NewMultiplexedChannel[ProcessOutput](100)
	defer mc.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ch, cancel := mc.Fork()
		// Don't defer cancel in benchmark - would overflow
		_ = ch
		_ = cancel
	}
}

// BenchmarkBaseProcess_StateRead benchmarks concurrent state reading.
func BenchmarkBaseProcess_StateRead(b *testing.B) {
	config := ProcessConfig{
		Type:     ProcessTypeREPL,
		Language: "python",
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
		Type:     ProcessTypeREPL,
		Language: "python",
	}

	bp := NewBaseProcess("test-id", ProcessTypeREPL, config)
	bp.SetPID(os.Getpid())

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = bp.ResourceUsage()
	}
}
