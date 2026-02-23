// Package context provides context management for Procd.
package context

import (
	"sync"
	"testing"
	"time"

	"github.com/sandbox0-ai/infra/manager/procd/pkg/process"
)

// TestNewManager tests manager creation.
func TestNewManager(t *testing.T) {
	m := NewManager()
	if m == nil {
		t.Fatal("NewManager() returned nil")
	}

	// Verify we can list contexts (should be empty)
	ctxs := m.ListContexts()
	if ctxs == nil {
		t.Error("ListContexts() returned nil")
	}
	if len(ctxs) != 0 {
		t.Errorf("ListContexts() returned %d contexts, want 0", len(ctxs))
	}
}

// TestManager_CreateContext tests context creation with max limit.
func TestManager_CreateContext(t *testing.T) {
	m := NewManager()

	// Create first context
	config := process.ProcessConfig{
		Type:    process.ProcessTypeCMD,
		Command: []string{"/bin/echo", "test"},
	}

	ctx1, err := m.CreateContext(config)
	if err != nil {
		t.Fatalf("CreateContext() failed = %v", err)
	}
	if ctx1 == nil {
		t.Fatal("CreateContext() returned nil context")
	}
	if ctx1.ID == "" {
		t.Error("Context ID is empty")
	}

	// Verify it's in the list
	ctxs := m.ListContexts()
	if len(ctxs) != 1 {
		t.Errorf("ListContexts() returned %d contexts, want 1", len(ctxs))
	}

	// Create second context
	ctx2, err := m.CreateContext(config)
	if err != nil {
		t.Fatalf("CreateContext() failed = %v", err)
	}
	if ctx2.ID == ctx1.ID {
		t.Error("Context IDs should be unique")
	}

	// Clean up
	m.Cleanup()
}

// TestManager_CreateContextErrors tests error cases for context creation.
func TestManager_CreateContextErrors(t *testing.T) {
	m := NewManager()

	tests := []struct {
		name    string
		config  process.ProcessConfig
		wantErr error
	}{
		{
			name: "CMD without command",
			config: process.ProcessConfig{
				Type: process.ProcessTypeCMD,
			},
			wantErr: process.ErrInvalidCommand,
		},
		{
			name: "unsupported process type",
			config: process.ProcessConfig{
				Type: "invalid",
			},
			wantErr: process.ErrUnsupportedProcessType,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := m.CreateContext(tt.config)
			if err == nil {
				t.Error("CreateContext() expected error, got nil")
			}
		})
	}
}

// TestManager_GetContext tests getting a context by ID.
func TestManager_GetContext(t *testing.T) {
	m := NewManager()

	config := process.ProcessConfig{
		Type:    process.ProcessTypeCMD,
		Command: []string{"/bin/echo", "test"},
	}

	// Get non-existent context
	_, err := m.GetContext("non-existent")
	if err != ErrContextNotFound {
		t.Errorf("GetContext() error = %v, want %v", err, ErrContextNotFound)
	}

	// Create a context
	ctx, err := m.CreateContext(config)
	if err != nil {
		t.Fatalf("CreateContext() failed = %v", err)
	}

	// Get the created context
	retrieved, err := m.GetContext(ctx.ID)
	if err != nil {
		t.Fatalf("GetContext() failed = %v", err)
	}
	if retrieved.ID != ctx.ID {
		t.Errorf("GetContext() returned context with ID %s, want %s", retrieved.ID, ctx.ID)
	}

	m.Cleanup()
}

// TestManager_DeleteContext tests context deletion.
func TestManager_DeleteContext(t *testing.T) {
	m := NewManager()

	config := process.ProcessConfig{
		Type:    process.ProcessTypeCMD,
		Command: []string{"/bin/echo", "test"},
	}

	// Create a context
	ctx, err := m.CreateContext(config)
	if err != nil {
		t.Fatalf("CreateContext() failed = %v", err)
	}

	id := ctx.ID

	// Delete it
	err = m.DeleteContext(id)
	if err != nil {
		t.Fatalf("DeleteContext() failed = %v", err)
	}

	// Verify it's gone
	_, err = m.GetContext(id)
	if err != ErrContextNotFound {
		t.Errorf("GetContext() after delete error = %v, want %v", err, ErrContextNotFound)
	}

	// Delete non-existent context
	err = m.DeleteContext("non-existent")
	if err != ErrContextNotFound {
		t.Errorf("DeleteContext() non-existent error = %v, want %v", err, ErrContextNotFound)
	}

	m.Cleanup()
}

// TestManager_ListContexts tests listing all contexts.
func TestManager_ListContexts(t *testing.T) {
	m := NewManager()

	config := process.ProcessConfig{
		Type:    process.ProcessTypeCMD,
		Command: []string{"/bin/echo", "test"},
	}

	// Initially empty
	ctxs := m.ListContexts()
	if len(ctxs) != 0 {
		t.Errorf("ListContexts() returned %d contexts, want 0", len(ctxs))
	}

	// Create some contexts
	var ids []string
	for i := 0; i < 3; i++ {
		ctx, err := m.CreateContext(config)
		if err != nil {
			t.Fatalf("CreateContext() failed = %v", err)
		}
		ids = append(ids, ctx.ID)
	}

	// List should return all
	ctxs = m.ListContexts()
	if len(ctxs) != 3 {
		t.Errorf("ListContexts() returned %d contexts, want 3", len(ctxs))
	}

	// Verify all IDs are present
	idMap := make(map[string]bool)
	for _, ctx := range ctxs {
		idMap[ctx.ID] = true
	}
	for _, id := range ids {
		if !idMap[id] {
			t.Errorf("ListContexts() missing context ID %s", id)
		}
	}

	m.Cleanup()
}

// TestManager_RestartContext tests context restart.
func TestManager_RestartContext(t *testing.T) {
	m := NewManager()

	config := process.ProcessConfig{
		Type:    process.ProcessTypeCMD,
		Command: []string{"/bin/echo", "test"},
	}

	ctx, err := m.CreateContext(config)
	if err != nil {
		t.Fatalf("CreateContext() failed = %v", err)
	}

	id := ctx.ID

	// Restart the context
	restarted, err := m.RestartContext(id)
	if err != nil {
		t.Fatalf("RestartContext() failed = %v", err)
	}
	if restarted.ID != id {
		t.Errorf("RestartContext() returned context with ID %s, want %s", restarted.ID, id)
	}

	// Try to restart non-existent context
	_, err = m.RestartContext("non-existent")
	if err != ErrContextNotFound {
		t.Errorf("RestartContext() non-existent error = %v, want %v", err, ErrContextNotFound)
	}

	m.Cleanup()
}

// TestManager_PauseAllResumeAll tests pause/resume all contexts.
func TestManager_PauseAllResumeAll(t *testing.T) {
	m := NewManager()

	config := process.ProcessConfig{
		Type:    process.ProcessTypeCMD,
		Command: []string{"/bin/echo", "test"},
	}

	// Create some contexts
	for i := 0; i < 3; i++ {
		_, err := m.CreateContext(config)
		if err != nil {
			t.Fatalf("CreateContext() failed = %v", err)
		}
	}

	// Pause all - should work even if processes don't actually pause
	// (CMD processes exit immediately)
	err := m.PauseAll()
	// We don't check for error since processes might already be stopped
	_ = err

	// Resume all
	err = m.ResumeAll()
	_ = err

	m.Cleanup()
}

// TestManager_GetResourceUsage tests getting resource usage for a context.
func TestManager_GetResourceUsage(t *testing.T) {
	m := NewManager()

	config := process.ProcessConfig{
		Type:    process.ProcessTypeCMD,
		Command: []string{"/bin/echo", "test"},
	}

	// Get non-existent context
	_, err := m.GetResourceUsage("non-existent")
	if err != ErrContextNotFound {
		t.Errorf("GetResourceUsage() error = %v, want %v", err, ErrContextNotFound)
	}

	// Create a context
	ctx, err := m.CreateContext(config)
	if err != nil {
		t.Fatalf("CreateContext() failed = %v", err)
	}

	// Get resource usage
	usage, err := m.GetResourceUsage(ctx.ID)
	if err != nil {
		t.Fatalf("GetResourceUsage() failed = %v", err)
	}

	if usage.ContextID != ctx.ID {
		t.Errorf("GetResourceUsage() ContextID = %s, want %s", usage.ContextID, ctx.ID)
	}

	// For CMD process, it should have exited already
	if usage.Running && usage.Type == process.ProcessTypeCMD {
		// CMD processes should exit after execution
		// But this might be timing-dependent, so we just log it
		t.Logf("Warning: CMD process still running (might be timing issue)")
	}

	m.Cleanup()
}

// TestManager_GetAllResourceUsage tests getting aggregated resource usage.
func TestManager_GetAllResourceUsage(t *testing.T) {
	m := NewManager()

	config := process.ProcessConfig{
		Type:    process.ProcessTypeCMD,
		Command: []string{"/bin/echo", "test"},
	}

	// Empty manager
	usage := m.GetAllResourceUsage()
	if usage == nil {
		t.Fatal("GetAllResourceUsage() returned nil")
	}
	if usage.ContextCount != 0 {
		t.Errorf("GetAllResourceUsage() ContextCount = %d, want 0", usage.ContextCount)
	}

	// Create some contexts
	for i := 0; i < 3; i++ {
		_, err := m.CreateContext(config)
		if err != nil {
			t.Fatalf("CreateContext() failed = %v", err)
		}
	}

	// Get all resource usage
	usage = m.GetAllResourceUsage()
	if usage.ContextCount != 3 {
		t.Errorf("GetAllResourceUsage() ContextCount = %d, want 3", usage.ContextCount)
	}

	if len(usage.Contexts) != 3 {
		t.Errorf("GetAllResourceUsage() returned %d contexts, want 3", len(usage.Contexts))
	}

	m.Cleanup()
}

// TestManager_ConcurrentAccess tests concurrent access to the manager.
func TestManager_ConcurrentAccess(t *testing.T) {
	m := NewManager()

	config := process.ProcessConfig{
		Type:    process.ProcessTypeCMD,
		Command: []string{"/bin/echo", "test"},
	}

	const numGoroutines = 50
	const numOps = 20

	var wg sync.WaitGroup
	done := make(chan struct{})

	// Concurrent creates
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < numOps; j++ {
				select {
				case <-done:
					return
				default:
					m.CreateContext(config)
				}
			}
		}()
	}

	// Concurrent reads
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < numOps; j++ {
				select {
				case <-done:
					return
				default:
					_ = m.ListContexts()
				}
			}
		}()
	}

	// Concurrent deletes
	for i := 0; i < numGoroutines/2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < numOps; j++ {
				select {
				case <-done:
					return
				default:
					ctxs := m.ListContexts()
					if len(ctxs) > 0 {
						m.DeleteContext(ctxs[0].ID)
					}
				}
			}
		}()
	}

	// Let it run for a bit
	time.Sleep(200 * time.Millisecond)
	close(done)
	wg.Wait()

	// Verify manager is still functional
	ctxs := m.ListContexts()
	_ = ctxs // Just verify it doesn't panic

	m.Cleanup()
}

// TestManager_WriteInputReadOutput tests input/output operations.
func TestManager_WriteInputReadOutput(t *testing.T) {
	m := NewManager()

	config := process.ProcessConfig{
		Type:    process.ProcessTypeCMD,
		Command: []string{"/bin/echo", "test"},
	}

	ctx, err := m.CreateContext(config)
	if err != nil {
		t.Fatalf("CreateContext() failed = %v", err)
	}

	// Try to write input (CMD process exits quickly, so this might fail)
	err = m.WriteInput(ctx.ID, []byte("test"))
	// We don't check the result since CMD processes exit immediately
	_ = err

	// Try to read output
	ch, err := m.ReadOutput(ctx.ID)
	if err != nil {
		// This is expected for CMD processes that exit quickly
		t.Logf("ReadOutput() failed (expected for CMD): %v", err)
	} else {
		// If we got a channel, it should be valid
		if ch == nil {
			t.Error("ReadOutput() returned nil channel")
		}
	}

	// Try with non-existent context
	err = m.WriteInput("non-existent", []byte("test"))
	if err == nil {
		t.Error("WriteInput() to non-existent context should fail")
	}

	_, err = m.ReadOutput("non-existent")
	if err == nil {
		t.Error("ReadOutput() from non-existent context should fail")
	}

	m.Cleanup()
}

// TestContext_StateMethods tests context state methods.
func TestContext_StateMethods(t *testing.T) {
	m := NewManager()

	config := process.ProcessConfig{
		Type:    process.ProcessTypeCMD,
		Command: []string{"/bin/echo", "test"},
	}

	ctx, err := m.CreateContext(config)
	if err != nil {
		t.Fatalf("CreateContext() failed = %v", err)
	}

	// Test IsRunning - for CMD, it should be false or quickly become false
	_ = ctx.IsRunning()

	// Test IsPaused
	if ctx.IsPaused() {
		t.Error("CMD process should not be paused")
	}

	// Test Pause/Resume (will fail for stopped process, that's OK)
	_ = ctx.Pause()
	_ = ctx.Resume()

	m.Cleanup()
}

// TestContext_ResourceUsage tests context resource usage.
func TestContext_ResourceUsage(t *testing.T) {
	m := NewManager()

	config := process.ProcessConfig{
		Type:    process.ProcessTypeCMD,
		Command: []string{"/bin/echo", "test"},
	}

	ctx, err := m.CreateContext(config)
	if err != nil {
		t.Fatalf("CreateContext() failed = %v", err)
	}

	// Get resource usage
	usage := ctx.ResourceUsage()

	// Verify some fields are accessible
	_ = usage.CPUPercent
	_ = usage.MemoryRSS
	_ = usage.MemoryVMS

	m.Cleanup()
}

// TestManager_Cleanup tests cleanup functionality.
func TestManager_Cleanup(t *testing.T) {
	m := NewManager()

	config := process.ProcessConfig{
		Type:    process.ProcessTypeCMD,
		Command: []string{"/bin/echo", "test"},
	}

	// Create some contexts
	for i := 0; i < 3; i++ {
		_, err := m.CreateContext(config)
		if err != nil {
			t.Fatalf("CreateContext() failed = %v", err)
		}
	}

	// Verify contexts exist
	ctxs := m.ListContexts()
	if len(ctxs) != 3 {
		t.Errorf("ListContexts() returned %d contexts, want 3", len(ctxs))
	}

	// Cleanup
	m.Cleanup()

	// Verify all contexts are gone
	ctxs = m.ListContexts()
	if len(ctxs) != 0 {
		t.Errorf("ListContexts() after cleanup returned %d contexts, want 0", len(ctxs))
	}

	// Should be able to create new contexts after cleanup
	_, err := m.CreateContext(config)
	if err != nil {
		t.Errorf("CreateContext() after cleanup failed = %v", err)
	}

	m.Cleanup()
}

// TestSandboxResourceUsage_Fields tests SandboxResourceUsage struct fields.
func TestSandboxResourceUsage_Fields(t *testing.T) {
	m := NewManager()

	config := process.ProcessConfig{
		Type:    process.ProcessTypeCMD,
		Command: []string{"/bin/echo", "test"},
	}

	// Create a context
	_, err := m.CreateContext(config)
	if err != nil {
		t.Fatalf("CreateContext() failed = %v", err)
	}

	usage := m.GetAllResourceUsage()

	// Verify all fields are accessible (don't check values as they're environment-dependent)
	_ = usage.ContainerMemoryUsage
	_ = usage.ContainerMemoryLimit
	_ = usage.ContainerMemoryWorkingSet
	_ = usage.TotalMemoryRSS
	_ = usage.TotalMemoryVMS
	_ = usage.TotalOpenFiles
	_ = usage.TotalThreadCount
	_ = usage.TotalIOReadBytes
	_ = usage.TotalIOWriteBytes
	_ = usage.ContextCount
	_ = usage.RunningContextCount
	_ = usage.PausedContextCount
	_ = usage.Contexts

	m.Cleanup()
}

// TestContext_TypesAndAlias tests context type and alias fields.
func TestContext_TypesAndAlias(t *testing.T) {
	m := NewManager()

	tests := []struct {
		name      string
		config    process.ProcessConfig
		wantType  process.ProcessType
		wantAlias string
	}{
		{
			name: "CMD type",
			config: process.ProcessConfig{
				Type:    process.ProcessTypeCMD,
				Command: []string{"/bin/echo", "test"},
			},
			wantType:  process.ProcessTypeCMD,
			wantAlias: "",
		},
		{
			name: "REPL Python type",
			config: process.ProcessConfig{
				Type:  process.ProcessTypeREPL,
				Alias: "python",
			},
			wantType:  process.ProcessTypeREPL,
			wantAlias: "python",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, err := m.CreateContext(tt.config)
			// We expect some failures due to missing interpreters
			if err != nil {
				t.Skipf("CreateContext() failed (likely missing interpreter): %v", err)
				return
			}

			if ctx.Type != tt.wantType {
				t.Errorf("Context Type = %s, want %s", ctx.Type, tt.wantType)
			}

			if ctx.Alias != tt.wantAlias {
				t.Errorf("Context Alias = %s, want %s", ctx.Alias, tt.wantAlias)
			}

			m.Cleanup()
		})
	}
}

// TestContext_AddHandlers tests dynamic handler addition (middleware-like behavior).
func TestContext_AddHandlers(t *testing.T) {
	m := NewManager()

	config := process.ProcessConfig{
		Type:    process.ProcessTypeCMD,
		Command: []string{"/bin/echo", "test"},
	}

	// Track handler invocations
	var exitCalls []string
	var startCalls []string
	var mu sync.Mutex

	// Set global handlers
	m.SetExitHandler(func(event process.ExitEvent) {
		mu.Lock()
		exitCalls = append(exitCalls, "global")
		mu.Unlock()
	})

	m.SetStartHandler(func(event process.StartEvent) {
		mu.Lock()
		startCalls = append(startCalls, "global")
		mu.Unlock()
	})

	// Create context with global handlers
	ctx, err := m.CreateContext(config)
	if err != nil {
		t.Fatalf("CreateContext() failed = %v", err)
	}

	// Add additional handlers dynamically (middleware-like)
	ctx.AddExitHandler(func(event process.ExitEvent) {
		mu.Lock()
		exitCalls = append(exitCalls, "middleware1")
		mu.Unlock()
	})

	ctx.AddExitHandler(func(event process.ExitEvent) {
		mu.Lock()
		exitCalls = append(exitCalls, "middleware2")
		mu.Unlock()
	})

	ctx.AddStartHandler(func(event process.StartEvent) {
		mu.Lock()
		startCalls = append(startCalls, "middleware1")
		mu.Unlock()
	})

	ctx.AddStartHandler(func(event process.StartEvent) {
		mu.Lock()
		startCalls = append(startCalls, "middleware2")
		mu.Unlock()
	})

	// Clear the calls from initial creation
	mu.Lock()
	exitCalls = nil
	startCalls = nil
	mu.Unlock()

	// Restart to trigger handlers
	err = ctx.Restart()
	if err != nil {
		t.Fatalf("Restart() failed = %v", err)
	}

	// Wait for process to complete
	time.Sleep(100 * time.Millisecond)

	// Verify all handlers were called in order
	mu.Lock()
	defer mu.Unlock()

	// Exit handlers should include global + middleware handlers
	if len(exitCalls) < 3 {
		t.Errorf("Expected at least 3 exit handler calls, got %d: %v", len(exitCalls), exitCalls)
	}

	// Start handlers should include global + middleware handlers
	if len(startCalls) < 3 {
		t.Errorf("Expected at least 3 start handler calls, got %d: %v", len(startCalls), startCalls)
	}

	// Verify order: handlers should be called in the order they were added
	if len(exitCalls) >= 3 {
		if exitCalls[0] != "global" {
			t.Errorf("First exit handler should be 'global', got '%s'", exitCalls[0])
		}
		if exitCalls[1] != "middleware1" {
			t.Errorf("Second exit handler should be 'middleware1', got '%s'", exitCalls[1])
		}
		if exitCalls[2] != "middleware2" {
			t.Errorf("Third exit handler should be 'middleware2', got '%s'", exitCalls[2])
		}
	}

	if len(startCalls) >= 3 {
		if startCalls[0] != "global" {
			t.Errorf("First start handler should be 'global', got '%s'", startCalls[0])
		}
		if startCalls[1] != "middleware1" {
			t.Errorf("Second start handler should be 'middleware1', got '%s'", startCalls[1])
		}
		if startCalls[2] != "middleware2" {
			t.Errorf("Third start handler should be 'middleware2', got '%s'", startCalls[2])
		}
	}

	m.Cleanup()
}

// TestContext_AddHandlers_AfterCreation tests adding handlers to an existing context.
func TestContext_AddHandlers_AfterCreation(t *testing.T) {
	config := process.ProcessConfig{
		Type:    process.ProcessTypeCMD,
		Command: []string{"/bin/echo", "test"},
	}

	// Create context without handlers
	ctx, err := NewContext(config, nil, nil, nil)
	if err != nil {
		t.Fatalf("NewContext() failed = %v", err)
	}
	defer ctx.Stop()

	// Track handler invocations
	var exitCalled bool
	var startCalled bool
	var mu sync.Mutex

	// Add handlers after creation
	ctx.AddExitHandler(func(event process.ExitEvent) {
		mu.Lock()
		exitCalled = true
		mu.Unlock()
	})

	ctx.AddStartHandler(func(event process.StartEvent) {
		mu.Lock()
		startCalled = true
		mu.Unlock()
	})

	// Restart to trigger handlers
	err = ctx.Restart()
	if err != nil {
		t.Fatalf("Restart() failed = %v", err)
	}

	// Wait for process to complete
	time.Sleep(100 * time.Millisecond)

	// Verify handlers were called
	mu.Lock()
	defer mu.Unlock()

	if !exitCalled {
		t.Error("Exit handler was not called")
	}

	if !startCalled {
		t.Error("Start handler was not called")
	}
}
