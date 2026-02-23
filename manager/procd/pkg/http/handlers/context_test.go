package handlers

import (
	"context"
	"reflect"
	"syscall"
	"testing"
	"unsafe"

	ctxpkg "github.com/sandbox0-ai/infra/manager/procd/pkg/context"
	"github.com/sandbox0-ai/infra/manager/procd/pkg/process"
	"go.uber.org/zap"
)

type fakeProcess struct {
	outputCh chan process.ProcessOutput
	onWrite  func([]byte)
	finished bool
}

func (f *fakeProcess) ID() string                           { return "proc-test" }
func (f *fakeProcess) Type() process.ProcessType            { return process.ProcessTypeREPL }
func (f *fakeProcess) PID() int                             { return 1 }
func (f *fakeProcess) Start() error                         { return nil }
func (f *fakeProcess) Stop() error                          { return nil }
func (f *fakeProcess) Restart() error                       { return nil }
func (f *fakeProcess) IsRunning() bool                      { return true }
func (f *fakeProcess) IsFinished() bool                     { return f.finished }
func (f *fakeProcess) State() process.ProcessState          { return process.ProcessStateRunning }
func (f *fakeProcess) AddStartHandler(process.StartHandler) {}
func (f *fakeProcess) AddExitHandler(process.ExitHandler)   {}
func (f *fakeProcess) Pause() error                         { return nil }
func (f *fakeProcess) Resume() error                        { return nil }
func (f *fakeProcess) IsPaused() bool                       { return false }
func (f *fakeProcess) WriteInput(data []byte) error {
	if f.onWrite != nil {
		f.onWrite(data)
	}
	return nil
}
func (f *fakeProcess) ReadOutput() <-chan process.ProcessOutput { return f.outputCh }
func (f *fakeProcess) ResizePTY(process.PTYSize) error          { return nil }
func (f *fakeProcess) SendSignal(syscall.Signal) error          { return nil }
func (f *fakeProcess) ExitCode() (int, error)                   { return 0, nil }
func (f *fakeProcess) ResourceUsage() process.ResourceUsage     { return process.ResourceUsage{} }

func attachContext(manager *ctxpkg.Manager, ctx *ctxpkg.Context) {
	managerValue := reflect.ValueOf(manager).Elem()
	contextsField := managerValue.FieldByName("contexts")
	contexts := reflect.NewAt(contextsField.Type(), unsafe.Pointer(contextsField.UnsafeAddr())).Elem()
	if contexts.IsNil() {
		contexts.Set(reflect.MakeMap(contexts.Type()))
	}
	contexts.SetMapIndex(reflect.ValueOf(ctx.ID), reflect.ValueOf(ctx))
}

func newHandlerWithContext(proc process.Process, processType process.ProcessType) (*ContextHandler, *ctxpkg.Context) {
	manager := ctxpkg.NewManager()
	ctx := &ctxpkg.Context{
		ID:          "ctx-test",
		Type:        processType,
		Alias:       "python",
		MainProcess: proc,
	}
	attachContext(manager, ctx)
	return NewContextHandler(manager, zap.NewNop()), ctx
}

func TestExecInputSync_PromptBeforeOutputReturnsEmpty(t *testing.T) {
	outputCh := make(chan process.ProcessOutput, 2)
	proc := &fakeProcess{
		outputCh: outputCh,
		onWrite: func([]byte) {
			outputCh <- process.ProcessOutput{Source: process.OutputSourcePrompt}
			outputCh <- process.ProcessOutput{Source: process.OutputSourcePTY, Data: []byte("hello world")}
			close(outputCh)
		},
	}

	handler, ctx := newHandlerWithContext(proc, process.ProcessTypeREPL)
	output, execErr, timedOut := handler.execInputSync(ctx, "print('hello world')\n", context.Background())

	if timedOut {
		t.Fatal("execInputSync() timed out")
	}
	if execErr != nil {
		t.Fatalf("execInputSync() error = %v", execErr)
	}
	if output != "hello world" {
		t.Errorf("output = %q, want %q when prompt arrives first", output, "hello world")
	}
}

func TestExecInputSync_OutputBeforePromptReturnsOutput(t *testing.T) {
	outputCh := make(chan process.ProcessOutput, 2)
	proc := &fakeProcess{
		outputCh: outputCh,
		onWrite: func([]byte) {
			outputCh <- process.ProcessOutput{Source: process.OutputSourcePTY, Data: []byte("hello world")}
			outputCh <- process.ProcessOutput{Source: process.OutputSourcePrompt}
			close(outputCh)
		},
	}

	handler, ctx := newHandlerWithContext(proc, process.ProcessTypeREPL)
	output, execErr, timedOut := handler.execInputSync(ctx, "print('hello world')\n", context.Background())

	if timedOut {
		t.Fatal("execInputSync() timed out")
	}
	if execErr != nil {
		t.Fatalf("execInputSync() error = %v", execErr)
	}
	if output != "hello world" {
		t.Errorf("output = %q, want %q", output, "hello world")
	}
}

func TestExecInputSync_AppendsNewlineForREPL(t *testing.T) {
	outputCh := make(chan process.ProcessOutput, 1)
	var captured string
	proc := &fakeProcess{
		outputCh: outputCh,
		onWrite: func(data []byte) {
			captured = string(data)
			outputCh <- process.ProcessOutput{Source: process.OutputSourcePrompt}
			close(outputCh)
		},
	}

	handler, ctx := newHandlerWithContext(proc, process.ProcessTypeREPL)
	_, execErr, timedOut := handler.execInputSync(ctx, "print('hello world')", context.Background())

	if timedOut {
		t.Fatal("execInputSync() timed out")
	}
	if execErr != nil {
		t.Fatalf("execInputSync() error = %v", execErr)
	}
	if captured != "print('hello world')\n" {
		t.Errorf("input = %q, want %q", captured, "print('hello world')\n")
	}
}

func TestExecInputSync_DoesNotAppendNewlineForCMD(t *testing.T) {
	outputCh := make(chan process.ProcessOutput, 1)
	var captured string
	proc := &fakeProcess{
		outputCh: outputCh,
		onWrite: func(data []byte) {
			captured = string(data)
			outputCh <- process.ProcessOutput{Source: process.OutputSourcePrompt}
			close(outputCh)
		},
	}

	handler, ctx := newHandlerWithContext(proc, process.ProcessTypeCMD)
	_, execErr, timedOut := handler.execInputSync(ctx, "echo hello", context.Background())

	if timedOut {
		t.Fatal("execInputSync() timed out")
	}
	if execErr != nil {
		t.Fatalf("execInputSync() error = %v", execErr)
	}
	if captured != "echo hello" {
		t.Errorf("input = %q, want %q", captured, "echo hello")
	}
}
