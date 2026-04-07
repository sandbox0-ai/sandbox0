package handlers

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"syscall"
	"testing"
	"time"
	"unsafe"

	"github.com/gorilla/mux"
	"github.com/gorilla/websocket"
	ctxpkg "github.com/sandbox0-ai/sandbox0/manager/procd/pkg/context"
	"github.com/sandbox0-ai/sandbox0/manager/procd/pkg/process"
	"go.uber.org/zap"
)

type fakeProcess struct {
	outputCh     chan process.ProcessOutput
	onWrite      func([]byte)
	finished     bool
	exitCode     int
	state        process.ProcessState
	processType  process.ProcessType
	exitHandlers []process.ExitHandler
}

func (f *fakeProcess) ID() string { return "proc-test" }
func (f *fakeProcess) Type() process.ProcessType {
	if f.processType == "" {
		return process.ProcessTypeREPL
	}
	return f.processType
}
func (f *fakeProcess) PID() int         { return 1 }
func (f *fakeProcess) Start() error     { return nil }
func (f *fakeProcess) Stop() error      { return nil }
func (f *fakeProcess) Restart() error   { return nil }
func (f *fakeProcess) IsRunning() bool  { return true }
func (f *fakeProcess) IsFinished() bool { return f.finished }
func (f *fakeProcess) State() process.ProcessState {
	if f.state == "" {
		return process.ProcessStateRunning
	}
	return f.state
}
func (f *fakeProcess) AddStartHandler(process.StartHandler) {}
func (f *fakeProcess) AddExitHandler(handler process.ExitHandler) {
	f.exitHandlers = append(f.exitHandlers, handler)
}
func (f *fakeProcess) Pause() error   { return nil }
func (f *fakeProcess) Resume() error  { return nil }
func (f *fakeProcess) IsPaused() bool { return false }
func (f *fakeProcess) WriteInput(data []byte) error {
	if f.onWrite != nil {
		f.onWrite(data)
	}
	return nil
}
func (f *fakeProcess) ReadOutput() <-chan process.ProcessOutput { return f.outputCh }
func (f *fakeProcess) ResizePTY(process.PTYSize) error          { return nil }
func (f *fakeProcess) SendSignal(syscall.Signal) error          { return nil }
func (f *fakeProcess) ExitCode() (int, error)                   { return f.exitCode, nil }
func (f *fakeProcess) ResourceUsage() process.ResourceUsage     { return process.ResourceUsage{} }

func (f *fakeProcess) triggerExit(event process.ExitEvent) {
	f.finished = true
	f.exitCode = event.ExitCode
	f.state = event.State
	for _, handler := range f.exitHandlers {
		if handler != nil {
			handler(event)
		}
	}
}

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
	if output != "_S0_> hello world" {
		t.Errorf("output = %q, want %q when prompt arrives first", output, "_S0_> hello world")
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
	if output != "_S0_> hello world" {
		t.Errorf("output = %q, want %q", output, "_S0_> hello world")
	}
}

func TestExecInputSync_REPLPromptFormatting_FirstAndMultipleRuns(t *testing.T) {
	outputCh := make(chan process.ProcessOutput, 8)
	writeCount := 0
	proc := &fakeProcess{
		outputCh: outputCh,
		onWrite: func([]byte) {
			writeCount++
			switch writeCount {
			case 1:
				outputCh <- process.ProcessOutput{
					Source: process.OutputSourcePTY,
					Data:   []byte("print('hello')\nhello\n_S0_> "),
				}
				outputCh <- process.ProcessOutput{Source: process.OutputSourcePrompt}
			case 2:
				outputCh <- process.ProcessOutput{
					Source: process.OutputSourcePTY,
					Data:   []byte("print('world')\nworld\n_S0_> "),
				}
				outputCh <- process.ProcessOutput{Source: process.OutputSourcePrompt}
			default:
				t.Fatalf("unexpected write count: %d", writeCount)
			}
		},
	}

	handler, ctx := newHandlerWithContext(proc, process.ProcessTypeREPL)

	firstOutput, execErr, timedOut := handler.execInputSync(ctx, "print('hello')", context.Background())
	if timedOut {
		t.Fatal("first execInputSync() timed out")
	}
	if execErr != nil {
		t.Fatalf("first execInputSync() error = %v", execErr)
	}
	wantFirst := "_S0_> print('hello')\nhello\n"
	if firstOutput != wantFirst {
		t.Fatalf("first output = %q, want %q", firstOutput, wantFirst)
	}

	secondOutput, execErr, timedOut := handler.execInputSync(ctx, "print('world')", context.Background())
	if timedOut {
		t.Fatal("second execInputSync() timed out")
	}
	if execErr != nil {
		t.Fatalf("second execInputSync() error = %v", execErr)
	}
	wantSecond := "_S0_> print('world')\nworld\n"
	if secondOutput != wantSecond {
		t.Fatalf("second output = %q, want %q", secondOutput, wantSecond)
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

func TestNormalizeStringMap_ReturnsEmptyMapForNil(t *testing.T) {
	got := normalizeStringMap(nil)
	if got == nil {
		t.Fatal("normalizeStringMap(nil) returned nil")
	}
	if len(got) != 0 {
		t.Fatalf("normalizeStringMap(nil) length = %d, want 0", len(got))
	}
}

func TestGetContext_EncodesNilEnvVarsAsEmptyObject(t *testing.T) {
	proc := &fakeProcess{outputCh: make(chan process.ProcessOutput)}
	handler, ctx := newHandlerWithContext(proc, process.ProcessTypeREPL)
	ctx.EnvVars = nil

	req := httptest.NewRequest(http.MethodGet, "/contexts/"+ctx.ID, nil)
	req = mux.SetURLVars(req, map[string]string{"id": ctx.ID})
	rec := httptest.NewRecorder()

	handler.Get(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}

	var resp struct {
		Success bool `json:"success"`
		Data    struct {
			EnvVars map[string]string `json:"env_vars"`
		} `json:"data"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}
	if !resp.Success {
		t.Fatalf("success = false, body = %s", rec.Body.String())
	}
	if resp.Data.EnvVars == nil {
		t.Fatal("env_vars decoded as nil, want empty object")
	}
	if len(resp.Data.EnvVars) != 0 {
		t.Fatalf("env_vars length = %d, want 0", len(resp.Data.EnvVars))
	}
	if strings.Contains(rec.Body.String(), "\"env_vars\":null") {
		t.Fatalf("response body contains null env_vars: %s", rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "\"env_vars\":{}") {
		t.Fatalf("response body does not contain empty object env_vars: %s", rec.Body.String())
	}
}

func TestNewWSOutputMessage(t *testing.T) {
	msg := newWSOutputMessage(process.OutputSourcePTY, "hello")
	if msg.Type != "output" {
		t.Fatalf("type = %q, want %q", msg.Type, "output")
	}
	if msg.Source != "pty" {
		t.Fatalf("source = %q, want %q", msg.Source, "pty")
	}
	if msg.Data != "hello" {
		t.Fatalf("data = %q, want %q", msg.Data, "hello")
	}
}

func TestNewWSDoneMessage(t *testing.T) {
	msg := newWSRequestDoneMessage("req-1")
	if msg.Type != "done" {
		t.Fatalf("type = %q, want %q", msg.Type, "done")
	}
	if msg.RequestID != "req-1" {
		t.Fatalf("request_id = %q, want %q", msg.RequestID, "req-1")
	}
	if msg.ExitCode != nil {
		t.Fatal("exit_code should be nil for request done")
	}
	if msg.State != "" {
		t.Fatal("state should be empty for request done")
	}
}

func TestNewWSProcessDoneMessage(t *testing.T) {
	msg := newWSProcessDoneMessage(0, process.ProcessStateStopped)
	if msg.Type != "done" {
		t.Fatalf("type = %q, want %q", msg.Type, "done")
	}
	if msg.RequestID != "" {
		t.Fatalf("request_id = %q, want empty", msg.RequestID)
	}
	if msg.ExitCode == nil || *msg.ExitCode != 0 {
		t.Fatalf("exit_code = %#v, want 0", msg.ExitCode)
	}
	if msg.State != string(process.ProcessStateStopped) {
		t.Fatalf("state = %q, want %q", msg.State, process.ProcessStateStopped)
	}
}

func TestWebSocketSendsProcessDoneMessage(t *testing.T) {
	outputCh := make(chan process.ProcessOutput, 2)
	proc := &fakeProcess{
		outputCh:    outputCh,
		processType: process.ProcessTypeCMD,
	}
	handler, ctx := newHandlerWithContext(proc, process.ProcessTypeCMD)

	router := mux.NewRouter()
	router.HandleFunc("/contexts/{id}/ws", handler.WebSocket)
	server := httptest.NewServer(router)
	defer server.Close()

	wsURL, err := url.Parse(server.URL)
	if err != nil {
		t.Fatalf("url.Parse() error = %v", err)
	}
	wsURL.Scheme = "ws"
	wsURL.Path = "/contexts/" + ctx.ID + "/ws"

	conn, _, err := websocket.DefaultDialer.Dial(wsURL.String(), nil)
	if err != nil {
		t.Fatalf("Dial() error = %v", err)
	}
	defer conn.Close()

	outputCh <- process.ProcessOutput{Source: process.OutputSourceStdout, Data: []byte("done\n")}
	proc.triggerExit(process.ExitEvent{ExitCode: 7, State: process.ProcessStateCrashed})
	close(outputCh)

	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	var messages []map[string]any
	for {
		var msg map[string]any
		if err := conn.ReadJSON(&msg); err != nil {
			if websocket.IsCloseError(err, websocket.CloseNormalClosure) {
				break
			}
			t.Fatalf("ReadJSON() error = %v", err)
		}
		messages = append(messages, msg)
	}

	if len(messages) != 2 {
		t.Fatalf("messages length = %d, want 2", len(messages))
	}
	if got := messages[0]["type"]; got != "output" {
		t.Fatalf("first message type = %v, want output", got)
	}
	if got := messages[1]["type"]; got != "done" {
		t.Fatalf("second message type = %v, want done", got)
	}
	if got := messages[1]["exit_code"]; got != float64(7) {
		t.Fatalf("exit_code = %v, want 7", got)
	}
	if got := messages[1]["state"]; got != string(process.ProcessStateCrashed) {
		t.Fatalf("state = %v, want %q", got, process.ProcessStateCrashed)
	}
}
