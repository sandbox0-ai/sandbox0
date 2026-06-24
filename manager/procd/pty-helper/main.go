package main

import (
	"bufio"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/creack/pty"
)

type helperConfig struct {
	Command []string          `json:"command"`
	CWD     string            `json:"cwd,omitempty"`
	Env     map[string]string `json:"env,omitempty"`
	Rows    uint16            `json:"rows,omitempty"`
	Cols    uint16            `json:"cols,omitempty"`
}

type controlFrame struct {
	Type   string          `json:"type"`
	Data   string          `json:"data,omitempty"`
	Rows   uint16          `json:"rows,omitempty"`
	Cols   uint16          `json:"cols,omitempty"`
	Signal json.RawMessage `json:"signal,omitempty"`
}

type outputFrame struct {
	Type     string `json:"type"`
	PID      int    `json:"pid,omitempty"`
	Data     string `json:"data,omitempty"`
	ExitCode *int   `json:"exit_code,omitempty"`
	Signal   string `json:"signal,omitempty"`
	Message  string `json:"message,omitempty"`
}

func main() {
	cfg, err := readConfig()
	if err != nil {
		writeFrame(outputFrame{Type: "error", Message: err.Error()})
		os.Exit(1)
	}
	if len(cfg.Command) == 0 || strings.TrimSpace(cfg.Command[0]) == "" {
		writeFrame(outputFrame{Type: "error", Message: "command is required"})
		os.Exit(1)
	}

	cmd := exec.Command(cfg.Command[0], cfg.Command[1:]...)
	if cfg.CWD != "" {
		cmd.Dir = cfg.CWD
	}
	cmd.Env = mergeEnv(os.Environ(), cfg.Env)

	size := &pty.Winsize{Rows: cfg.Rows, Cols: cfg.Cols}
	if size.Rows == 0 {
		size.Rows = 100
	}
	if size.Cols == 0 {
		size.Cols = 500
	}

	ptmx, err := pty.StartWithSize(cmd, size)
	if err != nil {
		writeFrame(outputFrame{Type: "error", Message: err.Error()})
		os.Exit(1)
	}
	defer ptmx.Close()

	writer := newFrameWriter(os.Stdout)
	writer.write(outputFrame{Type: "start", PID: cmd.Process.Pid})

	waitCh := make(chan waitResult, 1)
	go func() {
		waitCh <- resolveExit(cmd.Wait())
	}()

	readDone := make(chan struct{})
	go func() {
		defer close(readDone)
		readPTY(ptmx, writer)
	}()
	go handleControl(os.Stdin, ptmx, cmd.Process.Pid, writer)

	result := <-waitCh
	select {
	case <-readDone:
	case <-time.After(500 * time.Millisecond):
		_ = ptmx.Close()
		<-readDone
	}
	_ = ptmx.Close()
	writer.write(outputFrame{
		Type:     "exit",
		ExitCode: &result.exitCode,
		Signal:   result.signal,
	})
}

func readConfig() (helperConfig, error) {
	raw := strings.TrimSpace(os.Getenv("SANDBOX0_PTY_CONFIG"))
	if raw == "" {
		return helperConfig{}, fmt.Errorf("SANDBOX0_PTY_CONFIG is required")
	}
	data, err := base64.StdEncoding.DecodeString(raw)
	if err != nil {
		return helperConfig{}, fmt.Errorf("decode SANDBOX0_PTY_CONFIG: %w", err)
	}
	var cfg helperConfig
	if err := json.Unmarshal(data, &cfg); err != nil {
		return helperConfig{}, fmt.Errorf("parse SANDBOX0_PTY_CONFIG: %w", err)
	}
	return cfg, nil
}

type frameWriter struct {
	mu  sync.Mutex
	enc *json.Encoder
}

func newFrameWriter(w io.Writer) *frameWriter {
	return &frameWriter{enc: json.NewEncoder(w)}
}

func (w *frameWriter) write(frame outputFrame) {
	w.mu.Lock()
	defer w.mu.Unlock()
	_ = w.enc.Encode(frame)
}

func writeFrame(frame outputFrame) {
	_ = json.NewEncoder(os.Stdout).Encode(frame)
}

func readPTY(ptmx *os.File, writer *frameWriter) {
	buf := make([]byte, 32768)
	for {
		n, err := ptmx.Read(buf)
		if n > 0 {
			writer.write(outputFrame{
				Type: "output",
				Data: base64.StdEncoding.EncodeToString(buf[:n]),
			})
		}
		if err != nil {
			return
		}
	}
}

func handleControl(r io.Reader, ptmx *os.File, pid int, writer *frameWriter) {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	for scanner.Scan() {
		var frame controlFrame
		if err := json.Unmarshal(scanner.Bytes(), &frame); err != nil {
			writer.write(outputFrame{Type: "error", Message: "invalid control frame: " + err.Error()})
			continue
		}
		switch frame.Type {
		case "input":
			data, err := base64.StdEncoding.DecodeString(frame.Data)
			if err != nil {
				writer.write(outputFrame{Type: "error", Message: "invalid input frame: " + err.Error()})
				continue
			}
			if _, err := ptmx.Write(data); err != nil {
				writer.write(outputFrame{Type: "error", Message: "write input: " + err.Error()})
			}
		case "resize":
			if frame.Rows == 0 || frame.Cols == 0 {
				writer.write(outputFrame{Type: "error", Message: "rows and cols must be > 0"})
				continue
			}
			if err := pty.Setsize(ptmx, &pty.Winsize{Rows: frame.Rows, Cols: frame.Cols}); err != nil {
				writer.write(outputFrame{Type: "error", Message: "resize pty: " + err.Error()})
			}
		case "signal":
			sig, err := parseSignal(frame.Signal)
			if err != nil {
				writer.write(outputFrame{Type: "error", Message: err.Error()})
				continue
			}
			if err := signalProcessGroup(pid, sig); err != nil {
				writer.write(outputFrame{Type: "error", Message: "send signal: " + err.Error()})
			}
		}
	}
}

func mergeEnv(base []string, overlay map[string]string) []string {
	env := make(map[string]string, len(base)+len(overlay))
	for _, item := range base {
		key, value, ok := strings.Cut(item, "=")
		if ok {
			env[key] = value
		}
	}
	for key, value := range overlay {
		if key == "" {
			continue
		}
		env[key] = value
	}
	out := make([]string, 0, len(env))
	for key, value := range env {
		out = append(out, key+"="+value)
	}
	return out
}

func signalProcessGroup(pid int, sig syscall.Signal) error {
	if pid <= 0 {
		return fmt.Errorf("process pid is not available")
	}
	if err := syscall.Kill(-pid, sig); err != nil {
		if errors.Is(err, syscall.ESRCH) {
			return nil
		}
		if err := syscall.Kill(pid, sig); err != nil && !errors.Is(err, syscall.ESRCH) {
			return err
		}
	}
	return nil
}

func parseSignal(raw json.RawMessage) (syscall.Signal, error) {
	if len(raw) == 0 || string(raw) == "null" {
		return 0, fmt.Errorf("signal is required")
	}
	var num int
	if err := json.Unmarshal(raw, &num); err == nil {
		if num > 0 && num <= 64 {
			return syscall.Signal(num), nil
		}
		return 0, fmt.Errorf("unsupported signal")
	}
	var text string
	if err := json.Unmarshal(raw, &text); err != nil {
		return 0, fmt.Errorf("unsupported signal")
	}
	trimmed := strings.TrimSpace(strings.ToUpper(strings.TrimPrefix(text, "SIG")))
	if trimmed == "" {
		return 0, fmt.Errorf("signal is required")
	}
	switch trimmed {
	case "INT":
		return syscall.SIGINT, nil
	case "TERM":
		return syscall.SIGTERM, nil
	case "KILL":
		return syscall.SIGKILL, nil
	case "HUP":
		return syscall.SIGHUP, nil
	case "QUIT":
		return syscall.SIGQUIT, nil
	case "USR1":
		return syscall.SIGUSR1, nil
	case "USR2":
		return syscall.SIGUSR2, nil
	case "WINCH":
		return syscall.SIGWINCH, nil
	case "STOP":
		return syscall.SIGSTOP, nil
	case "CONT":
		return syscall.SIGCONT, nil
	}
	n, err := strconv.Atoi(trimmed)
	if err == nil && n > 0 && n <= 64 {
		return syscall.Signal(n), nil
	}
	return 0, fmt.Errorf("unsupported signal")
}

type waitResult struct {
	exitCode int
	signal   string
}

func resolveExit(err error) waitResult {
	if err == nil {
		return waitResult{exitCode: 0}
	}
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		if status, ok := exitErr.Sys().(syscall.WaitStatus); ok {
			if status.Signaled() {
				sig := status.Signal()
				return waitResult{exitCode: 128 + int(sig), signal: sig.String()}
			}
			return waitResult{exitCode: status.ExitStatus()}
		}
		return waitResult{exitCode: exitErr.ExitCode()}
	}
	return waitResult{exitCode: 1}
}
