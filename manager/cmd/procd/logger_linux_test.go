//go:build linux

package main

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	coreobs "github.com/sandbox0-ai/sandbox0/pkg/observability/core"
)

func TestProcdLoggerTimezoneHelper(t *testing.T) {
	mode := os.Getenv("S0_PROCD_LOGGER_TEST")
	if mode == "" {
		return
	}
	logger, err := coreobs.NewLogger(coreobs.LoggerConfig{ServiceName: "timezone-control"})
	if err != nil {
		t.Fatal(err)
	}
	if mode == "procd" {
		logger, err = newProcdLogger("info")
		if err != nil {
			t.Fatal(err)
		}
	}
	originalTZ := os.Getenv("TZ")
	originalLocation := time.Local
	if _, err := fmt.Fprintln(os.Stdout, "ready"); err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	logger.Info("first record")
	fmt.Fprintf(os.Stdout, "finished %d\n", time.Since(started).Nanoseconds())
	if os.Getenv("TZ") != originalTZ || time.Local != originalLocation {
		t.Fatal("logger changed the process timezone")
	}
}

func TestProcdLoggerAvoidsTimezoneIO(t *testing.T) {
	for _, mode := range []string{"default", "procd"} {
		t.Run(mode, func(t *testing.T) {
			fifo := filepath.Join(t.TempDir(), "timezone")
			if err := syscall.Mkfifo(fifo, 0600); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			command := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestProcdLoggerTimezoneHelper$")
			for _, entry := range os.Environ() {
				if !strings.HasPrefix(entry, "TZ=") && !strings.HasPrefix(entry, "S0_PROCD_LOGGER_TEST=") {
					command.Env = append(command.Env, entry)
				}
			}
			command.Env = append(command.Env, "TZ="+fifo, "S0_PROCD_LOGGER_TEST="+mode)
			stdout, err := command.StdoutPipe()
			if err != nil {
				t.Fatal(err)
			}
			var stderr bytes.Buffer
			command.Stderr = &stderr
			if err := command.Start(); err != nil {
				t.Fatal(err)
			}
			reader := bufio.NewReader(stdout)
			first, err := reader.ReadString('\n')
			if err != nil || first != "ready\n" {
				cancel()
				_ = command.Wait()
				t.Fatalf("child did not reach first record: %q %v", first, err)
			}
			type result struct {
				output []byte
				err    error
			}
			done := make(chan result, 1)
			go func() {
				output, readErr := io.ReadAll(reader)
				waitErr := command.Wait()
				if readErr != nil {
					waitErr = readErr
				}
				done <- result{output, waitErr}
			}()
			if mode == "default" {
				select {
				case result := <-done:
					t.Fatalf("default logger did not wait for timezone file: %s %v", result.output, result.err)
				case <-time.After(100 * time.Millisecond):
				}
				// A nonblocking writer succeeds only when the child has a
				// reader waiting on this exact FIFO. Closing releases EOF.
				descriptor, err := syscall.Open(fifo, syscall.O_WRONLY|syscall.O_NONBLOCK, 0)
				if err != nil {
					cancel()
					<-done
					t.Fatalf("timezone read was not observed: %v", err)
				}
				if err := syscall.Close(descriptor); err != nil {
					cancel()
					<-done
					t.Fatal(err)
				}
			}
			select {
			case result := <-done:
				if result.err != nil {
					t.Fatalf("child failed: %v %s", result.err, stderr.String())
				}
				if !bytes.Contains(result.output, []byte(`"msg":"first record"`)) || !bytes.Contains(result.output, []byte("finished ")) {
					t.Fatalf("missing first record: %s", result.output)
				}
				if mode == "procd" && !bytes.Contains(result.output, []byte(`Z"`)) {
					t.Fatalf("UTC timestamp absent: %s", result.output)
				}
				t.Logf("%s: first record completed; process TZ preserved", mode)
			case <-ctx.Done():
				<-done
				t.Fatalf("%s logger blocked on timezone I/O", mode)
			}
		})
	}
}
