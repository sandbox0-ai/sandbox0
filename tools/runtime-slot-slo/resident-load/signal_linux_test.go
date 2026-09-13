package main

import (
	"encoding/json"
	"os"
	"os/exec"
	"syscall"
	"testing"
	"time"
)

// This is a small local subprocess unit test, not a guest/runtime acceptance run.
func TestSignalStopsLoadedProcess(t *testing.T) {
	for _, signal := range []os.Signal{os.Interrupt, syscall.SIGTERM} {
		t.Run(signal.String(), func(t *testing.T) {
			cmd := exec.Command(os.Args[0], "-test.run=^TestResidentLoadChild$")
			cmd.Env = append(os.Environ(), "S0_RESIDENT_LOAD_TEST_CHILD=1")
			input, err := cmd.StdinPipe()
			if err != nil {
				t.Fatal(err)
			}
			output, err := cmd.StdoutPipe()
			if err != nil {
				t.Fatal(err)
			}
			if err := cmd.Start(); err != nil {
				t.Fatal(err)
			}
			done := make(chan error, 1)
			events := make(chan event, 32)
			go func() {
				decoder := json.NewDecoder(output)
				for {
					var value event
					if err := decoder.Decode(&value); err != nil {
						break
					}
					events <- value
				}
				done <- cmd.Wait()
			}()
			t.Cleanup(func() {
				_ = input.Close()
				// Kill only this test's exact child if an assertion aborted early.
				_ = cmd.Process.Kill()
			})
			if err := json.NewEncoder(input).Encode(control{ID: "signal-test", Command: "load"}); err != nil {
				t.Fatal(err)
			}
			loaded := false
			deadline := time.NewTimer(3 * time.Second)
			defer deadline.Stop()
			for !loaded {
				select {
				case value := <-events:
					loaded = value.State == "loaded"
				case err := <-done:
					t.Fatalf("child exited before load: %v", err)
				case <-deadline.C:
					t.Fatal("child did not start bounded test load")
				}
			}
			if err := cmd.Process.Signal(signal); err != nil {
				t.Fatal(err)
			}
			if err := awaitResult(t, done); err != nil {
				t.Fatalf("signal was not handled cleanly: %v", err)
			}
		})
	}
}

func TestResidentLoadChild(t *testing.T) {
	if os.Getenv("S0_RESIDENT_LOAD_TEST_CHILD") != "1" {
		return
	}
	os.Args = []string{"resident-load", "--id", "signal-test", "--memory-bytes", "16384", "--workers", "2", "--lifetime", "5s"}
	main()
}
