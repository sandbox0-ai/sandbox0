// Command resident-load supplies explicitly controlled, finite guest load for
// occupied-node acceptance. Its telemetry is not proof of host memory residency;
// the acceptance runner must independently sample the exact leased cgroup.
package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"runtime"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

const (
	pageBytes       = 4096
	maximumMemory   = 8 << 30
	maximumWorkers  = 32
	maximumLifetime = 50 * time.Second
)

type loadConfig struct {
	ID       string
	Memory   int64
	Workers  int
	Lifetime time.Duration
	Control  string
}

func (c loadConfig) validate() error {
	if c.Control != "" && c.Control != "stdin" && c.Control != "signals" {
		return errors.New("control must be stdin or signals")
	}
	if c.ID == "" || len(c.ID) > 64 || strings.Trim(c.ID, "abcdefghijklmnopqrstuvwxyz0123456789-") != "" {
		return errors.New("id must contain 1..64 lowercase letters, digits or hyphens")
	}
	if c.Memory < pageBytes || c.Memory > maximumMemory || c.Memory%pageBytes != 0 || c.Memory > int64(int(^uint(0)>>1)) {
		return errors.New("memory-bytes must be a page multiple in [4096, 8GiB] fitting the process address space")
	}
	if c.Workers < 1 || c.Workers > maximumWorkers {
		return errors.New("workers must be in [1, 32]")
	}
	if c.Lifetime < time.Second || c.Lifetime > maximumLifetime {
		return errors.New("lifetime must be in [1s, 50s]")
	}
	return nil
}

type control struct {
	ID      string `json:"id"`
	Command string `json:"command"`
}

type event struct {
	Version        int      `json:"version"`
	ID             string   `json:"id"`
	PID            int      `json:"pid"`
	Sequence       uint64   `json:"sequence"`
	ElapsedNS      int64    `json:"elapsed_ns"`
	LifetimeNS     int64    `json:"lifetime_ns"`
	State          string   `json:"state"`
	AllocatedBytes int64    `json:"allocated_bytes"`
	Workers        int      `json:"workers"`
	Iterations     []uint64 `json:"iterations"`
}

type activeLoad struct {
	memory   []byte
	counters []atomic.Uint64
	cancel   context.CancelFunc
	wait     sync.WaitGroup
}

// Touch before acknowledging load. A virtual mapping or zero-page read does not
// establish physical residency; use nonzero, page-distinct writes and retain the
// allocation until every worker has exited.
func startLoad(ctx context.Context, cfg loadConfig) (*activeLoad, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	load := &activeLoad{memory: make([]byte, int(cfg.Memory)), counters: make([]atomic.Uint64, cfg.Workers)}
	for offset := 0; offset < len(load.memory); offset += pageBytes {
		if offset%(1024*pageBytes) == 0 {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
		}
		binary.LittleEndian.PutUint64(load.memory[offset:], uint64(offset/pageBytes)+1)
	}
	workerCtx, cancel := context.WithCancel(ctx)
	load.cancel = cancel
	started := make(chan struct{}, cfg.Workers)
	for index := range cfg.Workers {
		load.wait.Add(1)
		go func(index int) {
			defer load.wait.Done()
			var count uint64
			value := uint64(index + 1)
			for {
				select {
				case <-workerCtx.Done():
					return
				default:
				}
				for range 4096 {
					value ^= value << 13
					value ^= value >> 7
					value ^= value << 17
				}
				// Consume data from the retained allocation without a streaming
				// memory-write workload or allocation/GC churn in each iteration.
				page := int(value % uint64(len(load.memory)/pageBytes))
				value ^= binary.LittleEndian.Uint64(load.memory[page*pageBytes:])
				count++
				load.counters[index].Store(count)
				if count == 1 {
					started <- struct{}{}
				}
			}
		}(index)
	}
	for range cfg.Workers {
		select {
		case <-ctx.Done():
			load.stop()
			return nil, ctx.Err()
		case <-started:
		}
	}
	return load, nil
}

func (l *activeLoad) stop() {
	if l == nil {
		return
	}
	l.cancel()
	l.wait.Wait()
	runtime.KeepAlive(l.memory)
	l.memory = nil
}

type receivedControl struct {
	value control
	err   error
}

func readControls(ctx context.Context, input io.Reader) <-chan receivedControl {
	result := make(chan receivedControl, 1)
	go func() {
		defer close(result)
		scanner := bufio.NewScanner(input)
		scanner.Buffer(make([]byte, 4096), 4096)
		for scanner.Scan() {
			decoder := json.NewDecoder(bytes.NewReader(scanner.Bytes()))
			decoder.DisallowUnknownFields()
			var cmd control
			err := decoder.Decode(&cmd)
			if err == nil && decoder.Decode(new(any)) != io.EOF {
				err = errors.New("control must be exactly one JSON object")
			}
			if err != nil {
				err = errors.New("invalid load control input")
			}
			select {
			case result <- receivedControl{value: cmd, err: err}:
			case <-ctx.Done():
				return
			}
			if err != nil {
				return
			}
		}
		if scanner.Err() != nil {
			select {
			case result <- receivedControl{err: errors.New("load control input exceeds bound or failed")}:
			case <-ctx.Done():
			}
		}
	}()
	return result
}

// Non-PTY HTTP CMD contexts do not supply a stdin pipe. Signal mode is an
// explicit diagnostic opt-in using the existing exact-context signal API. The
// runner must wait for idle before signaling, then await each state ack without
// replaying a signal: standard Unix signals are not a reliable queued protocol.
func signalControls(ctx context.Context, owner string) (<-chan receivedControl, func()) {
	signals := make(chan os.Signal, 2)
	signal.Notify(signals, syscall.SIGUSR1, syscall.SIGUSR2)
	result := make(chan receivedControl, 1)
	go func() {
		defer close(result)
		for {
			select {
			case <-ctx.Done():
				return
			case sig := <-signals:
				command := "load"
				if sig == syscall.SIGUSR2 {
					command = "release"
				}
				select {
				case result <- receivedControl{value: control{ID: owner, Command: command}}:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return result, func() { signal.Stop(signals) }
}

type writeEvent struct {
	value event
	done  chan error
}

// At most one event is pending. A blocked output consumer must not keep CPU load
// alive after expiry. The process exits without waiting for a blocked writer;
// terminal telemetry can therefore be absent and must never be assumed successful.
func eventEmitter(ctx context.Context, output io.Writer) func(event) error {
	queue := make(chan writeEvent)
	go func() {
		encoder := json.NewEncoder(output)
		for {
			select {
			case <-ctx.Done():
				return
			case request := <-queue:
				request.done <- encoder.Encode(request.value)
			}
		}
	}()
	return func(value event) error {
		request := writeEvent{value: value, done: make(chan error, 1)}
		select {
		case queue <- request:
		case <-ctx.Done():
			return ctx.Err()
		}
		select {
		case err := <-request.done:
			return err
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func runLoad(parent context.Context, cfg loadConfig, input io.ReadCloser, output io.Writer, heartbeat time.Duration) error {
	if err := cfg.validate(); err != nil {
		return err
	}
	if input == nil || output == nil || heartbeat <= 0 {
		return errors.New("input, output and positive heartbeat are required")
	}
	ctx, cancel := context.WithTimeout(parent, cfg.Lifetime)
	defer cancel()
	defer input.Close()
	started := time.Now()
	emit := eventEmitter(ctx, output)
	var controls <-chan receivedControl
	if cfg.Control == "signals" {
		var stop func()
		controls, stop = signalControls(ctx, cfg.ID)
		defer stop()
	} else {
		controls = readControls(ctx, input)
	}
	state := "idle"
	var current *activeLoad
	defer func() {
		current.stop()
	}()
	var sequence uint64
	publish := func() error {
		sequence++
		value := event{Version: 1, ID: cfg.ID, PID: os.Getpid(), Sequence: sequence, ElapsedNS: time.Since(started).Nanoseconds(), LifetimeNS: cfg.Lifetime.Nanoseconds(), State: state, Iterations: []uint64{}}
		if current != nil {
			value.AllocatedBytes, value.Workers = int64(len(current.memory)), len(current.counters)
			for i := range current.counters {
				value.Iterations = append(value.Iterations, current.counters[i].Load())
			}
		}
		return publishChecked(ctx, emit, value)
	}
	if err := publish(); err != nil {
		return err
	}
	ticker := time.NewTicker(heartbeat)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := publish(); err != nil {
				return err
			}
		case received, open := <-controls:
			if !open {
				current.stop()
				current = nil
				state = "stopped"
				return publish()
			}
			if received.err != nil {
				return received.err
			}
			cmd := received.value
			if cmd.ID != cfg.ID {
				return errors.New("load control owner mismatch")
			}
			switch cmd.Command {
			case "load":
				if state != "idle" {
					return errors.New("load is single-use and only accepted while idle")
				}
				var err error
				current, err = startLoad(ctx, cfg)
				if err != nil {
					return err
				}
				state = "loaded"
			case "release":
				if state != "loaded" {
					return errors.New("release is only accepted while loaded")
				}
				current.stop()
				current = nil
				// Outside target timing: request return of the guest allocation.
				// Only host cgroup measurements can prove resident charge fell.
				debug.FreeOSMemory()
				state = "released"
			case "stop":
				current.stop()
				current = nil
				state = "stopped"
				return publish()
			default:
				return errors.New("unknown load control command")
			}
			if err := publish(); err != nil {
				return err
			}
		}
	}
}

func publishChecked(ctx context.Context, emit func(event) error, value event) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return emit(value)
}

func program(ctx context.Context, args []string, input io.ReadCloser, output, stderr io.Writer) int {
	flags := flag.NewFlagSet("resident-load", flag.ContinueOnError)
	flags.SetOutput(stderr)
	var cfg loadConfig
	flags.StringVar(&cfg.ID, "id", "", "required unique load owner ID")
	flags.Int64Var(&cfg.Memory, "memory-bytes", 0, "required allocated bytes, page multiple, at most 8GiB")
	flags.IntVar(&cfg.Workers, "workers", 0, "required CPU worker count, 1..32")
	flags.DurationVar(&cfg.Lifetime, "lifetime", 45*time.Second, "process lifetime, at most 50s; never refreshed by control input")
	flags.StringVar(&cfg.Control, "control", "stdin", "control transport: stdin JSON or signals (USR1 load, USR2 release)")
	if err := flags.Parse(args); err != nil {
		return 2
	}
	if flags.NArg() != 0 {
		fmt.Fprintln(stderr, "positional arguments are not allowed")
		return 2
	}
	err := runLoad(ctx, cfg, input, output, 250*time.Millisecond)
	if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
		fmt.Fprintln(stderr, err)
		return 1
	}
	return 0
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	code := program(ctx, os.Args[1:], os.Stdin, os.Stdout, os.Stderr)
	stop()
	os.Exit(code)
}
