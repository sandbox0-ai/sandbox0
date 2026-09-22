//go:build linux

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	oci "github.com/opencontainers/runtime-spec/specs-go"
	"github.com/sandbox0-ai/sandbox0/manager/procd/pkg/session"
	"github.com/sandbox0-ai/sandbox0/pkg/gvisorcli"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"github.com/sandbox0-ai/sandbox0/pkg/runtimecontrol"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

// TestPrivilegedProcdFilesystemResume exercises the real PID-1 shutdown path
// and durable session journals across fresh stock-runsc guests. It intentionally
// does not restore process memory or treat a stopped/finished session as active.
func TestPrivilegedProcdFilesystemResume(t *testing.T) {
	if os.Getenv("SANDBOX0_RUN_PRIVILEGED_CHECKPOINT") != "1" {
		t.Skip("requires isolated Linux runsc host and a static test binary")
	}
	require.Zero(t, os.Geteuid())
	runsc, err := exec.LookPath("runsc")
	require.NoError(t, err)
	binary, err := os.Executable()
	require.NoError(t, err)
	root := t.TempDir()
	rootfs := filepath.Join(root, "rootfs")
	for _, d := range []string{"proc", "dev", "tmp", "workspace", "config", "evidence", "var/lib/sandbox0/procd"} {
		require.NoError(t, os.MkdirAll(filepath.Join(rootfs, d), 0755))
	}
	in, err := os.Open(binary)
	require.NoError(t, err)
	out, err := os.Create(filepath.Join(rootfs, "payload"))
	require.NoError(t, err)
	_, err = io.Copy(out, in)
	require.NoError(t, err)
	require.NoError(t, in.Close())
	require.NoError(t, out.Close())
	require.NoError(t, os.Chmod(filepath.Join(rootfs, "payload"), 0755))
	private, public, err := internalauth.GenerateEd25519KeyPair()
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(rootfs, "config/internal_jwt_public.key"), public, 0600))
	require.NoError(t, os.WriteFile(filepath.Join(rootfs, "config/test-private.key"), private, 0600))
	shim := filepath.Join(root, "runsc-loopback")
	require.NoError(t, os.WriteFile(shim, []byte("#!/bin/sh\nexec '"+runsc+"' --network=none \"$@\"\n"), 0700))
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()
	for generation := 1; generation <= 4; generation++ {
		id := fmt.Sprintf("resume-%d", generation)
		bundle := filepath.Join(root, id)
		require.NoError(t, os.Mkdir(bundle, 0700))
		assignment, _ := json.Marshal(runtimecontrol.Assignment{SandboxID: "issue795", TeamID: "test-team", RuntimeGeneration: int64(generation), SecurityClass: "standard", EnvVars: map[string]string{runtimecontrol.EnvSandboxID: "issue795"}})
		spec := oci.Spec{Version: oci.Version, Root: &oci.Root{Path: rootfs}, Process: &oci.Process{Cwd: "/workspace", Args: []string{"/payload", "-test.run=^TestFilesystemResumeGuest$"}, Env: []string{"PATH=/", "GOMAXPROCS=2", "ISSUE795_GUEST=1", "ISSUE795_GENERATION=" + strconv.Itoa(generation), runtimecontrol.EnvControlMode + "=" + runtimecontrol.ControlModeStatic, runtimecontrol.EnvStaticAssignment + "=" + string(assignment)}}, Mounts: []oci.Mount{{Destination: "/proc", Type: "proc", Source: "proc"}, {Destination: "/dev", Type: "tmpfs", Source: "tmpfs", Options: []string{"mode=755", "size=1m"}}, {Destination: "/tmp", Type: "tmpfs", Source: "tmpfs", Options: []string{"mode=1777", "size=32m"}}}, Linux: &oci.Linux{Namespaces: []oci.LinuxNamespace{{Type: oci.PIDNamespace}, {Type: oci.MountNamespace}, {Type: oci.NetworkNamespace}, {Type: oci.IPCNamespace}, {Type: oci.UTSNamespace}}}}
		data, _ := json.Marshal(spec)
		require.NoError(t, os.WriteFile(filepath.Join(bundle, "config.json"), data, 0600))
		runtimeRoot := filepath.Join(root, id+"-runsc")
		runner := gvisorcli.New(gvisorcli.Config{Path: shim, Root: runtimeRoot, Platform: "systrap", Overlay2: "none", FileAccess: "shared", DirectFS: true})
		t.Cleanup(func() {
			_ = runner.Delete(context.Background(), id, true)
			_ = unix.Unmount(filepath.Join(runtimeRoot, "null-netns"), 0)
		})
		require.NoError(t, runner.Create(ctx, bundle, id))
		started := time.Now()
		require.NoError(t, runner.Start(ctx, id))
		ready := filepath.Join(rootfs, "evidence", id+".json")
		require.Eventually(t, func() bool { _, err := os.Stat(ready); return err == nil }, 30*time.Second, 20*time.Millisecond)
		data, err = os.ReadFile(ready)
		require.NoError(t, err)
		var evidence struct {
			Error    string            `json:"error"`
			Sessions []session.Session `json:"sessions"`
		}
		require.NoError(t, json.Unmarshal(data, &evidence))
		require.Empty(t, evidence.Error)
		require.Len(t, evidence.Sessions, 6)
		for _, s := range evidence.Sessions {
			switch s.Spec.Name {
			case "naturally-exited":
				require.Equal(t, session.PhaseExited, s.Phase)
				require.EqualValues(t, 1, s.Attempt.Number)
			case "recovery-stop":
				if generation > 1 {
					require.Equal(t, session.PhaseStopped, s.Phase)
					require.EqualValues(t, 1, s.Attempt.Number)
				}
			default:
				require.Equal(t, session.PhaseRunning, s.Phase)
				require.EqualValues(t, generation, s.Attempt.Number)
			}
		}
		t.Logf("generation=%d all session recovery checks passed in %s", generation, time.Since(started))
		require.NoError(t, runner.SignalInit(ctx, id, "TERM"))
		_, err = runner.Wait(ctx, id)
		require.NoError(t, err)
		require.NoError(t, runner.Delete(ctx, id, true))
	}
}

func TestFilesystemResumeGuest(t *testing.T) {
	if os.Getenv("ISSUE795_GUEST") != "1" {
		t.Skip("guest helper")
	}
	go func() {
		generation, _ := strconv.Atoi(os.Getenv("ISSUE795_GENERATION"))
		result := map[string]any{}
		err := filesystemResumeGuestProbe(generation, result)
		if err != nil {
			result["error"] = err.Error()
		}
		data, _ := json.Marshal(result)
		_ = os.WriteFile(fmt.Sprintf("/evidence/resume-%d.json", generation), data, 0600)
	}()
	main()
}

func filesystemResumeGuestProbe(generation int, result map[string]any) error {
	key, err := internalauth.LoadEd25519PrivateKeyFromFile("/config/test-private.key")
	if err != nil {
		return err
	}
	generator := internalauth.NewGenerator(internalauth.GeneratorConfig{Caller: "cluster-gateway", PrivateKey: key})
	client := &http.Client{Timeout: 5 * time.Second}
	request := func(method, path string, body any, target any) error {
		data, _ := json.Marshal(body)
		r, _ := http.NewRequest(method, "http://127.0.0.1:49983"+path, bytes.NewReader(data))
		token, err := generator.Generate("procd", "test-team", "", internalauth.GenerateOptions{SandboxID: "issue795"})
		if err != nil {
			return err
		}
		r.Header.Set("Authorization", "Bearer "+token)
		r.Header.Set("Content-Type", "application/json")
		response, err := client.Do(r)
		if err != nil {
			return err
		}
		defer response.Body.Close()
		if response.StatusCode >= 300 {
			return fmt.Errorf("%s %s: HTTP %d", method, path, response.StatusCode)
		}
		if target != nil {
			return json.NewDecoder(response.Body).Decode(target)
		}
		return nil
	}
	deadline := time.Now().Add(25 * time.Second)
	for request("GET", "/api/v1/sessions", nil, nil) != nil {
		if time.Now().After(deadline) {
			return fmt.Errorf("procd did not become ready")
		}
		time.Sleep(20 * time.Millisecond)
	}
	if generation == 1 {
		for i := 0; i < 6; i++ {
			name := fmt.Sprintf("running-%d", i)
			mode := "running"
			recovery := "restart"
			if i == 4 {
				name = "recovery-stop"
				recovery = "stop"
			}
			if i == 5 {
				name = "naturally-exited"
				mode = "exit"
			}
			body := map[string]any{"name": name, "command": []string{"/payload", "-test.run=^TestFilesystemResumeWorker$"}, "env": map[string]string{"ISSUE795_WORKER": mode}, "lifecycle": map[string]any{"runtime_recovery": recovery, "max_lifetime_seconds": 300}, "readiness": map[string]any{"type": "output", "output": "READY"}}
			if err := request("POST", "/api/v1/sessions", body, nil); err != nil {
				return err
			}
		}
	}
	for {
		var envelope struct {
			Data struct {
				Sessions []session.Session `json:"sessions"`
			} `json:"data"`
		}
		if err := request("GET", "/api/v1/sessions", nil, &envelope); err != nil {
			return err
		}
		ready := len(envelope.Data.Sessions) == 6
		for _, s := range envelope.Data.Sessions {
			if s.Spec.Name == "naturally-exited" {
				ready = ready && s.Phase == session.PhaseExited
			} else if s.Spec.Name == "recovery-stop" && generation > 1 {
				ready = ready && s.Phase == session.PhaseStopped
			} else {
				ready = ready && s.Phase == session.PhaseRunning
			}
		}
		if ready {
			result["sessions"] = envelope.Data.Sessions
			return nil
		}
		if time.Now().After(deadline) {
			result["sessions"] = envelope.Data.Sessions
			return fmt.Errorf("session recovery did not reach expected phases")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestFilesystemResumeWorker(t *testing.T) {
	mode := os.Getenv("ISSUE795_WORKER")
	if mode == "" {
		t.Skip("guest child")
	}
	if mode == "exit" {
		fmt.Println("READY")
		return
	}
	fmt.Print(strings.Repeat("x", 1<<20))
	fmt.Println("READY")
	time.Sleep(5 * time.Minute)
}
