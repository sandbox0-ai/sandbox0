//go:build linux

package http

import (
	"bufio"
	"context"
	"debug/elf"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	nethttp "net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/opencontainers/go-digest"
	oci "github.com/opencontainers/runtime-spec/specs-go"
	"github.com/sandbox0-ai/sandbox0/manager/procd/pkg/session"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"github.com/sandbox0-ai/sandbox0/pkg/gvisorcli"
	"github.com/sandbox0-ai/sandbox0/pkg/objectstore"
	"github.com/sandbox0-ai/sandbox0/pkg/procdapi"
	"github.com/sandbox0-ai/sandbox0/pkg/runtimecheckpoint"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"golang.org/x/sys/unix"
)

const migrationGuestEnv = "SANDBOX0_MIGRATION_TEST_GUEST"
const migrationWorkerEnv = "SANDBOX0_MIGRATION_TEST_WORKER"

type migrationGuestEvidence struct {
	Instance        string `json:"instance"`
	Attempt         string `json:"attempt"`
	PID             int    `json:"pid"`
	Token           string `json:"token"`
	Generation      int64  `json:"generation"`
	ReceiptSurvived bool   `json:"receipt_survived"`
}

// TestPrivilegedProcdSessionCheckpoint runs the real supervisor, authenticated
// migration HTTP path and child process inside stock runsc. Three independent
// runtime roots and copied readonly filesystems exercise process restoration;
// they do not replace two-node block-COW, cgroup or network-handover acceptance.
func TestPrivilegedProcdSessionCheckpoint(t *testing.T) {
	if os.Getenv("SANDBOX0_RUN_PRIVILEGED_CHECKPOINT") != "1" {
		t.Skip("requires isolated Linux runsc test host")
	}
	require.Zero(t, os.Geteuid())
	runsc, err := exec.LookPath("runsc")
	require.NoError(t, err)
	// The probe needs only guest loopback. A fresh OCI network namespace has
	// no configured interfaces; stock runsc's none mode supplies its isolated
	// loopback stack without depending on the host's CNI or production routing.
	shim := filepath.Join(t.TempDir(), "runsc-loopback")
	require.NoError(t, os.WriteFile(shim, []byte("#!/bin/sh\nexec '"+
		strings.ReplaceAll(runsc, "'", "'\\''")+"' --network=none --restore-spec-validation=enforce \"$@\"\n"), 0700))
	runsc = shim
	binary, err := os.Executable()
	require.NoError(t, err)
	executable, err := elf.Open(binary)
	require.NoError(t, err)
	defer executable.Close()
	for _, p := range executable.Progs {
		require.NotEqual(t, elf.PT_INTERP, p.Type, "compile with CGO_ENABLED=0")
	}
	for _, directFS := range []bool{true, false} {
		t.Run(fmt.Sprintf("directfs_%t", directFS), func(t *testing.T) {
			root := t.TempDir()
			evidence := filepath.Join(root, "evidence")
			require.NoError(t, os.Mkdir(evidence, 0700))
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
			defer cancel()
			makeRuntime := func(name string) (gvisorcli.CheckpointRunsc, string) {
				rootfs := filepath.Join(root, name+"-rootfs")
				for _, dir := range []string{rootfs, filepath.Join(rootfs, "proc"), filepath.Join(rootfs, "tmp"), filepath.Join(rootfs, "dev"), filepath.Join(rootfs, "evidence")} {
					require.NoError(t, os.Mkdir(dir, 0700))
				}
				input, err := os.Open(binary)
				require.NoError(t, err)
				output, err := os.OpenFile(filepath.Join(rootfs, "payload"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0700)
				require.NoError(t, err)
				_, err = io.Copy(output, input)
				require.NoError(t, err)
				require.NoError(t, input.Close())
				require.NoError(t, output.Close())
				bundle := filepath.Join(root, name+"-bundle")
				require.NoError(t, os.Mkdir(bundle, 0700))
				generation := "1"
				if name == "target" {
					generation = "2"
				} else if name == "return" {
					generation = "3"
				}
				spec := oci.Spec{Version: oci.Version, Root: &oci.Root{Path: rootfs, Readonly: true},
					Process: &oci.Process{Cwd: "/", Args: []string{"/payload", "-test.run=^TestMigrationSessionGuest$"},
						Env: []string{migrationGuestEnv + "=1", "GOMAXPROCS=2", "SANDBOX0_PROBE_SPEC_GENERATION=" + generation}},
					Mounts: []oci.Mount{
						{Destination: "/proc", Type: "proc", Source: "proc"},
						{Destination: "/dev", Type: "tmpfs", Source: "tmpfs", Options: []string{"mode=755", "size=1m"}},
						{Destination: "/tmp", Type: "tmpfs", Source: "tmpfs", Options: []string{"mode=1777", "size=64m"}},
						{Destination: "/evidence", Type: "bind", Source: evidence, Options: []string{"rbind", "rw"}},
					}, Linux: &oci.Linux{Namespaces: []oci.LinuxNamespace{
						{Type: oci.PIDNamespace}, {Type: oci.MountNamespace}, {Type: oci.NetworkNamespace}, {Type: oci.IPCNamespace}, {Type: oci.UTSNamespace},
					}},
				}
				payload, err := json.Marshal(spec)
				require.NoError(t, err)
				require.NoError(t, os.WriteFile(filepath.Join(bundle, "config.json"), payload, 0600))
				runtimeRoot := filepath.Join(root, name+"-runsc")
				runner := gvisorcli.New(gvisorcli.Config{Path: runsc, Root: runtimeRoot, Platform: "systrap", Overlay2: "none", FileAccess: "shared", DirectFS: directFS}).(gvisorcli.CheckpointRunsc)
				t.Cleanup(func() {
					cleanup, done := context.WithTimeout(context.Background(), 15*time.Second)
					defer done()
					if err := runner.Delete(cleanup, name, true); err != nil {
						t.Logf("isolated runtime cleanup: %v", err)
					}
					if err := unix.Unmount(filepath.Join(runtimeRoot, "null-netns"), 0); err != nil && !errors.Is(err, unix.ENOENT) && !errors.Is(err, unix.EINVAL) {
						t.Errorf("isolated netns cleanup: %v", err)
					}
				})
				return runner, bundle
			}
			source, sourceBundle := makeRuntime("source")
			target, targetBundle := makeRuntime("target")
			require.NoError(t, source.Create(ctx, sourceBundle, "source"))
			require.NoError(t, source.Start(ctx, "source"))
			before := waitMigrationGuestEvidence(t, ctx, filepath.Join(evidence, "prepared.json"))
			image := filepath.Join(root, "image")
			require.NoError(t, source.Checkpoint(ctx, "source", image))
			require.NoError(t, source.Delete(ctx, "source", true))
			store, err := runtimecheckpoint.New(objectstore.NewMemoryStore(""), 512<<20)
			require.NoError(t, err)
			d := digest.FromString("isolated-procd-migration-probe").String()
			binding := runtimecheckpoint.Binding{OperationID: "probe", SandboxID: "sandbox-1", TeamID: "team-1", SourceBindingDigest: d,
				RuntimeCompatibilityDigest: d, AssignmentRevision: d, CPUFeaturesDigest: d, RootFSGenerationID: "probe", RootFSDescriptorDigest: d}
			ref, err := store.Publish(ctx, binding, image)
			require.NoError(t, err)
			restored := filepath.Join(root, "restored-image")
			_, err = store.Download(ctx, binding, ref, restored)
			require.NoError(t, err)
			require.NoError(t, target.Create(ctx, targetBundle, "target"))
			require.NoError(t, target.Restore(ctx, "target", restored))
			require.NoError(t, os.WriteFile(filepath.Join(evidence, "restore"), nil, 0600))
			after := waitMigrationGuestEvidence(t, ctx, filepath.Join(evidence, "restored.json"))
			require.Equal(t, before.Instance, after.Instance)
			require.Equal(t, before.Attempt, after.Attempt)
			require.Equal(t, before.PID, after.PID)
			require.Equal(t, before.Token, after.Token)
			require.EqualValues(t, 2, after.Generation)
			require.True(t, after.ReceiptSurvived)
			waitMigrationGuestEvidence(t, ctx, filepath.Join(evidence, "prepared-again.json"))
			secondImage := filepath.Join(root, "second-image")
			require.NoError(t, target.Checkpoint(ctx, "target", secondImage))
			require.NoError(t, target.Delete(ctx, "target", true))
			returned, returnBundle := makeRuntime("return")
			require.NoError(t, returned.Create(ctx, returnBundle, "return"))
			require.NoError(t, returned.Restore(ctx, "return", secondImage))
			require.NoError(t, os.WriteFile(filepath.Join(evidence, "restore-again"), nil, 0600))
			again := waitMigrationGuestEvidence(t, ctx, filepath.Join(evidence, "restored-again.json"))
			require.Equal(t, before.Instance, again.Instance)
			require.Equal(t, before.Attempt, again.Attempt)
			require.Equal(t, before.PID, again.PID)
			require.Equal(t, before.Token, again.Token)
			require.EqualValues(t, 3, again.Generation)
			require.True(t, again.ReceiptSurvived)
		})
	}
}

func waitMigrationGuestEvidence(t *testing.T, ctx context.Context, path string) migrationGuestEvidence {
	t.Helper()
	for {
		payload, err := os.ReadFile(path)
		var result migrationGuestEvidence
		if err == nil && json.Unmarshal(payload, &result) == nil {
			return result
		}
		if log, err := os.ReadFile(filepath.Join(filepath.Dir(path), "guest.log")); err == nil &&
			(strings.Contains(string(log), "--- FAIL:") || strings.Contains(string(log), "panic:")) {
			t.Fatalf("isolated migration guest failed: %s", log)
		}
		select {
		case <-ctx.Done():
			t.Fatalf("guest evidence %s unavailable: %v", filepath.Base(path), ctx.Err())
		case <-time.After(20 * time.Millisecond):
		}
	}
}

func TestMigrationSessionGuest(t *testing.T) {
	if os.Getenv(migrationGuestEnv) != "1" {
		t.Skip("isolated checkpoint guest only")
	}
	// Capture guest-side diagnostics on the probe's private evidence mount;
	// the stock CLI adapter deliberately does not retain inherited stdio pipes.
	log, err := os.OpenFile("/evidence/guest.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
	require.NoError(t, err)
	require.NoError(t, unix.Dup2(int(log.Fd()), 1))
	require.NoError(t, unix.Dup2(int(log.Fd()), 2))
	store, err := session.NewFileStore("/tmp/sessions")
	require.NoError(t, err)
	supervisor, err := session.NewSupervisor(store, zap.NewNop())
	require.NoError(t, err)
	defer supervisor.Close()
	f := newMigrationHTTPFixture(t, supervisor)
	host := httptest.NewUnstartedServer(f.s.router)
	require.NoError(t, host.Listener.Close())
	host.Listener, err = net.Listen("tcp", ":0")
	require.NoError(t, err)
	host.Start()
	host.URL = "http://127.0.0.1:" + strconv.Itoa(host.Listener.Addr().(*net.TCPAddr).Port)
	defer host.Close()
	client := procdapi.NewProcdClient(procdapi.ProcdClientConfig{})
	value, _, err := supervisor.Create(session.SessionSpec{Command: []string{"/payload", "-test.run=^TestMigrationSessionWorker$"},
		Env: map[string]string{migrationWorkerEnv: "1"}, IO: session.IOSpec{Mode: session.IOModePipes}}, "stable-creation-key")
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		value, err = supervisor.Get(value.ID)
		return err == nil && value.Phase == session.PhaseRunning
	}, 5*time.Second, 10*time.Millisecond)
	input := session.InputRequest{InputID: "before", ExpectedAttemptID: value.Attempt.ID, DataBase64: base64.StdEncoding.EncodeToString([]byte("before\n"))}
	_, err = supervisor.WriteInput(value.ID, input)
	require.NoError(t, err)
	beforeLine := waitMigrationSessionOutput(t, supervisor, value.ID, 1)
	parts := strings.Split(strings.TrimSpace(beforeLine), ":")
	require.Len(t, parts, 3)
	require.Equal(t, "before", parts[2])
	pid, err := strconv.Atoi(parts[0])
	require.NoError(t, err)
	require.Equal(t, value.Attempt.PID, pid)
	_, err = client.MigrateRuntime(context.Background(), host.URL, f.request, f.token(t, f.request))
	require.NoError(t, err)
	writeEvidence := func(name string, generation int64, receipt bool) {
		payload, err := json.Marshal(migrationGuestEvidence{Instance: f.s.instanceID, Attempt: value.Attempt.ID, PID: pid, Token: parts[1], Generation: generation, ReceiptSurvived: receipt})
		require.NoError(t, err)
		require.NoError(t, os.WriteFile("/evidence/"+name+".new", payload, 0600))
		require.NoError(t, os.Rename("/evidence/"+name+".new", "/evidence/"+name+".json"))
	}
	writeEvidence("prepared", 1, false)
	for {
		if _, err := os.Stat("/evidence/restore"); err == nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	// The destination OCI environment changed, but this is still the captured
	// process. Only the authenticated in-memory handover advances its identity.
	require.Equal(t, "1", os.Getenv("SANDBOX0_PROBE_SPEC_GENERATION"))
	restore := f.request
	restore.Action = procdapi.MigrationRestore
	_, err = client.MigrateRuntime(context.Background(), host.URL, restore, f.token(t, restore))
	require.NoError(t, err)
	receipt, err := supervisor.WriteInput(value.ID, input)
	require.NoError(t, err)
	require.True(t, receipt.Duplicate)
	input.InputID = "after"
	input.DataBase64 = base64.StdEncoding.EncodeToString([]byte("after\n"))
	_, err = supervisor.WriteInput(value.ID, input)
	require.NoError(t, err)
	afterOutput := waitMigrationSessionOutput(t, supervisor, value.ID, 2)
	require.Equal(t, beforeLine+parts[0]+":"+parts[1]+":after\n", afterOutput)
	after, err := supervisor.Get(value.ID)
	require.NoError(t, err)
	require.Equal(t, value.Attempt.ID, after.Attempt.ID)
	require.Equal(t, value.Attempt.PID, after.Attempt.PID)
	require.EqualValues(t, 1, after.Attempt.RuntimeGeneration)
	require.EqualValues(t, 2, after.RuntimeGeneration)
	readRequest, err := nethttp.NewRequest(nethttp.MethodGet, host.URL+"/api/v1/sessions/"+value.ID, nil)
	require.NoError(t, err)
	readRequest.Header.Set("X-Internal-Token", f.token(t, restore))
	readResponse, err := nethttp.DefaultClient.Do(readRequest)
	require.NoError(t, err)
	require.Equal(t, nethttp.StatusOK, readResponse.StatusCode)
	visible, apiErr, err := spec.DecodeResponse[session.Session](readResponse.Body)
	require.NoError(t, readResponse.Body.Close())
	require.NoError(t, err)
	require.Nil(t, apiErr)
	require.Equal(t, after.Attempt.ID, visible.Attempt.ID)
	require.EqualValues(t, 2, visible.RuntimeGeneration)
	writeEvidence("restored", after.RuntimeGeneration, receipt.Duplicate)
	next := f.request
	next.LifecycleEpoch++
	next.Assignment.OperationID = "migration-2"
	next.Assignment.SourceGeneration = 2
	next.Assignment.SourceRevision, err = next.Assignment.Target.Revision()
	require.NoError(t, err)
	next.Assignment.Target.RuntimeGeneration = 3
	_, err = client.MigrateRuntime(context.Background(), host.URL, next, f.token(t, next))
	require.NoError(t, err)
	writeEvidence("prepared-again", 2, true)
	for {
		if _, err := os.Stat("/evidence/restore-again"); err == nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	// Preserve blocked guest goroutines when a repeat restore loses HTTP
	// progress; a live child alone cannot prove procd's control plane survived.
	diagnostic := time.AfterFunc(10*time.Second, func() {
		stack := make([]byte, 1<<20)
		n := runtime.Stack(stack, true)
		_, _ = log.Write(stack[:n])
	})
	next.Action = procdapi.MigrationRestore
	_, err = client.MigrateRuntime(context.Background(), host.URL, next, f.token(t, next))
	diagnostic.Stop()
	require.NoError(t, err)
	againReceipt, err := supervisor.WriteInput(value.ID, input)
	require.NoError(t, err)
	require.True(t, againReceipt.Duplicate)
	again, err := supervisor.Get(value.ID)
	require.NoError(t, err)
	require.EqualValues(t, 3, again.RuntimeGeneration)
	writeEvidence("restored-again", again.RuntimeGeneration, againReceipt.Duplicate)
	select {}
}

func waitMigrationSessionOutput(t *testing.T, supervisor *session.Supervisor, id string, lines int) string {
	t.Helper()
	var output string
	require.Eventually(t, func() bool {
		page, err := supervisor.Events(id, 0, 1000)
		if err != nil {
			return false
		}
		output = ""
		for _, event := range page.Events {
			if event.Stream == "stdout" {
				data, err := base64.StdEncoding.DecodeString(event.DataBase64)
				if err != nil {
					return false
				}
				output += string(data)
			}
		}
		return strings.Count(output, "\n") >= lines
	}, 5*time.Second, 10*time.Millisecond)
	return output
}

// The worker's token exists only in its original process memory. A restart is
// observable even if a new supervisor invents the same public session identity.
func TestMigrationSessionWorker(t *testing.T) {
	if os.Getenv(migrationWorkerEnv) != "1" {
		t.Skip("isolated session worker only")
	}
	token := time.Now().UnixNano()
	input := bufio.NewScanner(os.Stdin)
	for input.Scan() {
		_, err := fmt.Fprintf(os.Stdout, "%d:%d:%s\n", os.Getpid(), token, input.Text())
		require.NoError(t, err)
	}
	require.NoError(t, input.Err())
}
