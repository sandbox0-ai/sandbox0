//go:build linux

package gvisorcli

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

// The parent's real unmount and directory removal occur after the child has
// copied the mount table. The old single-attempt launcher fails with ENOENT.
// A successful retry must abandon that namespace, preserve the current bundle,
// and leave the parent's mounts intact.
func TestPrivilegedMountPruneAfterHostPathRemoval(t *testing.T) {
	if os.Getenv("SANDBOX0_RUN_PRIVILEGED_MOUNT_ISOLATION") != "1" {
		t.Skip("requires an isolated Linux host")
	}
	if root := os.Getenv("SANDBOX0_MOUNT_PRUNE_RETRY_CHILD_ROOT"); root != "" {
		mountPruneRetryChild(root)
		return
	}
	require.Zero(t, os.Geteuid())
	root := t.TempDir()
	current, victim := filepath.Join(root, "current"), filepath.Join(root, "victim")
	for _, path := range []string{current, victim} {
		require.NoError(t, os.Mkdir(path, 0700))
		require.NoError(t, unix.Mount("tmpfs", path, "tmpfs", 0, "size=1m"))
		defer unix.Unmount(path, 0)
	}
	require.NoError(t, os.WriteFile(filepath.Join(current, "proof"), []byte("current-bundle"), 0600))
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	binary, err := os.Executable()
	require.NoError(t, err)
	cmd := exec.CommandContext(ctx, binary, "-test.run=^TestPrivilegedMountPruneAfterHostPathRemoval$")
	cmd.Env = append(os.Environ(), "SANDBOX0_MOUNT_PRUNE_RETRY_CHILD_ROOT="+root)
	stdout, err := cmd.StdoutPipe()
	require.NoError(t, err)
	stdin, err := cmd.StdinPipe()
	require.NoError(t, err)
	cmd.Stderr = os.Stderr
	require.NoError(t, cmd.Start())
	defer func() { cancel(); _ = cmd.Wait() }()
	reader := bufio.NewReader(stdout)
	line, err := reader.ReadString('\n')
	require.NoError(t, err)
	require.Equal(t, "MOUNT_SNAPSHOT_COPIED\n", line)
	require.NoError(t, unix.Unmount(victim, 0))
	require.NoError(t, os.Remove(victim))
	_, err = fmt.Fprintln(stdin, "continue")
	require.NoError(t, err)
	require.NoError(t, stdin.Close())
	line, err = reader.ReadString('\n')
	require.NoError(t, err)
	require.Equal(t, "FRESH_NAMESPACE_PRUNED\n", line)
	require.NoError(t, cmd.Wait())
	proof, err := os.ReadFile(filepath.Join(current, "proof"))
	require.NoError(t, err)
	require.Equal(t, "current-bundle", string(proof))
}

func mountPruneRetryChild(root string) {
	fail := func(err error) {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	reads := 0
	err := isolateAndPruneRootFSMounts(root, []string{filepath.Join(root, "current")}, func() ([]byte, error) {
		data, err := readMountTable()
		if err == nil && reads == 0 {
			fmt.Println("MOUNT_SNAPSHOT_COPIED")
			_, err = bufio.NewReader(os.Stdin).ReadString('\n')
		}
		reads++
		return data, err
	})
	if err != nil {
		fail(err)
	}
	if reads != 2 {
		fail(fmt.Errorf("expected exactly one fresh namespace retry, got %d reads", reads))
	}
	data, err := readMountTable()
	if err != nil {
		fail(err)
	}
	paths, err := rootFSPrunePaths(data, root, []string{filepath.Join(root, "current")})
	if err != nil || len(paths) != 0 || strings.Contains(string(data), root+"/victim") {
		fail(fmt.Errorf("unrelated mount survived retry: paths=%v err=%v", paths, err))
	}
	proof, err := os.ReadFile(filepath.Join(root, "current", "proof"))
	if err != nil || string(proof) != "current-bundle" {
		fail(fmt.Errorf("current bundle changed: %v", err))
	}
	fmt.Println("FRESH_NAMESPACE_PRUNED")
	// Do not return this private locked thread to the Go test runtime.
	os.Exit(0)
}

func TestPrivilegedMountPruneFailsClosedOnBusyMount(t *testing.T) {
	if os.Getenv("SANDBOX0_RUN_PRIVILEGED_MOUNT_ISOLATION") != "1" {
		t.Skip("requires an isolated Linux host")
	}
	if root := os.Getenv("SANDBOX0_MOUNT_PRUNE_BUSY_CHILD_ROOT"); root != "" {
		if err := os.Chdir(filepath.Join(root, "victim")); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		reads := 0
		err := isolateAndPruneRootFSMounts(root, nil, func() ([]byte, error) {
			reads++
			return readMountTable()
		})
		if !errors.Is(err, unix.EBUSY) || reads != 1 {
			fmt.Fprintf(os.Stderr, "busy mount must reject launch without retry: reads=%d err=%v\n", reads, err)
			os.Exit(1)
		}
		os.Exit(0)
	}
	require.Zero(t, os.Geteuid())
	root := t.TempDir()
	victim := filepath.Join(root, "victim")
	require.NoError(t, os.Mkdir(victim, 0700))
	require.NoError(t, unix.Mount("tmpfs", victim, "tmpfs", 0, "size=1m"))
	defer unix.Unmount(victim, 0)
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	binary, err := os.Executable()
	require.NoError(t, err)
	cmd := exec.CommandContext(ctx, binary, "-test.run=^TestPrivilegedMountPruneFailsClosedOnBusyMount$")
	cmd.Env = append(os.Environ(), "SANDBOX0_MOUNT_PRUNE_BUSY_CHILD_ROOT="+root)
	output, err := cmd.CombinedOutput()
	require.NoError(t, err, string(output))
	require.NoError(t, unix.Unmount(victim, 0), "child must leave the host mount unchanged")
}
