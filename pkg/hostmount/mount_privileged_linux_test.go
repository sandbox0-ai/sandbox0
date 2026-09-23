//go:build linux

package hostmount

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/sys/unix"
)

func TestSystemUnmountKeepsBusyTaskRootVisible(t *testing.T) {
	if os.Getenv("SANDBOX0_PRIVILEGED_MOUNT_TEST") != "1" {
		t.Skip("set SANDBOX0_PRIVILEGED_MOUNT_TEST=1 on an isolated Linux host")
	}
	if os.Geteuid() != 0 {
		t.Fatal("privileged mount test requires root")
	}
	root := t.TempDir()
	source := filepath.Join(root, "source")
	target := filepath.Join(root, "task-root")
	for _, path := range []string{source, target} {
		if err := os.Mkdir(path, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	if err := unix.Mount("tmpfs", source, "tmpfs", 0, "size=1m"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = unix.Unmount(source, 0) })
	if err := unix.Mount(source, target, "", unix.MS_BIND, ""); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = unix.Unmount(target, 0) })
	consumer, err := os.Open(target)
	if err != nil {
		t.Fatal(err)
	}
	defer consumer.Close()

	if err := (System{}).Unmount(target); !errors.Is(err, unix.EBUSY) {
		t.Fatalf("unmount busy task root = %v, want EBUSY", err)
	}
	if err := consumer.Close(); err != nil {
		t.Fatal(err)
	}
	if err := (System{}).Unmount(target); err != nil {
		t.Fatalf("retry task root unmount after consumer exit: %v", err)
	}
}
