package slotnetwork

import (
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/containerd/errdefs"
)

type recheckInspector struct {
	mu     sync.Mutex
	ips    map[string]string
	errors map[string]error
	calls  []string
	hook   func() error
}

func (i *recheckInspector) Inspect(path, _ string) (string, error) {
	i.mu.Lock()
	i.calls = append(i.calls, path)
	ip, err, hook := i.ips[path], i.errors[path], i.hook
	i.hook = nil
	i.mu.Unlock()
	if hook != nil {
		if err := hook(); err != nil {
			return "", err
		}
	}
	return ip, err
}

func (i *recheckInspector) InspectClaimed(path, identity, expected string) error {
	ip, err := i.Inspect(path, identity)
	if err != nil {
		return err
	}
	// An empty address with the exact live carrier models stock runsc's
	// transfer into netstack, rather than physical namespace disappearance.
	if ip != "" && ip != expected {
		return errdefs.ErrFailedPrecondition
	}
	return nil
}

func recheckRegistry(t *testing.T, count int) (*Registry, *recheckInspector, []string) {
	t.Helper()
	directory := t.TempDir()
	root := filepath.Join(directory, "netns")
	if err := ensureDirectory(root); err != nil {
		t.Fatal(err)
	}
	i := &recheckInspector{ips: make(map[string]string), errors: make(map[string]error)}
	r := newTestRegistry(t, filepath.Join(directory, "network.db"), root, i, time.Hour)
	t.Cleanup(func() { _ = r.Close() })
	autoAcknowledge(r)
	paths := make([]string, 0, count)
	for index := 0; index < count; index++ {
		request := testRegistrationRequest()
		if index > 0 {
			request.SlotID = fmt.Sprintf("slot-%03d", index+1)
			request.AllocationID = fmt.Sprintf("allocation-%03d", index+1)
			request.NetNSRelativePath = request.AllocationID
			request.NetNSIdentity = fmt.Sprintf("netns-v1:1:%x", index+2)
		}
		path := filepath.Join(root, request.NetNSRelativePath)
		i.ips[path] = fmt.Sprintf("192.0.2.%d", index+1)
		if err := r.Register(t.Context(), request); err != nil {
			t.Fatal(err)
		}
		paths = append(paths, path)
	}
	i.calls = nil
	return r, i, paths
}

func TestRegistryRevalidationFencesUniqueAbsentNamespaceWithoutDeletingJournal(t *testing.T) {
	for _, absent := range []error{errExactNamespaceAbsent, errExactNamespaceUnroutable} {
		t.Run(absent.Error(), func(t *testing.T) {
			r, i, paths := recheckRegistry(t, 1)
			i.errors[paths[0]] = absent
			if err := r.RevalidateNamespaces(); err != nil {
				t.Fatal(err)
			}
			snapshot, _, err := r.Snapshot()
			if err != nil || len(snapshot) != 0 {
				t.Fatalf("snapshot = %+v, error = %v", snapshot, err)
			}
			if stats := r.Stats(); stats.Orphaned != 1 || stats.Terminal != 0 {
				t.Fatalf("stats = %+v", stats)
			}
			// A projection fence is never a regional cleanup acknowledgement.
			if entry := r.entries[testRegistrationRequest().SlotID]; entry.record.Cleanup != nil || entry.record.State != recordStateWarm {
				t.Fatalf("durable cleanup authority changed: %+v", entry.record)
			}
			if _, err := r.Prune(time.Now().Add(48 * time.Hour)); err != nil {
				t.Fatal(err)
			}
			if len(r.entries) != 1 {
				t.Fatal("unacknowledged journal was pruned")
			}
		})
	}
}

func TestRegistryRevalidationBoundsInspectionsAndRotatesPastLiveEntries(t *testing.T) {
	r, i, paths := recheckRegistry(t, 65)
	seen := make(map[string]bool)
	for pass := 0; pass < 3; pass++ {
		i.calls = nil
		if err := r.RevalidateNamespaces(); err != nil {
			t.Fatal(err)
		}
		if len(i.calls) != 32 {
			t.Fatalf("physical inspections = %d, want 32", len(i.calls))
		}
		for _, path := range i.calls {
			seen[path] = true
		}
	}
	if len(seen) != len(paths) {
		t.Fatalf("checked %d of %d namespaces", len(seen), len(paths))
	}
}

func TestRegistryRevalidationPreservesUnknownFailureAndContinuesRotation(t *testing.T) {
	r, i, paths := recheckRegistry(t, 65)
	for _, path := range paths {
		i.errors[path] = errdefs.ErrUnavailable
	}
	if err := r.RevalidateNamespaces(); !errors.Is(err, errdefs.ErrUnavailable) {
		t.Fatalf("error = %v", err)
	}
	first := i.calls[0]
	i.calls = nil
	if err := r.RevalidateNamespaces(); !errors.Is(err, errdefs.ErrUnavailable) {
		t.Fatalf("error = %v", err)
	}
	if len(i.calls) != 32 || i.calls[0] == first {
		t.Fatalf("rotation stalled: %v", i.calls)
	}
	if stats := r.Stats(); stats.Orphaned != 0 || stats.Warm != 65 {
		t.Fatalf("stats = %+v", stats)
	}
}

func TestRegistryRevalidationRetainsClaimedNetstackAddress(t *testing.T) {
	r, i, paths := recheckRegistry(t, 1)
	if _, err := r.Prepare(t.Context(), testPrepareRequest()); err != nil {
		t.Fatal(err)
	}
	i.ips[paths[0]] = ""
	if err := r.RevalidateNamespaces(); err != nil {
		t.Fatal(err)
	}
	snapshot, _, err := r.Snapshot()
	if err != nil || len(snapshot) != 1 || snapshot[0].SourceIP != "192.0.2.1" {
		t.Fatalf("snapshot = %+v, error = %v", snapshot, err)
	}
}

func TestRegistryRevalidationDoesNotFenceConcurrentClaim(t *testing.T) {
	r, i, _ := recheckRegistry(t, 1)
	started, proceed := make(chan struct{}), make(chan struct{})
	i.hook = func() error { close(started); <-proceed; return errExactNamespaceUnroutable }
	done := make(chan error, 1)
	go func() { done <- r.RevalidateNamespaces() }()
	<-started
	_, prepareErr := r.Prepare(t.Context(), testPrepareRequest())
	close(proceed)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if prepareErr != nil {
		t.Fatal(prepareErr)
	}
	if stats := r.Stats(); stats.Orphaned != 0 || stats.Claimed != 1 {
		t.Fatalf("stats = %+v", stats)
	}
}
