package service

import (
	"context"
	"crypto/sha256"
	"errors"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	"github.com/sandbox0-ai/sandbox0/pkg/managerapi"
)

func TestNomadSandboxReaderProjectsRuntimeSlotAndLifecycleFence(t *testing.T) {
	now := time.Date(2026, time.August, 21, 2, 0, 0, 0, time.UTC)
	store := &memorySandboxStore{
		records: map[string]*sandboxstore.SandboxRecord{
			"sandbox-a": {
				ID: "sandbox-a", TeamID: "team-a", TemplateID: "default",
				DesiredState: sandboxstore.SandboxDesiredStateActive,
				RuntimeID:    "allocation-a", CreatedAt: now,
			},
		},
		lifecycleTxns: map[string]*sandboxstore.SandboxLifecycleTxn{
			"pause-a": {
				ID: "pause-a", SandboxID: "sandbox-a", Kind: sandboxstore.SandboxLifecycleKindPause,
				Phase: sandboxstore.SandboxLifecyclePhasePreparing,
			},
		},
		runtimeSlots: map[string]*sandboxstore.RuntimeSlot{
			"sandbox-a": {
				ID: "slot-a", SandboxID: "sandbox-a", AllocationID: "allocation-a",
				State: sandboxstore.RuntimeSlotStateActive, ProcdInstanceID: "procd-a",
				ProcdAddress: "http://192.0.2.2:49983", CommandReadyDigest: make([]byte, sha256.Size),
				CommandReadyAt: now, HeartbeatExpiresAt: now.Add(time.Minute), AuthorityObservedAt: now,
			},
		},
	}
	reader, err := NewNomadSandboxReader(store)
	if err != nil {
		t.Fatal(err)
	}

	sandbox, err := reader.GetSandbox(context.Background(), "sandbox-a")
	if err != nil {
		t.Fatalf("GetSandbox() error = %v", err)
	}
	if sandbox.Status != managerapi.SandboxStatusRunning || sandbox.RuntimeID != "allocation-a" || sandbox.InternalAddr != "" {
		t.Fatalf("fenced sandbox projection = %+v", sandbox)
	}
	listed, err := reader.ListSandboxes(context.Background(), &sandboxstore.ListSandboxesRequest{TeamID: "team-a"})
	if err != nil {
		t.Fatalf("ListSandboxes() error = %v", err)
	}
	if listed.Count != 1 || len(listed.Sandboxes) != 1 || listed.Sandboxes[0].Status != managerapi.SandboxStatusRunning {
		t.Fatalf("list projection = %+v", listed)
	}
	status, err := reader.GetSandboxStatus(context.Background(), "sandbox-a")
	if err != nil || status["status"] != managerapi.SandboxStatusRunning {
		t.Fatalf("status = %+v, error = %v", status, err)
	}
}

func TestNomadSandboxReaderProjectsPausedOnlyAfterRuntimeSlotIsTerminal(t *testing.T) {
	now := time.Date(2026, time.August, 30, 3, 0, 0, 0, time.UTC)
	store := &memorySandboxStore{
		records: map[string]*sandboxstore.SandboxRecord{
			"sandbox-a": {
				ID: "sandbox-a", TeamID: "team-a", TemplateID: "default",
				DesiredState: sandboxstore.SandboxDesiredStatePaused,
				CreatedAt:    now,
			},
		},
		runtimeSlots: map[string]*sandboxstore.RuntimeSlot{
			"sandbox-a": {
				ID: "slot-a", SandboxID: "sandbox-a", AllocationID: "allocation-a",
				State: sandboxstore.RuntimeSlotStateQuiescing,
			},
		},
	}
	reader, err := NewNomadSandboxReader(store)
	if err != nil {
		t.Fatal(err)
	}

	pausing, err := reader.GetSandbox(context.Background(), "sandbox-a")
	if err != nil {
		t.Fatalf("GetSandbox() error = %v", err)
	}
	if pausing.Paused || pausing.Status != managerapi.SandboxStatusStarting || pausing.RuntimeID != "allocation-a" {
		t.Fatalf("non-terminal pause projection = %+v", pausing)
	}
	listed, err := reader.ListSandboxes(context.Background(), &sandboxstore.ListSandboxesRequest{TeamID: "team-a"})
	if err != nil {
		t.Fatalf("ListSandboxes() error = %v", err)
	}
	if listed.Count != 1 || listed.Sandboxes[0].Paused || listed.Sandboxes[0].Status != managerapi.SandboxStatusStarting {
		t.Fatalf("non-terminal pause list projection = %+v", listed)
	}

	delete(store.runtimeSlots, "sandbox-a")
	paused, err := reader.GetSandbox(context.Background(), "sandbox-a")
	if err != nil {
		t.Fatalf("GetSandbox() after terminalization error = %v", err)
	}
	if !paused.Paused || paused.Status != managerapi.SandboxStatusPaused || paused.RuntimeID != "" {
		t.Fatalf("terminal pause projection = %+v", paused)
	}
}

func TestNomadSandboxReaderFailsClosedForInvalidRequests(t *testing.T) {
	store := &memorySandboxStore{records: map[string]*sandboxstore.SandboxRecord{}}
	reader, err := NewNomadSandboxReader(store)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reader.GetSandbox(context.Background(), "missing"); !errors.Is(err, sandboxstore.ErrSandboxRecordNotFound) {
		t.Fatalf("missing error = %v", err)
	}
	if _, err := reader.ListSandboxes(context.Background(), &sandboxstore.ListSandboxesRequest{Offset: -1}); err == nil {
		t.Fatal("negative list offset was accepted")
	}
	if _, err := NewNomadSandboxReader(nil); err == nil {
		t.Fatal("nil projection store was accepted")
	}
}
