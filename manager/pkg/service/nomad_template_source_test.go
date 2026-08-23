package service

import (
	"context"
	"crypto/sha256"
	"errors"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	"github.com/sandbox0-ai/sandbox0/pkg/template"
)

func TestNomadSandboxTemplateSourceResolverUsesDurableClaimAndRuntimeSlot(t *testing.T) {
	now := time.Date(2026, time.August, 21, 4, 0, 0, 0, time.UTC)
	record := nomadTemplateSourceTestRecord(now, sandboxstore.SandboxDesiredStateActive)
	store := &memorySandboxStore{
		records: map[string]*sandboxstore.SandboxRecord{record.ID: record},
		runtimeSlots: map[string]*sandboxstore.RuntimeSlot{record.ID: {
			ID: "slot-a", SandboxID: record.ID, AllocationID: record.RuntimeID,
			State: sandboxstore.RuntimeSlotStateActive, ProcdInstanceID: "procd-a",
			ProcdAddress: "http://192.0.2.2:49983", CommandReadyDigest: make([]byte, sha256.Size),
			CommandReadyAt: now, HeartbeatExpiresAt: now.Add(time.Minute), AuthorityObservedAt: now,
		}},
	}
	resolver, err := NewNomadSandboxTemplateSourceResolver(store, true, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}

	source, err := resolver.ResolveSandboxTemplateSource(context.Background(), record.ID, record.TeamID)
	if err != nil {
		t.Fatalf("ResolveSandboxTemplateSource() error = %v", err)
	}
	if source.SandboxID != record.ID || source.TeamID != record.TeamID || source.ClusterID != record.ClusterID ||
		source.Spec.MainContainer.Image != record.TemplateSpec.MainContainer.Image {
		t.Fatalf("source = %+v", source)
	}
	source.Spec.MainContainer.Image = "changed"
	if record.TemplateSpec.MainContainer.Image != "ubuntu:24.04" {
		t.Fatal("resolved source spec aliases the durable record")
	}
}

func TestNomadSandboxTemplateSourceResolverAcceptsPausedSourceWithoutSlot(t *testing.T) {
	now := time.Date(2026, time.August, 21, 4, 0, 0, 0, time.UTC)
	record := nomadTemplateSourceTestRecord(now, sandboxstore.SandboxDesiredStatePaused)
	store := &memorySandboxStore{records: map[string]*sandboxstore.SandboxRecord{record.ID: record}}
	resolver, err := NewNomadSandboxTemplateSourceResolver(store, true, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	if _, err := resolver.ResolveSandboxTemplateSource(context.Background(), record.ID, record.TeamID); err != nil {
		t.Fatalf("ResolveSandboxTemplateSource() error = %v", err)
	}
}

func TestNomadSandboxTemplateSourceResolverFailsClosed(t *testing.T) {
	now := time.Date(2026, time.August, 21, 4, 0, 0, 0, time.UTC)
	if _, err := NewNomadSandboxTemplateSourceResolver(nil, true, nil); err == nil {
		t.Fatal("nil source store was accepted")
	}

	tests := []struct {
		name        string
		configure   func(*sandboxstore.SandboxRecord)
		available   bool
		teamID      string
		targetError error
	}{
		{name: "publisher unavailable", available: false, teamID: "team-a", targetError: template.ErrTemplateSourceUnavailable},
		{name: "active slot missing", available: true, teamID: "team-a", targetError: template.ErrTemplateSourceNotReady},
		{name: "foreign runtime", available: true, teamID: "team-a", configure: func(record *sandboxstore.SandboxRecord) {
			record.RuntimeBackend = "kubernetes"
		}, targetError: template.ErrTemplateSourceUnavailable},
		{name: "cross team", available: true, teamID: "team-b", targetError: template.ErrTemplateSourceForbidden},
		{name: "hard expired", available: true, teamID: "team-a", configure: func(record *sandboxstore.SandboxRecord) {
			record.HardExpiresAt = now
		}, targetError: template.ErrTemplateSourceNotFound},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			record := nomadTemplateSourceTestRecord(now, sandboxstore.SandboxDesiredStateActive)
			if test.configure != nil {
				test.configure(record)
			}
			store := &memorySandboxStore{records: map[string]*sandboxstore.SandboxRecord{record.ID: record}}
			resolver, err := NewNomadSandboxTemplateSourceResolver(store, test.available, func() time.Time { return now })
			if err != nil {
				t.Fatal(err)
			}
			_, err = resolver.ResolveSandboxTemplateSource(context.Background(), record.ID, test.teamID)
			if !errors.Is(err, test.targetError) {
				t.Fatalf("ResolveSandboxTemplateSource() error = %v, want %v", err, test.targetError)
			}
		})
	}
}

func TestNomadSandboxTemplateSourceResolverRejectsActiveLifecycle(t *testing.T) {
	now := time.Date(2026, time.August, 21, 4, 0, 0, 0, time.UTC)
	record := nomadTemplateSourceTestRecord(now, sandboxstore.SandboxDesiredStatePaused)
	store := &memorySandboxStore{
		records: map[string]*sandboxstore.SandboxRecord{record.ID: record},
		lifecycleTxns: map[string]*sandboxstore.SandboxLifecycleTxn{"resume-a": {
			ID: "resume-a", SandboxID: record.ID, Kind: sandboxstore.SandboxLifecycleKindResume,
			Phase: sandboxstore.SandboxLifecyclePhasePreparing,
		}},
	}
	resolver, err := NewNomadSandboxTemplateSourceResolver(store, true, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	if _, err := resolver.ResolveSandboxTemplateSource(context.Background(), record.ID, record.TeamID); !errors.Is(err, template.ErrTemplateSourceNotReady) {
		t.Fatalf("ResolveSandboxTemplateSource() error = %v", err)
	}
}

func nomadTemplateSourceTestRecord(now time.Time, desiredState string) *sandboxstore.SandboxRecord {
	return &sandboxstore.SandboxRecord{
		ID: "sandbox-a", TeamID: "team-a", UserID: "user-a", ClusterID: "cluster-a",
		TemplateID: "default", RuntimeBackend: sandboxstore.SandboxRuntimeBackendNomad,
		DesiredState: desiredState, RuntimeID: "allocation-a",
		TemplateSpec: v1alpha1.SandboxTemplateSpec{
			MainContainer: v1alpha1.ContainerSpec{Image: "ubuntu:24.04"},
		},
		CreatedAt: now.Add(-time.Minute), UpdatedAt: now,
	}
}
