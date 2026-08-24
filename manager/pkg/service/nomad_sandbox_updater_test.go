package service

import (
	"context"
	"crypto/sha256"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	"github.com/sandbox0-ai/sandbox0/pkg/apierror"
	"github.com/sandbox0-ai/sandbox0/pkg/managerapi"
	v1alpha1 "github.com/sandbox0-ai/sandbox0/pkg/sandboxspec"
)

func TestNomadSandboxUpdaterPersistsLifecycleFields(t *testing.T) {
	now := time.Date(2026, time.August, 21, 3, 0, 0, 987654321, time.UTC)
	for _, desiredState := range []string{
		sandboxstore.SandboxDesiredStateActive,
		sandboxstore.SandboxDesiredStatePaused,
	} {
		t.Run(desiredState, func(t *testing.T) {
			store := newNomadSandboxUpdaterTestStore(now, desiredState)
			updater, err := NewNomadSandboxUpdater(store, time.Minute, func() time.Time { return now })
			if err != nil {
				t.Fatal(err)
			}
			ttl, hardTTL, autoResume := int32(90), int32(180), false
			updated, err := updater.UpdateSandbox(context.Background(), "sandbox-a", &SandboxUpdateConfig{
				TTL: &ttl, HardTTL: &hardTTL, AutoResume: &autoResume,
			})
			if err != nil {
				t.Fatalf("UpdateSandbox() error = %v", err)
			}
			if desiredState == sandboxstore.SandboxDesiredStateActive && updated.Status != managerapi.SandboxStatusRunning {
				t.Fatalf("active status = %q", updated.Status)
			}
			if desiredState == sandboxstore.SandboxDesiredStatePaused && (!updated.Paused || updated.Status != managerapi.SandboxStatusPaused) {
				t.Fatalf("paused projection = %+v", updated)
			}
			record, err := store.GetSandbox(context.Background(), "sandbox-a")
			if err != nil {
				t.Fatal(err)
			}
			if record.Config.TTL == nil || *record.Config.TTL != ttl ||
				record.Config.HardTTL == nil || *record.Config.HardTTL != hardTTL ||
				record.Config.AutoResume == nil || *record.Config.AutoResume {
				t.Fatalf("persisted config = %+v", record.Config)
			}
			deadlineBase := now.UTC().Truncate(time.Second)
			if !record.ExpiresAt.Equal(deadlineBase.Add(90*time.Second)) ||
				!record.HardExpiresAt.Equal(deadlineBase.Add(180*time.Second)) {
				t.Fatalf("deadlines = %s, %s", record.ExpiresAt, record.HardExpiresAt)
			}
		})
	}
}

func TestNomadSandboxUpdaterRejectsUnorchestratedRuntimeFields(t *testing.T) {
	now := time.Date(2026, time.August, 21, 3, 0, 0, 0, time.UTC)
	tests := []struct {
		name   string
		config *SandboxUpdateConfig
	}{
		{name: "environment", config: &SandboxUpdateConfig{EnvVars: map[string]string{"NEW": "value"}}},
		{name: "resources", config: &SandboxUpdateConfig{Resources: &managerapi.SandboxResourceConfig{Memory: "1Gi"}}},
		{name: "network", config: &SandboxUpdateConfig{Network: &v1alpha1.SandboxNetworkPolicy{}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := newNomadSandboxUpdaterTestStore(now, sandboxstore.SandboxDesiredStateActive)
			before, err := store.GetSandbox(context.Background(), "sandbox-a")
			if err != nil {
				t.Fatal(err)
			}
			updater, err := NewNomadSandboxUpdater(store, time.Minute, func() time.Time { return now })
			if err != nil {
				t.Fatal(err)
			}
			if _, err := updater.UpdateSandbox(context.Background(), "sandbox-a", test.config); !errors.Is(err, ErrSandboxRuntimeUpdateUnavailable) {
				t.Fatalf("UpdateSandbox() error = %v", err)
			}
			after, err := store.GetSandbox(context.Background(), "sandbox-a")
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(before, after) {
				t.Fatalf("rejected update changed record\nbefore: %+v\nafter:  %+v", before, after)
			}
		})
	}
}

func TestNomadSandboxUpdaterPersistsNormalizedServices(t *testing.T) {
	now := time.Date(2026, time.August, 21, 3, 0, 0, 0, time.UTC)
	store := newNomadSandboxUpdaterTestStore(now, sandboxstore.SandboxDesiredStateActive)
	updater, err := NewNomadSandboxUpdater(store, time.Minute, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	updated, err := updater.UpdateSandbox(context.Background(), "sandbox-a", &SandboxUpdateConfig{
		Services: []managerapi.SandboxAppService{{
			ID: "WEB", Port: 8080,
			Ingress: managerapi.SandboxAppServiceIngress{Public: true},
		}},
	})
	if err != nil {
		t.Fatalf("UpdateSandbox() error = %v", err)
	}
	if len(updated.Services) != 1 || updated.Services[0].ID != "web" || len(updated.Services[0].Ingress.Routes) != 1 {
		t.Fatalf("services = %+v", updated.Services)
	}
	updated.Services[0].Ingress.Routes[0].PathPrefix = "/mutated"
	record, err := store.GetSandbox(context.Background(), "sandbox-a")
	if err != nil {
		t.Fatal(err)
	}
	if record.Config.Services[0].Ingress.Routes[0].PathPrefix != "/" {
		t.Fatalf("stored services shared response memory: %+v", record.Config.Services)
	}
}

func TestNomadSandboxUpdaterFailsClosed(t *testing.T) {
	if _, err := NewNomadSandboxUpdater(nil, time.Minute, nil); err == nil {
		t.Fatal("nil mutation store was accepted")
	}
	store := newNomadSandboxUpdaterTestStore(time.Now(), sandboxstore.SandboxDesiredStateActive)
	if _, err := NewNomadSandboxUpdater(store, -time.Second, nil); err == nil {
		t.Fatal("negative default TTL was accepted")
	}
	updater, err := NewNomadSandboxUpdater(store, time.Minute, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := updater.UpdateSandbox(context.Background(), "sandbox-a", nil); err == nil {
		t.Fatal("nil update config was accepted")
	}

	store.records["sandbox-a"].DesiredState = sandboxstore.SandboxDesiredStateTerminating
	if _, err := updater.UpdateSandbox(context.Background(), "sandbox-a", &SandboxUpdateConfig{}); !apierror.IsConflict(err) {
		t.Fatalf("terminating update error = %v", err)
	}
	store.records["sandbox-a"].DesiredState = "recovering"
	if _, err := updater.UpdateSandbox(context.Background(), "sandbox-a", &SandboxUpdateConfig{}); !apierror.IsConflict(err) {
		t.Fatalf("non-mutable state error = %v", err)
	}
}

func TestNomadSandboxUpdaterRefreshesDurableDeadlines(t *testing.T) {
	now := time.Date(2026, time.August, 21, 3, 0, 0, 987654321, time.UTC)
	ttl, hardTTL := int32(60), int32(120)
	tests := []struct {
		name              string
		config            sandboxstore.SandboxConfig
		defaultTTL        time.Duration
		request           *RefreshRequest
		wantTTL, wantHard time.Duration
	}{
		{name: "configured", config: sandboxstore.SandboxConfig{TTL: &ttl, HardTTL: &hardTTL}, defaultTTL: 45 * time.Second, wantTTL: time.Minute, wantHard: 2 * time.Minute},
		{name: "default", defaultTTL: 45 * time.Second, wantTTL: 45 * time.Second},
		{name: "requested", config: sandboxstore.SandboxConfig{TTL: &ttl, HardTTL: &hardTTL}, defaultTTL: 45 * time.Second, request: &RefreshRequest{Duration: 90}, wantTTL: 90 * time.Second, wantHard: 2 * time.Minute},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := newNomadSandboxUpdaterTestStore(now, sandboxstore.SandboxDesiredStateActive)
			store.records["sandbox-a"].Config = test.config
			updater, err := NewNomadSandboxUpdater(store, test.defaultTTL, func() time.Time { return now })
			if err != nil {
				t.Fatal(err)
			}
			response, err := updater.RefreshSandbox(context.Background(), "sandbox-a", test.request)
			if err != nil {
				t.Fatalf("RefreshSandbox() error = %v", err)
			}
			base := now.UTC().Truncate(time.Second)
			assertOptionalDeadline(t, "expires_at", response.ExpiresAt, base, test.wantTTL)
			assertOptionalDeadline(t, "hard_expires_at", response.HardExpiresAt, base, test.wantHard)
			record, err := store.GetSandbox(context.Background(), "sandbox-a")
			if err != nil {
				t.Fatal(err)
			}
			if !optionalDeadlineMatches(response.ExpiresAt, record.ExpiresAt) ||
				!optionalDeadlineMatches(response.HardExpiresAt, record.HardExpiresAt) {
				t.Fatalf("response and record deadlines differ: response=%+v record=%+v", response, record)
			}
		})
	}
}

func TestNomadSandboxUpdaterRejectsNegativeRefreshWithoutMutation(t *testing.T) {
	now := time.Date(2026, time.August, 21, 3, 0, 0, 0, time.UTC)
	store := newNomadSandboxUpdaterTestStore(now, sandboxstore.SandboxDesiredStateActive)
	before, err := store.GetSandbox(context.Background(), "sandbox-a")
	if err != nil {
		t.Fatal(err)
	}
	updater, err := NewNomadSandboxUpdater(store, time.Minute, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	if _, err := updater.RefreshSandbox(context.Background(), "sandbox-a", &RefreshRequest{Duration: -1}); !errors.Is(err, ErrInvalidClaimRequest) {
		t.Fatalf("RefreshSandbox() error = %v", err)
	}
	after, err := store.GetSandbox(context.Background(), "sandbox-a")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, after) {
		t.Fatalf("rejected refresh changed record\nbefore: %+v\nafter:  %+v", before, after)
	}
}

func newNomadSandboxUpdaterTestStore(now time.Time, desiredState string) *memorySandboxStore {
	ttl, hardTTL, autoResume := int32(60), int32(120), true
	store := &memorySandboxStore{
		records: map[string]*sandboxstore.SandboxRecord{
			"sandbox-a": {
				ID: "sandbox-a", TeamID: "team-a", TemplateID: "default",
				DesiredState: desiredState, RuntimeID: "allocation-a",
				Config: sandboxstore.SandboxConfig{
					TTL: &ttl, HardTTL: &hardTTL, AutoResume: &autoResume,
					EnvVars: map[string]string{"OLD": "value"},
				},
				ExpiresAt: now.Add(time.Minute), HardExpiresAt: now.Add(2 * time.Minute),
				CreatedAt: now.Add(-time.Minute), UpdatedAt: now,
			},
		},
		runtimeSlots: map[string]*sandboxstore.RuntimeSlot{},
	}
	if desiredState == sandboxstore.SandboxDesiredStateActive {
		store.runtimeSlots["sandbox-a"] = &sandboxstore.RuntimeSlot{
			ID: "slot-a", SandboxID: "sandbox-a", AllocationID: "allocation-a",
			State: sandboxstore.RuntimeSlotStateActive, ProcdInstanceID: "procd-a",
			ProcdAddress: "http://192.0.2.2:49983", CommandReadyDigest: make([]byte, sha256.Size),
			CommandReadyAt: now, HeartbeatExpiresAt: now.Add(time.Minute), AuthorityObservedAt: now,
		}
	}
	return store
}

func assertOptionalDeadline(t *testing.T, name string, got *time.Time, base time.Time, duration time.Duration) {
	t.Helper()
	if duration == 0 {
		if got != nil {
			t.Fatalf("%s = %s, want nil", name, got)
		}
		return
	}
	if got == nil || !got.Equal(base.Add(duration)) {
		t.Fatalf("%s = %v, want %s", name, got, base.Add(duration))
	}
}

func optionalDeadlineMatches(pointer *time.Time, value time.Time) bool {
	if pointer == nil {
		return value.IsZero()
	}
	return pointer.Equal(value)
}
