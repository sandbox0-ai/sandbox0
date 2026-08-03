//go:build linux

package ha

import (
	"context"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"golang.org/x/sys/unix"
)

func TestMetricsCollectorReportsElectionStateAndTransitions(t *testing.T) {
	coordinator := newTestCoordinator(t, t.TempDir(), "a")
	coordinator.setState(func(state *State) {
		*state = State{Role: RoleStandby, Epoch: 3, Synchronized: true}
	})
	coordinator.setState(func(state *State) {
		*state = State{Role: RolePrimary, Epoch: 4, Synchronized: true, Standbys: 1}
	})
	coordinator.setState(func(state *State) {
		state.Standbys = 2
	})

	collector, err := newMetricsCollector(coordinator, "node-a", "a")
	if err != nil {
		t.Fatalf("newMetricsCollector() error = %v", err)
	}
	registry := prometheus.NewRegistry()
	registry.MustRegister(collector)
	want := `
# HELP ctld_ha_epoch Current node-local ctld HA election epoch
# TYPE ctld_ha_epoch gauge
ctld_ha_epoch{node="node-a",slot="a"} 4
# HELP ctld_ha_primary Whether this ctld peer currently owns the node-local primary lock
# TYPE ctld_ha_primary gauge
ctld_ha_primary{node="node-a",slot="a"} 1
# HELP ctld_ha_role Current ctld HA role as a one-hot labeled gauge
# TYPE ctld_ha_role gauge
ctld_ha_role{node="node-a",role="primary",slot="a"} 1
# HELP ctld_ha_role_transitions_total Ctld HA role changes since process start
# TYPE ctld_ha_role_transitions_total counter
ctld_ha_role_transitions_total{from="standby",node="node-a",slot="a",to="primary"} 1
ctld_ha_role_transitions_total{from="starting",node="node-a",slot="a",to="standby"} 1
# HELP ctld_ha_standbys Number of synchronized standbys connected to this primary
# TYPE ctld_ha_standbys gauge
ctld_ha_standbys{node="node-a",slot="a"} 2
# HELP ctld_ha_synchronized Whether the ctld peer has a synchronized HA counterpart
# TYPE ctld_ha_synchronized gauge
ctld_ha_synchronized{node="node-a",slot="a"} 1
`
	if err := testutil.GatherAndCompare(
		registry,
		strings.NewReader(want),
		"ctld_ha_epoch",
		"ctld_ha_primary",
		"ctld_ha_role",
		"ctld_ha_role_transitions_total",
		"ctld_ha_standbys",
		"ctld_ha_synchronized",
	); err != nil {
		t.Fatal(err)
	}
}

func TestCoordinatorRecordsSharedLockIdentity(t *testing.T) {
	root := t.TempDir()
	coordinator := newTestCoordinator(t, root, "a")
	lease, err := coordinator.WaitForPrimary(context.Background())
	if err != nil {
		t.Fatalf("WaitForPrimary() error = %v", err)
	}
	defer lease.Close()

	snapshot := coordinator.MetricsSnapshot()
	if !snapshot.LockIdentity.Known {
		t.Fatal("lock identity is not known after opening the primary lock")
	}
	var stat unix.Stat_t
	if err := unix.Stat(filepath.Join(root, "ha", "primary.lock"), &stat); err != nil {
		t.Fatalf("stat primary lock: %v", err)
	}
	if snapshot.LockIdentity.Device != uint64(stat.Dev) || snapshot.LockIdentity.Inode != stat.Ino {
		t.Fatalf("lock identity = %#v, want device=%d inode=%d", snapshot.LockIdentity, stat.Dev, stat.Ino)
	}
}

func TestMetricsServerExposesStartingPeerBeforeElection(t *testing.T) {
	coordinator := newTestCoordinator(t, t.TempDir(), "b")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	server, err := StartMetricsServer(ctx, "127.0.0.1:0", coordinator, "node-b", "b")
	if err != nil {
		t.Fatalf("StartMetricsServer() error = %v", err)
	}
	defer server.Close()

	response, err := http.Get("http://" + server.Addr() + "/metrics")
	if err != nil {
		t.Fatalf("GET HA metrics: %v", err)
	}
	defer response.Body.Close()
	payload, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf("read HA metrics: %v", err)
	}
	metrics := string(payload)
	for _, want := range []string{
		`ctld_ha_primary{node="node-b",slot="b"} 0`,
		`ctld_ha_role{node="node-b",role="starting",slot="b"} 1`,
	} {
		if !strings.Contains(metrics, want) {
			t.Fatalf("HA metrics missing %q:\n%s", want, metrics)
		}
	}
}

func TestRecordLockIdentityRejectsClosedFile(t *testing.T) {
	coordinator := newTestCoordinator(t, t.TempDir(), "a")
	file, err := os.CreateTemp(t.TempDir(), "closed-lock-")
	if err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	if err := coordinator.recordLockIdentity(file); err == nil {
		t.Fatal("recordLockIdentity() error = nil for a closed file")
	}
}
