//go:build linux

package driver

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/egressauthstore"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/nodeauth"
	rootfsauthority "github.com/sandbox0-ai/sandbox0/manager/pkg/rootfswriterauthority"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/runtimeslotauthority"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/runtimeslotnode"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	"github.com/sandbox0-ai/sandbox0/pkg/dbpool"
	"github.com/sandbox0-ai/sandbox0/pkg/quota"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshandoff"
	"github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

type liveNomadMigrationLogger struct{}

func (liveNomadMigrationLogger) Printf(string, ...any) {}
func (liveNomadMigrationLogger) Fatalf(format string, args ...any) {
	panic(fmt.Sprintf(format, args...))
}

func TestLiveNomadRegionalReceiptFixture(t *testing.T) {
	if os.Getenv("S0_NOMAD_LIVE_REAL_ROOTFS") != "1" {
		t.Skip("requires the isolated real regional Nomad fixture")
	}
	store := sandboxstore.NewPGSandboxStore(liveNomadRegionalPool(t))
	root := "/opt/s0-live-update-test/f"
	data, err := os.ReadFile(filepath.Join(root, "regional-stage.json"))
	require.NoError(t, err)
	var stage rootfshandoff.StageRequest
	require.NoError(t, json.Unmarshal(data, &stage))
	require.NoError(t, stage.ValidateDurableBinding())
	descriptor, err := rootfsblock.DecodeDescriptor(stage.Generation.Descriptor)
	require.NoError(t, err)
	require.Equal(t, int64(64<<40), descriptor.LogicalSizeBytes)
	mountName := fmt.Sprintf("%x", sha256.Sum256([]byte(stage.Parent)))
	xfsMount := filepath.Join(root, "mounts", mountName, "xfs")
	var stat unix.Statfs_t
	require.NoError(t, unix.Statfs(xfsMount, &stat))
	require.Equal(t, int64(unix.XFS_SUPER_MAGIC), int64(stat.Type))
	size := int64(stat.Blocks) * int64(stat.Bsize)
	require.GreaterOrEqual(t, size, int64(1<<30), "XFS must grow beyond its initial 300 MiB")
	require.LessOrEqual(t, size, int64(1200<<20))
	client, err := rootfsauthority.NewManagerClient(rootfsauthority.ManagerClientConfig{BaseURL: "https://s0-fixture.local:19445",
		CAFile: filepath.Join(root, "pki/ca.pem"), ClientCertFile: filepath.Join(root, "pki/peer.pem"), ClientKeyFile: filepath.Join(root, "pki/peer-key.pem"),
		TokenFile: filepath.Join(root, "token"), Timeout: 5 * time.Second})
	require.NoError(t, err)
	observation, err := client.ObserveLiveCommit(t.Context(), stage)
	require.NoError(t, err)
	if os.Getenv("S0_NOMAD_LIVE_MEMORY") == "1" {
		if _, statErr := os.Stat(filepath.Join(root, "memory-result.json")); statErr == nil {
			// Restore consumes a new writer, whose own live cursor begins at zero.
			// Require it to publish and install a fresh cut; the source's receipts
			// cannot stand in for continued live publication by this new owner.
			require.Eventually(t, func() bool {
				observation, err = client.ObserveLiveCommit(t.Context(), stage)
				return err == nil && observation.Progress.InstalledSequence > 0
			}, 90*time.Second, 100*time.Millisecond, "restored writer must install a fresh live cut")
		}
	}
	require.Positive(t, observation.Progress.InstalledSequence)
	grant, err := store.GetRootFSWriterGrant(t.Context(), stage.Identity.WriterGrantID)
	require.NoError(t, err)
	require.Equal(t, sandboxstore.RootFSWriterGrantStateConsumed, grant.State)
	filesystem, err := store.GetRootFSFilesystem(t.Context(), stage.Identity.RootFSID)
	require.NoError(t, err)
	require.Equal(t, stage.InitialGeneration, filesystem.HeadGenerationID)
	slot, err := store.GetRuntimeSlot(t.Context(), stage.Identity.SlotNonce)
	require.NoError(t, err)
	require.Equal(t, sandboxstore.RuntimeSlotStateActive, slot.State)
	require.Equal(t, stage.Identity.AllocationID, slot.AllocationID)
	require.Equal(t, grant.ID, slot.WriterGrantID)
	t.Logf("real_nomad_postgres_s3_live_base=true xfs_bytes=%d logical_device_bytes=%d installed_sequence=%d committed_sequence=%d current_writer_consumed=true current_slot_active=true", size, descriptor.LogicalSizeBytes, observation.Progress.InstalledSequence, observation.Progress.CommittedSequence)
}

func liveNomadRegionalPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	require.Equal(t, "1", os.Getenv("S0_NOMAD_LIVE_REAL_ROOTFS"))
	require.Zero(t, os.Geteuid())
	_, err := os.Stat("/opt/s0-live-update-test/isolated-fixture")
	require.NoError(t, err)
	pool, err := dbpool.New(t.Context(), dbpool.Options{DatabaseURL: os.Getenv("INTEGRATION_DATABASE_URL"), Schema: "scheduler", MaxConns: 10})
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	var name string
	require.NoError(t, pool.QueryRow(t.Context(), `SELECT current_database()`).Scan(&name))
	require.True(t, strings.HasPrefix(name, "s0_rootfs_autogrow_nomad_"), "requires a dedicated disposable Nomad database")
	return pool
}

// The external harness runs this alongside actual ctld and Nomad. Both slot
// transitions and RootFS grants/publications use the production mTLS handlers
// and PostgreSQL store rather than the original Python authority double.
func TestLiveNomadRegionalAuthorityFixture(t *testing.T) {
	if os.Getenv("S0_NOMAD_LIVE_REAL_ROOTFS") != "1" {
		t.Skip("requires the isolated real regional Nomad fixture")
	}
	pool := liveNomadRegionalPool(t)
	_, err := pool.Exec(t.Context(), `CREATE SCHEMA IF NOT EXISTS scheduler;
		CREATE OR REPLACE FUNCTION scheduler.update_updated_at_column() RETURNS TRIGGER AS $$
		BEGIN NEW.updated_at = NOW(); RETURN NEW; END; $$ LANGUAGE plpgsql;`)
	require.NoError(t, err)
	logger := liveNomadMigrationLogger{}
	require.NoError(t, quota.RunMigrations(t.Context(), pool, logger))
	require.NoError(t, egressauthstore.RunMigrations(t.Context(), pool, logger))
	require.NoError(t, sandboxstore.RunSandboxStoreMigrations(t.Context(), pool, logger))
	store := sandboxstore.NewPGSandboxStore(pool)
	nodeID := os.Getenv("S0_NOMAD_LIVE_NODE_ID")
	require.NotEmpty(t, nodeID)
	if os.Getenv("S0_NOMAD_LIVE_MEMORY") == "1" {
		liveNomadMemoryAuthority(t, store, nodeID)
		return
	}
	verifier, err := nodeauth.NewCertificateVerifier([]nodeauth.CertificateIdentity{
		{CommonName: "s0-live-fixture", ClusterID: "fixture-cluster", NodeID: nodeID, NodeUID: "fixture-node-1", AgentUID: "fixture-ctld"},
	})
	require.NoError(t, err)
	slots, err := runtimeslotauthority.NewHandler(runtimeslotauthority.HandlerConfig{Verifier: verifier, Store: store, HeartbeatTTL: time.Minute})
	require.NoError(t, err)
	writer, err := rootfsauthority.NewHandler(rootfsauthority.HandlerConfig{Verifier: verifier, Store: store, LeaseTTL: 10 * time.Minute})
	require.NoError(t, err)
	writer, err = rootfsauthority.NewLifecycleHandler(verifier, store, writer)
	require.NoError(t, err)
	channel, err := runtimeslotnode.NewChannelHub(verifier, store)
	require.NoError(t, err)
	defer channel.Close()
	mux := http.NewServeMux()
	mux.Handle(runtimeslot.NodeChannelPath, channel)
	mux.Handle("/internal/v1/runtime-slots/", slots)
	mux.Handle("/", writer)
	handler, err := nodeauth.NewVerifiedCertificateMiddleware(verifier, mux)
	require.NoError(t, err)
	pki := "/opt/s0-live-update-test/f/pki"
	ca, err := os.ReadFile(filepath.Join(pki, "ca.pem"))
	require.NoError(t, err)
	roots := x509.NewCertPool()
	require.True(t, roots.AppendCertsFromPEM(ca))
	listener, err := net.Listen("tcp", "127.0.0.1:19445")
	require.NoError(t, err)
	server := &http.Server{Handler: handler, ReadHeaderTimeout: 10 * time.Second,
		TLSConfig: &tls.Config{MinVersion: tls.VersionTLS12, ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: roots}}
	done := make(chan error, 1)
	go func() {
		done <- server.ServeTLS(listener, filepath.Join(pki, "peer.pem"), filepath.Join(pki, "peer-key.pem"))
	}()
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		require.NoError(t, server.Shutdown(ctx))
	}()
	ready := "/opt/s0-live-update-test/f/region-ready"
	require.NoError(t, os.WriteFile(ready, []byte("real_postgres_mtls_authority\n"), 0600))
	defer os.Remove(ready)
	t.Log("real PostgreSQL runtime-slot and RootFS authority listening on fixture-only loopback")
	for {
		select {
		case err := <-done:
			require.ErrorIs(t, err, http.ErrServerClosed)
			return
		case <-t.Context().Done():
			return
		case <-time.After(time.Second):
			if _, err := os.Stat("/opt/s0-live-update-test/f/stop-region"); err == nil {
				return
			} else {
				require.True(t, os.IsNotExist(err))
			}
		}
	}
}
