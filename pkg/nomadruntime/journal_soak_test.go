package nomadruntime

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/internal/soakstate"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"
)

const (
	defaultRuntimeSlotSoakDuration = 24 * time.Hour
	defaultRuntimeSlotSoakProofs   = 10_000
	defaultRuntimeSlotSoakBursts   = 20
	runtimeSlotSoakStateVersion    = 1
)

var runtimeSlotSoakIdentityBucket = []byte("runtime-slot-soak-v1")

type runtimeSlotSoakConfig struct {
	Duration      string `json:"duration"`
	Proofs        int    `json:"proofs"`
	Bursts        int    `json:"bursts"`
	TerminalTTL   string `json:"terminal_ttl"`
	StateDir      string `json:"state_dir"`
	JournalFormat int    `json:"journal_format"`
}

type runtimeSlotSoakCheckpoint struct {
	Version         int    `json:"version"`
	Phase           string `json:"phase"`
	ActiveElapsedNS int64  `json:"active_elapsed_ns"`
	Next            int    `json:"next"`
	Deleted         int    `json:"deleted"`
	Restarted       bool   `json:"restarted"`
	WarmSize        int64  `json:"warm_size"`
}

// TestRuntimeSlotJournalTwentyFourHourSoak is the opt-in active-time Bolt
// companion to the real PostgreSQL/RustFS materializer soak. It exercises the
// exact production journal encoding, pruning, reopen, and freelist reuse path.
func TestRuntimeSlotJournalTwentyFourHourSoak(t *testing.T) {
	rawDuration := strings.TrimSpace(os.Getenv("SANDBOX0_RUNTIME_SLOT_SOAK_DURATION"))
	if rawDuration == "" {
		t.Skip("set SANDBOX0_RUNTIME_SLOT_SOAK_DURATION=24h to run the endurance gate")
	}
	duration, err := time.ParseDuration(rawDuration)
	require.NoError(t, err)
	require.GreaterOrEqual(t, duration, 10*time.Second)
	require.LessOrEqual(t, duration, 7*24*time.Hour)
	proofs := runtimeSlotSoakIntEnv(t, "SANDBOX0_RUNTIME_SLOT_SOAK_PROOFS", defaultRuntimeSlotSoakProofs)
	require.GreaterOrEqual(t, proofs, 10)
	require.LessOrEqual(t, proofs, 100_000)
	bursts := runtimeSlotSoakIntEnv(t, "SANDBOX0_RUNTIME_SLOT_SOAK_BURSTS", defaultRuntimeSlotSoakBursts)
	require.GreaterOrEqual(t, bursts, 1)
	require.LessOrEqual(t, bursts, proofs/2)
	outputPath := strings.TrimSpace(os.Getenv("SANDBOX0_RUNTIME_SLOT_SOAK_OUTPUT"))
	require.NotEmpty(t, outputPath)
	stateDir := runtimeSlotSoakStateDir(t, os.Getenv("SANDBOX0_RUNTIME_SLOT_SOAK_STATE_DIR"))
	mode, err := soakstate.ParseMode(envOrRuntimeSlotSoak("SANDBOX0_RUNTIME_SLOT_SOAK_MODE", string(soakstate.ModeCreate)))
	require.NoError(t, err)
	config := runtimeSlotSoakConfig{
		Duration: duration.String(), Proofs: proofs, Bursts: bursts,
		TerminalTTL: time.Hour.String(), StateDir: stateDir,
		JournalFormat: RuntimeSlotJournalVersion,
	}
	initial := runtimeSlotSoakCheckpoint{
		Version: runtimeSlotSoakStateVersion, Phase: "active", WarmSize: -1,
	}
	evidence, err := soakstate.Open(soakstate.OpenOptions{
		Path: outputPath, Mode: mode, Config: config, Initial: initial,
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, evidence.Close()) }()
	state := initial
	if evidence.ResumeInfo().Resumed {
		require.NoError(t, evidence.DecodeCheckpoint(&state))
	}
	require.NoError(t, validateRuntimeSlotSoakCheckpoint(state, proofs, duration))
	if state.Phase == "complete" {
		t.Logf("runtime-slot journal soak %s is already complete", evidence.RunID())
		return
	}

	path := filepath.Join(stateDir, "runtime-slots.db")
	journal, err := newRuntimeSlotJournal(path, time.Hour)
	require.NoError(t, err)
	journalClosed := false
	defer func() {
		if !journalClosed {
			require.NoError(t, journal.Close())
		}
	}()
	allowIdentityInitialization := !evidence.ResumeInfo().Resumed ||
		(state.Next == 0 && state.Deleted == 0 && state.ActiveElapsedNS == 0 &&
			!state.Restarted && state.WarmSize == -1)
	require.NoError(t, ensureRuntimeSlotSoakIdentity(
		journal, evidence.RunID(), evidence.ConfigSHA256(), allowIdentityInitialization,
	))
	if evidence.ResumeInfo().Resumed {
		pruned, pruneErr := journal.Prune(time.Now().UTC())
		require.NoError(t, pruneErr)
		require.NoError(t, journal.db.Sync())
		require.NoError(t, evidence.Commit("resumed", time.Duration(state.ActiveElapsedNS), map[string]any{
			"resume": evidence.ResumeInfo(), "reconciled_uncheckpointed_records": pruned,
		}, state))
	}

	schedule := buildRuntimeSlotSoakSchedule(proofs, duration, bursts)
	require.Len(t, schedule, proofs)
	completedAt := time.Now().UTC().Add(-2 * time.Hour).Format(time.RFC3339Nano)
	warmAt := min(max(proofs/10, 100), proofs)
	nextSample := nextRuntimeSlotSoakBoundary(time.Duration(state.ActiveElapsedNS), time.Minute)
	nextCheckpoint := nextRuntimeSlotSoakBoundary(time.Duration(state.ActiveElapsedNS), runtimeSlotSoakCheckpointInterval(duration))
	segmentStarted := time.Now()
	activeElapsed := func() time.Duration {
		return time.Duration(state.ActiveElapsedNS) + time.Since(segmentStarted)
	}
	commit := func(eventType string, data any) {
		now := time.Now()
		state.ActiveElapsedNS += now.Sub(segmentStarted).Nanoseconds()
		segmentStarted = now
		require.NoError(t, evidence.Commit(eventType, time.Duration(state.ActiveElapsedNS), data, state))
	}

	for {
		now := time.Now().UTC()
		elapsed := activeElapsed()
		if state.Next < len(schedule) && schedule[state.Next].offset <= elapsed {
			end := state.Next + 1
			for end < len(schedule) && schedule[end].offset <= elapsed {
				end++
			}
			require.NoError(t, putRuntimeSlotSoakProofs(journal, schedule[state.Next:end], completedAt))
			pruned, pruneErr := journal.Prune(now)
			require.NoError(t, pruneErr)
			require.Equal(t, end-state.Next, pruned)
			state.Deleted += pruned
			state.Next = end
			require.NoError(t, journal.db.Sync())
			if state.WarmSize < 0 && state.Next >= warmAt {
				state.WarmSize = runtimeSlotSoakFileSize(t, path)
				commit("warm", runtimeSlotSoakSnapshot{
					Inserted: state.Next, Deleted: state.Deleted, FileBytes: state.WarmSize,
					Bolt: runtimeSlotSoakBoltStats(journal.db.Stats()),
				})
			} else {
				commit("progress", runtimeSlotSoakSnapshot{
					Inserted: state.Next, Deleted: state.Deleted, FileBytes: runtimeSlotSoakFileSize(t, path),
					Bolt: runtimeSlotSoakBoltStats(journal.db.Stats()),
				})
			}
		}

		if !state.Restarted && elapsed >= duration/3 {
			require.NoError(t, journal.Close())
			journalClosed = true
			journal, err = newRuntimeSlotJournal(path, time.Hour)
			require.NoError(t, err)
			journalClosed = false
			require.NoError(t, journal.Ping())
			require.NoError(t, ensureRuntimeSlotSoakIdentity(journal, evidence.RunID(), evidence.ConfigSHA256(), false))
			state.Restarted = true
			commit("restarted", runtimeSlotSoakSnapshot{
				Inserted: state.Next, Deleted: state.Deleted, FileBytes: runtimeSlotSoakFileSize(t, path),
				Bolt: runtimeSlotSoakBoltStats(journal.db.Stats()),
			})
		}

		if elapsed >= nextSample {
			commit("sample", runtimeSlotSoakSnapshot{
				Inserted: state.Next, Deleted: state.Deleted, FileBytes: runtimeSlotSoakFileSize(t, path),
				Bolt: runtimeSlotSoakBoltStats(journal.db.Stats()),
			})
			for elapsed >= nextSample {
				nextSample += time.Minute
			}
		}

		if elapsed >= nextCheckpoint {
			commit("checkpoint", runtimeSlotSoakSnapshot{
				Inserted: state.Next, Deleted: state.Deleted, FileBytes: runtimeSlotSoakFileSize(t, path),
				Bolt: runtimeSlotSoakBoltStats(journal.db.Stats()),
			})
			interval := runtimeSlotSoakCheckpointInterval(duration)
			for elapsed >= nextCheckpoint {
				nextCheckpoint += interval
			}
		}

		if elapsed >= duration {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	require.Equal(t, proofs, state.Next)
	require.Equal(t, proofs, state.Deleted)
	require.True(t, state.Restarted)
	require.GreaterOrEqual(t, activeElapsed(), duration)
	require.NoError(t, journal.db.Sync())
	finalSize := runtimeSlotSoakFileSize(t, path)
	require.Greater(t, state.WarmSize, int64(0))
	require.LessOrEqual(t, finalSize, state.WarmSize+int64(os.Getpagesize()),
		"24-hour terminal churn must reuse Bolt freelist pages")
	state.Phase = "complete"
	commit("final", map[string]any{
		"passed": true, "inserted": state.Next, "deleted": state.Deleted,
		"warm_file_bytes": state.WarmSize, "final_file_bytes": finalSize,
		"growth_bytes": finalSize - state.WarmSize,
		"bolt":         runtimeSlotSoakBoltStats(journal.db.Stats()),
	})
}

type runtimeSlotSoakIdentity struct {
	Version      int    `json:"version"`
	RunID        string `json:"run_id"`
	ConfigSHA256 string `json:"config_sha256"`
}

func ensureRuntimeSlotSoakIdentity(
	journal *runtimeSlotJournal,
	runID string,
	configSHA256 string,
	allowInitialize bool,
) error {
	return journal.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(runtimeSlotSoakIdentityBucket)
		if bucket == nil {
			if !allowInitialize {
				return fmt.Errorf("runtime-slot soak identity is absent")
			}
			production := tx.Bucket(runtimeSlotJournalBucket)
			if production == nil {
				return fmt.Errorf("runtime-slot production journal bucket is absent")
			}
			cursor := production.Cursor()
			if key, _ := cursor.First(); key != nil {
				return fmt.Errorf("unbound runtime-slot journal is not empty")
			}
			var err error
			bucket, err = tx.CreateBucket(runtimeSlotSoakIdentityBucket)
			if err != nil {
				return err
			}
			payload, err := json.Marshal(runtimeSlotSoakIdentity{
				Version: runtimeSlotSoakStateVersion, RunID: runID, ConfigSHA256: configSHA256,
			})
			if err != nil {
				return err
			}
			return bucket.Put([]byte("identity"), payload)
		}
		var identity runtimeSlotSoakIdentity
		if err := json.Unmarshal(bucket.Get([]byte("identity")), &identity); err != nil {
			return fmt.Errorf("decode runtime-slot soak identity: %w", err)
		}
		if identity != (runtimeSlotSoakIdentity{
			Version: runtimeSlotSoakStateVersion, RunID: runID, ConfigSHA256: configSHA256,
		}) {
			return fmt.Errorf("runtime-slot soak identity changed")
		}
		return nil
	})
}

func validateRuntimeSlotSoakCheckpoint(
	state runtimeSlotSoakCheckpoint,
	proofs int,
	duration time.Duration,
) error {
	if state.Version != runtimeSlotSoakStateVersion ||
		(state.Phase != "active" && state.Phase != "complete") {
		return fmt.Errorf("runtime-slot soak checkpoint version or phase is invalid")
	}
	if state.ActiveElapsedNS < 0 || state.Next < 0 || state.Next > proofs ||
		state.Deleted != state.Next || state.WarmSize < -1 {
		return fmt.Errorf("runtime-slot soak checkpoint progress is invalid")
	}
	if state.Phase == "complete" && (state.Next != proofs || !state.Restarted ||
		time.Duration(state.ActiveElapsedNS) < duration || state.WarmSize <= 0) {
		return fmt.Errorf("completed runtime-slot soak checkpoint is incomplete")
	}
	return nil
}

func runtimeSlotSoakStateDir(t *testing.T, raw string) string {
	t.Helper()
	path := filepath.Clean(strings.TrimSpace(raw))
	require.True(t, filepath.IsAbs(path))
	require.NotEqual(t, string(filepath.Separator), path)
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		require.NoError(t, os.MkdirAll(path, 0o700))
		info, err = os.Lstat(path)
	}
	require.NoError(t, err)
	require.Zero(t, info.Mode()&os.ModeSymlink)
	require.True(t, info.IsDir())
	require.NoError(t, os.Chmod(path, 0o700))
	return path
}

func runtimeSlotSoakCheckpointInterval(duration time.Duration) time.Duration {
	interval := min(5*time.Second, duration/20)
	return max(interval, 100*time.Millisecond)
}

func nextRuntimeSlotSoakBoundary(elapsed, interval time.Duration) time.Duration {
	return (elapsed/interval + 1) * interval
}

func envOrRuntimeSlotSoak(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}

type runtimeSlotSoakItem struct {
	offset time.Duration
	index  int
}

func buildRuntimeSlotSoakSchedule(total int, duration time.Duration, bursts int) []runtimeSlotSoakItem {
	window := duration - time.Second
	if window < duration/2 {
		window = duration * 3 / 4
	}
	burstTotal := total / 2
	baselineTotal := total - burstTotal
	result := make([]runtimeSlotSoakItem, 0, total)
	nextIndex := 0
	for index := 0; index < baselineTotal; index++ {
		offset := time.Duration(0)
		if baselineTotal > 1 {
			offset = time.Duration(int64(window) * int64(index) / int64(baselineTotal-1))
		}
		result = append(result, runtimeSlotSoakItem{offset: offset, index: nextIndex})
		nextIndex++
	}
	remaining := burstTotal
	for burst := 0; burst < bursts; burst++ {
		size := remaining / (bursts - burst)
		remaining -= size
		offset := time.Duration(int64(window) * int64(burst+1) / int64(bursts+1))
		for range size {
			result = append(result, runtimeSlotSoakItem{offset: offset, index: nextIndex})
			nextIndex++
		}
	}
	sort.Slice(result, func(left, right int) bool {
		if result[left].offset == result[right].offset {
			return result[left].index < result[right].index
		}
		return result[left].offset < result[right].offset
	})
	return result
}

func putRuntimeSlotSoakProofs(
	journal *runtimeSlotJournal,
	items []runtimeSlotSoakItem,
	completedAt string,
) error {
	return journal.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(runtimeSlotJournalBucket)
		for _, item := range items {
			slotID := fmt.Sprintf("soak-slot-%05d", item.index)
			containerID := protocol.NomadRunscContainerID(slotID)
			registration := RuntimeSlotRegistration{
				Version: RuntimeSlotJournalVersion, SlotID: slotID, ClusterID: "soak-cluster",
				AllocationID: "allocation-" + slotID, NodeID: "soak-node", NodeBootID: "soak-boot",
				NetNSPath: "/var/run/netns/" + slotID, NetNSIdentity: "netns-" + slotID,
				NetworkChain: networkChainName(containerID), RunscContainerID: containerID,
				StableMount: "/opt/nomad/" + slotID + "/rootfs", StableMountID: "mount-" + slotID,
				MountNamespaceID: "mnt:[1]",
			}
			request := runtimeSlotSoakCleanup(registration)
			proof := testRuntimeSlotJournalProofWithoutTesting(request)
			record := runtimeSlotJournalRecord{
				Version: RuntimeSlotJournalVersion, Registration: registration,
				Cleanup: &request, Proof: &proof, CreatedAt: completedAt,
				UpdatedAt: completedAt, CompletedAt: completedAt,
			}
			if err := putRuntimeSlotJournalRecord(bucket, record); err != nil {
				return err
			}
		}
		return nil
	})
}

func runtimeSlotSoakCleanup(registration RuntimeSlotRegistration) protocol.NodeCleanupControlRequest {
	return protocol.NodeCleanupControlRequest{
		OperationID: "cleanup-" + registration.SlotID, SlotID: registration.SlotID,
		ClusterID: registration.ClusterID, AllocationID: registration.AllocationID,
		NodeID: registration.NodeID, NodeUID: "soak-node-uid", NodeBootID: registration.NodeBootID,
		NetNSIdentity: registration.NetNSIdentity, RunscContainerID: registration.RunscContainerID,
	}
}

func testRuntimeSlotJournalProofWithoutTesting(
	request protocol.NodeCleanupControlRequest,
) protocol.NodeCleanupControlProof {
	proof := protocol.NodeCleanupControlProof{
		Version: protocol.NodeCleanupProofVersion, OperationID: request.OperationID,
		SlotID: request.SlotID, ClusterID: request.ClusterID,
		AllocationID: request.AllocationID, NodeID: request.NodeID,
		NodeUID: request.NodeUID, NodeBootID: request.NodeBootID,
		NetNSIdentity: request.NetNSIdentity, RunscContainerID: request.RunscContainerID,
		RunscAbsent: true, StableMountAbsent: true, RootFSWriterAbsent: true,
		NetworkPolicyAbsent: true,
	}
	digestValue, err := proof.Digest()
	if err != nil {
		panic(err)
	}
	proof.ProofDigest = digestValue
	if err := proof.Validate(); err != nil {
		panic(err)
	}
	return proof
}

type runtimeSlotSoakSnapshot struct {
	Inserted  int                           `json:"inserted"`
	Deleted   int                           `json:"deleted"`
	FileBytes int64                         `json:"file_bytes"`
	Bolt      runtimeSlotSoakBoltStatsValue `json:"bolt"`
}

type runtimeSlotSoakBoltStatsValue struct {
	FreePageN     int `json:"free_pages"`
	PendingPageN  int `json:"pending_pages"`
	FreeAlloc     int `json:"free_alloc_bytes"`
	FreelistInuse int `json:"freelist_inuse_bytes"`
	TxN           int `json:"transactions"`
	OpenTxN       int `json:"open_transactions"`
}

func runtimeSlotSoakBoltStats(stats bolt.Stats) runtimeSlotSoakBoltStatsValue {
	return runtimeSlotSoakBoltStatsValue{
		FreePageN: stats.FreePageN, PendingPageN: stats.PendingPageN,
		FreeAlloc: stats.FreeAlloc, FreelistInuse: stats.FreelistInuse,
		TxN: stats.TxN, OpenTxN: stats.OpenTxN,
	}
}

func runtimeSlotSoakFileSize(t *testing.T, path string) int64 {
	t.Helper()
	info, err := os.Stat(path)
	require.NoError(t, err)
	return info.Size()
}

func runtimeSlotSoakIntEnv(t *testing.T, key string, fallback int) int {
	t.Helper()
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	require.NoError(t, err)
	return parsed
}

func TestBuildRuntimeSlotSoakSchedule(t *testing.T) {
	schedule := buildRuntimeSlotSoakSchedule(10_000, 24*time.Hour, 20)
	require.Len(t, schedule, 10_000)
	seen := make(map[int]struct{}, len(schedule))
	duplicates := 0
	for index, item := range schedule {
		require.GreaterOrEqual(t, item.offset, time.Duration(0))
		require.Less(t, item.offset, 24*time.Hour)
		if index > 0 {
			require.LessOrEqual(t, schedule[index-1].offset, item.offset)
			if schedule[index-1].offset == item.offset {
				duplicates++
			}
		}
		_, found := seen[item.index]
		require.False(t, found)
		seen[item.index] = struct{}{}
	}
	require.Greater(t, duplicates, 4_000)
}

func TestRuntimeSlotSoakIdentityBindsJournalToEvidence(t *testing.T) {
	journal, err := newRuntimeSlotJournal(filepath.Join(t.TempDir(), "runtime-slots.db"), time.Hour)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, journal.Close()) })
	require.NoError(t, ensureRuntimeSlotSoakIdentity(journal, "run-a", "config-a", true))
	require.NoError(t, ensureRuntimeSlotSoakIdentity(journal, "run-a", "config-a", false))
	require.ErrorContains(t, ensureRuntimeSlotSoakIdentity(journal, "run-b", "config-a", false), "changed")
	require.ErrorContains(t, ensureRuntimeSlotSoakIdentity(journal, "run-a", "config-b", false), "changed")
}

func TestValidateRuntimeSlotSoakCheckpointRejectsIncompleteTerminalState(t *testing.T) {
	state := runtimeSlotSoakCheckpoint{
		Version: runtimeSlotSoakStateVersion, Phase: "active", WarmSize: -1,
		Next: 10, Deleted: 10,
	}
	require.NoError(t, validateRuntimeSlotSoakCheckpoint(state, 100, 10*time.Second))
	state.Phase = "complete"
	require.ErrorContains(t, validateRuntimeSlotSoakCheckpoint(state, 100, 10*time.Second), "incomplete")
	state.Next = 100
	state.Deleted = 100
	state.Restarted = true
	state.WarmSize = 4096
	state.ActiveElapsedNS = int64(10 * time.Second)
	require.NoError(t, validateRuntimeSlotSoakCheckpoint(state, 100, 10*time.Second))
}
