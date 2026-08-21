package nomadruntime

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"
)

const (
	defaultRuntimeSlotSoakDuration = 24 * time.Hour
	defaultRuntimeSlotSoakProofs   = 10_000
	defaultRuntimeSlotSoakBursts   = 20
)

// TestRuntimeSlotJournalTwentyFourHourSoak is the opt-in wall-clock Bolt
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

	evidence, err := newRuntimeSlotSoakEvidence(outputPath)
	require.NoError(t, err)
	defer func() { require.NoError(t, evidence.Close()) }()
	startedAt := time.Now().UTC()
	evidence.startedAt = startedAt
	require.NoError(t, evidence.Write("configuration", map[string]any{
		"duration": duration.String(), "proofs": proofs, "bursts": bursts,
		"terminal_ttl": time.Hour.String(),
	}))

	root := t.TempDir()
	path := filepath.Join(root, "runtime-slots.db")
	journal, err := newRuntimeSlotJournal(path, time.Hour)
	require.NoError(t, err)
	journalClosed := false
	defer func() {
		if !journalClosed {
			require.NoError(t, journal.Close())
		}
	}()

	schedule := buildRuntimeSlotSoakSchedule(proofs, duration, bursts)
	require.Len(t, schedule, proofs)
	completedAt := startedAt.Add(-2 * time.Hour).Format(time.RFC3339Nano)
	next := 0
	deleted := 0
	restarted := false
	warmAt := min(max(proofs/10, 100), proofs)
	var warmSize int64 = -1
	nextSample := startedAt.Add(time.Minute)
	deadline := startedAt.Add(duration)

	for {
		now := time.Now().UTC()
		elapsed := now.Sub(startedAt)
		if next < len(schedule) && schedule[next].offset <= elapsed {
			end := next + 1
			for end < len(schedule) && schedule[end].offset <= elapsed {
				end++
			}
			require.NoError(t, putRuntimeSlotSoakProofs(journal, schedule[next:end], completedAt))
			pruned, pruneErr := journal.Prune(now)
			require.NoError(t, pruneErr)
			require.Equal(t, end-next, pruned)
			deleted += pruned
			next = end
			require.NoError(t, journal.db.Sync())
			if warmSize < 0 && next >= warmAt {
				warmSize = runtimeSlotSoakFileSize(t, path)
				require.NoError(t, evidence.Write("warm", runtimeSlotSoakSnapshot{
					Inserted: next, Deleted: deleted, FileBytes: warmSize,
					Bolt: runtimeSlotSoakBoltStats(journal.db.Stats()),
				}))
			}
		}

		if !restarted && elapsed >= duration/3 {
			require.NoError(t, journal.Close())
			journalClosed = true
			journal, err = newRuntimeSlotJournal(path, time.Hour)
			require.NoError(t, err)
			journalClosed = false
			require.NoError(t, journal.Ping())
			restarted = true
			require.NoError(t, evidence.Write("restarted", runtimeSlotSoakSnapshot{
				Inserted: next, Deleted: deleted, FileBytes: runtimeSlotSoakFileSize(t, path),
				Bolt: runtimeSlotSoakBoltStats(journal.db.Stats()),
			}))
		}

		if !now.Before(nextSample) {
			require.NoError(t, evidence.Write("sample", runtimeSlotSoakSnapshot{
				Inserted: next, Deleted: deleted, FileBytes: runtimeSlotSoakFileSize(t, path),
				Bolt: runtimeSlotSoakBoltStats(journal.db.Stats()),
			}))
			for !nextSample.After(now) {
				nextSample = nextSample.Add(time.Minute)
			}
		}

		if !now.Before(deadline) {
			break
		}
		wake := time.Now().Add(250 * time.Millisecond)
		for _, candidate := range []time.Time{nextSample, deadline} {
			if candidate.Before(wake) {
				wake = candidate
			}
		}
		if next < len(schedule) {
			candidate := startedAt.Add(schedule[next].offset)
			if candidate.Before(wake) {
				wake = candidate
			}
		}
		time.Sleep(max(time.Until(wake), time.Millisecond))
	}

	require.Equal(t, proofs, next)
	require.Equal(t, proofs, deleted)
	require.True(t, restarted)
	require.GreaterOrEqual(t, time.Since(startedAt), duration)
	require.NoError(t, journal.db.Sync())
	finalSize := runtimeSlotSoakFileSize(t, path)
	require.Greater(t, warmSize, int64(0))
	require.LessOrEqual(t, finalSize, warmSize+int64(os.Getpagesize()),
		"24-hour terminal churn must reuse Bolt freelist pages")
	require.NoError(t, evidence.Write("final", map[string]any{
		"passed": true, "inserted": next, "deleted": deleted,
		"warm_file_bytes": warmSize, "final_file_bytes": finalSize,
		"growth_bytes": finalSize - warmSize,
		"bolt":         runtimeSlotSoakBoltStats(journal.db.Stats()),
	}))
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

type runtimeSlotSoakEvidence struct {
	file      *os.File
	encoder   *json.Encoder
	startedAt time.Time
}

func newRuntimeSlotSoakEvidence(path string) (*runtimeSlotSoakEvidence, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, err
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return nil, err
	}
	return &runtimeSlotSoakEvidence{file: file, encoder: json.NewEncoder(file)}, nil
}

func (w *runtimeSlotSoakEvidence) Write(eventType string, data any) error {
	event := map[string]any{"type": eventType, "at": time.Now().UTC(), "data": data}
	if !w.startedAt.IsZero() {
		event["elapsed_seconds"] = time.Since(w.startedAt).Seconds()
	}
	if err := w.encoder.Encode(event); err != nil {
		return err
	}
	return w.file.Sync()
}

func (w *runtimeSlotSoakEvidence) Close() error {
	return w.file.Close()
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
