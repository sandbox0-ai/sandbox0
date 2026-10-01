//go:build linux

package driver

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/opencontainers/go-digest"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
	"github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

type liveNomadObjectPublisher string

func (s liveNomadObjectPublisher) PutImmutable(_ context.Context, key string, data []byte) error {
	path := filepath.Join(string(s), key)
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return err
	}
	return os.WriteFile(path, data, 0600)
}

// The external isolated fixture supplies real Nomad, ctld, stock runsc and
// kernel NBD. This helper submits a real driver claim, never edits its state.
// Regional authority and S3 are synthetic peers; their lease/fence semantics
// have separate PostgreSQL and protocol tests.
func TestLiveNomadClaimFixture(t *testing.T) {
	root := os.Getenv("S0_NOMAD_LIVE_FIXTURE_ROOT")
	if root == "" {
		t.Skip("requires an isolated complete node fixture")
	}
	require.Equal(t, "/opt/s0-live-update-test", root)
	_, err := os.Stat(filepath.Join(root, "isolated-fixture"))
	require.NoError(t, err)
	require.Zero(t, os.Geteuid())
	data, err := os.ReadFile(filepath.Join(root, "warm-state.json"))
	require.NoError(t, err)
	var state PersistedState
	require.NoError(t, json.Unmarshal(data, &state))
	require.Equal(t, phaseWarm, state.Phase)
	source := t.TempDir()
	stage, token, networkPolicy := newAuthorizedRootFSStage(t, source)
	nonce := sha256.Sum256([]byte(state.TaskConfig.ID))
	suffix := hex.EncodeToString(nonce[:])
	stage.Parent = "sha256:" + suffix
	stage.Identity.RootFSID = "fixture-" + suffix
	stage.Identity.WriterGrantID = "grant-" + suffix
	stage.Generation.FilesystemID = stage.Identity.RootFSID
	stage.Identity.AllocationID = state.TaskConfig.AllocID
	stage.Identity.TaskName = state.TaskConfig.Name
	stage.Identity.SlotNonce = state.TaskConfig.ID
	stage.Identity.NodeUID = "fixture-node-1"
	boot, err := os.ReadFile("/proc/sys/kernel/random/boot_id")
	require.NoError(t, err)
	stage.Identity.BootID = strings.TrimSpace(string(boot))
	stage.ExpectedPolicyToken.AllocationID = state.TaskConfig.AllocID
	stage.ExpectedPolicyToken.NetNSIdentity, err = networkNamespaceIdentity(state.TaskConfig.NetworkIsolation.Path)
	require.NoError(t, err)
	ip, _, err := nomadProcdEndpoint(state.TaskConfig)
	require.NoError(t, err)
	stage.ExpectedPolicyToken.SourceIP = ip
	assignment := runtimeSlotAssignment()
	revision, err := assignment.Revision()
	require.NoError(t, err)
	resources, err := runtimeslot.NewRuntimeResourceLease("operation-1", "claim-1", state.TaskConfig.ID, "fixture-cluster",
		state.TaskConfig.NodeID, stage.Identity.NodeUID, stage.Identity.BootID,
		runtimeslot.RuntimeResourceRequest{Version: runtimeslot.RuntimeResourceRequestVersion, CPUMillicores: 250, MemoryBytes: 256 << 20, PIDsLimit: runtimeslot.DefaultRuntimePIDsLimit}, "0-3", "0")
	require.NoError(t, err)
	resourceDigest, err := resources.Digest()
	require.NoError(t, err)
	stage.Labels = map[string]string{runtimeslot.RuntimeAssignmentRevisionLabel: revision, runtimeslot.RuntimeResourceLeaseDigestLabel: resourceDigest}
	image, err := os.Create(filepath.Join(source, "real.xfs"))
	require.NoError(t, err)
	defer image.Close()
	require.NoError(t, image.Truncate(512<<20))
	output, err := exec.Command("mkfs.xfs", "-f", image.Name()).CombinedOutput()
	require.NoError(t, err, "%s", output)
	mount := filepath.Join(source, "mount")
	require.NoError(t, os.Mkdir(mount, 0700))
	output, err = exec.Command("mount", "-o", "loop,nouuid", image.Name(), mount).CombinedOutput()
	require.NoError(t, err, "%s", output)
	for _, directory := range []string{"lower", "upper", "work", "lower/proc", "lower/dev", "lower/tmp", "lower/sys"} {
		require.NoError(t, os.MkdirAll(filepath.Join(mount, directory), 0755))
	}
	executable, err := os.Executable()
	require.NoError(t, err)
	payload, err := os.ReadFile(executable)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(mount, "lower/payload"), payload, 0755))
	require.NoError(t, os.WriteFile(filepath.Join(mount, "lower/live-fixture"), []byte("isolated-fixture"), 0600))
	procdPath, err := exec.Command(filepath.Join(root, "f/bin/ctld"), "--install-procd", "--procd-cache-dir="+filepath.Join(root, "f/procd")).Output()
	require.NoError(t, err)
	procd, err := os.ReadFile(strings.TrimSpace(string(procdPath)))
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(mount, "lower/procd"), procd, 0755))
	output, err = exec.Command("umount", mount).CombinedOutput()
	require.NoError(t, err, "%s", output)
	built, err := rootfsblock.BuildMaterializedGeneration(t.Context(), image, 512<<20, liveNomadObjectPublisher(filepath.Join(root, "f/objects")), rootfsblock.BuildOptions{})
	require.NoError(t, err)
	stage.Generation.BaseBlockRoot, stage.Generation.CurrentBlockHead = built.Descriptor.MappingRoot.RootDigest, built.Descriptor.MappingRoot.RootDigest
	stage.Generation.Descriptor = built.Payload
	stage.Generation.BaseArtifactDigest = digest.FromBytes(built.Payload).String()
	stage.Generation.GenerationID = "base-" + stage.Generation.BaseArtifactDigest
	stage.InitialGeneration = stage.Generation.GenerationID
	require.NoError(t, stage.Validate())
	require.NoError(t, os.WriteFile(filepath.Join(root, "f/claim-slot"), []byte(state.TaskConfig.ID), 0600))
	request := ClaimRequest{OperationID: "operation-1", ClaimID: "claim-1", PolicyToken: token, WriterEpoch: "1", Stage: &stage, NetworkPolicy: networkPolicy, Runtime: assignment, Resources: resources}
	data, err = json.Marshal(request)
	require.NoError(t, err)
	control := controlSocketPath("/run/sandbox0/nomad-slots", state.TaskConfig.ID)
	httpRequest, err := http.NewRequestWithContext(t.Context(), http.MethodPut, "http://driver/claim", bytes.NewReader(data))
	require.NoError(t, err)
	response, err := unixHTTPClient(control).Do(httpRequest)
	require.NoError(t, err)
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, response.StatusCode, "%s", body)
	log, err := os.OpenFile(filepath.Join(root, "guest-payload.log"), os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
	require.NoError(t, err)
	defer log.Close()
	command := exec.Command("/usr/local/bin/runsc", "--root=/run/sandbox0/runsc", "exec", "--detach", state.ContainerID,
		"/payload", "-test.run=^TestLiveNomadGuestPayload$", "-test.timeout=30m")
	command.Stdout, command.Stderr = log, log
	require.NoError(t, command.Run())
	t.Logf("real driver claim launched original guest %s; continuous payload started once", state.ContainerID)
}

func TestLiveNomadGuestPayload(t *testing.T) {
	marker, err := os.ReadFile("/live-fixture")
	if err != nil || string(marker) != "isolated-fixture" || os.Getpid() == 1 {
		t.Skip("isolated stock-runsc exec payload")
	}
	var token [32]byte
	_, err = rand.Read(token[:])
	require.NoError(t, err)
	memory := make([]byte, 4<<20)
	_, err = rand.Read(memory)
	require.NoError(t, err)
	digest := sha256.Sum256(memory)
	tokenText := hex.EncodeToString(token[:])
	require.NoError(t, os.WriteFile("/tmp/live-token", []byte(tokenText), 0600))
	file, err := os.OpenFile("/unlinked", os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
	require.NoError(t, err)
	require.NoError(t, os.Remove("/unlinked"))
	mapping, err := os.OpenFile("/mapped", os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
	require.NoError(t, err)
	require.NoError(t, mapping.Truncate(4096))
	area, err := unix.Mmap(int(mapping.Fd()), 0, 4096, unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
	require.NoError(t, err)
	for count := uint64(1); ; count++ {
		require.Equal(t, digest, sha256.Sum256(memory))
		actual, err := os.ReadFile("/tmp/live-token")
		require.NoError(t, err)
		require.Equal(t, tokenText, string(actual))
		_, err = fmt.Fprintln(file, count)
		require.NoError(t, err)
		require.NoError(t, file.Sync())
		offset, err := file.Seek(0, io.SeekCurrent)
		require.NoError(t, err)
		binary.LittleEndian.PutUint64(area, count)
		require.NoError(t, unix.Msync(area, unix.MS_SYNC))
		data, err := json.Marshal(map[string]any{"pid": os.Getpid(), "token": tokenText, "heap": hex.EncodeToString(digest[:]), "counter": count, "offset": offset, "mmap": binary.LittleEndian.Uint64(area)})
		require.NoError(t, err)
		require.NoError(t, os.WriteFile("/guest-state.tmp", data, 0600))
		require.NoError(t, os.Rename("/guest-state.tmp", "/guest-state.json"))
		time.Sleep(20 * time.Millisecond)
	}
}
