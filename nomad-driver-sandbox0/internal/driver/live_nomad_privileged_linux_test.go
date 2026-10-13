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
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/opencontainers/go-digest"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshandoff"
	"github.com/sandbox0-ai/sandbox0/pkg/runtimecontrol"
	"github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	"github.com/sandbox0-ai/sandbox0/pkg/sandboxspec"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

type liveNomadObjectPublisher string

func liveNomadGrowthFixtureMiB(t *testing.T) int {
	t.Helper()
	mib := 600
	if value := os.Getenv("S0_NOMAD_LIVE_GROWTH_MIB"); value != "" {
		var err error
		mib, err = strconv.Atoi(value)
		require.NoError(t, err)
	}
	require.GreaterOrEqual(t, mib, 600)
	require.LessOrEqual(t, mib, 4096)
	return mib
}

func liveNomadGrowthFixtureMarker(t *testing.T) string {
	t.Helper()
	return fmt.Sprintf("isolated-growth-fixture:%d", liveNomadGrowthFixtureMiB(t))
}

func (s liveNomadObjectPublisher) PutImmutable(_ context.Context, key string, data []byte) error {
	path := filepath.Join(string(s), key)
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return err
	}
	return os.WriteFile(path, data, 0600)
}

// The external isolated fixture supplies real Nomad, ctld, stock runsc and
// kernel NBD. This helper submits a real driver claim, never edits its state.
// The default uses synthetic regional authority and S3 peers. The explicit
// real-rootfs mode uses production mTLS handlers, PostgreSQL and real S3.
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
	realRegion := os.Getenv("S0_NOMAD_LIVE_REAL_ROOTFS") == "1"
	source := t.TempDir()
	stage, token, networkPolicy := newAuthorizedRootFSStage(t, source)
	if realRegion {
		writerToken := make([]byte, 32)
		_, err := rand.Read(writerToken)
		require.NoError(t, err)
		stage.Identity.WriterGrantToken = hex.EncodeToString(writerToken)
		stage.Identity.WriterGrantTokenDigest = rootfshandoff.WriterGrantTokenDigest(stage.Identity.WriterGrantToken)
		token = stage.Identity.WriterGrantToken
	}
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
	if realRegion {
		assignment.SandboxID, assignment.TeamID = stage.Identity.RootFSID, "live-nomad-team"
		assignment.EnvVars[runtimecontrol.EnvSandboxID] = assignment.SandboxID
		networkPolicy, err = sandboxspec.NetworkPolicyToAnnotation(&sandboxspec.NetworkPolicySpec{
			Version: "v1", SandboxID: assignment.SandboxID, TeamID: assignment.TeamID, Mode: sandboxspec.NetworkModeAllowAll,
		})
		require.NoError(t, err)
		stage.ExpectedPolicyToken.PolicyDigest = runtimeslot.NetworkPolicyDigest(networkPolicy)
	}
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
	initialBytes := int64(512 << 20)
	if realRegion {
		initialBytes = 300 << 20
	}
	require.NoError(t, image.Truncate(initialBytes))
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
	if realRegion {
		require.NoError(t, os.WriteFile(filepath.Join(mount, "lower/live-growth"), []byte(liveNomadGrowthFixtureMarker(t)), 0600))
	}
	procdPath, err := exec.Command(filepath.Join(root, "f/bin/ctld"), "--install-procd", "--procd-cache-dir="+filepath.Join(root, "f/procd")).Output()
	require.NoError(t, err)
	procd, err := os.ReadFile(strings.TrimSpace(string(procdPath)))
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(mount, "lower/procd"), procd, 0755))
	output, err = exec.Command("umount", mount).CombinedOutput()
	require.NoError(t, err, "%s", output)
	if realRegion {
		artifact := liveNomadImportBase(t, image, procd, suffix)
		liveNomadPrepareRegionalClaim(t, &stage, artifact, resources, assignment, networkPolicy)
		if os.Getenv("S0_NOMAD_LIVE_PUBLIC_API") == "1" {
			// The public lifecycle gate needs a distinct attested rebase target,
			// not a no-op rebase onto the source artifact.
			target := liveNomadImportBase(t, image, procd, suffix+"-public-rebase")
			require.NoError(t, os.WriteFile(filepath.Join(root, "f/public-rebase-artifact"), []byte(target.ArtifactDigest), 0600))
			config := defaultPluginConfig()
			config.DirectFS = false // The isolated Nomad agent explicitly selects this.
			version, err := NewCommandRunsc(*config).Version(t.Context())
			require.NoError(t, err)
			compatibility := runtimeslot.RuntimeCompatibility{
				Version: runtimeslot.RuntimeCompatibilityVersion, Architecture: runtime.GOARCH,
				DriverVersion: PluginVersion, RunscVersion: strings.TrimSpace(version),
				Platform: config.Platform, Overlay2: config.Overlay2, FileAccess: config.FileAccess,
				DirectFS: config.DirectFS, Command: "/procd", ProcdPort: runtimeslot.NomadProcdPort,
				RuntimeMode: runtimecontrol.ControlModeStatic, SecurityClass: state.DriverConfig.SecurityClass,
			}
			slot, err := sandboxstore.NewPGSandboxStore(liveNomadRegionalPool(t)).GetRuntimeSlot(t.Context(), stage.Identity.SlotNonce)
			require.NoError(t, err)
			classDigest, err := compatibility.Digest()
			require.NoError(t, err)
			require.Equal(t, slot.CompatibilityDigest, classDigest, "fixture class must match the actual registered carrier")
			catalog, err := json.Marshal(map[string]any{"version": 3, "classes": []any{map[string]any{
				"name": "fixture-privileged", "cluster_id": slot.ClusterID,
				"artifact_platform": map[string]string{"os": "linux", "architecture": runtime.GOARCH}, "compatibility": compatibility,
			}}})
			require.NoError(t, err)
			require.NoError(t, os.WriteFile(filepath.Join(root, "f/public-runtime-classes.json"), catalog, 0600))
		}
	} else {
		built, err := rootfsblock.BuildMaterializedGeneration(t.Context(), image, initialBytes, liveNomadObjectPublisher(filepath.Join(root, "f/objects")), rootfsblock.BuildOptions{})
		require.NoError(t, err)
		stage.Generation.BaseBlockRoot, stage.Generation.CurrentBlockHead = built.Descriptor.MappingRoot.RootDigest, built.Descriptor.MappingRoot.RootDigest
		stage.Generation.Descriptor = built.Payload
		stage.Generation.BaseArtifactDigest = digest.FromBytes(built.Payload).String()
		stage.Generation.GenerationID = "base-" + stage.Generation.BaseArtifactDigest
		stage.InitialGeneration = stage.Generation.GenerationID
	}
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
	if realRegion {
		liveNomadCommitCommandReady(t, state, stage)
		liveNomadCompleteRegionalClaim(t, stage)
		if os.Getenv("S0_NOMAD_LIVE_MEMORY") == "1" {
			active, err := readPersistedState(filepath.Join(state.BundleDir, ".sandbox0-driver-state.json"))
			require.NoError(t, err)
			require.NotNil(t, active.Claim)
			require.NotNil(t, active.Claim.MigrationCPULaunch, "memory fixture requires native CPU launch evidence from the canonical stock bundle")
			_, err = active.Claim.MigrationCPULaunch.Digest()
			require.NoError(t, err)
		}
		metadata, err := json.Marshal(stage.WithoutWriterGrantToken())
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(filepath.Join(root, "f/regional-stage.json"), metadata, 0600))
		liveNomadStartManagedPayload(t, state)
		t.Logf("real driver claim launched original guest %s; managed procd payload started once", state.ContainerID)
		return
	}
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
	growthMarker, growthErr := os.ReadFile("/live-growth")
	var growing *os.File
	var chunk []byte
	var hashes [][sha256.Size]byte
	var growthTarget int64
	growthBytes := int64(0)
	verified := false
	verificationCount := uint64(0)
	var lastVerification time.Time
	if growthErr == nil {
		prefix, size, found := strings.Cut(string(growthMarker), ":")
		require.Equal(t, "isolated-growth-fixture", prefix)
		mib := 600 // Keep historical fixture images readable.
		if found {
			mib, err = strconv.Atoi(size)
			require.NoError(t, err)
		}
		require.GreaterOrEqual(t, mib, 600)
		require.LessOrEqual(t, mib, 4096)
		growthTarget = int64(mib) << 20
		hashes = make([][sha256.Size]byte, mib)
		growing, err = os.OpenFile("/growing.bin", os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
		require.NoError(t, err)
		defer growing.Close()
		chunk = make([]byte, 1<<20)
	}
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
		if growing != nil && growthBytes < growthTarget {
			_, err := rand.Read(chunk)
			require.NoError(t, err)
			hashes[growthBytes>>20] = sha256.Sum256(chunk)
			written, err := growing.Write(chunk)
			require.NoError(t, err)
			require.Equal(t, len(chunk), written)
			require.NoError(t, growing.Sync())
			growthBytes += int64(written)
		}
		if growing != nil && growthBytes == growthTarget && (!verified || time.Since(lastVerification) >= 15*time.Second) {
			for index, expected := range hashes {
				_, err := growing.ReadAt(chunk, int64(index)<<20)
				require.NoError(t, err)
				require.Equal(t, expected, sha256.Sum256(chunk), "growth chunk=%d", index)
			}
			verified = true
			verificationCount++
			lastVerification = time.Now()
		}
		if growing != nil {
			position, err := growing.Seek(0, io.SeekCurrent)
			require.NoError(t, err)
			require.Equal(t, growthBytes, position)
		}
		data, err := json.Marshal(map[string]any{"pid": os.Getpid(), "token": tokenText, "heap": hex.EncodeToString(digest[:]), "counter": count, "offset": offset, "mmap": binary.LittleEndian.Uint64(area), "growth_bytes": growthBytes, "growth_verified": verified, "growth_verifications": verificationCount})
		require.NoError(t, err)
		require.NoError(t, os.WriteFile("/guest-state.tmp", data, 0600))
		require.NoError(t, os.Rename("/guest-state.tmp", "/guest-state.json"))
		time.Sleep(20 * time.Millisecond)
	}
}
