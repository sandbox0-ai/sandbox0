//go:build linux

package driver

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"sort"
	"testing"
	"time"

	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"github.com/sandbox0-ai/sandbox0/pkg/objectstore"
	"github.com/sandbox0-ai/sandbox0/pkg/procdapi"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshandoff"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsimporter"
	"github.com/sandbox0-ai/sandbox0/pkg/runtimecontrol"
	"github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	"github.com/stretchr/testify/require"
)

func liveNomadRegionalObjects(t *testing.T) objectstore.ContextConditionalStore {
	t.Helper()
	var config struct {
		Objects struct {
			Type      string `json:"type"`
			Bucket    string `json:"bucket"`
			Region    string `json:"region"`
			Endpoint  string `json:"endpoint"`
			AccessKey string `json:"access_key"`
			SecretKey string `json:"secret_key"`
		} `json:"rootfs_object_storage"`
	}
	data, err := os.ReadFile("/etc/sandbox0/ctld.yaml")
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(data, &config))
	require.Equal(t, "s3", config.Objects.Type)
	require.Equal(t, "http://127.0.0.1:19000", config.Objects.Endpoint)
	require.Contains(t, config.Objects.Bucket, "s0-autogrow-nomad-")
	objects, err := objectstore.Create(objectstore.Config{Type: config.Objects.Type, Bucket: config.Objects.Bucket, Region: config.Objects.Region,
		Endpoint: config.Objects.Endpoint, AccessKey: config.Objects.AccessKey, SecretKey: config.Objects.SecretKey})
	require.NoError(t, err)
	require.NoError(t, objects.Create())
	conditional, ok := objects.(objectstore.ContextConditionalStore)
	require.True(t, ok)
	return conditional
}

// Image contents are the actual embedded procd plus the continuous guest
// payload. OCI identities are fixture provenance; import CAS/custody are real.
func liveNomadImportBase(t *testing.T, image *os.File, procd []byte, suffix string) *sandboxstore.RootFSBaseArtifact {
	t.Helper()
	store := sandboxstore.NewPGSandboxStore(liveNomadRegionalPool(t))
	objects := liveNomadRegionalObjects(t)
	source := digest.FromString("live-nomad-source-" + suffix)
	spec := rootfsimporter.OperationSpec{SourceOCIRef: "fixture.invalid/nomad@" + source.String(),
		Platform:         rootfsimporter.ReadyArtifactPlatform{OS: "linux", Architecture: "amd64"},
		FormatGeneration: 2, ProcdProtocol: "sandbox0.procd.v3", ProcdDigest: digest.FromBytes(procd).String(),
		LogicalSizeBytes: 64 << 40, InitialFilesystemSizeBytes: 300 << 20,
		BlockOptions: rootfsblock.BuildOptions{ObjectPrefix: "rootfs/live-nomad/" + suffix + "/import"}}
	op, spec, err := rootfsimporter.DeterministicOperation(spec)
	require.NoError(t, err)
	_, err = store.BeginRootFSImport(t.Context(), &sandboxstore.BeginRootFSImportRequest{OperationID: op, Spec: spec})
	require.NoError(t, err)
	operation, err := store.LeaseNextRootFSImport(t.Context(), "live-nomad-fixture", 5*time.Minute)
	require.NoError(t, err)
	require.Equal(t, op, operation.ID)
	lease, err := operation.Lease()
	require.NoError(t, err)
	journal, err := sandboxstore.NewRootFSImportPublicationJournal(store, lease)
	require.NoError(t, err)
	publisher := rootfsimporter.JournaledPublisher{OperationID: op, Journal: journal, Publisher: rootfsblock.ObjectStorePublisher{Store: objects}}
	readonly, err := os.Open(image.Name())
	require.NoError(t, err)
	defer readonly.Close()
	base, err := rootfsblock.BuildMaterializedFileGeneration(t.Context(), readonly, spec.InitialFilesystemSizeBytes, publisher, spec.BlockOptions, nil)
	require.NoError(t, err)
	sparse, err := rootfsblock.GrowSparseGeneration(t.Context(), objects, base.Descriptor, spec.LogicalSizeBytes, publisher, spec.BlockOptions)
	require.NoError(t, err)
	refs := make(map[string]rootfsblock.ObjectReference)
	for _, ref := range append(base.References, sparse.References...) {
		refs[ref.Key] = ref
	}
	var references []rootfsblock.ObjectReference
	var total int64
	for _, ref := range refs {
		references = append(references, ref)
		total += ref.Size
	}
	sort.Slice(references, func(i, j int) bool { return references[i].Key < references[j].Key })
	executable, err := os.Executable()
	require.NoError(t, err)
	payload, err := os.Stat(executable)
	require.NoError(t, err)
	result := rootfsimporter.BuildResult{SourceOCIRef: spec.SourceOCIRef, SourceOCIDigest: source,
		ManifestDigest: digest.FromString("live-nomad-manifest-" + suffix), ConfigDigest: digest.FromString("live-nomad-config-" + suffix),
		Platform: ocispec.Platform{OS: "linux", Architecture: "amd64"}, LayerDigests: []digest.Digest{digest.FromString("live-nomad-layer-" + suffix)},
		DiffIDs: []digest.Digest{digest.FromString("live-nomad-diff-" + suffix)}, ProcdDigest: digest.Digest(spec.ProcdDigest), Files: 4,
		UnpackedBytes:    payload.Size() + int64(len(procd)+len("isolated-fixture")+len("isolated-growth-fixture")),
		LogicalSizeBytes: spec.LogicalSizeBytes, InitialFilesystemSizeBytes: spec.InitialFilesystemSizeBytes,
		DescriptorDigest: digest.FromBytes(sparse.Payload), BaseBlockRoot: digest.Digest(sparse.Descriptor.MappingRoot.RootDigest),
		Descriptor: sparse.Descriptor, DescriptorBytes: sparse.Payload, Objects: len(references), Bytes: total, References: references}
	require.NoError(t, result.Validate())
	artifact, err := store.PublishReadyRootFSImport(t.Context(), &sandboxstore.PublishReadyRootFSImportRequest{Lease: lease, Result: result})
	require.NoError(t, err)
	return artifact
}

func liveNomadPrepareRegionalClaim(t *testing.T, stage *rootfshandoff.StageRequest, artifact *sandboxstore.RootFSBaseArtifact, resources runtimeslot.RuntimeResourceLease, assignment *runtimecontrol.Assignment, networkPolicy string) {
	t.Helper()
	revision, err := assignment.Revision()
	require.NoError(t, err)
	payload, err := json.Marshal(assignment)
	require.NoError(t, err)
	store := sandboxstore.NewPGSandboxStore(liveNomadRegionalPool(t))
	slot, err := store.GetRuntimeSlot(t.Context(), stage.Identity.SlotNonce)
	require.NoError(t, err)
	require.Equal(t, sandboxstore.RuntimeSlotStateFastpathReady, slot.State)
	_, err = store.RegisterRuntimeNodeCapacity(t.Context(), &sandboxstore.RegisterRuntimeNodeCapacityRequest{
		ClusterID: slot.ClusterID, NodeID: slot.NodeID, NodeUID: slot.NodeUID, NodeBootID: slot.NodeBootID,
		CPUMillicores: 1000, MemoryBytes: 1 << 30, CPUSetCPUs: "0-3", CPUSetMems: "0", TTL: 5 * time.Minute})
	require.NoError(t, err)
	record := &sandboxstore.SandboxRecord{ID: stage.Identity.RootFSID, TeamID: "live-nomad-team", UserID: "live-nomad-user",
		TemplateID: "live-nomad-template", TemplateName: "live-nomad-template", TemplateNamespace: "sandbox0", ClusterID: slot.ClusterID,
		DesiredState: sandboxstore.SandboxDesiredStateActive, RuntimeGeneration: 1, CreatedAt: time.Now().UTC(), ResourceMillicpu: 250, ResourceMemoryMiB: 256}
	_, err = store.ReserveSandboxClaim(t.Context(), &sandboxstore.ReserveSandboxClaimRequest{Record: record, OperationID: "operation-1", LeaseTTL: time.Minute})
	require.NoError(t, err)
	filesystem, initial, err := store.EnsureInitialRootFSGeneration(t.Context(), &sandboxstore.EnsureInitialRootFSGenerationRequest{
		SandboxID: record.ID, TeamID: record.TeamID, SourceOCIRef: artifact.SourceOCIRef, SourceOCIDigest: artifact.SourceOCIDigest, BaseArtifactDigest: artifact.ArtifactDigest})
	require.NoError(t, err)
	stage.Identity.RootFSID, stage.Identity.NetworkIncarnationID, stage.Identity.RuntimeGeneration = filesystem.ID, record.ID, "1"
	stage.Identity.SourceOCIDigest = initial.SourceOCIDigest
	stage.ExpectedPolicyToken.NetworkIncarnationID = record.ID
	stage.InitialGeneration = initial.ID
	stage.Generation = &rootfshandoff.GenerationDescriptor{Version: rootfshandoff.GenerationDescriptorVersion, GenerationID: initial.ID, FilesystemID: filesystem.ID,
		SourceOCIDigest: initial.SourceOCIDigest, BaseArtifactDigest: initial.BaseArtifactDigest, BaseBlockRoot: initial.BaseBlockRoot,
		CurrentBlockHead: initial.CurrentBlockHead, WriterEpoch: initial.WriterEpoch, FormatGeneration: initial.FormatGeneration,
		DurabilityState: initial.DurabilityState, LocatorVersion: initial.LocatorVersion, Descriptor: initial.Descriptor}
	claimed, err := store.AcquireRuntimeSlot(t.Context(), &sandboxstore.AcquireRuntimeSlotRequest{
		OperationID: "operation-1", ClaimID: "claim-1", SandboxID: record.ID, FilesystemID: filesystem.ID, SourceGenerationID: initial.ID,
		CompatibilityDigest: slot.CompatibilityDigest, ClusterID: slot.ClusterID, RuntimeAssignmentRevision: revision,
		RuntimeAssignmentPayload: string(payload), NetworkPolicy: networkPolicy,
		NetworkPolicyDigest: stage.ExpectedPolicyToken.PolicyDigest, ClaimTTL: time.Minute,
		Resources: runtimeslot.RuntimeResourceRequest{Version: runtimeslot.RuntimeResourceRequestVersion, CPUMillicores: 250, MemoryBytes: 256 << 20, PIDsLimit: runtimeslot.DefaultRuntimePIDsLimit}})
	require.NoError(t, err)
	require.Equal(t, resources, claimed.ResourceLease)
	inputs, err := store.GetRuntimeSlotClaimInputs(t.Context(), slot.ID)
	require.NoError(t, err)
	require.NotNil(t, inputs)
	require.Equal(t, *assignment, inputs.Runtime)
	require.Equal(t, networkPolicy, inputs.NetworkPolicy)
	binding, err := stage.BindingDigest()
	require.NoError(t, err)
	issued, err := store.IssueRootFSWriterGrant(t.Context(), &sandboxstore.IssueRootFSWriterGrantRequest{
		GrantID: stage.Identity.WriterGrantID, SandboxID: record.ID, ExpectedFilesystemID: filesystem.ID, InitialGenerationID: initial.ID,
		ClaimID: stage.Identity.ClaimID, SlotID: slot.ID, OperationID: "live-nomad-issue", RawToken: stage.Identity.WriterGrantToken,
		BindingVersion: stage.BindingVersion, BindingDigest: binding[:], NodeUID: slot.NodeUID, NodeBootID: slot.NodeBootID,
		RuntimeNamespace: slot.AllocationNamespace, RuntimeID: stage.Identity.TaskName, RuntimeIncarnationID: slot.AllocationID,
		NodeName: slot.NodeID, GateParent: stage.Parent, RuntimeGeneration: "1", ConsumeExpiresAt: time.Now().Add(time.Minute)})
	require.NoError(t, err)
	require.Equal(t, stage.Identity.WriterEpoch, issued.Grant.WriterEpoch)
	_, err = store.BindRuntimeSlotWriterGrant(t.Context(), &sandboxstore.BindRuntimeSlotWriterGrantRequest{
		SlotID: slot.ID, OperationID: "operation-1", ClaimID: "claim-1", GrantID: stage.Identity.WriterGrantID})
	require.NoError(t, err)
}

func liveNomadCommitCommandReady(t *testing.T, state PersistedState, stage rootfshandoff.StageRequest) {
	t.Helper()
	key, err := internalauth.LoadEd25519PrivateKeyFromFile("/opt/s0-live-update-test/f/pki/procd-key.pem")
	require.NoError(t, err)
	generator := internalauth.NewGenerator(internalauth.GeneratorConfig{Caller: "manager", PrivateKey: key})
	token, err := generator.Generate("procd", "live-nomad-team", "live-nomad-user", internalauth.GenerateOptions{})
	require.NoError(t, err)
	_, address, err := nomadProcdEndpoint(state.TaskConfig)
	require.NoError(t, err)
	client := procdapi.NewProcdClient(procdapi.ProcdClientConfig{Timeout: time.Second})
	var probe *procdapi.CommandReadyProbeResult
	require.Eventually(t, func() bool {
		probe, err = client.ProbeCommandReady(t.Context(), address, token)
		return err == nil && probe != nil && probe.Status == "ready"
	}, 10*time.Second, 25*time.Millisecond, "authenticated actual procd readiness")
	proof := runtimeslot.CommandReadyProof{Version: runtimeslot.CommandReadyProofVersion, SlotID: state.TaskConfig.ID,
		OperationID: "operation-1", ClaimID: "claim-1", LaunchAttempt: stage.Identity.LaunchAttempt,
		RunscContainerID: state.ContainerID, ProcdInstanceID: probe.InstanceID, ProcdAddress: address,
		RequestMethod: http.MethodPut, RequestPath: runtimeslot.ProcdCommandReadyProbePath, ResponseStatus: http.StatusOK, ResponseBodyDigest: probe.ResponseBodyDigest}
	require.NoError(t, proof.Validate())
	payload, err := json.Marshal(runtimeslot.CommandReadyControlRequest{Proof: proof})
	require.NoError(t, err)
	request, err := http.NewRequestWithContext(t.Context(), http.MethodPut, "http://driver"+runtimeslot.NodeCommandReadyControlPath, bytes.NewReader(payload))
	require.NoError(t, err)
	response, err := unixHTTPClient(controlSocketPath("/run/sandbox0/nomad-slots", state.TaskConfig.ID)).Do(request)
	require.NoError(t, err)
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, response.StatusCode, "%s", body)
}

func liveNomadCompleteRegionalClaim(t *testing.T, stage rootfshandoff.StageRequest) {
	t.Helper()
	store := sandboxstore.NewPGSandboxStore(liveNomadRegionalPool(t))
	slot, err := store.GetRuntimeSlot(t.Context(), stage.Identity.SlotNonce)
	require.NoError(t, err)
	require.Equal(t, sandboxstore.RuntimeSlotStateActive, slot.State)
	require.NotEmpty(t, slot.ProcdInstanceID)
	require.Len(t, slot.CommandReadyDigest, 32)
	_, err = store.CompleteSandboxClaim(t.Context(), &sandboxstore.CompleteSandboxClaimRequest{
		SandboxID: stage.Identity.RootFSID, OperationID: "operation-1", SlotID: slot.ID, AllocationID: slot.AllocationID,
		AllocationNamespace: slot.AllocationNamespace, ResourceLeaseID: slot.ResourceLease.LeaseID, ResourceLeaseDigest: slot.ResourceLeaseDigest})
	require.NoError(t, err)
	t.Log("actual driver/procd handshake committed through regional PostgreSQL claim/slot authority")
}

// The payload must be a procd-managed child of the container's init process.
// Stock runsc intentionally kills processes injected with runsc exec on restore.
func liveNomadStartManagedPayload(t *testing.T, state PersistedState) {
	t.Helper()
	_, address, err := nomadProcdEndpoint(state.TaskConfig)
	require.NoError(t, err)
	request := procdapi.CreateContextRequest{Type: procdapi.ProcessTypeCMD,
		Cmd: &procdapi.CreateCMDContextRequest{Command: []string{"/payload", "-test.run=^TestLiveNomadGuestPayload$", "-test.timeout=30m", "-test.v"}},
		CWD: "/", IdleTimeoutSec: 3600, TTLSec: 3600}
	context := liveNomadManagedContext(t, address, http.MethodPost, procdapi.ContextsPath, http.StatusCreated, request)
	require.NotEmpty(t, context.ID)
	require.True(t, context.Running, "original managed payload must be running")
	metadata, err := json.Marshal(map[string]string{"context_id": context.ID})
	require.NoError(t, err)
	require.NoError(t, os.WriteFile("/opt/s0-live-update-test/f/managed-context.json", metadata, 0600))
	t.Logf("continuous payload launched once through authenticated procd context %s", context.ID)
}

func liveNomadManagedContext(t *testing.T, address, method, path string, status int, value any) *procdapi.ContextResponse {
	t.Helper()
	key, err := internalauth.LoadEd25519PrivateKeyFromFile("/opt/s0-live-update-test/f/pki/procd-key.pem")
	require.NoError(t, err)
	generator := internalauth.NewGenerator(internalauth.GeneratorConfig{Caller: "manager", PrivateKey: key})
	token, err := generator.Generate("procd", "live-nomad-team", "live-nomad-user", internalauth.GenerateOptions{})
	require.NoError(t, err)
	var body io.Reader
	if value != nil {
		payload, err := json.Marshal(value)
		require.NoError(t, err)
		body = bytes.NewReader(payload)
	}
	request, err := http.NewRequestWithContext(t.Context(), method, address+path, body)
	require.NoError(t, err)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-Internal-Token", token)
	response, err := (&http.Client{Timeout: 10 * time.Second}).Do(request)
	require.NoError(t, err)
	defer response.Body.Close()
	payload, err := io.ReadAll(io.LimitReader(response.Body, (1<<20)+1))
	require.NoError(t, err)
	require.LessOrEqual(t, len(payload), 1<<20)
	require.Equal(t, status, response.StatusCode, "managed context response: %s", payload)
	result, apiError, err := spec.DecodeResponse[procdapi.ContextResponse](bytes.NewReader(payload))
	require.NoError(t, err)
	require.Nil(t, apiError)
	require.NotNil(t, result)
	return result
}
