package runtimeslotnode

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/opencontainers/go-digest"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshandoff"
	"github.com/sandbox0-ai/sandbox0/pkg/runtimecheckpoint"
	"github.com/sandbox0-ai/sandbox0/pkg/runtimecontrol"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	"github.com/stretchr/testify/require"
)

type migrationChannelExecutor struct {
	mu    sync.Mutex
	calls int
	peer  runtimecheckpoint.PeerEndpoint
}

func (e *migrationChannelExecutor) AcknowledgeMigrationSourceGC(_ context.Context, r protocol.MigrationSourceGCRequest) (*protocol.MigrationSourceGCAcknowledgement, error) {
	digest, err := r.Digest()
	return &protocol.MigrationSourceGCAcknowledgement{RequestDigest: digest}, err
}

func (e *migrationChannelExecutor) CaptureMigration(_ context.Context, request protocol.MigrationCaptureRequest) (*protocol.MigrationCapture, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.calls++
	digest, err := request.Digest()
	return &protocol.MigrationCapture{Request: request, RequestDigest: digest, State: protocol.MigrationCaptureIntent}, err
}

func TestMigrationCaptureUsesAuthenticatedNodeCapability(t *testing.T) {
	for _, supported := range []bool{false, true} {
		name := "unsupported"
		if supported {
			name = "supported"
		}
		t.Run(name, func(t *testing.T) {
			hub, err := NewChannelHub(channelTestVerifier{}, channelTestCapacityStore{})
			require.NoError(t, err)
			defer hub.Close()
			server, files := newNodeChannelTLSServer(t, hub)
			defer server.Close()
			executor := &migrationChannelExecutor{}
			peerIdentity, err := runtimecheckpoint.NewPeerIdentity()
			require.NoError(t, err)
			executor.peer, err = runtimecheckpoint.NewPeerEndpoint("127.0.0.1:19443", peerIdentity)
			require.NoError(t, err)
			config := protocol.NodeChannelAgentConfig{BaseURL: server.URL, CAFile: files.ca, ClientCertFile: files.clientCert,
				ClientKeyFile: files.clientKey, TokenFile: files.token, PeerURISAN: testNodeChannelServerURI,
				NodeUID: "node-uid-1", NodeBootIDFile: files.boot, ClusterID: "cluster-1", NodeID: "node-1",
				Executor: &channelTestExecutor{}, Capacity: channelTestCapacity(), AgentInstanceID: "agent-1",
				ReconnectMin: time.Millisecond, ReconnectMax: 5 * time.Millisecond}
			if supported {
				config.MigrationCaptureExecutor = executor
				config.MigrationPublishExecutor = executor
				config.MigrationPublicationPlanExecutor = executor
				config.MigrationFenceExecutor = executor
				config.MigrationImagePrepareExecutor = executor
				config.MigrationImagePrefetchExecutor = executor
				config.MigrationFinalizeExecutor = executor
				config.MigrationSourceGCExecutor = executor
			}
			agent, err := protocol.NewNodeChannelAgent(config)
			require.NoError(t, err)
			ctx, cancel := context.WithCancel(t.Context())
			done := make(chan error, 1)
			go func() { done <- agent.Run(ctx) }()
			defer func() { cancel(); <-done }()
			waitNodeChannelConnected(t, hub, "cluster-1", "node-1", "node-uid-1", "boot-1")
			gc := protocol.MigrationSourceGCRequest{Target: protocol.NodeChannelTarget{SlotID: "slot-1", ClusterID: "cluster-1", AllocationID: "allocation-1", NodeID: "node-1", NodeUID: "node-uid-1", NodeBootID: "boot-1", ControlEndpoint: "unix:///var/run/sandbox0/source.sock"}, FinalizationDigest: strings.Repeat("a", 64), CleanupProofDigest: strings.Repeat("b", 64), AllocationAbsenceDigest: strings.Repeat("c", 64)}
			ack, gcErr := hub.AcknowledgeMigrationSourceGC(ctx, gc)
			if supported {
				require.NoError(t, gcErr)
				require.NoError(t, ack.ValidateFor(gc))
			} else {
				require.Error(t, gcErr)
			}
			gc.Target.NodeUID = "another-physical-node"
			staleCtx, staleCancel := context.WithTimeout(ctx, 100*time.Millisecond)
			_, gcErr = hub.AcknowledgeMigrationSourceGC(staleCtx, gc)
			staleCancel()
			require.Error(t, gcErr)
			request := protocol.MigrationCaptureRequest{Target: protocol.NodeChannelTarget{SlotID: "slot-1", ClusterID: "cluster-1", AllocationID: "allocation-1",
				NodeID: "node-1", NodeUID: "node-uid-1", NodeBootID: "boot-1", ControlEndpoint: "unix:///var/run/sandbox0/source.sock"},
				OperationID: "migration-1", LifecycleEpoch: 2, SandboxID: "sandbox-1", SourceGeneration: 1, ProcdInstanceID: "procd-1",
				AssignmentRevision: strings.Repeat("ab", 32), BindingDigest: strings.Repeat("cd", 32), ResourceLeaseDigest: strings.Repeat("ef", 32)}
			result, err := hub.CaptureMigration(ctx, request)
			if supported {
				require.NoError(t, err)
				require.Equal(t, request, result.Request)
				require.Equal(t, protocol.MigrationCaptureIntent, result.State)
			} else {
				require.Error(t, err)
			}
			publication := migrationChannelPublication(t, request)
			publication.DestinationPeerCertificateSHA256 = digest.FromString("reserved-destination").String()
			plan, err := hub.PlanMigrationPublication(ctx, publication)
			if supported {
				require.NoError(t, err)
				require.NoError(t, plan.ValidateFor(publication))
				command, err := protocol.NewNodeChannelMigrationPublicationPlanCommand(publication)
				require.NoError(t, err)
				response := protocol.NodeChannelResult{Version: protocol.NodeChannelVersion, Kind: command.Kind, RequestID: command.RequestID, MigrationPublicationPlan: plan}
				require.NoError(t, response.ValidateFor(command))
				receipt := migrationChannelReceipt(t, publication)
				response.MigrationPublish = &receipt
				require.Error(t, response.ValidateFor(command), "a plan cannot be combined with a publication receipt")
				response.MigrationPublish = nil
				plan.RequestDigest = strings.Repeat("f", 64)
				require.Error(t, response.ValidateFor(command))
			} else {
				require.Error(t, err)
			}
			published, err := hub.PublishMigration(ctx, publication)
			if supported {
				require.NoError(t, err)
				require.NoError(t, published.ValidateFor(publication))
			} else {
				require.Error(t, err)
			}
			pubReceipt := migrationChannelReceipt(t, publication)
			fenceRequest := protocol.MigrationSourceFenceRequest{PublicationRequest: publication, Publication: pubReceipt}
			fenced, err := hub.FenceMigrationSource(ctx, fenceRequest)
			if supported {
				require.NoError(t, err)
				require.NoError(t, fenced.ValidateFor(fenceRequest))
			} else {
				require.Error(t, err)
			}
			sourceCapture := request
			sourceCapture.Target.NodeID, sourceCapture.Target.NodeUID = "source-node", "source-uid"
			sourceCapture.Target.SlotID, sourceCapture.Target.AllocationID = "source-slot", "source-allocation"
			sourcePublication := migrationChannelPublication(t, sourceCapture)
			sourcePublication.DestinationPeerCertificateSHA256 = digest.FromString("reserved-destination").String()
			resources, err := protocol.NewRuntimeResourceLease(request.OperationID, "target-claim", request.Target.SlotID, request.Target.ClusterID,
				request.Target.NodeID, request.Target.NodeUID, request.Target.NodeBootID,
				protocol.RuntimeResourceRequest{Version: protocol.RuntimeResourceRequestVersion, CPUMillicores: 1000, MemoryBytes: 128 << 20, PIDsLimit: 1024}, "0", "0")
			require.NoError(t, err)
			prepare := protocol.MigrationImagePrepareRequest{Target: request.Target, Resources: resources, Publication: sourcePublication, Receipt: migrationChannelReceipt(t, sourcePublication)}
			resourceDigest, err := resources.Digest()
			require.NoError(t, err)
			prefetch := protocol.MigrationImagePrefetchRequest{Publication: sourcePublication,
				Staging: protocol.MigrationStagingRequest{Target: request.Target, Destination: request.Target, Source: sourcePublication.Capture.Request,
					DestinationResourceLeaseDigest: strings.TrimPrefix(resourceDigest, "sha256:"), Bytes: runtimecheckpoint.ChunkBytes, Inodes: 64},
				Plan: protocol.MigrationPublicationPlan{RequestDigest: prepare.Receipt.RequestDigest, Binding: prepare.Receipt.Binding,
					Reference: prepare.Receipt.Reference, Peer: executor.peer}}
			prefetched, err := hub.PrefetchMigrationImage(ctx, prefetch)
			if supported {
				require.NoError(t, err)
				require.NoError(t, prefetched.ValidateFor(prefetch))
				command, err := protocol.NewNodeChannelMigrationImagePrepareCommand(prepare)
				require.NoError(t, err)
				response := protocol.NodeChannelResult{Version: protocol.NodeChannelVersion, Kind: command.Kind, RequestID: command.RequestID, MigrationImagePrefetch: prefetched}
				require.Error(t, response.ValidateFor(command), "a cache acknowledgement cannot replace normal image preparation")
			} else {
				require.Error(t, err)
			}
			stalePrefetch := prefetch
			stalePrefetch.Staging.Target.NodeBootID, stalePrefetch.Staging.Destination.NodeBootID = "old-boot", "old-boot"
			_, err = hub.PrefetchMigrationImage(ctx, stalePrefetch)
			require.Error(t, err)
			prepared, err := hub.PrepareMigrationImage(ctx, prepare)
			if supported {
				require.NoError(t, err)
				require.NoError(t, prepared.ValidateFor(prepare))
			} else {
				require.Error(t, err)
			}
			command, err := protocol.NewNodeChannelMigrationImagePrepareCommand(prepare)
			require.NoError(t, err)
			response := protocol.NodeChannelResult{Version: protocol.NodeChannelVersion, Kind: command.Kind, RequestID: command.RequestID, MigrationImagePrepare: prepared}
			if supported {
				require.NoError(t, response.ValidateFor(command))
				response.MigrationImagePrepare.RequestDigest = strings.Repeat("ab", 32)
				require.Error(t, response.ValidateFor(command))
			}
			prepare.Target.NodeBootID, prepare.Resources.NodeBootID = "old-boot", "old-boot"
			_, err = hub.PrepareMigrationImage(ctx, prepare)
			require.Error(t, err)
			finalize := migrationChannelFinalize(t, request)
			finalized, err := hub.FinalizeMigrationSource(ctx, finalize)
			if supported {
				require.NoError(t, err)
				require.NoError(t, finalized.ValidateFor(finalize))
				finalized.ImageAbsent = false
				require.Error(t, finalized.ValidateFor(finalize))
			} else {
				require.Error(t, err)
			}
			staleCapture := request
			staleCapture.Target.NodeBootID = "old-boot"
			_, err = hub.FinalizeMigrationSource(ctx, migrationChannelFinalize(t, staleCapture))
			require.Error(t, err)
			wrongBoot := request
			wrongBoot.Target.NodeBootID = "old-boot"
			_, err = hub.CaptureMigration(ctx, wrongBoot)
			require.Error(t, err)
			_, err = hub.PublishMigration(ctx, migrationChannelPublication(t, wrongBoot))
			require.Error(t, err)
			wrongPlan := migrationChannelPublication(t, wrongBoot)
			wrongPlan.DestinationPeerCertificateSHA256 = publication.DestinationPeerCertificateSHA256
			_, err = hub.PlanMigrationPublication(ctx, wrongPlan)
			require.Error(t, err)
			executor.mu.Lock()
			calls := executor.calls
			executor.mu.Unlock()
			want := 0
			if supported {
				want = 7
			}
			require.Equal(t, want, calls)
		})
	}
}

func (e *migrationChannelExecutor) PrefetchMigrationImage(_ context.Context, request protocol.MigrationImagePrefetchRequest) (*protocol.MigrationImagePrefetched, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.calls++
	digest, err := request.Digest()
	return &protocol.MigrationImagePrefetched{RequestDigest: digest, ManifestDigest: request.Plan.Reference.ManifestDigest, TotalBytes: 8192}, err
}

func (e *migrationChannelExecutor) PlanMigrationPublication(ctx context.Context, request protocol.MigrationPublicationRequest) (*protocol.MigrationPublicationPlan, error) {
	p, err := e.PublishMigration(ctx, request)
	if err != nil {
		return nil, err
	}
	return &protocol.MigrationPublicationPlan{RequestDigest: p.RequestDigest, Binding: p.Binding, Reference: p.Reference, Peer: e.peer}, nil
}

func TestMigrationPublicationPlanIsBoundAndCannotReplacePublication(t *testing.T) {
	capture := protocol.MigrationCaptureRequest{Target: protocol.NodeChannelTarget{SlotID: "slot-1", ClusterID: "cluster-1", AllocationID: "allocation-1",
		NodeID: "node-1", NodeUID: "node-uid-1", NodeBootID: "boot-1", ControlEndpoint: "unix:///var/run/sandbox0/source.sock"},
		OperationID: "migration-1", LifecycleEpoch: 2, SandboxID: "sandbox-1", SourceGeneration: 1, ProcdInstanceID: "procd-1",
		AssignmentRevision: strings.Repeat("ab", 32), BindingDigest: strings.Repeat("cd", 32), ResourceLeaseDigest: strings.Repeat("ef", 32)}
	request := migrationChannelPublication(t, capture)
	request.DestinationPeerCertificateSHA256 = digest.FromString("destination").String()
	identity, err := runtimecheckpoint.NewPeerIdentity()
	require.NoError(t, err)
	endpoint, err := runtimecheckpoint.NewPeerEndpoint("127.0.0.1:19443", identity)
	require.NoError(t, err)
	executor := &migrationChannelExecutor{peer: endpoint}
	plan, err := executor.PlanMigrationPublication(t.Context(), request)
	require.NoError(t, err)
	require.NoError(t, plan.ValidateFor(request))
	for _, test := range []struct {
		name   string
		change func(*protocol.MigrationPublicationPlan)
	}{
		{"source binding", func(p *protocol.MigrationPublicationPlan) { p.Binding.SandboxID = "another-sandbox" }},
		{"reference binding", func(p *protocol.MigrationPublicationPlan) {
			p.Reference.BindingDigest = digest.FromString("another-cut").String()
		}},
		{"missing manifest", func(p *protocol.MigrationPublicationPlan) { p.Reference.ManifestDigest = "" }},
		{"missing peer", func(p *protocol.MigrationPublicationPlan) { p.Peer = runtimecheckpoint.PeerEndpoint{} }},
		{"public peer", func(p *protocol.MigrationPublicationPlan) { p.Peer.Address = "https://8.8.8.8:443" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			changed := *plan
			test.change(&changed)
			require.Error(t, changed.ValidateFor(request))
		})
	}
	changedRequest := request
	changedRequest.DestinationPeerCertificateSHA256 = ""
	require.Error(t, plan.ValidateFor(changedRequest))
	_, err = protocol.NewNodeChannelMigrationPublicationPlanCommand(changedRequest)
	require.Error(t, err)
	publish, err := protocol.NewNodeChannelMigrationPublishCommand(request)
	require.NoError(t, err)
	response := protocol.NodeChannelResult{Version: protocol.NodeChannelVersion, Kind: publish.Kind, RequestID: publish.RequestID, MigrationPublicationPlan: plan}
	require.Error(t, response.ValidateFor(publish), "a plan alone cannot acknowledge durable publication")
}

func (e *migrationChannelExecutor) PublishMigration(_ context.Context, request protocol.MigrationPublicationRequest) (*protocol.MigrationPublication, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.calls++
	d, err := request.Digest()
	if err != nil {
		return nil, err
	}
	b, err := request.Binding()
	if err != nil {
		return nil, err
	}
	bd, err := b.Digest()
	return &protocol.MigrationPublication{RequestDigest: d, Binding: b, Reference: runtimecheckpoint.Reference{BindingDigest: bd, ManifestDigest: digest.FromString("uploaded-image").String()}}, err
}

func migrationChannelPublication(t *testing.T, capture protocol.MigrationCaptureRequest) protocol.MigrationPublicationRequest {
	t.Helper()
	source := runtimecontrol.Assignment{SandboxID: capture.SandboxID, TeamID: "team", RuntimeGeneration: capture.SourceGeneration, SecurityClass: "standard"}
	var err error
	capture.AssignmentRevision, err = source.Revision()
	require.NoError(t, err)
	d, err := capture.Digest()
	require.NoError(t, err)
	descriptor, err := rootfsblock.EncodeDescriptor(rootfsblock.Descriptor{Version: rootfsblock.DescriptorVersion, LogicalSizeBytes: 4096, BlockSizeBytes: 4096,
		MappingRoot: rootfsblock.MappingRootLocator{Version: rootfsblock.MappingPageVersion, RootDigest: digest.FromString("map").String(), Object: rootfsblock.ObjectRange{Key: "maps/test", Length: 1, Checksum: digest.FromString("map").String()}}})
	require.NoError(t, err)
	cutRequest := rootfshandoff.MigrationRootFSCutRequest{OperationID: capture.OperationID, CaptureRequestDigest: d, SourceBindingDigest: capture.BindingDigest, GenerationID: "migration-" + d}
	root := digest.FromString("map").String()
	cut, err := rootfshandoff.NewMigrationRootFSCut(cutRequest, rootfshandoff.GenerationDescriptor{Version: rootfshandoff.GenerationDescriptorVersion, GenerationID: cutRequest.GenerationID,
		FilesystemID: "fs", SourceOCIDigest: root, BaseArtifactDigest: root, BaseBlockRoot: root, CurrentBlockHead: root, WriterEpoch: 2, FormatGeneration: 2, DurabilityState: rootfsblock.DurabilityS3, LocatorVersion: 2, Descriptor: descriptor}, 1)
	require.NoError(t, err)
	source.RuntimeGeneration++
	request := protocol.MigrationPublicationRequest{Capture: protocol.MigrationCapture{Request: capture, RequestDigest: d, State: protocol.MigrationCaptureComplete, RootFS: &cut},
		Assignment:          runtimecontrol.MigrationAssignment{OperationID: capture.OperationID, SourceGeneration: capture.SourceGeneration, SourceRevision: capture.AssignmentRevision, Target: source},
		CompatibilityDigest: root, CPUFeaturesDigest: root}
	_, err = request.Digest()
	require.NoError(t, err)
	return request
}

func migrationChannelReceipt(t *testing.T, request protocol.MigrationPublicationRequest) protocol.MigrationPublication {
	t.Helper()
	d, err := request.Digest()
	require.NoError(t, err)
	b, err := request.Binding()
	require.NoError(t, err)
	bd, err := b.Digest()
	require.NoError(t, err)
	return protocol.MigrationPublication{RequestDigest: d, Binding: b, Reference: runtimecheckpoint.Reference{BindingDigest: bd, ManifestDigest: digest.FromString("uploaded-image").String()}}
}

func (e *migrationChannelExecutor) FenceMigrationSource(_ context.Context, request protocol.MigrationSourceFenceRequest) (*protocol.MigrationSourceFenceProof, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.calls++
	d, err := request.Digest()
	if err != nil {
		return nil, err
	}
	r, err := request.RootFSRequest()
	if err != nil {
		return nil, err
	}
	capture := request.PublicationRequest.Capture
	rootfs, err := rootfshandoff.NewMigrationRootFSDetachProof(r, rootfshandoff.CrashFenceSessionObservation{Parent: "parent", RootFSID: capture.RootFS.Generation.FilesystemID,
		WriterEpoch: capture.RootFS.Generation.WriterEpoch, OperationID: r.OperationID, BindingDigest: capture.Request.BindingDigest, SessionState: rootfshandoff.StateTombstoned,
		BranchPath: "/private/captured.wal", DeviceBound: true, DevicePath: "/dev/nbd0", LiveSessionAbsent: true, MergedMountAbsent: true, XFSMountAbsent: true, ObservedAt: time.Now().UTC().Format(time.RFC3339Nano)})
	if err != nil {
		return nil, err
	}
	proof := &protocol.MigrationSourceFenceProof{RequestDigest: d, RootFS: rootfs, ContainerID: protocol.NomadRunscContainerID(capture.Request.Target.SlotID), MountNamespaceID: "mnt:[1]", ContainerAbsent: true, StableMountAbsent: true}
	proof.Digest, err = proof.ProofDigest()
	return proof, err
}

func (e *migrationChannelExecutor) PrepareMigrationImage(_ context.Context, request protocol.MigrationImagePrepareRequest) (*protocol.MigrationImagePrepared, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.calls++
	d, err := request.Digest()
	return &protocol.MigrationImagePrepared{RequestDigest: d, ManifestDigest: request.Receipt.Reference.ManifestDigest, TotalBytes: 1024}, err
}

func migrationChannelFinalize(t *testing.T, capture protocol.MigrationCaptureRequest) protocol.MigrationSourceFinalizeRequest {
	t.Helper()
	target := capture.Target
	resources, err := protocol.NewRuntimeResourceLease("source-operation", "source-claim", target.SlotID, target.ClusterID, target.NodeID, target.NodeUID, target.NodeBootID,
		protocol.RuntimeResourceRequest{Version: protocol.RuntimeResourceRequestVersion, CPUMillicores: 1000, MemoryBytes: 128 << 20, PIDsLimit: 1024}, "0", "0")
	require.NoError(t, err)
	digest, err := resources.Digest()
	require.NoError(t, err)
	capture.ResourceLeaseDigest = strings.TrimPrefix(digest, "sha256:")
	publication := migrationChannelPublication(t, capture)
	fence := protocol.MigrationSourceFenceRequest{PublicationRequest: publication, Publication: migrationChannelReceipt(t, publication)}
	proof, err := (&migrationChannelExecutor{}).FenceMigrationSource(t.Context(), fence)
	require.NoError(t, err)
	adoption := protocol.MigrationAdoptionRequest{Target: protocol.NodeChannelTarget{SlotID: "target-slot", ClusterID: target.ClusterID, NodeID: "target-node", NodeUID: "target-uid", NodeBootID: "target-boot", AllocationID: "target-allocation", ControlEndpoint: "unix:///target/control.sock"},
		OperationID: capture.OperationID, ClaimID: "target-claim", SandboxID: capture.SandboxID, RuntimeGeneration: publication.Assignment.Target.RuntimeGeneration, ProcdInstanceID: capture.ProcdInstanceID,
		RestoreDigest: strings.Repeat("a", 64), CommandReadyDigest: strings.Repeat("b", 64)}
	digest, err = adoption.Digest()
	require.NoError(t, err)
	cleanup := protocol.NodeCleanupControlRequest{OperationID: protocol.MigrationSourceCleanupOperationID(capture.OperationID), WriterOperationID: capture.OperationID, WriterRetireKind: protocol.WriterRetireKindMigration,
		SlotID: target.SlotID, ClusterID: target.ClusterID, AllocationID: target.AllocationID, NodeID: target.NodeID, NodeUID: target.NodeUID, NodeBootID: target.NodeBootID,
		NetNSIdentity: "source-netns", RunscContainerID: proof.ContainerID, WriterGrantID: "source-writer", WriterAuthorityDigest: proof.Digest, Resources: resources, ResourceLeaseDigest: capture.ResourceLeaseDigest}
	request := protocol.MigrationSourceFinalizeRequest{Fence: fence, SourceProof: *proof, Cleanup: cleanup, Adoption: protocol.MigrationAdoptionReceipt{Request: adoption, Proof: protocol.MigrationAdoptionProof{RequestDigest: digest, ImageAbsent: true}}}
	require.NoError(t, request.Validate())
	return request
}

func (e *migrationChannelExecutor) FinalizeMigrationSource(_ context.Context, request protocol.MigrationSourceFinalizeRequest) (*protocol.MigrationSourceFinalizeProof, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.calls++
	rootRequest, err := request.RootFSRequest()
	if err != nil {
		return nil, err
	}
	root := rootfshandoff.MigrationRootFSFinalizeProof{Request: rootRequest, Parent: request.SourceProof.RootFS.Session.Parent, BindingDigest: request.SourceProof.RootFS.Session.BindingDigest, BranchAbsent: true, MountDirectoriesAbsent: true}
	root.Digest, err = root.ProofDigest()
	if err != nil {
		return nil, err
	}
	c := request.Cleanup
	cleanup := protocol.NodeCleanupControlProof{Version: protocol.NodeCleanupProofVersion, OperationID: c.OperationID, WriterOperationID: c.WriterOperationID, WriterRetireKind: c.WriterRetireKind,
		SlotID: c.SlotID, ClusterID: c.ClusterID, AllocationID: c.AllocationID, NodeID: c.NodeID, NodeUID: c.NodeUID, NodeBootID: c.NodeBootID, NetNSIdentity: c.NetNSIdentity,
		RunscContainerID: c.RunscContainerID, WriterGrantID: c.WriterGrantID, WriterAuthorityDigest: c.WriterAuthorityDigest, RootFSOperationID: c.WriterOperationID, RootFSProofDigest: root.Digest,
		Resources: c.Resources, ResourceLeaseID: c.Resources.LeaseID, ResourceLeaseDigest: c.ResourceLeaseDigest, RunscAbsent: true, StableMountAbsent: true, RootFSWriterAbsent: true, NetworkPolicyAbsent: true, ResourceCgroupAbsent: true}
	cleanup.ProofDigest, err = cleanup.Digest()
	if err != nil {
		return nil, err
	}
	digest, err := request.Digest()
	if err != nil {
		return nil, err
	}
	return &protocol.MigrationSourceFinalizeProof{RequestDigest: digest, RootFS: root, ImageAbsent: true, Cleanup: cleanup}, nil
}
