package rootfshandoff

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/containerd/errdefs"
	"github.com/opencontainers/go-digest"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/rootfswriterauthority"
	"github.com/stretchr/testify/require"
)

func TestHandlerStagesAndReadsParent(t *testing.T) {
	controller := &fakeController{}
	request := validStageRequest()
	payload, err := json.Marshal(request)
	require.NoError(t, err)
	httpRequest := httptest.NewRequest(http.MethodPut, parentURL(request.Parent), bytes.NewReader(payload))
	response := httptest.NewRecorder()
	NewHandler(controller).ServeHTTP(response, httpRequest)
	require.Equal(t, http.StatusNoContent, response.Code)
	require.Equal(t, request, controller.staged)
}

func TestHandlerRejectsMismatchedParent(t *testing.T) {
	controller := &fakeController{}
	handler := NewHandler(controller)
	request := httptest.NewRequest("PUT", parentURL(validStageRequest().Parent), strings.NewReader(`{"parent":"sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"}`))
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	require.Equal(t, 400, response.Code)
}

func TestHandlerReportsRuntimeIncarnation(t *testing.T) {
	controller := &fakeController{}
	request := httptest.NewRequest(http.MethodGet, "/v1/incarnation", nil)
	response := httptest.NewRecorder()
	NewHandler(controller).ServeHTTP(response, request)
	require.Equal(t, http.StatusOK, response.Code)
	var result RuntimeIncarnation
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &result))
	require.Equal(t, "node", result.NodeUID)
}

func TestHandlerBeginsAndReadsPlannedRetire(t *testing.T) {
	controller := &fakeController{}
	stage := validStageRequest()
	request := RetireRequest{Parent: stage.Parent, OperationID: "retire-operation"}
	payload, err := json.Marshal(request)
	require.NoError(t, err)
	begin := httptest.NewRequest(http.MethodPut, parentURL(stage.Parent)+"/retire", bytes.NewReader(payload))
	beginResponse := httptest.NewRecorder()
	NewHandler(controller).ServeHTTP(beginResponse, begin)
	require.Equal(t, http.StatusNoContent, beginResponse.Code)
	require.Equal(t, request, controller.retire)

	read := httptest.NewRequest(http.MethodGet, parentURL(stage.Parent)+"/retire?operation_id=retire-operation", nil)
	readResponse := httptest.NewRecorder()
	NewHandler(controller).ServeHTTP(readResponse, read)
	require.Equal(t, http.StatusOK, readResponse.Code)
	var result RetireResult
	require.NoError(t, json.Unmarshal(readResponse.Body.Bytes(), &result))
	require.NoError(t, result.Validate())
}

func TestClientRoundTripsPlannedRetire(t *testing.T) {
	controller := &fakeController{}
	server := httptest.NewServer(NewHandler(controller))
	defer server.Close()
	client := &Client{http: server.Client()}
	transport := client.http.Transport
	client.http.Transport = roundTripperFunc(func(request *http.Request) (*http.Response, error) {
		request.URL.Scheme = "http"
		request.URL.Host = strings.TrimPrefix(server.URL, "http://")
		return transport.RoundTrip(request)
	})
	request := RetireRequest{Parent: validStageRequest().Parent, OperationID: "retire-operation"}
	require.NoError(t, client.BeginRetire(t.Context(), request))
	result, err := client.RetireResult(t.Context(), request)
	require.NoError(t, err)
	require.NoError(t, result.Validate())
}

func TestClientRoundTripsCrashFenceProof(t *testing.T) {
	controller := &fakeController{}
	server := httptest.NewServer(NewHandler(controller))
	defer server.Close()
	client := &Client{http: server.Client()}
	transport := client.http.Transport
	client.http.Transport = roundTripperFunc(func(request *http.Request) (*http.Response, error) {
		request.URL.Scheme = "http"
		request.URL.Host = strings.TrimPrefix(server.URL, "http://")
		return transport.RoundTrip(request)
	})
	request := CrashFenceRequest{Parent: validStageRequest().Parent, OperationID: "crash-operation"}
	result, err := client.CrashFence(t.Context(), request)
	require.NoError(t, err)
	require.NoError(t, result.Validate())
	require.Equal(t, request, controller.crashFence)
}

func TestClientExplicitlyVerifiesConsumer(t *testing.T) {
	controller := &fakeController{}
	server := httptest.NewServer(NewHandler(controller))
	defer server.Close()
	client := &Client{http: server.Client()}
	transport := client.http.Transport
	client.http.Transport = roundTripperFunc(func(request *http.Request) (*http.Response, error) {
		request.URL.Scheme = "http"
		request.URL.Host = strings.TrimPrefix(server.URL, "http://")
		return transport.RoundTrip(request)
	})
	request := ConsumerRequest{Parent: validStageRequest().Parent}
	require.NoError(t, client.VerifyConsumer(t.Context(), request))
	require.Equal(t, request, controller.consumer)
}

func TestWriterGrantHandlerConsumesExactBinding(t *testing.T) {
	controller := &fakeWriterGrantController{}
	request := validStageRequest()
	payload, err := json.Marshal(request)
	require.NoError(t, err)
	httpRequest := httptest.NewRequest(http.MethodPut, writerGrantURL(request.Identity.WriterGrantID), bytes.NewReader(payload))
	response := httptest.NewRecorder()
	NewWriterGrantHandler(controller).ServeHTTP(response, httpRequest)
	require.Equal(t, http.StatusOK, response.Code)
	require.Equal(t, request, controller.consumed)
}

func TestWriterGrantHandlerVerifiesTokenlessTerminalBinding(t *testing.T) {
	controller := &fakeWriterGrantController{}
	request := validStageRequest().WithoutWriterGrantToken()
	payload, err := json.Marshal(request)
	require.NoError(t, err)
	httpRequest := httptest.NewRequest(http.MethodPut, writerGrantURL(request.Identity.WriterGrantID)+"/terminal", bytes.NewReader(payload))
	response := httptest.NewRecorder()
	NewWriterGrantHandler(controller).ServeHTTP(response, httpRequest)
	require.Equal(t, http.StatusNoContent, response.Code)
	require.Equal(t, request, controller.terminal)
}

func TestWriterGrantHandlerRenewsTokenlessDurableBinding(t *testing.T) {
	controller := &fakeWriterGrantController{}
	request := validStageRequest().WithoutWriterGrantToken()
	payload, err := json.Marshal(request)
	require.NoError(t, err)
	httpRequest := httptest.NewRequest(http.MethodPut, writerGrantURL(request.Identity.WriterGrantID)+"/renew", bytes.NewReader(payload))
	response := httptest.NewRecorder()
	NewWriterGrantHandler(controller).ServeHTTP(response, httpRequest)
	require.Equal(t, http.StatusOK, response.Code)
	require.Equal(t, request, controller.renewed)
}

func TestWriterGrantHandlerBatchRenewsTokenlessBindings(t *testing.T) {
	controller := &fakeWriterGrantController{}
	first := validStageRequest().WithoutWriterGrantToken()
	second := first
	second.Identity.WriterGrantID = "writer-grant-2"
	second.Identity.WriterGrantTokenDigest = strings.Repeat("b", 64)
	payload, err := json.Marshal(WriterGrantBatchRenewRequest{Items: []StageRequest{first, second}})
	require.NoError(t, err)
	request := httptest.NewRequest(http.MethodPut, "/v1/writer-grants:renew", bytes.NewReader(payload))
	response := httptest.NewRecorder()
	NewWriterGrantHandler(controller).ServeHTTP(response, request)
	require.Equal(t, http.StatusOK, response.Code)
	require.Len(t, controller.batch, 2)
	var result protocol.BatchRenewResponse
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &result))
	require.NoError(t, result.Validate(2))
}

func TestWriterGrantHandlerRejectsEncodedSlash(t *testing.T) {
	request := validStageRequest().WithoutWriterGrantToken()
	payload, err := json.Marshal(request)
	require.NoError(t, err)
	httpRequest := httptest.NewRequest(http.MethodPut, "/v1/writer-grants/grant%2Fescape/terminal", bytes.NewReader(payload))
	response := httptest.NewRecorder()
	NewWriterGrantHandler(&fakeWriterGrantController{}).ServeHTTP(response, httpRequest)
	require.Equal(t, http.StatusBadRequest, response.Code)
}

func TestWriterGrantHandlerRejectsMismatchedGrantID(t *testing.T) {
	request := validStageRequest()
	payload, err := json.Marshal(request)
	require.NoError(t, err)
	httpRequest := httptest.NewRequest(http.MethodPut, writerGrantURL("another-grant"), bytes.NewReader(payload))
	response := httptest.NewRecorder()
	NewWriterGrantHandler(&fakeWriterGrantController{}).ServeHTTP(response, httpRequest)
	require.Equal(t, http.StatusBadRequest, response.Code)
}

func TestClientMapsUnauthorizedToPermissionDenied(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusUnauthorized)
	}))
	defer server.Close()
	client := &Client{http: server.Client()}
	request := validStageRequest()
	transport := client.http.Transport
	client.http.Transport = roundTripperFunc(func(httpRequest *http.Request) (*http.Response, error) {
		httpRequest.URL.Scheme = "http"
		httpRequest.URL.Host = strings.TrimPrefix(server.URL, "http://")
		return transport.RoundTrip(httpRequest)
	})
	_, err := client.ConsumeWriterGrant(t.Context(), request)
	require.ErrorIs(t, err, errdefs.ErrPermissionDenied)
}

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}

type fakeWriterGrantController struct {
	consumed StageRequest
	renewed  StageRequest
	batch    []StageRequest
	terminal StageRequest
}

func (f *fakeWriterGrantController) RenewWriterGrants(_ context.Context, requests []StageRequest) (protocol.BatchRenewResponse, error) {
	f.batch = append([]StageRequest(nil), requests...)
	observation := testLeaseObservation()
	results := make([]protocol.BatchRenewResult, 0, len(requests))
	for _, request := range requests {
		results = append(results, protocol.BatchRenewResult{
			GrantID: request.Identity.WriterGrantID, Observation: &observation,
		})
	}
	return protocol.BatchRenewResponse{Results: results}, nil
}

func (f *fakeWriterGrantController) ConsumeWriterGrant(_ context.Context, request StageRequest) (protocol.LeaseObservation, error) {
	f.consumed = request
	return testLeaseObservation(), nil
}

func (f *fakeWriterGrantController) RenewWriterGrant(_ context.Context, request StageRequest) (protocol.LeaseObservation, error) {
	f.renewed = request
	return testLeaseObservation(), nil
}

func testLeaseObservation() protocol.LeaseObservation {
	now := time.Unix(1_700_000_000, 0).UTC()
	return protocol.LeaseObservation{ServerTime: now, RenewAfter: now.Add(time.Minute), LeaseExpiresAt: now.Add(2 * time.Minute)}
}

func (f *fakeWriterGrantController) VerifyTerminalWriterGrant(_ context.Context, request StageRequest) error {
	f.terminal = request
	return nil
}

type fakeController struct {
	staged     StageRequest
	consumer   ConsumerRequest
	retire     RetireRequest
	crashFence CrashFenceRequest
}

func (f *fakeController) CrashFence(_ context.Context, request CrashFenceRequest) (CrashFenceResult, error) {
	f.crashFence = request
	stage := validStageRequest()
	binding, err := stage.BindingDigest()
	if err != nil {
		return CrashFenceResult{}, err
	}
	blockHead := digest.FromString("crash-initial-head").String()
	observedAt := time.Unix(1_700_000_100, 0).UTC().Format(time.RFC3339Nano)
	proof := CrashFenceProof{
		Version: CrashFenceProofVersion, OperationID: request.OperationID, Parent: request.Parent,
		ClaimID: stage.Identity.ClaimID, WriterGrantID: stage.Identity.WriterGrantID,
		WriterEpoch: stage.Identity.WriterEpoch, BindingVersion: stage.BindingVersion,
		BindingDigest: fmt.Sprintf("%x", binding), RootFSID: stage.Identity.RootFSID,
		InitialGeneration: stage.InitialGeneration, InitialBlockHead: blockHead,
		HeadAction: CrashFenceHeadKeepInitial, NodeUID: stage.Identity.NodeUID, BootID: stage.Identity.BootID,
		RuntimeGeneration: stage.Identity.RuntimeGeneration, HostMountNamespaceID: "mntns",
		AllocationID: stage.Identity.AllocationID, NetworkIncarnationID: stage.Identity.NetworkIncarnationID,
		TaskName: stage.Identity.TaskName, SlotNonce: stage.Identity.SlotNonce,
		ActiveKey: "active", ConsumerBound: true, ContainerID: "container", ContainerAbsent: true, TaskAbsent: true,
		FrontendSnapshotAbsent: true, StableMountAbsent: true, RootFSState: StateTombstoned,
		Session: CrashFenceSessionObservation{
			Parent: request.Parent, RootFSID: stage.Identity.RootFSID, WriterEpoch: stage.Identity.WriterEpoch,
			OperationID: request.OperationID, BindingDigest: fmt.Sprintf("%x", binding),
			SessionState: StateTombstoned,
			BranchPath:   "/branch", DeviceBound: true, DevicePath: "/dev/nbd0", LiveSessionAbsent: true,
			MergedMountAbsent: true, XFSMountAbsent: true, ObservedAt: observedAt,
		},
		ObservedAt: observedAt,
	}
	digest, err := proof.Digest()
	if err != nil {
		return CrashFenceResult{}, err
	}
	return CrashFenceResult{Proof: proof, ProofDigest: fmt.Sprintf("%x", digest)}, nil
}

func (f *fakeController) Stage(_ context.Context, request StageRequest) error {
	f.staged = request
	return nil
}

func (f *fakeController) MarkReady(context.Context, ReadyRequest) error { return nil }

func (f *fakeController) VerifyConsumer(_ context.Context, request ConsumerRequest) error {
	f.consumer = request
	return nil
}

func (f *fakeController) Status(context.Context, string) (ParentStatus, error) {
	return ParentStatus{}, errdefs.ErrNotFound
}

func (f *fakeController) Remove(context.Context, string) error { return nil }

func (f *fakeController) BeginRetire(_ context.Context, request RetireRequest) error {
	f.retire = request
	return nil
}

func (f *fakeController) RetireResult(_ context.Context, request RetireRequest) (RetireResult, error) {
	stage := validStageRequest()
	checksum := digest.FromString("mapping-root")
	descriptor, err := rootfsblock.EncodeDescriptor(rootfsblock.Descriptor{
		Version: rootfsblock.DescriptorVersion, LogicalSizeBytes: rootfsblock.LogicalBlockSize,
		BlockSizeBytes: rootfsblock.LogicalBlockSize,
		MappingRoot: rootfsblock.MappingRootLocator{
			Version: rootfsblock.MappingPageVersion, RootDigest: checksum.String(),
			Object: rootfsblock.ObjectRange{
				Key: "rootfs/v1/maps/sha256/" + checksum.Encoded(), Length: 1, Checksum: checksum.String(),
			},
		},
	})
	if err != nil {
		return RetireResult{}, err
	}
	return RetireResult{
		Parent: request.Parent, RootFSID: stage.Identity.RootFSID, WriterEpoch: stage.Identity.WriterEpoch,
		OperationID: request.OperationID, CurrentBlockHead: checksum.String(),
		DurabilityState: rootfsblock.DurabilityS3, Descriptor: descriptor,
		DetachProof: strings.Repeat("ab", 32),
	}, nil
}

func (f *fakeController) Incarnation(context.Context) (RuntimeIncarnation, error) {
	return RuntimeIncarnation{
		NodeUID: "node", BootID: "boot", RuntimeGeneration: "runtime",
		HostMountNamespaceID: "mntns", AdmissionReady: true, WriterRenewalReady: true,
	}, nil
}
