package power

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
)

type fakeCRIRuntimeClient struct {
	listResp  *runtimeapi.ListPodSandboxResponse
	listErr   error
	statsResp *runtimeapi.PodSandboxStatsResponse
	statsErr  error
	statsReq  *runtimeapi.PodSandboxStatsRequest
}

func (f *fakeCRIRuntimeClient) ListPodSandbox(_ context.Context, _ *runtimeapi.ListPodSandboxRequest, _ ...grpc.CallOption) (*runtimeapi.ListPodSandboxResponse, error) {
	return f.listResp, f.listErr
}

func (f *fakeCRIRuntimeClient) PodSandboxStats(_ context.Context, in *runtimeapi.PodSandboxStatsRequest, _ ...grpc.CallOption) (*runtimeapi.PodSandboxStatsResponse, error) {
	f.statsReq = in
	return f.statsResp, f.statsErr
}

func TestCRIStatsProviderSandboxResourceUsage(t *testing.T) {
	client := &fakeCRIRuntimeClient{
		listResp: &runtimeapi.ListPodSandboxResponse{Items: []*runtimeapi.PodSandbox{{
			Id: "sandbox-runtime-id",
			Metadata: &runtimeapi.PodSandboxMetadata{
				Name:      "sandbox-pod",
				Namespace: "default",
				Uid:       "pod-uid",
			},
		}}},
		statsResp: &runtimeapi.PodSandboxStatsResponse{Stats: &runtimeapi.PodSandboxStats{Linux: &runtimeapi.LinuxPodSandboxStats{
			Memory: &runtimeapi.MemoryUsage{
				UsageBytes:      &runtimeapi.UInt64Value{Value: 256},
				WorkingSetBytes: &runtimeapi.UInt64Value{Value: 200},
				AvailableBytes:  &runtimeapi.UInt64Value{Value: 56},
				RssBytes:        &runtimeapi.UInt64Value{Value: 144},
			},
			Process: &runtimeapi.ProcessUsage{ProcessCount: &runtimeapi.UInt64Value{Value: 12}},
		}}},
	}
	provider := &CRIStatsProvider{Client: client}

	usage, err := provider.SandboxResourceUsage(context.Background(), Target{PodNamespace: "default", PodName: "sandbox-pod", PodUID: "pod-uid"})
	require.NoError(t, err)
	require.NotNil(t, usage)
	assert.Equal(t, "sandbox-runtime-id", client.statsReq.GetPodSandboxId())
	assert.Equal(t, int64(256), usage.ContainerMemoryUsage)
	assert.Equal(t, int64(200), usage.ContainerMemoryWorkingSet)
	assert.Equal(t, int64(312), usage.ContainerMemoryLimit)
	assert.Equal(t, int64(144), usage.TotalMemoryRSS)
	assert.Equal(t, 12, usage.TotalThreadCount)
}

func TestCRIStatsProviderReturnsNotFoundForMissingSandbox(t *testing.T) {
	provider := &CRIStatsProvider{Client: &fakeCRIRuntimeClient{listResp: &runtimeapi.ListPodSandboxResponse{}}}
	_, err := provider.SandboxResourceUsage(context.Background(), Target{PodNamespace: "default", PodName: "sandbox-pod", PodUID: "pod-uid"})
	require.ErrorIs(t, err, ErrSandboxNotFound)
}
