package nodepoollifecycle

import (
	"context"
	"testing"
	"time"

	"github.com/aliyun/alibaba-cloud-sdk-go/services/ess"
	"github.com/aliyun/alibaba-cloud-sdk-go/services/vpc"
	"github.com/stretchr/testify/require"
)

type lifecycleESSStub struct {
	describeActions func(*ess.DescribeLifecycleActionsRequest) (*ess.DescribeLifecycleActionsResponse, error)
	describeNodes   func(*ess.DescribeScalingInstancesRequest) (*ess.DescribeScalingInstancesResponse, error)
	completed       *ess.CompleteLifecycleActionRequest
}

func (s *lifecycleESSStub) DescribeLifecycleActions(request *ess.DescribeLifecycleActionsRequest) (*ess.DescribeLifecycleActionsResponse, error) {
	return s.describeActions(request)
}

func (s *lifecycleESSStub) DescribeScalingInstances(request *ess.DescribeScalingInstancesRequest) (*ess.DescribeScalingInstancesResponse, error) {
	return s.describeNodes(request)
}

func (*lifecycleESSStub) RecordLifecycleActionHeartbeat(*ess.RecordLifecycleActionHeartbeatRequest) (*ess.RecordLifecycleActionHeartbeatResponse, error) {
	return ess.CreateRecordLifecycleActionHeartbeatResponse(), nil
}

func (s *lifecycleESSStub) CompleteLifecycleAction(request *ess.CompleteLifecycleActionRequest) (*ess.CompleteLifecycleActionResponse, error) {
	s.completed = request
	return ess.CreateCompleteLifecycleActionResponse(), nil
}

func (*lifecycleESSStub) SetInstancesProtection(*ess.SetInstancesProtectionRequest) (*ess.SetInstancesProtectionResponse, error) {
	return ess.CreateSetInstancesProtectionResponse(), nil
}

type lifecycleVPCStub struct{}

func (lifecycleVPCStub) DescribeRouteEntryList(*vpc.DescribeRouteEntryListRequest) (*vpc.DescribeRouteEntryListResponse, error) {
	return vpc.CreateDescribeRouteEntryListResponse(), nil
}

func (lifecycleVPCStub) DeleteRouteEntry(*vpc.DeleteRouteEntryRequest) (*vpc.DeleteRouteEntryResponse, error) {
	return vpc.CreateDeleteRouteEntryResponse(), nil
}

func TestAliyunLifecycleListsAllPendingActionsAndCanonicalizesInstances(t *testing.T) {
	requests := 0
	client := &lifecycleESSStub{
		describeActions: func(request *ess.DescribeLifecycleActionsRequest) (*ess.DescribeLifecycleActionsResponse, error) {
			requests++
			require.Equal(t, "Pending", request.LifecycleActionStatus)
			response := ess.CreateDescribeLifecycleActionsResponse()
			if requests == 1 {
				require.Empty(t, request.NextToken)
				response.NextToken = "next"
				response.LifecycleActions.LifecycleAction = []ess.LifecycleAction{{
					LifecycleHookId: "hook-out", LifecycleActionToken: "token-1",
					InstanceIds: ess.InstanceIds{InstanceId: []string{"i-2", "i-1", "i-1"}},
				}}
			} else {
				require.Equal(t, "next", request.NextToken)
				response.LifecycleActions.LifecycleAction = []ess.LifecycleAction{{
					LifecycleHookId: "hook-in", LifecycleActionToken: "token-2",
					InstanceIds: ess.InstanceIds{InstanceId: []string{"i-3"}},
				}}
			}
			return response, nil
		},
		describeNodes: func(*ess.DescribeScalingInstancesRequest) (*ess.DescribeScalingInstancesResponse, error) {
			return ess.CreateDescribeScalingInstancesResponse(), nil
		},
	}
	cloud, err := newAliyunCloud(client, lifecycleVPCStub{}, "asg-1", []string{"rt-1"})
	require.NoError(t, err)
	actions, err := cloud.ListPendingLifecycleActions(context.Background())
	require.NoError(t, err)
	require.Equal(t, 2, requests)
	require.Equal(t, []string{"i-1", "i-2"}, actions[0].InstanceIDs)
	require.Equal(t, "token-2", actions[1].Token)
}

func TestAliyunLifecycleAcceptsScaleOutAbandonWithIdempotencyToken(t *testing.T) {
	client := &lifecycleESSStub{
		describeActions: func(*ess.DescribeLifecycleActionsRequest) (*ess.DescribeLifecycleActionsResponse, error) {
			return ess.CreateDescribeLifecycleActionsResponse(), nil
		},
		describeNodes: func(*ess.DescribeScalingInstancesRequest) (*ess.DescribeScalingInstancesResponse, error) {
			return ess.CreateDescribeScalingInstancesResponse(), nil
		},
	}
	cloud, err := newAliyunCloud(client, lifecycleVPCStub{}, "asg-1", []string{"rt-1"})
	require.NoError(t, err)
	action := Action{Token: "token", HookID: "hook-out", InstanceIDs: []string{"i-1"}}
	require.NoError(t, cloud.CompleteLifecycleAction(context.Background(), action, LifecycleAbandon))
	require.Equal(t, LifecycleAbandon, client.completed.LifecycleActionResult)
	require.Len(t, client.completed.ClientToken, 64)

	client.completed = nil
	require.NoError(t, cloud.HeartbeatLifecycleAction(context.Background(), action, 2*time.Minute))
}

func TestAliyunLifecycleRequiresExactInServiceMembership(t *testing.T) {
	client := &lifecycleESSStub{
		describeActions: func(*ess.DescribeLifecycleActionsRequest) (*ess.DescribeLifecycleActionsResponse, error) {
			return ess.CreateDescribeLifecycleActionsResponse(), nil
		},
		describeNodes: func(request *ess.DescribeScalingInstancesRequest) (*ess.DescribeScalingInstancesResponse, error) {
			response := ess.CreateDescribeScalingInstancesResponse()
			response.ScalingInstances.ScalingInstance = []ess.ScalingInstance{
				{ScalingGroupId: "asg-1", InstanceId: "i-1", LifecycleState: "InService"},
				{ScalingGroupId: "asg-1", InstanceId: "i-2", LifecycleState: "Pending"},
			}
			return response, nil
		},
	}
	cloud, err := newAliyunCloud(client, lifecycleVPCStub{}, "asg-1", []string{"rt-1"})
	require.NoError(t, err)
	states, err := cloud.ElasticInstancesInService(context.Background(), []string{"i-1", "i-2"})
	require.NoError(t, err)
	require.True(t, states["i-1"])
	require.False(t, states["i-2"])
}
