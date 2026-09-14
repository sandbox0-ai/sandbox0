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
	describeActivities func(*ess.DescribeScalingActivitiesRequest) (*ess.DescribeScalingActivitiesResponse, error)
	describeActions    func(*ess.DescribeLifecycleActionsRequest) (*ess.DescribeLifecycleActionsResponse, error)
	describeNodes      func(*ess.DescribeScalingInstancesRequest) (*ess.DescribeScalingInstancesResponse, error)
	completed          *ess.CompleteLifecycleActionRequest
}

func (s *lifecycleESSStub) DescribeScalingActivities(request *ess.DescribeScalingActivitiesRequest) (*ess.DescribeScalingActivitiesResponse, error) {
	return s.describeActivities(request)
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
	activityRequests := 0
	client := &lifecycleESSStub{
		describeActivities: func(request *ess.DescribeScalingActivitiesRequest) (*ess.DescribeScalingActivitiesResponse, error) {
			activityRequests++
			require.Equal(t, "asg-1", request.ScalingGroupId)
			require.Equal(t, "InProgress", request.StatusCode)
			require.Equal(t, "50", string(request.PageSize))
			response := ess.CreateDescribeScalingActivitiesResponse()
			response.TotalCount = 51
			id := "activity-out"
			if activityRequests == 2 {
				require.Equal(t, "2", string(request.PageNumber))
				id = "activity-in"
			}
			response.ScalingActivities.ScalingActivity = []ess.ScalingActivity{{
				ScalingActivityId: id, ScalingGroupId: "asg-1", StatusCode: "InProgress",
			}}
			return response, nil
		},
		describeActions: func(request *ess.DescribeLifecycleActionsRequest) (*ess.DescribeLifecycleActionsResponse, error) {
			requests++
			require.Equal(t, "Pending", request.LifecycleActionStatus)
			require.Equal(t, "50", string(request.MaxResults))
			response := ess.CreateDescribeLifecycleActionsResponse()
			if request.ScalingActivityId == "activity-in" {
				require.Empty(t, request.NextToken)
				response.LifecycleActions.LifecycleAction = []ess.LifecycleAction{{
					LifecycleHookId: "hook-in", LifecycleActionToken: "token-3",
					InstanceIds: ess.InstanceIds{InstanceId: []string{"i-4"}},
				}}
				return response, nil
			}
			require.Equal(t, "activity-out", request.ScalingActivityId)
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
	require.Equal(t, 2, activityRequests)
	require.Equal(t, 3, requests)
	require.Equal(t, []string{"i-1", "i-2"}, actions[0].InstanceIDs)
	require.Equal(t, "token-2", actions[1].Token)
	require.Equal(t, "token-3", actions[2].Token)
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
				{ScalingGroupId: "asg-1", InstanceId: "i-3", LifecycleState: "Protected"},
			}
			return response, nil
		},
	}
	cloud, err := newAliyunCloud(client, lifecycleVPCStub{}, "asg-1", []string{"rt-1"})
	require.NoError(t, err)
	states, err := cloud.ElasticInstancesInService(context.Background(), []string{"i-1", "i-2", "i-3"})
	require.NoError(t, err)
	require.True(t, states["i-1"])
	require.False(t, states["i-2"])
	require.True(t, states["i-3"])
}

func TestAliyunLifecycleIgnoresFinishedActivitiesAndRejectsOtherGroups(t *testing.T) {
	for _, group := range []string{"asg-1", "other"} {
		t.Run(group, func(t *testing.T) {
			client := &lifecycleESSStub{describeActivities: func(*ess.DescribeScalingActivitiesRequest) (*ess.DescribeScalingActivitiesResponse, error) {
				response := ess.CreateDescribeScalingActivitiesResponse()
				response.TotalCount = 1
				response.ScalingActivities.ScalingActivity = []ess.ScalingActivity{{
					ScalingGroupId: group, ScalingActivityId: "finished", StatusCode: "Successful",
				}}
				return response, nil
			}}
			cloud, err := newAliyunCloud(client, lifecycleVPCStub{}, "asg-1", []string{"rt-1"})
			require.NoError(t, err)
			actions, err := cloud.ListPendingLifecycleActions(context.Background())
			if group == "other" {
				require.ErrorContains(t, err, "unexpected scaling activity")
			} else {
				require.NoError(t, err)
			}
			require.Empty(t, actions)
		})
	}
}

func TestAliyunLifecycleRejectsRepeatedActionPaginationTokens(t *testing.T) {
	requests := 0
	client := &lifecycleESSStub{
		describeActivities: func(*ess.DescribeScalingActivitiesRequest) (*ess.DescribeScalingActivitiesResponse, error) {
			response := ess.CreateDescribeScalingActivitiesResponse()
			response.TotalCount = 1
			response.ScalingActivities.ScalingActivity = []ess.ScalingActivity{{
				ScalingGroupId: "asg-1", ScalingActivityId: "active", StatusCode: "InProgress",
			}}
			return response, nil
		},
		describeActions: func(*ess.DescribeLifecycleActionsRequest) (*ess.DescribeLifecycleActionsResponse, error) {
			requests++
			response := ess.CreateDescribeLifecycleActionsResponse()
			response.NextToken = "repeated"
			return response, nil
		},
	}
	cloud, err := newAliyunCloud(client, lifecycleVPCStub{}, "asg-1", []string{"rt-1"})
	require.NoError(t, err)
	_, err = cloud.ListPendingLifecycleActions(context.Background())
	require.ErrorContains(t, err, "repeated a lifecycle action pagination token")
	require.Equal(t, 2, requests)
}
