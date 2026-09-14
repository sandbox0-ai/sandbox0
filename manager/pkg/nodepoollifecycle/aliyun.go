package nodepoollifecycle

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/aliyunclient"

	"github.com/aliyun/alibaba-cloud-sdk-go/sdk/requests"
	"github.com/aliyun/alibaba-cloud-sdk-go/services/ess"
	"github.com/aliyun/alibaba-cloud-sdk-go/services/vpc"
)

type aliyunLifecycleESS interface {
	DescribeScalingActivities(*ess.DescribeScalingActivitiesRequest) (*ess.DescribeScalingActivitiesResponse, error)
	DescribeScalingInstances(*ess.DescribeScalingInstancesRequest) (*ess.DescribeScalingInstancesResponse, error)
	DescribeLifecycleActions(*ess.DescribeLifecycleActionsRequest) (*ess.DescribeLifecycleActionsResponse, error)
	RecordLifecycleActionHeartbeat(*ess.RecordLifecycleActionHeartbeatRequest) (*ess.RecordLifecycleActionHeartbeatResponse, error)
	CompleteLifecycleAction(*ess.CompleteLifecycleActionRequest) (*ess.CompleteLifecycleActionResponse, error)
	SetInstancesProtection(*ess.SetInstancesProtectionRequest) (*ess.SetInstancesProtectionResponse, error)
}

func (c *AliyunCloud) ElasticInstancesInService(
	ctx context.Context,
	instanceIDs []string,
) (map[string]bool, error) {
	result := make(map[string]bool, len(instanceIDs))
	for start := 0; start < len(instanceIDs); start += 20 {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		end := min(start+20, len(instanceIDs))
		chunk := append([]string(nil), instanceIDs[start:end]...)
		request := ess.CreateDescribeScalingInstancesRequest()
		request.ScalingGroupId = c.scalingGroupID
		request.InstanceId = &chunk
		request.PageNumber = requests.NewInteger(1)
		request.PageSize = requests.NewInteger(50)
		response, err := c.ess.DescribeScalingInstances(request)
		if err != nil {
			return nil, err
		}
		for _, instance := range response.ScalingInstances.ScalingInstance {
			if instance.ScalingGroupId != c.scalingGroupID || !slices.Contains(chunk, instance.InstanceId) {
				return nil, errors.New("aliyun returned an unexpected elastic instance")
			}
			result[instance.InstanceId] = instance.LifecycleState == "InService" || instance.LifecycleState == "Protected"
		}
	}
	return result, nil
}

type aliyunLifecycleVPC interface {
	DescribeRouteEntryList(*vpc.DescribeRouteEntryListRequest) (*vpc.DescribeRouteEntryListResponse, error)
	DeleteRouteEntry(*vpc.DeleteRouteEntryRequest) (*vpc.DeleteRouteEntryResponse, error)
}

type AliyunCloud struct {
	ess            aliyunLifecycleESS
	vpc            aliyunLifecycleVPC
	scalingGroupID string
	routeTableIDs  []string
}

func NewAliyunCloud(region, scalingGroupID string, routeTableIDs []string) (*AliyunCloud, error) {
	credential := aliyunclient.Credentials()
	essClient, err := ess.NewClientWithOptions(region, aliyunclient.Config(), credential)
	if err != nil {
		return nil, err
	}
	vpcClient, err := vpc.NewClientWithOptions(region, aliyunclient.Config(), credential)
	if err != nil {
		return nil, err
	}
	return newAliyunCloud(essClient, vpcClient, scalingGroupID, routeTableIDs)
}

func newAliyunCloud(
	essClient aliyunLifecycleESS,
	vpcClient aliyunLifecycleVPC,
	scalingGroupID string,
	routeTableIDs []string,
) (*AliyunCloud, error) {
	scalingGroupID = strings.TrimSpace(scalingGroupID)
	cleanRoutes := make([]string, 0, len(routeTableIDs))
	for _, routeID := range routeTableIDs {
		routeID = strings.TrimSpace(routeID)
		if routeID == "" {
			return nil, errors.New("aliyun lifecycle route table ID is empty")
		}
		if !slices.Contains(cleanRoutes, routeID) {
			cleanRoutes = append(cleanRoutes, routeID)
		}
	}
	if essClient == nil || vpcClient == nil || scalingGroupID == "" || len(cleanRoutes) == 0 {
		return nil, errors.New("aliyun lifecycle cloud config is incomplete")
	}
	return &AliyunCloud{ess: essClient, vpc: vpcClient,
		scalingGroupID: scalingGroupID, routeTableIDs: cleanRoutes}, nil
}

// ListPendingLifecycleActions resolves current scaling activities for this exact
// group before querying their hooks. Instance creation activity IDs cannot be
// reused here because scale-in has a different activity.
func (c *AliyunCloud) ListPendingLifecycleActions(ctx context.Context) ([]Action, error) {
	activityIDs, err := c.inProgressScalingActivities(ctx)
	if err != nil {
		return nil, err
	}
	var actions []Action
	for _, activityID := range activityIDs {
		observed, err := c.pendingActivityActions(ctx, activityID)
		if err != nil {
			return nil, err
		}
		actions = append(actions, observed...)
	}
	return actions, nil
}

func (c *AliyunCloud) inProgressScalingActivities(ctx context.Context) ([]string, error) {
	var ids []string
	seen := make(map[string]bool)
	for page := 1; ; page++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		request := ess.CreateDescribeScalingActivitiesRequest()
		request.ScalingGroupId = c.scalingGroupID
		request.StatusCode = "InProgress"
		request.PageNumber = requests.NewInteger(page)
		request.PageSize = requests.NewInteger(50)
		response, err := c.ess.DescribeScalingActivities(request)
		if err != nil {
			return nil, err
		}
		for _, activity := range response.ScalingActivities.ScalingActivity {
			if activity.ScalingGroupId != c.scalingGroupID || strings.TrimSpace(activity.ScalingActivityId) == "" {
				return nil, errors.New("aliyun returned an unexpected scaling activity")
			}
			if activity.StatusCode == "InProgress" && !seen[activity.ScalingActivityId] {
				seen[activity.ScalingActivityId] = true
				ids = append(ids, activity.ScalingActivityId)
			}
		}
		if len(response.ScalingActivities.ScalingActivity) == 0 || page*50 >= response.TotalCount {
			break
		}
	}
	return ids, nil
}

func (c *AliyunCloud) pendingActivityActions(ctx context.Context, activityID string) ([]Action, error) {
	var actions []Action
	var nextToken string
	seenTokens := make(map[string]bool)
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		request := ess.CreateDescribeLifecycleActionsRequest()
		request.ScalingActivityId = activityID
		request.LifecycleActionStatus = "Pending"
		request.MaxResults = requests.NewInteger(50)
		request.NextToken = nextToken
		response, err := c.ess.DescribeLifecycleActions(request)
		if err != nil {
			return nil, err
		}
		for _, observed := range response.LifecycleActions.LifecycleAction {
			instanceIDs := append([]string(nil), observed.InstanceIds.InstanceId...)
			slices.Sort(instanceIDs)
			instanceIDs = slices.Compact(instanceIDs)
			if strings.TrimSpace(observed.LifecycleActionToken) == "" ||
				strings.TrimSpace(observed.LifecycleHookId) == "" || len(instanceIDs) == 0 {
				return nil, errors.New("aliyun returned an incomplete lifecycle action")
			}
			actions = append(actions, Action{
				Token: observed.LifecycleActionToken, HookID: observed.LifecycleHookId,
				InstanceIDs: instanceIDs,
			})
		}
		nextToken = strings.TrimSpace(response.NextToken)
		if nextToken == "" {
			break
		}
		if seenTokens[nextToken] {
			return nil, errors.New("aliyun repeated a lifecycle action pagination token")
		}
		seenTokens[nextToken] = true
	}
	return actions, nil
}

func (c *AliyunCloud) HeartbeatLifecycleAction(
	ctx context.Context,
	action Action,
	timeout time.Duration,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	request := ess.CreateRecordLifecycleActionHeartbeatRequest()
	request.LifecycleActionToken = action.Token
	request.LifecycleHookId = action.HookID
	request.HeartbeatTimeout = requests.NewInteger(int(timeout.Seconds()))
	_, err := c.ess.RecordLifecycleActionHeartbeat(request)
	return err
}

func (c *AliyunCloud) CompleteLifecycleAction(
	ctx context.Context,
	action Action,
	result string,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if result != LifecycleContinue && result != LifecycleRollback && result != LifecycleAbandon {
		return errors.New("aliyun lifecycle completion result is invalid")
	}
	request := ess.CreateCompleteLifecycleActionRequest()
	request.LifecycleActionToken = action.Token
	request.LifecycleHookId = action.HookID
	request.LifecycleActionResult = result
	digest := sha256.Sum256([]byte(action.Token + "\x00" + result))
	request.ClientToken = hex.EncodeToString(digest[:])
	_, err := c.ess.CompleteLifecycleAction(request)
	return err
}

func (c *AliyunCloud) SetInstancesProtection(
	ctx context.Context,
	instanceIDs []string,
	protected bool,
) error {
	for start := 0; start < len(instanceIDs); start += 20 {
		if err := ctx.Err(); err != nil {
			return err
		}
		end := min(start+20, len(instanceIDs))
		chunk := append([]string(nil), instanceIDs[start:end]...)
		request := ess.CreateSetInstancesProtectionRequest()
		request.ScalingGroupId = c.scalingGroupID
		request.InstanceId = &chunk
		request.ProtectedFromScaleIn = requests.NewBoolean(protected)
		if _, err := c.ess.SetInstancesProtection(request); err != nil {
			return err
		}
	}
	return nil
}

func (c *AliyunCloud) DeleteAllocationRoutes(
	ctx context.Context,
	instanceID, allocationCIDR string,
) error {
	for _, routeTableID := range c.routeTableIDs {
		if err := ctx.Err(); err != nil {
			return err
		}
		describe := vpc.CreateDescribeRouteEntryListRequest()
		describe.RouteTableId = routeTableID
		describe.DestinationCidrBlock = allocationCIDR
		describe.NextHopId = instanceID
		describe.NextHopType = "Instance"
		describe.MaxResult = requests.NewInteger(100)
		response, err := c.vpc.DescribeRouteEntryList(describe)
		if err != nil {
			return err
		}
		entries := response.RouteEntrys.RouteEntry
		if len(entries) == 0 {
			continue
		}
		if len(entries) != 1 || entries[0].RouteTableId != routeTableID ||
			entries[0].DestinationCidrBlock != allocationCIDR {
			return fmt.Errorf("allocation route lookup in %s is not exact", routeTableID)
		}
		entry := entries[0]
		if entry.InstanceId != "" && entry.InstanceId != instanceID {
			return fmt.Errorf("allocation route in %s belongs to another instance", routeTableID)
		}
		if len(entry.NextHops.NextHop) > 0 &&
			!slices.ContainsFunc(entry.NextHops.NextHop, func(hop vpc.NextHop) bool {
				return hop.NextHopId == instanceID && hop.NextHopType == "Instance"
			}) {
			return fmt.Errorf("allocation route in %s has an unexpected next hop", routeTableID)
		}
		remove := vpc.CreateDeleteRouteEntryRequest()
		remove.RouteTableId = routeTableID
		remove.RouteEntryId = entry.RouteEntryId
		remove.DestinationCidrBlock = allocationCIDR
		remove.NextHopId = instanceID
		if _, err := c.vpc.DeleteRouteEntry(remove); err != nil {
			return err
		}
	}
	return nil
}
