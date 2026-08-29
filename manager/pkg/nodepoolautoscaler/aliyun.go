package nodepoolautoscaler

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/aliyun/alibaba-cloud-sdk-go/sdk/requests"
	"github.com/aliyun/alibaba-cloud-sdk-go/services/ess"
)

type essDesiredCapacityClient interface {
	DescribeScalingGroups(*ess.DescribeScalingGroupsRequest) (*ess.DescribeScalingGroupsResponse, error)
	ModifyScalingGroup(*ess.ModifyScalingGroupRequest) (*ess.ModifyScalingGroupResponse, error)
}

// AliyunESS implements Cloud for one exact ESS scaling group. The SDK default
// credential chain supports the RAM role attached to manager's control host.
type AliyunESS struct {
	client         essDesiredCapacityClient
	scalingGroupID string
}

// NewAliyunESS uses the Alibaba Cloud default credential chain.
func NewAliyunESS(region, scalingGroupID string) (*AliyunESS, error) {
	region = strings.TrimSpace(region)
	scalingGroupID = strings.TrimSpace(scalingGroupID)
	if region == "" || scalingGroupID == "" {
		return nil, errors.New("aliyun ESS region and scaling group ID are required")
	}
	client, err := ess.NewClientWithProvider(region)
	if err != nil {
		return nil, fmt.Errorf("create Aliyun ESS client: %w", err)
	}
	return newAliyunESS(client, scalingGroupID)
}

func newAliyunESS(client essDesiredCapacityClient, scalingGroupID string) (*AliyunESS, error) {
	if client == nil || strings.TrimSpace(scalingGroupID) == "" {
		return nil, errors.New("aliyun ESS client and scaling group ID are required")
	}
	return &AliyunESS{client: client, scalingGroupID: strings.TrimSpace(scalingGroupID)}, nil
}

func (a *AliyunESS) DesiredCapacity(ctx context.Context) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	request := ess.CreateDescribeScalingGroupsRequest()
	ids := []string{a.scalingGroupID}
	request.ScalingGroupId = &ids
	request.PageNumber = requests.NewInteger(1)
	request.PageSize = requests.NewInteger(10)
	response, err := a.client.DescribeScalingGroups(request)
	if err != nil {
		return 0, err
	}
	groups := response.ScalingGroups.ScalingGroup
	if len(groups) != 1 || groups[0].ScalingGroupId != a.scalingGroupID {
		return 0, fmt.Errorf("aliyun ESS scaling group %q was not returned exactly once", a.scalingGroupID)
	}
	group := groups[0]
	if !group.EnableDesiredCapacity || group.MinSize != 0 || group.MaxSize != 299 {
		return 0, fmt.Errorf("aliyun ESS scaling group %q violates desired-capacity or 0..299 bounds", a.scalingGroupID)
	}
	return group.DesiredCapacity, nil
}

func (a *AliyunESS) SetDesiredCapacity(ctx context.Context, desired int) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if desired < 0 || desired > 299 {
		return fmt.Errorf("aliyun ESS desired capacity %d is outside 0..299", desired)
	}
	request := ess.CreateModifyScalingGroupRequest()
	request.ScalingGroupId = a.scalingGroupID
	request.MinSize = requests.NewInteger(0)
	request.MaxSize = requests.NewInteger(299)
	request.DesiredCapacity = requests.NewInteger(desired)
	request.DisableDesiredCapacity = requests.NewBoolean(false)
	_, err := a.client.ModifyScalingGroup(request)
	return err
}
