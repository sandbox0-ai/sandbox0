package nodepoolautoscaler

import (
	"context"
	"testing"

	"github.com/aliyun/alibaba-cloud-sdk-go/services/ess"
	"github.com/stretchr/testify/require"
)

type fakeESSClient struct {
	group    ess.ScalingGroup
	modified *ess.ModifyScalingGroupRequest
}

func (f *fakeESSClient) DescribeScalingGroups(*ess.DescribeScalingGroupsRequest) (*ess.DescribeScalingGroupsResponse, error) {
	response := ess.CreateDescribeScalingGroupsResponse()
	response.ScalingGroups.ScalingGroup = []ess.ScalingGroup{f.group}
	return response, nil
}

func (f *fakeESSClient) ModifyScalingGroup(request *ess.ModifyScalingGroupRequest) (*ess.ModifyScalingGroupResponse, error) {
	f.modified = request
	return ess.CreateModifyScalingGroupResponse(), nil
}

func TestAliyunESSRejectsDriftedProviderBounds(t *testing.T) {
	client := &fakeESSClient{group: ess.ScalingGroup{
		ScalingGroupId: "asg-1", EnableDesiredCapacity: true,
		MinSize: 0, MaxSize: 300, DesiredCapacity: 2,
	}}
	cloud, err := newAliyunESS(client, "asg-1")
	require.NoError(t, err)
	_, err = cloud.DesiredCapacity(context.Background())
	require.ErrorContains(t, err, "violates")
}

func TestAliyunESSWritesExactDesiredCapacityAndBounds(t *testing.T) {
	client := &fakeESSClient{}
	cloud, err := newAliyunESS(client, "asg-1")
	require.NoError(t, err)
	require.NoError(t, cloud.SetDesiredCapacity(context.Background(), 17))
	require.NotNil(t, client.modified)
	require.Equal(t, "asg-1", client.modified.ScalingGroupId)
	require.Equal(t, "0", string(client.modified.MinSize))
	require.Equal(t, "299", string(client.modified.MaxSize))
	require.Equal(t, "17", string(client.modified.DesiredCapacity))
}
