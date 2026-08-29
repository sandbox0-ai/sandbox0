package nodeenrollment

import (
	"context"
	"testing"

	"github.com/aliyun/alibaba-cloud-sdk-go/services/ecs"
	"github.com/aliyun/alibaba-cloud-sdk-go/services/ess"
	"github.com/aliyun/alibaba-cloud-sdk-go/services/vpc"
	"github.com/stretchr/testify/require"
)

type enrollmentESSClientStub struct{}

func (enrollmentESSClientStub) DescribeScalingInstances(*ess.DescribeScalingInstancesRequest) (*ess.DescribeScalingInstancesResponse, error) {
	return ess.CreateDescribeScalingInstancesResponse(), nil
}

type enrollmentECSClientStub struct{}

func (enrollmentECSClientStub) DescribeNetworkInterfaces(*ecs.DescribeNetworkInterfacesRequest) (*ecs.DescribeNetworkInterfacesResponse, error) {
	response := ecs.CreateDescribeNetworkInterfacesResponse()
	response.NetworkInterfaceSets.NetworkInterfaceSet = []ecs.NetworkInterfaceSet{{
		InstanceId: "i-1", Type: "Primary", NetworkInterfaceId: "eni-1",
	}}
	return response, nil
}
func (enrollmentECSClientStub) ModifyNetworkInterfaceAttribute(*ecs.ModifyNetworkInterfaceAttributeRequest) (*ecs.ModifyNetworkInterfaceAttributeResponse, error) {
	return ecs.CreateModifyNetworkInterfaceAttributeResponse(), nil
}

type enrollmentVPCClientStub struct {
	entries []vpc.RouteEntry
	creates int
}

func (v *enrollmentVPCClientStub) DescribeRouteEntryList(*vpc.DescribeRouteEntryListRequest) (*vpc.DescribeRouteEntryListResponse, error) {
	response := vpc.CreateDescribeRouteEntryListResponse()
	response.RouteEntrys.RouteEntry = append([]vpc.RouteEntry(nil), v.entries...)
	return response, nil
}
func (v *enrollmentVPCClientStub) CreateRouteEntry(*vpc.CreateRouteEntryRequest) (*vpc.CreateRouteEntryResponse, error) {
	v.creates++
	return vpc.CreateCreateRouteEntryResponse(), nil
}

func TestPrepareElasticInstanceReusesOnlyExactAllocationRoute(t *testing.T) {
	for _, test := range []struct {
		name        string
		entries     []vpc.RouteEntry
		wantCreates int
		wantError   bool
	}{
		{name: "create absent route", wantCreates: 1},
		{name: "reuse exact route", entries: []vpc.RouteEntry{{
			RouteTableId: "rt-1", DestinationCidrBlock: "172.27.0.0/26", InstanceId: "i-1",
		}}},
		{name: "reject conflicting route", wantError: true, entries: []vpc.RouteEntry{{
			RouteTableId: "rt-1", DestinationCidrBlock: "172.27.0.0/26", InstanceId: "i-other",
		}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			vpcClient := &enrollmentVPCClientStub{entries: test.entries}
			cloud, err := newAliyunCloud(enrollmentESSClientStub{}, enrollmentECSClientStub{},
				vpcClient, "asg-1", []string{"rt-1"})
			require.NoError(t, err)
			err = cloud.PrepareElasticInstance(context.Background(), "i-1", "172.27.0.0/26")
			if test.wantError {
				require.ErrorContains(t, err, "conflicts")
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, test.wantCreates, vpcClient.creates)
		})
	}
}
