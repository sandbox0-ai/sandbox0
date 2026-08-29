package nodeenrollment

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"

	"github.com/aliyun/alibaba-cloud-sdk-go/sdk/requests"
	"github.com/aliyun/alibaba-cloud-sdk-go/services/ecs"
	"github.com/aliyun/alibaba-cloud-sdk-go/services/ess"
	"github.com/aliyun/alibaba-cloud-sdk-go/services/vpc"
)

type aliyunESSInstances interface {
	DescribeScalingInstances(*ess.DescribeScalingInstancesRequest) (*ess.DescribeScalingInstancesResponse, error)
}

type aliyunECSNetwork interface {
	DescribeNetworkInterfaces(*ecs.DescribeNetworkInterfacesRequest) (*ecs.DescribeNetworkInterfacesResponse, error)
	ModifyNetworkInterfaceAttribute(*ecs.ModifyNetworkInterfaceAttributeRequest) (*ecs.ModifyNetworkInterfaceAttributeResponse, error)
}

type aliyunVPCRoutes interface {
	DescribeRouteEntryList(*vpc.DescribeRouteEntryListRequest) (*vpc.DescribeRouteEntryListResponse, error)
	CreateRouteEntry(*vpc.CreateRouteEntryRequest) (*vpc.CreateRouteEntryResponse, error)
}

// AliyunCloud validates live ESS membership and prepares routed ECS workers.
type AliyunCloud struct {
	ess            aliyunESSInstances
	ecs            aliyunECSNetwork
	vpc            aliyunVPCRoutes
	scalingGroupID string
	routeTableIDs  []string
}

func NewAliyunCloud(
	region, scalingGroupID string,
	routeTableIDs []string,
) (*AliyunCloud, error) {
	essClient, err := ess.NewClientWithProvider(region)
	if err != nil {
		return nil, err
	}
	ecsClient, err := ecs.NewClientWithProvider(region)
	if err != nil {
		return nil, err
	}
	vpcClient, err := vpc.NewClientWithProvider(region)
	if err != nil {
		return nil, err
	}
	return newAliyunCloud(essClient, ecsClient, vpcClient, scalingGroupID, routeTableIDs)
}

func newAliyunCloud(
	essClient aliyunESSInstances,
	ecsClient aliyunECSNetwork,
	vpcClient aliyunVPCRoutes,
	scalingGroupID string,
	routeTableIDs []string,
) (*AliyunCloud, error) {
	scalingGroupID = strings.TrimSpace(scalingGroupID)
	cleanRoutes := make([]string, 0, len(routeTableIDs))
	seen := make(map[string]struct{}, len(routeTableIDs))
	for _, value := range routeTableIDs {
		value = strings.TrimSpace(value)
		if value == "" {
			return nil, errors.New("aliyun route table ID is empty")
		}
		if _, ok := seen[value]; !ok {
			seen[value] = struct{}{}
			cleanRoutes = append(cleanRoutes, value)
		}
	}
	if essClient == nil || ecsClient == nil || vpcClient == nil || scalingGroupID == "" || len(cleanRoutes) == 0 {
		return nil, errors.New("aliyun node enrollment cloud config is incomplete")
	}
	return &AliyunCloud{ess: essClient, ecs: ecsClient, vpc: vpcClient,
		scalingGroupID: scalingGroupID, routeTableIDs: cleanRoutes}, nil
}

func (c *AliyunCloud) ValidateElasticInstance(ctx context.Context, identity AliyunInstanceIdentity) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	request := ess.CreateDescribeScalingInstancesRequest()
	request.ScalingGroupId = c.scalingGroupID
	ids := []string{identity.InstanceID}
	request.InstanceId = &ids
	request.PageNumber = requests.NewInteger(1)
	request.PageSize = requests.NewInteger(10)
	response, err := c.ess.DescribeScalingInstances(request)
	if err != nil {
		return err
	}
	instances := response.ScalingInstances.ScalingInstance
	if len(instances) != 1 {
		return fmt.Errorf("instance is not an exact member of ESS group %q", c.scalingGroupID)
	}
	instance := instances[0]
	if instance.InstanceId != identity.InstanceID || instance.ScalingGroupId != c.scalingGroupID ||
		instance.PrivateIpAddress != identity.PrivateIPv4 || instance.InstanceType != identity.InstanceType ||
		(instance.LifecycleState != "InService" && instance.LifecycleState != "Pending") {
		return errors.New("ESS instance membership differs from signed identity")
	}
	return nil
}

func (c *AliyunCloud) PrepareElasticInstance(
	ctx context.Context,
	instanceID, allocationCIDR string,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	request := ecs.CreateDescribeNetworkInterfacesRequest()
	request.InstanceId = instanceID
	request.Type = "Primary"
	request.PageNumber = requests.NewInteger(1)
	request.PageSize = requests.NewInteger(10)
	response, err := c.ecs.DescribeNetworkInterfaces(request)
	if err != nil {
		return err
	}
	interfaces := response.NetworkInterfaceSets.NetworkInterfaceSet
	if len(interfaces) != 1 || interfaces[0].InstanceId != instanceID || interfaces[0].Type != "Primary" {
		return errors.New("elastic instance does not have one exact primary network interface")
	}
	modify := ecs.CreateModifyNetworkInterfaceAttributeRequest()
	modify.NetworkInterfaceId = interfaces[0].NetworkInterfaceId
	modify.SourceDestCheck = requests.NewBoolean(false)
	if _, err := c.ecs.ModifyNetworkInterfaceAttribute(modify); err != nil {
		return fmt.Errorf("disable source/destination check: %w", err)
	}
	for _, routeTableID := range c.routeTableIDs {
		describe := vpc.CreateDescribeRouteEntryListRequest()
		describe.RouteTableId = routeTableID
		describe.DestinationCidrBlock = allocationCIDR
		describe.MaxResult = requests.NewInteger(100)
		observed, err := c.vpc.DescribeRouteEntryList(describe)
		if err != nil {
			return fmt.Errorf("describe elastic allocation route in %s: %w", routeTableID, err)
		}
		entries := observed.RouteEntrys.RouteEntry
		if len(entries) > 0 {
			if len(entries) != 1 || entries[0].RouteTableId != routeTableID ||
				entries[0].DestinationCidrBlock != allocationCIDR ||
				!routeTargetsInstance(entries[0], instanceID) {
				return fmt.Errorf("elastic allocation route in %s conflicts with another next hop", routeTableID)
			}
			continue
		}
		route := vpc.CreateCreateRouteEntryRequest()
		route.RouteTableId = routeTableID
		route.DestinationCidrBlock = allocationCIDR
		route.NextHopType = "Instance"
		route.NextHopId = instanceID
		route.RouteEntryName = "sandbox0-elastic"
		digest := sha256.Sum256([]byte(routeTableID + "\x00" + allocationCIDR + "\x00" + instanceID))
		route.ClientToken = hex.EncodeToString(digest[:])
		if _, err := c.vpc.CreateRouteEntry(route); err != nil {
			return fmt.Errorf("create elastic allocation route in %s: %w", routeTableID, err)
		}
	}
	return nil
}

func routeTargetsInstance(entry vpc.RouteEntry, instanceID string) bool {
	if entry.InstanceId != "" && entry.InstanceId != instanceID {
		return false
	}
	if len(entry.NextHops.NextHop) == 0 {
		return entry.InstanceId == instanceID
	}
	for _, hop := range entry.NextHops.NextHop {
		if hop.NextHopId == instanceID && hop.NextHopType == "Instance" {
			return true
		}
	}
	return false
}
