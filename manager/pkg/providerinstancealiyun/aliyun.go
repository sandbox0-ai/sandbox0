// Package providerinstancealiyun observes physical cloud instance presence. An
// observation is not itself authorization to release a runtime or its writer.
package providerinstancealiyun

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strconv"

	sdkerrors "github.com/aliyun/alibaba-cloud-sdk-go/sdk/errors"
	"github.com/aliyun/alibaba-cloud-sdk-go/services/ecs"
	"github.com/aliyun/alibaba-cloud-sdk-go/services/sts"
	"github.com/sandbox0-ai/sandbox0/pkg/aliyunclient"
	"github.com/sandbox0-ai/sandbox0/pkg/providerinstance"
)

type aliyunECS interface {
	DescribeInstanceAttribute(*ecs.DescribeInstanceAttributeRequest) (*ecs.DescribeInstanceAttributeResponse, error)
}

type aliyunSTS interface {
	GetCallerIdentity(*sts.GetCallerIdentityRequest) (*sts.GetCallerIdentityResponse, error)
}

type AliyunObserver struct {
	ecs       aliyunECS
	sts       aliyunSTS
	region    string
	accountID string
}

var (
	canonicalRegion   = regexp.MustCompile(`^[a-z0-9]+(?:-[a-z0-9]+)+$`)
	canonicalInstance = regexp.MustCompile(`^i-[a-z0-9]{1,64}$`)
	canonicalReceipt  = regexp.MustCompile(`^[A-Za-z0-9._:-]{1,256}$`)
)

// NewAliyunObserver pins the configured owner account and region. STS validates
// the actual refreshable credential's account on every observation.
func NewAliyunObserver(region, accountID string) (*AliyunObserver, error) {
	if _, err := validateAliyunScope(region, accountID); err != nil {
		return nil, err
	}
	credential := aliyunclient.Credentials()
	ecsClient, err := ecs.NewClientWithOptions(region, aliyunclient.Config(), credential)
	if err != nil {
		return nil, errors.New("initialize ECS instance observer")
	}
	stsClient, err := sts.NewClientWithOptions(region, aliyunclient.Config(), credential)
	if err != nil {
		return nil, errors.New("initialize STS identity observer")
	}
	return newAliyunObserver(ecsClient, stsClient, region, accountID)
}

func newAliyunObserver(ecsClient aliyunECS, stsClient aliyunSTS, region, accountID string) (*AliyunObserver, error) {
	_, err := validateAliyunScope(region, accountID)
	if err != nil {
		return nil, err
	}
	if ecsClient == nil || stsClient == nil {
		return nil, errors.New("ECS and STS instance observer clients are required")
	}
	return &AliyunObserver{ecs: ecsClient, sts: stsClient, region: region, accountID: accountID}, nil
}

func validateAliyunScope(region, accountID string) (int64, error) {
	ownerID, err := strconv.ParseInt(accountID, 10, 64)
	if err != nil || ownerID <= 0 || strconv.FormatInt(ownerID, 10) != accountID ||
		len(region) > 64 || !canonicalRegion.MatchString(region) {
		return 0, errors.New("instance observer requires a canonical owner account and region")
	}
	return ownerID, nil
}

func (o *AliyunObserver) Observe(ctx context.Context, scope providerinstance.Scope) (providerinstance.Observation, error) {
	if err := ctx.Err(); err != nil {
		return providerinstance.Observation{}, err
	}
	if o == nil || o.ecs == nil || o.sts == nil || scope.Provider != "aliyun" ||
		scope.AccountID != o.accountID || scope.RegionID != o.region ||
		!canonicalInstance.MatchString(scope.InstanceID) {
		return providerinstance.Observation{}, errors.New("provider instance target is outside the pinned ECS scope")
	}
	identity, err := o.sts.GetCallerIdentity(sts.CreateGetCallerIdentityRequest())
	if err := ctx.Err(); err != nil {
		return providerinstance.Observation{}, err
	}
	if err != nil || identity == nil || identity.AccountId != o.accountID ||
		!canonicalReceipt.MatchString(identity.RequestId) {
		return providerinstance.Observation{}, errors.New("provider credential owner account could not be verified")
	}
	request := ecs.CreateDescribeInstanceAttributeRequest()
	request.RegionId = o.region
	// OwnerId is exposed by the generated SDK but is not supported by this
	// operation. Its authenticated credential owner is pinned by STS above.
	request.InstanceId = scope.InstanceID
	response, err := o.ecs.DescribeInstanceAttribute(request)
	if ctxErr := ctx.Err(); ctxErr != nil {
		return providerinstance.Observation{}, ctxErr
	}
	observation := providerinstance.Observation{Scope: scope, IdentityRequestID: identity.RequestId}
	if err != nil {
		var server *sdkerrors.ServerError
		if !errors.As(err, &server) || server == nil || server.HttpStatus() != 404 ||
			server.ErrorCode() != "InvalidInstanceId.NotFound" ||
			!canonicalReceipt.MatchString(server.RequestId()) {
			// SDK error strings can include response headers. They are not part
			// of the durable proof or manager logs.
			return providerinstance.Observation{}, errors.New("ECS physical instance presence is unavailable")
		}
		observation.RequestID = server.RequestId()
	} else {
		if response == nil || response.InstanceId != scope.InstanceID || response.RegionId != o.region ||
			!canonicalReceipt.MatchString(response.RequestId) {
			return providerinstance.Observation{}, errors.New("ECS returned inconsistent physical instance identity")
		}
		observation.Present = true // Stopped and stopping instances still exist.
		observation.RequestID = response.RequestId
	}
	if err := observation.ValidateFor(scope); err != nil {
		return providerinstance.Observation{}, fmt.Errorf("validate ECS presence receipt: %w", err)
	}
	return observation, nil
}
