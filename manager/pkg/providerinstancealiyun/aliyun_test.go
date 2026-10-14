package providerinstancealiyun

import (
	"context"
	"errors"
	"fmt"
	"testing"

	sdkerrors "github.com/aliyun/alibaba-cloud-sdk-go/sdk/errors"
	"github.com/aliyun/alibaba-cloud-sdk-go/services/ecs"
	"github.com/aliyun/alibaba-cloud-sdk-go/services/sts"
	"github.com/sandbox0-ai/sandbox0/pkg/providerinstance"
	"github.com/stretchr/testify/require"
)

type ecsReaderFunc func(*ecs.DescribeInstanceAttributeRequest) (*ecs.DescribeInstanceAttributeResponse, error)

func (f ecsReaderFunc) DescribeInstanceAttribute(r *ecs.DescribeInstanceAttributeRequest) (*ecs.DescribeInstanceAttributeResponse, error) {
	return f(r)
}

type stsReaderFunc func(*sts.GetCallerIdentityRequest) (*sts.GetCallerIdentityResponse, error)

func (f stsReaderFunc) GetCallerIdentity(r *sts.GetCallerIdentityRequest) (*sts.GetCallerIdentityResponse, error) {
	return f(r)
}

func verifiedIdentity(*sts.GetCallerIdentityRequest) (*sts.GetCallerIdentityResponse, error) {
	r := sts.CreateGetCallerIdentityResponse()
	r.AccountId, r.RequestId = "1234567890123456", "identity-receipt"
	return r, nil
}

func testScope() providerinstance.Scope {
	return providerinstance.Scope{Provider: "aliyun", AccountID: "1234567890123456", RegionID: "us-east-1", InstanceID: "i-exact"}
}

func providerError(status int, code, receipt string) error {
	return sdkerrors.NewServerError(status, fmt.Sprintf(`{"Code":%q,"RequestId":%q,"Message":"private error body"}`, code, receipt), "")
}

func TestAliyunObserverRequiresExplicitNotFoundInVerifiedScope(t *testing.T) {
	reader := ecsReaderFunc(func(r *ecs.DescribeInstanceAttributeRequest) (*ecs.DescribeInstanceAttributeResponse, error) {
		require.Equal(t, "i-exact", r.InstanceId)
		require.Equal(t, "us-east-1", r.RegionId)
		require.Equal(t, "1234567890123456", string(r.OwnerId))
		return nil, fmt.Errorf("wrapped: %w", providerError(404, "InvalidInstanceId.NotFound", "not-found-receipt"))
	})
	o, err := newAliyunObserver(reader, stsReaderFunc(verifiedIdentity), "us-east-1", "1234567890123456")
	require.NoError(t, err)
	observation, err := o.Observe(context.Background(), testScope())
	require.NoError(t, err)
	require.False(t, observation.Present)
	require.Equal(t, "not-found-receipt", observation.RequestID)
	require.Equal(t, "identity-receipt", observation.IdentityRequestID)
	require.NoError(t, observation.ValidateFor(testScope()))
}

func TestAliyunObserverNeverTreatsStoppedOrDetachedInstancesAsAbsent(t *testing.T) {
	for _, status := range []string{"Running", "Stopped", "Stopping", "Pending", "Starting", "UnknownFutureStatus"} {
		t.Run(status, func(t *testing.T) {
			reader := ecsReaderFunc(func(*ecs.DescribeInstanceAttributeRequest) (*ecs.DescribeInstanceAttributeResponse, error) {
				r := ecs.CreateDescribeInstanceAttributeResponse()
				r.InstanceId, r.RegionId, r.RequestId, r.Status = "i-exact", "us-east-1", "present-receipt", status
				return r, nil
			})
			o, err := newAliyunObserver(reader, stsReaderFunc(verifiedIdentity), "us-east-1", "1234567890123456")
			require.NoError(t, err)
			observation, err := o.Observe(context.Background(), testScope())
			require.NoError(t, err)
			require.True(t, observation.Present)
		})
	}
}

func TestAliyunObserverFailsClosedOnErrorsAndMalformedResponses(t *testing.T) {
	var nilServer *sdkerrors.ServerError
	for name, apiErr := range map[string]error{
		"typed_nil":       nilServer,
		"forbidden":       providerError(403, "Forbidden.RAM", "denied"),
		"wrong_status":    providerError(403, "InvalidInstanceId.NotFound", "denied"),
		"wrong_code":      providerError(404, "InvalidRegionId.NotFound", "other"),
		"missing_receipt": providerError(404, "InvalidInstanceId.NotFound", ""),
		"unavailable":     providerError(500, "InternalError", "error"),
		"transport":       errors.New("private transport error"),
		"nil_response":    nil,
	} {
		t.Run(name, func(t *testing.T) {
			reader := ecsReaderFunc(func(*ecs.DescribeInstanceAttributeRequest) (*ecs.DescribeInstanceAttributeResponse, error) {
				return nil, apiErr
			})
			o, err := newAliyunObserver(reader, stsReaderFunc(verifiedIdentity), "us-east-1", "1234567890123456")
			require.NoError(t, err)
			observation, err := o.Observe(context.Background(), testScope())
			require.Error(t, err)
			require.Zero(t, observation)
			require.NotContains(t, err.Error(), "private")
		})
	}
	for _, field := range []string{"instance", "region", "receipt"} {
		t.Run(field, func(t *testing.T) {
			reader := ecsReaderFunc(func(*ecs.DescribeInstanceAttributeRequest) (*ecs.DescribeInstanceAttributeResponse, error) {
				r := ecs.CreateDescribeInstanceAttributeResponse()
				r.InstanceId, r.RegionId, r.RequestId = "i-exact", "us-east-1", "present"
				switch field {
				case "instance":
					r.InstanceId = "i-other"
				case "region":
					r.RegionId = "cn-hangzhou"
				case "receipt":
					r.RequestId = ""
				}
				return r, nil
			})
			o, err := newAliyunObserver(reader, stsReaderFunc(verifiedIdentity), "us-east-1", "1234567890123456")
			require.NoError(t, err)
			_, err = o.Observe(context.Background(), testScope())
			require.Error(t, err)
		})
	}
}

func TestAliyunObserverChecksActualCredentialAccountBeforeECS(t *testing.T) {
	for _, account := range []string{"other-account", ""} {
		t.Run(account, func(t *testing.T) {
			calls := 0
			reader := ecsReaderFunc(func(*ecs.DescribeInstanceAttributeRequest) (*ecs.DescribeInstanceAttributeResponse, error) {
				calls++
				return nil, nil
			})
			identity := stsReaderFunc(func(r *sts.GetCallerIdentityRequest) (*sts.GetCallerIdentityResponse, error) {
				response, _ := verifiedIdentity(r)
				response.AccountId = account
				return response, nil
			})
			o, err := newAliyunObserver(reader, identity, "us-east-1", "1234567890123456")
			require.NoError(t, err)
			_, err = o.Observe(context.Background(), testScope())
			require.Error(t, err)
			require.Zero(t, calls)
		})
	}
}

func TestAliyunObserverRejectsScopeDriftAndCancellation(t *testing.T) {
	calls := 0
	reader := ecsReaderFunc(func(*ecs.DescribeInstanceAttributeRequest) (*ecs.DescribeInstanceAttributeResponse, error) {
		calls++
		return nil, nil
	})
	o, err := newAliyunObserver(reader, stsReaderFunc(verifiedIdentity), "us-east-1", "1234567890123456")
	require.NoError(t, err)
	for _, scope := range []providerinstance.Scope{
		{Provider: "aliyun", AccountID: "other", RegionID: "us-east-1", InstanceID: "i-exact"},
		{Provider: "aliyun", AccountID: "1234567890123456", RegionID: "cn-hangzhou", InstanceID: "i-exact"},
		{Provider: "other", AccountID: "1234567890123456", RegionID: "us-east-1", InstanceID: "i-exact"},
		{Provider: "aliyun", AccountID: "1234567890123456", RegionID: "us-east-1", InstanceID: ""},
	} {
		_, err := o.Observe(context.Background(), scope)
		require.Error(t, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = o.Observe(ctx, testScope())
	require.ErrorIs(t, err, context.Canceled)
	require.Zero(t, calls)
	ctx, cancel = context.WithCancel(context.Background())
	o.ecs = ecsReaderFunc(func(*ecs.DescribeInstanceAttributeRequest) (*ecs.DescribeInstanceAttributeResponse, error) {
		cancel()
		return nil, providerError(404, "InvalidInstanceId.NotFound", "not-found")
	})
	_, err = o.Observe(ctx, testScope())
	require.ErrorIs(t, err, context.Canceled)
}
