package nomadruntime

import (
	"context"
	"fmt"
	"reflect"
	"time"

	"github.com/containerd/errdefs"
	"github.com/sandbox0-ai/sandbox0/pkg/processidentity"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshandoff"
	rootfssession "github.com/sandbox0-ai/sandbox0/pkg/rootfssession"
)

func (r *rootfsRuntime) AdoptLiveConsumer(ctx context.Context, stage rootfshandoff.StageRequest, consumer ConsumerRequest) (ConsumerLease, error) {
	if stage.ValidateDurableBinding() != nil || consumer.RenewalProtocol != 1 || consumer.OwnerProcess == "" {
		return ConsumerLease{}, fmt.Errorf("invalid live consumer adoption: %w", errdefs.ErrInvalidArgument)
	}
	current, err := r.sessions.RecoverySession(stage.Parent)
	if err != nil {
		return ConsumerLease{}, err
	}
	old := current.Consumer
	if !current.Live || current.State != "ready" || old == nil || old.RenewalProtocol != 1 || old.OwnerProcess == "" || !reflect.DeepEqual(current.Stage, stage.WithoutWriterGrantToken()) {
		return ConsumerLease{}, fmt.Errorf("live consumer adoption has no exact serving owner: %w", errdefs.ErrFailedPrecondition)
	}
	// Registration stores canonical paths. Nomad may preserve the equivalent
	// /var/run alias in its task config, so resolve against the same allowed
	// roots before comparing; namespace identity is still checked at register.
	if r.consumerMountRoot != "" {
		consumer.StableMount, err = validateRootfsPath(consumer.StableMount, r.consumerMountRoot)
		if err != nil {
			return ConsumerLease{}, fmt.Errorf("validate live consumer mount: %w", err)
		}
	}
	if consumer.NetNSPath != "" {
		if r.consumerNetNSRoot == "" {
			return ConsumerLease{}, fmt.Errorf("live consumer network root is unavailable: %w", errdefs.ErrFailedPrecondition)
		}
		consumer.NetNSPath, err = validateExistingPath(consumer.NetNSPath, r.consumerNetNSRoot)
		if err != nil {
			return ConsumerLease{}, fmt.Errorf("validate live consumer network path: %w", err)
		}
	}
	if old.ActiveKey != consumer.ActiveKey || old.ContainerID != consumer.ContainerID || old.StableMount != consumer.StableMount || old.HostMountNamespace != consumer.HostMountNamespace || old.NetNSPath != consumer.NetNSPath || old.NetNSIdentity != consumer.NetNSIdentity || old.NetworkChain != consumer.NetworkChain {
		return ConsumerLease{}, fmt.Errorf("live consumer adoption changed runtime identity: %w", errdefs.ErrPermissionDenied)
	}
	expires, err := time.Parse(time.RFC3339Nano, old.LeaseExpiresAt)
	if err != nil || !time.Now().Before(expires) {
		return ConsumerLease{}, fmt.Errorf("live consumer adoption lease expired: %w", errdefs.ErrFailedPrecondition)
	}
	if old.OwnerProcess != consumer.OwnerProcess {
		alive, err := processidentity.Alive(old.OwnerProcess)
		if err != nil {
			return ConsumerLease{}, err
		}
		if alive {
			return ConsumerLease{}, fmt.Errorf("previous task driver is still alive: %w", errdefs.ErrFailedPrecondition)
		}
	}
	alive, err := processidentity.Alive(consumer.OwnerProcess)
	if err != nil || !alive {
		return ConsumerLease{}, fmt.Errorf("new task driver process is absent: %w", errdefs.ErrFailedPrecondition)
	}
	if _, err := r.liveWriterRequests(ctx, rootfssession.LiveHandoff{Version: 1, Sessions: []rootfssession.LiveHandoffSession{{Parent: stage.Parent}}}, nil); err != nil {
		return ConsumerLease{}, err
	}
	return r.registerConsumer(stage, consumer, old.LeaseID)
}

func (c *Client) AdoptLiveConsumer(ctx context.Context, stage rootfshandoff.StageRequest, consumer ConsumerRequest) (ConsumerLease, error) {
	var response nodeRuntimeRPCResponse
	err := c.call(ctx, "/v1/sessions/consumer/adopt", nodeRuntimeRPCRequest{Stage: stage.WithoutWriterGrantToken(), Consumer: consumer}, &response)
	return response.Lease, err
}
