package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"go.uber.org/zap"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
)

const (
	defaultManagerLeaderElectionName = "sandbox0-manager"
	serviceAccountNamespacePath      = "/var/run/secrets/kubernetes.io/serviceaccount/namespace"

	managerLeaderLeaseDuration = 15 * time.Second
	managerLeaderRenewDeadline = 10 * time.Second
	managerLeaderRetryPeriod   = 2 * time.Second
)

func runManagerLeaderElection(
	ctx context.Context,
	k8sClient kubernetes.Interface,
	logger *zap.Logger,
	startControllers func(context.Context),
	onLeadershipLost func(),
) error {
	if k8sClient == nil {
		return fmt.Errorf("kubernetes client is required for manager leader election")
	}
	if logger == nil {
		logger = zap.NewNop()
	}

	identity, err := managerLeaderElectionIdentity()
	if err != nil {
		return err
	}
	namespace := managerLeaderElectionNamespace()
	name := managerLeaderElectionName()
	lostLeadership := false
	elector, err := leaderelection.NewLeaderElector(leaderelection.LeaderElectionConfig{
		Lock: &resourcelock.LeaseLock{
			LeaseMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: namespace,
			},
			Client: k8sClient.CoordinationV1(),
			LockConfig: resourcelock.ResourceLockConfig{
				Identity: identity,
			},
		},
		LeaseDuration: managerLeaderLeaseDuration,
		RenewDeadline: managerLeaderRenewDeadline,
		RetryPeriod:   managerLeaderRetryPeriod,
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: func(leaderCtx context.Context) {
				logger.Info("Manager controller leadership acquired",
					zap.String("identity", identity),
					zap.String("lease", namespace+"/"+name),
				)
				startControllers(leaderCtx)
			},
			OnStoppedLeading: func() {
				if ctx.Err() != nil {
					return
				}
				lostLeadership = true
				logger.Error("Manager controller leadership lost",
					zap.String("identity", identity),
					zap.String("lease", namespace+"/"+name),
				)
				if onLeadershipLost != nil {
					onLeadershipLost()
				}
			},
			OnNewLeader: func(currentIdentity string) {
				if currentIdentity == identity {
					return
				}
				logger.Info("Manager controller leadership held by another replica",
					zap.String("identity", currentIdentity),
					zap.String("lease", namespace+"/"+name),
				)
			},
		},
		// Do not release the Lease before the leader-scoped controller
		// goroutines have fully stopped. The short expiry gap is preferable to
		// overlapping reconcilers during a rolling Deployment handoff.
		ReleaseOnCancel: false,
		Name:            name,
	})
	if err != nil {
		return fmt.Errorf("configure manager leader election: %w", err)
	}

	logger.Info("Waiting for manager controller leadership",
		zap.String("identity", identity),
		zap.String("lease", namespace+"/"+name),
	)
	elector.Run(ctx)
	if lostLeadership {
		return fmt.Errorf("manager controller leadership lost")
	}
	return nil
}

func managerLeaderElectionName() string {
	if name := strings.TrimSpace(os.Getenv(config.ManagerLeaderElectionNameEnv)); name != "" {
		return name
	}
	return defaultManagerLeaderElectionName
}

func managerLeaderElectionNamespace() string {
	if namespace := strings.TrimSpace(os.Getenv("POD_NAMESPACE")); namespace != "" {
		return namespace
	}
	if data, err := os.ReadFile(serviceAccountNamespacePath); err == nil {
		if namespace := strings.TrimSpace(string(data)); namespace != "" {
			return namespace
		}
	}
	return metav1.NamespaceDefault
}

func managerLeaderElectionIdentity() (string, error) {
	if identity := strings.TrimSpace(os.Getenv("POD_NAME")); identity != "" {
		return identity, nil
	}
	hostname, err := os.Hostname()
	if err != nil {
		return "", fmt.Errorf("resolve manager leader election identity: %w", err)
	}
	hostname = strings.TrimSpace(hostname)
	if hostname == "" {
		return "", fmt.Errorf("resolve manager leader election identity: hostname is empty")
	}
	return fmt.Sprintf("%s-%d", hostname, os.Getpid()), nil
}
