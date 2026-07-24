package apply

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/controller"
	"github.com/sandbox0-ai/sandbox0/netd/pkg/watcher"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
)

const appliedHashPatchConcurrency = 16

type Patcher struct {
	client kubernetes.Interface
	logger *zap.Logger
}

func NewPatcher(client kubernetes.Interface, logger *zap.Logger) *Patcher {
	if logger == nil {
		logger = zap.NewNop()
	}
	return &Patcher{client: client, logger: logger}
}

func (p *Patcher) SyncAppliedHashes(ctx context.Context, sandboxes []*watcher.SandboxInfo) error {
	if p.client == nil {
		return fmt.Errorf("k8s client is nil")
	}
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(appliedHashPatchConcurrency)
	for _, sandbox := range sandboxes {
		if sandbox == nil || sandbox.NetworkPolicyHash == "" {
			continue
		}
		annotations := map[string]string{}
		if sandbox.NetworkPolicyHash != "" && sandbox.NetworkPolicyHash != sandbox.NetworkAppliedHash {
			annotations[controller.AnnotationNetworkPolicyAppliedHash] = sandbox.NetworkPolicyHash
		}
		if len(annotations) == 0 {
			continue
		}
		patchBytes, err := buildAnnotationPatch(annotations)
		if err != nil {
			return err
		}
		sandbox := sandbox
		group.Go(func() error {
			_, patchErr := p.client.CoreV1().Pods(sandbox.Namespace).Patch(
				groupCtx,
				sandbox.Name,
				types.MergePatchType,
				patchBytes,
				metav1.PatchOptions{},
			)
			if patchErr != nil {
				p.logger.Warn("Failed to patch applied hashes",
					zap.String("namespace", sandbox.Namespace),
					zap.String("pod", sandbox.Name),
					zap.String("pod_ip", sandbox.PodIP),
					zap.Error(patchErr),
				)
				return nil
			}
			p.logger.Info("Applied hashes patched",
				zap.String("namespace", sandbox.Namespace),
				zap.String("pod", sandbox.Name),
				zap.String("pod_ip", sandbox.PodIP),
				zap.String("network_policy_applied_hash", annotations[controller.AnnotationNetworkPolicyAppliedHash]),
			)
			return nil
		})
	}
	return group.Wait()
}

func buildAnnotationPatch(annotations map[string]string) ([]byte, error) {
	payload := map[string]any{
		"metadata": map[string]any{
			"annotations": annotations,
		},
	}
	return json.Marshal(payload)
}
