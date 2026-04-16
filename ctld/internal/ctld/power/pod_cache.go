package power

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/controller"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
)

const podSandboxIDIndex = "sandboxID"

// PodCache owns the pod informer used by ctld for local pod lookups.
type PodCache struct {
	factory  informers.SharedInformerFactory
	informer cache.SharedIndexInformer
	lister   corelisters.PodLister
}

// NewNodePodCache builds a node-scoped pod cache for ctld pod resolution.
func NewNodePodCache(k8sClient kubernetes.Interface, nodeName string, resyncPeriod time.Duration) (*PodCache, error) {
	nodeName = strings.TrimSpace(nodeName)
	if nodeName == "" {
		return nil, errors.New("node name is required")
	}

	options := []informers.SharedInformerOption{}
	options = append(options, informers.WithTweakListOptions(func(opts *metav1.ListOptions) {
		opts.FieldSelector = fields.OneTermEqualSelector("spec.nodeName", nodeName).String()
	}))

	factory := informers.NewSharedInformerFactoryWithOptions(k8sClient, resyncPeriod, options...)
	podInformer := factory.Core().V1().Pods()
	informer := podInformer.Informer()
	if err := informer.AddIndexers(cache.Indexers{podSandboxIDIndex: podSandboxIDIndexFunc}); err != nil {
		return nil, err
	}

	return &PodCache{
		factory:  factory,
		informer: informer,
		lister:   podInformer.Lister(),
	}, nil
}

func (c *PodCache) Start(ctx context.Context) {
	if c == nil || c.factory == nil {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}
	c.factory.Start(ctx.Done())
}

func (c *PodCache) WaitForSync(ctx context.Context) bool {
	if c == nil || c.informer == nil {
		return false
	}
	if ctx == nil {
		ctx = context.Background()
	}
	return cache.WaitForCacheSync(ctx.Done(), c.informer.HasSynced)
}

func (c *PodCache) PodLister() corelisters.PodLister {
	if c == nil {
		return nil
	}
	return c.lister
}

func (c *PodCache) PodIndexer() cache.Indexer {
	if c == nil || c.informer == nil {
		return nil
	}
	return c.informer.GetIndexer()
}

func (c *PodCache) AddEventHandler(handler cache.ResourceEventHandler) error {
	if c == nil || c.informer == nil {
		return errors.New("pod informer is not configured")
	}
	_, err := c.informer.AddEventHandler(handler)
	return err
}

func podSandboxIDIndexFunc(obj interface{}) ([]string, error) {
	accessor, err := meta.Accessor(obj)
	if err != nil {
		return nil, err
	}
	value := strings.TrimSpace(accessor.GetLabels()[controller.LabelSandboxID])
	if value == "" {
		return nil, nil
	}
	return []string{value}, nil
}
