package snapshotter

import (
	"context"
	"strings"

	"github.com/containerd/containerd/v2/pkg/namespaces"
)

const defaultContainerdNamespace = "k8s.io"

func ensureContainerdNamespace(ctx context.Context, namespace string) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	if _, ok := namespaces.Namespace(ctx); ok {
		return ctx
	}
	namespace = strings.TrimSpace(namespace)
	if namespace == "" {
		namespace = defaultContainerdNamespace
	}
	return namespaces.WithNamespace(ctx, namespace)
}
