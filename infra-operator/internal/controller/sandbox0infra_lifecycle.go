package controller

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"

	infrav1alpha1 "github.com/sandbox0-ai/sandbox0/infra-operator/api/v1alpha1"
)

func collectRetainedResources(ctx context.Context, kubeClient ctrlclient.Client, namespace string, planned []infrav1alpha1.RetainedResourceStatus) ([]infrav1alpha1.RetainedResourceStatus, error) {
	if kubeClient == nil || len(planned) == 0 {
		return nil, nil
	}

	var retained []infrav1alpha1.RetainedResourceStatus
	for _, candidate := range planned {
		obj, err := retainedResourceObject(candidate.Kind)
		if err != nil {
			return nil, err
		}
		exists, err := resourceExists(ctx, kubeClient, namespace, candidate.Name, obj)
		if err != nil {
			return nil, err
		}
		if exists {
			retained = append(retained, candidate)
		}
	}

	return retained, nil
}

func retainedResourceObject(kind string) (ctrlclient.Object, error) {
	switch kind {
	case "Secret":
		return &corev1.Secret{}, nil
	case "PersistentVolumeClaim":
		return &corev1.PersistentVolumeClaim{}, nil
	default:
		return nil, fmt.Errorf("unsupported retained resource kind %q", kind)
	}
}

func resourceExists(ctx context.Context, kubeClient ctrlclient.Client, namespace, name string, obj ctrlclient.Object) (bool, error) {
	if err := kubeClient.Get(ctx, ctrlclient.ObjectKey{Namespace: namespace, Name: name}, obj); err != nil {
		if apierrors.IsNotFound(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}
