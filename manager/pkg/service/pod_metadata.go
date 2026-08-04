package service

import (
	"github.com/sandbox0-ai/sandbox0/manager/pkg/podmeta"
	corev1 "k8s.io/api/core/v1"
)

func sandboxPodID(pod *corev1.Pod) string {
	return podmeta.SandboxID(pod)
}

func sandboxPodFromInformerEvent(obj any) *corev1.Pod {
	return podmeta.FromInformerEvent(obj)
}
