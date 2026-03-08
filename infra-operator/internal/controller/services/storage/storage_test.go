package storage

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
)

func TestRustfsPodSecurityContextSetsWritablePersistentVolumePermissions(t *testing.T) {
	securityContext := rustfsPodSecurityContext()
	if securityContext == nil {
		t.Fatal("expected pod security context")
	}
	if securityContext.RunAsNonRoot == nil || !*securityContext.RunAsNonRoot {
		t.Fatalf("expected runAsNonRoot=true, got %#v", securityContext.RunAsNonRoot)
	}
	if securityContext.RunAsUser == nil || *securityContext.RunAsUser != rustfsUID {
		t.Fatalf("expected runAsUser=%d, got %#v", rustfsUID, securityContext.RunAsUser)
	}
	if securityContext.RunAsGroup == nil || *securityContext.RunAsGroup != rustfsUID {
		t.Fatalf("expected runAsGroup=%d, got %#v", rustfsUID, securityContext.RunAsGroup)
	}
	if securityContext.FSGroup == nil || *securityContext.FSGroup != rustfsUID {
		t.Fatalf("expected fsGroup=%d, got %#v", rustfsUID, securityContext.FSGroup)
	}
	if securityContext.FSGroupChangePolicy == nil || *securityContext.FSGroupChangePolicy != corev1.FSGroupChangeOnRootMismatch {
		t.Fatalf("expected fsGroupChangePolicy=%q, got %#v", corev1.FSGroupChangeOnRootMismatch, securityContext.FSGroupChangePolicy)
	}
}
