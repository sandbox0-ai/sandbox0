package storage

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
)

func TestRustfsPodSecurityContextSetsWritablePersistentVolumePermissions(t *testing.T) {
	securityContext := rustfsPodSecurityContext()
	if securityContext == nil {
		t.Fatal("expected pod security context")
	} else {
		runAsNonRoot := securityContext.RunAsNonRoot
		if runAsNonRoot == nil || !*runAsNonRoot {
			t.Fatalf("expected runAsNonRoot=true, got %#v", runAsNonRoot)
		}
		runAsUser := securityContext.RunAsUser
		if runAsUser == nil || *runAsUser != rustfsUID {
			t.Fatalf("expected runAsUser=%d, got %#v", rustfsUID, runAsUser)
		}
		runAsGroup := securityContext.RunAsGroup
		if runAsGroup == nil || *runAsGroup != rustfsUID {
			t.Fatalf("expected runAsGroup=%d, got %#v", rustfsUID, runAsGroup)
		}
		fsGroup := securityContext.FSGroup
		if fsGroup == nil || *fsGroup != rustfsUID {
			t.Fatalf("expected fsGroup=%d, got %#v", rustfsUID, fsGroup)
		}
		fsGroupChangePolicy := securityContext.FSGroupChangePolicy
		if fsGroupChangePolicy == nil || *fsGroupChangePolicy != corev1.FSGroupChangeOnRootMismatch {
			t.Fatalf("expected fsGroupChangePolicy=%q, got %#v", corev1.FSGroupChangeOnRootMismatch, fsGroupChangePolicy)
		}
	}
}
