package sandboxpod

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestIsClaimed(t *testing.T) {
	idle := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{LabelPoolType: PoolTypeIdle}}}
	if IsClaimed(idle) {
		t.Fatal("idle pod reported as claimed")
	}

	active := idle.DeepCopy()
	active.Labels[LabelPoolType] = PoolTypeActive
	if !IsClaimed(active) {
		t.Fatal("active pod reported as unclaimed")
	}

	reserved := idle.DeepCopy()
	reserved.Annotations = map[string]string{AnnotationHotClaimReservation: "reservation-a"}
	if !IsClaimed(reserved) {
		t.Fatal("reserved idle pod reported as unclaimed")
	}
}
