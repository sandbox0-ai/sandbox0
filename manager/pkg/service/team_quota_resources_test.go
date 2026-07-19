package service

import (
	"context"
	"math"
	"testing"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/pkg/teamquota"
	corev1 "k8s.io/api/core/v1"
	nodev1 "k8s.io/api/node/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestPodSpecTeamQuotaResourcesIncludesContainersInitAndOverhead(t *testing.T) {
	always := corev1.ContainerRestartPolicyAlways
	spec := &corev1.PodSpec{
		Containers: []corev1.Container{
			{Resources: quotaTestResources("500m", "1Gi", "2Gi")},
			{Resources: quotaTestResources("250m", "256Mi", "1Gi")},
		},
		InitContainers: []corev1.Container{
			{
				RestartPolicy: &always,
				Resources:     quotaTestResources("100m", "128Mi", "512Mi"),
			},
			{Resources: quotaTestResources("2", "3Gi", "4Gi")},
		},
		Overhead: corev1.ResourceList{
			corev1.ResourceCPU:              resource.MustParse("50m"),
			corev1.ResourceMemory:           resource.MustParse("64Mi"),
			corev1.ResourceEphemeralStorage: resource.MustParse("256Mi"),
		},
	}

	got := PodSpecTeamQuotaResources(spec)
	if got[teamquota.KeySandboxCPUMillicores] != 2_150 {
		t.Fatalf("cpu = %d, want 2150 millicores", got[teamquota.KeySandboxCPUMillicores])
	}
	if got[teamquota.KeySandboxMemoryBytes] != quotaTestQuantityValue("3264Mi") {
		t.Fatalf("memory = %d, want 3264Mi", got[teamquota.KeySandboxMemoryBytes])
	}
	if got[teamquota.KeySandboxEphemeralStorageBytes] != quotaTestQuantityValue("4864Mi") {
		t.Fatalf("ephemeral storage = %d, want 4864Mi", got[teamquota.KeySandboxEphemeralStorageBytes])
	}
}

func TestPodSpecTeamQuotaResourcesUsesRequestsWithoutLimits(t *testing.T) {
	spec := &corev1.PodSpec{Containers: []corev1.Container{{
		Resources: corev1.ResourceRequirements{
			Requests: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("125m"),
				corev1.ResourceMemory: resource.MustParse("96Mi"),
			},
		},
	}}}

	got := PodSpecTeamQuotaResources(spec)
	if got[teamquota.KeySandboxCPUMillicores] != 125 {
		t.Fatalf("cpu = %d, want 125 millicores", got[teamquota.KeySandboxCPUMillicores])
	}
	if got[teamquota.KeySandboxMemoryBytes] != quotaTestQuantityValue("96Mi") {
		t.Fatalf("memory = %d, want 96Mi", got[teamquota.KeySandboxMemoryBytes])
	}
}

func TestWarmPoolTeamQuotaTargetMultipliesResources(t *testing.T) {
	template := quotaResourceTestTemplate()
	target, err := testWarmPoolTeamQuotaTarget(template, 3)
	if err != nil {
		t.Fatalf("testWarmPoolTeamQuotaTarget() error = %v", err)
	}
	perPod := PodSpecTeamQuotaResources(ptrTo(v1alpha1.BuildIdlePodSpec(template)))
	if target[teamquota.KeySandboxRuntimeCount] != 3 {
		t.Fatalf("runtime count = %d, want 3", target[teamquota.KeySandboxRuntimeCount])
	}
	for _, key := range []teamquota.Key{
		teamquota.KeySandboxCPUMillicores,
		teamquota.KeySandboxMemoryBytes,
		teamquota.KeySandboxEphemeralStorageBytes,
	} {
		if target[key] != perPod[key]*3 {
			t.Fatalf("%s = %d, want %d", key, target[key], perPod[key]*3)
		}
	}
}

func TestReserveSandboxTeamQuotaIncludesRuntimeClassOverheadForClaimAndResume(t *testing.T) {
	const runtimeClassName = "quota-runtime"
	withClaimTestManagerConfig(t, "sandbox_runtime_class_name: "+runtimeClassName+"\n")
	overhead := corev1.ResourceList{
		corev1.ResourceCPU:              resource.MustParse("75m"),
		corev1.ResourceMemory:           resource.MustParse("96Mi"),
		corev1.ResourceEphemeralStorage: resource.MustParse("32Mi"),
	}
	client := fake.NewSimpleClientset(&nodev1.RuntimeClass{
		ObjectMeta: metav1.ObjectMeta{Name: runtimeClassName},
		Handler:    "sandbox",
		Overhead:   &nodev1.Overhead{PodFixed: overhead},
	})
	template := quotaResourceTestTemplate()
	baseSpec := v1alpha1.BuildPodSpec(template)
	overheadResources := resourceListQuotaResources(overhead, nil)
	if baseSpec.Overhead != nil {
		t.Fatalf("submitted PodSpec overhead = %v, want nil before Kubernetes admission", baseSpec.Overhead)
	}
	baseTarget := PodSpecTeamQuotaResources(&baseSpec)

	for _, operationKind := range []string{"claim", "resume"} {
		t.Run(operationKind, func(t *testing.T) {
			store := &recordingReserveTargetStore{}
			svc := &SandboxService{
				k8sClient:      client,
				teamQuotaStore: store,
			}
			request := &ClaimRequest{
				TeamID:            "team-a",
				SandboxID:         "sandbox-" + operationKind,
				RuntimeGeneration: 2,
			}

			reservation, err := svc.reserveSandboxTeamQuota(
				context.Background(),
				request,
				template,
				operationKind,
			)
			if err != nil {
				t.Fatalf("reserveSandboxTeamQuota() error = %v", err)
			}
			if reservation == nil {
				t.Fatal("reserveSandboxTeamQuota() reservation = nil")
			}
			if len(store.requests) != 1 {
				t.Fatalf("ReserveTarget calls = %d, want 1", len(store.requests))
			}
			got := store.requests[0]
			if got.Operation.Kind != operationKind {
				t.Fatalf("operation kind = %q, want %q", got.Operation.Kind, operationKind)
			}
			if got.Target[teamquota.KeySandboxIdentityCount] != 1 {
				t.Fatalf("identity count = %d, want 1", got.Target[teamquota.KeySandboxIdentityCount])
			}
			if got.Target[teamquota.KeySandboxRuntimeCount] != 1 {
				t.Fatalf("runtime count = %d, want 1", got.Target[teamquota.KeySandboxRuntimeCount])
			}
			wantResources := map[teamquota.Key]int64{
				teamquota.KeySandboxCPUMillicores: baseTarget[teamquota.KeySandboxCPUMillicores] +
					overheadResources.cpuMillicores,
				teamquota.KeySandboxMemoryBytes: baseTarget[teamquota.KeySandboxMemoryBytes] +
					overheadResources.memoryBytes,
				teamquota.KeySandboxEphemeralStorageBytes: baseTarget[teamquota.KeySandboxEphemeralStorageBytes] +
					overheadResources.ephemeralStorageBytes,
			}
			for key, want := range wantResources {
				if got.Target[key] != want {
					t.Fatalf("%s = %d, want %d", key, got.Target[key], want)
				}
			}
		})
	}

	if spec := v1alpha1.BuildPodSpec(template); spec.Overhead != nil {
		t.Fatalf("BuildPodSpec() overhead after quota admission = %v, want nil", spec.Overhead)
	}
}

func TestPausedSandboxTeamQuotaTargetIsIdentityOnly(t *testing.T) {
	target := pausedSandboxTeamQuotaTarget()
	if target[teamquota.KeySandboxIdentityCount] != 1 {
		t.Fatalf("identity count = %d, want 1", target[teamquota.KeySandboxIdentityCount])
	}
	for _, key := range []teamquota.Key{
		teamquota.KeySandboxRuntimeCount,
		teamquota.KeySandboxCPUMillicores,
		teamquota.KeySandboxMemoryBytes,
		teamquota.KeySandboxEphemeralStorageBytes,
	} {
		if target[key] != 0 {
			t.Fatalf("%s = %d, want 0 for a paused fork before resume", key, target[key])
		}
	}
}

func TestMultiplyQuotaValueRejectsOverflow(t *testing.T) {
	if _, err := multiplyQuotaValue(math.MaxInt64, 2); err == nil {
		t.Fatal("multiplyQuotaValue() error = nil, want overflow error")
	}
}

type recordingReserveTargetStore struct {
	permissiveTeamQuotaCapacityStore
	requests []teamquota.ReserveRequest
}

func (s *recordingReserveTargetStore) ReserveTarget(
	_ context.Context,
	request teamquota.ReserveRequest,
) (*teamquota.Reservation, error) {
	s.requests = append(s.requests, request)
	return testQuotaReservation(request.Owner, request.Operation, request.Target), nil
}

func quotaTestResources(cpu, memory, ephemeral string) corev1.ResourceRequirements {
	return corev1.ResourceRequirements{
		Limits: corev1.ResourceList{
			corev1.ResourceCPU:              resource.MustParse(cpu),
			corev1.ResourceMemory:           resource.MustParse(memory),
			corev1.ResourceEphemeralStorage: resource.MustParse(ephemeral),
		},
	}
}

func quotaResourceTestTemplate() *v1alpha1.SandboxTemplate {
	return &v1alpha1.SandboxTemplate{
		ObjectMeta: metav1.ObjectMeta{Name: "quota-test", Namespace: "default"},
		Spec: v1alpha1.SandboxTemplateSpec{
			MainContainer: v1alpha1.ContainerSpec{
				Image: "busybox",
				Resources: v1alpha1.SandboxResourceLimits{
					CPU:              resource.MustParse("500m"),
					Memory:           resource.MustParse("1Gi"),
					EphemeralStorage: resource.MustParse("2Gi"),
				},
			},
		},
	}
}

func quotaTestQuantityValue(value string) int64 {
	quantity := resource.MustParse(value)
	return quantity.Value()
}
