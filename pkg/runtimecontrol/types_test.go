package runtimecontrol

import (
	"testing"

	"github.com/sandbox0-ai/sandbox0/pkg/volumeportal"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestAssignmentRevisionIsStable(t *testing.T) {
	first := Assignment{
		SandboxID:         "sandbox-a",
		RuntimeGeneration: 2,
		EnvVars: map[string]string{
			"B": "2",
			"A": "1",
		},
	}
	second := Assignment{
		SandboxID:         "sandbox-a",
		RuntimeGeneration: 2,
		EnvVars: map[string]string{
			"A": "1",
			"B": "2",
		},
	}

	firstRevision, err := first.Revision()
	if err != nil {
		t.Fatal(err)
	}
	secondRevision, err := second.Revision()
	if err != nil {
		t.Fatal(err)
	}
	if firstRevision != secondRevision {
		t.Fatalf("assignment revisions differ: %q != %q", firstRevision, secondRevision)
	}
}

func TestAssignmentRevisionRejectsInvalidGeneration(t *testing.T) {
	if _, err := (Assignment{SandboxID: "sandbox-a"}).Revision(); err == nil {
		t.Fatal("Revision() error = nil, want invalid generation")
	}
}

func TestAssignmentFromPodUsesExistingManifest(t *testing.T) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Annotations: map[string]string{
				AnnotationSandboxID:         "sandbox-1",
				AnnotationTeamID:            "team-1",
				AnnotationRuntimeGeneration: "3",
				AnnotationAppDomain:         "region.example.test.",
				AnnotationConfig:            `{"env_vars":{"USER_VALUE":"yes"},"webhook":{"url":"https://example.test/events","watch_dir":"/workspace"}}`,
			},
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{
				Name: ProcdContainerName,
				VolumeMounts: []corev1.VolumeMount{
					{Name: "workspace", MountPath: "/workspace"},
					{Name: "state", MountPath: volumeportal.WebhookStateMountPath},
				},
			}},
			Volumes: []corev1.Volume{
				{
					Name: "workspace",
					VolumeSource: corev1.VolumeSource{CSI: &corev1.CSIVolumeSource{
						Driver: volumeportal.DriverName,
						VolumeAttributes: map[string]string{
							volumeportal.AttributePortalName: "workspace",
						},
					}},
				},
				{
					Name: "state",
					VolumeSource: corev1.VolumeSource{CSI: &corev1.CSIVolumeSource{
						Driver: volumeportal.DriverName,
						VolumeAttributes: map[string]string{
							volumeportal.AttributePortalName: volumeportal.WebhookStatePortalName,
						},
					}},
				},
			},
		},
	}

	assignment, revision, err := AssignmentFromPod(pod)
	if err != nil {
		t.Fatalf("AssignmentFromPod() error = %v", err)
	}
	if revision == "" {
		t.Fatal("AssignmentFromPod() returned an empty revision")
	}
	if assignment.SandboxID != "sandbox-1" || assignment.TeamID != "team-1" || assignment.RuntimeGeneration != 3 {
		t.Fatalf("AssignmentFromPod() = %#v", assignment)
	}
	if assignment.EnvVars[EnvSandboxID] != "sandbox-1" ||
		assignment.EnvVars[EnvAppDomain] != "region.example.test" ||
		assignment.EnvVars["USER_VALUE"] != "yes" {
		t.Fatalf("AssignmentFromPod() env = %#v", assignment.EnvVars)
	}
	if len(assignment.MountDirs) != 1 || assignment.MountDirs[0] != "/workspace" {
		t.Fatalf("AssignmentFromPod() mount dirs = %#v", assignment.MountDirs)
	}
}
