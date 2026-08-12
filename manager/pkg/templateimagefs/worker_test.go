package templateimagefs

import (
	"context"
	"testing"

	api "github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/pkg/template"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/client-go/kubernetes/fake"
)

func TestCreateImportPodPrimesFixedCarrierBaseOnTargetNode(t *testing.T) {
	worker := &Worker{
		k8s: fake.NewSimpleClientset(),
		config: Config{
			ClusterID: "cluster-a", BaseImageRef: "sandbox0ai/infra:carrier-base-v1", PrimerImageRef: "alpine:3.20",
		},
	}
	tpl := &template.Template{
		TemplateID: "python", Scope: "team", TeamID: "team-1",
		Spec: api.SandboxTemplateSpec{MainContainer: api.ContainerSpec{
			Image: "python:3.13", Resources: api.ResourceQuota{CPU: resource.MustParse("500m"), Memory: resource.MustParse("512Mi")},
		}},
	}
	revision, err := template.NewTemplateImageRevision(tpl)
	require.NoError(t, err)
	pod, err := worker.createImportPod(context.Background(), "sandbox-team", tpl, revision)
	require.NoError(t, err)
	require.NotEmpty(t, pod.Spec.InitContainers)
	assert.Equal(t, "carrier-base-primer", pod.Spec.InitContainers[0].Name)
	assert.Equal(t, "alpine:3.20", pod.Spec.InitContainers[0].Image)
	require.Len(t, pod.Spec.InitContainers[0].VolumeMounts, 1)
	assert.Equal(t, "/carrier-base", pod.Spec.InitContainers[0].VolumeMounts[0].MountPath)
	assert.Equal(t, "python:3.13", pod.Spec.Containers[0].Image)
	var primer *corev1.ImageVolumeSource
	for i := range pod.Spec.Volumes {
		if pod.Spec.Volumes[i].Name == "carrier-base-primer" {
			primer = pod.Spec.Volumes[i].Image
		}
	}
	require.NotNil(t, primer)
	assert.Equal(t, "sandbox0ai/infra:carrier-base-v1", primer.Reference)
}

func TestTemplateNamespaceKeepsPublicAndTeamImportsIsolated(t *testing.T) {
	teamNamespace, err := templateNamespace(&template.Template{TemplateID: "python", Scope: "team", TeamID: "team-1"})
	require.NoError(t, err)
	publicNamespace, err := templateNamespace(&template.Template{TemplateID: "python", Scope: "public"})
	require.NoError(t, err)
	assert.NotEqual(t, teamNamespace, publicNamespace)
}
