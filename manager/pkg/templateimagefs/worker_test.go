package templateimagefs

import (
	"context"
	"testing"

	api "github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/pkg/s0fsrollout"
	"github.com/sandbox0-ai/sandbox0/pkg/template"
	templstore "github.com/sandbox0-ai/sandbox0/pkg/template/store"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/client-go/kubernetes/fake"
)

func TestEnsureTemplateRevisionImportsInShadowWithoutSelecting(t *testing.T) {
	tpl := imageFSWorkerTestTemplate()
	queue := &workerQueueStub{}
	worker := &Worker{queue: queue, config: Config{Admission: mustAdmission(t, "off", nil, nil)}}

	require.NoError(t, worker.ensureTemplateRevision(context.Background(), tpl))
	require.Equal(t, 1, queue.ensureCalls)
	require.Equal(t, 0, queue.selectCalls)
	require.Equal(t, 1, queue.clearCalls)
}

func TestEnsureTemplateRevisionSelectsOnlyAdmittedPrivateTeam(t *testing.T) {
	tpl := imageFSWorkerTestTemplate()
	queue := &workerQueueStub{}
	worker := &Worker{queue: queue, config: Config{Admission: mustAdmission(t, "cold", []string{"team-1"}, nil)}}

	require.NoError(t, worker.ensureTemplateRevision(context.Background(), tpl))
	require.Equal(t, 1, queue.ensureCalls)
	require.Equal(t, 1, queue.selectCalls)
	require.Equal(t, 0, queue.clearCalls)
}

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

type workerQueueStub struct {
	templstore.TemplateStore
	templstore.TemplateImageRevisionStore
	ensureCalls int
	selectCalls int
	clearCalls  int
}

func (s *workerQueueStub) EnsureTemplateImageRevision(_ context.Context, tpl *template.Template) (*template.TemplateImageRevision, bool, error) {
	s.ensureCalls++
	revision, err := template.NewTemplateImageRevision(tpl)
	return revision, true, err
}

func (s *workerQueueStub) SelectCurrentTemplateImageRevision(_ context.Context, _ *template.TemplateImageRevision) error {
	s.selectCalls++
	return nil
}

func (s *workerQueueStub) ClearCurrentTemplateImageRevision(_ context.Context, _, _, _ string) error {
	s.clearCalls++
	return nil
}

func imageFSWorkerTestTemplate() *template.Template {
	return &template.Template{
		TemplateID: "python", Scope: "team", TeamID: "team-1",
		Spec: api.SandboxTemplateSpec{MainContainer: api.ContainerSpec{
			Image: "python:3.13", Resources: api.ResourceQuota{CPU: resource.MustParse("500m"), Memory: resource.MustParse("512Mi")},
		}},
	}
}

func mustAdmission(t *testing.T, mode string, teamIDs, templateIDs []string) s0fsrollout.Admission {
	t.Helper()
	admission, err := s0fsrollout.NewAdmission(mode, teamIDs, templateIDs, false, false)
	require.NoError(t, err)
	return admission
}
