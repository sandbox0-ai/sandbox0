package templateimagefs

import (
	"context"
	"errors"
	"testing"
	"time"

	api "github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	fakeclientset "github.com/sandbox0-ai/sandbox0/manager/pkg/generated/clientset/versioned/fake"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	"github.com/sandbox0-ai/sandbox0/pkg/naming"
	"github.com/sandbox0-ai/sandbox0/pkg/s0fsrollout"
	"github.com/sandbox0-ai/sandbox0/pkg/template"
	templstore "github.com/sandbox0-ai/sandbox0/pkg/template/store"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

func TestNewWorkerDefaultsToLargeImageImportWindow(t *testing.T) {
	worker, err := NewWorker(
		&workerQueueStub{},
		workerHeadStoreStub{},
		fake.NewSimpleClientset(),
		fakeclientset.NewSimpleClientset().Sandbox0V1alpha1(),
		nil,
		func(context.Context, *corev1.Pod) (string, error) { return "http://ctld", nil },
		Config{WorkerID: "manager/green", BaseImageRef: "sandbox0ai/infra:carrier-base"},
		zap.NewNop(),
	)
	require.NoError(t, err)
	require.Equal(t, 4*time.Hour, worker.config.ImportTimeout)
	require.Equal(t, 2*time.Minute, worker.config.LeaseDuration)
}

func TestNewWorkerPreservesExplicitImportTimeout(t *testing.T) {
	worker, err := NewWorker(
		&workerQueueStub{},
		workerHeadStoreStub{},
		fake.NewSimpleClientset(),
		fakeclientset.NewSimpleClientset().Sandbox0V1alpha1(),
		nil,
		func(context.Context, *corev1.Pod) (string, error) { return "http://ctld", nil },
		Config{WorkerID: "manager/green", BaseImageRef: "sandbox0ai/infra:carrier-base", ImportTimeout: 30 * time.Minute},
		zap.NewNop(),
	)
	require.NoError(t, err)
	require.Equal(t, 30*time.Minute, worker.config.ImportTimeout)
}

func TestRunReportsClaimFailure(t *testing.T) {
	queue := &workerQueueStub{claimErr: errors.New("claim queue unavailable")}
	logCore, observedLogs := observer.New(zap.WarnLevel)
	worker := &Worker{
		queue:  queue,
		config: Config{PollInterval: time.Millisecond, ClaimTimeout: time.Second, EnsureInterval: time.Second},
		logger: zap.New(logCore),
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- worker.Run(ctx) }()
	require.Eventually(t, func() bool {
		return observedLogs.FilterMessage("Failed to claim template ImageFS revision").Len() == 1
	}, time.Second, time.Millisecond)
	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
	require.GreaterOrEqual(t, queue.claimCalls, 1)
	require.Equal(t, "claim queue unavailable", observedLogs.All()[0].ContextMap()["error"])
}

func TestRunBoundsStuckClaim(t *testing.T) {
	queue := &workerQueueStub{blockClaim: true}
	logCore, observedLogs := observer.New(zap.WarnLevel)
	worker := &Worker{
		queue:  queue,
		config: Config{PollInterval: time.Millisecond, ClaimTimeout: 10 * time.Millisecond, EnsureInterval: time.Second},
		logger: zap.New(logCore),
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- worker.Run(ctx) }()
	require.Eventually(t, func() bool {
		entries := observedLogs.FilterMessage("Failed to claim template ImageFS revision").All()
		return len(entries) == 1 && entries[0].ContextMap()["error"] == context.DeadlineExceeded.Error()
	}, time.Second, time.Millisecond)
	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
}

func TestEnsureTemplateRevisionImportsInShadowWithoutSelecting(t *testing.T) {
	tpl := imageFSWorkerTestTemplate()
	queue := &workerQueueStub{}
	worker := &Worker{queue: queue, config: Config{Admission: mustAdmission(t, "off", nil, nil)}}

	require.NoError(t, worker.ensureTemplateRevision(context.Background(), tpl))
	require.Equal(t, 1, queue.ensureCalls)
	require.Equal(t, 0, queue.selectCalls)
	require.Equal(t, 1, queue.clearCalls)
}

func TestEnsureTemplateRevisionKeepsSelectedRevisionWhenAdmissionTurnsOff(t *testing.T) {
	tpl := imageFSWorkerTestTemplate()
	tpl.Status = &api.SandboxTemplateStatus{ImageRevision: &api.TemplateImageRevisionStatus{
		RevisionID:    "tir-selected",
		ImageFSHeadID: "head-selected",
		State:         api.TemplateImageRevisionStateReady,
	}}
	queue := &workerQueueStub{}
	worker := &Worker{queue: queue, config: Config{Admission: mustAdmission(t, "off", nil, nil)}}

	require.NoError(t, worker.ensureTemplateRevision(context.Background(), tpl))
	require.Equal(t, 1, queue.ensureCalls)
	require.Equal(t, 0, queue.selectCalls)
	require.Equal(t, 0, queue.clearCalls)
}

func TestEnsureTemplateRevisionSkipsTemplateOutsideImportCohort(t *testing.T) {
	tpl := imageFSWorkerTestTemplate()
	queue := &workerQueueStub{}
	worker := &Worker{queue: queue, config: Config{
		ImportCohort: s0fsrollout.NewCohort([]string{"team-2"}, []string{"node"}),
		Admission:    mustAdmission(t, "off", nil, nil),
	}}

	require.NoError(t, worker.ensureTemplateRevision(context.Background(), tpl))
	require.Equal(t, 0, queue.ensureCalls)
	require.Equal(t, 0, queue.selectCalls)
	require.Equal(t, 1, queue.clearCalls)
}

func TestEnsureTemplateRevisionImportsTemplateInsideImportCohort(t *testing.T) {
	tpl := imageFSWorkerTestTemplate()
	queue := &workerQueueStub{}
	worker := &Worker{queue: queue, config: Config{
		ImportCohort: s0fsrollout.NewCohort(nil, []string{"python"}),
		Admission:    mustAdmission(t, "off", nil, nil),
	}}

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

func TestProcessSupersedesClaimWhoseSpecIsNoLongerCurrent(t *testing.T) {
	claimedTemplate := imageFSWorkerTestTemplate()
	claimed, err := template.NewTemplateImageRevision(claimedTemplate)
	require.NoError(t, err)
	currentTemplate := imageFSWorkerTestTemplate()
	currentTemplate.Spec.MainContainer.Image = "python:3.14"
	queue := &workerQueueStub{template: currentTemplate}
	worker := &Worker{
		queue: queue,
		k8s:   fake.NewSimpleClientset(),
		config: Config{
			WorkerID:      "manager/green",
			LeaseDuration: time.Hour,
			ImportTimeout: time.Minute,
		},
		logger: zap.NewNop(),
	}

	worker.process(context.Background(), claimed)

	require.Equal(t, 1, queue.failCalls)
	require.Equal(t, claimed.RevisionID, queue.failedRevisionID)
	require.Equal(t, template.TemplateImageRevisionReasonSuperseded, queue.failedReason)
	require.Contains(t, queue.failedMessage, "current template requires")
}

func TestProcessSupersededRevisionDeletesStaleImporterPod(t *testing.T) {
	claimedTemplate := imageFSWorkerTestTemplate()
	claimed, err := template.NewTemplateImageRevision(claimedTemplate)
	require.NoError(t, err)
	currentTemplate := imageFSWorkerTestTemplate()
	currentTemplate.Spec.MainContainer.Image = "python:3.14"
	namespace, err := templateNamespace(currentTemplate)
	require.NoError(t, err)
	client := fake.NewSimpleClientset(
		&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "stale-matching", Namespace: namespace, Labels: map[string]string{importerLabel: claimed.RevisionID}}},
		&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "different-revision", Namespace: namespace, Labels: map[string]string{importerLabel: "tir-other"}}},
	)
	queue := &workerQueueStub{template: currentTemplate}
	worker := &Worker{
		queue: queue,
		k8s:   client,
		config: Config{
			WorkerID:      "manager/green",
			LeaseDuration: time.Hour,
			ImportTimeout: time.Minute,
		},
		logger: zap.NewNop(),
	}

	worker.process(context.Background(), claimed)

	pods, err := client.CoreV1().Pods(namespace).List(context.Background(), metav1.ListOptions{})
	require.NoError(t, err)
	require.Len(t, pods.Items, 1)
	require.Equal(t, "different-revision", pods.Items[0].Name)
	require.Equal(t, 1, queue.failCalls)
}

func TestWaitForImportContainerReturnsDeterministicStartupFailures(t *testing.T) {
	tests := []struct {
		name   string
		reason string
		init   bool
	}{
		{name: "image pull", reason: "ErrImagePull"},
		{name: "pull backoff", reason: "ImagePullBackOff"},
		{name: "invalid image", reason: "InvalidImageName"},
		{name: "invalid config", reason: "CreateContainerConfigError"},
		{name: "create failure", reason: "CreateContainerError"},
		{name: "runtime failure", reason: "RunContainerError"},
		{name: "init failure", reason: "CreateContainerError", init: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			status := corev1.ContainerStatus{
				Name:  "procd",
				State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: tt.reason, Message: "failed"}},
			}
			podStatus := corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{status}}
			if tt.init {
				status.Name = "carrier-base-primer"
				podStatus = corev1.PodStatus{InitContainerStatuses: []corev1.ContainerStatus{status}}
			}
			client := fake.NewSimpleClientset(&corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: "importer", Namespace: "sandbox-team"},
				Status:     podStatus,
			})
			worker := &Worker{k8s: client, config: Config{PollInterval: time.Millisecond}}

			_, err := worker.waitForImportContainer(context.Background(), "sandbox-team", "importer")

			require.Error(t, err)
			require.Contains(t, err.Error(), tt.reason)
		})
	}
}

func TestWaitForImportContainerObservesFailureAfterPodCreation(t *testing.T) {
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "importer", Namespace: "sandbox-team"}}
	client := fake.NewSimpleClientset(pod)
	getCalls := 0
	client.Fake.PrependReactor("get", "pods", func(k8stesting.Action) (bool, runtime.Object, error) {
		getCalls++
		current := pod.DeepCopy()
		if getCalls > 1 {
			current.Status.ContainerStatuses = []corev1.ContainerStatus{{
				Name: "procd",
				State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{
					Reason: "ErrImagePull", Message: "not found",
				}},
			}}
		}
		return true, current, nil
	})
	worker := &Worker{k8s: client, config: Config{PollInterval: time.Millisecond}}

	_, err := worker.waitForImportContainer(context.Background(), "sandbox-team", "importer")

	require.ErrorContains(t, err, "ErrImagePull")
	require.GreaterOrEqual(t, getCalls, 2)
}

func TestProcessStartupFailureDeletesImporterAndReleasesRevision(t *testing.T) {
	tpl := imageFSWorkerTestTemplate()
	revision, err := template.NewTemplateImageRevision(tpl)
	require.NoError(t, err)
	namespace, err := templateNamespace(tpl)
	require.NoError(t, err)
	resourceName := naming.TemplateNameForCluster(tpl.Scope, tpl.TeamID, tpl.TemplateID)
	owner := &api.SandboxTemplate{ObjectMeta: metav1.ObjectMeta{
		Name: resourceName, Namespace: namespace, UID: types.UID("template-uid"),
	}}
	importPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "importer", Namespace: namespace, UID: types.UID("importer-uid")},
		Status: corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{{
			Name: "procd",
			State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{
				Reason: "ErrImagePull", Message: "not found",
			}},
		}}},
	}
	client := fake.NewSimpleClientset()
	client.Fake.PrependReactor("create", "pods", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, importPod.DeepCopy(), nil
	})
	client.Fake.PrependReactor("get", "pods", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, importPod.DeepCopy(), nil
	})
	queue := &workerQueueStub{template: tpl}
	worker := &Worker{
		queue:     queue,
		heads:     workerHeadStoreStub{},
		k8s:       client,
		templates: fakeclientset.NewSimpleClientset(owner).Sandbox0V1alpha1(),
		config: Config{
			WorkerID: "manager/green", ClusterID: "green",
			BaseImageRef: "sandbox0ai/infra:carrier-base", PrimerImageRef: "sandbox0ai/infra:manager",
			LeaseDuration: time.Hour, ImportTimeout: time.Minute, PollInterval: time.Millisecond,
		},
		logger: zap.NewNop(),
	}

	require.NotPanics(t, func() { worker.process(context.Background(), revision) })
	require.Equal(t, 1, queue.releaseCalls)
	require.Equal(t, revision.RevisionID, queue.releasedRevisionID)
	deleteActions := 0
	for _, action := range client.Actions() {
		if action.GetVerb() == "delete" && action.GetResource().Resource == "pods" {
			deleteActions++
			assert.Equal(t, "importer", action.(k8stesting.DeleteAction).GetName())
		}
	}
	require.Equal(t, 1, deleteActions)
}

func TestCreateImportPodPrimesFixedCarrierBaseOnTargetNode(t *testing.T) {
	namespace := "sandbox-team"
	resourceName := naming.TemplateNameForCluster("team", "team-1", "python")
	owner := &api.SandboxTemplate{ObjectMeta: metav1.ObjectMeta{
		Name: resourceName, Namespace: namespace, UID: types.UID("template-uid"),
	}}
	worker := &Worker{
		k8s:       fake.NewSimpleClientset(),
		templates: fakeclientset.NewSimpleClientset(owner).Sandbox0V1alpha1(),
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
	pod, err := worker.createImportPod(context.Background(), namespace, tpl, revision)
	require.NoError(t, err)
	require.Len(t, pod.OwnerReferences, 1)
	assert.Equal(t, owner.Name, pod.OwnerReferences[0].Name)
	assert.Equal(t, owner.UID, pod.OwnerReferences[0].UID)
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
	ensureCalls        int
	selectCalls        int
	clearCalls         int
	claimCalls         int
	claimErr           error
	blockClaim         bool
	template           *template.Template
	failCalls          int
	failedRevisionID   string
	failedReason       string
	failedMessage      string
	releaseCalls       int
	releasedRevisionID string
}

type workerHeadStoreStub struct{}

func (workerHeadStoreStub) StageRootFSHead(context.Context, *sandboxstore.SandboxRootFSHead) error {
	return nil
}

func (s *workerQueueStub) ListTemplates(context.Context) ([]*template.Template, error) {
	return nil, nil
}

func (s *workerQueueStub) ClaimTemplateImageRevision(ctx context.Context, _ string, _ time.Duration, _, _ []string) (*template.TemplateImageRevision, error) {
	s.claimCalls++
	if s.blockClaim {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return nil, s.claimErr
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

func (s *workerQueueStub) GetTemplate(context.Context, string, string, string) (*template.Template, error) {
	return s.template, nil
}

func (s *workerQueueStub) FailTemplateImageRevision(_ context.Context, revisionID, _ string, reason, message string) error {
	s.failCalls++
	s.failedRevisionID = revisionID
	s.failedReason = reason
	s.failedMessage = message
	return nil
}

func (s *workerQueueStub) ReleaseTemplateImageRevision(_ context.Context, revisionID, _ string, _ time.Time, _ string) error {
	s.releaseCalls++
	s.releasedRevisionID = revisionID
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
