// Package templateimagefs imports immutable template OCI revisions into S0FS.
package templateimagefs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	api "github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	"github.com/sandbox0-ai/sandbox0/pkg/ctldapi"
	"github.com/sandbox0-ai/sandbox0/pkg/naming"
	"github.com/sandbox0-ai/sandbox0/pkg/s0fsrollout"
	"github.com/sandbox0-ai/sandbox0/pkg/template"
	templstore "github.com/sandbox0-ai/sandbox0/pkg/template/store"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
)

const (
	importerLabel       = "sandbox0.ai/imagefs-import"
	defaultPollInterval = 500 * time.Millisecond
	defaultLease        = 10 * time.Minute
	defaultImportTTL    = 20 * time.Minute
)

type queue interface {
	templstore.TemplateStore
	templstore.TemplateImageRevisionStore
}

type headStore interface {
	StageRootFSHead(context.Context, *sandboxstore.SandboxRootFSHead) error
}

type Config struct {
	WorkerID       string
	ClusterID      string
	BaseImageRef   string
	PrimerImageRef string
	LeaseDuration  time.Duration
	ImportTimeout  time.Duration
	PollInterval   time.Duration
	EnsureInterval time.Duration
	Admission      s0fsrollout.Admission
}

// Worker owns both revision discovery and region-wide leased imports. The
// PostgreSQL lease ensures multiple data-plane clusters do not import the same
// revision concurrently.
type Worker struct {
	queue       queue
	heads       headStore
	k8s         kubernetes.Interface
	ctld        *ctldapi.Client
	ctldAddress func(context.Context, *corev1.Pod) (string, error)
	config      Config
	logger      *zap.Logger
}

func NewWorker(q queue, heads headStore, k8sClient kubernetes.Interface, ctldClient *ctldapi.Client, ctldAddress func(context.Context, *corev1.Pod) (string, error), cfg Config, logger *zap.Logger) (*Worker, error) {
	if q == nil || heads == nil || k8sClient == nil || ctldAddress == nil {
		return nil, fmt.Errorf("template ImageFS worker dependencies are required")
	}
	cfg.WorkerID = strings.TrimSpace(cfg.WorkerID)
	cfg.BaseImageRef = strings.TrimSpace(cfg.BaseImageRef)
	cfg.PrimerImageRef = strings.TrimSpace(cfg.PrimerImageRef)
	if cfg.WorkerID == "" || cfg.BaseImageRef == "" {
		return nil, fmt.Errorf("template ImageFS worker_id and base_image_ref are required")
	}
	if cfg.PrimerImageRef == "" {
		cfg.PrimerImageRef = cfg.BaseImageRef
	}
	if cfg.LeaseDuration <= 0 {
		cfg.LeaseDuration = defaultLease
	}
	if cfg.ImportTimeout <= 0 {
		cfg.ImportTimeout = defaultImportTTL
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = defaultPollInterval
	}
	if cfg.EnsureInterval <= 0 {
		cfg.EnsureInterval = 5 * time.Second
	}
	if ctldClient == nil {
		ctldClient = ctldapi.NewClientWithTimeout(cfg.ImportTimeout)
	}
	if logger == nil {
		logger = zap.NewNop()
	}
	return &Worker{queue: q, heads: heads, k8s: k8sClient, ctld: ctldClient, ctldAddress: ctldAddress, config: cfg, logger: logger}, nil
}

func (w *Worker) Run(ctx context.Context) error {
	var group sync.WaitGroup
	group.Add(1)
	go func() {
		defer group.Done()
		w.ensureLoop(ctx)
	}()
	defer group.Wait()
	for ctx.Err() == nil {
		revision, err := w.queue.ClaimTemplateImageRevision(ctx, w.config.WorkerID, w.config.LeaseDuration)
		if err != nil {
			if !waitForContext(ctx, w.config.PollInterval) {
				return ctx.Err()
			}
			continue
		}
		if revision == nil {
			if !waitForContext(ctx, w.config.PollInterval) {
				return ctx.Err()
			}
			continue
		}
		w.process(ctx, revision)
	}
	return ctx.Err()
}

func (w *Worker) ensureLoop(ctx context.Context) {
	for {
		templates, err := w.queue.ListTemplates(ctx)
		if err == nil {
			for _, tpl := range templates {
				if tpl == nil || !tpl.ReadyForReconcile() || strings.TrimSpace(tpl.Spec.MainContainer.Image) == "" {
					continue
				}
				if err := w.ensureTemplateRevision(ctx, tpl); err != nil {
					w.logger.Warn("Failed to ensure template image revision", zap.String("templateID", tpl.TemplateID), zap.Error(err))
				}
			}
		} else if !errors.Is(err, context.Canceled) {
			w.logger.Warn("Failed to list templates for ImageFS revisions", zap.Error(err))
		}
		if !waitForContext(ctx, w.config.EnsureInterval) {
			return
		}
	}
}

func (w *Worker) ensureTemplateRevision(ctx context.Context, tpl *template.Template) error {
	revision, _, err := w.queue.EnsureTemplateImageRevision(ctx, tpl)
	if err != nil {
		return err
	}
	if w.config.Admission.Admits(tpl.Scope, tpl.TeamID, tpl.TemplateID) {
		return w.queue.SelectCurrentTemplateImageRevision(ctx, revision)
	}
	return w.queue.ClearCurrentTemplateImageRevision(ctx, tpl.Scope, tpl.TeamID, tpl.TemplateID)
}

func (w *Worker) process(parent context.Context, revision *template.TemplateImageRevision) {
	ctx, cancel := context.WithTimeout(parent, w.config.ImportTimeout)
	defer cancel()
	stopRenew := make(chan struct{})
	defer close(stopRenew)
	go w.renewLease(ctx, revision.RevisionID, stopRenew)
	if err := w.importRevision(ctx, revision); err != nil {
		if errors.Is(err, template.ErrTemplateImageRevisionLeaseLost) {
			return
		}
		retryAt := time.Now().UTC().Add(time.Duration(min(revision.AttemptCount+1, 12)) * 5 * time.Second)
		if releaseErr := w.queue.ReleaseTemplateImageRevision(context.WithoutCancel(parent), revision.RevisionID, w.config.WorkerID, retryAt, err.Error()); releaseErr != nil && !errors.Is(releaseErr, template.ErrTemplateImageRevisionLeaseLost) {
			w.logger.Warn("Failed to release template ImageFS revision", zap.String("revisionID", revision.RevisionID), zap.Error(releaseErr))
		}
		w.logger.Warn("Template ImageFS import failed", zap.String("revisionID", revision.RevisionID), zap.Error(err))
	}
}

func (w *Worker) importRevision(ctx context.Context, revision *template.TemplateImageRevision) error {
	tpl, err := w.queue.GetTemplate(ctx, revision.Scope, revision.TeamID, revision.TemplateID)
	if err != nil || tpl == nil {
		return fmt.Errorf("load template for ImageFS revision: %w", err)
	}
	namespace, err := templateNamespace(tpl)
	if err != nil {
		return err
	}
	pod, err := w.createImportPod(ctx, namespace, tpl, revision)
	if err != nil {
		return err
	}
	defer func() {
		grace := int64(0)
		_ = w.k8s.CoreV1().Pods(pod.Namespace).Delete(context.WithoutCancel(ctx), pod.Name, metav1.DeleteOptions{GracePeriodSeconds: &grace})
	}()
	pod, err = w.waitForImportContainer(ctx, pod.Namespace, pod.Name)
	if err != nil {
		return err
	}
	address, err := w.ctldAddress(ctx, pod)
	if err != nil {
		return err
	}
	response, err := w.ctld.ImportRootFSImage(ctx, address, ctldapi.ImportRootFSImageRequest{
		Target:     ctldapi.RootFSContainerRef{Namespace: pod.Namespace, PodName: pod.Name, PodUID: string(pod.UID), ContainerName: "procd"},
		RevisionID: revision.RevisionID, TeamID: revision.ImageFSStorageScope(), HeadID: "imagefs-" + revision.RevisionID,
		BaseImageRef: w.config.BaseImageRef,
	}, w.config.ImportTimeout)
	if err != nil {
		return fmt.Errorf("import OCI rootfs into S0FS: %w", err)
	}
	var config ocispec.Image
	if err := json.Unmarshal(response.OCIConfig, &config); err != nil {
		return fmt.Errorf("decode imported OCI config: %w", err)
	}
	if err := w.queue.MarkTemplateImageRevisionResolved(ctx, revision.RevisionID, w.config.WorkerID, response.SourceDigest, config.OS, config.Architecture, config.Variant, response.OCIConfig); err != nil {
		return err
	}
	head := &sandboxstore.SandboxRootFSHead{
		SandboxID: "imagefs:" + revision.RevisionID, SourceSandboxID: "imagefs:" + revision.RevisionID,
		TeamID: revision.ImageFSStorageScope(), RuntimeGeneration: 1,
		Reference: response.Reference, Base: response.Head.Base, Image: response.Image,
	}
	if err := w.heads.StageRootFSHead(ctx, head); err != nil {
		return fmt.Errorf("publish ImageFS Head: %w", err)
	}
	if err := w.queue.MarkTemplateImageRevisionReady(ctx, revision.RevisionID, w.config.WorkerID, response.Reference.HeadID, time.Now().UTC()); err != nil {
		return err
	}
	w.logger.Info("Template ImageFS revision ready", zap.String("revisionID", revision.RevisionID), zap.String("headID", response.Reference.HeadID), zap.Int64("createdBytes", response.CreatedBytes), zap.Duration("duration", response.Duration))
	return nil
}

func (w *Worker) createImportPod(ctx context.Context, namespace string, tpl *template.Template, revision *template.TemplateImageRevision) (*corev1.Pod, error) {
	resourceName := naming.TemplateNameForCluster(tpl.Scope, tpl.TeamID, tpl.TemplateID)
	resource := &api.SandboxTemplate{ObjectMeta: metav1.ObjectMeta{Name: resourceName, Namespace: namespace}, Spec: *tpl.Spec.DeepCopy()}
	resource.Spec.ClusterId = stringPointer(w.config.ClusterID)
	resource.Spec.VolumeMounts = nil
	if resource.Spec.Pod != nil {
		resource.Spec.Pod.EmptyDirMounts = nil
	}
	spec := api.BuildPodSpec(resource)
	spec.RestartPolicy = corev1.RestartPolicyNever
	// Mounting the fixed carrier base as an image volume pulls it onto the same
	// node without requiring the intentionally empty base to contain a shell.
	// EnsureBaseImage can then unpack its canonical snapshot without a registry
	// operation in ctld.
	const primerVolume = "carrier-base-primer"
	spec.Volumes = append(spec.Volumes, corev1.Volume{
		Name: primerVolume,
		VolumeSource: corev1.VolumeSource{Image: &corev1.ImageVolumeSource{
			Reference: w.config.BaseImageRef, PullPolicy: corev1.PullIfNotPresent,
		}},
	})
	spec.InitContainers = append([]corev1.Container{{
		Name:            "carrier-base-primer",
		Image:           w.config.PrimerImageRef,
		ImagePullPolicy: corev1.PullIfNotPresent,
		Command:         []string{"/bin/sh", "-ec", "true"},
		VolumeMounts:    []corev1.VolumeMount{{Name: primerVolume, MountPath: "/carrier-base", ReadOnly: true}},
	}}, spec.InitContainers...)
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{GenerateName: "s0fs-import-", Namespace: namespace, Labels: map[string]string{importerLabel: revision.RevisionID}}, Spec: spec}
	created, err := w.k8s.CoreV1().Pods(namespace).Create(ctx, pod, metav1.CreateOptions{})
	if err != nil {
		return nil, fmt.Errorf("create ImageFS import Pod: %w", err)
	}
	return created, nil
}

func (w *Worker) waitForImportContainer(ctx context.Context, namespace, name string) (*corev1.Pod, error) {
	var ready *corev1.Pod
	err := wait.PollUntilContextCancel(ctx, w.config.PollInterval, true, func(ctx context.Context) (bool, error) {
		pod, err := w.k8s.CoreV1().Pods(namespace).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		for _, status := range pod.Status.ContainerStatuses {
			if status.Name != "procd" {
				continue
			}
			if status.State.Running != nil {
				ready = pod
				return true, nil
			}
			if status.State.Terminated != nil {
				return false, fmt.Errorf("ImageFS import container terminated: %s", status.State.Terminated.Message)
			}
			if status.State.Waiting != nil && (status.State.Waiting.Reason == "ErrImagePull" || status.State.Waiting.Reason == "ImagePullBackOff") {
				return false, fmt.Errorf("ImageFS import image pull failed: %s", status.State.Waiting.Message)
			}
		}
		return false, nil
	})
	if err != nil {
		return nil, err
	}
	return ready, nil
}

func (w *Worker) renewLease(ctx context.Context, revisionID string, stop <-chan struct{}) {
	interval := w.config.LeaseDuration / 3
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-stop:
			return
		case <-ticker.C:
			if err := w.queue.RenewTemplateImageRevisionLease(ctx, revisionID, w.config.WorkerID, w.config.LeaseDuration); err != nil {
				w.logger.Warn("Failed to renew template ImageFS lease", zap.String("revisionID", revisionID), zap.Error(err))
				return
			}
		}
	}
}

func templateNamespace(tpl *template.Template) (string, error) {
	if tpl.Scope == naming.ScopeTeam {
		return naming.TemplateNamespaceForTeam(tpl.TeamID)
	}
	return naming.TemplateNamespaceForBuiltin(tpl.TemplateID)
}

func waitForContext(ctx context.Context, duration time.Duration) bool {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func stringPointer(value string) *string {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil
	}
	return &value
}
