package runtimewatch

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/runtimecontrol"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func TestHubSendsCurrentSnapshotAndActualChanges(t *testing.T) {
	sink := &recordingSink{}
	hub := NewHub(sink)
	pod := testPod()
	hub.UpdatePod(pod)

	subscriberID, updates, unsubscribe, err := hub.Subscribe(pod.Namespace, pod.Name, string(pod.UID))
	if err != nil {
		t.Fatalf("Subscribe() error = %v", err)
	}
	defer unsubscribe()

	if snapshot := receiveSnapshot(t, updates); snapshot.State != runtimecontrol.DesiredStandby {
		t.Fatalf("initial state = %q, want standby", snapshot.State)
	}

	claimed := pod.DeepCopy()
	claimed.Annotations = map[string]string{
		runtimecontrol.AnnotationSandboxID:         "sandbox-1",
		runtimecontrol.AnnotationTeamID:            "team-1",
		runtimecontrol.AnnotationRuntimeGeneration: "2",
		runtimecontrol.AnnotationConfig:            `{"env_vars":{"MODE":"test"}}`,
	}
	hub.UpdatePod(claimed)
	select {
	case snapshot := <-updates:
		t.Fatalf("unpublished assignment produced snapshot %#v", snapshot)
	default:
	}
	publishTestAssignment(t, claimed)
	hub.UpdatePod(claimed)
	waiting := receiveSnapshot(t, updates)
	if waiting.State != runtimecontrol.DesiredWaitingStorage || waiting.Assignment == nil {
		t.Fatalf("waiting snapshot = %#v", waiting)
	}

	ready := claimed.DeepCopy()
	ready.Annotations[runtimecontrol.AnnotationAssignmentReady] = waiting.Revision
	hub.UpdatePod(ready)
	active := receiveSnapshot(t, updates)
	if active.State != runtimecontrol.DesiredActive || active.Revision != waiting.Revision {
		t.Fatalf("active snapshot = %#v", active)
	}

	observation := runtimecontrol.Observation{
		State:             runtimecontrol.ObservedReady,
		Revision:          active.Revision,
		RuntimeGeneration: active.Assignment.RuntimeGeneration,
	}
	if err := hub.Observe(context.Background(), string(pod.UID), subscriberID, observation); err != nil {
		t.Fatalf("Observe() error = %v", err)
	}
	if got := sink.lastObservation(); got != observation {
		t.Fatalf("observed = %#v, want %#v", got, observation)
	}

	unchanged := ready.DeepCopy()
	unchanged.Annotations["unrelated"] = "value"
	hub.UpdatePod(unchanged)
	select {
	case snapshot := <-updates:
		t.Fatalf("unrelated update produced snapshot %#v", snapshot)
	default:
	}
}

func TestHubCoalescesToLatestCompleteSnapshot(t *testing.T) {
	hub := NewHub(nil)
	pod := testPod()
	hub.UpdatePod(pod)
	_, updates, unsubscribe, err := hub.Subscribe(pod.Namespace, pod.Name, string(pod.UID))
	if err != nil {
		t.Fatalf("Subscribe() error = %v", err)
	}
	defer unsubscribe()
	_ = receiveSnapshot(t, updates)

	first := pod.DeepCopy()
	first.Annotations = map[string]string{
		runtimecontrol.AnnotationSandboxID:         "sandbox-1",
		runtimecontrol.AnnotationRuntimeGeneration: "1",
		runtimecontrol.AnnotationConfig:            `{"env_vars":{"STEP":"one"}}`,
	}
	publishTestAssignment(t, first)
	hub.UpdatePod(first)
	second := first.DeepCopy()
	second.Annotations[runtimecontrol.AnnotationConfig] = `{"env_vars":{"STEP":"two"}}`
	publishTestAssignment(t, second)
	hub.UpdatePod(second)

	snapshot := receiveSnapshot(t, updates)
	if snapshot.Assignment == nil || snapshot.Assignment.EnvVars["STEP"] != "two" {
		t.Fatalf("coalesced snapshot = %#v", snapshot)
	}
}

func TestHubOnlyAcceptsNewestSubscription(t *testing.T) {
	sink := &recordingSink{}
	hub := NewHub(sink)
	pod := testPod()
	hub.UpdatePod(pod)

	firstID, firstUpdates, firstUnsubscribe, err := hub.Subscribe(pod.Namespace, pod.Name, string(pod.UID))
	if err != nil {
		t.Fatalf("first Subscribe() error = %v", err)
	}
	defer firstUnsubscribe()
	_ = receiveSnapshot(t, firstUpdates)

	secondID, secondUpdates, secondUnsubscribe, err := hub.Subscribe(pod.Namespace, pod.Name, string(pod.UID))
	if err != nil {
		t.Fatalf("second Subscribe() error = %v", err)
	}
	defer secondUnsubscribe()
	_ = receiveSnapshot(t, secondUpdates)

	if _, ok := <-firstUpdates; ok {
		t.Fatal("superseded subscription remained open")
	}
	if err := hub.Observe(context.Background(), string(pod.UID), firstID, runtimecontrol.Observation{
		State: runtimecontrol.ObservedStandby,
	}); !errors.Is(err, ErrStaleSubscription) {
		t.Fatalf("stale Observe() error = %v", err)
	}
	if err := hub.Observe(context.Background(), string(pod.UID), secondID, runtimecontrol.Observation{
		State: runtimecontrol.ObservedStandby,
	}); err != nil {
		t.Fatalf("current Observe() error = %v", err)
	}
}

func TestHubRejectsReadyObservationBeforeStorageActivation(t *testing.T) {
	hub := NewHub(nil)
	pod := testPod()
	pod.Annotations = map[string]string{
		runtimecontrol.AnnotationSandboxID:         "sandbox-1",
		runtimecontrol.AnnotationRuntimeGeneration: "1",
	}
	publishTestAssignment(t, pod)
	hub.UpdatePod(pod)

	subscriberID, updates, unsubscribe, err := hub.Subscribe(pod.Namespace, pod.Name, string(pod.UID))
	if err != nil {
		t.Fatalf("Subscribe() error = %v", err)
	}
	defer unsubscribe()
	snapshot := receiveSnapshot(t, updates)
	if snapshot.State != runtimecontrol.DesiredWaitingStorage {
		t.Fatalf("snapshot state = %q, want waiting_storage", snapshot.State)
	}

	err = hub.Observe(context.Background(), string(pod.UID), subscriberID, runtimecontrol.Observation{
		State:             runtimecontrol.ObservedReady,
		Revision:          snapshot.Revision,
		RuntimeGeneration: snapshot.Assignment.RuntimeGeneration,
	})
	if err == nil {
		t.Fatal("Observe() accepted ready before storage activation")
	}
}

func TestHubPublishesDesiredStateBeforeSnapshotCanBeObserved(t *testing.T) {
	sink := &blockingDesiredSink{
		recordingSink: recordingSink{},
		started:       make(chan struct{}),
		release:       make(chan struct{}),
	}
	hub := NewHub(sink)
	pod := testPod()
	hub.UpdatePod(pod)

	subscriberID, updates, unsubscribe, err := hub.Subscribe(pod.Namespace, pod.Name, string(pod.UID))
	if err != nil {
		t.Fatalf("Subscribe() error = %v", err)
	}
	defer unsubscribe()
	_ = receiveSnapshot(t, updates)

	claimed := pod.DeepCopy()
	claimed.Annotations = map[string]string{
		runtimecontrol.AnnotationSandboxID:         "sandbox-1",
		runtimecontrol.AnnotationRuntimeGeneration: "1",
	}
	publishTestAssignment(t, claimed)
	go hub.UpdatePod(claimed)

	select {
	case <-sink.started:
	case <-time.After(time.Second):
		t.Fatal("desired state publication did not start")
	}
	select {
	case snapshot := <-updates:
		t.Fatalf("snapshot %#v became visible before desired state publication completed", snapshot)
	default:
	}

	close(sink.release)
	snapshot := receiveSnapshot(t, updates)
	if snapshot.State != runtimecontrol.DesiredWaitingStorage {
		t.Fatalf("snapshot state = %q, want waiting_storage", snapshot.State)
	}
	if err := hub.Observe(context.Background(), string(pod.UID), subscriberID, runtimecontrol.Observation{
		State:             runtimecontrol.ObservedWaiting,
		Revision:          snapshot.Revision,
		RuntimeGeneration: snapshot.Assignment.RuntimeGeneration,
	}); err != nil {
		t.Fatalf("Observe() error = %v", err)
	}
	if got := sink.eventOrder(); len(got) < 2 || got[len(got)-2] != "desired" || got[len(got)-1] != "observed" {
		t.Fatalf("event order = %v, want desired before observed", got)
	}
}

func TestHubInformerHandlerDoesNotBlockOnStatusPublication(t *testing.T) {
	sink := &blockingDesiredSink{
		recordingSink: recordingSink{},
		started:       make(chan struct{}),
		release:       make(chan struct{}),
	}
	hub := NewHub(sink)
	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan struct{})
	go func() {
		hub.Run(ctx, 2)
		close(runDone)
	}()
	t.Cleanup(func() {
		cancel()
		<-runDone
	})

	claimed := testPod()
	claimed.Annotations = map[string]string{
		runtimecontrol.AnnotationSandboxID:         "sandbox-1",
		runtimecontrol.AnnotationRuntimeGeneration: "1",
	}
	publishTestAssignment(t, claimed)

	handler := hub.PodEventHandler()
	addDone := make(chan struct{})
	go func() {
		handler.OnAdd(claimed, false)
		close(addDone)
	}()
	select {
	case <-addDone:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("informer add handler blocked on desired state publication")
	}
	select {
	case <-sink.started:
	case <-time.After(time.Second):
		t.Fatal("desired state publication did not start")
	}

	active := claimed.DeepCopy()
	active.Annotations[runtimecontrol.AnnotationAssignmentReady] =
		active.Annotations[runtimecontrol.AnnotationAssignmentRevision]
	updateDone := make(chan struct{})
	go func() {
		handler.OnUpdate(claimed, active)
		close(updateDone)
	}()
	select {
	case <-updateDone:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("informer update handler blocked behind another Pod event")
	}

	close(sink.release)
	var updates <-chan runtimecontrol.Snapshot
	subscribeDeadline := time.After(time.Second)
	for updates == nil {
		_, candidate, unsubscribe, err := hub.Subscribe(claimed.Namespace, claimed.Name, string(claimed.UID))
		if err == nil {
			defer unsubscribe()
			updates = candidate
			break
		}
		select {
		case <-subscribeDeadline:
			t.Fatalf("Subscribe() error = %v", err)
		default:
			time.Sleep(time.Millisecond)
		}
	}

	snapshotDeadline := time.After(time.Second)
	for {
		select {
		case snapshot := <-updates:
			if snapshot.State == runtimecontrol.DesiredActive {
				return
			}
		case <-snapshotDeadline:
			t.Fatal("latest informer event was not applied")
		}
	}
}

func testPod() *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "sandbox-system",
			Name:      "sandbox-pod",
			UID:       types.UID("pod-uid"),
		},
		Status: corev1.PodStatus{PodIP: "127.0.0.1"},
	}
}

func publishTestAssignment(t *testing.T, pod *corev1.Pod) {
	t.Helper()
	_, revision, err := runtimecontrol.AssignmentFromPod(pod)
	if err != nil {
		t.Fatal(err)
	}
	if revision == "" {
		t.Fatal("runtime assignment revision is empty")
	}
	pod.Annotations[runtimecontrol.AnnotationAssignmentRevision] = revision
}

func receiveSnapshot(t *testing.T, updates <-chan runtimecontrol.Snapshot) runtimecontrol.Snapshot {
	t.Helper()
	snapshot, ok := <-updates
	if !ok {
		t.Fatal("snapshot channel closed")
	}
	return snapshot
}

type blockingDesiredSink struct {
	recordingSink
	started chan struct{}
	release chan struct{}
	once    sync.Once
	events  []string
}

func (s *blockingDesiredSink) Desired(ctx context.Context, pod *corev1.Pod, snapshot runtimecontrol.Snapshot) error {
	if snapshot.State == runtimecontrol.DesiredWaitingStorage {
		s.once.Do(func() {
			close(s.started)
			select {
			case <-s.release:
			case <-ctx.Done():
			}
		})
	}
	s.recordingSink.Desired(ctx, pod, snapshot)
	s.mu.Lock()
	s.events = append(s.events, "desired")
	s.mu.Unlock()
	return ctx.Err()
}

func (s *blockingDesiredSink) Observed(ctx context.Context, pod *corev1.Pod, observation runtimecontrol.Observation) error {
	if err := s.recordingSink.Observed(ctx, pod, observation); err != nil {
		return err
	}
	s.mu.Lock()
	s.events = append(s.events, "observed")
	s.mu.Unlock()
	return nil
}

func (s *blockingDesiredSink) eventOrder() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.events...)
}

type recordingSink struct {
	mu             sync.Mutex
	desired        []runtimecontrol.Snapshot
	observed       []runtimecontrol.Observation
	disconnections int
}

func (s *recordingSink) Desired(_ context.Context, _ *corev1.Pod, snapshot runtimecontrol.Snapshot) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.desired = append(s.desired, snapshot)
	return nil
}

func (s *recordingSink) Observed(_ context.Context, _ *corev1.Pod, observation runtimecontrol.Observation) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.observed = append(s.observed, observation)
	return nil
}

func (s *recordingSink) Disconnected(_ context.Context, _ *corev1.Pod) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.disconnections++
	return nil
}

func (s *recordingSink) lastObservation() runtimecontrol.Observation {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.observed) == 0 {
		return runtimecontrol.Observation{}
	}
	return s.observed[len(s.observed)-1]
}
