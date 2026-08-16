package runtimewatch

import (
	"context"
	"errors"
	"log"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/runtimecontrol"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
)

var (
	ErrPodNotFound       = errors.New("runtime watch pod not found")
	ErrStaleSubscription = errors.New("runtime watch subscription is stale")
)

type ObservationSink interface {
	Desired(context.Context, *corev1.Pod, runtimecontrol.Snapshot) error
	Observed(context.Context, *corev1.Pod, runtimecontrol.Observation) error
	Disconnected(context.Context, *corev1.Pod) error
}

type subscription struct {
	id      uint64
	updates chan runtimecontrol.Snapshot
}

type podState struct {
	pod               *corev1.Pod
	snapshot          runtimecontrol.Snapshot
	subscriptions     map[uint64]*subscription
	currentSubscriber uint64
}

type podEvent struct {
	order   uint64
	pod     *corev1.Pod
	deleted bool
}

// Hub converts informer and portal-ready annotations into level-triggered
// runtime snapshots. Subscribers receive only the latest complete state.
type Hub struct {
	mu             sync.Mutex
	pods           map[string]*podState
	pendingEvents  map[string]podEvent
	eventQueue     workqueue.TypedRateLimitingInterface[string]
	done           chan struct{}
	stopOnce       sync.Once
	nextEvent      uint64
	nextSequence   uint64
	nextSubscriber uint64
	sink           ObservationSink
	operationLocks [64]sync.Mutex
}

func NewHub(sink ObservationSink) *Hub {
	return &Hub{
		pods:          make(map[string]*podState),
		pendingEvents: make(map[string]podEvent),
		eventQueue:    workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[string]()),
		done:          make(chan struct{}),
		sink:          sink,
	}
}

func (h *Hub) PodEventHandler() cache.ResourceEventHandler {
	return cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj any) {
			if pod, ok := obj.(*corev1.Pod); ok {
				h.enqueuePodEvent(pod, false)
			}
		},
		UpdateFunc: func(_, newObj any) {
			if pod, ok := newObj.(*corev1.Pod); ok {
				h.enqueuePodEvent(pod, false)
			}
		},
		DeleteFunc: func(obj any) {
			pod, ok := obj.(*corev1.Pod)
			if !ok {
				tombstone, tombstoneOK := obj.(cache.DeletedFinalStateUnknown)
				if tombstoneOK {
					pod, ok = tombstone.Obj.(*corev1.Pod)
				}
			}
			if ok {
				h.enqueuePodEvent(pod, true)
			}
		},
	}
}

// Run processes informer events without blocking the shared informer callback.
// Events for one Pod are coalesced and serialized while different Pods can
// progress concurrently.
func (h *Hub) Run(ctx context.Context, workers int) {
	if h == nil || h.eventQueue == nil {
		return
	}
	if workers <= 0 {
		workers = 1
	}

	var workerGroup sync.WaitGroup
	workerGroup.Add(workers)
	for range workers {
		go func() {
			defer workerGroup.Done()
			for h.processNextPodEvent(ctx) {
			}
		}()
	}

	<-ctx.Done()
	h.stopOnce.Do(func() {
		close(h.done)
	})
	h.eventQueue.ShutDownWithDrain()
	workerGroup.Wait()
}

// Done closes when the primary-scoped hub stops. Runtime watch handlers use it
// to close upgraded connections before the HA primary lease is released.
func (h *Hub) Done() <-chan struct{} {
	if h == nil || h.done == nil {
		done := make(chan struct{})
		close(done)
		return done
	}
	return h.done
}

func (h *Hub) enqueuePodEvent(pod *corev1.Pod, deleted bool) {
	if h == nil || h.eventQueue == nil || pod == nil || pod.UID == "" {
		return
	}
	uid := string(pod.UID)
	h.mu.Lock()
	h.nextEvent++
	h.pendingEvents[uid] = podEvent{
		order:   h.nextEvent,
		pod:     pod.DeepCopy(),
		deleted: deleted,
	}
	h.mu.Unlock()
	h.eventQueue.Add(uid)
}

func (h *Hub) processNextPodEvent(ctx context.Context) bool {
	uid, shutdown := h.eventQueue.Get()
	if shutdown {
		return false
	}
	defer h.eventQueue.Done(uid)

	h.mu.Lock()
	event, ok := h.pendingEvents[uid]
	h.mu.Unlock()
	if !ok {
		h.eventQueue.Forget(uid)
		return true
	}

	var err error
	if event.deleted {
		h.DeletePod(event.pod)
	} else {
		err = h.updatePod(ctx, event.pod)
	}

	h.mu.Lock()
	current, stillCurrent := h.pendingEvents[uid]
	if stillCurrent && current.order == event.order && err == nil {
		delete(h.pendingEvents, uid)
	}
	h.mu.Unlock()

	if err != nil && stillCurrent && current.order == event.order && ctx.Err() == nil {
		log.Printf("ctld runtime watch apply pod %s/%s: %v", event.pod.Namespace, event.pod.Name, err)
		h.eventQueue.AddRateLimited(uid)
		return true
	}
	h.eventQueue.Forget(uid)
	return true
}

func (h *Hub) updatePod(parent context.Context, pod *corev1.Pod) error {
	if h == nil || pod == nil || pod.UID == "" {
		return nil
	}
	uid := string(pod.UID)
	operationLock := h.operationLock(uid)
	operationLock.Lock()
	defer operationLock.Unlock()

	next := snapshotForPod(pod)

	h.mu.Lock()
	state := h.pods[uid]
	if state == nil {
		state = &podState{subscriptions: make(map[uint64]*subscription)}
		h.pods[uid] = state
	}
	changed := !snapshotContentEqual(state.snapshot, next)
	state.pod = pod.DeepCopy()
	sink := h.sink
	podCopy := state.pod.DeepCopy()
	h.mu.Unlock()

	if !changed {
		return nil
	}
	var sinkErr error
	if sink != nil {
		ctx, cancel := context.WithTimeout(parent, 10*time.Second)
		err := sink.Desired(ctx, podCopy, next)
		cancel()
		if err != nil {
			sinkErr = err
			next = runtimecontrol.Snapshot{
				State:  runtimecontrol.DesiredRevoked,
				Reason: "failed to publish desired runtime state",
			}
		}
	}

	h.mu.Lock()
	state = h.pods[uid]
	if state != nil {
		h.nextSequence++
		next.Sequence = h.nextSequence
		state.snapshot = next
		for _, sub := range state.subscriptions {
			sendLatest(sub.updates, next)
		}
	}
	h.mu.Unlock()
	return sinkErr
}

func (h *Hub) DeletePod(pod *corev1.Pod) {
	if h == nil || pod == nil || pod.UID == "" {
		return
	}
	uid := string(pod.UID)
	operationLock := h.operationLock(uid)
	operationLock.Lock()
	defer operationLock.Unlock()

	h.mu.Lock()
	state := h.pods[uid]
	delete(h.pods, uid)
	if state != nil {
		for _, sub := range state.subscriptions {
			close(sub.updates)
		}
	}
	h.mu.Unlock()
}

func (h *Hub) Resolve(namespace, name, uid string) (*corev1.Pod, error) {
	uid = strings.TrimSpace(uid)
	operationLock := h.operationLock(uid)
	operationLock.Lock()
	defer operationLock.Unlock()

	h.mu.Lock()
	defer h.mu.Unlock()
	state := h.pods[uid]
	if state == nil || state.pod == nil ||
		state.pod.Namespace != strings.TrimSpace(namespace) ||
		state.pod.Name != strings.TrimSpace(name) {
		return nil, ErrPodNotFound
	}
	return state.pod.DeepCopy(), nil
}

func (h *Hub) Subscribe(namespace, name, uid string) (uint64, <-chan runtimecontrol.Snapshot, func(), error) {
	if h == nil {
		return 0, nil, nil, ErrPodNotFound
	}
	uid = strings.TrimSpace(uid)
	operationLock := h.operationLock(uid)
	operationLock.Lock()
	defer operationLock.Unlock()

	h.mu.Lock()
	state := h.pods[uid]
	if state == nil || state.pod == nil ||
		state.pod.Namespace != strings.TrimSpace(namespace) ||
		state.pod.Name != strings.TrimSpace(name) {
		h.mu.Unlock()
		return 0, nil, nil, ErrPodNotFound
	}
	pod := state.pod.DeepCopy()
	sink := h.sink
	h.mu.Unlock()

	if sink != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		err := sink.Disconnected(ctx, pod)
		cancel()
		if err != nil {
			return 0, nil, nil, err
		}
	}

	h.mu.Lock()
	state = h.pods[uid]
	if state == nil || state.pod == nil {
		h.mu.Unlock()
		return 0, nil, nil, ErrPodNotFound
	}
	for id, existing := range state.subscriptions {
		close(existing.updates)
		delete(state.subscriptions, id)
	}
	h.nextSubscriber++
	sub := &subscription{
		id:      h.nextSubscriber,
		updates: make(chan runtimecontrol.Snapshot, 1),
	}
	state.subscriptions[sub.id] = sub
	state.currentSubscriber = sub.id
	sendLatest(sub.updates, state.snapshot)
	h.mu.Unlock()

	var once sync.Once
	unsubscribe := func() {
		once.Do(func() {
			h.unsubscribe(uid, sub.id)
		})
	}
	return sub.id, sub.updates, unsubscribe, nil
}

func (h *Hub) unsubscribe(uid string, subscriberID uint64) {
	operationLock := h.operationLock(uid)
	operationLock.Lock()
	defer operationLock.Unlock()

	h.mu.Lock()
	state := h.pods[uid]
	if state == nil {
		h.mu.Unlock()
		return
	}
	sub := state.subscriptions[subscriberID]
	delete(state.subscriptions, subscriberID)
	current := state.currentSubscriber == subscriberID
	if current {
		state.currentSubscriber = 0
	}
	var pod *corev1.Pod
	if state.pod != nil {
		pod = state.pod.DeepCopy()
	}
	sink := h.sink
	h.mu.Unlock()

	if sub != nil {
		close(sub.updates)
	}
	if current && sink != nil && pod != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		err := sink.Disconnected(ctx, pod)
		cancel()
		if err != nil {
			log.Printf("ctld runtime watch mark pod %s/%s disconnected: %v", pod.Namespace, pod.Name, err)
		}
	}
}

func (h *Hub) Observe(ctx context.Context, uid string, subscriberID uint64, observation runtimecontrol.Observation) error {
	if h == nil {
		return ErrPodNotFound
	}
	uid = strings.TrimSpace(uid)
	operationLock := h.operationLock(uid)
	operationLock.Lock()
	defer operationLock.Unlock()

	h.mu.Lock()
	state := h.pods[uid]
	if state == nil || state.pod == nil {
		h.mu.Unlock()
		return ErrPodNotFound
	}
	if state.currentSubscriber != subscriberID || state.subscriptions[subscriberID] == nil {
		h.mu.Unlock()
		return ErrStaleSubscription
	}
	snapshot := state.snapshot
	pod := state.pod.DeepCopy()
	sink := h.sink
	h.mu.Unlock()

	if err := validateObservation(snapshot, observation); err != nil {
		return err
	}
	if sink != nil {
		return sink.Observed(ctx, pod, observation)
	}
	return nil
}

func (h *Hub) operationLock(uid string) *sync.Mutex {
	var hash uint32 = 2166136261
	for i := 0; i < len(uid); i++ {
		hash ^= uint32(uid[i])
		hash *= 16777619
	}
	return &h.operationLocks[hash%uint32(len(h.operationLocks))]
}

func snapshotForPod(pod *corev1.Pod) runtimecontrol.Snapshot {
	if pod == nil || pod.DeletionTimestamp != nil {
		return runtimecontrol.Snapshot{
			State:  runtimecontrol.DesiredRevoked,
			Reason: "pod is deleting",
		}
	}
	publishedRevision := ""
	readyRevision := ""
	if pod.Annotations != nil {
		publishedRevision = strings.TrimSpace(pod.Annotations[runtimecontrol.AnnotationAssignmentRevision])
		readyRevision = strings.TrimSpace(pod.Annotations[runtimecontrol.AnnotationAssignmentReady])
	}
	if publishedRevision == "" {
		return runtimecontrol.Snapshot{State: runtimecontrol.DesiredStandby}
	}
	assignment, revision, err := runtimecontrol.AssignmentFromPod(pod)
	if err != nil {
		return runtimecontrol.Snapshot{
			State:  runtimecontrol.DesiredRevoked,
			Reason: err.Error(),
		}
	}
	if assignment == nil {
		return runtimecontrol.Snapshot{
			State:  runtimecontrol.DesiredRevoked,
			Reason: "published runtime assignment is missing",
		}
	}
	if publishedRevision != revision {
		return runtimecontrol.Snapshot{
			State:  runtimecontrol.DesiredRevoked,
			Reason: "published runtime assignment revision does not match the manifest",
		}
	}
	state := runtimecontrol.DesiredWaitingRootFS
	if readyRevision == revision {
		state = runtimecontrol.DesiredActive
	} else if readyRevision != "" {
		return runtimecontrol.Snapshot{
			State:  runtimecontrol.DesiredRevoked,
			Reason: "runtime activation revision does not match the manifest",
		}
	}
	return runtimecontrol.Snapshot{
		State:      state,
		Revision:   revision,
		Assignment: assignment,
	}
}

func validateObservation(snapshot runtimecontrol.Snapshot, observation runtimecontrol.Observation) error {
	switch observation.State {
	case runtimecontrol.ObservedStandby:
		if snapshot.State != runtimecontrol.DesiredStandby {
			return errors.New("standby observation does not match desired state")
		}
		return nil
	case runtimecontrol.ObservedWaitingRootFS:
		if snapshot.Assignment == nil || snapshot.State != runtimecontrol.DesiredWaitingRootFS {
			return errors.New("waiting observation does not match desired state")
		}
	case runtimecontrol.ObservedLoading,
		runtimecontrol.ObservedRecovering,
		runtimecontrol.ObservedReady,
		runtimecontrol.ObservedFailed:
		if snapshot.Assignment == nil || snapshot.State != runtimecontrol.DesiredActive {
			return errors.New("activation observation does not match desired state")
		}
	default:
		return errors.New("invalid runtime observation state")
	}
	if observation.Revision != snapshot.Revision {
		return errors.New("runtime observation revision does not match assignment")
	}
	if observation.RuntimeGeneration != snapshot.Assignment.RuntimeGeneration {
		return errors.New("runtime observation generation does not match assignment")
	}
	return nil
}

func snapshotContentEqual(left, right runtimecontrol.Snapshot) bool {
	left.Sequence = 0
	right.Sequence = 0
	return reflect.DeepEqual(left, right)
}

func sendLatest(ch chan runtimecontrol.Snapshot, snapshot runtimecontrol.Snapshot) {
	select {
	case ch <- snapshot:
		return
	default:
	}
	select {
	case <-ch:
	default:
	}
	select {
	case ch <- snapshot:
	default:
	}
}
