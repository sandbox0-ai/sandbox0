package watcher

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/controller"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
)

type SandboxInfo struct {
	Namespace                 string
	Name                      string
	UID                       types.UID
	ResourceVersion           string
	PodIP                     string
	NodeName                  string
	SandboxID                 string
	TeamID                    string
	OwnerKind                 string
	FunctionID                string
	FunctionRevisionID        string
	FunctionRuntimeInstanceID string
	NetworkPolicy             string
	NetworkPolicyHash         string
	NetworkAppliedHash        string
}

type ServiceInfo struct {
	Namespace       string
	Name            string
	ResourceVersion string
	ClusterIP       string
	Ports           []corev1.ServicePort
	Labels          map[string]string
}

type EndpointsInfo struct {
	Namespace       string
	Name            string
	ResourceVersion string
	Addresses       []string
	Ports           []corev1.EndpointPort
}

type NodeInfo struct {
	Name            string
	ResourceVersion string
	InternalIPs     []string
}

type Watcher struct {
	k8sClient       kubernetes.Interface
	informerFactory informers.SharedInformerFactory
	podInformer     cache.SharedIndexInformer
	serviceInformer cache.SharedIndexInformer
	sliceInformer   cache.SharedIndexInformer
	nodeInformer    cache.SharedIndexInformer
	logger          *zap.Logger

	mu             sync.RWMutex
	sandboxes      map[string]*SandboxInfo
	services       map[string]*ServiceInfo
	endpoints      map[string]*EndpointsInfo
	endpointSlices map[string]*endpointSliceEntry
	nodes          map[string]*NodeInfo

	onSandboxUpsert   func(*SandboxInfo)
	onSandboxDelete   func(*SandboxInfo)
	onServiceUpsert   func(*ServiceInfo)
	onServiceDelete   func(*ServiceInfo)
	onEndpointsUpsert func(*EndpointsInfo)
	onEndpointsDelete func(*EndpointsInfo)
	onNodeUpsert      func(*NodeInfo)
	onNodeDelete      func(*NodeInfo)
}

const podNodeIndex = "spec.nodeName"

func NewWatcher(
	k8sClient kubernetes.Interface,
	resyncPeriod time.Duration,
	logger *zap.Logger,
) *Watcher {
	if logger == nil {
		logger = zap.NewNop()
	}
	return &Watcher{
		k8sClient:       k8sClient,
		informerFactory: informers.NewSharedInformerFactory(k8sClient, resyncPeriod),
		logger:          logger,
		sandboxes:       make(map[string]*SandboxInfo),
		services:        make(map[string]*ServiceInfo),
		endpoints:       make(map[string]*EndpointsInfo),
		endpointSlices:  make(map[string]*endpointSliceEntry),
		nodes:           make(map[string]*NodeInfo),
	}
}

func (w *Watcher) SetSandboxHandlers(
	onUpsert func(*SandboxInfo),
	onDelete func(*SandboxInfo),
) {
	w.onSandboxUpsert = onUpsert
	w.onSandboxDelete = onDelete
}

func (w *Watcher) SetServiceHandlers(
	onUpsert func(*ServiceInfo),
	onDelete func(*ServiceInfo),
) {
	w.onServiceUpsert = onUpsert
	w.onServiceDelete = onDelete
}

func (w *Watcher) SetEndpointsHandlers(
	onUpsert func(*EndpointsInfo),
	onDelete func(*EndpointsInfo),
) {
	w.onEndpointsUpsert = onUpsert
	w.onEndpointsDelete = onDelete
}

func (w *Watcher) SetNodeHandlers(
	onUpsert func(*NodeInfo),
	onDelete func(*NodeInfo),
) {
	w.onNodeUpsert = onUpsert
	w.onNodeDelete = onDelete
}

func (w *Watcher) Start(ctx context.Context) error {
	if w.k8sClient == nil {
		return fmt.Errorf("k8s client is nil")
	}
	podInformer := w.informerFactory.Core().V1().Pods().Informer()
	serviceInformer := w.informerFactory.Core().V1().Services().Informer()
	endpointSliceInformer := w.informerFactory.Discovery().V1().EndpointSlices().Informer()
	nodeInformer := w.informerFactory.Core().V1().Nodes().Informer()
	if err := podInformer.AddIndexers(cache.Indexers{podNodeIndex: indexPodByNode}); err != nil {
		return fmt.Errorf("add pod node indexer: %w", err)
	}
	w.podInformer = podInformer
	w.serviceInformer = serviceInformer
	w.sliceInformer = endpointSliceInformer
	w.nodeInformer = nodeInformer

	podInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    w.handlePodUpsert,
		UpdateFunc: func(_, obj any) { w.handlePodUpsert(obj) },
		DeleteFunc: w.handlePodDelete,
	})
	serviceInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    w.handleServiceUpsert,
		UpdateFunc: func(_, obj any) { w.handleServiceUpsert(obj) },
		DeleteFunc: w.handleServiceDelete,
	})
	endpointSliceInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    w.handleEndpointSliceUpsert,
		UpdateFunc: func(_, obj any) { w.handleEndpointSliceUpsert(obj) },
		DeleteFunc: w.handleEndpointSliceDelete,
	})
	nodeInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    w.handleNodeUpsert,
		UpdateFunc: func(_, obj any) { w.handleNodeUpsert(obj) },
		DeleteFunc: w.handleNodeDelete,
	})

	w.informerFactory.Start(ctx.Done())

	w.logger.Info("Waiting for informer caches to sync")
	if !cache.WaitForCacheSync(
		ctx.Done(),
		podInformer.HasSynced,
		serviceInformer.HasSynced,
		endpointSliceInformer.HasSynced,
		nodeInformer.HasSynced,
	) {
		return fmt.Errorf("failed to sync informer cache")
	}

	w.logger.Info("netd watcher started and cache synced")
	return nil
}

func (w *Watcher) ListSandboxesByNode(nodeName string) []*SandboxInfo {
	if w.podInformer != nil {
		var objects []any
		if nodeName != "" {
			items, err := w.podInformer.GetIndexer().ByIndex(podNodeIndex, nodeName)
			if err != nil {
				w.logger.Warn("Failed to list sandbox pods by node index", zap.String("node", nodeName), zap.Error(err))
				return nil
			}
			objects = items
		} else {
			objects = w.podInformer.GetStore().List()
		}
		out := make([]*SandboxInfo, 0, len(objects))
		for _, obj := range objects {
			info := activeSandboxInfoFromPod(getPod(obj))
			if info == nil {
				continue
			}
			out = append(out, info)
		}
		return out
	}

	w.mu.RLock()
	defer w.mu.RUnlock()
	out := make([]*SandboxInfo, 0, len(w.sandboxes))
	for _, info := range w.sandboxes {
		if nodeName == "" || info.NodeName == nodeName {
			out = append(out, cloneSandboxInfo(info))
		}
	}
	return out
}

func (w *Watcher) ListServices() []*ServiceInfo {
	if w.serviceInformer != nil {
		objects := w.serviceInformer.GetStore().List()
		out := make([]*ServiceInfo, 0, len(objects))
		for _, obj := range objects {
			info := serviceInfoFromService(getService(obj))
			if info != nil {
				out = append(out, info)
			}
		}
		return out
	}

	w.mu.RLock()
	defer w.mu.RUnlock()
	out := make([]*ServiceInfo, 0, len(w.services))
	for _, info := range w.services {
		out = append(out, cloneServiceInfo(info))
	}
	return out
}

func (w *Watcher) ListEndpoints() []*EndpointsInfo {
	if w.sliceInformer != nil {
		objects := w.sliceInformer.GetStore().List()
		return endpointsInfosFromSlices(objects)
	}

	w.mu.RLock()
	defer w.mu.RUnlock()
	out := make([]*EndpointsInfo, 0, len(w.endpoints))
	for _, info := range w.endpoints {
		out = append(out, cloneEndpointsInfo(info))
	}
	return out
}

func (w *Watcher) GetService(namespace, name string) *ServiceInfo {
	key := namespace + "/" + name
	w.mu.RLock()
	defer w.mu.RUnlock()
	info := w.services[key]
	if info == nil {
		return nil
	}
	return cloneServiceInfo(info)
}

func (w *Watcher) GetEndpoints(namespace, name string) *EndpointsInfo {
	key := namespace + "/" + name
	w.mu.RLock()
	defer w.mu.RUnlock()
	info := w.endpoints[key]
	if info == nil {
		return nil
	}
	return cloneEndpointsInfo(info)
}

func (w *Watcher) GetNode(name string) *NodeInfo {
	w.mu.RLock()
	defer w.mu.RUnlock()
	info := w.nodes[name]
	if info == nil {
		return nil
	}
	return cloneNodeInfo(info)
}

func (w *Watcher) handlePodUpsert(obj any) {
	pod := getPod(obj)
	if pod == nil {
		return
	}
	info := activeSandboxInfoFromPod(pod)
	if info == nil {
		w.removeCachedSandboxPod(pod, false)
		return
	}

	key := pod.Namespace + "/" + pod.Name
	w.mu.Lock()
	if existing := w.sandboxes[key]; existing != nil {
		if !isResourceVersionNewer(existing.ResourceVersion, info.ResourceVersion) {
			w.mu.Unlock()
			return
		}
	}
	w.sandboxes[key] = info
	w.mu.Unlock()

	w.logger.Info("Sandbox pod detected",
		zap.String("sandbox", key),
		zap.String("sandbox_id", info.SandboxID),
		zap.String("pod_ip", info.PodIP),
		zap.String("node_name", info.NodeName),
		zap.String("network_policy_hash", info.NetworkPolicyHash),
	)

	if w.onSandboxUpsert != nil {
		w.onSandboxUpsert(cloneSandboxInfo(info))
	}
}

func (w *Watcher) handlePodDelete(obj any) {
	pod := getPod(obj)
	if pod == nil {
		return
	}
	w.removeCachedSandboxPod(pod, true)
}

func (w *Watcher) removeCachedSandboxPod(pod *corev1.Pod, allowPodFallback bool) {
	if pod == nil {
		return
	}
	key := pod.Namespace + "/" + pod.Name
	w.mu.Lock()
	info := w.sandboxes[key]
	if info == nil {
		if !allowPodFallback {
			w.mu.Unlock()
			return
		}
		info = sandboxInfoFromPod(pod)
	}
	if info == nil {
		w.mu.Unlock()
		return
	}
	if existing := w.sandboxes[key]; existing != nil {
		if existing.UID != "" && info.UID != "" && existing.UID != info.UID {
			w.mu.Unlock()
			return
		}
		info = existing
	}
	delete(w.sandboxes, key)
	w.mu.Unlock()

	w.logger.Info("Sandbox pod removed",
		zap.String("sandbox", key),
		zap.String("sandbox_id", info.SandboxID),
		zap.String("pod_ip", info.PodIP),
	)

	if w.onSandboxDelete != nil {
		w.onSandboxDelete(cloneSandboxInfo(info))
	}
}

func (w *Watcher) handleServiceUpsert(obj any) {
	service := getService(obj)
	if service == nil {
		return
	}
	info := serviceInfoFromService(service)
	if info == nil {
		return
	}

	key := service.Namespace + "/" + service.Name
	w.mu.Lock()
	if existing := w.services[key]; existing != nil {
		if !isResourceVersionNewer(existing.ResourceVersion, info.ResourceVersion) {
			w.mu.Unlock()
			return
		}
	}
	w.services[key] = info
	w.mu.Unlock()

	if w.onServiceUpsert != nil {
		w.onServiceUpsert(cloneServiceInfo(info))
	}
}

func (w *Watcher) handleServiceDelete(obj any) {
	service := getService(obj)
	if service == nil {
		return
	}
	info := serviceInfoFromService(service)
	if info == nil {
		return
	}

	key := service.Namespace + "/" + service.Name
	w.mu.Lock()
	delete(w.services, key)
	w.mu.Unlock()

	if w.onServiceDelete != nil {
		w.onServiceDelete(cloneServiceInfo(info))
	}
}

func (w *Watcher) handleEndpointSliceUpsert(obj any) {
	slice := getEndpointSlice(obj)
	if slice == nil {
		return
	}
	entry := endpointSliceEntryFromSlice(slice)
	if entry == nil || entry.ServiceName == "" {
		return
	}

	key := slice.Namespace + "/" + slice.Name
	w.mu.Lock()
	if existing := w.endpointSlices[key]; existing != nil {
		if !isResourceVersionNewer(existing.ResourceVersion, entry.ResourceVersion) {
			w.mu.Unlock()
			return
		}
	}
	w.endpointSlices[key] = entry
	w.mu.Unlock()

	w.rebuildEndpointsForService(entry.Namespace, entry.ServiceName)
}

func (w *Watcher) handleEndpointSliceDelete(obj any) {
	slice := getEndpointSlice(obj)
	if slice == nil {
		return
	}
	entry := endpointSliceEntryFromSlice(slice)
	if entry == nil || entry.ServiceName == "" {
		return
	}

	key := slice.Namespace + "/" + slice.Name
	w.mu.Lock()
	delete(w.endpointSlices, key)
	w.mu.Unlock()

	w.rebuildEndpointsForService(entry.Namespace, entry.ServiceName)
}

func (w *Watcher) rebuildEndpointsForService(namespace, serviceName string) {
	if namespace == "" || serviceName == "" {
		return
	}
	serviceKey := namespace + "/" + serviceName
	var addresses []string
	var ports []corev1.EndpointPort
	resourceVersion := ""

	w.mu.RLock()
	for _, entry := range w.endpointSlices {
		if entry.Namespace != namespace || entry.ServiceName != serviceName {
			continue
		}
		addresses = append(addresses, entry.Addresses...)
		ports = append(ports, entry.Ports...)
		if isResourceVersionNewer(resourceVersion, entry.ResourceVersion) {
			resourceVersion = entry.ResourceVersion
		}
	}
	w.mu.RUnlock()

	info := &EndpointsInfo{
		Namespace:       namespace,
		Name:            serviceName,
		ResourceVersion: resourceVersion,
		Addresses:       addresses,
		Ports:           ports,
	}

	w.mu.Lock()
	existing := w.endpoints[serviceKey]
	if len(addresses) == 0 && len(ports) == 0 {
		delete(w.endpoints, serviceKey)
		w.mu.Unlock()
		if existing != nil && w.onEndpointsDelete != nil {
			w.onEndpointsDelete(cloneEndpointsInfo(info))
		}
		return
	}
	if existing != nil && !isResourceVersionNewer(existing.ResourceVersion, info.ResourceVersion) {
		w.mu.Unlock()
		return
	}
	w.endpoints[serviceKey] = info
	w.mu.Unlock()

	if w.onEndpointsUpsert != nil {
		w.onEndpointsUpsert(cloneEndpointsInfo(info))
	}
}

func (w *Watcher) handleNodeUpsert(obj any) {
	node := getNode(obj)
	if node == nil {
		return
	}
	info := nodeInfoFromNode(node)
	if info == nil {
		return
	}

	w.mu.Lock()
	if existing := w.nodes[node.Name]; existing != nil {
		if !isResourceVersionNewer(existing.ResourceVersion, info.ResourceVersion) {
			w.mu.Unlock()
			return
		}
	}
	w.nodes[node.Name] = info
	w.mu.Unlock()

	if w.onNodeUpsert != nil {
		w.onNodeUpsert(cloneNodeInfo(info))
	}
}

func (w *Watcher) handleNodeDelete(obj any) {
	node := getNode(obj)
	if node == nil {
		return
	}
	info := nodeInfoFromNode(node)
	if info == nil {
		return
	}

	w.mu.Lock()
	delete(w.nodes, node.Name)
	w.mu.Unlock()

	if w.onNodeDelete != nil {
		w.onNodeDelete(cloneNodeInfo(info))
	}
}

func sandboxInfoFromPod(pod *corev1.Pod) *SandboxInfo {
	if pod == nil {
		return nil
	}
	sandboxID := pod.Labels[controller.LabelSandboxID]
	if sandboxID == "" {
		return nil
	}
	if pod.Labels[controller.LabelPoolType] != controller.PoolTypeActive {
		return nil
	}
	teamID := pod.Annotations[controller.AnnotationTeamID]
	return &SandboxInfo{
		Namespace:                 pod.Namespace,
		Name:                      pod.Name,
		UID:                       pod.UID,
		ResourceVersion:           pod.ResourceVersion,
		PodIP:                     pod.Status.PodIP,
		NodeName:                  pod.Spec.NodeName,
		SandboxID:                 sandboxID,
		TeamID:                    teamID,
		OwnerKind:                 pod.Annotations[controller.AnnotationOwnerKind],
		FunctionID:                pod.Annotations[controller.AnnotationFunctionID],
		FunctionRevisionID:        pod.Annotations[controller.AnnotationFunctionRevisionID],
		FunctionRuntimeInstanceID: pod.Annotations[controller.AnnotationFunctionRuntimeInstanceID],
		NetworkPolicy:             pod.Annotations[controller.AnnotationNetworkPolicy],
		NetworkPolicyHash:         pod.Annotations[controller.AnnotationNetworkPolicyHash],
		NetworkAppliedHash:        pod.Annotations[controller.AnnotationNetworkPolicyAppliedHash],
	}
}

func activeSandboxInfoFromPod(pod *corev1.Pod) *SandboxInfo {
	if pod == nil || pod.DeletionTimestamp != nil {
		return nil
	}
	if pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed {
		return nil
	}
	if pod.Status.PodIP == "" || pod.Spec.NodeName == "" {
		return nil
	}
	return sandboxInfoFromPod(pod)
}

func serviceInfoFromService(service *corev1.Service) *ServiceInfo {
	if service == nil {
		return nil
	}
	return &ServiceInfo{
		Namespace:       service.Namespace,
		Name:            service.Name,
		ResourceVersion: service.ResourceVersion,
		ClusterIP:       service.Spec.ClusterIP,
		Ports:           append([]corev1.ServicePort(nil), service.Spec.Ports...),
		Labels:          cloneStringMap(service.Labels),
	}
}

func endpointsInfosFromSlices(objects []any) []*EndpointsInfo {
	byService := make(map[string]*EndpointsInfo)
	for _, obj := range objects {
		entry := endpointSliceEntryFromSlice(getEndpointSlice(obj))
		if entry == nil || entry.ServiceName == "" {
			continue
		}
		key := entry.Namespace + "/" + entry.ServiceName
		info := byService[key]
		if info == nil {
			info = &EndpointsInfo{
				Namespace: entry.Namespace,
				Name:      entry.ServiceName,
			}
			byService[key] = info
		}
		if isResourceVersionNewer(info.ResourceVersion, entry.ResourceVersion) {
			info.ResourceVersion = entry.ResourceVersion
		}
		info.Addresses = append(info.Addresses, entry.Addresses...)
		info.Ports = append(info.Ports, entry.Ports...)
	}
	out := make([]*EndpointsInfo, 0, len(byService))
	for _, info := range byService {
		out = append(out, cloneEndpointsInfo(info))
	}
	return out
}

type endpointSliceEntry struct {
	Namespace       string
	Name            string
	ServiceName     string
	ResourceVersion string
	Addresses       []string
	Ports           []corev1.EndpointPort
}

func endpointSliceEntryFromSlice(slice *discoveryv1.EndpointSlice) *endpointSliceEntry {
	if slice == nil {
		return nil
	}
	serviceName := ""
	if slice.Labels != nil {
		serviceName = slice.Labels[discoveryv1.LabelServiceName]
	}
	entry := &endpointSliceEntry{
		Namespace:       slice.Namespace,
		Name:            slice.Name,
		ServiceName:     serviceName,
		ResourceVersion: slice.ResourceVersion,
		Addresses:       []string{},
		Ports:           []corev1.EndpointPort{},
	}
	for _, endpoint := range slice.Endpoints {
		for _, address := range endpoint.Addresses {
			if address != "" {
				entry.Addresses = append(entry.Addresses, address)
			}
		}
	}
	for _, port := range slice.Ports {
		entry.Ports = append(entry.Ports, endpointPortFromSlicePort(port))
	}
	return entry
}

func nodeInfoFromNode(node *corev1.Node) *NodeInfo {
	if node == nil {
		return nil
	}
	info := &NodeInfo{
		Name:            node.Name,
		ResourceVersion: node.ResourceVersion,
	}
	for _, addr := range node.Status.Addresses {
		if addr.Type == corev1.NodeInternalIP && addr.Address != "" {
			info.InternalIPs = append(info.InternalIPs, addr.Address)
		}
	}
	return info
}

func getPod(obj any) *corev1.Pod {
	pod, ok := obj.(*corev1.Pod)
	if ok {
		return pod
	}
	tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
	if !ok {
		return nil
	}
	pod, _ = tombstone.Obj.(*corev1.Pod)
	return pod
}

func getService(obj any) *corev1.Service {
	service, ok := obj.(*corev1.Service)
	if ok {
		return service
	}
	tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
	if !ok {
		return nil
	}
	service, _ = tombstone.Obj.(*corev1.Service)
	return service
}

func getEndpointSlice(obj any) *discoveryv1.EndpointSlice {
	slice, ok := obj.(*discoveryv1.EndpointSlice)
	if ok {
		return slice
	}
	tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
	if !ok {
		return nil
	}
	slice, _ = tombstone.Obj.(*discoveryv1.EndpointSlice)
	return slice
}

func getNode(obj any) *corev1.Node {
	node, ok := obj.(*corev1.Node)
	if ok {
		return node
	}
	tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
	if !ok {
		return nil
	}
	node, _ = tombstone.Obj.(*corev1.Node)
	return node
}

func indexPodByNode(obj any) ([]string, error) {
	pod := getPod(obj)
	if pod == nil || pod.Spec.NodeName == "" {
		return nil, nil
	}
	return []string{pod.Spec.NodeName}, nil
}

func cloneSandboxInfo(info *SandboxInfo) *SandboxInfo {
	if info == nil {
		return nil
	}
	clone := *info
	return &clone
}

func cloneServiceInfo(info *ServiceInfo) *ServiceInfo {
	if info == nil {
		return nil
	}
	clone := *info
	clone.Ports = append([]corev1.ServicePort(nil), info.Ports...)
	clone.Labels = cloneStringMap(info.Labels)
	return &clone
}

func cloneEndpointsInfo(info *EndpointsInfo) *EndpointsInfo {
	if info == nil {
		return nil
	}
	clone := *info
	clone.Addresses = append([]string(nil), info.Addresses...)
	clone.Ports = append([]corev1.EndpointPort(nil), info.Ports...)
	return &clone
}

func endpointPortFromSlicePort(port discoveryv1.EndpointPort) corev1.EndpointPort {
	out := corev1.EndpointPort{}
	if port.Name != nil {
		out.Name = *port.Name
	}
	if port.Port != nil {
		out.Port = *port.Port
	}
	if port.Protocol != nil {
		out.Protocol = *port.Protocol
	}
	out.AppProtocol = port.AppProtocol
	return out
}

func cloneNodeInfo(info *NodeInfo) *NodeInfo {
	if info == nil {
		return nil
	}
	clone := *info
	clone.InternalIPs = append([]string(nil), info.InternalIPs...)
	return &clone
}

func cloneStringMap(in map[string]string) map[string]string {
	if in == nil {
		return nil
	}
	out := make(map[string]string, len(in))
	for key, val := range in {
		out[key] = val
	}
	return out
}

func isResourceVersionNewer(current, incoming string) bool {
	if current == "" {
		return true
	}
	if incoming == "" {
		return false
	}
	currentVal, currentErr := strconv.ParseInt(current, 10, 64)
	incomingVal, incomingErr := strconv.ParseInt(incoming, 10, 64)
	if currentErr != nil || incomingErr != nil {
		return true
	}
	return incomingVal > currentVal
}
