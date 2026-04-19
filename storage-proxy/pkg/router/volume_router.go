package router

import "sync"

type Route struct {
	VolumeID      string
	PrimaryNodeID string
	PrimaryAddr   string
	Epoch         uint64
	LocalPrimary  bool
}

type VolumeRouter struct {
	mu     sync.RWMutex
	routes map[string]Route
}

func NewVolumeRouter() *VolumeRouter {
	return &VolumeRouter{
		routes: make(map[string]Route),
	}
}

func (r *VolumeRouter) SetRoute(route Route) bool {
	if r == nil || route.VolumeID == "" {
		return false
	}
	if route.PrimaryAddr == "" {
		route.LocalPrimary = true
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	current, ok := r.routes[route.VolumeID]
	if ok && current.Epoch > route.Epoch {
		return false
	}
	r.routes[route.VolumeID] = route
	return true
}

func (r *VolumeRouter) DeleteRoute(volumeID string) {
	if r == nil || volumeID == "" {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.routes, volumeID)
}

func (r *VolumeRouter) GetRoute(volumeID string) (Route, bool) {
	if r == nil || volumeID == "" {
		return Route{}, false
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	route, ok := r.routes[volumeID]
	return route, ok
}

func (r *VolumeRouter) Resolve(volumeID string) Route {
	if route, ok := r.GetRoute(volumeID); ok {
		return route
	}
	return Route{
		VolumeID:     volumeID,
		LocalPrimary: true,
	}
}

func (r *VolumeRouter) IsLocalPrimary(volumeID string) bool {
	return r.Resolve(volumeID).LocalPrimary
}
