package router

import "testing"

func TestVolumeRouterSetAndGet(t *testing.T) {
	r := NewVolumeRouter()
	if !r.SetRoute(Route{
		VolumeID:      "vol-1",
		PrimaryNodeID: "node-a",
		PrimaryAddr:   "10.0.0.1:8080",
		Epoch:         3,
		LocalPrimary:  true,
	}) {
		t.Fatal("SetRoute() = false, want true")
	}

	route, ok := r.GetRoute("vol-1")
	if !ok {
		t.Fatal("GetRoute() returned ok=false")
	}
	if route.PrimaryNodeID != "node-a" || route.PrimaryAddr != "10.0.0.1:8080" || route.Epoch != 3 {
		t.Fatalf("route = %+v", route)
	}
	if !r.IsLocalPrimary("vol-1") {
		t.Fatal("IsLocalPrimary() = false, want true")
	}
}

func TestVolumeRouterDelete(t *testing.T) {
	r := NewVolumeRouter()
	r.SetRoute(Route{VolumeID: "vol-1"})
	r.DeleteRoute("vol-1")
	if _, ok := r.GetRoute("vol-1"); ok {
		t.Fatal("GetRoute() after delete returned ok=true")
	}
}

func TestVolumeRouterRejectsStaleEpoch(t *testing.T) {
	r := NewVolumeRouter()
	r.SetRoute(Route{
		VolumeID:      "vol-1",
		PrimaryNodeID: "node-b",
		PrimaryAddr:   "10.0.0.2:8080",
		Epoch:         5,
		LocalPrimary:  false,
	})
	if r.SetRoute(Route{
		VolumeID:      "vol-1",
		PrimaryNodeID: "node-a",
		PrimaryAddr:   "10.0.0.1:8080",
		Epoch:         4,
		LocalPrimary:  true,
	}) {
		t.Fatal("SetRoute() accepted stale epoch")
	}

	route, ok := r.GetRoute("vol-1")
	if !ok {
		t.Fatal("GetRoute() returned ok=false")
	}
	if route.PrimaryNodeID != "node-b" || route.Epoch != 5 || route.LocalPrimary {
		t.Fatalf("route = %+v", route)
	}
}

func TestVolumeRouterResolveDefaultsToLocalPrimary(t *testing.T) {
	r := NewVolumeRouter()
	route := r.Resolve("vol-2")
	if !route.LocalPrimary {
		t.Fatalf("Resolve() = %+v, want local primary", route)
	}
}
