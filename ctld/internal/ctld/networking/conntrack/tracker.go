package conntrack

import (
	"net/netip"
	"sync"
	"time"
)

const (
	defaultTTL      = 5 * time.Minute
	defaultMaxPerIP = 2048
	defaultMaxTotal = 65536
)

type Tracker struct {
	mu        sync.Mutex
	bySrc     map[string]map[FlowKey]time.Time
	ttl       time.Duration
	maxPerIP  int
	maxTotal  int
	totalSize int
}

func NewTracker() *Tracker {
	return &Tracker{
		bySrc:    make(map[string]map[FlowKey]time.Time),
		ttl:      defaultTTL,
		maxPerIP: defaultMaxPerIP,
		maxTotal: defaultMaxTotal,
	}
}

func (t *Tracker) Record(flow FlowKey) {
	if !flow.SrcIP.IsValid() || !flow.DstIP.IsValid() {
		return
	}
	src := flow.SrcIP.String()
	t.mu.Lock()
	defer t.mu.Unlock()
	t.cleanupLocked()
	if t.totalSize >= t.maxTotal {
		t.evictLocked()
	}
	perSrc := t.bySrc[src]
	if perSrc == nil {
		perSrc = make(map[FlowKey]time.Time)
		t.bySrc[src] = perSrc
	}
	if len(perSrc) >= t.maxPerIP {
		t.evictPerSrcLocked(perSrc)
	}
	if _, exists := perSrc[flow]; !exists {
		t.totalSize++
	}
	perSrc[flow] = time.Now()
}

func (t *Tracker) PopBySrc(srcIP string) []FlowKey {
	srcAddr, err := netip.ParseAddr(srcIP)
	if err != nil || !srcAddr.IsValid() {
		return nil
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	perSrc := t.bySrc[srcIP]
	if len(perSrc) == 0 {
		return nil
	}
	out := make([]FlowKey, 0, len(perSrc))
	for flow := range perSrc {
		out = append(out, flow)
	}
	t.totalSize -= len(perSrc)
	delete(t.bySrc, srcIP)
	return out
}

func (t *Tracker) cleanupLocked() {
	cutoff := time.Now().Add(-t.ttl)
	for src, flows := range t.bySrc {
		for flow, ts := range flows {
			if ts.Before(cutoff) {
				delete(flows, flow)
				t.totalSize--
			}
		}
		if len(flows) == 0 {
			delete(t.bySrc, src)
		}
	}
}

func (t *Tracker) evictLocked() {
	for src, flows := range t.bySrc {
		t.totalSize -= len(flows)
		delete(t.bySrc, src)
		if t.totalSize <= t.maxTotal/2 {
			return
		}
	}
}

func (t *Tracker) evictPerSrcLocked(flows map[FlowKey]time.Time) {
	if len(flows) == 0 {
		return
	}
	var oldestFlow FlowKey
	var oldestTime time.Time
	first := true
	for flow, ts := range flows {
		if first || ts.Before(oldestTime) {
			oldestFlow = flow
			oldestTime = ts
			first = false
		}
	}
	if !first {
		delete(flows, oldestFlow)
		t.totalSize--
	}
}
