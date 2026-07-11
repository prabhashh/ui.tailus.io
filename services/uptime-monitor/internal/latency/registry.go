package latency

import (
	"sync"

	"github.com/google/uuid"
)

// Registry owns one Tracker per monitor for a single probe process. Kept
// in-process (not Redis-backed) deliberately: a monitor is consistently
// routed to the same region's consumer group via NATS work-queue
// distribution, so co-locating the adaptive state with the process that
// actually observes the latency avoids a round trip on the hot path. The
// registry is flushed periodically (see resultprocessor) so the estimate
// survives probe restarts and is visible for observability.
type Registry struct {
	mu       sync.RWMutex
	trackers map[uuid.UUID]*Tracker
}

func NewRegistry() *Registry {
	return &Registry{trackers: make(map[uuid.UUID]*Tracker)}
}

func (r *Registry) Get(monitorID uuid.UUID) *Tracker {
	r.mu.RLock()
	t, ok := r.trackers[monitorID]
	r.mu.RUnlock()
	if ok {
		return t
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if t, ok := r.trackers[monitorID]; ok {
		return t
	}
	t = NewTracker()
	r.trackers[monitorID] = t
	return t
}

// Seed primes a tracker from a previously persisted snapshot (e.g. on
// process start, loaded from Postgres) so timeouts stay sensible instead of
// reverting to cold-start defaults after a deploy.
func (r *Registry) Seed(monitorID uuid.UUID, snap Snapshot) {
	if snap.Samples == 0 {
		return
	}
	t := r.Get(monitorID)
	t.mu.Lock()
	defer t.mu.Unlock()
	t.mean = snap.Mean
	t.varEwma = snap.StdDev * snap.StdDev
	t.samples = snap.Samples
	// Prime the P² markers with the persisted P95 so early estimates are
	// centered correctly; it will keep converging from real observations.
	for i := 0; i < 5; i++ {
		t.p95.q[i] = snap.P95
	}
	t.p95.init = 5
	t.p95.n = [5]int{1, 2, 3, 4, 5}
	t.p95.np = [5]float64{1, 1 + 2*t.p95.p, 1 + 4*t.p95.p, 3 + 2*t.p95.p, 5}
}

// Snapshots returns a copy of all current monitor->snapshot summaries,
// for periodic flush to durable storage.
func (r *Registry) Snapshots() map[uuid.UUID]Snapshot {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make(map[uuid.UUID]Snapshot, len(r.trackers))
	for id, t := range r.trackers {
		out[id] = t.Snapshot()
	}
	return out
}
