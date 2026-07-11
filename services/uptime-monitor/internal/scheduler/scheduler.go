// Package scheduler decides *when* each monitor's next check fires and
// publishes the corresponding job(s) to NATS JetStream. It does not execute
// checks itself — that's the prober's job — so a scheduler process stays
// cheap (a few hundred MB of RAM even at millions of monitors, since it only
// holds monitor metadata, not check results).
//
// # Scheduling algorithm
//
// Each scheduler replica owns a partition of the monitor set, assigned via
// consistent hashing over the set of currently-live scheduler replicas
// (see PeerSource). This means:
//   - Adding a scheduler replica to handle growth only reassigns ~1/N of
//     monitors to it; the rest keep their existing owner.
//   - A scheduler crash reassigns its monitors to the remaining replicas
//     within one reconcile cycle, with no single point of failure.
//
// For its owned monitors, the scheduler keeps a min-heap keyed by next-run
// time (see heap.go). A single dispatch loop pops everything due "now" and
// publishes one CheckJob per configured region. This is O(log n) per
// schedule/reschedule, so it comfortably handles millions of owned monitors
// per replica on a single core.
//
// # Avoiding thundering herds
//
// A monitor's very first next-run time is not "now" — it's
// `now + (hash(monitorID) mod intervalSeconds)`. That deterministically
// spreads 50,000 monitors on a 30s interval evenly across the full 30s
// window (≈1,667 checks/sec steady state) instead of bursting all of them
// in the same tick. Subsequent runs simply add the interval to the previous
// scheduled time (not to "now"), which keeps the phase stable and avoids
// drift; if a monitor falls behind by more than one interval (e.g. the
// scheduler was down), its schedule is re-anchored with a fresh jittered
// offset rather than firing a burst of catch-up checks.
package scheduler

import (
	"container/heap"
	"context"
	"hash/fnv"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/tailus/uptime-monitor/internal/hashring"
	"github.com/tailus/uptime-monitor/internal/models"
)

type Config struct {
	InstanceID        string
	DispatchTick      time.Duration // how often the dispatch loop checks for due items
	ReconcileInterval time.Duration // how often monitors/peers are refreshed
	RingReplicas      int
}

func DefaultConfig(instanceID string) Config {
	return Config{
		InstanceID:        instanceID,
		DispatchTick:      200 * time.Millisecond,
		ReconcileInterval: 15 * time.Second,
		RingReplicas:      128,
	}
}

type Scheduler struct {
	cfg       Config
	monitors  MonitorSource
	peers     PeerSource
	publisher JobPublisher

	mu       sync.Mutex
	ring     *hashring.Ring
	heap     dueHeap
	items    map[uuid.UUID]*dueItem
	interval map[uuid.UUID]time.Duration // last known interval, to detect changes on reconcile
	regions  map[uuid.UUID][]string

	enrichHook enrichFunc
}

func New(cfg Config, monitors MonitorSource, peers PeerSource, publisher JobPublisher) *Scheduler {
	return &Scheduler{
		cfg:       cfg,
		monitors:  monitors,
		peers:     peers,
		publisher: publisher,
		ring:      hashring.New(cfg.RingReplicas),
		heap:      dueHeap{},
		items:     make(map[uuid.UUID]*dueItem),
		interval:  make(map[uuid.UUID]time.Duration),
		regions:   make(map[uuid.UUID][]string),
	}
}

// Run blocks, driving both the reconcile loop and the dispatch loop, until
// ctx is cancelled.
func (s *Scheduler) Run(ctx context.Context) error {
	if err := s.reconcile(ctx); err != nil {
		slog.Error("initial reconcile failed", "error", err)
	}

	reconcileTicker := time.NewTicker(s.cfg.ReconcileInterval)
	dispatchTicker := time.NewTicker(s.cfg.DispatchTick)
	defer reconcileTicker.Stop()
	defer dispatchTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-reconcileTicker.C:
			if err := s.reconcile(ctx); err != nil {
				slog.Error("reconcile failed", "error", err)
			}
		case <-dispatchTicker.C:
			s.dispatchDue(ctx)
		}
	}
}

// dispatchDue pops every item due at or before now and publishes its jobs.
func (s *Scheduler) dispatchDue(ctx context.Context) {
	now := time.Now()

	s.mu.Lock()
	var due []*dueItem
	for s.heap.Len() > 0 && !s.heap[0].nextRun.After(now) {
		item := heap.Pop(&s.heap).(*dueItem)
		due = append(due, item)
	}
	s.mu.Unlock()

	// Publish concurrently: each dispatchOne call does a network round trip
	// (job enrichment lookup + NATS publish) that would otherwise serialize
	// the whole tick behind Redis/NATS latency. dispatchOne only touches
	// shared state (the heap, interval/regions maps) under s.mu, so this is
	// safe to fan out.
	for _, item := range due {
		item := item
		go s.dispatchOne(ctx, item, now)
	}
}

func (s *Scheduler) dispatchOne(ctx context.Context, item *dueItem, now time.Time) {
	s.mu.Lock()
	interval := s.interval[item.monitorID]
	regions := s.regions[item.monitorID]
	s.mu.Unlock()

	if interval <= 0 {
		return // monitor was removed between pop and dispatch
	}

	for _, region := range regions {
		job := models.CheckJob{
			MonitorID:   item.monitorID,
			Region:      region,
			ScheduledAt: item.nextRun,
		}
		// Job fields beyond MonitorID/Region/ScheduledAt (URL, headers,
		// timeouts, ...) are filled in by the caller via a decorator before
		// Publish — see cmd/scheduler for how the full Monitor record is
		// looked up and merged in. Kept separate here so the hot dispatch
		// path never touches monitor bodies/headers, only the heap.
		if err := s.publisher.Publish(ctx, s.enrich(job)); err != nil {
			slog.Error("failed to publish check job", "monitor_id", item.monitorID, "region", region, "error", err)
		}
	}

	// Re-anchor: if the monitor fell more than one interval behind (e.g. the
	// scheduler process was down), don't fire a burst of catch-up checks —
	// just resume on a freshly jittered phase.
	next := item.nextRun.Add(interval)
	if now.Sub(next) > interval {
		next = now.Add(jitterOffset(item.monitorID, interval))
	}
	item.nextRun = next

	s.mu.Lock()
	heap.Push(&s.heap, item)
	s.mu.Unlock()
}

// enrich attaches the fields owned by MonitorEnricher, if one was set via
// WithEnricher. In the default configuration the scheduler embeds only
// MonitorID/Region into the job and relies on the prober to fetch full
// monitor config via its own cache; call sites that want the scheduler to
// push full config inline should wrap Scheduler with an enrichHook.
func (s *Scheduler) enrich(job models.CheckJob) models.CheckJob {
	if s.enrichHook != nil {
		return s.enrichHook(job)
	}
	return job
}

// WithEnricher lets the caller (cmd/scheduler) inject monitor config (URL,
// headers, timeouts, expected status range) into each job before publish,
// sourced from a Redis-backed monitor cache so this stays a cheap in-memory
// lookup rather than a Postgres round trip per dispatch.
func (s *Scheduler) WithEnricher(fn func(models.CheckJob) models.CheckJob) {
	s.enrichHook = fn
}

// reconcile refreshes ring membership and the owned monitor set. Cheap
// enough to run every few seconds even at millions of monitors because it
// only touches metadata, not check history.
func (s *Scheduler) reconcile(ctx context.Context) error {
	peers, err := s.peers.ActivePeers(ctx)
	if err != nil {
		return err
	}
	newRing := hashring.New(s.cfg.RingReplicas)
	for _, p := range peers {
		newRing.Add(p)
	}
	if len(newRing.Members()) == 0 {
		newRing.Add(s.cfg.InstanceID) // always at least own ourselves
	}

	monitors, err := s.monitors.ActiveMonitors(ctx)
	if err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.ring = newRing

	seen := make(map[uuid.UUID]bool, len(monitors))
	for _, m := range monitors {
		owner, err := s.ring.Get(m.ID.String())
		if err != nil || owner != s.cfg.InstanceID {
			// Not ours: make sure it's not lingering in our heap from a
			// previous ownership assignment.
			s.removeLocked(m.ID)
			continue
		}
		seen[m.ID] = true

		existingInterval, tracked := s.interval[m.ID]
		newInterval := time.Duration(m.IntervalSeconds) * time.Second

		switch {
		case !tracked:
			s.scheduleLocked(m.ID, newInterval, m.Regions)
		case existingInterval != newInterval:
			s.removeLocked(m.ID)
			s.scheduleLocked(m.ID, newInterval, m.Regions)
		default:
			s.regions[m.ID] = m.Regions // regions can change without touching the schedule
		}
	}

	// Drop anything we own in-memory that's no longer in the active set
	// (deleted or disabled).
	for id := range s.interval {
		if !seen[id] {
			s.removeLocked(id)
		}
	}
	return nil
}

// scheduleLocked inserts a monitor into the heap with a deterministically
// jittered first run. Caller must hold s.mu.
func (s *Scheduler) scheduleLocked(id uuid.UUID, interval time.Duration, regions []string) {
	item := &dueItem{
		monitorID: id,
		nextRun:   time.Now().Add(jitterOffset(id, interval)),
	}
	heap.Push(&s.heap, item)
	s.items[id] = item
	s.interval[id] = interval
	s.regions[id] = regions
}

// removeLocked evicts a monitor from the heap in O(log n). Caller must hold
// s.mu.
func (s *Scheduler) removeLocked(id uuid.UUID) {
	item, ok := s.items[id]
	if !ok {
		delete(s.interval, id)
		delete(s.regions, id)
		return
	}
	if item.index >= 0 && item.index < len(s.heap) {
		heap.Remove(&s.heap, item.index)
	}
	delete(s.items, id)
	delete(s.interval, id)
	delete(s.regions, id)
}

// jitterOffset deterministically maps a monitor ID into [0, interval), so
// restarts and multi-replica reconciles always compute the same phase for a
// given monitor instead of re-randomizing (which would cause visible
// clustering as monitors get reshuffled across ticks).
func jitterOffset(id uuid.UUID, interval time.Duration) time.Duration {
	h := fnv.New64a()
	_, _ = h.Write(id[:])
	if interval <= 0 {
		return 0
	}
	return time.Duration(h.Sum64() % uint64(interval))
}

// OwnedCount reports how many monitors this replica currently owns —
// exported as a gauge for capacity-planning dashboards.
func (s *Scheduler) OwnedCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.items)
}

type enrichFunc = func(models.CheckJob) models.CheckJob
