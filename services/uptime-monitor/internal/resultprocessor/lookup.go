package resultprocessor

import (
	"context"

	"github.com/google/uuid"

	"github.com/tailus/uptime-monitor/internal/models"
)

// MonitorCache is the narrow read side of internal/cache/redis.MonitorCache.
type MonitorCache interface {
	Get(ctx context.Context, id string) (models.Monitor, bool, error)
	Set(ctx context.Context, m models.Monitor) error
}

// MonitorStore is the narrow read side of internal/storage/postgres.Store
// needed as a cache-miss fallback.
type MonitorStore interface {
	GetMonitor(ctx context.Context, id uuid.UUID) (models.Monitor, error)
}

// CachedMonitorLookup resolves a monitor's config (specifically its
// FailureThreshold, for incident detection) through Redis first, falling
// back to Postgres on a miss and populating the cache — this keeps the
// result processor's hot path off Postgres for the overwhelming majority of
// lookups while staying correct.
type CachedMonitorLookup struct {
	cache MonitorCache
	store MonitorStore
}

func NewCachedMonitorLookup(cache MonitorCache, store MonitorStore) *CachedMonitorLookup {
	return &CachedMonitorLookup{cache: cache, store: store}
}

func (l *CachedMonitorLookup) Get(ctx context.Context, id uuid.UUID) (models.Monitor, error) {
	if m, ok, err := l.cache.Get(ctx, id.String()); err == nil && ok {
		return m, nil
	}
	m, err := l.store.GetMonitor(ctx, id)
	if err != nil {
		return m, err
	}
	_ = l.cache.Set(ctx, m)
	return m, nil
}
