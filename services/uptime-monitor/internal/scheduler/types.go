package scheduler

import (
	"context"

	"github.com/tailus/uptime-monitor/internal/models"
)

// MonitorSource loads the set of monitors that should be actively
// scheduled. Implemented by internal/storage/postgres against a Redis-cached
// read path in production.
type MonitorSource interface {
	ActiveMonitors(ctx context.Context) ([]models.Monitor, error)
}

// PeerSource reports which scheduler instances are currently alive, used to
// build the consistent-hash ring that partitions the monitor set across
// scheduler replicas. Implemented via a Postgres heartbeat table (see
// internal/storage/postgres.SchedulerHeartbeat) — each replica upserts its
// own row on an interval, and a row is considered live if seen within the
// last few heartbeat intervals.
type PeerSource interface {
	ActivePeers(ctx context.Context) ([]string, error)
}

// JobPublisher is the narrow interface the scheduler needs from
// internal/queue, kept here so scheduler doesn't import the queue package's
// NATS-specific types directly.
type JobPublisher interface {
	Publish(ctx context.Context, job models.CheckJob) error
}
