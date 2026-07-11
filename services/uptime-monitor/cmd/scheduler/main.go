// Command scheduler owns the "when does each monitor run next" decision and
// publishes CheckJob messages to NATS JetStream. See internal/scheduler for
// the algorithm. Run one or more replicas — they self-partition the monitor
// set via a Postgres heartbeat table + consistent hashing.
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/tailus/uptime-monitor/internal/cache/redis"
	"github.com/tailus/uptime-monitor/internal/config"
	"github.com/tailus/uptime-monitor/internal/metrics"
	"github.com/tailus/uptime-monitor/internal/models"
	"github.com/tailus/uptime-monitor/internal/queue"
	"github.com/tailus/uptime-monitor/internal/scheduler"
	"github.com/tailus/uptime-monitor/internal/storage/postgres"
	"github.com/tailus/uptime-monitor/internal/tracing"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	cfg, err := config.Load("scheduler")
	if err != nil {
		slog.Error("failed to load config", "error", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	shutdownTracing, err := tracing.Setup(ctx, "uptime-scheduler", cfg.OTLPEndpoint)
	if err != nil {
		slog.Error("failed to set up tracing", "error", err)
		os.Exit(1)
	}
	defer shutdownTracing(context.Background())

	store, err := postgres.New(ctx, cfg.PostgresDSN, cfg.PostgresMaxConns, cfg.PostgresMinConns)
	if err != nil {
		slog.Error("failed to connect to postgres", "error", err)
		os.Exit(1)
	}
	defer store.Close()

	rdb := redis.NewClient(cfg.RedisAddr, cfg.RedisPassword, cfg.RedisDB)
	defer rdb.Close()
	monitorCache := redis.NewMonitorCache(rdb)

	nc, err := queue.Connect(cfg.NATSURL, cfg.NATSCredsFile)
	if err != nil {
		slog.Error("failed to connect to nats", "error", err)
		os.Exit(1)
	}
	defer nc.Close()

	js, err := jetstream.New(nc)
	if err != nil {
		slog.Error("failed to init jetstream", "error", err)
		os.Exit(1)
	}
	if err := queue.EnsureStreams(ctx, js, cfg.JobStreamName, cfg.ResultStreamName); err != nil {
		slog.Error("failed to ensure streams", "error", err)
		os.Exit(1)
	}
	publisher := queue.NewJobPublisher(js)

	sched := scheduler.New(scheduler.DefaultConfig(cfg.InstanceID), store, store, publisher)
	sched.WithEnricher(enricherFor(monitorCache, store))

	go monitorCacheRefreshLoop(ctx, store, monitorCache)
	go heartbeatLoop(ctx, store, cfg.InstanceID)
	go ownedGaugeLoop(ctx, sched)

	go func() {
		if err := metrics.Serve(cfg.MetricsAddr); err != nil {
			slog.Error("metrics server exited", "error", err)
		}
	}()

	slog.Info("scheduler starting", "instance_id", cfg.InstanceID)
	if err := sched.Run(ctx); err != nil && ctx.Err() == nil {
		slog.Error("scheduler exited with error", "error", err)
		os.Exit(1)
	}
	slog.Info("scheduler shut down cleanly")
}

// enricherFor fills in the parts of a CheckJob the scheduler's hot path
// doesn't otherwise touch (URL, headers, timeouts, expected status range),
// reading through the Redis monitor cache with a Postgres fallback so a
// cold cache entry doesn't fail the dispatch.
func enricherFor(cache *redis.MonitorCache, store *postgres.Store) func(models.CheckJob) models.CheckJob {
	return func(job models.CheckJob) models.CheckJob {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		m, ok, err := cache.Get(ctx, job.MonitorID.String())
		if err != nil || !ok {
			m, err = store.GetMonitor(ctx, job.MonitorID)
			if err != nil {
				slog.Error("enrich: monitor lookup failed", "monitor_id", job.MonitorID, "error", err)
				return job
			}
			_ = cache.Set(ctx, m)
		}

		job.URL = m.URL
		job.CheckType = m.CheckType
		job.Method = m.Method
		job.Headers = m.Headers
		job.Body = m.Body
		job.ExpectedStatusMin = m.ExpectedStatusMin
		job.ExpectedStatusMax = m.ExpectedStatusMax
		job.BaseTimeoutMS = m.BaseTimeoutMS
		job.MinTimeoutMS = m.MinTimeoutMS
		job.MaxTimeoutMS = m.MaxTimeoutMS
		return job
	}
}

// monitorCacheRefreshLoop keeps Redis's monitor cache warm so the dispatch
// path's enrichment lookups almost always hit. A full refresh (rather than
// relying solely on cache TTL) also picks up edits made directly in
// Postgres without going through the API's write-through path.
func monitorCacheRefreshLoop(ctx context.Context, store *postgres.Store, cache *redis.MonitorCache) {
	ticker := time.NewTicker(60 * time.Second)
	defer ticker.Stop()
	refresh := func() {
		monitors, err := store.ActiveMonitors(ctx)
		if err != nil {
			slog.Error("monitor cache refresh: failed to load monitors", "error", err)
			return
		}
		if err := cache.SetMany(ctx, monitors); err != nil {
			slog.Error("monitor cache refresh: failed to populate cache", "error", err)
		}
	}
	refresh()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			refresh()
		}
	}
}

func heartbeatLoop(ctx context.Context, store *postgres.Store, instanceID string) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		if err := store.Heartbeat(ctx, instanceID); err != nil {
			slog.Error("heartbeat failed", "error", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func ownedGaugeLoop(ctx context.Context, sched *scheduler.Scheduler) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		metrics.SchedulerOwnedMonitors.Set(float64(sched.OwnedCount()))
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
