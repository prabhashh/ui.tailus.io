// Command prober is the regional worker: it pulls CheckJob messages for its
// region off NATS JetStream, executes them under bounded concurrency, and
// publishes CheckResult messages back. Run a small fleet of these per
// region — they are entirely stateless from a durability standpoint (all
// state that must survive a restart lives in Postgres/Redis), so scaling
// out is just starting more processes pointed at the same NATS cluster.
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/tailus/uptime-monitor/internal/circuitbreaker"
	"github.com/tailus/uptime-monitor/internal/config"
	"github.com/tailus/uptime-monitor/internal/latency"
	"github.com/tailus/uptime-monitor/internal/metrics"
	"github.com/tailus/uptime-monitor/internal/models"
	"github.com/tailus/uptime-monitor/internal/prober"
	"github.com/tailus/uptime-monitor/internal/queue"
	"github.com/tailus/uptime-monitor/internal/retry"
	"github.com/tailus/uptime-monitor/internal/storage/postgres"
	"github.com/tailus/uptime-monitor/internal/tracing"
	"github.com/tailus/uptime-monitor/internal/workerpool"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	cfg, err := config.Load("prober")
	if err != nil {
		slog.Error("failed to load config", "error", err)
		os.Exit(1)
	}
	if cfg.Region == "default" {
		slog.Warn("UPTIME_REGION not set, using 'default' — set it explicitly in production")
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	shutdownTracing, err := tracing.Setup(ctx, "uptime-prober-"+cfg.Region, cfg.OTLPEndpoint)
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

	consumer, err := queue.EnsureConsumer(ctx, js, cfg.JobStreamName,
		"prober-"+cfg.Region, queue.CheckSubject(cfg.Region),
		cfg.MaxTimeout+10*time.Second, // AckWait must exceed the worst-case check duration
		3,
	)
	if err != nil {
		slog.Error("failed to create consumer", "error", err)
		os.Exit(1)
	}

	latencies := latency.NewRegistry()
	breakers := circuitbreaker.NewRegistry(circuitbreaker.DefaultConfig())
	seedLatencyRegistry(ctx, store, cfg.Region, latencies)

	transportCfg := prober.TransportConfig{
		MaxIdleConns:        cfg.MaxIdleConns,
		MaxIdleConnsPerHost: cfg.MaxIdleConnsPerHost,
		IdleConnTimeout:     cfg.IdleConnTimeout,
		DNSCacheTTL:         cfg.DNSCacheTTL,
		DialTimeout:         5 * time.Second,
		TLSHandshakeTimeout: 5 * time.Second,
	}
	httpProber := prober.NewHTTPProber(transportCfg, latencies, breakers, retry.DefaultPolicy(), cfg.DefaultTimeout, cfg.MinTimeout, cfg.MaxTimeout)
	go httpProber.MaintainDNSCache(ctx, cfg.DNSCacheTTL)

	router := workerpool.NewRouter(cfg.WorkerPoolSize, cfg.QuarantinePoolSize)
	resultPublisher := queue.NewResultPublisher(js)

	go poolGaugeLoop(ctx, router)
	go latencyFlushLoop(ctx, store, cfg.Region, latencies)
	go func() {
		if err := metrics.Serve(cfg.MetricsAddr); err != nil {
			slog.Error("metrics server exited", "error", err)
		}
	}()

	slog.Info("prober starting", "region", cfg.Region, "instance_id", cfg.InstanceID)

	err = queue.PullLoop(ctx, consumer, 100, 2*time.Second, func(ctx context.Context, msg jetstream.Msg, job models.CheckJob) {
		quarantined := breakers.Get(job.MonitorID).State() == circuitbreaker.Open
		lane := router.Lane(quarantined)

		submitted := lane.TrySubmit(func() {
			result := httpProber.Check(ctx, job)
			if pubErr := resultPublisher.Publish(ctx, result); pubErr != nil {
				metrics.JetStreamPublishErrorsTotal.WithLabelValues("results").Inc()
				slog.Error("failed to publish result", "monitor_id", job.MonitorID, "error", pubErr)
			}
			if ackErr := msg.Ack(); ackErr != nil {
				slog.Error("failed to ack job message", "monitor_id", job.MonitorID, "error", ackErr)
			}
		})
		if !submitted {
			// Pool saturated: Nak so JetStream redelivers shortly instead of
			// silently dropping a scheduled check under load.
			_ = msg.Nak()
		}
	})
	if err != nil && ctx.Err() == nil {
		slog.Error("prober exited with error", "error", err)
		os.Exit(1)
	}

	router.Wait()
	slog.Info("prober shut down cleanly")
}

func seedLatencyRegistry(ctx context.Context, store *postgres.Store, region string, registry *latency.Registry) {
	stats, err := store.LatencyStatsByRegion(ctx, region)
	if err != nil {
		slog.Warn("failed to seed latency registry from postgres", "error", err)
		return
	}
	for _, s := range stats {
		registry.Seed(s.MonitorID, latency.Snapshot{Mean: s.Mean, StdDev: s.StdDev, P95: s.P95, Samples: s.Samples})
	}
	slog.Info("seeded latency registry", "region", region, "monitors", len(stats))
}

func latencyFlushLoop(ctx context.Context, store *postgres.Store, region string, registry *latency.Registry) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		snaps := registry.Snapshots()
		if len(snaps) == 0 {
			continue
		}
		batch := make([]models.LatencyStats, 0, len(snaps))
		for id, s := range snaps {
			batch = append(batch, models.LatencyStats{MonitorID: id, Region: region, Mean: s.Mean, StdDev: s.StdDev, P95: s.P95, Samples: s.Samples})
		}
		if err := store.UpsertLatencyStats(ctx, batch); err != nil {
			slog.Error("failed to flush latency stats", "error", err)
		}
	}
}

func poolGaugeLoop(ctx context.Context, router *workerpool.Router) {
	metrics.WorkerPoolCapacity.WithLabelValues("main").Set(float64(router.Main.Capacity()))
	metrics.WorkerPoolCapacity.WithLabelValues("quarantine").Set(float64(router.Quarantine.Capacity()))

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		metrics.WorkerPoolInFlight.WithLabelValues("main").Set(float64(router.Main.InFlight()))
		metrics.WorkerPoolInFlight.WithLabelValues("quarantine").Set(float64(router.Quarantine.InFlight()))
		metrics.WorkerPoolDroppedTotal.WithLabelValues("main").Add(0) // ensure series exists even at zero
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
