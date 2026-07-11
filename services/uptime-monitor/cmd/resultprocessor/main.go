// Command resultprocessor consumes CheckResult messages from every region,
// batches them into Postgres, and runs incident detection + notification
// dispatch. Horizontally scalable — see internal/resultprocessor's package
// doc for why multiple replicas can safely share the load.
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	uredis "github.com/tailus/uptime-monitor/internal/cache/redis"
	"github.com/tailus/uptime-monitor/internal/config"
	"github.com/tailus/uptime-monitor/internal/metrics"
	"github.com/tailus/uptime-monitor/internal/notify"
	"github.com/tailus/uptime-monitor/internal/notify/channels"
	"github.com/tailus/uptime-monitor/internal/queue"
	"github.com/tailus/uptime-monitor/internal/resultprocessor"
	"github.com/tailus/uptime-monitor/internal/storage/postgres"
	"github.com/tailus/uptime-monitor/internal/tracing"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	cfg, err := config.Load("resultprocessor")
	if err != nil {
		slog.Error("failed to load config", "error", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	shutdownTracing, err := tracing.Setup(ctx, "uptime-resultprocessor", cfg.OTLPEndpoint)
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

	rdb := uredis.NewClient(cfg.RedisAddr, cfg.RedisPassword, cfg.RedisDB)
	defer rdb.Close()
	monitorCache := uredis.NewMonitorCache(rdb)
	failureTracker := uredis.NewFailureCounter(rdb)
	dedup := uredis.NewNotificationDedup(rdb)

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
	consumer, err := queue.EnsureConsumer(ctx, js, cfg.ResultStreamName,
		"resultprocessor", queue.ResultSubjectPrefix+".>", 30*time.Second, 5)
	if err != nil {
		slog.Error("failed to create result consumer", "error", err)
		os.Exit(1)
	}

	dispatcher := notify.NewDispatcher(dedup, channels.NewWebhook(), newEmailChannel())
	lookup := resultprocessor.NewCachedMonitorLookup(monitorCache, store)
	processor := resultprocessor.New(resultprocessor.DefaultConfig(), store, store, lookup, failureTracker, dispatcher)

	go func() {
		if err := metrics.Serve(cfg.MetricsAddr); err != nil {
			slog.Error("metrics server exited", "error", err)
		}
	}()

	slog.Info("result processor starting", "instance_id", cfg.InstanceID)
	if err := processor.Run(ctx, consumer); err != nil && ctx.Err() == nil {
		slog.Error("result processor exited with error", "error", err)
		os.Exit(1)
	}
	slog.Info("result processor shut down cleanly")
}

func newEmailChannel() *channels.Email {
	port := 587
	if v := os.Getenv("UPTIME_SMTP_PORT"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			port = n
		}
	}
	return channels.NewEmail(channels.EmailConfig{
		Host:     os.Getenv("UPTIME_SMTP_HOST"),
		Port:     port,
		Username: os.Getenv("UPTIME_SMTP_USERNAME"),
		Password: os.Getenv("UPTIME_SMTP_PASSWORD"),
		From:     os.Getenv("UPTIME_SMTP_FROM"),
	})
}
