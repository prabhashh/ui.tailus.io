// Package resultprocessor consumes check results from the RESULTS JetStream
// stream, batches them into Postgres, and drives incident detection +
// notification dispatch. It is horizontally scalable: any number of
// replicas can share the same durable consumer, since incident-detection
// state lives in Redis (not in-process) and Postgres enforces the
// "one open incident per monitor/region" invariant at the database level.
package resultprocessor

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/tailus/uptime-monitor/internal/metrics"
	"github.com/tailus/uptime-monitor/internal/models"
	"github.com/tailus/uptime-monitor/internal/notify"
	"github.com/tailus/uptime-monitor/internal/queue"
)

type ResultStore interface {
	InsertCheckResultsBatch(ctx context.Context, results []models.CheckResult) error
}

type IncidentStore interface {
	OpenIncident(ctx context.Context, monitorID uuid.UUID, region, cause string) (models.Incident, bool, error)
	ResolveIncident(ctx context.Context, monitorID uuid.UUID, region string) (models.Incident, bool, error)
	MonitorOwner(ctx context.Context, monitorID uuid.UUID) (uuid.UUID, error)
	NotificationChannelsByOwner(ctx context.Context, ownerID uuid.UUID) ([]models.NotificationChannel, error)
}

type MonitorLookup interface {
	Get(ctx context.Context, id uuid.UUID) (models.Monitor, error)
}

type FailureTracker interface {
	Incr(ctx context.Context, monitorID, region string) (int64, error)
	Reset(ctx context.Context, monitorID, region string) error
}

type Notifier interface {
	Dispatch(ctx context.Context, event notify.Event, targets []models.NotificationChannel)
}

type Config struct {
	BatchSize     int
	FlushInterval time.Duration
	FetchBatch    int
	FetchWait     time.Duration
}

func DefaultConfig() Config {
	return Config{
		BatchSize:     500,
		FlushInterval: 500 * time.Millisecond,
		FetchBatch:    200,
		FetchWait:     2 * time.Second,
	}
}

type Processor struct {
	cfg       Config
	results   ResultStore
	incidents IncidentStore
	monitors  MonitorLookup
	failures  FailureTracker
	notifier  Notifier

	mu      sync.Mutex
	pending []models.CheckResult
	acks    []jetstream.Msg
}

func New(cfg Config, results ResultStore, incidents IncidentStore, monitors MonitorLookup, failures FailureTracker, notifier Notifier) *Processor {
	return &Processor{
		cfg:       cfg,
		results:   results,
		incidents: incidents,
		monitors:  monitors,
		failures:  failures,
		notifier:  notifier,
	}
}

// Run consumes from consumer until ctx is cancelled, batching writes and
// running incident detection inline per message (cheap: one Redis round
// trip on the common path, Postgres only on an actual state transition).
func (p *Processor) Run(ctx context.Context, consumer jetstream.Consumer) error {
	flushTicker := time.NewTicker(p.cfg.FlushInterval)
	defer flushTicker.Stop()

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-flushTicker.C:
				p.flush(ctx)
			}
		}
	}()

	return queue.PullLoop(ctx, consumer, p.cfg.FetchBatch, p.cfg.FetchWait, func(ctx context.Context, msg jetstream.Msg, result models.CheckResult) {
		p.handleResult(ctx, msg, result)
	})
}

func (p *Processor) handleResult(ctx context.Context, msg jetstream.Msg, result models.CheckResult) {
	outcome := "success"
	if !result.Success {
		outcome = "failure"
	}
	metrics.ChecksTotal.WithLabelValues(result.Region, outcome).Inc()
	metrics.CheckDurationSeconds.WithLabelValues(result.Region).Observe(float64(result.DurationMS) / 1000)

	p.evaluateIncident(ctx, result)

	p.mu.Lock()
	p.pending = append(p.pending, result)
	p.acks = append(p.acks, msg)
	shouldFlush := len(p.pending) >= p.cfg.BatchSize
	p.mu.Unlock()

	if shouldFlush {
		p.flush(ctx)
	}
}

// evaluateIncident implements the incident state machine:
//
//	success -> reset consecutive-failure counter; if an incident was open,
//	           resolve it and notify "resolved".
//	failure -> increment consecutive-failure counter; once it reaches the
//	           monitor's FailureThreshold, open an incident (idempotent) and
//	           notify "opened".
//
// A single transient failure never opens an incident — this is what
// prevents one dropped packet from paging anyone.
func (p *Processor) evaluateIncident(ctx context.Context, result models.CheckResult) {
	monitorIDStr := result.MonitorID.String()

	if result.Success {
		if resetErr := p.failures.Reset(ctx, monitorIDStr, result.Region); resetErr != nil {
			slog.Error("failed to reset failure counter", "error", resetErr)
			return
		}
		inc, resolved, err := p.incidents.ResolveIncident(ctx, result.MonitorID, result.Region)
		if err != nil {
			slog.Error("failed to resolve incident", "error", err)
			return
		}
		if resolved {
			metrics.IncidentsResolvedTotal.WithLabelValues(result.Region).Inc()
			p.notify(ctx, inc, "resolved")
		}
		return
	}

	count, err := p.failures.Incr(ctx, monitorIDStr, result.Region)
	if err != nil {
		slog.Error("failed to increment failure counter", "error", err)
		return
	}

	monitor, err := p.monitors.Get(ctx, result.MonitorID)
	if err != nil {
		slog.Error("failed to look up monitor for incident evaluation", "error", err)
		return
	}
	threshold := int64(monitor.FailureThreshold)
	if threshold <= 0 {
		threshold = 3
	}
	if count < threshold {
		return
	}

	cause := result.ErrorMessage
	if cause == "" {
		cause = fmt.Sprintf("unexpected status code %d", result.StatusCode)
	}
	inc, opened, err := p.incidents.OpenIncident(ctx, result.MonitorID, result.Region, cause)
	if err != nil {
		slog.Error("failed to open incident", "error", err)
		return
	}
	if opened {
		metrics.IncidentsOpenedTotal.WithLabelValues(result.Region).Inc()
		p.notify(ctx, inc, "opened")
	}
}

func (p *Processor) notify(ctx context.Context, inc models.Incident, transition string) {
	ownerID, err := p.incidents.MonitorOwner(ctx, inc.MonitorID)
	if err != nil {
		slog.Error("failed to resolve monitor owner for notification", "error", err)
		return
	}
	channels, err := p.incidents.NotificationChannelsByOwner(ctx, ownerID)
	if err != nil {
		slog.Error("failed to load notification channels", "error", err)
		return
	}
	monitor, err := p.monitors.Get(ctx, inc.MonitorID)
	if err != nil {
		slog.Error("failed to load monitor for notification", "error", err)
		return
	}
	p.notifier.Dispatch(ctx, notify.Event{Monitor: monitor, Incident: inc, Transition: transition}, channels)
}

// flush writes the pending batch to Postgres and acks the corresponding
// JetStream messages only after the write succeeds — if InsertCheckResultsBatch
// fails, messages stay unacked and JetStream redelivers them, so results are
// never silently dropped on a Postgres outage (at the cost of at-least-once
// duplication, which the (monitor_id, started_at) access pattern tolerates
// fine for a monitoring dataset).
func (p *Processor) flush(ctx context.Context) {
	p.mu.Lock()
	if len(p.pending) == 0 {
		p.mu.Unlock()
		return
	}
	batch := p.pending
	acks := p.acks
	p.pending = nil
	p.acks = nil
	p.mu.Unlock()

	start := time.Now()
	err := p.results.InsertCheckResultsBatch(ctx, batch)
	metrics.ResultBatchWriteSeconds.Observe(time.Since(start).Seconds())
	metrics.ResultBatchSize.Observe(float64(len(batch)))

	if err != nil {
		slog.Error("batch insert failed, leaving messages unacked for redelivery", "batch_size", len(batch), "error", err)
		for _, msg := range acks {
			_ = msg.Nak()
		}
		return
	}
	for _, msg := range acks {
		_ = msg.Ack()
	}
}
