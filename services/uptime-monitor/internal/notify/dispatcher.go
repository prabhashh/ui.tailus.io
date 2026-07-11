// Package notify turns a resolved incident state transition into outbound
// alerts across an owner's configured channels, with dedup so a transition
// is never announced twice and per-channel failures never block delivery to
// other channels.
package notify

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/tailus/uptime-monitor/internal/metrics"
	"github.com/tailus/uptime-monitor/internal/models"
	"github.com/tailus/uptime-monitor/internal/retry"
)

// Event is the payload handed to every channel.
type Event struct {
	Monitor    models.Monitor
	Incident   models.Incident
	Transition string // "opened" | "resolved"
}

// Channel is implemented by each transport (webhook, email, ...).
type Channel interface {
	Type() models.NotificationChannelType
	Send(ctx context.Context, cfg models.NotificationChannel, event Event) error
}

// Deduper guards against sending the same (incident, transition) more than
// once, e.g. under concurrent result-processor replicas.
type Deduper interface {
	ShouldSend(ctx context.Context, key string) (bool, error)
}

type Dispatcher struct {
	channels map[models.NotificationChannelType]Channel
	dedup    Deduper
	retry    retry.Policy
}

func NewDispatcher(dedup Deduper, channels ...Channel) *Dispatcher {
	m := make(map[models.NotificationChannelType]Channel, len(channels))
	for _, c := range channels {
		m[c.Type()] = c
	}
	return &Dispatcher{channels: m, dedup: dedup, retry: retry.Policy{MaxAttempts: 3, BaseDelay: 500 * time.Millisecond, MaxDelay: 5 * time.Second}}
}

// Dispatch fans an event out to every provided channel concurrently. A
// failure on one channel is logged and counted, never propagated to others.
func (d *Dispatcher) Dispatch(ctx context.Context, event Event, targets []models.NotificationChannel) {
	for _, target := range targets {
		target := target
		go func() {
			d.sendOne(ctx, event, target)
		}()
	}
}

func (d *Dispatcher) sendOne(ctx context.Context, event Event, target models.NotificationChannel) {
	channel, ok := d.channels[target.Type]
	if !ok {
		slog.Warn("no sender registered for channel type", "type", target.Type)
		return
	}

	dedupKey := fmt.Sprintf("%s:%s:%s:%s", event.Incident.ID, event.Incident.Region, target.ID, event.Transition)
	shouldSend, err := d.dedup.ShouldSend(ctx, dedupKey)
	if err != nil {
		slog.Error("dedup check failed, sending anyway to avoid missed alert", "error", err)
	} else if !shouldSend {
		return
	}

	err = retry.DoAlways(ctx, d.retry, func(int) error {
		return channel.Send(ctx, target, event)
	})
	result := "success"
	if err != nil {
		result = "failure"
		slog.Error("notification send failed", "channel_type", target.Type, "monitor_id", event.Monitor.ID, "error", err)
	}
	metrics.NotificationsSentTotal.WithLabelValues(string(target.Type), result).Inc()
}
