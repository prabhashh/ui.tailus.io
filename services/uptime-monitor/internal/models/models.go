// Package models defines the core domain types shared across every service
// (scheduler, prober, result processor, API).
package models

import (
	"time"

	"github.com/google/uuid"
)

// CheckType identifies the protocol-level probe strategy for a monitor.
type CheckType string

const (
	CheckTypeHTTP CheckType = "http"
	CheckTypeTCP  CheckType = "tcp"
)

// Monitor is a single target under observation.
type Monitor struct {
	ID                uuid.UUID         `json:"id"`
	OwnerID           uuid.UUID         `json:"owner_id"`
	Name              string            `json:"name"`
	URL               string            `json:"url"`
	CheckType         CheckType         `json:"check_type"`
	Method            string            `json:"method"`
	Headers           map[string]string `json:"headers,omitempty"`
	Body              string            `json:"body,omitempty"`
	ExpectedStatusMin int               `json:"expected_status_min"`
	ExpectedStatusMax int               `json:"expected_status_max"`
	IntervalSeconds   int               `json:"interval_seconds"`
	BaseTimeoutMS     int               `json:"base_timeout_ms"`   // used until adaptive stats warm up
	MinTimeoutMS      int               `json:"min_timeout_ms"`    // floor for adaptive timeout
	MaxTimeoutMS      int               `json:"max_timeout_ms"`    // ceiling for adaptive timeout
	Regions           []string          `json:"regions"`           // e.g. ["us-east", "eu-west"]
	FailureThreshold  int               `json:"failure_threshold"` // consecutive failures before incident
	Enabled           bool              `json:"enabled"`
	CreatedAt         time.Time         `json:"created_at"`
	UpdatedAt         time.Time         `json:"updated_at"`
}

// CheckJob is the message published to the scheduling queue (NATS JetStream)
// for a regional prober to execute.
type CheckJob struct {
	MonitorID         uuid.UUID         `json:"monitor_id"`
	URL               string            `json:"url"`
	CheckType         CheckType         `json:"check_type"`
	Method            string            `json:"method"`
	Headers           map[string]string `json:"headers,omitempty"`
	Body              string            `json:"body,omitempty"`
	ExpectedStatusMin int               `json:"expected_status_min"`
	ExpectedStatusMax int               `json:"expected_status_max"`
	Region            string            `json:"region"`
	ScheduledAt       time.Time         `json:"scheduled_at"`
	BaseTimeoutMS     int               `json:"base_timeout_ms"`
	MinTimeoutMS      int               `json:"min_timeout_ms"`
	MaxTimeoutMS      int               `json:"max_timeout_ms"`
	Attempt           int               `json:"attempt"`
}

// IdempotencyKey returns a stable identifier used as the NATS Nats-Msg-Id
// header so redeliveries / duplicate publishes are deduplicated by JetStream.
func (j CheckJob) IdempotencyKey() string {
	return j.MonitorID.String() + ":" + j.Region + ":" + j.ScheduledAt.UTC().Format(time.RFC3339)
}

// CheckResult is the outcome of executing a CheckJob, published by a prober
// to the results stream for asynchronous persistence.
type CheckResult struct {
	MonitorID    uuid.UUID `json:"monitor_id"`
	Region       string    `json:"region"`
	StartedAt    time.Time `json:"started_at"`
	DurationMS   int64     `json:"duration_ms"`
	Success      bool      `json:"success"`
	StatusCode   int       `json:"status_code,omitempty"`
	ErrorMessage string    `json:"error_message,omitempty"`

	// Timing breakdown, populated via httptrace for HTTP checks.
	DNSMS     int64 `json:"dns_ms,omitempty"`
	ConnectMS int64 `json:"connect_ms,omitempty"`
	TLSMS     int64 `json:"tls_ms,omitempty"`
	TTFBMS    int64 `json:"ttfb_ms,omitempty"`

	TimeoutUsedMS int64 `json:"timeout_used_ms"`
}

// IncidentStatus is the lifecycle state of an incident.
type IncidentStatus string

const (
	IncidentOpen     IncidentStatus = "open"
	IncidentResolved IncidentStatus = "resolved"
)

// Incident tracks a contiguous period during which a monitor was considered
// down (FailureThreshold consecutive failures in a region).
type Incident struct {
	ID         uuid.UUID      `json:"id"`
	MonitorID  uuid.UUID      `json:"monitor_id"`
	Region     string         `json:"region"`
	Status     IncidentStatus `json:"status"`
	Cause      string         `json:"cause"`
	StartedAt  time.Time      `json:"started_at"`
	ResolvedAt *time.Time     `json:"resolved_at,omitempty"`
}

// NotificationChannelType enumerates supported outbound alert transports.
type NotificationChannelType string

const (
	ChannelWebhook NotificationChannelType = "webhook"
	ChannelEmail   NotificationChannelType = "email"
)

// NotificationChannel is a configured alert destination for an owner.
type NotificationChannel struct {
	ID      uuid.UUID               `json:"id"`
	OwnerID uuid.UUID               `json:"owner_id"`
	Type    NotificationChannelType `json:"type"`
	Target  string                  `json:"target"`           // URL for webhook, address for email
	Secret  string                  `json:"secret,omitempty"` // HMAC signing secret for webhooks
}

// LatencyStats is the periodically-flushed summary of a monitor's adaptive
// timeout tracker, persisted for observability and failover bootstrap.
type LatencyStats struct {
	MonitorID uuid.UUID `json:"monitor_id"`
	Region    string    `json:"region"`
	Mean      float64   `json:"mean_ms"`
	StdDev    float64   `json:"stddev_ms"`
	P95       float64   `json:"p95_ms"`
	Samples   int64     `json:"samples"`
	UpdatedAt time.Time `json:"updated_at"`
}
