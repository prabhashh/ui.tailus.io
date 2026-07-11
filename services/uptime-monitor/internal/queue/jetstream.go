// Package queue wraps NATS JetStream for job distribution and result
// collection. Two streams are used:
//
//   - CHECKS  (subjects "checks.<region>.>")  — scheduler -> regional probes
//   - RESULTS (subjects "results.<region>")    — probes -> result processor
//
// Subjects are partitioned by region so each region's probe fleet only
// consumes its own work (regional isolation: a slow region never backs up
// another region's queue), while still sharing a single NATS cluster.
package queue

import (
	"context"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const (
	CheckSubjectPrefix  = "checks"
	ResultSubjectPrefix = "results"
)

func CheckSubject(region string) string  { return fmt.Sprintf("%s.%s", CheckSubjectPrefix, region) }
func ResultSubject(region string) string { return fmt.Sprintf("%s.%s", ResultSubjectPrefix, region) }

// Connect opens a NATS connection with production-sane defaults: unlimited
// reconnect attempts (a probe should never give up trying to reach the
// queue) with capped backoff, and a ping interval that detects a dead
// connection well within one check interval.
func Connect(url, credsFile string) (*nats.Conn, error) {
	opts := []nats.Option{
		nats.Name("uptime-monitor"),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(2 * time.Second),
		nats.PingInterval(10 * time.Second),
		nats.RetryOnFailedConnect(true),
	}
	if credsFile != "" {
		opts = append(opts, nats.UserCredentials(credsFile))
	}
	return nats.Connect(url, opts...)
}

// EnsureStreams creates (or updates) the CHECKS and RESULTS streams. Safe to
// call from every process on startup; JetStream treats it as idempotent.
func EnsureStreams(ctx context.Context, js jetstream.JetStream, checkStream, resultStream string) error {
	_, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:      checkStream,
		Subjects:  []string{CheckSubjectPrefix + ".>"},
		Retention: jetstream.WorkQueuePolicy, // a job is removed once a probe acks it — no fan-out needed
		Storage:   jetstream.FileStorage,
		Replicas:  1, // bump to 3 across a multi-node NATS cluster for HA
		// Jobs are only useful within roughly one check interval; drop
		// anything older so a probe outage doesn't cause a backlog replay
		// storm once it recovers.
		MaxAge:            2 * time.Minute,
		Duplicates:        30 * time.Second, // dedup window keyed by Nats-Msg-Id
		Discard:           jetstream.DiscardOld,
		MaxMsgsPerSubject: -1,
	})
	if err != nil {
		return fmt.Errorf("ensure check stream: %w", err)
	}

	_, err = js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:       resultStream,
		Subjects:   []string{ResultSubjectPrefix + ".>"},
		Retention:  jetstream.WorkQueuePolicy,
		Storage:    jetstream.FileStorage,
		Replicas:   1,
		MaxAge:     10 * time.Minute,
		Duplicates: 30 * time.Second,
		Discard:    jetstream.DiscardOld,
	})
	if err != nil {
		return fmt.Errorf("ensure result stream: %w", err)
	}
	return nil
}

// EnsureConsumer creates a durable pull consumer for a given stream/subject
// filter. AckWait must exceed the maximum possible check duration (max
// adaptive timeout + retry budget) or JetStream will redeliver a message
// while a probe is still legitimately working on it.
func EnsureConsumer(ctx context.Context, js jetstream.JetStream, stream, durableName, filterSubject string, ackWait time.Duration, maxDeliver int) (jetstream.Consumer, error) {
	return js.CreateOrUpdateConsumer(ctx, stream, jetstream.ConsumerConfig{
		Durable:       durableName,
		FilterSubject: filterSubject,
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       ackWait,
		MaxDeliver:    maxDeliver,
		MaxAckPending: 10000, // upper bound on in-flight unacked messages across the pool
	})
}
