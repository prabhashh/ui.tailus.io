package queue

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/tailus/uptime-monitor/internal/models"
)

// JobPublisher publishes check jobs, one per region subject.
type JobPublisher struct {
	js jetstream.JetStream
}

func NewJobPublisher(js jetstream.JetStream) *JobPublisher {
	return &JobPublisher{js: js}
}

// Publish sends a job with its idempotency key set as the JetStream message
// ID, so a scheduler retry (e.g. after a publish ack timeout) never results
// in the same check running twice.
func (p *JobPublisher) Publish(ctx context.Context, job models.CheckJob) error {
	data, err := json.Marshal(job)
	if err != nil {
		return fmt.Errorf("marshal check job: %w", err)
	}
	msg := &nats.Msg{
		Subject: CheckSubject(job.Region),
		Data:    data,
		Header:  nats.Header{jetstream.MsgIDHeader: []string{job.IdempotencyKey()}},
	}
	_, err = p.js.PublishMsg(ctx, msg)
	return err
}

// ResultPublisher publishes check results for asynchronous persistence.
type ResultPublisher struct {
	js jetstream.JetStream
}

func NewResultPublisher(js jetstream.JetStream) *ResultPublisher {
	return &ResultPublisher{js: js}
}

func (p *ResultPublisher) Publish(ctx context.Context, result models.CheckResult) error {
	data, err := json.Marshal(result)
	if err != nil {
		return fmt.Errorf("marshal check result: %w", err)
	}
	// PublishAsync is used on the result path: probes should never block a
	// worker-pool slot waiting for a publish ack from NATS. Errors surface
	// via PublishAsyncComplete monitoring (see metrics) rather than inline.
	_, err = p.js.PublishAsync(ResultSubject(result.Region), data)
	return err
}
