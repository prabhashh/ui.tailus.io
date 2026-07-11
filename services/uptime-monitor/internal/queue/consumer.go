package queue

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// JobHandler processes one decoded message. It returns quickly after
// handing the job to a worker pool (see cmd/prober) — Consume does not wait
// for check execution to finish before fetching the next batch, since the
// worker pool itself provides backpressure via TrySubmit.
type JobHandler[T any] func(ctx context.Context, msg jetstream.Msg, payload T)

// PullLoop continuously fetches batches from a pull consumer and dispatches
// each message to handler. It blocks until ctx is cancelled.
//
// Ack timing is the caller's responsibility (handler must Ack/Nak/Term the
// message once it knows the outcome) — this keeps at-least-once delivery
// semantics correct even though dispatch itself is non-blocking.
func PullLoop[T any](ctx context.Context, consumer jetstream.Consumer, batchSize int, fetchTimeout time.Duration, handler JobHandler[T]) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		batch, err := consumer.Fetch(batchSize, jetstream.FetchMaxWait(fetchTimeout))
		if err != nil {
			// No messages available within the wait window is the normal
			// steady-state case, not an error worth logging loudly.
			if err == jetstream.ErrNoMessages || err == context.DeadlineExceeded {
				continue
			}
			slog.Error("jetstream fetch failed", "error", err)
			time.Sleep(time.Second)
			continue
		}

		for msg := range batch.Messages() {
			var payload T
			if err := json.Unmarshal(msg.Data(), &payload); err != nil {
				slog.Error("failed to decode message, terminating redelivery", "error", err)
				_ = msg.Term()
				continue
			}
			handler(ctx, msg, payload)
		}
		if err := batch.Error(); err != nil {
			slog.Warn("jetstream batch completed with error", "error", err)
		}
	}
}
