// Package retry provides bounded exponential backoff with jitter for
// transient check failures, plus classification of which failures are worth
// retrying at all. Retries happen within the same scheduled check cycle
// (not by rescheduling a new job), and are budgeted so a single bad monitor
// can't multiply its cost across the worker pool.
package retry

import (
	"context"
	"errors"
	"math"
	"math/rand"
	"net"
	"time"
)

// Policy configures the backoff curve and retry budget.
type Policy struct {
	MaxAttempts int           // total attempts including the first, e.g. 2 = one retry
	BaseDelay   time.Duration // delay before the first retry
	MaxDelay    time.Duration // cap on any single backoff delay
}

func DefaultPolicy() Policy {
	return Policy{
		MaxAttempts: 2,
		BaseDelay:   200 * time.Millisecond,
		MaxDelay:    2 * time.Second,
	}
}

// Delay returns the backoff duration before attempt N (1-indexed: attempt 2
// is the first retry), using full jitter (Delay = rand(0, min(max, base*2^n))
// as recommended by AWS's backoff-and-jitter guidance to avoid synchronized
// retry storms across many monitors failing at once.
func (p Policy) Delay(attempt int) time.Duration {
	if attempt <= 1 {
		return 0
	}
	exp := float64(p.BaseDelay) * math.Pow(2, float64(attempt-2))
	capped := math.Min(exp, float64(p.MaxDelay))
	return time.Duration(rand.Int63n(int64(capped) + 1))
}

// Retryable reports whether an error represents a transient condition worth
// retrying. Application-level failures (4xx/5xx handled by the caller
// separately, non-matching status code) are NOT retried here — only
// network/transport-level errors are, since a retried HTTP 500 is a
// meaningful, intentional signal from the target, not noise.
func Retryable(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var netErr net.Error
	if errors.As(err, &netErr) {
		return netErr.Timeout() || isConnRefusedOrReset(err)
	}
	return isConnRefusedOrReset(err)
}

func isConnRefusedOrReset(err error) bool {
	var sysErr *net.OpError
	if errors.As(err, &sysErr) {
		return true
	}
	return false
}

// Do runs fn up to policy.MaxAttempts times, sleeping the jittered backoff
// between attempts, stopping early on success, a non-retryable error, or
// context cancellation. It returns the last error observed. Use this for
// network checks, where Retryable's classification (only transport-level
// failures) is the point — an application-level 4xx/5xx is a real signal,
// not noise to retry away.
func Do(ctx context.Context, p Policy, fn func(attempt int) error) error {
	return do(ctx, p, fn, Retryable)
}

// DoAlways runs fn up to policy.MaxAttempts times like Do, but retries on
// any non-nil error rather than classifying it first. Use this for
// operations where every failure is worth another attempt within budget,
// e.g. delivering an outbound notification.
func DoAlways(ctx context.Context, p Policy, fn func(attempt int) error) error {
	return do(ctx, p, fn, func(error) bool { return true })
}

func do(ctx context.Context, p Policy, fn func(attempt int) error, retryable func(error) bool) error {
	var lastErr error
	for attempt := 1; attempt <= p.MaxAttempts; attempt++ {
		if attempt > 1 {
			d := p.Delay(attempt)
			select {
			case <-time.After(d):
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		err := fn(attempt)
		if err == nil {
			return nil
		}
		lastErr = err
		if !retryable(err) {
			return err
		}
	}
	return lastErr
}
