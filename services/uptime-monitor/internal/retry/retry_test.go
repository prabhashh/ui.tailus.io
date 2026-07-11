package retry

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"
)

func TestDoRetriesRetryableErrorsUntilSuccess(t *testing.T) {
	p := Policy{MaxAttempts: 3, BaseDelay: time.Millisecond, MaxDelay: 5 * time.Millisecond}
	attempts := 0
	err := Do(context.Background(), p, func(attempt int) error {
		attempts++
		if attempt < 3 {
			return &net.OpError{Op: "dial", Err: errors.New("connection refused")}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("expected eventual success, got %v", err)
	}
	if attempts != 3 {
		t.Errorf("expected 3 attempts, got %d", attempts)
	}
}

func TestDoStopsOnNonRetryableError(t *testing.T) {
	p := Policy{MaxAttempts: 5, BaseDelay: time.Millisecond, MaxDelay: 5 * time.Millisecond}
	attempts := 0
	appErr := errors.New("unexpected status code 500")
	err := Do(context.Background(), p, func(attempt int) error {
		attempts++
		return appErr
	})
	if !errors.Is(err, appErr) {
		t.Fatalf("expected the non-retryable error to be returned, got %v", err)
	}
	if attempts != 1 {
		t.Errorf("expected exactly 1 attempt for a non-retryable error, got %d", attempts)
	}
}

func TestDoAlwaysRetriesAnyError(t *testing.T) {
	p := Policy{MaxAttempts: 3, BaseDelay: time.Millisecond, MaxDelay: 5 * time.Millisecond}
	attempts := 0
	err := DoAlways(context.Background(), p, func(attempt int) error {
		attempts++
		if attempt < 3 {
			return errors.New("some app error, e.g. HTTP 503 from webhook receiver")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("expected eventual success, got %v", err)
	}
	if attempts != 3 {
		t.Errorf("expected 3 attempts, got %d", attempts)
	}
}

func TestDelayIsBoundedByMaxDelay(t *testing.T) {
	p := Policy{MaxAttempts: 10, BaseDelay: 100 * time.Millisecond, MaxDelay: 200 * time.Millisecond}
	for attempt := 2; attempt <= 10; attempt++ {
		d := p.Delay(attempt)
		if d > p.MaxDelay {
			t.Errorf("Delay(%d) = %v exceeds MaxDelay %v", attempt, d, p.MaxDelay)
		}
		if d < 0 {
			t.Errorf("Delay(%d) = %v is negative", attempt, d)
		}
	}
}

func TestContextCancellationStopsRetryLoop(t *testing.T) {
	p := Policy{MaxAttempts: 5, BaseDelay: 50 * time.Millisecond, MaxDelay: 50 * time.Millisecond}
	ctx, cancel := context.WithCancel(context.Background())
	attempts := 0
	go func() {
		time.Sleep(10 * time.Millisecond)
		cancel()
	}()
	err := DoAlways(ctx, p, func(attempt int) error {
		attempts++
		return errors.New("always fails")
	})
	if err == nil {
		t.Fatal("expected an error when context is cancelled mid-retry")
	}
	if attempts >= 5 {
		t.Errorf("expected cancellation to cut the retry loop short, got %d attempts", attempts)
	}
}
