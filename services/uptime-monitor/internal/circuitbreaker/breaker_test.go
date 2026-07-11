package circuitbreaker

import (
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestTripsAfterConsecutiveFailures(t *testing.T) {
	b := New(Config{FailureThreshold: 3, OpenDuration: time.Hour, SuccessThreshold: 1})

	for i := 0; i < 2; i++ {
		if !b.Allow() {
			t.Fatal("breaker should allow while closed")
		}
		b.RecordFailure()
	}
	if b.State() != Closed {
		t.Fatalf("expected Closed before threshold, got %v", b.State())
	}

	b.RecordFailure() // 3rd consecutive failure
	if b.State() != Open {
		t.Fatalf("expected Open after reaching threshold, got %v", b.State())
	}
	if b.Allow() {
		t.Fatal("breaker should not allow while open and cooldown hasn't elapsed")
	}
}

func TestHalfOpenRecoversToClosedOnSuccess(t *testing.T) {
	b := New(Config{FailureThreshold: 1, OpenDuration: 10 * time.Millisecond, SuccessThreshold: 2})

	b.RecordFailure() // trips immediately
	if b.State() != Open {
		t.Fatalf("expected Open, got %v", b.State())
	}

	time.Sleep(15 * time.Millisecond)
	if !b.Allow() {
		t.Fatal("expected trial request to be allowed after cooldown")
	}
	if b.State() != HalfOpen {
		t.Fatalf("expected HalfOpen after cooldown elapses, got %v", b.State())
	}

	b.RecordSuccess()
	if b.State() != HalfOpen {
		t.Fatalf("expected still HalfOpen after 1 of 2 required successes, got %v", b.State())
	}
	b.RecordSuccess()
	if b.State() != Closed {
		t.Fatalf("expected Closed after SuccessThreshold successes, got %v", b.State())
	}
}

func TestHalfOpenReopensOnFailure(t *testing.T) {
	b := New(Config{FailureThreshold: 1, OpenDuration: 10 * time.Millisecond, SuccessThreshold: 2})
	b.RecordFailure()
	time.Sleep(15 * time.Millisecond)
	b.Allow() // transitions to HalfOpen

	b.RecordFailure()
	if b.State() != Open {
		t.Fatalf("expected Open after a failure in HalfOpen, got %v", b.State())
	}
}

func TestSuccessResetsConsecutiveFailureCount(t *testing.T) {
	b := New(Config{FailureThreshold: 3, OpenDuration: time.Hour, SuccessThreshold: 1})
	b.RecordFailure()
	b.RecordFailure()
	b.RecordSuccess() // should reset the counter
	b.RecordFailure()
	b.RecordFailure()
	if b.State() != Closed {
		t.Fatalf("expected Closed since failures didn't reach threshold consecutively, got %v", b.State())
	}
}

func TestRegistryReusesBreakerPerMonitor(t *testing.T) {
	reg := NewRegistry(DefaultConfig())
	id := uuid.New()
	b1 := reg.Get(id)
	b2 := reg.Get(id)
	if b1 != b2 {
		t.Fatal("expected the same breaker instance for the same monitor ID")
	}
}
