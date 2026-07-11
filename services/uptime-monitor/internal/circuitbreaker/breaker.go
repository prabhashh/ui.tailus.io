// Package circuitbreaker implements a per-monitor circuit breaker so that a
// target which is completely unreachable (DNS failure, connection refused,
// TLS failure) doesn't keep consuming a full worker-pool slot and a full
// timeout on every scheduled interval. Breakers are kept in-process per
// probe (see the package doc in internal/latency for why that's safe here):
// consistent hashing routes a monitor's checks to the same consumer group,
// so cross-node state sharing isn't needed for correctness.
package circuitbreaker

import (
	"sync"
	"time"
)

type State int

const (
	Closed   State = iota // normal operation, requests pass through
	Open                  // failing fast, requests are short-circuited
	HalfOpen              // trial request allowed through to test recovery
)

func (s State) String() string {
	switch s {
	case Closed:
		return "closed"
	case Open:
		return "open"
	case HalfOpen:
		return "half_open"
	default:
		return "unknown"
	}
}

// Config tunes breaker sensitivity. Defaults are deliberately conservative:
// a monitor has to fail consistently across several consecutive scheduled
// checks (spaced by its own interval, typically 30s+) before it trips, so
// a single blip never trips the breaker.
type Config struct {
	FailureThreshold int           // consecutive failures to trip from Closed -> Open
	OpenDuration     time.Duration // how long to stay Open before allowing a trial request
	SuccessThreshold int           // consecutive successes in HalfOpen to close again
}

func DefaultConfig() Config {
	return Config{
		FailureThreshold: 5,
		OpenDuration:     2 * time.Minute,
		SuccessThreshold: 2,
	}
}

// Breaker is a single circuit breaker instance.
type Breaker struct {
	mu              sync.Mutex
	cfg             Config
	state           State
	consecFailures  int
	consecSuccesses int
	openedAt        time.Time
}

func New(cfg Config) *Breaker {
	return &Breaker{cfg: cfg, state: Closed}
}

// Allow reports whether a check should actually be executed. When the
// breaker is Open and the cooldown has elapsed, it transitions to HalfOpen
// and allows exactly one trial request through.
func (b *Breaker) Allow() bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	switch b.state {
	case Closed, HalfOpen:
		return true
	case Open:
		if time.Since(b.openedAt) >= b.cfg.OpenDuration {
			b.state = HalfOpen
			b.consecSuccesses = 0
			return true
		}
		return false
	}
	return true
}

// RecordSuccess reports a successful check outcome.
func (b *Breaker) RecordSuccess() {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.consecFailures = 0
	switch b.state {
	case HalfOpen:
		b.consecSuccesses++
		if b.consecSuccesses >= b.cfg.SuccessThreshold {
			b.state = Closed
		}
	case Open:
		// shouldn't normally happen (Allow gates this), but stay consistent
		b.state = Closed
	}
}

// RecordFailure reports a failed check outcome.
func (b *Breaker) RecordFailure() {
	b.mu.Lock()
	defer b.mu.Unlock()

	switch b.state {
	case HalfOpen:
		b.trip()
	case Closed:
		b.consecFailures++
		if b.consecFailures >= b.cfg.FailureThreshold {
			b.trip()
		}
	}
}

func (b *Breaker) trip() {
	b.state = Open
	b.openedAt = time.Now()
	b.consecFailures = 0
	b.consecSuccesses = 0
}

func (b *Breaker) State() State {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.state
}
