// Package latency tracks per-monitor response-time distributions and derives
// an adaptive request timeout from them, so that chronically slow (but
// healthy) targets get enough rope while chronically fast targets get tight
// timeouts that fail over quickly. This is what keeps one slow site from
// eating a disproportionate share of worker-pool time.
package latency

import (
	"math"
	"sync"
	"time"
)

const (
	// WarmupSamples is the minimum observation count before the adaptive
	// estimate is trusted over the monitor's configured base timeout.
	WarmupSamples = 20

	// ewmaAlpha controls how quickly the mean/variance trackers adapt to
	// recent samples. 0.1 ≈ effective window of ~19 samples.
	ewmaAlpha = 0.1

	// p95Multiplier and stdDevZ are the two independent estimates blended
	// into the final timeout; we take whichever is larger so a bursty-but-
	// rare slow tail (caught by stddev) or a consistently elevated tail
	// (caught by p95) both get covered.
	p95Multiplier = 1.5
	stdDevZ       = 3.0

	// safetyMarginMS is added on top of the statistical estimate to absorb
	// network jitter that isn't in the historical sample (cold TLS handshake,
	// a one-off slow DNS resolver, etc).
	safetyMarginMS = 250
)

// Tracker maintains a streaming latency distribution for a single
// (monitor, region) pair.
type Tracker struct {
	mu      sync.Mutex
	p95     *p2Quantile
	mean    float64
	varEwma float64
	samples int64
}

func NewTracker() *Tracker {
	return &Tracker{p95: newP2Quantile(0.95)}
}

// Observe records a completed check's duration in milliseconds. Call this
// regardless of success/failure — timeout sizing cares about how long the
// target takes to respond, not whether the response was a 200.
func (t *Tracker) Observe(durationMS float64) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.p95.observe(durationMS)
	t.samples++

	if t.samples == 1 {
		t.mean = durationMS
		t.varEwma = 0
		return
	}
	delta := durationMS - t.mean
	t.mean += ewmaAlpha * delta
	// EWMA of squared deviation approximates a decaying variance estimate.
	t.varEwma = (1 - ewmaAlpha) * (t.varEwma + ewmaAlpha*delta*delta)
}

// Snapshot returns the current distribution summary (for persistence /
// observability), in milliseconds.
type Snapshot struct {
	Mean    float64
	StdDev  float64
	P95     float64
	Samples int64
}

func (t *Tracker) Snapshot() Snapshot {
	t.mu.Lock()
	defer t.mu.Unlock()
	return Snapshot{
		Mean:    t.mean,
		StdDev:  math.Sqrt(t.varEwma),
		P95:     t.p95.value(),
		Samples: t.samples,
	}
}

// Timeout computes the adaptive request timeout for the next check.
//
//	timeout = clamp( max(P95 * 1.5, mean + 3*stddev) + 250ms, floor, ceiling )
//
// Below WarmupSamples observations the estimate is unreliable (P² needs a
// handful of points to converge and EWMA variance is noisy), so callers
// should prefer the monitor's configured base timeout until then — see
// Tracker.Ready().
func (t *Tracker) Timeout(floor, ceiling time.Duration) time.Duration {
	s := t.Snapshot()
	fromP95 := s.P95 * p95Multiplier
	fromStdDev := s.Mean + stdDevZ*s.StdDev
	estMS := math.Max(fromP95, fromStdDev) + safetyMarginMS

	d := time.Duration(estMS) * time.Millisecond
	if d < floor {
		return floor
	}
	if d > ceiling {
		return ceiling
	}
	return d
}

// Ready reports whether enough samples have accumulated to trust the
// adaptive estimate over a static base timeout.
func (t *Tracker) Ready() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.samples >= WarmupSamples
}
