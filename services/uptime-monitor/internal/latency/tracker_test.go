package latency

import (
	"math"
	"math/rand"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestP2QuantileConvergesToApproximateP95(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	e := newP2Quantile(0.95)

	samples := make([]float64, 0, 10000)
	for i := 0; i < 10000; i++ {
		v := 100 + rng.NormFloat64()*15 // mean 100, stddev 15
		samples = append(samples, v)
		e.observe(v)
	}

	// exact p95 via sort for comparison
	sorted := append([]float64(nil), samples...)
	for i := 1; i < len(sorted); i++ {
		for j := i; j > 0 && sorted[j-1] > sorted[j]; j-- {
			sorted[j-1], sorted[j] = sorted[j], sorted[j-1]
		}
	}
	exact := sorted[int(0.95*float64(len(sorted)))]
	got := e.value()

	if math.Abs(got-exact) > 5 {
		t.Errorf("P² estimate %.2f too far from exact p95 %.2f", got, exact)
	}
}

func TestTrackerReadyRequiresWarmup(t *testing.T) {
	tr := NewTracker()
	if tr.Ready() {
		t.Fatal("tracker should not be ready before any samples")
	}
	for i := 0; i < WarmupSamples-1; i++ {
		tr.Observe(100)
	}
	if tr.Ready() {
		t.Fatal("tracker should not be ready before WarmupSamples observations")
	}
	tr.Observe(100)
	if !tr.Ready() {
		t.Fatal("tracker should be ready after WarmupSamples observations")
	}
}

func TestTimeoutRespectsFloorAndCeiling(t *testing.T) {
	tr := NewTracker()
	for i := 0; i < 50; i++ {
		tr.Observe(10) // very fast, consistent responses
	}
	floor := 2 * time.Second
	ceiling := 30 * time.Second

	got := tr.Timeout(floor, ceiling)
	if got != floor {
		t.Errorf("expected timeout to clamp to floor %v for a fast monitor, got %v", floor, got)
	}
}

func TestTimeoutScalesWithLatency(t *testing.T) {
	tr := NewTracker()
	for i := 0; i < 50; i++ {
		tr.Observe(8000) // consistently slow: 8s responses
	}
	floor := 2 * time.Second
	ceiling := 30 * time.Second

	got := tr.Timeout(floor, ceiling)
	if got <= floor {
		t.Errorf("expected adaptive timeout above floor for a consistently slow monitor, got %v", got)
	}
	if got > ceiling {
		t.Errorf("timeout %v exceeded ceiling %v", got, ceiling)
	}
}

func TestRegistrySeedThenObserve(t *testing.T) {
	reg := NewRegistry()
	id := uuid.New()
	reg.Seed(id, Snapshot{Mean: 500, StdDev: 50, P95: 600, Samples: 100})

	tr := reg.Get(id)
	if !tr.Ready() {
		t.Fatal("seeded tracker with samples >= warmup should be ready")
	}
	snap := tr.Snapshot()
	if snap.P95 != 600 {
		t.Errorf("expected seeded P95 600, got %.2f", snap.P95)
	}

	tr.Observe(550)
	snap2 := tr.Snapshot()
	if snap2.Samples != 101 {
		t.Errorf("expected sample count to increment from seed, got %d", snap2.Samples)
	}
}
