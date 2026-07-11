package workerpool

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestPoolBoundsConcurrency(t *testing.T) {
	const capacity = 5
	p := New(capacity)

	var (
		current int64
		maxSeen int64
		wg      sync.WaitGroup
		release = make(chan struct{})
	)

	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			p.Submit(context.Background(), func() {
				n := atomic.AddInt64(&current, 1)
				for {
					old := atomic.LoadInt64(&maxSeen)
					if n <= old || atomic.CompareAndSwapInt64(&maxSeen, old, n) {
						break
					}
				}
				<-release
				atomic.AddInt64(&current, -1)
			})
		}()
	}

	// Give goroutines a chance to pile up against the semaphore.
	time.Sleep(50 * time.Millisecond)
	close(release)
	wg.Wait()
	p.Wait()

	if maxSeen > capacity {
		t.Errorf("pool allowed %d concurrent jobs, capacity was %d", maxSeen, capacity)
	}
}

func TestTrySubmitDropsWhenSaturated(t *testing.T) {
	p := New(1)
	block := make(chan struct{})
	if !p.TrySubmit(func() { <-block }) {
		t.Fatal("expected first TrySubmit to succeed on an empty pool")
	}

	// Pool is now full (capacity 1); a second TrySubmit must not block and
	// must report failure rather than queueing.
	ok := p.TrySubmit(func() {})
	if ok {
		t.Error("expected TrySubmit to fail when the pool is saturated")
	}
	if p.Dropped() != 1 {
		t.Errorf("expected Dropped() == 1, got %d", p.Dropped())
	}

	close(block)
	p.Wait()
}

func TestSubmitUnblocksOnContextCancel(t *testing.T) {
	p := New(1)
	block := make(chan struct{})
	p.TrySubmit(func() { <-block })

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	start := time.Now()
	ok := p.Submit(ctx, func() {})
	if ok {
		t.Error("expected Submit to fail once context deadline is exceeded")
	}
	if time.Since(start) > 200*time.Millisecond {
		t.Error("Submit took too long to respect context cancellation")
	}
	close(block)
	p.Wait()
}

func TestRouterLanesAreIndependent(t *testing.T) {
	r := NewRouter(2, 1)
	if r.Lane(false) != r.Main {
		t.Error("expected non-quarantined jobs to route to Main")
	}
	if r.Lane(true) != r.Quarantine {
		t.Error("expected quarantined jobs to route to Quarantine")
	}
}
