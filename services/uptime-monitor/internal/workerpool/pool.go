// Package workerpool provides bounded-concurrency execution for check jobs.
// Each job still runs on its own goroutine (so one hung HTTP call can never
// block another), but a semaphore caps how many run at once, which is what
// keeps 50,000+ monitors from turning into 50,000 simultaneous goroutines
// and sockets. Combined with per-check context timeouts (see internal/prober)
// this is the primary mechanism preventing slow targets from starving fast
// ones: a slow check occupies exactly one slot for at most MaxTimeout, never
// more, and never blocks the dispatch of other jobs.
package workerpool

import (
	"context"
	"sync"
	"sync/atomic"
)

// Pool is a bounded-concurrency job runner.
type Pool struct {
	sem      chan struct{}
	wg       sync.WaitGroup
	inFlight int64
	dropped  int64
}

func New(size int) *Pool {
	if size <= 0 {
		size = 1
	}
	return &Pool{sem: make(chan struct{}, size)}
}

// Submit blocks until a slot is free or ctx is cancelled, then runs job on a
// new goroutine. Returns false if ctx was cancelled before a slot opened up.
func (p *Pool) Submit(ctx context.Context, job func()) bool {
	select {
	case p.sem <- struct{}{}:
	case <-ctx.Done():
		return false
	}
	p.run(job)
	return true
}

// TrySubmit attempts to acquire a slot without blocking. Use this on the
// dispatch hot path (e.g. the JetStream consumer loop) so a saturated pool
// applies backpressure to the queue (message stays unacked / redelivered)
// instead of piling up an unbounded in-process backlog.
func (p *Pool) TrySubmit(job func()) bool {
	select {
	case p.sem <- struct{}{}:
		p.run(job)
		return true
	default:
		atomic.AddInt64(&p.dropped, 1)
		return false
	}
}

func (p *Pool) run(job func()) {
	atomic.AddInt64(&p.inFlight, 1)
	p.wg.Add(1)
	go func() {
		defer func() {
			<-p.sem
			atomic.AddInt64(&p.inFlight, -1)
			p.wg.Done()
		}()
		job()
	}()
}

// Wait blocks until all submitted jobs have completed. Used during graceful
// shutdown.
func (p *Pool) Wait() { p.wg.Wait() }

func (p *Pool) InFlight() int64 { return atomic.LoadInt64(&p.inFlight) }
func (p *Pool) Capacity() int   { return cap(p.sem) }
func (p *Pool) Dropped() int64  { return atomic.LoadInt64(&p.dropped) }
