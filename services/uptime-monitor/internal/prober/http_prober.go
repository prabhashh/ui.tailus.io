package prober

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net/http"
	"net/http/httptrace"
	"time"

	"github.com/tailus/uptime-monitor/internal/circuitbreaker"
	"github.com/tailus/uptime-monitor/internal/latency"
	"github.com/tailus/uptime-monitor/internal/models"
	"github.com/tailus/uptime-monitor/internal/retry"
)

// maxBodyBytes bounds how much of a response body we read — a check only
// needs to confirm the target responded and the status code matched;
// draining a multi-GB body would waste worker time and bandwidth.
const maxBodyBytes = 64 * 1024

// HTTPProber executes HTTP(S) check jobs.
type HTTPProber struct {
	client         *http.Client
	dnsCache       *DNSCache
	latencies      *latency.Registry
	breakers       *circuitbreaker.Registry
	retryPolicy    retry.Policy
	defaultTimeout time.Duration
	minTimeout     time.Duration
	maxTimeout     time.Duration
}

func NewHTTPProber(
	transportCfg TransportConfig,
	latencies *latency.Registry,
	breakers *circuitbreaker.Registry,
	retryPolicy retry.Policy,
	defaultTimeout, minTimeout, maxTimeout time.Duration,
) *HTTPProber {
	transport, dnsCache := NewTransport(transportCfg)
	return &HTTPProber{
		client: &http.Client{
			Transport: transport,
			// CheckRedirect is left at default (follow up to 10 redirects);
			// the overall per-request context deadline still bounds total time.
		},
		dnsCache:       dnsCache,
		latencies:      latencies,
		breakers:       breakers,
		retryPolicy:    retryPolicy,
		defaultTimeout: defaultTimeout,
		minTimeout:     minTimeout,
		maxTimeout:     maxTimeout,
	}
}

// timeoutFor picks the adaptive timeout if the monitor's tracker has warmed
// up, otherwise falls back to the job's configured base timeout.
func (p *HTTPProber) timeoutFor(job models.CheckJob) time.Duration {
	tracker := p.latencies.Get(job.MonitorID)
	floor := p.minTimeout
	ceiling := p.maxTimeout
	if job.MinTimeoutMS > 0 {
		floor = time.Duration(job.MinTimeoutMS) * time.Millisecond
	}
	if job.MaxTimeoutMS > 0 {
		ceiling = time.Duration(job.MaxTimeoutMS) * time.Millisecond
	}
	if tracker.Ready() {
		return tracker.Timeout(floor, ceiling)
	}
	if job.BaseTimeoutMS > 0 {
		return time.Duration(job.BaseTimeoutMS) * time.Millisecond
	}
	return p.defaultTimeout
}

// Check executes a single job end to end: circuit breaker gate, adaptive
// timeout selection, retry-wrapped request execution with timing capture,
// and breaker/latency bookkeeping. It never returns an error — network and
// protocol failures are represented in the returned CheckResult so the
// caller can always publish a result.
func (p *HTTPProber) Check(ctx context.Context, job models.CheckJob) models.CheckResult {
	breaker := p.breakers.Get(job.MonitorID)
	started := time.Now()

	if !breaker.Allow() {
		return models.CheckResult{
			MonitorID:    job.MonitorID,
			Region:       job.Region,
			StartedAt:    started,
			Success:      false,
			ErrorMessage: "circuit breaker open: skipped",
		}
	}

	timeout := p.timeoutFor(job)
	var timing timingBreakdown
	var result models.CheckResult

	err := retry.Do(ctx, p.retryPolicy, func(attempt int) error {
		reqCtx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()

		timing = timingBreakdown{}
		start := time.Now()
		statusCode, execErr := p.doRequest(reqCtx, job, &timing)
		duration := time.Since(start)

		result = models.CheckResult{
			MonitorID:     job.MonitorID,
			Region:        job.Region,
			StartedAt:     start,
			DurationMS:    duration.Milliseconds(),
			StatusCode:    statusCode,
			DNSMS:         timing.dns.Milliseconds(),
			ConnectMS:     timing.connect.Milliseconds(),
			TLSMS:         timing.tls.Milliseconds(),
			TTFBMS:        timing.ttfb.Milliseconds(),
			TimeoutUsedMS: timeout.Milliseconds(),
		}

		if execErr != nil {
			result.Success = false
			result.ErrorMessage = execErr.Error()
			return execErr
		}
		if statusCode < job.ExpectedStatusMin || statusCode > job.ExpectedStatusMax {
			result.Success = false
			result.ErrorMessage = fmt.Sprintf("unexpected status code %d", statusCode)
			return nil // not a transport error: don't retry, this is a real signal
		}
		result.Success = true
		return nil
	})

	if err != nil && result.ErrorMessage == "" {
		result.ErrorMessage = err.Error()
	}

	// Feed the observation back into the adaptive systems regardless of
	// success/failure classification above — a 500 still tells us how long
	// the target took to respond, which is real timeout-sizing signal.
	p.latencies.Get(job.MonitorID).Observe(float64(result.DurationMS))
	if result.Success {
		breaker.RecordSuccess()
	} else {
		breaker.RecordFailure()
	}

	return result
}

type timingBreakdown struct {
	dnsStart, dnsEnd         time.Time
	connectStart, connectEnd time.Time
	tlsStart, tlsEnd         time.Time
	reqStart, firstByte      time.Time

	dns, connect, tls, ttfb time.Duration
}

func (p *HTTPProber) doRequest(ctx context.Context, job models.CheckJob, timing *timingBreakdown) (int, error) {
	var body io.Reader
	if job.Body != "" {
		body = bytes.NewReader([]byte(job.Body))
	}

	method := job.Method
	if method == "" {
		method = http.MethodGet
	}

	req, err := http.NewRequestWithContext(ctx, method, job.URL, body)
	if err != nil {
		return 0, err
	}
	for k, v := range job.Headers {
		req.Header.Set(k, v)
	}
	if req.Header.Get("User-Agent") == "" {
		req.Header.Set("User-Agent", "TailusUptimeMonitor/1.0 (+https://ui.tailus.io)")
	}

	timing.reqStart = time.Now()
	trace := &httptrace.ClientTrace{
		DNSStart: func(httptrace.DNSStartInfo) { timing.dnsStart = time.Now() },
		DNSDone: func(httptrace.DNSDoneInfo) {
			timing.dnsEnd = time.Now()
			timing.dns = timing.dnsEnd.Sub(timing.dnsStart)
		},
		ConnectStart: func(_, _ string) { timing.connectStart = time.Now() },
		ConnectDone: func(_, _ string, _ error) {
			timing.connectEnd = time.Now()
			timing.connect = timing.connectEnd.Sub(timing.connectStart)
		},
		TLSHandshakeStart: func() { timing.tlsStart = time.Now() },
		TLSHandshakeDone: func(tls.ConnectionState, error) {
			timing.tlsEnd = time.Now()
			timing.tls = timing.tlsEnd.Sub(timing.tlsStart)
		},
		GotFirstResponseByte: func() {
			timing.firstByte = time.Now()
			timing.ttfb = timing.firstByte.Sub(timing.reqStart)
		},
	}
	req = req.WithContext(httptrace.WithClientTrace(ctx, trace))

	resp, err := p.client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, maxBodyBytes))
	return resp.StatusCode, nil
}

// MaintainDNSCache should be run as a background goroutine; it periodically
// evicts expired DNS cache entries to bound memory on long-running probes.
func (p *HTTPProber) MaintainDNSCache(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			p.dnsCache.Purge()
		}
	}
}
