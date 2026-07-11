// Package prober implements the actual network checks: a DNS-caching,
// keep-alive-tuned HTTP client wrapped with timing instrumentation.
package prober

import (
	"context"
	"errors"
	"net"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"
)

// DNSCache is a TTL-based resolver cache sitting in front of the system
// resolver. At 50,000 monitors on a 30s interval, an uncached resolver would
// issue up to ~1,667 lookups/sec against upstream DNS; most targets' IPs
// don't change within a 30s window, so caching collapses that to roughly one
// lookup per unique host per TTL window.
type DNSCache struct {
	ttl   time.Duration
	mu    sync.RWMutex
	cache map[string]cacheEntry
	group singleflight.Group
}

type cacheEntry struct {
	ips     []net.IPAddr
	expires time.Time
}

func NewDNSCache(ttl time.Duration) *DNSCache {
	if ttl <= 0 {
		ttl = 30 * time.Second
	}
	return &DNSCache{ttl: ttl, cache: make(map[string]cacheEntry)}
}

var ErrNoAddresses = errors.New("dns cache: no addresses returned")

func (c *DNSCache) Resolve(ctx context.Context, host string) ([]net.IPAddr, error) {
	if ip := net.ParseIP(host); ip != nil {
		return []net.IPAddr{{IP: ip}}, nil
	}

	c.mu.RLock()
	entry, ok := c.cache[host]
	c.mu.RUnlock()
	if ok && time.Now().Before(entry.expires) {
		return entry.ips, nil
	}

	// singleflight collapses concurrent lookups for the same host (e.g. many
	// monitors on the same domain, or a cache-expiry moment coinciding with a
	// burst of scheduled checks) into a single upstream resolution.
	v, err, _ := c.group.Do(host, func() (interface{}, error) {
		ips, err := net.DefaultResolver.LookupIPAddr(ctx, host)
		if err != nil {
			return nil, err
		}
		if len(ips) == 0 {
			return nil, ErrNoAddresses
		}
		c.mu.Lock()
		c.cache[host] = cacheEntry{ips: ips, expires: time.Now().Add(c.ttl)}
		c.mu.Unlock()
		return ips, nil
	})
	if err != nil {
		// Fall back to a stale cache entry rather than failing the check
		// outright on a transient resolver hiccup.
		c.mu.RLock()
		stale, ok := c.cache[host]
		c.mu.RUnlock()
		if ok {
			return stale.ips, nil
		}
		return nil, err
	}
	return v.([]net.IPAddr), nil
}

// DialContext returns a dial function suitable for http.Transport.DialContext
// that resolves through the cache and tries each returned address in order
// until one connects (basic Happy-Eyeballs-lite fallback).
func (c *DNSCache) DialContext(dialer *net.Dialer) func(ctx context.Context, network, addr string) (net.Conn, error) {
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		host, port, err := net.SplitHostPort(addr)
		if err != nil {
			return nil, err
		}
		ips, err := c.Resolve(ctx, host)
		if err != nil {
			return nil, err
		}

		var lastErr error
		for _, ip := range ips {
			conn, err := dialer.DialContext(ctx, network, net.JoinHostPort(ip.IP.String(), port))
			if err == nil {
				return conn, nil
			}
			lastErr = err
		}
		return nil, lastErr
	}
}

// Purge evicts expired entries; call periodically to bound memory when
// monitoring a long tail of rarely-checked hosts.
func (c *DNSCache) Purge() {
	now := time.Now()
	c.mu.Lock()
	defer c.mu.Unlock()
	for host, e := range c.cache {
		if now.After(e.expires.Add(c.ttl)) {
			delete(c.cache, host)
		}
	}
}
