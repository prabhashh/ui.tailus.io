package prober

import (
	"crypto/tls"
	"net"
	"net/http"
	"time"
)

// TransportConfig tunes the shared http.Transport used for all checks on a
// probe node. A single shared transport (not one per request) is what makes
// keep-alive actually work: idle connections are pooled per host and reused
// across scheduling cycles instead of paying a fresh TCP+TLS handshake every
// 30 seconds for every monitor.
type TransportConfig struct {
	MaxIdleConns        int
	MaxIdleConnsPerHost int
	IdleConnTimeout     time.Duration
	DNSCacheTTL         time.Duration
	DialTimeout         time.Duration
	TLSHandshakeTimeout time.Duration
}

func DefaultTransportConfig() TransportConfig {
	return TransportConfig{
		MaxIdleConns:        4000,
		MaxIdleConnsPerHost: 4,
		IdleConnTimeout:     90 * time.Second,
		DNSCacheTTL:         30 * time.Second,
		DialTimeout:         5 * time.Second,
		TLSHandshakeTimeout: 5 * time.Second,
	}
}

// NewTransport builds the shared *http.Transport plus its backing DNS cache.
// The DNS cache is returned separately so callers can wire its Purge() into
// a periodic maintenance goroutine.
func NewTransport(cfg TransportConfig) (*http.Transport, *DNSCache) {
	dnsCache := NewDNSCache(cfg.DNSCacheTTL)
	dialer := &net.Dialer{
		Timeout:   cfg.DialTimeout,
		KeepAlive: 30 * time.Second,
	}

	t := &http.Transport{
		DialContext:           dnsCache.DialContext(dialer),
		MaxIdleConns:          cfg.MaxIdleConns,
		MaxIdleConnsPerHost:   cfg.MaxIdleConnsPerHost,
		IdleConnTimeout:       cfg.IdleConnTimeout,
		TLSHandshakeTimeout:   cfg.TLSHandshakeTimeout,
		ExpectContinueTimeout: 1 * time.Second,
		ForceAttemptHTTP2:     true,
		DisableCompression:    false,
		TLSClientConfig:       &tls.Config{MinVersion: tls.VersionTLS12},
		// Individual checks set their own per-request deadline via context,
		// so ResponseHeaderTimeout is intentionally left unset here.
	}
	return t, dnsCache
}
