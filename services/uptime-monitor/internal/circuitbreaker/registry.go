package circuitbreaker

import (
	"sync"

	"github.com/google/uuid"
)

// Registry owns one Breaker per monitor for a single probe process.
type Registry struct {
	mu       sync.RWMutex
	cfg      Config
	breakers map[uuid.UUID]*Breaker
}

func NewRegistry(cfg Config) *Registry {
	return &Registry{cfg: cfg, breakers: make(map[uuid.UUID]*Breaker)}
}

func (r *Registry) Get(monitorID uuid.UUID) *Breaker {
	r.mu.RLock()
	b, ok := r.breakers[monitorID]
	r.mu.RUnlock()
	if ok {
		return b
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if b, ok := r.breakers[monitorID]; ok {
		return b
	}
	b = New(r.cfg)
	r.breakers[monitorID] = b
	return b
}

// OpenCount returns how many monitors currently have a tripped breaker —
// exported as a gauge for observability.
func (r *Registry) OpenCount() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	n := 0
	for _, b := range r.breakers {
		if b.State() == Open {
			n++
		}
	}
	return n
}
