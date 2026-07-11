package workerpool

// Router splits work across two lanes so that monitors with an open circuit
// breaker (i.e. already known to be failing/slow) can never crowd out the
// vast majority of healthy, fast checks:
//
//   - Main lane: large pool (e.g. 500 slots) for monitors whose breaker is
//     Closed or HalfOpen.
//   - Quarantine lane: small pool (e.g. 50 slots) for monitors whose breaker
//     is Open. Even if every quarantined monitor times out simultaneously,
//     it can only ever consume the quarantine lane's fixed capacity.
type Router struct {
	Main       *Pool
	Quarantine *Pool
}

func NewRouter(mainSize, quarantineSize int) *Router {
	return &Router{
		Main:       New(mainSize),
		Quarantine: New(quarantineSize),
	}
}

// Lane picks the pool for a job given whether the monitor's breaker is
// currently open.
func (r *Router) Lane(quarantined bool) *Pool {
	if quarantined {
		return r.Quarantine
	}
	return r.Main
}

func (r *Router) Wait() {
	r.Main.Wait()
	r.Quarantine.Wait()
}
