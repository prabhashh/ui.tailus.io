package latency

// p2Quantile implements the P² (piecewise-parabolic) streaming quantile
// estimator (Jain & Chlamtac, 1985). It estimates a single quantile (e.g.
// p95) in O(1) memory and O(1) amortized time per observation, with no need
// to retain the sample history. This is what lets every probe track a
// running P95-per-monitor for potentially millions of monitors without
// keeping raw latency windows in memory.
type p2Quantile struct {
	p    float64    // target quantile, e.g. 0.95
	n    [5]int     // marker positions
	np   [5]float64 // desired marker positions
	dn   [5]float64 // increment for desired positions
	q    [5]float64 // marker heights (the estimate)
	init int        // number of observations seen while priming the first 5 markers
}

func newP2Quantile(p float64) *p2Quantile {
	e := &p2Quantile{p: p}
	e.dn = [5]float64{0, p / 2, p, (1 + p) / 2, 1}
	return e
}

func (e *p2Quantile) observe(x float64) {
	if e.init < 5 {
		e.q[e.init] = x
		e.init++
		if e.init == 5 {
			// sort the 5 priming samples to establish initial marker heights
			for i := 1; i < 5; i++ {
				for j := i; j > 0 && e.q[j-1] > e.q[j]; j-- {
					e.q[j-1], e.q[j] = e.q[j], e.q[j-1]
				}
			}
			for i := 0; i < 5; i++ {
				e.n[i] = i + 1
			}
			e.np = [5]float64{1, 1 + 2*e.p, 1 + 4*e.p, 3 + 2*e.p, 5}
		}
		return
	}

	// find cell k such that q[k] <= x < q[k+1], clamping at the ends
	var k int
	switch {
	case x < e.q[0]:
		e.q[0] = x
		k = 0
	case x >= e.q[4]:
		e.q[4] = x
		k = 3
	default:
		for i := 0; i < 4; i++ {
			if x < e.q[i+1] {
				k = i
				break
			}
		}
	}

	for i := k + 1; i < 5; i++ {
		e.n[i]++
	}
	for i := 0; i < 5; i++ {
		e.np[i] += e.dn[i]
	}

	for i := 1; i < 4; i++ {
		d := e.np[i] - float64(e.n[i])
		if (d >= 1 && e.n[i+1]-e.n[i] > 1) || (d <= -1 && e.n[i-1]-e.n[i] < -1) {
			sign := 1
			if d < 0 {
				sign = -1
			}
			qi := e.parabolic(i, sign)
			if e.q[i-1] < qi && qi < e.q[i+1] {
				e.q[i] = qi
			} else {
				e.q[i] = e.linear(i, sign)
			}
			e.n[i] += sign
		}
	}
}

func (e *p2Quantile) parabolic(i, d int) float64 {
	dd := float64(d)
	return e.q[i] + dd/float64(e.n[i+1]-e.n[i-1])*
		((float64(e.n[i]-e.n[i-1])+dd)*(e.q[i+1]-e.q[i])/float64(e.n[i+1]-e.n[i])+
			(float64(e.n[i+1]-e.n[i])-dd)*(e.q[i]-e.q[i-1])/float64(e.n[i]-e.n[i-1]))
}

func (e *p2Quantile) linear(i, d int) float64 {
	dd := float64(d)
	return e.q[i] + dd*(e.q[i+dd_int(dd)]-e.q[i])/float64(e.n[i+dd_int(dd)]-e.n[i])
}

func dd_int(d float64) int {
	if d < 0 {
		return -1
	}
	return 1
}

// value returns the current quantile estimate. Before 5 samples have been
// observed it falls back to the max seen so far (best available signal).
func (e *p2Quantile) value() float64 {
	if e.init < 5 {
		if e.init == 0 {
			return 0
		}
		max := e.q[0]
		for i := 1; i < e.init; i++ {
			if e.q[i] > max {
				max = e.q[i]
			}
		}
		return max
	}
	return e.q[2]
}
