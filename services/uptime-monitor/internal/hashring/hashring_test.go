package hashring

import (
	"fmt"
	"testing"
)

func TestGetIsStableAndDistributes(t *testing.T) {
	r := New(128)
	for i := 0; i < 5; i++ {
		r.Add(fmt.Sprintf("node-%d", i))
	}

	counts := make(map[string]int)
	for i := 0; i < 10000; i++ {
		key := fmt.Sprintf("monitor-%d", i)
		owner, err := r.Get(key)
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		counts[owner]++
	}

	for node, c := range counts {
		if c < 1000 || c > 3000 {
			t.Errorf("node %s got %d keys, expected roughly even distribution around 2000", node, c)
		}
	}
}

func TestRemoveOnlyReshufflesOwnedKeys(t *testing.T) {
	r := New(128)
	for i := 0; i < 5; i++ {
		r.Add(fmt.Sprintf("node-%d", i))
	}

	before := make(map[string]string, 2000)
	for i := 0; i < 2000; i++ {
		key := fmt.Sprintf("monitor-%d", i)
		owner, _ := r.Get(key)
		before[key] = owner
	}

	r.Remove("node-2")

	moved := 0
	for key, owner := range before {
		newOwner, err := r.Get(key)
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if newOwner != owner {
			moved++
		}
	}

	// Only keys owned by node-2 should move; with 5 nodes that's ~1/5 of keys.
	// Allow generous slack for hash variance.
	if moved > 700 {
		t.Errorf("removing one of 5 nodes moved %d/2000 keys, expected roughly 1/5 (~400)", moved)
	}
	if moved == 0 {
		t.Error("expected some keys to move after removing a node")
	}
}

func TestGetNReturnsDistinctMembers(t *testing.T) {
	r := New(128)
	for i := 0; i < 5; i++ {
		r.Add(fmt.Sprintf("node-%d", i))
	}
	members, err := r.GetN("some-key", 3)
	if err != nil {
		t.Fatalf("GetN: %v", err)
	}
	if len(members) != 3 {
		t.Fatalf("expected 3 members, got %d", len(members))
	}
	seen := map[string]bool{}
	for _, m := range members {
		if seen[m] {
			t.Errorf("duplicate member %s in GetN result", m)
		}
		seen[m] = true
	}
}

func TestEmptyRing(t *testing.T) {
	r := New(128)
	if _, err := r.Get("x"); err != ErrEmptyRing {
		t.Errorf("expected ErrEmptyRing, got %v", err)
	}
}
