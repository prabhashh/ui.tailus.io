// Package hashring implements consistent hashing with virtual nodes, used to
// assign monitors to scheduler shards and to assign shards to live probe
// instances. Adding or removing a member only reshuffles ~1/N of the keyspace,
// which keeps shard-rebalancing cheap when probe nodes scale in/out.
package hashring

import (
	"errors"
	"sort"
	"strconv"
	"sync"

	"github.com/cespare/xxhash/v2"
)

var ErrEmptyRing = errors.New("hashring: no members")

// Ring is a thread-safe consistent hash ring.
type Ring struct {
	mu           sync.RWMutex
	replicas     int
	sortedHashes []uint64
	hashToMember map[uint64]string
	members      map[string]bool
}

// New creates a ring with the given number of virtual nodes per member.
// 100-200 replicas gives a good load distribution for a few hundred members.
func New(replicas int) *Ring {
	if replicas <= 0 {
		replicas = 128
	}
	return &Ring{
		replicas:     replicas,
		hashToMember: make(map[uint64]string),
		members:      make(map[string]bool),
	}
}

func (r *Ring) hashKey(s string) uint64 {
	return xxhash.Sum64String(s)
}

// Add registers a member (e.g. a probe instance ID) on the ring.
func (r *Ring) Add(member string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.members[member] {
		return
	}
	r.members[member] = true
	for i := 0; i < r.replicas; i++ {
		h := r.hashKey(member + "#" + strconv.Itoa(i))
		r.hashToMember[h] = member
		r.sortedHashes = append(r.sortedHashes, h)
	}
	sort.Slice(r.sortedHashes, func(i, j int) bool { return r.sortedHashes[i] < r.sortedHashes[j] })
}

// Remove takes a member off the ring; its keyspace is redistributed to
// neighboring members.
func (r *Ring) Remove(member string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.members[member] {
		return
	}
	delete(r.members, member)
	filtered := r.sortedHashes[:0]
	for _, h := range r.sortedHashes {
		if r.hashToMember[h] == member {
			delete(r.hashToMember, h)
			continue
		}
		filtered = append(filtered, h)
	}
	r.sortedHashes = filtered
}

// Get returns the member owning the given key (e.g. a monitor ID).
func (r *Ring) Get(key string) (string, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if len(r.sortedHashes) == 0 {
		return "", ErrEmptyRing
	}
	h := r.hashKey(key)
	idx := sort.Search(len(r.sortedHashes), func(i int) bool { return r.sortedHashes[i] >= h })
	if idx == len(r.sortedHashes) {
		idx = 0
	}
	return r.hashToMember[r.sortedHashes[idx]], nil
}

// GetN returns up to n distinct members starting from key's position,
// walking the ring clockwise. Useful for replica placement (e.g. "which
// probe nodes should retry this check if the primary owner is unhealthy").
func (r *Ring) GetN(key string, n int) ([]string, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if len(r.sortedHashes) == 0 {
		return nil, ErrEmptyRing
	}
	if n > len(r.members) {
		n = len(r.members)
	}
	h := r.hashKey(key)
	idx := sort.Search(len(r.sortedHashes), func(i int) bool { return r.sortedHashes[i] >= h })

	seen := make(map[string]bool, n)
	result := make([]string, 0, n)
	for i := 0; len(result) < n && i < len(r.sortedHashes); i++ {
		pos := (idx + i) % len(r.sortedHashes)
		m := r.hashToMember[r.sortedHashes[pos]]
		if !seen[m] {
			seen[m] = true
			result = append(result, m)
		}
	}
	return result, nil
}

// Members returns a snapshot of the current member list.
func (r *Ring) Members() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]string, 0, len(r.members))
	for m := range r.members {
		out = append(out, m)
	}
	sort.Strings(out)
	return out
}
