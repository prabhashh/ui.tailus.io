package scheduler

import (
	"container/heap"
	"time"

	"github.com/google/uuid"
)

// dueItem represents a monitor's next scheduled run.
type dueItem struct {
	monitorID uuid.UUID
	nextRun   time.Time
	index     int // maintained by container/heap for O(log n) update/remove
}

// dueHeap is a min-heap ordered by nextRun, giving O(log n) push/pop and,
// via the index-tracking map in Scheduler, O(log n) removal/reschedule of an
// arbitrary monitor (needed when a monitor's interval changes or it's
// deleted) instead of O(n).
type dueHeap []*dueItem

func (h dueHeap) Len() int           { return len(h) }
func (h dueHeap) Less(i, j int) bool { return h[i].nextRun.Before(h[j].nextRun) }
func (h dueHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i]; h[i].index = i; h[j].index = j }
func (h *dueHeap) Push(x interface{}) {
	item := x.(*dueItem)
	item.index = len(*h)
	*h = append(*h, item)
}
func (h *dueHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	item.index = -1
	*h = old[:n-1]
	return item
}

var _ heap.Interface = (*dueHeap)(nil)
