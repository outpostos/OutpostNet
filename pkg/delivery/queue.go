package delivery

import (
	"container/heap"
	"time"
)

// entryQueue supports both ordered prefixes and removal on state transitions.
// Items reference immutable envelope payloads; callers copy only selected pages.
type entryQueue struct {
	items     []entry
	positions map[MessageID]int
	less      func(entry, entry) bool
}

func newQueue(less func(entry, entry) bool) *entryQueue {
	return &entryQueue{positions: make(map[MessageID]int), less: less}
}
func (q *entryQueue) Len() int           { return len(q.items) }
func (q *entryQueue) Less(i, j int) bool { return q.less(q.items[i], q.items[j]) }
func (q *entryQueue) Swap(i, j int) {
	q.items[i], q.items[j] = q.items[j], q.items[i]
	q.positions[q.items[i].Envelope.MessageID] = i
	q.positions[q.items[j].Envelope.MessageID] = j
}
func (q *entryQueue) Push(v any) {
	e := v.(entry)
	q.positions[e.Envelope.MessageID] = len(q.items)
	q.items = append(q.items, e)
}
func (q *entryQueue) Pop() any {
	n := len(q.items) - 1
	e := q.items[n]
	q.items[n] = entry{}
	q.items = q.items[:n]
	delete(q.positions, e.Envelope.MessageID)
	return e
}
func (q *entryQueue) remove(id MessageID) {
	if i, ok := q.positions[id]; ok {
		heap.Remove(q, i)
	}
}
func before(a, b entry) bool {
	x, y := a.Envelope, b.Envelope
	if x.Priority != y.Priority {
		return x.Priority < y.Priority
	}
	if x.MessageID.Seq != y.MessageID.Seq {
		return x.MessageID.Seq < y.MessageID.Seq
	}
	if x.MessageID.SourceNodeID != y.MessageID.SourceNodeID {
		return x.MessageID.SourceNodeID < y.MessageID.SourceNodeID
	}
	return x.MessageID.SourceIncarnation < y.MessageID.SourceIncarnation
}
func (b *box) resetIndexes() {
	b.ready = make(map[string]*entryQueue)
	b.mailboxes = make(map[string]*readyNode)
	b.receipts = make(map[string]*entryQueue)
	b.expiries = newQueue(func(a, c entry) bool { return b.collectAt(a).Before(b.collectAt(c)) })
}
func (b *box) collectAt(e entry) time.Time {
	if b.identity.Kind == "outbox" && e.Status == Pending || b.identity.Kind == "inbox" && !e.Acked {
		return e.Envelope.ExpiryTime
	}
	return e.Envelope.ExpiryTime.Add(b.cfg.ClockSkew)
}
func (b *box) queueKey(e entry) string {
	if b.identity.Kind == "inbox" {
		return e.Envelope.Mailbox
	}
	return e.Envelope.DestinationNodeID
}
func removeFrom(queues map[string]*entryQueue, key string, id MessageID) {
	if q := queues[key]; q != nil {
		q.remove(id)
		if q.Len() == 0 {
			delete(queues, key)
		}
	}
}
func addTo(queues map[string]*entryQueue, key string, e entry) {
	q := queues[key]
	if q == nil {
		q = newQueue(before)
		queues[key] = q
	}
	heap.Push(q, e)
}
func (b *box) unindex(e entry) {
	id := e.Envelope.MessageID
	if b.identity.Kind == "inbox" {
		key := e.Envelope.Mailbox
		root := removeReady(b.mailboxes[key], e)
		if root == nil {
			delete(b.mailboxes, key)
		} else {
			b.mailboxes[key] = root
		}
	} else {
		removeFrom(b.ready, b.queueKey(e), id)
	}
	removeFrom(b.receipts, id.SourceNodeID, id)
	b.expiries.remove(id)
}
func (b *box) index(e entry) {
	heap.Push(b.expiries, e)
	if e.Status == Pending && !e.Acked && !b.leased[e.Envelope.MessageID] {
		if b.identity.Kind == "inbox" {
			key := e.Envelope.Mailbox
			b.mailboxes[key] = insertReady(b.mailboxes[key], e)
		} else {
			addTo(b.ready, b.queueKey(e), e)
		}
	}
	if e.ReceiptPending {
		addTo(b.receipts, e.Envelope.MessageID.SourceNodeID, e)
	}
}

// page builds an exact byte-bounded prefix, encoding each item only once.
// Restore the popped items before unlocking; a failed send retains the queue.
func (b *box) page(q *entryQueue, receipts bool) Batch {
	batch := Batch{}
	size := len(`{"envelopes":[]}`)
	if receipts {
		size = len(`{"receipts":[]}`)
	}
	var selected []entry
	for q.Len() > 0 {
		e := q.items[0]
		var n int
		r := Receipt{e.Envelope.MessageID, b.cfg.NodeID}
		if receipts {
			n = encodedSize(r)
		} else {
			n = encodedSize(e.Envelope)
		}
		if len(selected) > 0 {
			n++
		}
		if size+n > b.cfg.MaxBatchBytes {
			break
		}
		size += n
		selected = append(selected, heap.Pop(q).(entry))
		if receipts {
			batch.Receipts = append(batch.Receipts, r)
		} else {
			batch.Envelopes = append(batch.Envelopes, clone(e.Envelope))
		}
	}
	for _, e := range selected {
		heap.Push(q, e)
	}
	return batch
}
