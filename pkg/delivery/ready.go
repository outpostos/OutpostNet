package delivery

import "time"

// readyNode is an AVL index ordered by priority then source sequence. Each
// subtree also stores its latest expiry: Receive skips an entirely expired
// subtree in O(1), so even an expired prefix cannot turn Receive into a scan.
// Leased and acked entries are removed. Collect removes expired metadata later.
type readyNode struct {
	value       entry
	left, right *readyNode
	height      int
	latest      time.Time
}

func height(n *readyNode) int {
	if n == nil {
		return 0
	}
	return n.height
}
func refresh(n *readyNode) {
	n.height = 1 + max(height(n.left), height(n.right))
	n.latest = n.value.Envelope.ExpiryTime
	for _, child := range []*readyNode{n.left, n.right} {
		if child != nil && child.latest.After(n.latest) {
			n.latest = child.latest
		}
	}
}
func rotateLeft(n *readyNode) *readyNode {
	r := n.right
	n.right = r.left
	r.left = n
	refresh(n)
	refresh(r)
	return r
}
func rotateRight(n *readyNode) *readyNode {
	l := n.left
	n.left = l.right
	l.right = n
	refresh(n)
	refresh(l)
	return l
}
func balance(n *readyNode) *readyNode {
	if n == nil {
		return nil
	}
	refresh(n)
	if height(n.left)-height(n.right) > 1 {
		if height(n.left.left) < height(n.left.right) {
			n.left = rotateLeft(n.left)
		}
		return rotateRight(n)
	}
	if height(n.right)-height(n.left) > 1 {
		if height(n.right.right) < height(n.right.left) {
			n.right = rotateRight(n.right)
		}
		return rotateLeft(n)
	}
	return n
}
func insertReady(n *readyNode, e entry) *readyNode {
	if n == nil {
		return &readyNode{value: e, height: 1, latest: e.Envelope.ExpiryTime}
	}
	if before(e, n.value) {
		n.left = insertReady(n.left, e)
	} else if before(n.value, e) {
		n.right = insertReady(n.right, e)
	} else {
		n.value = e
	}
	return balance(n)
}
func removeReady(n *readyNode, e entry) *readyNode {
	if n == nil {
		return nil
	}
	if before(e, n.value) {
		n.left = removeReady(n.left, e)
	} else if before(n.value, e) {
		n.right = removeReady(n.right, e)
	} else {
		if n.left == nil {
			return n.right
		}
		if n.right == nil {
			return n.left
		}
		next := n.right
		for next.left != nil {
			next = next.left
		}
		n.value = next.value
		n.right = removeReady(n.right, next.value)
	}
	return balance(n)
}
func firstLive(n *readyNode, now time.Time) *readyNode {
	if n == nil || !now.Before(n.latest) {
		return nil
	}
	if n.left != nil && now.Before(n.left.latest) {
		return firstLive(n.left, now)
	}
	if now.Before(n.value.Envelope.ExpiryTime) {
		return n
	}
	return firstLive(n.right, now)
}
