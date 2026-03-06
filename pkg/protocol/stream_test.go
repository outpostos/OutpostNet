package protocol

import (
	"sync"
	"testing"
)

func TestStreamAllocator(t *testing.T) {
	a := NewStreamAllocator()
	id1 := a.Allocate()
	id2 := a.Allocate()
	id3 := a.Allocate()

	if id1 != 1 {
		t.Errorf("first ID: got %d, want 1", id1)
	}
	if id2 != 2 {
		t.Errorf("second ID: got %d, want 2", id2)
	}
	if id3 != 3 {
		t.Errorf("third ID: got %d, want 3", id3)
	}
}

func TestStreamAllocatorNeverZero(t *testing.T) {
	a := NewStreamAllocator()
	for i := 0; i < 100; i++ {
		id := a.Allocate()
		if id == 0 {
			t.Fatalf("allocator returned 0 on iteration %d", i)
		}
	}
}

func TestStreamAllocatorConcurrent(t *testing.T) {
	a := NewStreamAllocator()
	const goroutines = 10
	const perGoroutine = 100
	total := goroutines * perGoroutine

	results := make(chan uint32, total)
	var wg sync.WaitGroup
	wg.Add(goroutines)

	for g := 0; g < goroutines; g++ {
		go func() {
			defer wg.Done()
			for i := 0; i < perGoroutine; i++ {
				results <- a.Allocate()
			}
		}()
	}

	wg.Wait()
	close(results)

	seen := make(map[uint32]bool, total)
	for id := range results {
		if id == 0 {
			t.Fatal("allocator returned 0")
		}
		if seen[id] {
			t.Fatalf("duplicate ID: %d", id)
		}
		seen[id] = true
	}
	if len(seen) != total {
		t.Errorf("unique IDs: got %d, want %d", len(seen), total)
	}
}
